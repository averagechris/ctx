use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use chrono::Utc;
use ctx_history_core::{
    search_query_terms, utc_now, Artifact, ContextCitation, ContextCitationType, ContextLinks,
    ContextPagination, ContextTruncation, Event, EventType, FileTouched, HistoryRecord,
    RedactionState, Run, SearchMatchMode, SearchQueryPlan, Session, Summary, VcsChange, Visibility,
};
use ctx_history_store::{
    EventSearchAgentScope, EventSearchHit, EventSearchSqlFilters, FileTouchScope,
    SearchArtifactRow, SearchCaptureSourceRow, SearchEventRow, SearchFileTouchedRow, SearchRunRow,
    SearchSessionRow, SearchSummaryRow, SearchVcsChangeRow, Store,
};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

#[cfg(test)]
thread_local! {
    static DISABLE_FILE_SCOPE_PUSHDOWN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static RESIDUAL_COUNTS: std::cell::Cell<[u64; 3]> = const { std::cell::Cell::new([0; 3]) };
    static USE_PAGED_RANKED_REFERENCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static SEARCH_PHASES: std::cell::Cell<Option<SearchPhaseCapture>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SearchPhaseCapture {
    active: bool,
    path: SearchProfilePath,
    preparation_ns: u64,
    execution_hydration_ns: u64,
    assembly_ns: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SearchProfilePath {
    #[default]
    Unknown,
    FastEvent,
    FallbackRanked,
}

#[cfg(test)]
fn capture_search_phases<T>(run: impl FnOnce() -> T) -> (T, SearchPhaseCapture) {
    struct Reset(Option<SearchPhaseCapture>);
    impl Drop for Reset {
        fn drop(&mut self) {
            SEARCH_PHASES.set(self.0);
        }
    }
    let previous = SEARCH_PHASES.replace(Some(SearchPhaseCapture {
        active: true,
        ..SearchPhaseCapture::default()
    }));
    let reset = Reset(previous);
    let value = run();
    let capture = SEARCH_PHASES.get().unwrap_or_default();
    drop(reset);
    (value, capture)
}

#[cfg(test)]
fn update_search_phases(update: impl FnOnce(&mut SearchPhaseCapture)) {
    SEARCH_PHASES.with(|slot| {
        if let Some(mut capture) = slot.get() {
            if capture.active {
                update(&mut capture);
                slot.set(Some(capture));
            }
        }
    });
}

#[cfg(test)]
fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
fn increment_residual_count(index: usize) {
    RESIDUAL_COUNTS.with(|counts| {
        let mut next = counts.get();
        next[index] += 1;
        counts.set(next);
    });
}

#[cfg(test)]
fn use_paged_ranked_reference() -> bool {
    USE_PAGED_RANKED_REFERENCE.get()
}

#[cfg(test)]
fn with_paged_ranked_reference<T>(enabled: bool, run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            USE_PAGED_RANKED_REFERENCE.set(self.0);
        }
    }
    let previous = USE_PAGED_RANKED_REFERENCE.replace(enabled);
    let _reset = Reset(previous);
    run()
}

pub const SEARCH_PACKET_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_RESULT_LIMIT: usize = 10;
pub const MAX_RESULT_LIMIT: usize = 200;
pub const DEFAULT_SNIPPET_CHARS: usize = 320;
pub const MAX_QUERY_CLAUSES: usize = 32;
pub const MAX_QUERY_CLAUSE_BYTES: usize = 4 * 1024;
pub const MAX_AGGREGATE_QUERY_BYTES: usize = 64 * 1024;
const LARGE_EVENT_CORPUS_THRESHOLD: i64 = 1_024;
const FILTERED_SEARCH_PAGE_SIZE: usize = 500;
const FILTERED_SEARCH_MAX_PAGES: usize = 20;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("store error: {0}")]
    Store(#[from] ctx_history_store::StoreError),
    #[error("invalid search request: {0}")]
    InvalidRequest(String),
}

pub type Result<T> = std::result::Result<T, SearchError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketOptions {
    pub limit: usize,
    pub snippet_chars: usize,
    pub filters: SearchFilters,
    pub result_mode: SearchResultMode,
    pub match_mode: SearchMatchMode,
}

impl Default for PacketOptions {
    fn default() -> Self {
        Self {
            limit: DEFAULT_RESULT_LIMIT,
            snippet_chars: DEFAULT_SNIPPET_CHARS,
            filters: SearchFilters::default(),
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchResultMode {
    Sessions,
    Events,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SearchFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ctx_history_core::CaptureProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<String>,
    #[serde(default, rename = "workspace", skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<chrono::DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub primary_only: bool,
    #[serde(default)]
    pub include_subagents: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<EventType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<ctx_history_core::EventRole>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_roles: Vec<ctx_history_core::EventRole>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub exclude_tool_noise: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_tool_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_provider_session: Option<ProviderSessionFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderSessionFilter {
    pub provider: ctx_history_core::CaptureProvider,
    pub provider_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchPacket {
    pub schema_version: u32,
    pub query: String,
    #[serde(default)]
    pub query_plan: SearchQueryPlan,
    pub filters: SearchFilters,
    pub generated_at: chrono::DateTime<Utc>,
    pub results: Vec<SearchPacketResult>,
    pub pagination: ContextPagination,
    pub truncation: ContextTruncation,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchPacketResult {
    pub record_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_seq: Option<u64>,
    pub title: String,
    pub snippet: String,
    pub rank: f32,
    #[serde(default, skip_serializing_if = "is_default_result_scope")]
    pub result_scope: SearchResultScope,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub more_matches_in_session: usize,
    #[serde(default, skip_serializing_if = "is_zero_f32")]
    pub session_importance: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ctx_history_core::CaptureProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_source_plugin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<chrono::DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_source_exists: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub why_matched: Vec<String>,
    #[serde(default)]
    pub citations: Vec<ContextCitation>,
    #[serde(default)]
    pub links: ContextLinks,
    #[serde(default)]
    pub visibility: Visibility,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchResultScope {
    Session,
    #[default]
    Event,
}

fn is_default_result_scope(value: &SearchResultScope) -> bool {
    *value == SearchResultScope::Event
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

fn is_zero_f32(value: &f32) -> bool {
    *value == 0.0
}

#[derive(Debug, Clone)]
struct Candidate {
    record: HistoryRecord,
    context: RecordContext,
    score: f32,
    why_matched: Vec<String>,
    citations: Vec<ContextCitation>,
    primary_hit: Option<HitMetadata>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct RecordContext {
    sessions: Vec<SearchSessionRow>,
    runs: Vec<SearchRunRow>,
    events: Vec<SearchEventRow>,
    artifacts: Vec<SearchArtifactRow>,
    files_touched: Vec<SearchFileTouchedRow>,
    vcs_changes: Vec<SearchVcsChangeRow>,
    summaries: Vec<SearchSummaryRow>,
    sources: BTreeMap<Uuid, SearchCaptureSourceRow>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct FullRecordContext {
    sessions: Vec<Session>,
    runs: Vec<Run>,
    events: Vec<Event>,
    artifacts: Vec<Artifact>,
    files_touched: Vec<FileTouched>,
    vcs_changes: Vec<VcsChange>,
    summaries: Vec<Summary>,
    sources: BTreeMap<Uuid, ctx_history_core::CaptureSource>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HydrationIntent {
    #[default]
    Full,
    Compact,
}

#[derive(Debug, Clone)]
struct SearchSection {
    reason: &'static str,
    why_metadata: Vec<String>,
    weight: f32,
    text: String,
    citation: ContextCitation,
    hit: HitMetadata,
}

#[derive(Debug, Clone)]
struct HitMetadata {
    time: chrono::DateTime<Utc>,
    provider: Option<ctx_history_core::CaptureProvider>,
    provider_session_id: Option<String>,
    history_source: Option<String>,
    history_source_plugin: Option<String>,
    provider_key: Option<String>,
    source_id: Option<String>,
    source_format: Option<String>,
    session_id: Option<Uuid>,
    parent_session_id: Option<Uuid>,
    root_session_id: Option<Uuid>,
    event_id: Option<Uuid>,
    event_seq: Option<u64>,
    cwd: Option<String>,
    raw_source_path: Option<String>,
    raw_source_exists: Option<bool>,
    cursor: Option<String>,
}

struct CandidateSearch {
    candidates: Vec<Candidate>,
    scan_budget_exhausted: bool,
}

pub fn search_packet(store: &Store, query: &str, options: &PacketOptions) -> Result<SearchPacket> {
    search_packet_with_hydration(store, query, options, HydrationIntent::Full)
}

pub fn search_packet_with_hydration(
    store: &Store,
    query: &str,
    options: &PacketOptions,
    hydration: HydrationIntent,
) -> Result<SearchPacket> {
    validate_query_request(query, &[])?;
    search_packet_plan(
        store,
        SearchQueryPlan::new(options.match_mode, [query]),
        query,
        options,
        hydration,
    )
}

fn search_packet_plan(
    store: &Store,
    plan: SearchQueryPlan,
    display_query: &str,
    options: &PacketOptions,
    hydration: HydrationIntent,
) -> Result<SearchPacket> {
    #[cfg(test)]
    let phase_started = std::time::Instant::now();
    let options = normalized_options(options);
    if let Some(provider) = options.filters.provider {
        if !store.has_provider_data(provider)? {
            return Ok(empty_search_packet(display_query, plan, &options));
        }
    }
    let file_scope = file_filter_scope(store, &options.filters)?;
    if file_scope.as_ref().is_some_and(FileTouchScope::is_empty) {
        return Ok(empty_search_packet(display_query, plan, &options));
    }
    #[cfg(test)]
    update_search_phases(|capture| {
        capture.preparation_ns = duration_ns(phase_started.elapsed());
    });
    if let Some(packet) =
        fast_event_search_packet(store, &plan, display_query, &options, file_scope.as_ref())?
    {
        return Ok(packet);
    }
    #[cfg(test)]
    update_search_phases(|capture| {
        capture.path = SearchProfilePath::FallbackRanked;
        capture.preparation_ns = duration_ns(phase_started.elapsed());
    });
    #[cfg(test)]
    let ranked_started = std::time::Instant::now();
    let CandidateSearch {
        candidates,
        scan_budget_exhausted,
    } = ranked_candidates(store, Some(&plan), &options, file_scope.as_ref(), hydration)?;
    #[cfg(test)]
    update_search_phases(|capture| {
        capture.execution_hydration_ns = duration_ns(ranked_started.elapsed());
    });
    #[cfg(test)]
    let assembly_started = std::time::Instant::now();
    let mut truncation = ContextTruncation::default();
    let mut results = Vec::new();

    push_candidate_results(&mut results, &candidates, display_query, &options);

    let has_more = candidates.len() > results.len() || scan_budget_exhausted;
    if scan_budget_exhausted {
        truncation.truncated = true;
        truncation.omitted_results = 1;
        truncation.omitted_results_is_lower_bound = true;
        truncation.reason = Some("scan_budget".to_owned());
    } else if candidates.len() > results.len() {
        truncation.truncated = true;
        truncation.omitted_results = (candidates.len() - results.len()) as u32;
        truncation.reason = Some("limit".to_owned());
    }

    let cursor_offset = results.len();
    let packet = SearchPacket {
        schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        query: display_query.to_owned(),
        query_plan: plan,
        filters: options.filters,
        generated_at: utc_now(),
        results,
        pagination: pagination(Some(cursor_offset), has_more),
        truncation,
    };
    #[cfg(test)]
    update_search_phases(|capture| {
        capture.assembly_ns = duration_ns(assembly_started.elapsed());
    });
    Ok(packet)
}

pub fn search_packet_terms(
    store: &Store,
    query: &str,
    terms: &[String],
    options: &PacketOptions,
) -> Result<SearchPacket> {
    search_packet_terms_with_hydration(store, query, terms, options, HydrationIntent::Full)
}

pub fn search_packet_terms_with_hydration(
    store: &Store,
    query: &str,
    terms: &[String],
    options: &PacketOptions,
    hydration: HydrationIntent,
) -> Result<SearchPacket> {
    validate_query_request(query, terms)?;
    let options = normalized_options(options);
    let search_terms = composed_search_terms(query, terms)
        .into_iter()
        .filter(|term| !SearchQueryPlan::new(options.match_mode, [term.as_str()]).is_empty())
        .collect::<Vec<_>>();
    if search_terms.len() <= 1 {
        return search_packet_with_hydration(
            store,
            search_terms.first().map_or(query, String::as_str),
            &options,
            hydration,
        );
    }

    let mut child_options = options.clone();
    child_options.limit = options
        .limit
        .saturating_mul(2)
        .max(options.limit)
        .min(MAX_RESULT_LIMIT);

    let mut merged_results = Vec::<SearchPacketResult>::new();
    let mut result_index = BTreeMap::<Uuid, usize>::new();
    let mut truncated = false;
    let mut omitted_results = 0_u32;
    for term in &search_terms {
        let packet = search_packet_with_hydration(store, term, &child_options, hydration)?;
        truncated |= packet.truncation.truncated;
        omitted_results = omitted_results.saturating_add(packet.truncation.omitted_results);
        for mut result in packet.results {
            push_unique_why(&mut result.why_matched, format!("term:{term}"));
            let result_key = search_result_merge_key(&result, options.result_mode);
            if let Some(index) = result_index.get(&result_key).copied() {
                merge_search_result(&mut merged_results[index], result);
            } else {
                result_index.insert(result_key, merged_results.len());
                merged_results.push(result);
            }
        }
    }

    merged_results.sort_by(compare_search_results);
    let has_more = merged_results.len() > options.limit || truncated;
    if merged_results.len() > options.limit {
        omitted_results =
            omitted_results.saturating_add((merged_results.len() - options.limit) as u32);
        merged_results.truncate(options.limit);
    }
    normalize_search_result_ranks(&mut merged_results);

    let truncation = if has_more {
        ContextTruncation {
            truncated: true,
            reason: Some(if truncated { "source_limit" } else { "limit" }.to_owned()),
            omitted_results: omitted_results.max(1),
            omitted_results_is_lower_bound: truncated,
        }
    } else {
        ContextTruncation::default()
    };
    let cursor_offset = merged_results.len();

    Ok(SearchPacket {
        schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        query: search_terms.join(" OR "),
        query_plan: SearchQueryPlan::new(
            options.match_mode,
            search_terms.iter().map(String::as_str),
        ),
        filters: options.filters,
        generated_at: utc_now(),
        results: merged_results,
        pagination: pagination(Some(cursor_offset), has_more),
        truncation,
    })
}

/// Bound public repeated-query work before normalization or deduplication.
/// Raw duplicate and blank clauses count toward the request-level limit.
pub fn validate_query_request(query: &str, terms: &[String]) -> Result<()> {
    if terms.len() > MAX_QUERY_CLAUSES {
        return Err(SearchError::InvalidRequest(format!(
            "at most {MAX_QUERY_CLAUSES} repeated query clauses are allowed"
        )));
    }
    for clause in std::iter::once(query).chain(terms.iter().map(String::as_str)) {
        if clause.len() > MAX_QUERY_CLAUSE_BYTES {
            return Err(SearchError::InvalidRequest(format!(
                "each query clause must be at most {MAX_QUERY_CLAUSE_BYTES} UTF-8 bytes"
            )));
        }
    }
    let aggregate = std::iter::once(query.len())
        .chain(terms.iter().map(String::len))
        .try_fold(0usize, |total, bytes| total.checked_add(bytes));
    if aggregate.map_or(true, |bytes| bytes > MAX_AGGREGATE_QUERY_BYTES) {
        return Err(SearchError::InvalidRequest(format!(
            "aggregate query text must be at most {MAX_AGGREGATE_QUERY_BYTES} UTF-8 bytes"
        )));
    }
    Ok(())
}

fn composed_search_terms(query: &str, terms: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::<String>::new();
    let mut out = Vec::new();
    for value in std::iter::once(query).chain(terms.iter().map(String::as_str)) {
        let Some(term) = non_blank(value) else {
            continue;
        };
        let key = term.to_lowercase();
        if seen.insert(key) {
            out.push(term);
        }
    }
    out
}

fn search_result_merge_key(result: &SearchPacketResult, result_mode: SearchResultMode) -> Uuid {
    if result_mode == SearchResultMode::Sessions {
        result.session_id.unwrap_or(result.record_id)
    } else {
        result.event_id.unwrap_or(result.record_id)
    }
}

fn merge_search_result(existing: &mut SearchPacketResult, incoming: SearchPacketResult) {
    let incoming_rank = incoming.rank;
    let existing_rank = existing.rank;
    if incoming_rank > existing_rank {
        existing.title = incoming.title.clone();
        existing.snippet = incoming.snippet.clone();
        existing.record_id = incoming.record_id;
        existing.event_id = incoming.event_id;
        existing.event_seq = incoming.event_seq;
        existing.timestamp = incoming.timestamp;
        existing.cwd = incoming.cwd.clone();
        existing.provider = incoming.provider;
        existing.provider_session_id = incoming.provider_session_id.clone();
        existing.history_source = incoming.history_source.clone();
        existing.history_source_plugin = incoming.history_source_plugin.clone();
        existing.provider_key = incoming.provider_key.clone();
        existing.source_id = incoming.source_id.clone();
        existing.source_format = incoming.source_format.clone();
        existing.raw_source_path = incoming.raw_source_path.clone();
        existing.raw_source_exists = incoming.raw_source_exists;
        existing.cursor = incoming.cursor.clone();
    } else {
        existing.history_source = existing
            .history_source
            .clone()
            .or(incoming.history_source.clone());
        existing.history_source_plugin = existing
            .history_source_plugin
            .clone()
            .or(incoming.history_source_plugin.clone());
        existing.provider_key = existing
            .provider_key
            .clone()
            .or(incoming.provider_key.clone());
        existing.source_id = existing.source_id.clone().or(incoming.source_id.clone());
        existing.source_format = existing
            .source_format
            .clone()
            .or(incoming.source_format.clone());
    }
    existing.rank = existing_rank.max(incoming_rank) + 0.08;
    existing.more_matches_in_session = existing
        .more_matches_in_session
        .saturating_add(1)
        .saturating_add(incoming.more_matches_in_session);
    if existing.result_scope == SearchResultScope::Session {
        existing.session_importance =
            session_importance(existing.rank, existing.more_matches_in_session);
    }
    for reason in incoming.why_matched {
        push_unique_why(&mut existing.why_matched, reason);
    }
    for citation in incoming.citations {
        let duplicate = existing.citations.iter().any(|existing_citation| {
            existing_citation.citation_type == citation.citation_type
                && existing_citation.id == citation.id
        });
        if !duplicate {
            existing.citations.push(citation);
        }
    }
}

fn push_unique_why(why_matched: &mut Vec<String>, reason: String) {
    if !why_matched.iter().any(|value| value == &reason) {
        why_matched.push(reason);
    }
}

fn compare_search_results(left: &SearchPacketResult, right: &SearchPacketResult) -> Ordering {
    right
        .rank
        .partial_cmp(&left.rank)
        .unwrap_or(Ordering::Equal)
        .then_with(|| right.timestamp.cmp(&left.timestamp))
        .then_with(|| left.record_id.cmp(&right.record_id))
}

fn push_candidate_results(
    results: &mut Vec<SearchPacketResult>,
    candidates: &[Candidate],
    query: &str,
    options: &PacketOptions,
) {
    let mut clustered_index = BTreeMap::<Uuid, usize>::new();
    let plan = SearchQueryPlan::new(options.match_mode, [query]);
    for candidate in candidates {
        let mut result = candidate_search_result(candidate, query, &plan, options);
        if options.result_mode == SearchResultMode::Sessions {
            let cluster_id = result.session_id.unwrap_or(result.record_id);
            if let Some(index) = clustered_index.get(&cluster_id).copied() {
                let existing = &mut results[index];
                existing.more_matches_in_session =
                    existing.more_matches_in_session.saturating_add(1);
                existing.session_importance =
                    session_importance(existing.rank, existing.more_matches_in_session);
                continue;
            }
            if result.session_id.is_some() {
                result.result_scope = SearchResultScope::Session;
                result.session_importance = session_importance(result.rank, 0);
            }
            clustered_index.insert(cluster_id, results.len());
        }
        results.push(result);
        if results.len() >= options.limit {
            break;
        }
    }
}

fn candidate_search_result(
    candidate: &Candidate,
    query: &str,
    plan: &SearchQueryPlan,
    options: &PacketOptions,
) -> SearchPacketResult {
    let display_hit = candidate_display_hit(candidate, &options.filters);
    let record_id = candidate
        .primary_hit
        .as_ref()
        .and_then(|hit| hit.event_id)
        .unwrap_or(candidate.record.id);
    SearchPacketResult {
        record_id,
        session_id: display_hit.as_ref().and_then(|hit| hit.session_id),
        event_id: display_hit.as_ref().and_then(|hit| hit.event_id),
        event_seq: display_hit.as_ref().and_then(|hit| hit.event_seq),
        title: local_snippet(&candidate.record.title, 240),
        snippet: search_snippet(
            &candidate.record,
            &candidate.context,
            query,
            plan,
            options.snippet_chars,
            &options.filters,
        ),
        rank: candidate.score,
        result_scope: SearchResultScope::Event,
        more_matches_in_session: 0,
        session_importance: 0.0,
        provider: display_hit.as_ref().and_then(|hit| hit.provider),
        provider_session_id: display_hit
            .as_ref()
            .and_then(|hit| hit.provider_session_id.clone()),
        history_source: display_hit
            .as_ref()
            .and_then(|hit| hit.history_source.clone()),
        history_source_plugin: display_hit
            .as_ref()
            .and_then(|hit| hit.history_source_plugin.clone()),
        provider_key: display_hit
            .as_ref()
            .and_then(|hit| hit.provider_key.clone()),
        source_id: display_hit.as_ref().and_then(|hit| hit.source_id.clone()),
        source_format: display_hit
            .as_ref()
            .and_then(|hit| hit.source_format.clone()),
        timestamp: display_hit.as_ref().map(|hit| hit.time),
        cwd: display_hit.as_ref().and_then(|hit| hit.cwd.clone()),
        raw_source_path: display_hit
            .as_ref()
            .and_then(|hit| hit.raw_source_path.clone()),
        raw_source_exists: display_hit.as_ref().and_then(|hit| hit.raw_source_exists),
        cursor: display_hit.as_ref().and_then(|hit| hit.cursor.clone()),
        why_matched: candidate.why_matched.clone(),
        citations: candidate.citations.clone(),
        links: links_for(&candidate.record, options),
        visibility: Visibility::LocalOnly,
    }
}

fn candidate_display_hit(candidate: &Candidate, filters: &SearchFilters) -> Option<HitMetadata> {
    if let Some(hit) = &candidate.primary_hit {
        if hit.event_id.is_some() {
            return Some(hit.clone());
        }
    }
    if let Some(event) = candidate.context.events.iter().find(|event| {
        let hit = event_hit(event, &candidate.context);
        filters
            .provider
            .map_or(true, |provider| hit.provider == Some(provider))
            && filters
                .session
                .map_or(true, |id| hit.session_id == Some(id))
            && hit_matches_history_source_filter(&hit, filters)
    }) {
        return Some(event_hit(event, &candidate.context));
    }
    if let Some(hit) = &candidate.primary_hit {
        if hit.provider.is_some() || hit.session_id.is_some() {
            return Some(hit.clone());
        }
    }
    candidate
        .context
        .sessions
        .iter()
        .find(|session| {
            filters
                .provider
                .map_or(true, |provider| session.provider == provider)
                && filters.session.map_or(true, |id| session.id == id)
                && hit_matches_history_source_filter(
                    &session_hit(session, &candidate.context),
                    filters,
                )
        })
        .or_else(|| candidate.context.sessions.first())
        .map(|session| session_hit(session, &candidate.context))
}

fn fast_event_search_packet(
    store: &Store,
    plan: &SearchQueryPlan,
    display_query: &str,
    options: &PacketOptions,
    file_scope: Option<&FileTouchScope>,
) -> Result<Option<SearchPacket>> {
    #[cfg(test)]
    let fast_started = std::time::Instant::now();
    if plan.is_empty() {
        return Ok(None);
    }
    if has_history_source_filter(&options.filters) {
        return Ok(None);
    }
    if !store.has_at_least_events(LARGE_EVENT_CORPUS_THRESHOLD)? {
        return Ok(None);
    }
    if !store.has_event_search_index()? {
        return Ok(None);
    }

    let target_results = options.limit.saturating_add(1);
    // Exact-semantics filters are pushed into the ranked SQL page; only the
    // filters below still require Rust-side scanning across candidate pages
    // (with the scan budget). `event_hit_matches_filters` stays the final
    // authority over every hit either way, and `plan.matches_text` re-verifies
    // every hit against ctx literal-token semantics (FTS unicode61 folds
    // diacritics; ctx does not).
    let sql_filters = sql_pushdown_filters(&options.filters, file_scope);
    let residual_filtered = has_residual_event_filters(&options.filters, file_scope);
    // Bounded rerank pool: the final sort order is not the SQL bm25 order in
    // any match mode — `any`-mode ranking rewards matched-token counts, and
    // the role/event-type relevance penalty rescales every mode — so every
    // search (unfiltered, pushdown-only, default-scope, or residual-filtered)
    // must collect the documented pool of at least
    // max(limit * 8, 50, limit + 1) candidates before the final sort. A
    // deeply bm25-ranked user/assistant hit inside the pool can therefore be
    // promoted over higher-bm25 tool/command noise; hits ranked beyond the
    // pool by raw text relevance are not reconsidered.
    let collection_target = options.limit.saturating_mul(8).max(50).max(target_results);
    let clustered = options.result_mode == SearchResultMode::Sessions;
    // Clustered and residual-filtered searches scan wide pages (residual
    // filters are applied in Rust across the stream); with no residual
    // filters (unfiltered or pushdown-only), one exact SQL page sized to the
    // collection target satisfies the pool unless literal re-verification
    // drops rows, in which case paging continues under the same scan budget.
    let page_size = if clustered || residual_filtered {
        FILTERED_SEARCH_PAGE_SIZE.max(collection_target)
    } else {
        collection_target
    };
    let mut results = Vec::new();
    let mut clustered_results = Vec::<SearchPacketResult>::new();
    let mut clustered_index = BTreeMap::<Uuid, usize>::new();
    let mut pages_scanned = 0_usize;
    let mut scan_budget_exhausted = false;

    #[cfg(test)]
    update_search_phases(|capture| {
        capture.path = SearchProfilePath::FastEvent;
        capture.preparation_ns = capture
            .preparation_ns
            .saturating_add(duration_ns(fast_started.elapsed()));
    });
    #[cfg(test)]
    let execution_started = std::time::Instant::now();
    #[cfg(test)]
    let mut callback_ns = 0_u64;

    scan_event_hit_batches(
        store,
        plan,
        page_size,
        FILTERED_SEARCH_MAX_PAGES,
        &sql_filters,
        clustered || residual_filtered,
        |hits| {
            #[cfg(test)]
            let callback_started = std::time::Instant::now();
            pages_scanned = pages_scanned.saturating_add(1);
            let page_len = hits.len();

            for hit in hits {
                #[cfg(test)]
                increment_residual_count(0);
                // Hits come from an FTS MATCH over exactly `hit.preview`; the
                // ASCII-exact case needs no Rust re-verification (see
                // `fts_match_is_exact_for`), and the reference differential in
                // tests always re-verifies, pinning this skip to full semantics.
                if !plan.fts_match_is_exact_for(&hit.preview) && !plan.matches_text(&hit.preview) {
                    continue;
                }
                if !event_hit_matches_filters(hit, &options.filters, file_scope) {
                    #[cfg(test)]
                    increment_residual_count(1);
                    continue;
                }
                #[cfg(test)]
                increment_residual_count(2);
                if clustered {
                    let cluster_id = hit.session_id.unwrap_or(hit.event_id);
                    if let Some(index) = clustered_index.get(&cluster_id).copied() {
                        let existing = &mut clustered_results[index];
                        // Decide best-representative replacement from the raw hit
                        // keys (`compare_search_results` order for event results:
                        // rank desc, timestamp desc, record_id asc) before paying
                        // for snippet/citation construction, which includes a
                        // filesystem existence probe per result.
                        let candidate_rank = event_hit_rank(hit, plan);
                        let replaces = existing
                            .rank
                            .partial_cmp(&candidate_rank)
                            .unwrap_or(Ordering::Equal)
                            .then_with(|| existing.timestamp.cmp(&Some(hit.occurred_at)))
                            .then_with(|| hit.event_id.cmp(&existing.record_id))
                            .is_lt();
                        let more = existing.more_matches_in_session.saturating_add(1);
                        if replaces {
                            let mut candidate = event_search_result(
                                hit,
                                display_query,
                                plan,
                                options.snippet_chars,
                            );
                            candidate.result_scope = if candidate.session_id.is_some() {
                                SearchResultScope::Session
                            } else {
                                SearchResultScope::Event
                            };
                            *existing = candidate;
                        }
                        existing.more_matches_in_session = more;
                        existing.session_importance = session_importance(existing.rank, more);
                    } else {
                        let mut result =
                            event_search_result(hit, display_query, plan, options.snippet_chars);
                        result.result_scope = if result.session_id.is_some() {
                            SearchResultScope::Session
                        } else {
                            SearchResultScope::Event
                        };
                        result.session_importance = session_importance(result.rank, 0);
                        clustered_index.insert(cluster_id, clustered_results.len());
                        clustered_results.push(result);
                    }
                } else {
                    let result =
                        event_search_result(hit, display_query, plan, options.snippet_chars);
                    results.push(result);
                }
            }

            let enough_results = if clustered {
                clustered_results.len() >= collection_target
            } else {
                results.len() >= collection_target
            };
            if enough_results || page_len < page_size {
                #[cfg(test)]
                {
                    callback_ns =
                        callback_ns.saturating_add(duration_ns(callback_started.elapsed()));
                }
                return false;
            }
            if pages_scanned >= FILTERED_SEARCH_MAX_PAGES {
                scan_budget_exhausted = true;
                #[cfg(test)]
                {
                    callback_ns =
                        callback_ns.saturating_add(duration_ns(callback_started.elapsed()));
                }
                return false;
            }
            #[cfg(test)]
            {
                callback_ns = callback_ns.saturating_add(duration_ns(callback_started.elapsed()));
            }
            true
        },
    )?;

    #[cfg(test)]
    update_search_phases(|capture| {
        capture.execution_hydration_ns =
            duration_ns(execution_started.elapsed()).saturating_sub(callback_ns);
        capture.assembly_ns = callback_ns;
    });
    #[cfg(test)]
    let final_assembly_started = std::time::Instant::now();

    if clustered {
        results = clustered_results;
    }
    results.sort_by(compare_search_results);
    if results.is_empty() && !scan_budget_exhausted {
        return Ok(None);
    }
    let has_more = results.len() > options.limit || scan_budget_exhausted;
    if results.len() > options.limit {
        results.truncate(options.limit);
    }
    normalize_search_result_ranks(&mut results);

    let truncation = if scan_budget_exhausted {
        ContextTruncation {
            truncated: true,
            reason: Some("scan_budget".to_owned()),
            omitted_results: 1,
            omitted_results_is_lower_bound: true,
        }
    } else if has_more {
        ContextTruncation {
            truncated: true,
            reason: Some("limit".to_owned()),
            omitted_results: 1,
            omitted_results_is_lower_bound: true,
        }
    } else {
        ContextTruncation::default()
    };

    let cursor_offset = results.len();
    let packet = SearchPacket {
        schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        query: display_query.to_owned(),
        query_plan: plan.clone(),
        filters: options.filters.clone(),
        generated_at: utc_now(),
        results,
        pagination: pagination(Some(cursor_offset), has_more),
        truncation,
    };
    #[cfg(test)]
    update_search_phases(|capture| {
        capture.assembly_ns = capture
            .assembly_ns
            .saturating_add(duration_ns(final_assembly_started.elapsed()));
    });
    Ok(Some(packet))
}

/// Residual-filtered and clustered searches consume one bounded SQLite
/// cursor. Plain searches retain LIMIT/OFFSET paging: their common one-page
/// case is measurably cheaper, while later pages remain necessary when FTS
/// unicode61 folding yields candidates rejected by literal re-verification.
/// Tests can force paging through an invocation-local thread-local switch.
fn scan_event_hit_batches<F>(
    store: &Store,
    plan: &SearchQueryPlan,
    page_size: usize,
    max_pages: usize,
    filters: &EventSearchSqlFilters,
    stream_one_statement: bool,
    consume: F,
) -> ctx_history_store::Result<()>
where
    F: FnMut(&[EventSearchHit]) -> bool,
{
    #[cfg(test)]
    let use_paged = use_paged_ranked_reference() || !stream_one_statement;
    #[cfg(not(test))]
    let use_paged = !stream_one_statement;
    if use_paged {
        let mut consume = consume;
        let page_size = page_size.max(1);
        let mut offset = 0_usize;
        for _ in 0..max_pages.max(1) {
            let hits =
                store.search_event_hits_plan_page_filtered(plan, page_size, offset, filters)?;
            let page_len = hits.len();
            if !consume(&hits) || page_len < page_size {
                break;
            }
            offset = offset.saturating_add(page_size);
        }
        return Ok(());
    }
    store.scan_event_hits_plan_filtered(plan, page_size, max_pages, filters, consume)
}

fn empty_search_packet(
    query: &str,
    plan: SearchQueryPlan,
    options: &PacketOptions,
) -> SearchPacket {
    SearchPacket {
        schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        query: query.to_owned(),
        query_plan: plan,
        filters: options.filters.clone(),
        generated_at: utc_now(),
        results: Vec::new(),
        pagination: pagination(Some(0), false),
        truncation: ContextTruncation::default(),
    }
}

/// Builds the exact-semantics store-level filters that are pushed into the
/// ranked SQL page. Each pushed predicate is provably equivalent to the
/// corresponding branch of `event_hit_matches_filters` /
/// `event_hit_matches_agent_scope`, so pushing it can never change which hits
/// survive Rust filtering — it only stops non-matching rows from being
/// hydrated and scanned.
fn sql_pushdown_filters(
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
) -> EventSearchSqlFilters {
    let file_scope = file_scope.cloned();
    #[cfg(test)]
    let file_scope = if DISABLE_FILE_SCOPE_PUSHDOWN.get() {
        None
    } else {
        file_scope
    };
    let agent_scope = if filters.session.is_some() {
        // An explicit session filter makes the Rust agent-scope check vacuous:
        // `event_hit_matches_agent_scope` accepts any hit of that session
        // before consulting primary/subagent state. Pushing a scope predicate
        // here would wrongly drop subagent rows of the requested session, so
        // scope pushdown is disabled and only session equality is pushed.
        None
    } else if filters.primary_only {
        Some(EventSearchAgentScope::PrimaryOnly)
    } else if filters.include_subagents {
        None
    } else {
        Some(EventSearchAgentScope::PrimaryOrSessionless)
    };
    EventSearchSqlFilters {
        session_id: filters.session,
        provider: filters.provider,
        since: filters.since,
        event_type: filters.event_type,
        agent_scope,
        // Role include/exclude and the tool-noise event-type set are exact in
        // SQL: they test the same `e.role` / `e.event_type` columns phase two
        // hydrates into `hit.role` / `hit.event_type`, both CHECK-constrained
        // to the enum domain, with NULL roles mapped to no bitmask bit exactly
        // like `role_matches` treats `None`. `exclude_tool_names` is *not*
        // pushed: it derives executable names from payload JSON in Rust
        // (`event_tool_names_from_payload`) and has no provably-equivalent
        // SQL form, so it stays a residual filter below.
        roles: filters.roles.clone(),
        exclude_roles: filters.exclude_roles.clone(),
        exclude_tool_noise: filters.exclude_tool_noise,
        file_scope,
    }
}

/// Filters that cannot be pushed into the ranked SQL page and still require
/// Rust-side checking over candidate pages: repo substring matching over
/// cwd/raw-source/workspace, file-touch scopes (retained as the oracle after
/// SQL pushdown), excluded provider sessions,
/// history-source identity, and `exclude_tool_names` (payload-derived
/// executable names). The scan budget continues to bound these.
/// The history-source branch is defensive today: the fast event path returns
/// before reaching this helper whenever `has_history_source_filter` is true,
/// but keeping it here makes residual classification safe if that guard is
/// ever relaxed or this helper is reused.
fn has_residual_event_filters(
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
) -> bool {
    filters
        .repo
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || file_scope.is_some()
        || filters.exclude_provider_session.is_some()
        || filters
            .exclude_tool_names
            .iter()
            .any(|value| !value.trim().is_empty())
        || has_history_source_filter(filters)
}

fn event_hit_matches_filters(
    hit: &EventSearchHit,
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
) -> bool {
    if let Some(session_id) = filters.session {
        if hit.session_id != Some(session_id) {
            return false;
        }
    }
    if event_hit_matches_excluded_provider_session(hit, filters) {
        return false;
    }
    if let Some(provider) = filters.provider {
        if hit.provider != Some(provider) {
            return false;
        }
    }
    if let Some(since) = filters.since {
        if hit.occurred_at < since {
            return false;
        }
    }
    if !event_hit_matches_agent_scope(hit, filters) {
        return false;
    }
    if let Some(event_type) = filters.event_type {
        if hit.event_type != event_type {
            return false;
        }
    }
    if !role_matches(hit.role, filters) || event_hit_is_excluded_tool_noise(hit, filters) {
        return false;
    }
    if let Some(repo) = filters
        .repo
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let repo = repo.to_lowercase();
        let matches_repo = [
            hit.cwd.as_deref(),
            hit.raw_source_path.as_deref(),
            hit.record_workspace.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|value| value.to_lowercase().contains(&repo));
        if !matches_repo {
            return false;
        }
    }
    if let Some(scope) = file_scope {
        if !file_scope_matches_hit(scope, hit) {
            return false;
        }
    }
    true
}

fn role_matches(role: Option<ctx_history_core::EventRole>, filters: &SearchFilters) -> bool {
    if !filters.roles.is_empty() && !role.is_some_and(|role| filters.roles.contains(&role)) {
        return false;
    }
    if role.is_some_and(|role| filters.exclude_roles.contains(&role)) {
        return false;
    }
    true
}

fn event_hit_is_excluded_tool_noise(hit: &EventSearchHit, filters: &SearchFilters) -> bool {
    if filters.exclude_tool_noise
        && matches!(
            hit.event_type,
            EventType::ToolCall
                | EventType::ToolOutput
                | EventType::CommandStarted
                | EventType::CommandOutput
                | EventType::CommandFinished
        )
    {
        return true;
    }
    is_tool_or_command_event(hit.event_type)
        && excluded_tool_name_matches(filters, |needle| {
            hit.tool_names.iter().any(|name| name == needle)
        })
}

/// True when any `--exclude-tool` name (normalized like the stored
/// `tool_names` projection: trimmed, ASCII-lowercased) satisfies `matches`.
/// Empty or whitespace-only names never match anything.
fn excluded_tool_name_matches(
    filters: &SearchFilters,
    mut matches: impl FnMut(&str) -> bool,
) -> bool {
    filters
        .exclude_tool_names
        .iter()
        .map(|value| normalized_tool_name(value))
        .filter(|needle| !needle.is_empty())
        .any(|needle| matches(&needle))
}

fn event_hit_matches_excluded_provider_session(
    hit: &EventSearchHit,
    filters: &SearchFilters,
) -> bool {
    filters
        .exclude_provider_session
        .as_ref()
        .is_some_and(|excluded| {
            (hit.provider == Some(excluded.provider)
                && hit.session_external_session_id.as_deref()
                    == Some(excluded.provider_session_id.as_str()))
                || excluded_session_tree_matches(
                    excluded,
                    hit.session_id,
                    hit.session_parent_session_id,
                    hit.session_root_session_id,
                )
        })
}

fn hit_matches_excluded_provider_session(hit: &HitMetadata, filters: &SearchFilters) -> bool {
    filters
        .exclude_provider_session
        .as_ref()
        .is_some_and(|excluded| {
            (hit.provider == Some(excluded.provider)
                && hit.provider_session_id.as_deref()
                    == Some(excluded.provider_session_id.as_str()))
                || excluded_session_tree_matches(
                    excluded,
                    hit.session_id,
                    hit.parent_session_id,
                    hit.root_session_id,
                )
        })
}

fn context_has_excluded_provider_session(context: &RecordContext, filters: &SearchFilters) -> bool {
    filters
        .exclude_provider_session
        .as_ref()
        .is_some_and(|excluded| {
            context.sessions.iter().any(|session| {
                (session.provider == excluded.provider
                    && session.external_session_id.as_deref()
                        == Some(excluded.provider_session_id.as_str()))
                    || excluded_session_tree_matches(
                        excluded,
                        Some(session.id),
                        session.parent_session_id,
                        session.root_session_id,
                    )
            })
        })
}

fn excluded_session_tree_matches(
    excluded: &ProviderSessionFilter,
    session_id: Option<Uuid>,
    parent_session_id: Option<Uuid>,
    root_session_id: Option<Uuid>,
) -> bool {
    excluded.session_id.is_some_and(|excluded_session_id| {
        session_id == Some(excluded_session_id)
            || parent_session_id == Some(excluded_session_id)
            || root_session_id == Some(excluded_session_id)
    })
}

fn file_filter_scope(store: &Store, filters: &SearchFilters) -> Result<Option<FileTouchScope>> {
    let Some(file) = filters
        .file
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    Ok(Some(store.file_touch_scope(file)?))
}

fn file_scope_matches_hit(scope: &FileTouchScope, hit: &EventSearchHit) -> bool {
    scope.event_ids.contains(&hit.event_id)
        || hit
            .run_id
            .is_some_and(|run_id| scope.run_ids.contains(&run_id))
        || hit
            .session_id
            .is_some_and(|session_id| scope.session_ids.contains(&session_id))
        || hit
            .history_record_id
            .is_some_and(|record_id| scope.history_record_ids.contains(&record_id))
}

/// The ranking key `event_search_result` assigns to a hit: bm25-derived rank
/// plus the `any`-mode matched-token reward, scaled by the role/event-type
/// relevance penalty so incidental tool and command matches rank behind
/// equivalent user/assistant messages. Kept separate so the clustered fast
/// path can compare candidates without building full results.
fn event_hit_rank(hit: &EventSearchHit, plan: &SearchQueryPlan) -> f32 {
    ((-hit.score as f32).max(0.0)
        + if matches!(plan.mode, SearchMatchMode::Any) {
            matched_token_count(&hit.preview, plan) as f32
        } else {
            0.0
        })
        * event_relevance_penalty(hit)
}

fn event_search_result(
    hit: &EventSearchHit,
    query: &str,
    plan: &SearchQueryPlan,
    snippet_chars: usize,
) -> SearchPacketResult {
    let terms = query_terms(query);
    let raw_source_exists = hit
        .raw_source_path
        .as_deref()
        .map(|path| Path::new(path).exists());
    let mut citations = vec![ContextCitation {
        citation_type: ContextCitationType::Event,
        id: hit.event_id,
        label: event_result_label(hit).to_owned(),
        time: hit.occurred_at,
        provider: hit.provider,
        session_id: hit.session_id,
        event_seq: Some(hit.seq),
        raw_source_path: hit.raw_source_path.clone(),
        raw_source_exists,
        cursor: hit.cursor.clone(),
    }];
    if let Some(session_id) = hit.session_id {
        citations.push(ContextCitation {
            citation_type: ContextCitationType::Session,
            id: session_id,
            label: "session".to_owned(),
            time: hit.occurred_at,
            provider: hit.provider,
            session_id: Some(session_id),
            event_seq: None,
            raw_source_path: hit.raw_source_path.clone(),
            raw_source_exists,
            cursor: hit.cursor.clone(),
        });
    }

    SearchPacketResult {
        record_id: hit.event_id,
        session_id: hit.session_id,
        event_id: Some(hit.event_id),
        event_seq: Some(hit.seq),
        title: event_result_title(hit),
        snippet: matched_snippet(&hit.preview, plan, &terms, snippet_chars),
        rank: event_hit_rank(hit, plan),
        result_scope: SearchResultScope::Event,
        more_matches_in_session: 0,
        session_importance: 0.0,
        provider: hit.provider,
        provider_session_id: hit.session_external_session_id.clone(),
        history_source: hit.history_source.clone(),
        history_source_plugin: hit.history_source_plugin.clone(),
        provider_key: hit.provider_key.clone(),
        source_id: hit.source_id.clone(),
        source_format: hit.source_format.clone(),
        timestamp: Some(hit.occurred_at),
        cwd: hit.cwd.clone(),
        raw_source_path: hit.raw_source_path.clone(),
        raw_source_exists,
        cursor: hit.cursor.clone(),
        why_matched: event_why_matched(hit),
        citations,
        links: ContextLinks::default(),
        visibility: Visibility::LocalOnly,
    }
}

fn event_result_title(hit: &EventSearchHit) -> String {
    let provider = hit
        .provider
        .map(|provider| provider.as_str())
        .unwrap_or("agent");
    let source = hit
        .session_external_session_id
        .as_deref()
        .or_else(|| {
            hit.raw_source_path
                .as_deref()
                .and_then(|path| Path::new(path).file_name().and_then(|value| value.to_str()))
        })
        .map(|value| local_snippet(value, 80));
    match source {
        Some(source) => format!("{provider} {} - {source}", event_result_label(hit)),
        None => format!("{provider} {}", event_result_label(hit)),
    }
}

fn event_result_label(hit: &EventSearchHit) -> &'static str {
    match hit.event_type {
        EventType::Message => match hit.role {
            Some(ctx_history_core::EventRole::User) => "user message",
            Some(ctx_history_core::EventRole::Assistant) => "assistant message",
            Some(ctx_history_core::EventRole::System) => "system message",
            _ => "message",
        },
        EventType::ToolCall => "tool call",
        EventType::ToolOutput => "tool output",
        EventType::CommandStarted => "command started",
        EventType::CommandOutput => "command output",
        EventType::CommandFinished => "command finished",
        EventType::FileTouched => "file touched",
        EventType::VcsChange => "vcs change",
        EventType::Artifact => "artifact",
        EventType::Summary => "summary",
        EventType::Notice => "notice",
    }
}

fn event_reason(event_type: EventType) -> &'static str {
    match event_type {
        EventType::Message => "message",
        EventType::ToolCall => "tool_call",
        EventType::ToolOutput => "tool_output",
        EventType::CommandStarted | EventType::CommandOutput | EventType::CommandFinished => {
            "command_event"
        }
        EventType::FileTouched => "file_touched",
        EventType::VcsChange => "vcs_change",
        EventType::Artifact => "artifact",
        EventType::Summary => "summary",
        EventType::Notice => "notice",
    }
}

fn event_why_matched(hit: &EventSearchHit) -> Vec<String> {
    let mut why = vec![event_reason(hit.event_type).to_owned()];
    why.extend(event_why_metadata(hit.event_type, hit.role));
    why
}

fn event_why_metadata(
    event_type: EventType,
    role: Option<ctx_history_core::EventRole>,
) -> Vec<String> {
    let mut why = vec![
        format!("event_type:{}", event_type.as_str()),
        format!("source_field:{}", event_source_field(event_type)),
    ];
    if let Some(role) = role {
        why.push(format!("role:{}", role.as_str()));
    }
    let penalty = event_relevance_penalty_for(event_type, role);
    if penalty < 1.0 {
        why.push(format!("relevance_penalty:{penalty:.2}"));
    }
    why
}

fn event_source_field(event_type: EventType) -> &'static str {
    match event_type {
        EventType::Message => "message.body",
        EventType::ToolCall => "tool.arguments",
        EventType::ToolOutput => "tool.output",
        EventType::CommandStarted => "command",
        EventType::CommandOutput => "command.output",
        EventType::CommandFinished => "command.status",
        _ => "payload",
    }
}

fn event_relevance_penalty(hit: &EventSearchHit) -> f32 {
    event_relevance_penalty_for(hit.event_type, hit.role)
}

fn event_relevance_penalty_for(
    event_type: EventType,
    role: Option<ctx_history_core::EventRole>,
) -> f32 {
    match event_type {
        EventType::Message
            if matches!(
                role,
                Some(ctx_history_core::EventRole::User | ctx_history_core::EventRole::Assistant)
            ) =>
        {
            1.0
        }
        EventType::Message => 0.85,
        EventType::ToolCall | EventType::ToolOutput => 0.55,
        EventType::CommandStarted | EventType::CommandOutput | EventType::CommandFinished => 0.45,
        _ => 0.75,
    }
}

fn normalize_search_result_ranks(results: &mut [SearchPacketResult]) {
    let max_rank = results
        .iter()
        .map(|result| result.rank)
        .fold(0.0_f32, f32::max);
    if max_rank <= 0.0 {
        return;
    }
    for result in results.iter_mut() {
        result.rank = (result.rank / max_rank).clamp(0.0, 1.0);
    }
    for result in results.iter_mut() {
        if result.result_scope == SearchResultScope::Session {
            result.session_importance =
                session_importance(result.rank, result.more_matches_in_session);
        } else {
            result.session_importance = 0.0;
        }
    }
}

fn session_importance(rank: f32, more_matches_in_session: usize) -> f32 {
    let coverage_boost = ((more_matches_in_session as f32).ln_1p() * 0.08).min(0.24);
    (rank + coverage_boost).clamp(0.0, 1.0)
}

pub fn display_snippet(input: &str, max_chars: usize) -> String {
    local_snippet(input, max_chars)
}

fn normalized_options(options: &PacketOptions) -> PacketOptions {
    PacketOptions {
        limit: options.limit.clamp(1, MAX_RESULT_LIMIT),
        snippet_chars: options.snippet_chars.clamp(32, 2_000),
        filters: options.filters.clone(),
        result_mode: options.result_mode,
        match_mode: options.match_mode,
    }
}

fn ranked_candidates(
    store: &Store,
    plan: Option<&SearchQueryPlan>,
    options: &PacketOptions,
    file_scope: Option<&FileTouchScope>,
    hydration: HydrationIntent,
) -> Result<CandidateSearch> {
    let target_candidates = options.limit.saturating_add(1);
    let terms = plan
        .map(|p| {
            p.clauses
                .iter()
                .flat_map(|c| c.terms.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::<Uuid>::new();
    let mut scan_budget_exhausted = false;
    let file_only = terms.is_empty() && file_scope.is_some();
    if terms.is_empty() && !file_only {
        return Ok(CandidateSearch {
            candidates,
            scan_budget_exhausted,
        });
    }

    if file_only {
        let Some(scope) = file_scope else {
            return Ok(CandidateSearch {
                candidates,
                scan_budget_exhausted,
            });
        };
        let mut records = Vec::new();
        for record_id in &scope.history_record_ids {
            if !seen.insert(*record_id) {
                continue;
            }
            records.push(store.get_record(*record_id)?);
        }
        candidates.extend(candidates_for_records(
            store,
            records,
            plan,
            &terms,
            &options.filters,
            file_scope,
            hydration,
        )?);
        normalize_scores(&mut candidates);
        candidates.sort_by(compare_candidates);
        if candidates.len() > target_candidates {
            candidates.truncate(target_candidates);
        }
        return Ok(CandidateSearch {
            candidates,
            scan_budget_exhausted,
        });
    }

    let filtered = has_filters(&options.filters);
    if filtered {
        let page_size = FILTERED_SEARCH_PAGE_SIZE.max(target_candidates);
        #[cfg(test)]
        let used_paged_reference = if use_paged_ranked_reference() {
            let mut offset = 0_usize;
            let mut pages_scanned = 0_usize;
            loop {
                pages_scanned = pages_scanned.saturating_add(1);
                let records = match plan {
                    Some(plan) if !plan.is_empty() => {
                        store.search_records_plan_page(plan, page_size, offset)?
                    }
                    _ => Vec::new(),
                };
                let page_len = records.len();
                let page_records = records
                    .into_iter()
                    .filter(|record| {
                        seen.insert(record.id)
                            && file_scope.map_or(true, |scope| {
                                scope.history_record_ids.is_empty()
                                    || scope.history_record_ids.contains(&record.id)
                            })
                    })
                    .collect();
                candidates.extend(candidates_for_records(
                    store,
                    page_records,
                    plan,
                    &terms,
                    &options.filters,
                    file_scope,
                    hydration,
                )?);
                if candidates.len() >= target_candidates || page_len < page_size {
                    break;
                }
                if pages_scanned >= FILTERED_SEARCH_MAX_PAGES {
                    scan_budget_exhausted = true;
                    break;
                }
                offset = offset.saturating_add(page_size);
            }
            true
        } else {
            false
        };
        #[cfg(not(test))]
        let used_paged_reference = false;
        if !used_paged_reference {
            let ranked_hits = match plan {
                Some(plan) if !plan.is_empty() => store.search_record_hits_fts_plan(
                    plan,
                    page_size.saturating_mul(FILTERED_SEARCH_MAX_PAGES),
                    0,
                )?,
                _ => Some(Vec::new()),
            };
            if let Some(ranked_hits) = ranked_hits {
                for (page_index, hits) in ranked_hits.chunks(page_size).enumerate() {
                    let records = hits
                        .iter()
                        .map(|hit| store.get_record(hit.record_id))
                        .collect::<ctx_history_store::Result<Vec<_>>>()?;
                    let page_len = records.len();
                    let mut page_records = Vec::new();
                    for record in records {
                        if seen.insert(record.id)
                            && file_scope.map_or(true, |scope| {
                                scope.history_record_ids.is_empty()
                                    || scope.history_record_ids.contains(&record.id)
                            })
                        {
                            page_records.push(record);
                        }
                    }
                    candidates.extend(candidates_for_records(
                        store,
                        page_records,
                        plan,
                        &terms,
                        &options.filters,
                        file_scope,
                        hydration,
                    )?);
                    if candidates.len() >= target_candidates || page_len < page_size {
                        break;
                    }
                    if page_index + 1 >= FILTERED_SEARCH_MAX_PAGES {
                        scan_budget_exhausted = true;
                        break;
                    }
                }
            } else {
                let mut offset = 0_usize;
                let mut pages_scanned = 0_usize;
                loop {
                    pages_scanned = pages_scanned.saturating_add(1);
                    let records = match plan {
                        Some(plan) if !plan.is_empty() => {
                            store.search_records_plan_page(plan, page_size, offset)?
                        }
                        _ => Vec::new(),
                    };
                    let page_len = records.len();

                    let mut page_records = Vec::new();
                    for record in records {
                        if !seen.insert(record.id) {
                            continue;
                        }
                        if let Some(scope) = file_scope {
                            if !scope.history_record_ids.is_empty()
                                && !scope.history_record_ids.contains(&record.id)
                            {
                                continue;
                            }
                        }
                        page_records.push(record);
                    }
                    candidates.extend(candidates_for_records(
                        store,
                        page_records,
                        plan,
                        &terms,
                        &options.filters,
                        file_scope,
                        hydration,
                    )?);

                    if candidates.len() >= target_candidates || page_len < page_size {
                        break;
                    }
                    if pages_scanned >= FILTERED_SEARCH_MAX_PAGES {
                        scan_budget_exhausted = true;
                        break;
                    }
                    let next_offset = offset.saturating_add(page_size);
                    if next_offset == offset {
                        break;
                    }
                    offset = next_offset;
                }
            }
        }
    } else {
        let fetch_limit = options
            .limit
            .saturating_mul(8)
            .max(50)
            .max(target_candidates);
        let records = match plan {
            Some(plan) if !plan.is_empty() => store.search_records_plan(plan, fetch_limit)?,
            _ => Vec::new(),
        };
        let mut page_records = Vec::new();
        for record in records {
            if !seen.insert(record.id) {
                continue;
            }
            if file_scope.is_some_and(|scope| !scope.history_record_ids.contains(&record.id)) {
                continue;
            }
            page_records.push(record);
        }
        candidates.extend(candidates_for_records(
            store,
            page_records,
            plan,
            &terms,
            &options.filters,
            file_scope,
            hydration,
        )?);
    }

    normalize_scores(&mut candidates);
    candidates.sort_by(compare_candidates);
    if candidates.len() > target_candidates {
        candidates.truncate(target_candidates);
    }
    Ok(CandidateSearch {
        candidates,
        scan_budget_exhausted,
    })
}

fn compare_candidates(left: &Candidate, right: &Candidate) -> Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| right.record.updated_at.cmp(&left.record.updated_at))
        .then_with(|| left.record.title.cmp(&right.record.title))
        .then_with(|| left.record.id.cmp(&right.record.id))
}

fn candidates_for_records(
    store: &Store,
    records: Vec<HistoryRecord>,
    plan: Option<&SearchQueryPlan>,
    terms: &[String],
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
    hydration: HydrationIntent,
) -> Result<Vec<Candidate>> {
    let ids = records.iter().map(|record| record.id).collect::<Vec<_>>();
    let mut contexts = match hydration {
        HydrationIntent::Full => hydrate_record_contexts(store, &ids, filters.file.as_deref())?
            .into_iter()
            .map(|(id, context)| (id, context.into()))
            .collect(),
        HydrationIntent::Compact => {
            hydrate_compact_record_contexts(store, &ids, filters.file.as_deref())?
        }
    };
    let mut candidates = Vec::new();
    for record in records {
        let context = contexts.remove(&record.id).unwrap_or_default();
        if let Some(candidate) =
            candidate_for_record(record, context, plan, terms, filters, file_scope)
        {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

fn candidate_for_record(
    record: HistoryRecord,
    context: RecordContext,
    plan: Option<&SearchQueryPlan>,
    terms: &[String],
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
) -> Option<Candidate> {
    if !record_matches_filters(&record, &context, filters, file_scope) {
        return None;
    }
    let analysis = analyze_record(&record, &context, plan, terms, filters);
    if terms.is_empty() || analysis.score > 0.0 {
        Some(Candidate {
            record,
            context,
            score: analysis.score,
            why_matched: analysis.why_matched,
            citations: analysis.citations,
            primary_hit: analysis.primary_hit,
        })
    } else {
        None
    }
}

fn hydrate_record_contexts(
    store: &Store,
    record_ids: &[Uuid],
    file_filter: Option<&str>,
) -> Result<BTreeMap<Uuid, FullRecordContext>> {
    let mut sessions = store.sessions_for_records(record_ids)?;
    let mut runs = store.runs_for_records(record_ids)?;
    let mut events = store.events_for_records(record_ids)?;
    let mut artifacts = store.artifacts_for_records(record_ids)?;
    let mut files_touched =
        if let Some(file) = file_filter.map(str::trim).filter(|value| !value.is_empty()) {
            store.files_touched_for_records_matching(record_ids, file)?
        } else {
            store.files_touched_for_records(record_ids)?
        };
    let mut vcs_changes = store.vcs_changes_for_records(record_ids)?;
    let mut summaries = store.summaries_for_records(record_ids)?;
    let mut source_ids = BTreeSet::new();
    for session in sessions.values().flatten() {
        if let Some(id) = session.capture_source_id {
            source_ids.insert(id);
        }
    }
    for run in runs.values().flatten() {
        if let Some(id) = run.source_id {
            source_ids.insert(id);
        }
    }
    for event in events.values().flatten() {
        if let Some(id) = event.capture_source_id {
            source_ids.insert(id);
        }
    }
    for artifact in artifacts.values().flatten() {
        if let Some(id) = artifact.source_id {
            source_ids.insert(id);
        }
    }
    for file in files_touched.values().flatten() {
        if let Some(id) = file.source_id {
            source_ids.insert(id);
        }
    }
    for change in vcs_changes.values().flatten() {
        if let Some(id) = change.source_id {
            source_ids.insert(id);
        }
    }
    for summary in summaries.values().flatten() {
        if let Some(id) = summary.source_id {
            source_ids.insert(id);
        }
    }
    let sources = store.capture_sources_for_ids(&source_ids.into_iter().collect::<Vec<_>>())?;
    Ok(record_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|id| {
            let mut context = FullRecordContext {
                sessions: sessions.remove(&id).unwrap_or_default(),
                runs: runs.remove(&id).unwrap_or_default(),
                events: events.remove(&id).unwrap_or_default(),
                artifacts: artifacts.remove(&id).unwrap_or_default(),
                files_touched: files_touched.remove(&id).unwrap_or_default(),
                vcs_changes: vcs_changes.remove(&id).unwrap_or_default(),
                summaries: summaries.remove(&id).unwrap_or_default(),
                sources: BTreeMap::new(),
            };
            context.sources = sources
                .iter()
                .filter(|(source_id, _)| full_context_references_source(&context, **source_id))
                .map(|(id, source)| (*id, source.clone()))
                .collect();
            (id, context)
        })
        .collect())
}

fn full_context_references_source(context: &FullRecordContext, id: Uuid) -> bool {
    context
        .sessions
        .iter()
        .any(|item| item.capture_source_id == Some(id))
        || context.runs.iter().any(|item| item.source_id == Some(id))
        || context
            .events
            .iter()
            .any(|item| item.capture_source_id == Some(id))
        || context
            .artifacts
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .files_touched
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .vcs_changes
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .summaries
            .iter()
            .any(|item| item.source_id == Some(id))
}

fn hydrate_compact_record_contexts(
    store: &Store,
    record_ids: &[Uuid],
    file_filter: Option<&str>,
) -> Result<BTreeMap<Uuid, RecordContext>> {
    let mut sessions = store.search_sessions_for_records(record_ids)?;
    let mut runs = store.search_runs_for_records(record_ids)?;
    let mut events = store.search_events_for_records(record_ids)?;
    let mut artifacts = store.search_artifacts_for_records(record_ids)?;
    let mut files_touched =
        if let Some(file) = file_filter.map(str::trim).filter(|value| !value.is_empty()) {
            store.search_files_touched_for_records_matching(record_ids, file)?
        } else {
            store.search_files_touched_for_records(record_ids)?
        };
    let mut vcs_changes = store.search_vcs_changes_for_records(record_ids)?;
    let mut summaries = store.search_summaries_for_records(record_ids)?;
    let mut source_ids = BTreeSet::new();
    source_ids.extend(
        sessions
            .values()
            .flatten()
            .filter_map(|item| item.capture_source_id),
    );
    source_ids.extend(runs.values().flatten().filter_map(|item| item.source_id));
    source_ids.extend(
        events
            .values()
            .flatten()
            .filter_map(|item| item.capture_source_id),
    );
    source_ids.extend(
        artifacts
            .values()
            .flatten()
            .filter_map(|item| item.source_id),
    );
    source_ids.extend(
        files_touched
            .values()
            .flatten()
            .filter_map(|item| item.source_id),
    );
    source_ids.extend(
        vcs_changes
            .values()
            .flatten()
            .filter_map(|item| item.source_id),
    );
    source_ids.extend(
        summaries
            .values()
            .flatten()
            .filter_map(|item| item.source_id),
    );
    let sources =
        store.search_capture_sources_for_ids(&source_ids.into_iter().collect::<Vec<_>>())?;
    Ok(record_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|id| {
            let mut context = RecordContext {
                sessions: sessions.remove(&id).unwrap_or_default(),
                runs: runs.remove(&id).unwrap_or_default(),
                events: events.remove(&id).unwrap_or_default(),
                artifacts: artifacts.remove(&id).unwrap_or_default(),
                files_touched: files_touched.remove(&id).unwrap_or_default(),
                vcs_changes: vcs_changes.remove(&id).unwrap_or_default(),
                summaries: summaries.remove(&id).unwrap_or_default(),
                sources: BTreeMap::new(),
            };
            context.sources = sources
                .iter()
                .filter(|(source_id, _)| context_references_source(&context, **source_id))
                .map(|(source_id, source)| (*source_id, source.clone()))
                .collect();
            (id, context)
        })
        .collect())
}

fn context_references_source(context: &RecordContext, id: Uuid) -> bool {
    context
        .sessions
        .iter()
        .any(|item| item.capture_source_id == Some(id))
        || context.runs.iter().any(|item| item.source_id == Some(id))
        || context
            .events
            .iter()
            .any(|item| item.capture_source_id == Some(id))
        || context
            .artifacts
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .files_touched
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .vcs_changes
            .iter()
            .any(|item| item.source_id == Some(id))
        || context
            .summaries
            .iter()
            .any(|item| item.source_id == Some(id))
}

impl From<FullRecordContext> for RecordContext {
    fn from(context: FullRecordContext) -> Self {
        Self {
            sessions: context
                .sessions
                .into_iter()
                .map(search_session_from_full)
                .collect(),
            runs: context.runs.into_iter().map(search_run_from_full).collect(),
            events: context
                .events
                .into_iter()
                .map(search_event_from_full)
                .collect(),
            artifacts: context
                .artifacts
                .into_iter()
                .map(search_artifact_from_full)
                .collect(),
            files_touched: context
                .files_touched
                .into_iter()
                .map(search_file_from_full)
                .collect(),
            vcs_changes: context
                .vcs_changes
                .into_iter()
                .map(search_vcs_from_full)
                .collect(),
            summaries: context
                .summaries
                .into_iter()
                .map(search_summary_from_full)
                .collect(),
            sources: context
                .sources
                .into_iter()
                .map(|(id, source)| (id, search_source_from_full(source)))
                .collect(),
        }
    }
}

fn search_session_from_full(value: Session) -> SearchSessionRow {
    SearchSessionRow {
        id: value.id,
        parent_session_id: value.parent_session_id,
        root_session_id: value.root_session_id,
        capture_source_id: value.capture_source_id,
        provider: value.provider,
        external_session_id: value.external_session_id,
        external_agent_id: value.external_agent_id,
        agent_type: value.agent_type,
        role_hint: value.role_hint,
        is_primary: value.is_primary,
        status: value.status,
        started_at: value.started_at,
        ended_at: value.ended_at,
        metadata: value.sync.metadata,
    }
}

fn search_run_from_full(value: Run) -> SearchRunRow {
    SearchRunRow {
        id: value.id,
        session_id: value.session_id,
        run_type: value.run_type,
        status: value.status,
        started_at: value.started_at,
        exit_code: value.exit_code,
        cwd: value.cwd,
        command_preview: value.command_preview,
        source_id: value.source_id,
    }
}

fn search_event_from_full(value: Event) -> SearchEventRow {
    SearchEventRow {
        id: value.id,
        seq: value.seq,
        session_id: value.session_id,
        run_id: value.run_id,
        event_type: value.event_type,
        role: value.role,
        occurred_at: value.occurred_at,
        capture_source_id: value.capture_source_id,
        payload: value.payload,
        dedupe_key: value.dedupe_key,
        redaction_state: value.redaction_state,
        metadata: value.sync.metadata,
    }
}

fn search_artifact_from_full(value: Artifact) -> SearchArtifactRow {
    SearchArtifactRow {
        id: value.id,
        kind: value.kind,
        blob_path: value.blob_path,
        media_type: value.media_type,
        preview_text: value.preview_text,
        updated_at: value.timestamps.updated_at,
        source_id: value.source_id,
    }
}

fn search_file_from_full(value: FileTouched) -> SearchFileTouchedRow {
    SearchFileTouchedRow {
        id: value.id,
        event_id: value.event_id,
        path: value.path,
        change_kind: value.change_kind,
        old_path: value.old_path,
        updated_at: value.timestamps.updated_at,
        source_id: value.source_id,
    }
}

fn search_vcs_from_full(value: VcsChange) -> SearchVcsChangeRow {
    SearchVcsChangeRow {
        id: value.id,
        kind: value.kind,
        change_id: value.change_id,
        parent_change_ids: value.parent_change_ids,
        branch_or_bookmark: value.branch_or_bookmark,
        tree_hash: value.tree_hash,
        author_time: value.author_time,
        updated_at: value.timestamps.updated_at,
        source_id: value.source_id,
    }
}

fn search_summary_from_full(value: Summary) -> SearchSummaryRow {
    SearchSummaryRow {
        id: value.id,
        text: value.text,
        updated_at: value.timestamps.updated_at,
        source_id: value.source_id,
    }
}

fn search_source_from_full(value: ctx_history_core::CaptureSource) -> SearchCaptureSourceRow {
    SearchCaptureSourceRow {
        id: value.id,
        provider: value.descriptor.provider,
        cwd: value.descriptor.cwd,
        raw_source_path: value.descriptor.raw_source_path,
        external_session_id: value.descriptor.external_session_id,
        metadata: value.sync.metadata,
    }
}

struct MatchAnalysis {
    score: f32,
    why_matched: Vec<String>,
    citations: Vec<ContextCitation>,
    primary_hit: Option<HitMetadata>,
}

fn analyze_record(
    record: &HistoryRecord,
    context: &RecordContext,
    plan: Option<&SearchQueryPlan>,
    terms: &[String],
    filters: &SearchFilters,
) -> MatchAnalysis {
    let mut score = 0.0_f32;
    let mut why = Vec::new();
    let mut citations = Vec::new();

    if terms.is_empty() {
        if filters
            .file
            .as_ref()
            .is_some_and(|file| !file.trim().is_empty())
        {
            let mut primary_hit = None;
            for section in search_sections(record, context, filters)
                .into_iter()
                .filter(|section| section.reason == "file_touched")
            {
                if primary_hit.is_none() {
                    primary_hit = Some(section.hit.clone());
                }
                score += section.weight;
                add_match(
                    &mut why,
                    &mut citations,
                    section.reason,
                    section.citation,
                    &section.hit,
                );
            }
            if !why.is_empty() {
                return MatchAnalysis {
                    score,
                    why_matched: why,
                    citations,
                    primary_hit,
                };
            }
        }
        add_match(
            &mut why,
            &mut citations,
            "recent_activity",
            ContextCitation {
                citation_type: ContextCitationType::HistoryRecord,
                id: record.id,
                label: "recent session".to_owned(),
                time: record.updated_at,
                provider: None,
                session_id: None,
                event_seq: None,
                raw_source_path: None,
                raw_source_exists: None,
                cursor: None,
            },
            &empty_hit(record.updated_at),
        );
        return MatchAnalysis {
            score: 1.0,
            why_matched: why,
            citations,
            primary_hit: None,
        };
    }

    let mut primary_hit = None;
    let mut primary_weight = f32::MIN;
    for section in search_sections(record, context, filters) {
        if hit_matches_excluded_provider_session(&section.hit, filters) {
            continue;
        }
        let matched = plan.map_or_else(
            || matches_terms(&section.text, terms),
            |p| p.matches_text(&section.text),
        );
        if matched {
            score += section.weight
                * plan.map_or(1.0, |p| match p.mode {
                    SearchMatchMode::Any => matched_token_count(&section.text, p) as f32,
                    _ => 1.0,
                });
            if section.weight > primary_weight {
                primary_weight = section.weight;
                primary_hit = Some(section.hit.clone());
            }
            add_match(
                &mut why,
                &mut citations,
                section.reason,
                section.citation,
                &section.hit,
            );
            for reason in section.why_metadata {
                push_unique_why(&mut why, reason);
            }
        }
    }

    MatchAnalysis {
        score,
        why_matched: why,
        citations,
        primary_hit,
    }
}

fn add_match(
    why: &mut Vec<String>,
    citations: &mut Vec<ContextCitation>,
    reason: &str,
    mut citation: ContextCitation,
    hit: &HitMetadata,
) {
    if !why.iter().any(|value| value == reason) {
        why.push(reason.to_owned());
    }
    citation.provider = hit.provider;
    citation.session_id = hit.session_id;
    citation.event_seq = hit.event_seq;
    citation.raw_source_path = hit.raw_source_path.clone();
    citation.raw_source_exists = hit.raw_source_exists;
    citation.cursor = hit.cursor.clone().or_else(|| {
        hit.provider_session_id
            .as_ref()
            .map(|session_id| format!("session:{session_id}"))
    });
    if !citations.iter().any(|existing| {
        existing.citation_type == citation.citation_type && existing.id == citation.id
    }) {
        citations.push(citation);
    }
}

fn search_sections(
    record: &HistoryRecord,
    context: &RecordContext,
    filters: &SearchFilters,
) -> Vec<SearchSection> {
    let mut sections = Vec::new();
    let event_evidence_only = filters.event_type.is_some() || !filters.roles.is_empty();
    let record_hit = record_context_display_hit(context, filters, record.updated_at);
    let include_record_bookkeeping_text =
        !event_evidence_only && !is_agent_history_bookkeeping_record(record);
    if include_record_bookkeeping_text {
        sections.push(SearchSection {
            reason: "title",
            why_metadata: Vec::new(),
            weight: 8.0,
            text: record.title.clone(),
            citation: citation(
                ContextCitationType::HistoryRecord,
                record.id,
                "session title",
                record.updated_at,
            ),
            hit: record_hit.clone(),
        });
    }
    let include_record_text = include_record_bookkeeping_text
        && record_text_matches_agent_scope(context, filters)
        && !context_has_excluded_provider_session(context, filters);
    if include_record_text {
        sections.push(SearchSection {
            reason: "primary_user_message",
            why_metadata: Vec::new(),
            weight: 5.0,
            text: record.body.clone(),
            citation: citation(
                ContextCitationType::HistoryRecord,
                record.id,
                "session text",
                record.updated_at,
            ),
            hit: record_hit.clone(),
        });
    }
    if include_record_text {
        for tag in &record.tags {
            sections.push(SearchSection {
                reason: "tag",
                why_metadata: Vec::new(),
                weight: 3.0,
                text: tag.clone(),
                citation: citation(
                    ContextCitationType::HistoryRecord,
                    record.id,
                    "session tag",
                    record.updated_at,
                ),
                hit: record_hit.clone(),
            });
        }
    }
    for session in &context.sessions {
        if event_evidence_only {
            break;
        }
        if !session_matches_agent_scope(session, filters)
            || !source_id_matches_history_source_filter(session.capture_source_id, context, filters)
        {
            continue;
        }
        let hit = session_hit(session, context);
        sections.push(SearchSection {
            reason: "session_metadata",
            why_metadata: Vec::new(),
            weight: 2.5,
            text: joined([
                session.provider.as_str(),
                session.agent_type.as_str(),
                session.status.as_str(),
                session.external_session_id.as_deref().unwrap_or_default(),
                session.external_agent_id.as_deref().unwrap_or_default(),
                session.role_hint.as_deref().unwrap_or_default(),
            ]),
            citation: citation(
                ContextCitationType::Session,
                session.id,
                "session",
                session.started_at,
            ),
            hit,
        });
    }

    for run in &context.runs {
        if event_evidence_only || run_is_excluded_tool_noise(run, filters) {
            continue;
        }
        if !item_matches_agent_scope(run.session_id, run.source_id, context, filters) {
            continue;
        }
        let hit = run_hit(run, context);
        sections.push(SearchSection {
            reason: "run_command",
            why_metadata: Vec::new(),
            weight: if run.exit_code.unwrap_or(0) == 0 {
                3.0
            } else {
                4.0
            },
            text: joined([
                run.run_type.as_str(),
                run.status.as_str(),
                run.cwd.as_deref().unwrap_or_default(),
                run.command_preview.as_deref().unwrap_or_default(),
            ]),
            citation: citation(
                ContextCitationType::Run,
                run.id,
                "run command",
                run.started_at,
            ),
            hit,
        });
    }

    for event in &context.events {
        if !item_matches_agent_scope(event.session_id, event.capture_source_id, context, filters) {
            continue;
        }
        if filters
            .event_type
            .is_some_and(|event_type| event.event_type != event_type)
        {
            continue;
        }
        if !role_matches(event.role, filters) || event_is_excluded_tool_noise(event, filters) {
            continue;
        }
        let event_text = event_text(event);
        let hit = event_hit(event, context);
        sections.push(SearchSection {
            reason: match event.event_type {
                ctx_history_core::EventType::Message => "message",
                ctx_history_core::EventType::ToolCall => "tool_call",
                ctx_history_core::EventType::ToolOutput => "tool_output",
                ctx_history_core::EventType::CommandStarted
                | ctx_history_core::EventType::CommandOutput
                | ctx_history_core::EventType::CommandFinished => "command_event",
                _ => "event",
            },
            why_metadata: event_why_metadata(event.event_type, event.role),
            weight: event_weight(event),
            text: event_text,
            citation: citation(
                ContextCitationType::Event,
                event.id,
                "event",
                event.occurred_at,
            ),
            hit,
        });
    }

    for artifact in &context.artifacts {
        if event_evidence_only {
            break;
        }
        if !item_matches_agent_scope(None, artifact.source_id, context, filters) {
            continue;
        }
        let hit = artifact_hit(artifact, context);
        sections.push(SearchSection {
            reason: "artifact",
            why_metadata: Vec::new(),
            weight: 2.5,
            text: joined([
                artifact.kind.as_str(),
                artifact.media_type.as_deref().unwrap_or_default(),
                artifact.preview_text.as_deref().unwrap_or_default(),
                artifact.blob_path.as_str(),
            ]),
            citation: citation(
                ContextCitationType::Artifact,
                artifact.id,
                "artifact",
                artifact.updated_at,
            ),
            hit,
        });
    }

    for file in &context.files_touched {
        if event_evidence_only {
            break;
        }
        let session_id = file.event_id.and_then(|id| {
            context
                .events
                .iter()
                .find(|event| event.id == id)
                .and_then(|event| event.session_id)
        });
        if !item_matches_agent_scope(session_id, file.source_id, context, filters) {
            continue;
        }
        let hit = file_hit(file, context);
        sections.push(SearchSection {
            reason: "file_touched",
            why_metadata: Vec::new(),
            weight: 3.0,
            text: file_touched_search_text(file),
            citation: citation(
                ContextCitationType::File,
                file.id,
                "file touched",
                file.updated_at,
            ),
            hit,
        });
    }

    for change in &context.vcs_changes {
        if event_evidence_only {
            break;
        }
        if !item_matches_agent_scope(None, change.source_id, context, filters) {
            continue;
        }
        let parent_change_ids = change.parent_change_ids.join(" ");
        let hit = source_hit(
            change.source_id,
            change.author_time.unwrap_or(change.updated_at),
            context,
        );
        sections.push(SearchSection {
            reason: "vcs_change",
            why_metadata: Vec::new(),
            weight: 3.0,
            text: joined([
                change.kind.as_str(),
                change.change_id.as_str(),
                change.branch_or_bookmark.as_deref().unwrap_or_default(),
                change.tree_hash.as_deref().unwrap_or_default(),
                parent_change_ids.as_str(),
            ]),
            citation: citation(
                ContextCitationType::VcsChange,
                change.id,
                "vcs change",
                change.author_time.unwrap_or(change.updated_at),
            ),
            hit,
        });
    }

    for summary in &context.summaries {
        if event_evidence_only {
            break;
        }
        if !item_matches_agent_scope(None, summary.source_id, context, filters) {
            continue;
        }
        let hit = source_hit(summary.source_id, summary.updated_at, context);
        sections.push(SearchSection {
            reason: "summary",
            why_metadata: Vec::new(),
            weight: 4.0,
            text: summary.text.clone(),
            citation: citation(
                ContextCitationType::Summary,
                summary.id,
                "summary",
                summary.updated_at,
            ),
            hit,
        });
    }

    sections
}

fn is_agent_history_bookkeeping_record(record: &HistoryRecord) -> bool {
    record.kind == "agent_history"
        || record.tags.iter().any(|tag| tag == "agent-history")
        || record
            .body
            .trim_start()
            .starts_with("Indexed local agent history from ")
        || record
            .body
            .trim_start()
            .starts_with("Indexed custom agent history from ")
}

fn session_matches_agent_scope(session: &SearchSessionRow, filters: &SearchFilters) -> bool {
    if filters.session == Some(session.id) {
        return true;
    }
    if filters.include_subagents && !filters.primary_only {
        return true;
    }
    session_is_primary(session)
        || (!filters.primary_only
            && session.agent_type == ctx_history_core::AgentType::Unknown
            && session.parent_session_id.is_none())
}

fn session_is_primary(session: &SearchSessionRow) -> bool {
    session.is_primary || session.agent_type == ctx_history_core::AgentType::Primary
}

fn event_hit_matches_agent_scope(hit: &EventSearchHit, filters: &SearchFilters) -> bool {
    if filters.session.is_some() && filters.session == hit.session_id {
        return true;
    }
    if filters.include_subagents && !filters.primary_only {
        return true;
    }
    if hit.session_is_primary == Some(true)
        || hit.agent_type == Some(ctx_history_core::AgentType::Primary)
    {
        return true;
    }
    if filters.primary_only {
        return false;
    }
    hit.session_is_primary.is_none() && hit.agent_type.is_none()
}

fn record_text_matches_agent_scope(context: &RecordContext, filters: &SearchFilters) -> bool {
    if has_history_source_filter(filters) {
        return false;
    }
    context
        .sessions
        .iter()
        .all(|session| session_matches_agent_scope(session, filters))
}

fn item_matches_agent_scope(
    session_id: Option<Uuid>,
    source_id: Option<Uuid>,
    context: &RecordContext,
    filters: &SearchFilters,
) -> bool {
    let item_source_id = source_id.or_else(|| {
        session_id
            .and_then(|id| context.sessions.iter().find(|session| session.id == id))
            .and_then(|session| session.capture_source_id)
    });
    if !source_id_matches_history_source_filter(item_source_id, context, filters) {
        return false;
    }
    associated_session(session_id, source_id, context)
        .map(|session| session_matches_agent_scope(session, filters))
        .unwrap_or(true)
}

fn source_id_matches_history_source_filter(
    source_id: Option<Uuid>,
    context: &RecordContext,
    filters: &SearchFilters,
) -> bool {
    if !has_history_source_filter(filters) {
        return true;
    }
    source_id
        .and_then(|id| context.sources.get(&id))
        .is_some_and(|source| source_matches_history_source_filter(source, filters))
}

fn associated_session(
    session_id: Option<Uuid>,
    source_id: Option<Uuid>,
    context: &RecordContext,
) -> Option<&SearchSessionRow> {
    session_id
        .and_then(|id| context.sessions.iter().find(|session| session.id == id))
        .or_else(|| source_id.and_then(|id| associated_session_for_source(id, context)))
}

fn associated_session_for_source(
    source_id: Uuid,
    context: &RecordContext,
) -> Option<&SearchSessionRow> {
    context
        .sessions
        .iter()
        .find(|session| session.capture_source_id == Some(source_id))
        .or_else(|| {
            let source = context.sources.get(&source_id)?;
            context.sessions.iter().find(|session| {
                session.provider == source.provider
                    && session.external_session_id == source.external_session_id
            })
        })
}

fn record_context_display_hit(
    context: &RecordContext,
    filters: &SearchFilters,
    time: chrono::DateTime<Utc>,
) -> HitMetadata {
    context
        .sessions
        .iter()
        .find(|session| {
            session_matches_agent_scope(session, filters)
                && filters
                    .provider
                    .map_or(true, |provider| session.provider == provider)
                && filters.session.map_or(true, |id| session.id == id)
                && hit_matches_history_source_filter(&session_hit(session, context), filters)
        })
        .or_else(|| {
            context
                .sessions
                .iter()
                .find(|session| session_matches_agent_scope(session, filters))
        })
        .map(|session| session_hit(session, context))
        .unwrap_or_else(|| empty_hit(time))
}

fn file_touched_search_text(file: &SearchFileTouchedRow) -> String {
    let path = file.path.as_str();
    let old_path = file.old_path.as_deref().unwrap_or_default();
    joined([
        path,
        old_path,
        file.change_kind
            .map(|kind| kind.as_str())
            .unwrap_or_default(),
    ])
}

fn citation(
    citation_type: ContextCitationType,
    id: Uuid,
    label: &str,
    time: chrono::DateTime<Utc>,
) -> ContextCitation {
    ContextCitation {
        citation_type,
        id,
        label: label.to_owned(),
        time,
        provider: None,
        session_id: None,
        event_seq: None,
        raw_source_path: None,
        raw_source_exists: None,
        cursor: None,
    }
}

fn empty_hit(time: chrono::DateTime<Utc>) -> HitMetadata {
    HitMetadata {
        time,
        provider: None,
        provider_session_id: None,
        history_source: None,
        history_source_plugin: None,
        provider_key: None,
        source_id: None,
        source_format: None,
        session_id: None,
        parent_session_id: None,
        root_session_id: None,
        event_id: None,
        event_seq: None,
        cwd: None,
        raw_source_path: None,
        raw_source_exists: None,
        cursor: None,
    }
}

fn session_hit(session: &SearchSessionRow, context: &RecordContext) -> HitMetadata {
    let mut hit = source_hit(session.capture_source_id, session.started_at, context);
    hit.provider = Some(session.provider);
    hit.provider_session_id = session.external_session_id.clone();
    hit.session_id = Some(session.id);
    hit.parent_session_id = session.parent_session_id;
    hit.root_session_id = session.root_session_id;
    if hit.cwd.is_none() {
        hit.cwd =
            source_for_id(session.capture_source_id, context).and_then(|source| source.cwd.clone());
    }
    hit
}

fn run_hit(run: &SearchRunRow, context: &RecordContext) -> HitMetadata {
    let mut hit = source_hit(run.source_id, run.started_at, context);
    hit.session_id = run.session_id;
    if let Some(session) = run
        .session_id
        .and_then(|id| context.sessions.iter().find(|session| session.id == id))
    {
        if hit.provider.is_none() {
            hit.provider = Some(session.provider);
        }
        if hit.provider_session_id.is_none() {
            hit.provider_session_id = session.external_session_id.clone();
        }
        hit.parent_session_id = session.parent_session_id;
        hit.root_session_id = session.root_session_id;
    }
    if hit.cwd.is_none() {
        hit.cwd = run.cwd.clone();
    }
    hit
}

fn event_hit(event: &SearchEventRow, context: &RecordContext) -> HitMetadata {
    let mut hit = source_hit(event.capture_source_id, event.occurred_at, context);
    hit.session_id = event.session_id;
    hit.event_id = Some(event.id);
    hit.event_seq = Some(event.seq);
    hit.cursor = event_cursor(event).or(hit.cursor);
    if hit.provider.is_none() {
        if let Some(session) = event
            .session_id
            .and_then(|id| context.sessions.iter().find(|session| session.id == id))
        {
            hit.provider = Some(session.provider);
            if hit.provider_session_id.is_none() {
                hit.provider_session_id = session.external_session_id.clone();
            }
            hit.parent_session_id = session.parent_session_id;
            hit.root_session_id = session.root_session_id;
        }
    }
    hit
}

fn artifact_hit(artifact: &SearchArtifactRow, context: &RecordContext) -> HitMetadata {
    source_hit(artifact.source_id, artifact.updated_at, context)
}

fn file_hit(file: &SearchFileTouchedRow, context: &RecordContext) -> HitMetadata {
    let mut hit = source_hit(file.source_id, file.updated_at, context);
    hit.event_id = file.event_id;
    hit.session_id = file.event_id.and_then(|id| {
        context
            .events
            .iter()
            .find(|event| event.id == id)
            .and_then(|event| event.session_id)
    });
    if let Some(session) = hit
        .session_id
        .and_then(|id| context.sessions.iter().find(|session| session.id == id))
    {
        hit.provider = Some(session.provider);
        hit.provider_session_id = session.external_session_id.clone();
        hit.parent_session_id = session.parent_session_id;
        hit.root_session_id = session.root_session_id;
    }
    hit
}

fn source_hit(
    source_id: Option<Uuid>,
    time: chrono::DateTime<Utc>,
    context: &RecordContext,
) -> HitMetadata {
    let Some(source) = source_for_id(source_id, context) else {
        return empty_hit(time);
    };
    let raw_source_path = source.raw_source_path.clone();
    let identity = source_history_identity(source);
    let mut hit = HitMetadata {
        time,
        provider: Some(source.provider),
        provider_session_id: source.external_session_id.clone(),
        history_source: identity.history_source,
        history_source_plugin: identity.history_source_plugin,
        provider_key: identity.provider_key,
        source_id: identity.source_id,
        source_format: identity.source_format,
        session_id: None,
        parent_session_id: None,
        root_session_id: None,
        event_id: None,
        event_seq: None,
        cwd: source.cwd.clone(),
        raw_source_exists: raw_source_path
            .as_deref()
            .map(|path| Path::new(path).exists()),
        raw_source_path,
        cursor: source_cursor(source),
    };
    if let Some(session) = associated_session_for_source(source.id, context) {
        hit.provider = Some(session.provider);
        hit.provider_session_id = session.external_session_id.clone();
        hit.session_id = Some(session.id);
        hit.parent_session_id = session.parent_session_id;
        hit.root_session_id = session.root_session_id;
    }
    hit
}

fn source_for_id(
    source_id: Option<Uuid>,
    context: &RecordContext,
) -> Option<&SearchCaptureSourceRow> {
    source_id.and_then(|id| context.sources.get(&id))
}

fn source_cursor(source: &SearchCaptureSourceRow) -> Option<String> {
    source
        .metadata
        .get("cursor")
        .and_then(|cursor| cursor.get("after"))
        .and_then(|after| after.get("cursor"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SourceHistoryIdentity {
    history_source: Option<String>,
    history_source_plugin: Option<String>,
    provider_key: Option<String>,
    source_id: Option<String>,
    source_format: Option<String>,
}

fn source_history_identity(source: &SearchCaptureSourceRow) -> SourceHistoryIdentity {
    let metadata = &source.metadata;
    let source_metadata = metadata
        .get("source_metadata")
        .and_then(serde_json::Value::as_object);
    let plugin = source_metadata
        .and_then(|metadata| metadata.get("ctx_history_plugin"))
        .or_else(|| metadata.get("ctx_history_plugin"))
        .and_then(serde_json::Value::as_object);
    let custom = source_metadata
        .and_then(|metadata| metadata.get("ctx_history_jsonl_v1"))
        .or_else(|| metadata.get("ctx_history_jsonl_v1"))
        .and_then(serde_json::Value::as_object);
    let plugin_name = plugin
        .and_then(|plugin| plugin.get("plugin_name"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let plugin_source_id = plugin
        .and_then(|plugin| plugin.get("plugin_source_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let history_source = plugin
        .and_then(|plugin| plugin.get("history_source"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            plugin_name
                .as_deref()
                .zip(plugin_source_id.as_deref())
                .map(|(plugin_name, source_id)| format!("{plugin_name}/{source_id}"))
        });
    let provider_key = custom
        .and_then(|custom| custom.get("provider_key"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let source_id = custom
        .and_then(|custom| custom.get("source_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let source_format = custom
        .and_then(|custom| custom.get("source_format"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            source_metadata
                .and_then(|metadata| metadata.get("source_format"))
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            metadata
                .get("source_format")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_owned);
    SourceHistoryIdentity {
        history_source,
        history_source_plugin: plugin_name,
        provider_key,
        source_id,
        source_format,
    }
}

fn has_history_source_filter(filters: &SearchFilters) -> bool {
    filters
        .history_source
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || filters
            .provider_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || filters
            .source_id
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        || filters
            .source_format
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn source_matches_history_source_filter(
    source: &SearchCaptureSourceRow,
    filters: &SearchFilters,
) -> bool {
    let identity = source_history_identity(source);
    source_identity_matches_history_source_filter(&identity, filters)
}

fn hit_matches_history_source_filter(hit: &HitMetadata, filters: &SearchFilters) -> bool {
    if !has_history_source_filter(filters) {
        return true;
    }
    source_identity_matches_history_source_filter(
        &SourceHistoryIdentity {
            history_source: hit.history_source.clone(),
            history_source_plugin: hit.history_source_plugin.clone(),
            provider_key: hit.provider_key.clone(),
            source_id: hit.source_id.clone(),
            source_format: hit.source_format.clone(),
        },
        filters,
    )
}

fn source_identity_matches_history_source_filter(
    identity: &SourceHistoryIdentity,
    filters: &SearchFilters,
) -> bool {
    if let Some(selector) = filters
        .history_source
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let plugin_match = identity.history_source.as_deref() == Some(selector);
        let provider_source_match = identity
            .provider_key
            .as_deref()
            .zip(identity.source_id.as_deref())
            .is_some_and(|(provider_key, source_id)| {
                selector == format!("{provider_key}/{source_id}")
            });
        if !plugin_match && !provider_source_match {
            return false;
        }
    }
    if let Some(provider_key) = filters
        .provider_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if identity.provider_key.as_deref() != Some(provider_key) {
            return false;
        }
    }
    if let Some(source_id) = filters
        .source_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if identity.source_id.as_deref() != Some(source_id) {
            return false;
        }
    }
    if let Some(source_format) = filters
        .source_format
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if identity.source_format.as_deref() != Some(source_format) {
            return false;
        }
    }
    true
}

fn event_cursor(event: &SearchEventRow) -> Option<String> {
    event
        .payload
        .get("cursor")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .or_else(|| {
            event
                .metadata
                .get("cursor")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
}

fn joined<const N: usize>(parts: [&str; N]) -> String {
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn event_weight(event: &SearchEventRow) -> f32 {
    let base = match event.event_type {
        ctx_history_core::EventType::Message => 4.0,
        ctx_history_core::EventType::ToolCall | ctx_history_core::EventType::ToolOutput => 3.5,
        ctx_history_core::EventType::CommandStarted
        | ctx_history_core::EventType::CommandOutput
        | ctx_history_core::EventType::CommandFinished => 3.0,
        _ => 2.0,
    };
    base * event_relevance_penalty_for(event.event_type, event.role)
}

fn event_is_excluded_tool_noise(event: &SearchEventRow, filters: &SearchFilters) -> bool {
    if filters.exclude_tool_noise
        && matches!(
            event.event_type,
            EventType::ToolCall
                | EventType::ToolOutput
                | EventType::CommandStarted
                | EventType::CommandOutput
                | EventType::CommandFinished
        )
    {
        return true;
    }
    is_tool_or_command_event(event.event_type)
        && excluded_tool_name_matches(filters, |needle| {
            event_tool_names(event).iter().any(|name| name == needle)
        })
}

fn run_is_excluded_tool_noise(run: &SearchRunRow, filters: &SearchFilters) -> bool {
    if filters.exclude_tool_noise {
        return true;
    }
    excluded_tool_name_matches(filters, |needle| {
        run.command_preview
            .as_deref()
            .and_then(executable_name)
            .is_some_and(|name| name == needle)
    })
}

fn is_tool_or_command_event(event_type: EventType) -> bool {
    matches!(
        event_type,
        EventType::ToolCall
            | EventType::ToolOutput
            | EventType::CommandStarted
            | EventType::CommandOutput
            | EventType::CommandFinished
    )
}

fn event_tool_names(event: &SearchEventRow) -> Vec<String> {
    let mut names = Vec::new();
    collect_tool_names(&event.payload, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect_tool_names(value: &serde_json::Value, names: &mut Vec<String>) {
    let Some(object) = value.as_object() else {
        return;
    };
    for key in ["tool", "name", "executable", "command"] {
        if let Some(text) = object.get(key).and_then(|value| value.as_str()) {
            if let Some(name) = executable_name(text) {
                names.push(name);
            }
        }
    }
    if let Some(body) = object.get("body") {
        collect_tool_names(body, names);
    }
}

fn executable_name(text: &str) -> Option<String> {
    let first = text
        .split_whitespace()
        .next()?
        .trim_matches(|c: char| c == '"' || c == '\'' || c == '`' || c == '[' || c == ']');
    let name = Path::new(first).file_name()?.to_str()?.to_ascii_lowercase();
    (!name.is_empty()).then_some(name)
}

fn normalized_tool_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn event_text(event: &SearchEventRow) -> String {
    let payload_text = search_event_preview_text(event);
    let dedupe_key = event.dedupe_key.as_deref().unwrap_or_default();
    joined([
        event.event_type.as_str(),
        event.role.map(|role| role.as_str()).unwrap_or_default(),
        payload_text.as_str(),
        dedupe_key,
    ])
}

fn search_event_preview_text(event: &SearchEventRow) -> String {
    if matches!(
        event.redaction_state,
        RedactionState::Raw | RedactionState::Withheld
    ) {
        return "raw event payload withheld".to_owned();
    }
    if let Some(preview) = event_payload_preview(&event.payload) {
        return local_snippet(&preview, 900);
    }
    if event.payload.is_object() || event.payload.is_array() {
        return local_snippet(&event.payload.to_string(), 900);
    }
    String::new()
}

pub fn event_preview_text(event: &Event) -> String {
    if matches!(
        event.redaction_state,
        RedactionState::Raw | RedactionState::Withheld
    ) {
        return "raw event payload withheld".to_owned();
    }
    if let Some(preview) = event_payload_preview(&event.payload) {
        return local_snippet(&preview, 900);
    }
    if event.payload.is_object() || event.payload.is_array() {
        return local_snippet(&event.payload.to_string(), 900);
    }
    String::new()
}

fn event_payload_preview(payload: &serde_json::Value) -> Option<String> {
    if let Some(body) = payload.get("body") {
        if let Some(preview) = event_value_preview(body) {
            return Some(preview);
        }
    }
    event_value_preview(payload)
}

fn event_value_preview(value: &serde_json::Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return non_blank(value);
    }
    let object = value.as_object()?;
    for key in [
        "text",
        "preview",
        "summary",
        "command",
        "output_preview",
        "output",
        "message",
    ] {
        if let Some(value) = object.get(key).and_then(preview_fragment) {
            return Some(value);
        }
    }
    let structured = ["tool", "name", "arguments_preview", "status"]
        .into_iter()
        .filter_map(|key| {
            object
                .get(key)
                .and_then(preview_fragment)
                .map(|value| format!("{key}: {value}"))
        })
        .collect::<Vec<_>>();
    if structured.is_empty() {
        None
    } else {
        Some(structured.join(" | "))
    }
}

fn preview_fragment(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => non_blank(value),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn non_blank(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn normalize_scores(candidates: &mut [Candidate]) {
    let max_score = candidates
        .iter()
        .map(|candidate| candidate.score)
        .fold(0.0_f32, f32::max);
    if max_score <= 0.0 {
        return;
    }
    for candidate in candidates {
        candidate.score = (candidate.score / max_score).clamp(0.0, 1.0);
    }
}

fn query_terms(query: &str) -> Vec<String> {
    search_query_terms(query)
}

fn matched_token_count(value: &str, plan: &SearchQueryPlan) -> usize {
    let hay = search_query_terms(value);
    plan.clauses
        .iter()
        .map(|clause| {
            clause
                .terms
                .iter()
                .filter(|term| hay.contains(term))
                .count()
        })
        .max()
        .unwrap_or(1)
        .max(1)
}

fn has_filters(filters: &SearchFilters) -> bool {
    filters.session.is_some()
        || filters.provider.is_some()
        || filters
            .repo
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
        || filters.since.is_some()
        || filters.primary_only
        || !filters.include_subagents
        || filters.event_type.is_some()
        || !filters.roles.is_empty()
        || !filters.exclude_roles.is_empty()
        || filters.exclude_tool_noise
        || filters
            .exclude_tool_names
            .iter()
            .any(|value| !value.trim().is_empty())
        || filters
            .file
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
        || filters.exclude_provider_session.is_some()
        || has_history_source_filter(filters)
}

fn record_matches_filters(
    record: &HistoryRecord,
    context: &RecordContext,
    filters: &SearchFilters,
    file_scope: Option<&FileTouchScope>,
) -> bool {
    if let Some(session_id) = filters.session {
        if !context
            .sessions
            .iter()
            .any(|session| session.id == session_id)
            && !context
                .events
                .iter()
                .any(|event| event.session_id == Some(session_id))
            && !context
                .runs
                .iter()
                .any(|run| run.session_id == Some(session_id))
        {
            return false;
        }
    }

    if let Some(excluded) = &filters.exclude_provider_session {
        let matched_sessions = context
            .sessions
            .iter()
            .filter(|session| {
                (session.provider == excluded.provider
                    && session.external_session_id.as_deref()
                        == Some(excluded.provider_session_id.as_str()))
                    || excluded_session_tree_matches(
                        excluded,
                        Some(session.id),
                        session.parent_session_id,
                        session.root_session_id,
                    )
            })
            .count();
        if matched_sessions > 0 && matched_sessions == context.sessions.len() {
            return false;
        }
    }

    if let Some(provider) = filters.provider {
        let session_match = context
            .sessions
            .iter()
            .any(|session| session.provider == provider);
        let source_match = context
            .sources
            .values()
            .any(|source| source.provider == provider);
        if !session_match && !source_match {
            return false;
        }
    }

    if has_history_source_filter(filters)
        && !context
            .sources
            .values()
            .any(|source| source_matches_history_source_filter(source, filters))
    {
        return false;
    }

    if let Some(since) = filters.since {
        let has_recent_event = context
            .events
            .iter()
            .any(|event| event.occurred_at >= since);
        let has_recent_session = context.sessions.iter().any(|session| {
            session.started_at >= since || session.ended_at.is_some_and(|ended| ended >= since)
        });
        if record.updated_at < since && !has_recent_event && !has_recent_session {
            return false;
        }
    }

    if (filters.primary_only || !filters.include_subagents)
        && !context.sessions.is_empty()
        && !context
            .sessions
            .iter()
            .any(|session| session_matches_agent_scope(session, filters))
    {
        return false;
    }

    if let Some(event_type) = filters.event_type {
        if !context
            .events
            .iter()
            .any(|event| event.event_type == event_type && role_matches(event.role, filters))
        {
            return false;
        }
    }
    if filters.event_type.is_none()
        && !filters.roles.is_empty()
        && !context
            .events
            .iter()
            .any(|event| role_matches(event.role, filters))
    {
        return false;
    }

    if let Some(repo) = filters
        .repo
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let repo = repo.to_lowercase();
        let matches_record = record
            .workspace
            .as_deref()
            .is_some_and(|workspace| workspace.to_lowercase().contains(&repo));
        let matches_session = context.sessions.iter().any(|session| {
            session
                .metadata
                .get("metadata")
                .and_then(|value| value.as_object())
                .is_some_and(|metadata| {
                    metadata
                        .values()
                        .any(|value| value.to_string().to_lowercase().contains(&repo))
                })
        });
        let matches_source = context.sources.values().any(|source| {
            source
                .cwd
                .as_deref()
                .is_some_and(|cwd| cwd.to_lowercase().contains(&repo))
        });
        if !matches_record && !matches_session && !matches_source {
            return false;
        }
    }

    if let Some(file) = filters
        .file
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(scope) = file_scope {
            if !record_context_matches_file_scope(scope, record, context) {
                return false;
            }
        } else if !context.files_touched.iter().any(|touched| {
            touched.path == file
                || touched.path.ends_with(file)
                || touched.old_path.as_deref() == Some(file)
        }) {
            return false;
        }
    }

    true
}

fn record_context_matches_file_scope(
    scope: &FileTouchScope,
    record: &HistoryRecord,
    context: &RecordContext,
) -> bool {
    scope.history_record_ids.contains(&record.id)
        || context.sessions.iter().any(|session| {
            scope.session_ids.contains(&session.id)
                || session
                    .capture_source_id
                    .is_some_and(|source_id| scope.source_ids.contains(&source_id))
        })
        || context.runs.iter().any(|run| {
            scope.run_ids.contains(&run.id)
                || run
                    .session_id
                    .is_some_and(|session_id| scope.session_ids.contains(&session_id))
                || run
                    .source_id
                    .is_some_and(|source_id| scope.source_ids.contains(&source_id))
        })
        || context.events.iter().any(|event| {
            scope.event_ids.contains(&event.id)
                || event
                    .session_id
                    .is_some_and(|session_id| scope.session_ids.contains(&session_id))
                || event
                    .run_id
                    .is_some_and(|run_id| scope.run_ids.contains(&run_id))
                || event
                    .capture_source_id
                    .is_some_and(|source_id| scope.source_ids.contains(&source_id))
        })
        || context.files_touched.iter().any(|file| {
            file.source_id
                .is_some_and(|source_id| scope.source_ids.contains(&source_id))
        })
}

fn matches_terms(value: &str, terms: &[String]) -> bool {
    if terms.is_empty() {
        return false;
    }
    SearchQueryPlan::new(SearchMatchMode::All, [terms.join(" ")]).matches_text(value)
}

fn search_snippet(
    record: &HistoryRecord,
    context: &RecordContext,
    query: &str,
    plan: &SearchQueryPlan,
    max_chars: usize,
    filters: &SearchFilters,
) -> String {
    let terms = query_terms(query);
    for section in search_sections(record, context, filters) {
        if hit_matches_excluded_provider_session(&section.hit, filters) {
            continue;
        }
        if plan.matches_text(&section.text) {
            return matched_snippet(&section.text, plan, &terms, max_chars);
        }
    }
    if !record.body.trim().is_empty()
        && !is_agent_history_bookkeeping_record(record)
        && record_text_matches_agent_scope(context, filters)
        && !context_has_excluded_provider_session(context, filters)
    {
        return local_snippet(&record.body, max_chars);
    }
    String::new()
}

fn matched_snippet(
    input: &str,
    plan: &SearchQueryPlan,
    terms: &[String],
    max_chars: usize,
) -> String {
    let body = input.trim();
    if body.is_empty() {
        return String::new();
    }
    let spans = token_spans(body);
    let start = snippet_anchor(&spans, plan).unwrap_or_else(|| {
        spans
            .iter()
            .filter(|span| terms.iter().any(|term| term == &span.term))
            .map(|span| span.start)
            .min()
            .unwrap_or(0)
    });
    let start = start.saturating_sub(max_chars / 4);
    let snippet = take_chars_from(body, start, max_chars);
    local_snippet(&snippet, max_chars)
}

fn snippet_anchor(spans: &[TokenSpan], plan: &SearchQueryPlan) -> Option<usize> {
    for clause in &plan.clauses {
        if clause.terms.is_empty() {
            continue;
        }
        match plan.mode {
            SearchMatchMode::Phrase => {
                for window in spans.windows(clause.terms.len()) {
                    if window
                        .iter()
                        .map(|span| span.term.as_str())
                        .eq(clause.terms.iter().map(String::as_str))
                    {
                        return Some(window[0].start);
                    }
                }
            }
            SearchMatchMode::All => {
                if clause
                    .terms
                    .iter()
                    .all(|term| spans.iter().any(|span| &span.term == term))
                {
                    return spans
                        .iter()
                        .find(|span| clause.terms.iter().any(|term| term == &span.term))
                        .map(|span| span.start);
                }
            }
            SearchMatchMode::Any => {
                if let Some(span) = spans
                    .iter()
                    .find(|span| clause.terms.iter().any(|term| term == &span.term))
                {
                    return Some(span.start);
                }
            }
        }
    }
    None
}

#[derive(Debug)]
struct TokenSpan {
    term: String,
    start: usize,
}

fn token_spans(input: &str) -> Vec<TokenSpan> {
    let mut spans = Vec::new();
    let mut term = String::new();
    let mut start = 0usize;
    for (char_index, ch) in input.chars().enumerate() {
        if ch.is_alphanumeric() {
            if term.is_empty() {
                start = char_index;
            }
            for lower in ch.to_lowercase() {
                term.push(lower);
            }
        } else if !term.is_empty() {
            spans.push(TokenSpan {
                term: std::mem::take(&mut term),
                start,
            });
        }
    }
    if !term.is_empty() {
        spans.push(TokenSpan { term, start });
    }
    spans
}

fn local_snippet(input: &str, max_chars: usize) -> String {
    truncate_chars(input.trim(), max_chars)
}

fn truncate_chars(input: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in input.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn take_chars_from(input: &str, start: usize, max_chars: usize) -> String {
    input.chars().skip(start).take(max_chars).collect()
}

fn links_for(_record: &HistoryRecord, _options: &PacketOptions) -> ContextLinks {
    ContextLinks {}
}

fn pagination(cursor_base: Option<usize>, has_more: bool) -> ContextPagination {
    ContextPagination {
        cursor: if has_more {
            cursor_base.map(|value| format!("offset:{value}"))
        } else {
            None
        },
        has_more,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctx_history_core::{
        default_data_root, AgentType, ArtifactKind, CaptureProvider, CaptureSource,
        CaptureSourceDescriptor, CaptureSourceKind, Confidence, EntityTimestamps, EventRole,
        EventType, Fidelity, FileChangeKind, HistoryRecordLink, HistoryRecordLinkTargetType,
        HistoryRecordLinkType, RedactionState, RunStatus, RunType, SessionHistoryArchive,
        SessionStatus, SummaryKind, SyncMetadata, SyncState, VcsChangeKind, VcsHost, VcsKind,
        VcsWorkspace,
    };
    use serde::{Deserialize, Serialize};

    fn tempdir() -> tempfile::TempDir {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .unwrap()
            .join("target/test-data");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("ctx-history-search-")
            .tempdir_in(root)
            .unwrap()
    }

    fn fixed_time() -> chrono::DateTime<Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-06-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn timestamps() -> EntityTimestamps {
        EntityTimestamps {
            created_at: fixed_time(),
            updated_at: fixed_time(),
        }
    }

    fn sync_metadata() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({}),
        }
    }

    fn excluded_filter(session_id: Option<Uuid>) -> SearchFilters {
        SearchFilters {
            exclude_provider_session: Some(ProviderSessionFilter {
                provider: CaptureProvider::Codex,
                provider_session_id: "provider-session-1".into(),
                session_id,
            }),
            ..SearchFilters::default()
        }
    }

    fn test_store() -> (tempfile::TempDir, ctx_history_store::Store) {
        let temp = tempdir();
        let path = temp.path().join("work.sqlite");
        let store = ctx_history_store::Store::open(path).unwrap();
        (temp, store)
    }

    fn reference_record_context(store: &Store, id: Uuid, file: Option<&str>) -> FullRecordContext {
        let sessions = store.sessions_for_record(id).unwrap();
        let runs = store.runs_for_record(id).unwrap();
        let events = store.events_for_record(id).unwrap();
        let artifacts = store.artifacts_for_record(id).unwrap();
        let files_touched = match file.map(str::trim).filter(|value| !value.is_empty()) {
            Some(file) => store.files_touched_for_record_matching(id, file).unwrap(),
            None => store.files_touched_for_record(id).unwrap(),
        };
        let vcs_changes = store.vcs_changes_for_record(id).unwrap();
        let summaries = store.summaries_for_record(id).unwrap();
        let mut source_ids = BTreeSet::new();
        source_ids.extend(sessions.iter().filter_map(|value| value.capture_source_id));
        source_ids.extend(runs.iter().filter_map(|value| value.source_id));
        source_ids.extend(events.iter().filter_map(|value| value.capture_source_id));
        source_ids.extend(artifacts.iter().filter_map(|value| value.source_id));
        source_ids.extend(files_touched.iter().filter_map(|value| value.source_id));
        source_ids.extend(vcs_changes.iter().filter_map(|value| value.source_id));
        source_ids.extend(summaries.iter().filter_map(|value| value.source_id));
        let sources = source_ids
            .into_iter()
            .filter_map(|id| store.get_capture_source(id).ok().map(|source| (id, source)))
            .collect();
        FullRecordContext {
            sessions,
            runs,
            events,
            artifacts,
            files_touched,
            vcs_changes,
            summaries,
            sources,
        }
    }

    #[test]
    fn batched_contexts_match_single_record_reference_and_chunk_formula() {
        let mut archive = synthetic_perf_archive(5_020, 10);
        archive.files_touched[0].old_path = Some("legacy/perf_profile_old.rs".into());
        // Exercise indirect ownership instead of relying only on direct record IDs.
        archive.runs[1].history_record_id = None;
        let indirect_session_id = archive.sessions[1].id;
        archive
            .events
            .iter_mut()
            .filter(|event| event.session_id == Some(indirect_session_id))
            .for_each(|event| event.history_record_id = None);
        archive.summaries[1].history_record_id = None;
        archive.files_touched[1].history_record_id = None;
        // One source is shared between records. Missing-source behavior is
        // covered below at the bounded source-loader boundary because schema
        // foreign keys prevent creating a dangling entity reference.
        let shared_source = archive.capture_sources[0].id;
        archive.sessions[1].capture_source_id = Some(shared_source);
        archive.runs[1].source_id = Some(shared_source);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let ids = archive
            .records
            .iter()
            .map(|record| record.id)
            .collect::<Vec<_>>();

        for count in [1, 37, 500] {
            let before = store.relation_batch_executions();
            let contexts = hydrate_record_contexts(&store, &ids[..count], None).unwrap();
            assert_eq!(
                store.relation_batch_executions() - before,
                8,
                "one statement per relation for {count} candidates"
            );
            let mut samples = vec![ids[0], ids[count - 1]];
            if count > 2 {
                samples.push(ids[count / 2]);
            }
            for id in samples {
                assert_eq!(contexts[&id], reference_record_context(&store, id, None));
            }
        }

        let before = store.relation_batch_executions();
        let contexts = hydrate_record_contexts(&store, &ids[..501], None).unwrap();
        assert_eq!(
            store.relation_batch_executions() - before,
            16,
            "eight relations times two SQLite-ID chunks"
        );
        assert_eq!(
            contexts[&ids[0]],
            reference_record_context(&store, ids[0], None)
        );
        assert_eq!(
            contexts[&ids[500]],
            reference_record_context(&store, ids[500], None)
        );

        let nonexistent = Uuid::from_u128(u128::MAX);
        assert!(store
            .capture_sources_for_ids(&[nonexistent])
            .unwrap()
            .is_empty());
        let requested = [ids[0], ids[0], nonexistent];
        let contexts = hydrate_record_contexts(&store, &requested, None).unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(
            contexts[&ids[0]],
            reference_record_context(&store, ids[0], None)
        );
        assert_eq!(contexts[&nonexistent], FullRecordContext::default());
        assert!(hydrate_record_contexts(&store, &[], None)
            .unwrap()
            .is_empty());

        for filter in [
            "perf_profile.rs",
            "profile.rs",
            "perf_profile_old.rs",
            "missing.rs",
        ] {
            let contexts = hydrate_record_contexts(&store, &ids[..3], Some(filter)).unwrap();
            for id in &ids[..3] {
                assert_eq!(
                    contexts[id],
                    reference_record_context(&store, *id, Some(filter))
                );
            }
        }
    }

    #[test]
    #[ignore = "targeted fallback hydration evidence"]
    fn fallback_batch_hydration_evidence() {
        let archive = synthetic_perf_archive(600, 5);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let ids = archive
            .records
            .iter()
            .map(|record| record.id)
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 120);

        let reference = ids
            .iter()
            .map(|id| (*id, reference_record_context(&store, *id, None)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            hydrate_record_contexts(&store, &ids, None).unwrap(),
            reference
        );

        let mut reference_ms = Vec::new();
        let mut batch_ms = Vec::new();
        let before = store.relation_batch_executions();
        for _ in 0..5 {
            let started = std::time::Instant::now();
            let actual = hydrate_record_contexts(&store, &ids, None).unwrap();
            batch_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
            assert_eq!(actual, reference);

            let started = std::time::Instant::now();
            let actual = ids
                .iter()
                .map(|id| (*id, reference_record_context(&store, *id, None)))
                .collect::<BTreeMap<_, _>>();
            reference_ms.push(started.elapsed().as_secs_f64() * 1_000.0);
            assert_eq!(actual, reference);
        }
        reference_ms.sort_by(f64::total_cmp);
        batch_ms.sort_by(f64::total_cmp);
        let batch_statements = (store.relation_batch_executions() - before) / 5;
        let reference_statements = ids.len() as u64 * 8;
        let reduction = 1.0 - batch_statements as f64 / reference_statements as f64;
        let reference_p95 = reference_ms[4];
        let batch_p95 = batch_ms[4];
        assert!(reduction >= 0.40);
        assert!(
            batch_p95 <= reference_p95 * 1.05,
            "targeted total regressed: batch={batch_p95:.3}ms reference={reference_p95:.3}ms"
        );
        println!(
            "fallback batch hydration evidence: {}",
            serde_json::json!({
                "profile": "fallback-batch-hydration-v1", "candidates": ids.len(), "relations": 8,
                "warm_samples": 5, "reference_samples_ms": reference_ms, "batch_samples_ms": batch_ms,
                "reference_p95_ms": reference_p95, "batch_p95_ms": batch_p95,
                "reference_statements": reference_statements, "batch_statements": batch_statements,
                "statement_reduction_percent": reduction * 100.0,
                "gate_basis": "statement reduction >=40% and targeted batch p95 <= reference p95 * 1.05",
                "fallback_proof": "calls the production fallback hydrate_record_contexts and retained single-record reference directly"
            })
        );
    }

    #[test]
    #[ignore = "targeted compact fallback hydration evidence"]
    fn compact_fallback_hydration_evidence() {
        let archive = synthetic_perf_archive(1_000, 5);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let options = PacketOptions {
            limit: 100,
            filters: SearchFilters {
                repo: Some("ctx".into()),
                ..SearchFilters::default()
            },
            ..PacketOptions::default()
        };
        for hydration in [HydrationIntent::Full, HydrationIntent::Compact] {
            let packet =
                search_packet_with_hydration(&store, "perfneedle", &options, hydration).unwrap();
            assert!(!packet.results.is_empty());
        }

        let mut full_ms = Vec::new();
        let mut narrow_ms = Vec::new();
        for sample in 0..5 {
            let mut pair = Vec::new();
            let order = if sample % 2 == 0 {
                [HydrationIntent::Compact, HydrationIntent::Full]
            } else {
                [HydrationIntent::Full, HydrationIntent::Compact]
            };
            for hydration in order {
                let started = std::time::Instant::now();
                let packet =
                    search_packet_with_hydration(&store, "perfneedle", &options, hydration)
                        .unwrap();
                let elapsed = started.elapsed().as_secs_f64() * 1_000.0;
                match hydration {
                    HydrationIntent::Full => full_ms.push(elapsed),
                    HydrationIntent::Compact => narrow_ms.push(elapsed),
                }
                pair.push((hydration, packet));
            }
            pair[0].1.generated_at = pair[1].1.generated_at;
            assert_eq!(pair[0].1, pair[1].1);
        }
        full_ms.sort_by(f64::total_cmp);
        narrow_ms.sort_by(f64::total_cmp);
        let full_p95 = full_ms[4];
        let narrow_p95 = narrow_ms[4];
        let ratio = narrow_p95 / full_p95;
        println!(
            "compact fallback: full_ms={full_ms:?} narrow_ms={narrow_ms:?} full_p95={full_p95:.3}ms narrow_p95={narrow_p95:.3}ms ratio={ratio:.3}"
        );
        assert!(
            ratio <= 1.05,
            "narrow p95 regressed by more than 5%: {ratio:.3}"
        );
    }

    fn insert_match_mode_corpus(store: &Store, event_count: i64) -> Vec<Uuid> {
        let base_session_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000001001").unwrap();
        let filler_record = HistoryRecord::new(
            "match mode filler",
            "fallback body",
            Vec::new(),
            "agent_history",
            Some("/workspace/match".into()),
        );
        store.insert_record(&filler_record).unwrap();
        let filler_session = Session {
            id: base_session_id,
            history_record_id: Some(filler_record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("match-mode".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&filler_session).unwrap();
        let targets = [
            "Alpha beta write_to_file OR NOT title:body star*",
            "beta alpha write to file or not title body star",
            "alpha only",
            "Café résumé Δοκιμή 東京",
            "rank apple",
            "rank apple banana cherry",
        ];
        let mut target_ids = Vec::new();
        for index in 0..event_count.max(targets.len() as i64) as u64 {
            let is_target = (index as usize) < targets.len();
            let (record_id, session_id) = if is_target {
                let mut record = HistoryRecord::new(
                    format!("match target {index}"),
                    "target body",
                    Vec::new(),
                    "agent_history",
                    Some("/workspace/match".into()),
                );
                record.id =
                    Uuid::parse_str(&format!("018f45d0-0000-7000-8000-00000001{index:04x}"))
                        .unwrap();
                store.insert_record(&record).unwrap();
                let mut sid_bytes = *base_session_id.as_bytes();
                sid_bytes[15] = 0x80 + index as u8;
                let session_id = Uuid::from_bytes(sid_bytes);
                let mut session = filler_session.clone();
                session.id = session_id;
                session.history_record_id = Some(record.id);
                store.upsert_session(&session).unwrap();
                (record.id, session_id)
            } else {
                (filler_record.id, filler_session.id)
            };
            let mut bytes = *base_session_id.as_bytes();
            bytes[12] = ((index >> 24) & 0xff) as u8;
            bytes[13] = ((index >> 16) & 0xff) as u8;
            bytes[14] = ((index >> 8) & 0xff) as u8;
            bytes[15] = (index & 0xff) as u8;
            let event_id = Uuid::from_bytes(bytes);
            let text = targets
                .get(index as usize)
                .copied()
                .unwrap_or("filler event");
            if is_target {
                target_ids.push(event_id);
            }
            store
                .upsert_event(&Event {
                    id: event_id,
                    seq: index,
                    history_record_id: Some(record_id),
                    session_id: Some(session_id),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(index as i64),
                    capture_source_id: None,
                    payload: serde_json::json!({"body": {"text": text}}),
                    payload_blob_id: None,
                    dedupe_key: Some(format!("match-mode-{index}")),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();
        target_ids
    }

    fn event_ids_for(store: &Store, query: &str, mode: SearchMatchMode) -> Vec<Uuid> {
        search_packet(
            store,
            query,
            &PacketOptions {
                limit: 10,
                result_mode: SearchResultMode::Events,
                match_mode: mode,
                ..PacketOptions::default()
            },
        )
        .unwrap()
        .results
        .into_iter()
        .filter_map(|result| result.event_id)
        .collect()
    }

    #[test]
    fn role_and_tool_noise_filters_penalize_incidental_tool_matches() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "role noise corpus",
            "indexed role noise corpus",
            Vec::new(),
            "agent_history",
            Some("/workspace/role-noise".into()),
        );
        store.insert_record(&record).unwrap();
        let mut source_record = HistoryRecord::new(
            "source_only_token.rs",
            "fn source_code_fixture() { let source_only_token = \"zephyr-token\"; }",
            Vec::new(),
            "source_code",
            Some("/workspace/role-noise/src".into()),
        );
        source_record.id = Uuid::parse_str("018f45d0-0000-7000-8000-000000009210").unwrap();
        store.insert_record(&source_record).unwrap();
        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009193").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("role-noise".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let run = Run {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009206").unwrap(),
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_type: RunType::Command,
            status: RunStatus::Succeeded,
            started_at: fixed_time(),
            ended_at: Some(fixed_time()),
            exit_code: Some(0),
            cwd: Some("/workspace/role-noise".into()),
            command_preview: Some("ctx search zephyr-token".into()),
            input_blob_id: None,
            output_blob_id: None,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_run(&run).unwrap();
        let artifact = Artifact {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009207").unwrap(),
            kind: ArtifactKind::Markdown,
            blob_hash: "hash-role-noise-source".into(),
            blob_path: "objects/role-noise-source".into(),
            byte_size: 64,
            media_type: Some("text/rust".into()),
            preview_text: Some(
                "fn source_code_fixture() { let source_only_token = \"zephyr-token\"; }".into(),
            ),
            redaction_state: RedactionState::SafePreview,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_artifact(&artifact).unwrap();
        store
            .upsert_history_record_link(&HistoryRecordLink {
                id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009208").unwrap(),
                history_record_id: record.id,
                target_type: HistoryRecordLinkTargetType::Artifact,
                target_id: artifact.id,
                link_type: HistoryRecordLinkType::References,
                confidence: Confidence::Explicit,
                source_id: None,
                timestamps: timestamps(),
                sync: sync_metadata(),
            })
            .unwrap();
        store
            .upsert_file_touched(&FileTouched {
                id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009209").unwrap(),
                history_record_id: Some(record.id),
                run_id: None,
                event_id: None,
                vcs_workspace_id: None,
                path: "src/source_only_token.rs".into(),
                old_path: None,
                change_kind: Some(FileChangeKind::Modified),
                line_count_delta: Some(1),
                confidence: Confidence::Explicit,
                timestamps: timestamps(),
                source_id: None,
                sync: sync_metadata(),
            })
            .unwrap();
        let rows = [
            (
                "018f45d0-0000-7000-8000-000000009201",
                EventType::Message,
                Some(EventRole::User),
                serde_json::json!({"body":{"text":"Decided to use zephyr-token for rollout"}}),
            ),
            (
                "018f45d0-0000-7000-8000-000000009202",
                EventType::Message,
                Some(EventRole::Assistant),
                serde_json::json!({"body":{"text":"We will keep zephyr-token in the implementation plan"}}),
            ),
            (
                "018f45d0-0000-7000-8000-000000009203",
                EventType::CommandStarted,
                Some(EventRole::Tool),
                serde_json::json!({"command":"ctx search zephyr-token"}),
            ),
            (
                "018f45d0-0000-7000-8000-000000009204",
                EventType::CommandOutput,
                Some(EventRole::Tool),
                serde_json::json!({"output":"ctx result zephyr-token unrelated source shared token"}),
            ),
            (
                "018f45d0-0000-7000-8000-000000009205",
                EventType::ToolOutput,
                Some(EventRole::Tool),
                serde_json::json!({"tool":"shell","output":"unrelated source has zephyr-token"}),
            ),
        ];
        for (seq, (id, event_type, role, payload)) in rows.into_iter().enumerate() {
            store
                .upsert_event(&Event {
                    id: Uuid::parse_str(id).unwrap(),
                    seq: seq as u64,
                    history_record_id: Some(record.id),
                    session_id: Some(session.id),
                    run_id: None,
                    event_type,
                    role,
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(seq as i64),
                    capture_source_id: None,
                    payload,
                    payload_blob_id: None,
                    dedupe_key: None,
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();
        assert_role_noise_packet(&store, source_record.id);

        for seq in 5..1030_u64 {
            let mut bytes = *session.id.as_bytes();
            bytes[10] = ((seq >> 8) & 0xff) as u8;
            bytes[11] = (seq & 0xff) as u8;
            store
                .upsert_event(&Event {
                    id: Uuid::from_bytes(bytes),
                    seq,
                    history_record_id: Some(record.id),
                    session_id: Some(session.id),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(seq as i64),
                    capture_source_id: None,
                    payload: serde_json::json!({"body":{"text":"filler without target token"}}),
                    payload_blob_id: None,
                    dedupe_key: None,
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();
        assert_role_noise_packet(&store, source_record.id);
    }

    fn assert_role_noise_packet(store: &Store, source_record_id: Uuid) {
        let packet = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Events,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            packet.results[0].event_id,
            Some(Uuid::parse_str("018f45d0-0000-7000-8000-000000009201").unwrap())
        );
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|why| why.contains("role:user")));
        assert!(packet.results.iter().any(|result| result
            .why_matched
            .iter()
            .any(|why| why.contains("relevance_penalty:"))));

        let users = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Events,
                filters: SearchFilters {
                    roles: vec![EventRole::User],
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(users.results.len(), 1);

        let no_tools = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Events,
                filters: SearchFilters {
                    exclude_tool_noise: true,
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(no_tools.results.iter().all(|result| result
            .why_matched
            .iter()
            .all(|why| !why.contains("relevance_penalty:"))));

        let no_ctx = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Events,
                filters: SearchFilters {
                    exclude_tool_names: vec!["ctx".into()],
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(no_ctx.results.iter().all(|result| result.event_id
            != Some(Uuid::parse_str("018f45d0-0000-7000-8000-000000009203").unwrap())));

        assert!(no_ctx.results.iter().any(|result| result.event_id
            == Some(Uuid::parse_str("018f45d0-0000-7000-8000-000000009201").unwrap())));

        let default_packet = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Sessions,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(default_packet.results[0]
            .why_matched
            .iter()
            .any(|why| why.contains("role:user") || why.contains("role:assistant")));

        let source_packet = search_packet(
            store,
            "source_only_token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Sessions,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(source_packet
            .results
            .iter()
            .any(|result| result.record_id == source_record_id));

        let no_ctx_session = search_packet(
            store,
            "zephyr-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Sessions,
                filters: SearchFilters {
                    exclude_tool_names: vec!["ctx".into()],
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(no_ctx_session
            .results
            .iter()
            .all(|result| !result.why_matched.iter().any(|why| why == "run_command")));
    }

    #[test]
    fn fast_role_filter_pages_past_many_excluded_fts_hits() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "role paging corpus",
            "indexed role paging corpus",
            Vec::new(),
            "agent_history",
            Some("/workspace/role-paging".into()),
        );
        store.insert_record(&record).unwrap();
        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000009300").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("role-paging".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        for seq in 0..1100_u64 {
            let event_id =
                Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000001{seq:05x}")).unwrap();
            store
                .upsert_event(&Event {
                    id: event_id,
                    seq,
                    history_record_id: Some(record.id),
                    session_id: Some(session.id),
                    run_id: None,
                    event_type: EventType::CommandOutput,
                    role: Some(EventRole::Tool),
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(seq as i64 + 10),
                    capture_source_id: None,
                    payload: serde_json::json!({"output":"late-token noisy command output"}),
                    payload_blob_id: None,
                    dedupe_key: None,
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        let user_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000093ff").unwrap();
        store
            .upsert_event(&Event {
                id: user_id,
                seq: 1200,
                history_record_id: Some(record.id),
                session_id: Some(session.id),
                run_id: None,
                event_type: EventType::Message,
                role: Some(EventRole::User),
                occurred_at: fixed_time(),
                capture_source_id: None,
                payload: serde_json::json!({"body":{"text":"late-token human decision"}}),
                payload_blob_id: None,
                dedupe_key: None,
                redaction_state: RedactionState::SafePreview,
                sync: sync_metadata(),
            })
            .unwrap();
        store.refresh_search_index().unwrap();

        let packet = search_packet(
            &store,
            "late-token",
            &PacketOptions {
                limit: 1,
                result_mode: SearchResultMode::Events,
                filters: SearchFilters {
                    roles: vec![EventRole::User],
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(packet.results[0].event_id, Some(user_id));
    }

    #[test]
    fn rich_role_and_event_type_filters_only_score_matching_event_evidence() {
        let (_temp, store) = test_store();
        let mut wrong = HistoryRecord::new(
            "mixed-token misleading title",
            "mixed-token misleading body",
            Vec::new(),
            "agent_history",
            Some("/workspace/mixed".into()),
        );
        wrong.id = Uuid::parse_str("018f45d0-0000-7000-8000-000000009401").unwrap();
        store.insert_record(&wrong).unwrap();
        let mut right = HistoryRecord::new(
            "right record",
            "right body",
            Vec::new(),
            "agent_history",
            Some("/workspace/mixed".into()),
        );
        right.id = Uuid::parse_str("018f45d0-0000-7000-8000-000000009402").unwrap();
        store.insert_record(&right).unwrap();
        for (record, sid, eid, role, text, seq) in [
            (
                &wrong,
                "018f45d0-0000-7000-8000-000000009411",
                "018f45d0-0000-7000-8000-000000009421",
                EventRole::Assistant,
                "mixed-token assistant only",
                1_u64,
            ),
            (
                &right,
                "018f45d0-0000-7000-8000-000000009412",
                "018f45d0-0000-7000-8000-000000009422",
                EventRole::User,
                "mixed-token user decision",
                2_u64,
            ),
        ] {
            let session = Session {
                id: Uuid::parse_str(sid).unwrap(),
                history_record_id: Some(record.id),
                parent_session_id: None,
                root_session_id: None,
                capture_source_id: None,
                provider: CaptureProvider::Codex,
                external_session_id: Some(sid.into()),
                external_agent_id: None,
                agent_type: AgentType::Primary,
                role_hint: Some("primary".into()),
                is_primary: true,
                status: SessionStatus::Imported,
                transcript_blob_id: None,
                started_at: fixed_time(),
                ended_at: None,
                timestamps: timestamps(),
                sync: sync_metadata(),
            };
            store.upsert_session(&session).unwrap();
            store
                .upsert_event(&Event {
                    id: Uuid::parse_str(eid).unwrap(),
                    seq,
                    history_record_id: Some(record.id),
                    session_id: Some(session.id),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(role),
                    occurred_at: fixed_time(),
                    capture_source_id: None,
                    payload: serde_json::json!({"body":{"text":text}}),
                    payload_blob_id: None,
                    dedupe_key: None,
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();
        let packet = search_packet(
            &store,
            "mixed-token",
            &PacketOptions {
                limit: 5,
                result_mode: SearchResultMode::Events,
                filters: SearchFilters {
                    roles: vec![EventRole::User],
                    event_type: Some(EventType::Message),
                    repo: Some("mixed".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(packet.results.len(), 1, "{packet:#?}");
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|why| why == "role:user"));
    }

    #[test]
    fn match_modes_are_semantically_identical_below_at_and_above_fast_threshold() {
        let mut baselines: Option<Vec<(String, SearchMatchMode, Vec<Uuid>)>> = None;
        for count in [
            LARGE_EVENT_CORPUS_THRESHOLD - 1,
            LARGE_EVENT_CORPUS_THRESHOLD,
            LARGE_EVENT_CORPUS_THRESHOLD + 1,
        ] {
            let (_temp, store) = test_store();
            let target_ids = insert_match_mode_corpus(&store, count);
            let cases = vec![
                (
                    "alpha beta".to_owned(),
                    SearchMatchMode::All,
                    vec![target_ids[0], target_ids[1]],
                ),
                (
                    "alpha beta".to_owned(),
                    SearchMatchMode::Phrase,
                    vec![target_ids[0]],
                ),
                (
                    "beta alpha".to_owned(),
                    SearchMatchMode::Phrase,
                    vec![target_ids[1]],
                ),
                (
                    "alpha gamma".to_owned(),
                    SearchMatchMode::Any,
                    vec![target_ids[2], target_ids[0], target_ids[1]],
                ),
                (
                    "write_to_file".to_owned(),
                    SearchMatchMode::Phrase,
                    vec![target_ids[0], target_ids[1]],
                ),
                (
                    "OR NOT star* title:body".to_owned(),
                    SearchMatchMode::All,
                    vec![target_ids[0], target_ids[1]],
                ),
                (
                    "café résumé δοκιμή 東京".to_owned(),
                    SearchMatchMode::All,
                    vec![target_ids[3]],
                ),
            ];
            let observed = cases
                .into_iter()
                .map(|(query, mode, expected)| {
                    let ids = event_ids_for(&store, &query, mode);
                    for id in expected {
                        assert!(
                            ids.contains(&id),
                            "count {count} query {query:?} missing {id}; got {ids:?}"
                        );
                    }
                    (query, mode, ids)
                })
                .collect::<Vec<_>>();
            if let Some(baselines) = &baselines {
                assert_eq!(
                    observed, *baselines,
                    "semantic drift at event count {count}"
                );
            } else {
                baselines = Some(observed);
            }
            let ranked = event_ids_for(&store, "rank apple banana cherry", SearchMatchMode::Any);
            assert!(
                ranked.iter().position(|id| *id == target_ids[5]).unwrap()
                    < ranked.iter().position(|id| *id == target_ids[4]).unwrap(),
                "multi-token any hit should outrank one-token hit at count {count}: {ranked:?}"
            );
            assert!(event_ids_for(&store, "cafe resume", SearchMatchMode::All).is_empty());
        }
    }

    #[test]
    fn large_event_fast_path_zero_hits_falls_back_to_record_sections() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "title only fallbackneedle",
            "body only match",
            Vec::new(),
            "note",
            Some("/workspace/match".into()),
        );
        store.insert_record(&record).unwrap();
        for index in 0..=LARGE_EVENT_CORPUS_THRESHOLD as u64 {
            let mut bytes = *record.id.as_bytes();
            bytes[12] = ((index >> 24) & 0xff) as u8;
            bytes[13] = ((index >> 16) & 0xff) as u8;
            bytes[14] = ((index >> 8) & 0xff) as u8;
            bytes[15] = (index & 0xff) as u8;
            store
                .upsert_event(&Event {
                    id: Uuid::from_bytes(bytes),
                    seq: index,
                    history_record_id: Some(record.id),
                    session_id: None,
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at: fixed_time(),
                    capture_source_id: None,
                    payload: serde_json::json!({"body": {"text": "ordinary event"}}),
                    payload_blob_id: None,
                    dedupe_key: Some(format!("fallback-title-{index}")),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();
        let packet = search_packet(&store, "fallbackneedle", &PacketOptions::default()).unwrap();
        assert_eq!(packet.results.len(), 1);
        assert_eq!(packet.results[0].record_id, record.id);
    }

    #[test]
    fn local_snippets_preserve_transcript_text() {
        let snippet = display_snippet(
            "token=ghp_1234567890abcdef1234567890abcdef and password=hunter2",
            200,
        );

        assert!(snippet.contains("token=ghp_1234567890abcdef1234567890abcdef"));
        assert!(snippet.contains("password=hunter2"));
        assert!(!snippet.contains("[REDACTED"));
    }

    #[test]
    fn matched_snippet_anchors_mode_aware_token_spans_without_byte_offsets() {
        let phrase = SearchQueryPlan::new(SearchMatchMode::Phrase, ["needle phrase"]);
        let snippet = matched_snippet(
            "éééé early needle decoy ... later needle-phrase match",
            &phrase,
            &query_terms("needle phrase"),
            40,
        );
        assert!(snippet.contains("needle-phrase"), "{snippet}");

        let any = SearchQueryPlan::new(SearchMatchMode::Any, ["İİ x"]);
        let snippet = matched_snippet(
            "prefix 測試 測試 İİ x suffix",
            &any,
            &query_terms("İİ x"),
            30,
        );
        assert!(snippet.contains("İİ") || snippet.contains('x'), "{snippet}");
    }

    #[test]
    fn withheld_events_do_not_render_payload_previews() {
        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000010").unwrap(),
            seq: 1,
            history_record_id: None,
            session_id: None,
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({"text": "secret payload that must not render"}),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::Withheld,
            sync: sync_metadata(),
        };

        let preview = event_preview_text(&event);
        assert_eq!(preview, "raw event payload withheld");
        assert!(!preview.contains("secret payload"));
    }

    #[test]
    fn excluded_provider_session_matches_provider_external_id_for_hits() {
        let filters = excluded_filter(None);
        let hit = HitMetadata {
            provider: Some(CaptureProvider::Codex),
            provider_session_id: Some("provider-session-1".into()),
            ..empty_hit(fixed_time())
        };
        assert!(hit_matches_excluded_provider_session(&hit, &filters));

        let event_hit = EventSearchHit {
            event_id: Uuid::parse_str("018f45d0-0000-7000-8000-000000001001").unwrap(),
            history_record_id: None,
            session_id: None,
            session_parent_session_id: None,
            session_root_session_id: None,
            run_id: None,
            seq: 1,
            event_type: EventType::Message,
            role: Some(EventRole::User),
            occurred_at: fixed_time(),
            preview: "synthetic preview".into(),
            score: 1.0,
            provider: Some(CaptureProvider::Codex),
            session_external_session_id: Some("provider-session-1".into()),
            history_source: None,
            history_source_plugin: None,
            provider_key: None,
            source_id: None,
            source_format: None,
            agent_type: Some(AgentType::Primary),
            session_is_primary: Some(true),
            cwd: None,
            raw_source_path: None,
            cursor: None,
            record_title: None,
            record_kind: None,
            record_workspace: None,
            tool_names: Vec::new(),
        };
        assert!(event_hit_matches_excluded_provider_session(
            &event_hit, &filters
        ));

        let mut different_provider = event_hit;
        different_provider.provider = Some(CaptureProvider::Claude);
        assert!(!event_hit_matches_excluded_provider_session(
            &different_provider,
            &filters
        ));
    }

    #[test]
    fn excluded_provider_session_matches_parent_and_root_session_tree() {
        let excluded_session_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000001100").unwrap();
        let child_session_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000001101").unwrap();
        let grandchild_session_id =
            Uuid::parse_str("018f45d0-0000-7000-8000-000000001102").unwrap();
        let filters = excluded_filter(Some(excluded_session_id));

        let parent_hit = HitMetadata {
            session_id: Some(child_session_id),
            parent_session_id: Some(excluded_session_id),
            ..empty_hit(fixed_time())
        };
        assert!(hit_matches_excluded_provider_session(&parent_hit, &filters));

        let root_event_hit = EventSearchHit {
            event_id: Uuid::parse_str("018f45d0-0000-7000-8000-000000001103").unwrap(),
            history_record_id: None,
            session_id: Some(grandchild_session_id),
            session_parent_session_id: Some(child_session_id),
            session_root_session_id: Some(excluded_session_id),
            run_id: None,
            seq: 1,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            preview: "synthetic preview".into(),
            score: 1.0,
            provider: None,
            session_external_session_id: None,
            history_source: None,
            history_source_plugin: None,
            provider_key: None,
            source_id: None,
            source_format: None,
            agent_type: Some(AgentType::Subagent),
            session_is_primary: Some(false),
            cwd: None,
            raw_source_path: None,
            cursor: None,
            record_title: None,
            record_kind: None,
            record_workspace: None,
            tool_names: Vec::new(),
        };
        assert!(event_hit_matches_excluded_provider_session(
            &root_event_hit,
            &filters
        ));

        let context: RecordContext = FullRecordContext {
            sessions: vec![Session {
                id: grandchild_session_id,
                history_record_id: None,
                parent_session_id: Some(child_session_id),
                root_session_id: Some(excluded_session_id),
                capture_source_id: None,
                provider: CaptureProvider::Claude,
                external_session_id: Some("different-provider-session".into()),
                external_agent_id: None,
                agent_type: AgentType::Subagent,
                role_hint: None,
                is_primary: false,
                status: SessionStatus::Imported,
                transcript_blob_id: None,
                started_at: fixed_time(),
                ended_at: None,
                timestamps: timestamps(),
                sync: sync_metadata(),
            }],
            ..FullRecordContext::default()
        }
        .into();
        assert!(context_has_excluded_provider_session(&context, &filters));
    }

    #[test]
    fn rich_search_matches_typed_metadata_with_citations_and_redaction() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Plain work",
            "ordinary body without the query",
            vec!["needle-tag".into()],
            "task",
            None,
        );
        store.insert_record(&record).unwrap();

        let artifact = Artifact {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000201").unwrap(),
            kind: ArtifactKind::Markdown,
            blob_hash: "hash-rich-search-artifact".into(),
            blob_path: "blobs/rich-search-artifact".into(),
            byte_size: 32,
            media_type: Some("text/markdown".into()),
            preview_text: Some("needle-artifact /home/example/private/repo".into()),
            redaction_state: RedactionState::SafePreview,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_artifact(&artifact).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000202").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("needle-session".into()),
            external_agent_id: Some("agent-needle".into()),
            agent_type: AgentType::Primary,
            role_hint: Some("needle-role".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let run = Run {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000203").unwrap(),
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_type: RunType::Command,
            status: RunStatus::Failed,
            started_at: fixed_time(),
            ended_at: Some(fixed_time()),
            exit_code: Some(1),
            cwd: Some("/home/example/private/repo".into()),
            command_preview: Some("cargo test needle-run password=hunter2".into()),
            input_blob_id: None,
            output_blob_id: Some(artifact.id),
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_run(&run).unwrap();

        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000204").unwrap(),
            seq: 1,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: Some(run.id),
            event_type: EventType::ToolCall,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({
                "tool": "shell",
                "arguments": "needle-event token=secretvalue"
            }),
            payload_blob_id: None,
            dedupe_key: Some("needle-dedupe".into()),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();

        let workspace = VcsWorkspace {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000205").unwrap(),
            kind: VcsKind::Git,
            root_path: "/repo".into(),
            repo_fingerprint: "git:needle".into(),
            primary_remote_url_normalized: Some("https://github.com/ctxrs/ctx".into()),
            host: VcsHost::Github,
            owner: Some("ctxrs".into()),
            name: Some("ctx".into()),
            monorepo_subpath: None,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        let workspace_id = store.upsert_vcs_workspace(&workspace).unwrap();

        let change = VcsChange {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000206").unwrap(),
            vcs_workspace_id: workspace_id,
            kind: VcsChangeKind::GitCommit,
            change_id: "needle-change".into(),
            parent_change_ids: vec!["parent".into()],
            branch_or_bookmark: Some("ctx/needle-branch".into()),
            tree_hash: Some("tree".into()),
            author_time: Some(fixed_time()),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_vcs_change(&change).unwrap();

        let file = FileTouched {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000208").unwrap(),
            history_record_id: Some(record.id),
            run_id: Some(run.id),
            event_id: Some(event.id),
            vcs_workspace_id: Some(workspace_id),
            path: "crates/ctx-history-search/src/needle_file.rs".into(),
            change_kind: Some(FileChangeKind::Modified),
            old_path: None,
            line_count_delta: Some(12),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_file_touched(&file).unwrap();

        let summary = Summary {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000209").unwrap(),
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            kind: SummaryKind::ImportedProviderSummary,
            model_or_source: Some("codex".into()),
            text: "needle summary password=hunter2".into(),
            citations: Vec::new(),
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_summary(&summary).unwrap();

        for (target_type, target_id, link_type) in [
            (
                HistoryRecordLinkTargetType::VcsChange,
                change.id,
                HistoryRecordLinkType::References,
            ),
            (
                HistoryRecordLinkTargetType::Artifact,
                artifact.id,
                HistoryRecordLinkType::Produced,
            ),
        ] {
            store
                .upsert_history_record_link(&HistoryRecordLink {
                    id: new_link_id(target_id),
                    history_record_id: record.id,
                    target_type,
                    target_id,
                    link_type,
                    confidence: Confidence::Explicit,
                    source_id: None,
                    timestamps: timestamps(),
                    sync: sync_metadata(),
                })
                .unwrap();
        }

        let options = PacketOptions {
            limit: 5,
            snippet_chars: 600,
            ..PacketOptions::default()
        };
        store.reset_search_hydration_loader_executions();
        let mut packet =
            search_packet_with_hydration(&store, "needle", &options, HydrationIntent::Full)
                .unwrap();
        assert_eq!(store.search_hydration_loader_executions(), [8, 0]);
        store.reset_search_hydration_loader_executions();
        let narrow =
            search_packet_with_hydration(&store, "needle", &options, HydrationIntent::Compact)
                .unwrap();
        assert_eq!(store.search_hydration_loader_executions(), [0, 8]);
        packet.generated_at = narrow.generated_at;
        assert_eq!(
            narrow, packet,
            "narrow hydration changed fallback semantics"
        );

        assert_eq!(packet.results.len(), 1);
        let result = &packet.results[0];
        for reason in [
            "tag",
            "session_metadata",
            "run_command",
            "tool_call",
            "artifact",
            "file_touched",
            "vcs_change",
            "summary",
        ] {
            assert!(
                result.why_matched.iter().any(|value| value == reason),
                "missing why_matched reason {reason}: {:?}",
                result.why_matched
            );
        }
        for removed_reason in ["failed_command", "failed_evidence_output", "pull_request"] {
            assert!(
                !result
                    .why_matched
                    .iter()
                    .any(|value| value == removed_reason),
                "removed reason {removed_reason} leaked into search result: {:?}",
                result.why_matched
            );
        }

        for citation_type in [
            ContextCitationType::HistoryRecord,
            ContextCitationType::Session,
            ContextCitationType::Run,
            ContextCitationType::Event,
            ContextCitationType::Artifact,
            ContextCitationType::File,
            ContextCitationType::VcsChange,
            ContextCitationType::Summary,
        ] {
            assert!(
                result
                    .citations
                    .iter()
                    .any(|citation| citation.citation_type == citation_type),
                "missing citation type {citation_type:?}: {:?}",
                result.citations
            );
        }
        assert_eq!(result.visibility, Visibility::LocalOnly);
        assert!(!result.snippet.contains("hunter2"));
        assert!(!result.snippet.contains("ghp_123456"));
        assert!(!result.snippet.contains("secretvalue"));

        let secret_packet = search_packet(
            &store,
            "hunter2",
            &PacketOptions {
                limit: 1,
                snippet_chars: 600,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(secret_packet.results.is_empty());

        let shared_record =
            HistoryRecord::new("Shared relations", "plain", Vec::new(), "task", None);
        store.insert_record(&shared_record).unwrap();
        for (offset, target_type, target_id) in [
            (1_u128, HistoryRecordLinkTargetType::Artifact, artifact.id),
            (2_u128, HistoryRecordLinkTargetType::VcsChange, change.id),
        ] {
            store
                .upsert_history_record_link(&HistoryRecordLink {
                    id: Uuid::from_u128(shared_record.id.as_u128().wrapping_add(offset)),
                    history_record_id: shared_record.id,
                    target_type,
                    target_id,
                    link_type: HistoryRecordLinkType::References,
                    confidence: Confidence::Explicit,
                    source_id: None,
                    timestamps: timestamps(),
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        let contexts =
            hydrate_record_contexts(&store, &[record.id, shared_record.id], None).unwrap();
        assert_eq!(
            contexts[&record.id],
            reference_record_context(&store, record.id, None)
        );
        assert_eq!(
            contexts[&shared_record.id],
            reference_record_context(&store, shared_record.id, None)
        );
        assert_eq!(
            contexts[&record.id].artifacts[0].id,
            contexts[&shared_record.id].artifacts[0].id
        );
        assert_eq!(
            contexts[&record.id].vcs_changes[0].id,
            contexts[&shared_record.id].vcs_changes[0].id
        );

        maybe_write_synthetic_search_smoke_artifact();
    }

    #[test]
    fn nested_provider_body_event_preview_drives_search() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Provider event record",
            "ordinary body without event query",
            Vec::new(),
            "task",
            None,
        );
        store.insert_record(&record).unwrap();
        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000301").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("codex-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("worker".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000302").unwrap(),
            seq: 1,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::ToolCall,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({
                "provider": "codex",
                "body": {
                    "tool": "shell",
                    "name": "exec_command",
                    "arguments_preview": "nested-search-needle token=secretvalue",
                    "arguments": "unsafe-raw-needle password=hunter2"
                }
            }),
            payload_blob_id: None,
            dedupe_key: Some("nested-provider-event".into()),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();
        store.upsert_record(&record).unwrap();

        let packet = search_packet(
            &store,
            "nested-search-needle",
            &PacketOptions {
                limit: 5,
                snippet_chars: 600,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(packet.results.len(), 1);
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "tool_call"));
        assert!(packet.results[0]
            .snippet
            .contains("arguments_preview: nested-search-needle token=secretvalue"));
        assert!(!packet.results[0].snippet.contains("unsafe-raw-needle"));
        assert!(!packet.results[0].snippet.contains("hunter2"));

        let unsafe_packet = search_packet(
            &store,
            "unsafe-raw-needle",
            &PacketOptions {
                limit: 5,
                snippet_chars: 600,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(unsafe_packet.results.is_empty());
    }

    #[test]
    fn large_agent_history_search_returns_event_hits() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Large provider history",
            "single imported agent-history record",
            Vec::new(),
            "agent_history",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&record).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000601").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("large-history-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let other_record = HistoryRecord::new(
            "Large provider history shard",
            "another imported agent-history record",
            Vec::new(),
            "agent_history",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&other_record).unwrap();
        let other_session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000602").unwrap(),
            history_record_id: Some(other_record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("large-history-session-shard".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&other_session).unwrap();

        let target_event_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000006ff").unwrap();
        for index in 0..=(LARGE_EVENT_CORPUS_THRESHOLD as u64) {
            let (event_record_id, event_session) = if index < 512 {
                (other_record.id, other_session.id)
            } else {
                (record.id, session.id)
            };
            let event_id = if index == LARGE_EVENT_CORPUS_THRESHOLD as u64 {
                target_event_id
            } else {
                let mut bytes = *event_session.as_bytes();
                bytes[14] = (index / 256) as u8;
                bytes[15] = index as u8;
                Uuid::from_bytes(bytes)
            };
            let text = if event_id == target_event_id {
                "large-fast-event-needle from one transcript"
            } else {
                "ordinary large history event"
            };
            store
                .upsert_event(&Event {
                    id: event_id,
                    seq: 10_000 + index,
                    history_record_id: Some(event_record_id),
                    session_id: Some(event_session),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(index as i64),
                    capture_source_id: None,
                    payload: serde_json::json!({
                        "cursor": format!("line:{index}"),
                        "body": { "text": text }
                    }),
                    payload_blob_id: None,
                    dedupe_key: Some(format!("large-history-{index}")),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();

        let packet = search_packet(
            &store,
            "large-fast-event-needle",
            &PacketOptions {
                limit: 5,
                snippet_chars: 200,
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), 1);
        let result = &packet.results[0];
        assert_eq!(result.result_scope, SearchResultScope::Session);
        assert_eq!(result.record_id, target_event_id);
        assert_eq!(result.event_id, Some(target_event_id));
        assert_eq!(result.session_id, Some(session.id));
        assert_eq!(result.provider, Some(CaptureProvider::Codex));
        assert_eq!(
            result.snippet,
            "large-fast-event-needle from one transcript"
        );
        assert!(result.why_matched.iter().any(|why| why == "message"));
        assert!(result.why_matched.iter().any(|why| why == "role:assistant"));
        assert!(result.citations.iter().any(|citation| {
            citation.citation_type == ContextCitationType::Event
                && citation.id == target_event_id
                && citation.cursor.as_deref() == Some("line:1024")
        }));

        let event_packet = search_packet(
            &store,
            "large-fast-event-needle",
            &PacketOptions {
                limit: 5,
                snippet_chars: 200,
                result_mode: SearchResultMode::Events,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(event_packet.results.len(), 1);
        assert_eq!(
            event_packet.results[0].result_scope,
            SearchResultScope::Event
        );
        assert_eq!(event_packet.results[0].event_id, Some(target_event_id));

        let truncated_fast = search_packet(
            &store,
            "ordinary large history event",
            &PacketOptions {
                limit: 1,
                result_mode: SearchResultMode::Events,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(truncated_fast.truncation.truncated);
        assert_eq!(truncated_fast.truncation.reason.as_deref(), Some("limit"));
        assert_eq!(truncated_fast.truncation.omitted_results, 1);
        assert!(truncated_fast.truncation.omitted_results_is_lower_bound);
    }

    #[test]
    fn clustered_fast_search_pages_past_dominant_first_session() {
        let (_temp, store) = test_store();
        let dominant_record = HistoryRecord::new(
            "Dominant matching session",
            "dominant record",
            Vec::new(),
            "agent_history",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&dominant_record).unwrap();
        let dominant_session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000701").unwrap(),
            history_record_id: Some(dominant_record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("dominant-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&dominant_session).unwrap();

        let later_record = HistoryRecord::new(
            "Later matching session",
            "later record",
            Vec::new(),
            "agent_history",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&later_record).unwrap();
        let later_session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000702").unwrap(),
            history_record_id: Some(later_record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("later-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&later_session).unwrap();

        for index in 0..=(LARGE_EVENT_CORPUS_THRESHOLD as u64) {
            let (record_id, session_id, text, occurred_at) = if index < 600 {
                (
                    dominant_record.id,
                    dominant_session.id,
                    "cluster-paging-needle dominant hit",
                    fixed_time() + chrono::Duration::milliseconds(2_000 - index as i64),
                )
            } else if index == 600 {
                (
                    later_record.id,
                    later_session.id,
                    "cluster-paging-needle later hit",
                    fixed_time(),
                )
            } else {
                (
                    dominant_record.id,
                    dominant_session.id,
                    "ordinary large history event",
                    fixed_time() - chrono::Duration::milliseconds(index as i64),
                )
            };
            store
                .upsert_event(&Event {
                    id: Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000001{index:05x}"))
                        .unwrap(),
                    seq: 20_000 + index,
                    history_record_id: Some(record_id),
                    session_id: Some(session_id),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at,
                    capture_source_id: None,
                    payload: serde_json::json!({
                        "cursor": format!("line:{index}"),
                        "body": { "text": text }
                    }),
                    payload_blob_id: None,
                    dedupe_key: Some(format!("clustered-paging-{index}")),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();

        let packet = search_packet(
            &store,
            "cluster-paging-needle",
            &PacketOptions {
                limit: 2,
                snippet_chars: 200,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        let sessions = packet
            .results
            .iter()
            .filter_map(|result| result.session_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(packet.results.len(), 2);
        assert!(sessions.contains(&dominant_session.id));
        assert!(sessions.contains(&later_session.id));
        assert!(!packet.truncation.truncated);
    }

    #[test]
    fn search_filters_and_citations_expose_source_metadata() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Source-backed session",
            "ordinary body",
            Vec::new(),
            "session",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&record).unwrap();

        let source_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000000401").unwrap();
        let source = CaptureSource {
            id: source_id,
            descriptor: CaptureSourceDescriptor {
                kind: CaptureSourceKind::ProviderImport,
                provider: CaptureProvider::Codex,
                machine_id: "machine-1".into(),
                process_id: None,
                cwd: Some("/workspace/ctx".into()),
                raw_source_path: Some("/definitely/missing/source-filter.jsonl".into()),
                external_session_id: Some("source-filter-session".into()),
            },
            started_at: fixed_time(),
            ended_at: None,
            sync: SyncMetadata {
                metadata: serde_json::json!({
                    "source_format": "codex_session_jsonl",
                    "cursor": {
                        "after": {
                            "stream": "provider:codex:codex_session_jsonl",
                            "cursor": "line:8",
                            "observed_at": "2026-06-23T12:00:00Z"
                        }
                    }
                }),
                ..sync_metadata()
            },
        };
        store.upsert_capture_source(&source).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000402").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: Some(source_id),
            provider: CaptureProvider::Codex,
            external_session_id: Some("source-filter-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000403").unwrap(),
            seq: 401,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::ToolCall,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: Some(source_id),
            payload: serde_json::json!({
                "cursor": "line:8",
                "body": {
                    "tool": "shell",
                    "name": "exec_command",
                    "arguments_preview": "source-filter-needle"
                }
            }),
            payload_blob_id: None,
            dedupe_key: Some("source-filter-event".into()),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();

        let file = FileTouched {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000404").unwrap(),
            history_record_id: Some(record.id),
            run_id: None,
            event_id: Some(event.id),
            vcs_workspace_id: None,
            path: "crates/search/src/source_filter.rs".into(),
            change_kind: Some(FileChangeKind::Modified),
            old_path: None,
            line_count_delta: Some(1),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: Some(source_id),
            sync: sync_metadata(),
        };
        store.upsert_file_touched(&file).unwrap();
        store.upsert_record(&record).unwrap();

        let packet = search_packet(
            &store,
            "source-filter-needle",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    repo: Some("ctx".into()),
                    event_type: Some(EventType::ToolCall),
                    file: Some("source_filter.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), 1);
        let result = &packet.results[0];
        assert_eq!(result.provider, Some(CaptureProvider::Codex));
        assert_eq!(result.session_id, Some(session.id));
        assert_eq!(result.event_id, Some(event.id));
        assert_eq!(result.event_seq, Some(401));
        assert_eq!(
            result.raw_source_path.as_deref(),
            source.descriptor.raw_source_path.as_deref()
        );
        assert_eq!(result.raw_source_exists, Some(false));
        assert_eq!(result.cursor.as_deref(), Some("line:8"));
        assert!(result.citations.iter().any(|citation| {
            citation.citation_type == ContextCitationType::Event
                && citation.raw_source_path.as_deref()
                    == source.descriptor.raw_source_path.as_deref()
                && citation.raw_source_exists == Some(false)
                && citation.cursor.as_deref() == Some("line:8")
        }));

        let file_only = search_packet(
            &store,
            "",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    file: Some("source_filter.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(file_only.results.len(), 1);
        assert!(file_only.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "file_touched"));
        assert!(!file_only.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "recent_activity"));
        assert!(file_only.results[0].citations.iter().any(|citation| {
            citation.citation_type == ContextCitationType::File && citation.id == file.id
        }));

        let wrong_provider = search_packet(
            &store,
            "source-filter-needle",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Pi),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(wrong_provider.results.is_empty());
    }

    #[test]
    fn search_filters_custom_history_source_identity() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Custom plugin import",
            "ordinary body",
            Vec::new(),
            "session",
            Some("/workspace/custom".into()),
        );
        store.insert_record(&record).unwrap();

        let source_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000000451").unwrap();
        let source = CaptureSource {
            id: source_id,
            descriptor: CaptureSourceDescriptor {
                kind: CaptureSourceKind::ProviderImport,
                provider: CaptureProvider::Custom,
                machine_id: "machine-1".into(),
                process_id: None,
                cwd: Some("/workspace/custom".into()),
                raw_source_path: Some("/tmp/dorkos-plugin/ctx-history-plugin.json".into()),
                external_session_id: Some("ctx-history-jsonl-v1-session".into()),
            },
            started_at: fixed_time(),
            ended_at: None,
            sync: SyncMetadata {
                metadata: serde_json::json!({
                    "ctx_history_plugin": {
                        "plugin_name": "dorkos",
                        "plugin_source_id": "default",
                        "history_source": "dorkos/default"
                    },
                    "ctx_history_jsonl_v1": {
                        "provider_key": "dorkos",
                        "source_id": "default",
                        "source_format": "dorkos-history-v1"
                    }
                }),
                ..sync_metadata()
            },
        };
        store.upsert_capture_source(&source).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000452").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: Some(source_id),
            provider: CaptureProvider::Custom,
            external_session_id: Some("ctx-history-jsonl-v1-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000453").unwrap(),
            seq: 451,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: Some(source_id),
            payload: serde_json::json!({
                "body": {
                    "text": "dorkos-source-filter-needle"
                }
            }),
            payload_blob_id: None,
            dedupe_key: Some("custom-history-source-filter-event".into()),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();
        store.upsert_record(&record).unwrap();

        // Call the fallback selector directly so this remains a targeted
        // hydration regression even when the event fast path supports the
        // same source filter.
        let before = store.relation_batch_executions();
        let fallback_options = PacketOptions {
            limit: 10,
            filters: SearchFilters {
                history_source: Some("dorkos/default".into()),
                ..SearchFilters::default()
            },
            ..PacketOptions::default()
        };
        let fallback_plan =
            SearchQueryPlan::new(fallback_options.match_mode, ["dorkos-source-filter-needle"]);
        let fallback = ranked_candidates(
            &store,
            Some(&fallback_plan),
            &fallback_options,
            None,
            HydrationIntent::Full,
        )
        .unwrap();
        assert_eq!(fallback.candidates.len(), 1);
        assert_eq!(store.relation_batch_executions() - before, 8);

        let packet = search_packet(
            &store,
            "dorkos-source-filter-needle",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Custom),
                    history_source: Some("dorkos/default".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), 1);
        let result = &packet.results[0];
        assert_eq!(result.provider, Some(CaptureProvider::Custom));
        assert_eq!(result.history_source.as_deref(), Some("dorkos/default"));
        assert_eq!(result.history_source_plugin.as_deref(), Some("dorkos"));
        assert_eq!(result.provider_key.as_deref(), Some("dorkos"));
        assert_eq!(result.source_id.as_deref(), Some("default"));
        assert_eq!(result.source_format.as_deref(), Some("dorkos-history-v1"));

        let provider_source_packet = search_packet(
            &store,
            "dorkos-source-filter-needle",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Custom),
                    provider_key: Some("dorkos".into()),
                    source_id: Some("default".into()),
                    source_format: Some("dorkos-history-v1".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert_eq!(provider_source_packet.results.len(), 1);

        let wrong_source = search_packet(
            &store,
            "dorkos-source-filter-needle",
            &PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Custom),
                    history_source: Some("openclaw/default".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(wrong_source.results.is_empty());
    }

    #[test]
    fn fast_event_search_exposes_custom_history_source_identity() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Large custom plugin import",
            "ordinary body",
            Vec::new(),
            "agent_history",
            Some("/workspace/custom".into()),
        );
        store.insert_record(&record).unwrap();

        let source_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000000481").unwrap();
        store
            .upsert_capture_source(&CaptureSource {
                id: source_id,
                descriptor: CaptureSourceDescriptor {
                    kind: CaptureSourceKind::ProviderImport,
                    provider: CaptureProvider::Custom,
                    machine_id: "machine-1".into(),
                    process_id: None,
                    cwd: Some("/workspace/custom".into()),
                    raw_source_path: Some("/tmp/large-dorkos/ctx-history-plugin.json".into()),
                    external_session_id: Some("ctx-history-jsonl-v1-large".into()),
                },
                started_at: fixed_time(),
                ended_at: None,
                sync: SyncMetadata {
                    metadata: serde_json::json!({
                        "source_metadata": {
                            "ctx_history_plugin": {
                                "plugin_name": "dorkos",
                                "plugin_source_id": "default",
                                "history_source": "dorkos/default"
                            },
                            "ctx_history_jsonl_v1": {
                                "provider_key": "dorkos",
                                "source_id": "default",
                                "source_format": "dorkos-history-v1"
                            }
                        }
                    }),
                    ..sync_metadata()
                },
            })
            .unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000482").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: Some(source_id),
            provider: CaptureProvider::Custom,
            external_session_id: Some("ctx-history-jsonl-v1-large".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let target_event_id = Uuid::parse_str("018f45d0-0000-7000-8000-000000000483").unwrap();
        for index in 0..=(LARGE_EVENT_CORPUS_THRESHOLD as u64) {
            let event_id = if index == LARGE_EVENT_CORPUS_THRESHOLD as u64 {
                target_event_id
            } else {
                Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000002{index:05x}")).unwrap()
            };
            let text = if event_id == target_event_id {
                "large-custom-source-identity-needle"
            } else {
                "ordinary large custom event"
            };
            store
                .upsert_event(&Event {
                    id: event_id,
                    seq: 40_000 + index,
                    history_record_id: Some(record.id),
                    session_id: Some(session.id),
                    run_id: None,
                    event_type: EventType::Message,
                    role: Some(EventRole::Assistant),
                    occurred_at: fixed_time() + chrono::Duration::milliseconds(index as i64),
                    capture_source_id: Some(source_id),
                    payload: serde_json::json!({
                        "body": { "text": text }
                    }),
                    payload_blob_id: None,
                    dedupe_key: Some(format!("large-custom-source-identity-{index}")),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                })
                .unwrap();
        }
        store.refresh_search_index().unwrap();

        let packet = search_packet(
            &store,
            "large-custom-source-identity-needle",
            &PacketOptions {
                limit: 5,
                filters: SearchFilters {
                    provider: Some(CaptureProvider::Custom),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), 1);
        let result = &packet.results[0];
        assert_eq!(result.event_id, Some(target_event_id));
        assert_eq!(result.history_source.as_deref(), Some("dorkos/default"));
        assert_eq!(result.history_source_plugin.as_deref(), Some("dorkos"));
        assert_eq!(result.provider_key.as_deref(), Some("dorkos"));
        assert_eq!(result.source_id.as_deref(), Some("default"));
        assert_eq!(result.source_format.as_deref(), Some("dorkos-history-v1"));
    }

    #[test]
    fn filtered_search_pages_past_fts_decoys() {
        let (_temp, store) = test_store();
        let query = "overflow-filter-needle";
        let old_time = fixed_time() - chrono::Duration::days(14);
        let mut records = Vec::new();

        for index in 0..501_u16 {
            let mut decoy = HistoryRecord::new(
                "Overflow filter shared title",
                format!("{query} identical body for paging regression"),
                Vec::new(),
                "task",
                None,
            );
            decoy.id = Uuid::parse_str(&format!("018f45d0-0000-7000-8000-{index:012x}")).unwrap();
            decoy.created_at = old_time;
            decoy.updated_at = old_time;
            records.push(decoy);
        }

        let mut target = HistoryRecord::new(
            "Overflow filter shared title",
            format!("{query} identical body for paging regression"),
            Vec::new(),
            "task",
            Some("/workspace/ctx-filter-target".into()),
        );
        target.id = Uuid::parse_str("018f45d0-0000-7000-8000-ffffffffffff").unwrap();
        target.created_at = old_time;
        target.updated_at = fixed_time();
        records.push(target.clone());
        store.upsert_records(&records).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-fffffffffffe").unwrap(),
            history_record_id: Some(target.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("overflow-filter-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let file = FileTouched {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-fffffffffffd").unwrap(),
            history_record_id: Some(target.id),
            run_id: None,
            event_id: None,
            vcs_workspace_id: None,
            path: "crates/search/src/overflow_filter.rs".into(),
            change_kind: Some(FileChangeKind::Modified),
            old_path: None,
            line_count_delta: Some(3),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_file_touched(&file).unwrap();

        let first_raw_page = store.search_records(query, 500).unwrap();
        assert_eq!(first_raw_page.len(), 500);
        assert!(
            !first_raw_page.iter().any(|record| record.id == target.id),
            "regression setup must place the filtered hit behind the first 500 raw matches"
        );

        let cases = vec![
            (
                "provider",
                SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    ..SearchFilters::default()
                },
            ),
            (
                "repo",
                SearchFilters {
                    repo: Some("ctx-filter-target".into()),
                    ..SearchFilters::default()
                },
            ),
            (
                "file",
                SearchFilters {
                    file: Some("overflow_filter.rs".into()),
                    ..SearchFilters::default()
                },
            ),
            (
                "since",
                SearchFilters {
                    since: Some(fixed_time() - chrono::Duration::hours(1)),
                    ..SearchFilters::default()
                },
            ),
            (
                "combined",
                SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    repo: Some("ctx-filter-target".into()),
                    since: Some(fixed_time() - chrono::Duration::hours(1)),
                    file: Some("overflow_filter.rs".into()),
                    ..SearchFilters::default()
                },
            ),
        ];

        for (name, filters) in cases {
            let packet = search_packet(
                &store,
                query,
                &PacketOptions {
                    limit: 1,
                    filters,
                    ..PacketOptions::default()
                },
            )
            .unwrap();

            assert_eq!(
                packet
                    .results
                    .iter()
                    .map(|result| result.record_id)
                    .collect::<Vec<_>>(),
                vec![target.id],
                "{name} filter failed to page past decoys"
            );
        }
    }

    #[test]
    fn file_filter_matches_event_linked_file_touches_on_fast_path() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Event linked file touch",
            "record body without the event needle",
            Vec::new(),
            "task",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&record).unwrap();

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-00000000f101").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("event-linked-file-touch-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();

        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-00000000f102").unwrap(),
            seq: 7,
            history_record_id: None,
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::ToolCall,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({"text": "event-file-scope-needle apply patch"}),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();
        for index in 0..(LARGE_EVENT_CORPUS_THRESHOLD - 1) {
            let decoy = Event {
                id: Uuid::parse_str(&format!("018f45d0-0000-7000-8000-00000001{index:04x}"))
                    .unwrap(),
                seq: 1000 + index as u64,
                history_record_id: None,
                session_id: Some(session.id),
                run_id: None,
                event_type: EventType::Message,
                role: Some(EventRole::Assistant),
                occurred_at: fixed_time() + chrono::Duration::milliseconds(index),
                capture_source_id: None,
                payload: serde_json::json!({"text": format!("decoy event {index}")}),
                payload_blob_id: None,
                dedupe_key: None,
                redaction_state: RedactionState::SafePreview,
                sync: sync_metadata(),
            };
            store.upsert_event(&decoy).unwrap();
        }

        store
            .upsert_file_touched(&FileTouched {
                id: Uuid::parse_str("018f45d0-0000-7000-8000-00000000f103").unwrap(),
                history_record_id: None,
                run_id: None,
                event_id: Some(event.id),
                vcs_workspace_id: None,
                path: "crates/ctx-cli/src/main.rs".into(),
                change_kind: Some(FileChangeKind::Modified),
                old_path: None,
                line_count_delta: None,
                confidence: Confidence::Explicit,
                timestamps: timestamps(),
                source_id: None,
                sync: sync_metadata(),
            })
            .unwrap();

        let packet = search_packet(
            &store,
            "event-file-scope-needle",
            &PacketOptions {
                limit: 5,
                filters: SearchFilters {
                    file: Some("src/main.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), 1);
        assert_eq!(packet.results[0].event_id, Some(event.id));
        assert_eq!(packet.results[0].result_scope, SearchResultScope::Session);

        let wrong_file = search_packet(
            &store,
            "event-file-scope-needle",
            &PacketOptions {
                limit: 5,
                filters: SearchFilters {
                    file: Some("src/lib.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(wrong_file.results.is_empty());
    }

    #[test]
    fn file_filter_treats_like_wildcards_as_literal_path_characters() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "Literal file wildcard test",
            "literal-file-wildcard-needle",
            Vec::new(),
            "task",
            Some("/workspace/ctx".into()),
        );
        store.insert_record(&record).unwrap();
        store
            .upsert_file_touched(&FileTouched {
                id: Uuid::parse_str("018f45d0-0000-7000-8000-00000000f203").unwrap(),
                history_record_id: Some(record.id),
                run_id: None,
                event_id: None,
                vcs_workspace_id: None,
                path: "src/fooXbar.rs".into(),
                change_kind: Some(FileChangeKind::Modified),
                old_path: None,
                line_count_delta: None,
                confidence: Confidence::Explicit,
                timestamps: timestamps(),
                source_id: None,
                sync: sync_metadata(),
            })
            .unwrap();

        let packet = search_packet(
            &store,
            "literal-file-wildcard-needle",
            &PacketOptions {
                limit: 5,
                filters: SearchFilters {
                    file: Some("src/foo_bar.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert!(packet.results.is_empty());
    }

    #[test]
    fn file_only_search_finds_old_sparse_file_touch_beyond_recent_scan_budget() {
        let (_temp, store) = test_store();
        let old_time = fixed_time() - chrono::Duration::days(30);
        let target_id = Uuid::parse_str("018f45d0-0000-7000-8003-ffffffffffff").unwrap();
        let mut target = HistoryRecord::new(
            "Old sparse file touch",
            "older session that only relates through file touch scope",
            Vec::new(),
            "task",
            Some("/workspace/ctx".into()),
        );
        target.id = target_id;
        target.created_at = old_time;
        target.updated_at = old_time;
        store.upsert_record(&target).unwrap();
        store
            .upsert_file_touched(&FileTouched {
                id: Uuid::parse_str("018f45d0-0000-7000-8003-fffffffffffe").unwrap(),
                history_record_id: Some(target_id),
                run_id: None,
                event_id: None,
                vcs_workspace_id: None,
                path: "crates/ctx-history-search/src/sparse_history.rs".into(),
                change_kind: Some(FileChangeKind::Modified),
                old_path: None,
                line_count_delta: Some(1),
                confidence: Confidence::Explicit,
                timestamps: EntityTimestamps {
                    created_at: old_time,
                    updated_at: old_time,
                },
                source_id: None,
                sync: sync_metadata(),
            })
            .unwrap();

        let mut decoys = Vec::new();
        for index in 0..=(FILTERED_SEARCH_PAGE_SIZE * FILTERED_SEARCH_MAX_PAGES) {
            let decoy_time = fixed_time() + chrono::Duration::seconds(index as i64);
            let mut decoy = HistoryRecord::new(
                "Recent unrelated session",
                format!("recent non-file decoy {index:05}"),
                Vec::new(),
                "task",
                Some("/workspace/other".into()),
            );
            decoy.id = Uuid::parse_str(&format!("018f45d0-0000-7000-8004-{index:012x}")).unwrap();
            decoy.created_at = decoy_time;
            decoy.updated_at = decoy_time;
            decoys.push(decoy);
        }
        store.upsert_records(&decoys).unwrap();

        let old_scan_window = store
            .list_records_page(FILTERED_SEARCH_PAGE_SIZE * FILTERED_SEARCH_MAX_PAGES, 0)
            .unwrap();
        assert!(
            !old_scan_window.iter().any(|record| record.id == target_id),
            "regression setup must place the file match beyond the old recent-record scan window"
        );

        let packet = search_packet(
            &store,
            "",
            &PacketOptions {
                limit: 5,
                filters: SearchFilters {
                    file: Some("sparse_history.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            packet
                .results
                .iter()
                .map(|result| result.record_id)
                .collect::<Vec<_>>(),
            vec![target_id]
        );
        assert!(!packet.truncation.truncated);
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "file_touched"));
    }

    #[test]
    fn search_ignores_agent_history_bookkeeping_terms_without_content_evidence() {
        let (_temp, store) = test_store();
        let mut record = HistoryRecord::new(
            "codex agent history",
            "Indexed local agent history from /tmp/codex/sessions.jsonl (codex_session_jsonl)",
            vec!["agent-history".into(), "codex".into()],
            "agent_history",
            Some("/tmp/codex".into()),
        );
        record.id = Uuid::parse_str("018f45d0-0000-7000-8005-000000000001").unwrap();
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        store.upsert_record(&record).unwrap();

        for query in [
            "Indexed local agent history",
            "agent-history",
            "codex_session_jsonl",
        ] {
            let packet = search_packet(&store, query, &PacketOptions::default()).unwrap();
            assert!(
                packet.results.is_empty(),
                "bookkeeping-only query {query:?} returned {:?}",
                packet.results
            );
        }

        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8005-000000000002").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("bookkeeping-content-session".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("primary".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let event = Event {
            id: Uuid::parse_str("018f45d0-0000-7000-8005-000000000003").unwrap(),
            seq: 1,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({
                "text": "actual agent-history session evidence"
            }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.upsert_event(&event).unwrap();

        let packet = search_packet(&store, "agent-history", &PacketOptions::default()).unwrap();
        assert_eq!(packet.results.len(), 1);
        assert_eq!(packet.results[0].event_id, Some(event.id));
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "message"));
        assert!(!packet.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "title" || reason == "tag"));
    }

    #[test]
    fn filtered_search_stops_at_scan_budget_when_no_candidates_match() {
        let (_temp, store) = test_store();
        let query = "scan-budget-needle";
        let mut records = Vec::new();
        for index in 0..=(FILTERED_SEARCH_PAGE_SIZE * FILTERED_SEARCH_MAX_PAGES) {
            let mut record = HistoryRecord::new(
                "Scan budget decoy",
                format!("{query} decoy record {index:05}"),
                Vec::new(),
                "task",
                Some("/workspace/no-match".into()),
            );
            record.id = Uuid::parse_str(&format!("018f45d0-0000-7000-8000-{index:012x}")).unwrap();
            record.created_at = fixed_time() - chrono::Duration::seconds(index as i64);
            record.updated_at = record.created_at;
            records.push(record);
        }
        store.upsert_records(&records).unwrap();

        let packet = search_packet(
            &store,
            query,
            &PacketOptions {
                limit: 1,
                filters: SearchFilters {
                    repo: Some("workspace-that-does-not-exist".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert!(packet.results.is_empty());
        assert!(packet.truncation.truncated);
        assert_eq!(packet.truncation.reason.as_deref(), Some("scan_budget"));
    }

    #[test]
    fn empty_query_filtered_search_returns_empty_without_scanning() {
        let (_temp, store) = test_store();
        let mut records = Vec::new();
        for index in 0..=(FILTERED_SEARCH_PAGE_SIZE * FILTERED_SEARCH_MAX_PAGES) {
            let mut record = HistoryRecord::new(
                "Empty query scan budget decoy",
                format!("empty query decoy record {index:05}"),
                Vec::new(),
                "task",
                Some("/workspace/no-match".into()),
            );
            record.id = Uuid::parse_str(&format!("018f45d0-0000-7000-8001-{index:012x}")).unwrap();
            record.created_at = fixed_time() - chrono::Duration::seconds(index as i64);
            record.updated_at = record.created_at;
            records.push(record);
        }
        store.upsert_records(&records).unwrap();

        let packet = search_packet(
            &store,
            "",
            &PacketOptions {
                limit: 1,
                filters: SearchFilters {
                    repo: Some("workspace-that-does-not-exist".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert!(packet.results.is_empty());
        assert!(!packet.truncation.truncated);
        assert_eq!(packet.truncation.reason.as_deref(), None);
    }

    #[test]
    fn no_token_query_returns_empty_without_recent_activity() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "No-token query decoy",
            "This record should not be returned for punctuation-only search.",
            Vec::new(),
            "task",
            Some("/workspace/punctuation".into()),
        );
        store.upsert_record(&record).unwrap();

        for query in ["!!!", "---", "___"] {
            let packet =
                search_packet(&store, query, &PacketOptions::default()).expect("search packet");

            assert!(packet.results.is_empty(), "{query}");
            assert!(!packet.truncation.truncated, "{query}");
        }
    }

    #[test]
    fn search_result_limit_is_capped() {
        let (_temp, store) = test_store();
        let query = "limit-cap-needle";
        let mut records = Vec::new();
        for index in 0..250_usize {
            let mut record = HistoryRecord::new(
                "Limit cap candidate",
                format!("{query} candidate {index:03}"),
                Vec::new(),
                "task",
                Some("/workspace/limit-cap".into()),
            );
            record.id = Uuid::parse_str(&format!("018f45d0-0000-7000-8002-{index:012x}")).unwrap();
            record.created_at = fixed_time() - chrono::Duration::seconds(index as i64);
            record.updated_at = fixed_time() - chrono::Duration::seconds(index as i64);
            records.push(record);
        }
        store.upsert_records(&records).unwrap();

        let packet = search_packet(
            &store,
            query,
            &PacketOptions {
                limit: usize::MAX,
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(packet.results.len(), MAX_RESULT_LIMIT);
        assert!(packet.truncation.truncated);
        assert_eq!(packet.truncation.reason.as_deref(), Some("limit"));
    }

    #[test]
    fn repeated_query_requests_are_bounded_before_deduplication() {
        assert!(
            validate_query_request("q", &vec!["duplicate".to_owned(); MAX_QUERY_CLAUSES]).is_ok()
        );
        assert!(matches!(
            validate_query_request("q", &vec!["duplicate".to_owned(); MAX_QUERY_CLAUSES + 1]),
            Err(SearchError::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_query_request(&"x".repeat(MAX_QUERY_CLAUSE_BYTES + 1), &[]),
            Err(SearchError::InvalidRequest(_))
        ));
        let aggregate_terms = vec!["x".repeat(MAX_QUERY_CLAUSE_BYTES); MAX_QUERY_CLAUSES];
        assert!(matches!(
            validate_query_request("q", &aggregate_terms),
            Err(SearchError::InvalidRequest(_))
        ));
    }

    #[test]
    fn filtered_search_scores_full_fetched_page_before_limiting() {
        let (_temp, store) = test_store();
        let query = "samepagerankneedle";
        let workspace = Some("/workspace/same-page-rank".to_owned());
        let mut records = Vec::new();

        for (index, id) in [
            "018f45d0-0000-7000-8000-000000000101",
            "018f45d0-0000-7000-8000-000000000102",
            "018f45d0-0000-7000-8000-000000000103",
        ]
        .into_iter()
        .enumerate()
        {
            let mut record = HistoryRecord::new(
                "Same page filtered candidate",
                format!("{query} identical body for same page ranking"),
                Vec::new(),
                "task",
                workspace.clone(),
            );
            record.id = Uuid::parse_str(id).unwrap();
            record.created_at = fixed_time();
            record.updated_at = fixed_time() + chrono::Duration::seconds(index as i64);
            records.push(record);
        }

        let expected_best_id = records[2].id;
        store.upsert_records(&records).unwrap();

        let late_file_match = FileTouched {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-000000000104").unwrap(),
            history_record_id: Some(expected_best_id),
            run_id: None,
            event_id: None,
            vcs_workspace_id: None,
            path: "crates/search/src/samepagerankneedle.rs".into(),
            change_kind: Some(FileChangeKind::Modified),
            old_path: None,
            line_count_delta: Some(1),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        };
        store.upsert_file_touched(&late_file_match).unwrap();

        let raw_page = store.search_records(query, 3).unwrap();
        assert_eq!(
            raw_page.iter().map(|record| record.id).collect::<Vec<_>>(),
            records.iter().map(|record| record.id).collect::<Vec<_>>(),
            "regression setup must put the best filtered hit after the first limit+1 raw matches"
        );

        let packet = search_packet(
            &store,
            query,
            &PacketOptions {
                limit: 1,
                filters: SearchFilters {
                    repo: Some("same-page-rank".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            packet
                .results
                .iter()
                .map(|result| result.record_id)
                .collect::<Vec<_>>(),
            vec![expected_best_id]
        );
        assert!(packet.results[0]
            .why_matched
            .iter()
            .any(|reason| reason == "file_touched"));
    }

    fn new_link_id(target_id: Uuid) -> Uuid {
        let mut bytes = *target_id.as_bytes();
        bytes[15] = bytes[15].wrapping_add(80);
        Uuid::from_bytes(bytes)
    }

    fn deterministic_tie_record(id: &str) -> HistoryRecord {
        let mut record = HistoryRecord::new(
            "Stable tie title",
            "stabletie exact equal body for deterministic ranking",
            vec!["stabletie".into()],
            "task",
            None,
        );
        record.id = Uuid::parse_str(id).unwrap();
        record.created_at = fixed_time();
        record.updated_at = fixed_time();
        record
    }

    fn packet_without_generated_at<T: Serialize>(packet: &T) -> serde_json::Value {
        let mut value = serde_json::to_value(packet).unwrap();
        value.as_object_mut().unwrap().remove("generated_at");
        value
    }

    #[test]
    fn search_packet_terms_merges_broad_queries_without_requiring_all_terms() {
        let (_temp, store) = test_store();
        for (id, title, body) in [
            (
                "018f45d0-0000-7000-8000-000000020001",
                "Signed metadata release",
                "signed metadata verification and trusted release manifests",
            ),
            (
                "018f45d0-0000-7000-8000-000000020002",
                "Buildkite worker setup",
                "buildkite pipeline worker provisioning and release queue setup",
            ),
        ] {
            let mut record = HistoryRecord::new(title, body, Vec::new(), "task", None);
            record.id = Uuid::parse_str(id).unwrap();
            record.created_at = fixed_time();
            record.updated_at = fixed_time();
            store.insert_record(&record).unwrap();
        }
        let options = PacketOptions {
            limit: 10,
            snippet_chars: 160,
            ..PacketOptions::default()
        };

        let exact = search_packet(&store, "signed metadata buildkite", &options).unwrap();
        assert_eq!(exact.results.len(), 0);

        let broad = search_packet_terms(
            &store,
            "signed metadata",
            &[String::from("buildkite")],
            &options,
        )
        .unwrap();
        let titles = broad
            .results
            .iter()
            .map(|result| result.title.as_str())
            .collect::<Vec<_>>();
        assert!(titles.contains(&"Signed metadata release"));
        assert!(titles.contains(&"Buildkite worker setup"));
        assert_eq!(broad.query, "signed metadata OR buildkite");
    }

    fn maybe_write_synthetic_search_smoke_artifact() {
        let Ok(out_dir) = std::env::var("CTX_ARTIFACT_DIR") else {
            return;
        };

        let (_temp, store) = test_store();
        let mut records = Vec::new();
        for index in 0..48 {
            let mut record = HistoryRecord::new(
                format!("Synthetic search smoke {index:03}"),
                format!(
                    "syntheticneedle generated body {index:03} {}",
                    "detail ".repeat(12)
                ),
                vec!["synthetic".into(), "smoke".into()],
                "task",
                Some("/workspace/ctx".into()),
            );
            record.id =
                Uuid::parse_str(&format!("018f45d0-0000-7000-8000-00000002{index:04x}")).unwrap();
            record.created_at = fixed_time() + chrono::Duration::seconds(index);
            record.updated_at = record.created_at;
            records.push(record);
        }

        let import_started = std::time::Instant::now();
        store.upsert_records(&records).unwrap();
        let import_elapsed = import_started.elapsed();

        let options = PacketOptions {
            limit: 12,
            snippet_chars: 180,
            filters: SearchFilters::default(),
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        };
        let search_started = std::time::Instant::now();
        let search = search_packet(&store, "syntheticneedle", &options).unwrap();
        let search_elapsed = search_started.elapsed();

        let import_secs = import_elapsed.as_secs_f64();
        let artifact = serde_json::json!({
            "schema_version": 1,
            "profile": "smoke",
            "corpus": {
                "records": records.len(),
                "events": records.len()
            },
            "import": {
                "duration_ms": import_elapsed.as_millis(),
                "events_per_sec": if import_secs > 0.0 {
                    records.len() as f64 / import_secs
                } else {
                    records.len() as f64
                }
            },
            "storage": {
                "db_bytes": std::fs::metadata(store.path()).map(|metadata| metadata.len()).unwrap_or(0)
            },
            "search": {
                "duration_ms": search_elapsed.as_millis(),
                "result_count": search.results.len(),
                "citation_count": search.results.iter().map(|result| result.citations.len()).sum::<usize>(),
                "truncation": search.truncation
            }
        });

        let out_dir = std::path::Path::new(&out_dir);
        std::fs::create_dir_all(out_dir).unwrap();
        std::fs::write(
            out_dir.join("synthetic-search-smoke.json"),
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn streaming_large_profile_smoke_writes_stable_artifact() {
        let temp = tempdir();
        let cfg = LargeProfileConfig::smoke(temp.path().join("profile-out-a"));
        let first = run_streaming_large_profile(&cfg).unwrap();
        let second = run_streaming_large_profile(&LargeProfileConfig::smoke(
            temp.path().join("profile-out-b"),
        ))
        .unwrap();
        let first_typed: LargeProfileArtifactV1 = serde_json::from_value(first.clone()).unwrap();
        let second_typed: LargeProfileArtifactV1 = serde_json::from_value(second.clone()).unwrap();
        assert_eq!(
            first_typed.stable_projection(),
            second_typed.stable_projection()
        );
        assert_eq!(first["schema_version"], 2);
        assert_eq!(first["profile"], "ctx-large-index-profile");
        assert_eq!(first["config"], second["config"]);
        assert_eq!(first["achieved"], second["achieved"]);
        assert_eq!(first["counts"], second["counts"]);
        assert_eq!(
            first["reopen"]["ordered_result_ids"],
            second["reopen"]["ordered_result_ids"]
        );
        assert_eq!(
            first["reopen"]["result_digest"],
            second["reopen"]["result_digest"]
        );
        assert_eq!(first["measurements"]["noop_counts_unchanged"], true);
        assert_eq!(first["event_window"]["contains_target"], true);
        assert!(first["checkpoint"]["post"].is_object());
        assert!(first["rss"]["peak_bytes"].as_u64().is_some());
        assert_eq!(first["achieved"]["baseline_events"], 21);
        assert_eq!(first["achieved"]["incremental_events"], 3);
        assert_eq!(first["counts"]["events"], 24);
        assert_eq!(first["counts"]["records"], 8);
        assert_eq!(first["counts"]["sessions"], 8);
        assert_eq!(first["counts"]["runs"], 8);
        assert_eq!(first["counts"]["summaries"], 8);
        assert_eq!(first["counts"]["files_touched"], 8);
        assert_eq!(first["counts"]["record_fts"], 8);
        assert_eq!(first["counts"]["event_fts"], 24);
        assert!(first["search"]["ordinary_result_count"].as_u64().unwrap() > 0);
        assert!(first["search"]["filtered_result_count"].as_u64().unwrap() > 0);
        assert!(first["paths"]["db"]
            .as_str()
            .unwrap()
            .starts_with(temp.path().to_str().unwrap()));
        assert!(first["generation"]["max_batch_events"].as_u64().unwrap() <= 7);
        let parsed: serde_json::Value = serde_json::from_slice(
            &std::fs::read(first["paths"]["artifact"].as_str().unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(parsed["counts"], first["counts"]);
    }

    #[test]
    fn streaming_large_profile_batch_size_invariant_and_partial_batch() {
        let temp = tempdir();
        let mut a = LargeProfileConfig::smoke(temp.path().join("a"));
        a.total_events = 10;
        a.events_per_record = 3;
        a.batch_records = 1;
        let mut b = a.clone();
        b.output_dir = Some(temp.path().join("b"));
        b.batch_records = 4;
        let first = run_streaming_large_profile(&a).unwrap();
        let second = run_streaming_large_profile(&b).unwrap();
        assert_eq!(first["achieved"], second["achieved"]);
        assert_eq!(
            first["reopen"]["ordered_result_ids"],
            second["reopen"]["ordered_result_ids"]
        );
        assert_eq!(first["counts"]["events"], 13);
    }

    #[test]
    fn streaming_large_profile_records_requested_warm_samples() {
        let temp = tempdir();
        let mut cfg = LargeProfileConfig::smoke(temp.path().join("repeated"));
        cfg.measurement_repeats = 5;
        let artifact = run_streaming_large_profile(&cfg).unwrap();
        assert_eq!(artifact["config"]["measurement_repeats"], 5);
        for name in ["noop_replay", "warm_search", "filtered_search"] {
            assert_eq!(artifact["measurements"][name]["sample_count"], 5);
            assert_eq!(
                artifact["measurements"][name]["samples_ms"]
                    .as_array()
                    .unwrap()
                    .len(),
                5
            );
            assert!(artifact["measurements"][name]["p50_ms"].is_number());
            assert!(artifact["measurements"][name]["p95_ms"].is_number());
        }
        for name in ["ordinary_search_phases", "filtered_search_phases"] {
            assert_eq!(
                artifact["measurements"][name]["samples"]
                    .as_array()
                    .unwrap()
                    .len(),
                5
            );
            assert!(artifact["measurements"][name]["p50_ms"]
                ["execution_candidate_paging_and_base_context_hydration_ms"]
                .is_number());
            for sample in artifact["measurements"][name]["samples"]
                .as_array()
                .unwrap()
            {
                let sample: SearchPhaseEvidence = serde_json::from_value(sample.clone()).unwrap();
                assert!(sample.reconciles());
            }
        }
        let ranked = &artifact["measurements"]["ranked_fts_hydration"];
        assert_eq!(
            artifact["measurements"]["ranked_fts_attribution_revision"],
            1
        );
        assert_eq!(ranked["attribution_route"], "fallback_ranked_fts");
        assert_eq!(
            ranked["production_reference"],
            "production_ranked_fts_fallback"
        );
        assert_eq!(ranked["default_filtered_route"], "fallback_ranked_fts");
        assert_eq!(ranked["ticket_qualified"], false);
        assert_eq!(ranked["samples"].as_array().unwrap().len(), 5);
        assert_eq!(ranked["candidate_ids_preserved"], true);
        assert_eq!(ranked["candidate_order_preserved"], true);
        assert_eq!(ranked["candidate_digests_preserved"], true);
        for sample in ranked["samples"].as_array().unwrap() {
            assert!(sample["selected_candidate_count"].as_u64().unwrap() > 0);
            assert_eq!(
                sample["candidate_record_digest"],
                digest_strings(
                    sample["candidate_record_ids"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|id| id.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                        .as_slice()
                )
            );
            assert_eq!(
                sample["result_digest"],
                digest_strings(
                    &sample["result_ids"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|id| id.as_str().unwrap().to_owned())
                        .collect::<Vec<_>>()
                )
            );
        }
    }

    #[test]
    fn phase_capture_preserves_search_output() {
        let temp = tempdir();
        let store = ctx_history_store::Store::open(temp.path().join("phases.sqlite")).unwrap();
        let archive = synthetic_perf_archive_batch(0, 1_200, 1, 1_200, 0x186).unwrap();
        write_synthetic_batch(&store, &archive).unwrap();
        let options = PacketOptions {
            limit: 10,
            ..PacketOptions::default()
        };
        let plain = search_packet(&store, "perfneedle", &options).unwrap();
        let measured = measured_profile_search(&store, "perfneedle", &options).unwrap();
        assert_eq!(result_ids(&plain), result_ids(&measured.packet));
        assert_eq!(
            digest_strings(&result_ids(&plain)),
            digest_strings(&result_ids(&measured.packet))
        );
        assert_eq!(measured.phases.path, "fast_event");
        assert!(measured.phases.reconciles());
    }

    #[test]
    fn search_phase_reconciliation_allows_only_quantization_drift() {
        let mut evidence = SearchPhaseEvidence {
            path: "fallback_ranked_fts".into(),
            filter_source_preparation_ms: 0.001,
            execution_candidate_paging_and_base_context_hydration_ms: 0.001,
            result_assembly_clustering_sorting_projection_ms: 0.001,
            unattributed_overhead_ms: 0.001,
            end_to_end_ms: 0.002,
            event_search_statements: 0,
            candidate_rows_hydrated: 0,
            candidates_examined: 0,
            candidates_rejected: 0,
            candidates_accepted: 0,
            relation_hydration_statements: 0,
            hydration_loader_statements: [0; 2],
        };

        assert!(evidence.reconciles());
        evidence.end_to_end_ms = 0.001;
        assert!(!evidence.reconciles());

        for malformed in [-1.0, f64::INFINITY, f64::NAN, f64::MAX] {
            evidence.end_to_end_ms = malformed;
            assert!(!evidence.reconciles());
        }
    }

    #[test]
    fn search_phase_percentiles_use_all_samples() {
        let samples = [1.0, 5.0, 3.0, 2.0, 4.0]
            .into_iter()
            .map(|value| SearchPhaseEvidence {
                path: "fast_event".into(),
                filter_source_preparation_ms: value,
                execution_candidate_paging_and_base_context_hydration_ms: value,
                result_assembly_clustering_sorting_projection_ms: value,
                unattributed_overhead_ms: value,
                end_to_end_ms: value * 4.0,
                event_search_statements: 1,
                candidate_rows_hydrated: 1,
                candidates_examined: 1,
                candidates_rejected: 0,
                candidates_accepted: 1,
                relation_hydration_statements: 0,
                hydration_loader_statements: [0; 2],
            })
            .collect();
        let stats = search_phase_stats(samples);
        assert_eq!(stats.p50_ms.filter_source_preparation_ms, 3.0);
        assert_eq!(stats.p95_ms.filter_source_preparation_ms, 5.0);
    }

    #[test]
    fn streaming_large_profile_rejects_malformed_artifacts() {
        let temp = tempdir();
        let valid =
            run_streaming_large_profile(&LargeProfileConfig::smoke(temp.path().join("valid")))
                .unwrap();
        assert!(parse_artifact_v1(valid.clone()).is_ok());
        let mut wrong_type = valid.clone();
        wrong_type["counts"]["events"] = serde_json::json!("not-a-number");
        assert!(parse_artifact_v1(wrong_type).is_err());
        let mut missing = valid.clone();
        missing["search"]
            .as_object_mut()
            .unwrap()
            .remove("result_digest");
        assert!(parse_artifact_v1(missing).is_err());
        let mut wrong_version = valid.clone();
        wrong_version["schema_version"] = serde_json::json!(3);
        assert!(parse_artifact_v1(wrong_version)
            .unwrap_err()
            .contains("schema_version"));
        let mut wrong_profile = valid.clone();
        wrong_profile["profile"] = serde_json::json!("other");
        assert!(parse_artifact_v1(wrong_profile)
            .unwrap_err()
            .contains("profile"));
        let mut inconsistent = valid.clone();
        inconsistent["counts"]["events"] = serde_json::json!(999);
        assert!(parse_artifact_v1(inconsistent)
            .unwrap_err()
            .contains("counts"));

        let mut missing_ranked = valid.clone();
        missing_ranked["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("ranked_fts_hydration");
        assert!(parse_artifact_v1(missing_ranked)
            .unwrap_err()
            .contains("missing ranked FTS attribution"));

        let mut unsupported_revision = valid.clone();
        unsupported_revision["measurements"]["ranked_fts_attribution_revision"] =
            serde_json::json!(2);
        assert!(parse_artifact_v1(unsupported_revision)
            .unwrap_err()
            .contains("unsupported ranked FTS attribution revision"));

        let mut missing_revision = valid.clone();
        missing_revision["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("ranked_fts_attribution_revision");
        assert!(parse_artifact_v1(missing_revision)
            .unwrap_err()
            .contains("missing ranked FTS attribution revision"));

        let mut wrong_route = valid.clone();
        wrong_route["measurements"]["ranked_fts_hydration"]["attribution_route"] =
            serde_json::json!("fast_event");
        assert!(parse_artifact_v1(wrong_route)
            .unwrap_err()
            .contains("route attribution mismatch"));

        let mut mismatched_phase_route = valid.clone();
        mismatched_phase_route["measurements"]["filtered_search_phases"]["samples"][0]["path"] =
            serde_json::json!("fast_event");
        assert!(parse_artifact_v1(mismatched_phase_route)
            .unwrap_err()
            .contains("route attribution mismatch"));

        let mut manual_without_five = valid.clone();
        manual_without_five["mode"] = serde_json::json!("manual");
        assert!(parse_artifact_v1(manual_without_five)
            .unwrap_err()
            .contains("exactly five warm samples"));

        for malformed in [serde_json::json!(-0.001), serde_json::json!(f64::MAX)] {
            let mut malformed_timing = valid.clone();
            malformed_timing["measurements"]["ordinary_search_phases"]["samples"][0]
                ["end_to_end_ms"] = malformed;
            assert!(parse_artifact_v1(malformed_timing)
                .unwrap_err()
                .contains("search phase evidence mismatch"));
        }

        let mut incomplete_current = valid;
        incomplete_current["measurements"]
            .as_object_mut()
            .unwrap()
            .remove("filtered_search_phases");
        assert!(parse_artifact_v1(incomplete_current)
            .unwrap_err()
            .contains("incomplete current search phase evidence"));

        let mut stripped_current = run_streaming_large_profile(&LargeProfileConfig::smoke(
            temp.path().join("stripped-current"),
        ))
        .unwrap();
        stripped_current["environment"]
            .as_object_mut()
            .unwrap()
            .remove("command");
        let measurements = stripped_current["measurements"].as_object_mut().unwrap();
        for field in [
            "ordinary_search_phases",
            "filtered_search_phases",
            "ranked_fts_attribution_revision",
            "ranked_fts_hydration",
        ] {
            measurements.remove(field);
        }
        assert!(parse_artifact_v1(stripped_current)
            .unwrap_err()
            .contains("incomplete current search phase evidence"));
    }

    #[test]
    fn pre_phase_v1_artifact_fixture_remains_compatible() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/large-profile-v1-pre-phases.json"))
                .unwrap();
        let artifact = parse_artifact_v1(value).unwrap();
        assert!(artifact.environment.command.is_none());
        assert!(artifact.measurements.ordinary_search_phases.is_none());
        assert!(artifact.measurements.filtered_search_phases.is_none());
    }

    #[test]
    fn streaming_large_profile_scaled_projection_parity_stays_bounded() {
        let temp = tempdir();
        let mut cfg = LargeProfileConfig::smoke(temp.path().join("scaled-parity"));
        cfg.total_events = 1_200;
        cfg.events_per_record = 1;
        cfg.batch_records = 75;
        let out = prepare_profile_output(&cfg).unwrap();
        let store =
            ctx_history_store::Store::open(out.join("synthetic-large-profile.sqlite")).unwrap();
        let mut imported = 0usize;
        let mut records = 0usize;
        while imported < cfg.total_events {
            let batch = synthetic_perf_archive_batch(
                imported,
                cfg.total_events,
                cfg.events_per_record,
                cfg.batch_records,
                cfg.seed,
            )
            .unwrap();
            imported += batch.events.len();
            records += batch.records.len();
            write_synthetic_batch(&store, &batch).unwrap();
        }
        let inc_start = imported;
        let inc = synthetic_perf_archive_batch(
            inc_start,
            inc_start + cfg.events_per_record,
            cfg.events_per_record,
            1,
            cfg.seed,
        )
        .unwrap();
        write_synthetic_batch(&store, &inc).unwrap();
        let counts = profile_counts(&store).unwrap();
        assert_eq!(counts.events, 1_201);
        assert_fts_projection_parity(&store, counts, cfg.seed, records, imported, inc_start)
            .unwrap();
        let last_event = cfg.total_events - 1;
        assert_sampled_event_projection(
            &store,
            "scaled last event rowid sample",
            synthetic_event_rowid(last_event, imported, inc_start).unwrap(),
            synthetic_event_id(last_event, cfg.seed).unwrap(),
        )
        .unwrap();
        assert_sampled_event_projection(
            &store,
            "scaled incremental event rowid sample",
            synthetic_event_rowid(inc_start, imported, inc_start).unwrap(),
            synthetic_event_id(inc_start, cfg.seed).unwrap(),
        )
        .unwrap();
        assert!(assert_sampled_event_projection(
            &store,
            "scaled wrong expected event id sample",
            synthetic_event_rowid(inc_start, imported, inc_start).unwrap(),
            synthetic_event_id(0, cfg.seed).unwrap(),
        )
        .unwrap_err()
        .contains("expected 1"));
        assert!(
            small_test_table_ids(&store, "events", "id").unwrap().len() < counts.events as usize
        );
        assert!(store
            .raw_sql_query("DELETE FROM event_search", Default::default())
            .is_err());
    }

    #[test]
    fn streaming_large_profile_uuid_bounds_and_rollback() {
        assert!(validate_uuid_capacity(10, 3, 0x0000_ffff_ff00).is_ok());
        assert!(validate_uuid_capacity(10, 3, 0x0000_ffff_ffff).is_err());
        let temp = tempdir();
        let cfg = LargeProfileConfig::smoke(temp.path().join("rollback"));
        let out = prepare_profile_output(&cfg).unwrap();
        let store =
            ctx_history_store::Store::open(out.join("synthetic-large-profile.sqlite")).unwrap();
        let batch = synthetic_perf_archive_batch(0, 6, 3, 2, cfg.seed).unwrap();
        write_synthetic_batch(&store, &batch).unwrap();
        let before = profile_counts(&store).unwrap();
        let before_base = all_base_identities(&store).unwrap();
        let before_record_fts =
            small_test_fts_ids(&store, "ctx_history_search", "record_id").unwrap();
        let before_event_fts = small_test_fts_ids(&store, "event_search", "event_id").unwrap();
        let mutated = synthetic_perf_archive_batch(6, 9, 3, 1, cfg.seed).unwrap();
        assert!(write_synthetic_batch_injected_failure(&store, &mutated, 2).is_err());
        let after = profile_counts(&store).unwrap();
        assert_eq!(before, after);
        assert_eq!(before_base, all_base_identities(&store).unwrap());
        assert_eq!(
            before_record_fts,
            small_test_fts_ids(&store, "ctx_history_search", "record_id").unwrap()
        );
        assert_eq!(
            before_event_fts,
            small_test_fts_ids(&store, "event_search", "event_id").unwrap()
        );
    }

    #[test]
    fn streaming_large_profile_output_guardrails() {
        let temp = tempdir();
        let custom_root = temp.path().join("custom-root");
        std::fs::create_dir_all(&custom_root).unwrap();
        let mut cfg = LargeProfileConfig::manual(custom_root.join("child"));
        cfg.release_build = true;
        assert!(
            prepare_profile_output_with_protected_root(&cfg, Some(custom_root.clone()))
                .unwrap_err()
                .contains("data root")
        );

        let stale = temp.path().join("stale");
        std::fs::create_dir_all(&stale).unwrap();
        let cfg = LargeProfileConfig::smoke(stale.clone());
        assert!(prepare_profile_output(&cfg).unwrap_err().contains("marker"));

        let marked = temp.path().join("marked");
        std::fs::create_dir_all(&marked).unwrap();
        std::fs::write(marked.join(".ctx-large-profile-owned"), "wrong\n").unwrap();
        let cfg = LargeProfileConfig::smoke(marked);
        assert!(prepare_profile_output(&cfg).unwrap_err().contains("marker"));
    }

    #[test]
    fn streaming_large_profile_enforces_manual_target_and_release() {
        let temp = tempdir();
        let mut cfg = LargeProfileConfig::manual(temp.path().join("manual"));
        assert_eq!(cfg.measurement_repeats, 5);
        assert_eq!(
            LargeProfileConfig::smoke(temp.path().join("smoke")).measurement_repeats,
            1
        );
        let mut invalid_repeats = cfg.clone();
        invalid_repeats.release_build = true;
        invalid_repeats.measurement_repeats = 4;
        assert!(run_streaming_large_profile(&invalid_repeats)
            .unwrap_err()
            .contains("exactly five warm samples"));
        cfg.release_build = false;
        assert!(run_streaming_large_profile(&cfg)
            .unwrap_err()
            .contains("release"));
        cfg.release_build = true;
        cfg.output_dir = None;
        assert!(run_streaming_large_profile(&cfg)
            .unwrap_err()
            .contains("output"));
        cfg.output_dir = Some(std::path::PathBuf::from("relative"));
        assert!(run_streaming_large_profile(&cfg)
            .unwrap_err()
            .contains("absolute"));
        let mut threshold = LargeProfileConfig::smoke(temp.path().join("threshold"));
        threshold.min_footprint_bytes = 1_000_000_000;
        assert!(run_streaming_large_profile(&threshold)
            .unwrap_err()
            .contains("minimum footprint"));
    }

    #[test]
    #[ignore = "manual >=10 GiB profile; run with --release and CTX_LARGE_PROFILE_OUTPUT"]
    fn streaming_large_profile_manual_release() {
        let out = std::env::var_os("CTX_LARGE_PROFILE_OUTPUT")
            .map(std::path::PathBuf::from)
            .expect("CTX_LARGE_PROFILE_OUTPUT must name an explicit non-home output directory");
        let artifact = run_streaming_large_profile(&LargeProfileConfig::manual(out)).unwrap();
        println!(
            "large profile artifact: {}",
            artifact["paths"]["artifact"].as_str().unwrap()
        );
    }

    #[derive(Clone)]
    struct LargeProfileConfig {
        output_dir: Option<std::path::PathBuf>,
        total_events: usize,
        events_per_record: usize,
        batch_records: usize,
        seed: u64,
        manual: bool,
        release_build: bool,
        min_footprint_bytes: u64,
        measurement_repeats: usize,
    }

    impl LargeProfileConfig {
        fn smoke(output_dir: std::path::PathBuf) -> Self {
            Self {
                output_dir: Some(output_dir),
                total_events: 21,
                events_per_record: 3,
                batch_records: 2,
                seed: 0x186,
                manual: false,
                release_build: !cfg!(debug_assertions),
                min_footprint_bytes: 0,
                measurement_repeats: 1,
            }
        }
        fn manual(output_dir: std::path::PathBuf) -> Self {
            Self {
                output_dir: Some(output_dir),
                total_events: env_usize("CTX_LARGE_PROFILE_EVENTS").unwrap_or(1_250_000),
                events_per_record: env_usize("CTX_LARGE_PROFILE_EVENTS_PER_RECORD")
                    .unwrap_or(25)
                    .clamp(1, 100),
                batch_records: env_usize("CTX_LARGE_PROFILE_BATCH_RECORDS")
                    .unwrap_or(250)
                    .clamp(1, 10_000),
                seed: env_u64("CTX_LARGE_PROFILE_SEED").unwrap_or(0x186),
                manual: true,
                release_build: !cfg!(debug_assertions),
                min_footprint_bytes: env_u64("CTX_LARGE_PROFILE_MIN_FOOTPRINT_BYTES")
                    .unwrap_or(10 * 1024 * 1024 * 1024),
                measurement_repeats: env_usize("CTX_LARGE_PROFILE_MEASUREMENT_REPEATS")
                    .unwrap_or(5)
                    .clamp(1, 20),
            }
        }
    }

    /// Seq-ordered window of up to `before`/`after` session neighbors around
    /// `event_id`, target included, built from the public store read APIs
    /// (main has no dedicated bounded-window primitive). Events without a
    /// session yield a single-element window, matching the evidence contract.
    fn bounded_event_window(
        store: &Store,
        event_id: Uuid,
        before: usize,
        after: usize,
    ) -> std::result::Result<Vec<Event>, String> {
        let event = store.get_event(event_id).map_err(|e| e.to_string())?;
        let Some(session_id) = event.session_id else {
            return Ok(vec![event]);
        };
        let events = store
            .events_for_session(session_id)
            .map_err(|e| e.to_string())?;
        let target = events
            .iter()
            .position(|candidate| candidate.id == event.id)
            .ok_or("window target missing from session events")?;
        let start = target.saturating_sub(before);
        let end = events
            .len()
            .min(target.saturating_add(after).saturating_add(1));
        Ok(events[start..end].to_vec())
    }

    fn run_streaming_large_profile(
        cfg: &LargeProfileConfig,
    ) -> std::result::Result<serde_json::Value, String> {
        if cfg.total_events == 0 || cfg.events_per_record == 0 || cfg.batch_records == 0 {
            return Err("profile counts must be positive".into());
        }
        if (cfg.total_events as u64)
            .checked_add(cfg.seed)
            .is_none_or(|v| v > 0x0000_ffff_ffff)
        {
            return Err("deterministic UUID range exceeded".into());
        }
        let batch_event_bound = cfg
            .batch_records
            .checked_mul(cfg.events_per_record)
            .ok_or("batch bound overflow")?;
        if cfg.manual && !cfg.release_build {
            return Err("manual large profile requires --release".into());
        }
        if cfg.manual && cfg.measurement_repeats != 5 {
            return Err("manual ranked FTS evidence requires exactly five warm samples".into());
        }
        let out = prepare_profile_output(cfg)?;
        let db = out.join("synthetic-large-profile.sqlite");
        let store = ctx_history_store::Store::open(db.clone()).map_err(|e| e.to_string())?;
        let started = std::time::Instant::now();
        let mut imported = 0usize;
        let mut records = 0usize;
        let mut max_batch_events = 0usize;
        let mut last_batch = None;
        while imported < cfg.total_events {
            let archive = synthetic_perf_archive_batch(
                imported,
                cfg.total_events,
                cfg.events_per_record,
                cfg.batch_records,
                cfg.seed,
            )?;
            max_batch_events = max_batch_events.max(archive.events.len());
            records += archive.records.len();
            imported += archive.events.len();
            write_synthetic_batch(&store, &archive).map_err(|e| e.to_string())?;
            last_batch = Some(archive);
        }
        let initial_ms = elapsed_ms(started.elapsed());
        let baseline_counts = profile_counts(&store)?;
        assert_expected_counts("baseline", baseline_counts, records as u64, imported as u64)?;
        let mut noop_samples = Vec::with_capacity(cfg.measurement_repeats);
        for _ in 0..cfg.measurement_repeats {
            let noop_started = std::time::Instant::now();
            write_synthetic_batch(&store, last_batch.as_ref().unwrap())
                .map_err(|e| e.to_string())?;
            noop_samples.push(elapsed_ms(noop_started.elapsed()));
            if profile_counts(&store)? != baseline_counts {
                return Err("no-op replay changed base/FTS counts".into());
            }
        }
        let noop_stats = timing_stats(&noop_samples);
        let inc_start = imported
            .div_ceil(cfg.events_per_record)
            .checked_mul(cfg.events_per_record)
            .ok_or("incremental start overflow")?;
        let inc = synthetic_perf_archive_batch(
            inc_start,
            inc_start
                .checked_add(cfg.events_per_record)
                .ok_or("incremental overflow")?,
            cfg.events_per_record,
            1,
            cfg.seed,
        )?;
        let inc_started = std::time::Instant::now();
        write_synthetic_batch(&store, &inc).map_err(|e| e.to_string())?;
        let inc_ms = elapsed_ms(inc_started.elapsed());
        let counts = profile_counts(&store)?;
        assert_expected_counts(
            "incremental",
            counts,
            records as u64 + 1,
            imported as u64 + cfg.events_per_record as u64,
        )?;
        assert_fts_projection_parity(&store, counts, cfg.seed, records, imported, inc_start)?;
        let opts = PacketOptions {
            limit: 10,
            snippet_chars: 180,
            ..PacketOptions::default()
        };
        let filtered_opts = PacketOptions {
            filters: SearchFilters {
                provider: Some(CaptureProvider::Codex),
                repo: Some("ctx".into()),
                event_type: Some(EventType::ToolCall),
                file: Some("perf_profile.rs".into()),
                ..SearchFilters::default()
            },
            ..opts.clone()
        };
        let mut warm_samples = Vec::with_capacity(cfg.measurement_repeats);
        let mut filtered_samples = Vec::with_capacity(cfg.measurement_repeats);
        let mut warm_phase_samples = Vec::with_capacity(cfg.measurement_repeats);
        let mut filtered_phase_samples = Vec::with_capacity(cfg.measurement_repeats);
        let mut ranked_fts_hydration_samples = Vec::with_capacity(cfg.measurement_repeats);
        let mut ranked_fts_hydration_reference: Option<RankedFtsHydrationIdentity> = None;
        let ranked_fts_production_reference =
            ranked_fts_production_identity(&store, "perfneedle", &filtered_opts)?;
        let mut warm = None;
        let mut filt = None;
        for _ in 0..cfg.measurement_repeats {
            let measured = measured_profile_search(&store, "perfneedle", &opts)?;
            let current = measured.packet;
            warm_samples.push(measured.phases.end_to_end_ms);
            warm_phase_samples.push(measured.phases);
            if current.results.is_empty() {
                return Err("ordinary search results must be nonempty".into());
            }
            if let Some(first) = &warm {
                if result_ids(first) != result_ids(&current) {
                    return Err("ordinary search ordering changed between samples".into());
                }
            } else {
                warm = Some(current);
            }

            let measured = measured_profile_search(&store, "perfneedle", &filtered_opts)?;
            let current = measured.packet;
            let current_result_ids = result_ids(&current);
            filtered_samples.push(measured.phases.end_to_end_ms);
            filtered_phase_samples.push(measured.phases);
            if current.results.is_empty() {
                return Err("filtered search results must be nonempty".into());
            }
            if let Some(first) = &filt {
                if result_ids(first) != current_result_ids {
                    return Err("filtered search ordering changed between samples".into());
                }
            } else {
                filt = Some(current);
            }

            let two_stage = measured_ranked_fts_hydration(&store, "perfneedle", &filtered_opts)?;
            if two_stage.identity != ranked_fts_production_reference {
                return Err(
                    "ranked FTS two-stage IDs/order/digests differ from production fallback".into(),
                );
            }
            if let Some(reference) = &ranked_fts_hydration_reference {
                if reference != &two_stage.identity {
                    return Err(
                        "ranked FTS two-stage candidate IDs/order/digests changed between samples"
                            .into(),
                    );
                }
            } else {
                ranked_fts_hydration_reference = Some(two_stage.identity.clone());
            }
            ranked_fts_hydration_samples.push(two_stage);
        }
        let warm = warm.unwrap();
        let filt = filt.unwrap();
        let warm_stats = timing_stats(&warm_samples);
        let filt_stats = timing_stats(&filtered_samples);
        let default_filtered_route = filtered_phase_samples
            .first()
            .map(|sample| sample.path.clone())
            .ok_or("filtered search phase samples must not be empty")?;
        if filtered_phase_samples
            .iter()
            .any(|sample| sample.path != default_filtered_route)
        {
            return Err("default filtered search route changed between samples".into());
        }
        let warm_phase_stats = search_phase_stats(warm_phase_samples);
        let filtered_phase_stats = search_phase_stats(filtered_phase_samples);
        let ranked_fts_hydration_stats = ranked_fts_hydration_stats(
            ranked_fts_hydration_samples,
            default_filtered_route,
            digest_strings(&result_ids(&filt)),
            cfg.manual,
        )?;
        let middle_id = synthetic_event_id(imported / 2, cfg.seed)?;
        let window_started = std::time::Instant::now();
        let window = bounded_event_window(&store, middle_id, 1, 1)?;
        let window_ms = elapsed_ms(window_started.elapsed());
        let pre_checkpoint = storage_snapshot(&db);
        let checkpoint_started = std::time::Instant::now();
        store.checkpoint_wal_truncate().map_err(|e| e.to_string())?;
        let checkpoint_ms = elapsed_ms(checkpoint_started.elapsed());
        let post_checkpoint = storage_snapshot(&db);
        let sqlite = sqlite_metadata(&store)?;
        drop(store);
        let reopen_started = std::time::Instant::now();
        let store = ctx_history_store::Store::open(db.clone()).map_err(|e| e.to_string())?;
        let reopened = search_packet(&store, "perfneedle", &opts).map_err(|e| e.to_string())?;
        let reopen_ms = elapsed_ms(reopen_started.elapsed());
        let warm_ids = result_ids(&warm);
        let reopened_ids = result_ids(&reopened);
        let warm_digest = digest_strings(&warm_ids);
        let reopened_digest = digest_strings(&reopened_ids);
        if warm_ids != reopened_ids || warm_digest != reopened_digest {
            return Err("reopen search IDs/digest changed".into());
        }
        let artifact_path = out.join("ctx-large-index-profile-v1.json");
        let footprint = post_checkpoint.total_present_bytes();
        if footprint < cfg.min_footprint_bytes {
            return Err(format!(
                "minimum footprint not reached: {footprint} < {}",
                cfg.min_footprint_bytes
            ));
        }
        let artifact = serde_json::json!({
            "schema_version": 2, "profile": "ctx-large-index-profile", "mode": if cfg.manual {"manual"} else {"smoke"},
            "requested": {"baseline_events": cfg.total_events, "min_footprint_bytes": cfg.min_footprint_bytes, "min_footprint_override_env": std::env::var("CTX_LARGE_PROFILE_MIN_FOOTPRINT_BYTES").ok()},
            "achieved": {"baseline_events": imported, "baseline_records": records, "incremental_events": inc.events.len(), "incremental_records": inc.records.len()},
            "config": {"seed": cfg.seed, "events_per_record": cfg.events_per_record, "batch_records": cfg.batch_records, "batch_event_bound": batch_event_bound, "measurement_repeats": cfg.measurement_repeats},
            "environment": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH, "jj_change": local_jj_id("change"), "jj_commit": local_jj_id("commit"), "cache_state": "warm followed by reopen; true cold cache requires operator OS cache-drop steps", "command": profile_command(cfg)},
            "sqlite": sqlite,
            "paths": {"db": canonical_display(&db), "wal": canonical_display(&db.with_extension("sqlite-wal")), "shm": canonical_display(&db.with_extension("sqlite-shm")), "artifact": artifact_path.display().to_string()},
            "storage": {"pre_checkpoint": pre_checkpoint.to_json(), "post_checkpoint": post_checkpoint.to_json()},
            "counts": counts.to_json(),
            "generation": {"max_batch_events": max_batch_events, "bounded_by_batch_size": true},
            "measurements": {"initial_import_ms": initial_ms, "noop_import_ms": noop_stats.samples_ms[0], "incremental_import_ms": inc_ms, "warm_search_ms": warm_stats.samples_ms[0], "filtered_search_ms": filt_stats.samples_ms[0], "noop_replay": noop_stats.to_json(), "warm_search": warm_stats.to_json(), "filtered_search": filt_stats.to_json(), "ordinary_search_phases": warm_phase_stats, "filtered_search_phases": filtered_phase_stats, "ranked_fts_attribution_revision": 1, "ranked_fts_hydration": ranked_fts_hydration_stats, "noop_counts_unchanged": true},
            "search": {"ordinary_result_count": warm.results.len(), "filtered_result_count": filt.results.len(), "ordered_result_ids": warm_ids, "result_digest": warm_digest, "filtered_ordered_result_ids": result_ids(&filt), "filtered_result_digest": digest_strings(&result_ids(&filt))},
            "event_window": {"target_event_id": middle_id.to_string(), "ids": window.iter().map(|e| e.id.to_string()).collect::<Vec<_>>(), "count": window.len(), "bound": 3, "contains_target": window.iter().any(|e| e.id == middle_id), "duration_ms": window_ms},
            "checkpoint": {"duration_ms": checkpoint_ms, "pre": pre_checkpoint.to_json(), "post": post_checkpoint.to_json()},
            "reopen": {"cache_state": "reopen_not_cold", "ordered_result_ids": reopened_ids, "result_digest": reopened_digest, "reopen_ms": reopen_ms},
            "rss": peak_rss(), "privacy": "synthetic deterministic corpus only; no real home, no network, no private data"
        });
        let typed: LargeProfileArtifactV1 =
            serde_json::from_value(artifact.clone()).map_err(|e| e.to_string())?;
        typed.validate()?;
        let stable = typed.stable_projection();
        let artifact = serde_json::to_value(&typed).map_err(|e| e.to_string())?;
        let reparsed: LargeProfileArtifactV1 =
            serde_json::from_value(artifact.clone()).map_err(|e| e.to_string())?;
        reparsed.validate()?;
        if stable != reparsed.stable_projection() {
            return Err("typed stable projection changed".into());
        }
        std::fs::write(
            &artifact_path,
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .map_err(|e| e.to_string())?;
        Ok(artifact)
    }

    fn synthetic_perf_archive_batch(
        start_event: usize,
        total_events: usize,
        events_per_record: usize,
        batch_records: usize,
        seed: u64,
    ) -> std::result::Result<SessionHistoryArchive, String> {
        validate_uuid_capacity(total_events, events_per_record, seed)?;
        let mut archive = SessionHistoryArchive::default();
        let start_record = start_event / events_per_record;
        let max_records = (total_events - start_event)
            .div_ceil(events_per_record)
            .min(batch_records);
        for record_index in start_record..start_record + max_records {
            append_synthetic_perf_record(
                &mut archive,
                record_index,
                start_event,
                total_events,
                events_per_record,
                seed,
            )?;
        }
        Ok(archive)
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct ProfileCounts {
        records: u64,
        capture_sources: u64,
        sessions: u64,
        runs: u64,
        events: u64,
        summaries: u64,
        files_touched: u64,
        record_fts: u64,
        event_fts: u64,
        artifact_fts: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct LargeProfileStableProjection {
        schema_version: u64,
        profile: String,
        mode: String,
        requested: RequestedProfile,
        achieved: AchievedProfile,
        config: ProfileConfigArtifact,
        counts: ProfileCounts,
        search: SearchEvidence,
        event_window: EventWindowEvidenceStable,
        reopen: ReopenEvidenceStable,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LargeProfileArtifactV1 {
        schema_version: u64,
        profile: String,
        mode: String,
        requested: RequestedProfile,
        achieved: AchievedProfile,
        config: ProfileConfigArtifact,
        environment: EnvironmentEvidence,
        sqlite: SqliteEvidence,
        paths: PathEvidence,
        storage: StorageEvidence,
        counts: ProfileCounts,
        generation: GenerationEvidence,
        measurements: MeasurementEvidence,
        search: SearchEvidence,
        event_window: EventWindowEvidence,
        checkpoint: CheckpointEvidence,
        reopen: ReopenEvidence,
        rss: RssEvidence,
        privacy: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct RequestedProfile {
        baseline_events: usize,
        min_footprint_bytes: u64,
        min_footprint_override_env: Option<String>,
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct AchievedProfile {
        baseline_events: usize,
        baseline_records: usize,
        incremental_events: usize,
        incremental_records: usize,
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct ProfileConfigArtifact {
        seed: u64,
        events_per_record: usize,
        batch_records: usize,
        batch_event_bound: usize,
        measurement_repeats: usize,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct EnvironmentEvidence {
        os: String,
        arch: String,
        jj_change: Option<String>,
        jj_commit: Option<String>,
        cache_state: String,
        #[serde(default)]
        command: Option<String>,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SqliteEvidence {
        version: String,
        journal_mode: String,
        synchronous: i64,
        page_size: i64,
        foreign_keys: i64,
        user_version: i64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PathEvidence {
        db: String,
        wal: String,
        shm: String,
        artifact: String,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StorageEvidence {
        pre_checkpoint: StorageStageEvidence,
        post_checkpoint: StorageStageEvidence,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StorageStageEvidence {
        db: FileEvidence,
        wal: FileEvidence,
        shm: FileEvidence,
        total_present_bytes: u64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct FileEvidence {
        path: String,
        bytes: Option<u64>,
        error: Option<String>,
        present: bool,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct GenerationEvidence {
        max_batch_events: usize,
        bounded_by_batch_size: bool,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MeasurementEvidence {
        initial_import_ms: f64,
        noop_import_ms: f64,
        incremental_import_ms: f64,
        warm_search_ms: f64,
        filtered_search_ms: f64,
        noop_replay: ProfileTimingEvidence,
        warm_search: ProfileTimingEvidence,
        filtered_search: ProfileTimingEvidence,
        #[serde(default)]
        ordinary_search_phases: Option<SearchPhaseStats>,
        #[serde(default)]
        filtered_search_phases: Option<SearchPhaseStats>,
        #[serde(default)]
        ranked_fts_hydration: Option<RankedFtsHydrationStats>,
        #[serde(default)]
        ranked_fts_attribution_revision: Option<u32>,
        noop_counts_unchanged: bool,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SearchPhaseStats {
        samples: Vec<SearchPhaseEvidence>,
        p50_ms: SearchPhasePercentiles,
        p95_ms: SearchPhasePercentiles,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SearchPhasePercentiles {
        filter_source_preparation_ms: f64,
        execution_candidate_paging_and_base_context_hydration_ms: f64,
        result_assembly_clustering_sorting_projection_ms: f64,
        unattributed_overhead_ms: f64,
        end_to_end_ms: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SearchPhaseEvidence {
        path: String,
        filter_source_preparation_ms: f64,
        execution_candidate_paging_and_base_context_hydration_ms: f64,
        result_assembly_clustering_sorting_projection_ms: f64,
        unattributed_overhead_ms: f64,
        end_to_end_ms: f64,
        event_search_statements: u64,
        candidate_rows_hydrated: u64,
        candidates_examined: u64,
        candidates_rejected: u64,
        candidates_accepted: u64,
        relation_hydration_statements: u64,
        hydration_loader_statements: [u64; 2],
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RankedFtsHydrationStats {
        samples: Vec<RankedFtsHydrationSample>,
        p50_ms: RankedFtsHydrationPercentiles,
        p95_ms: RankedFtsHydrationPercentiles,
        #[serde(default)]
        attribution_route: String,
        #[serde(default)]
        production_reference: String,
        #[serde(default)]
        default_filtered_route: String,
        #[serde(default)]
        default_filtered_result_digest: String,
        #[serde(default)]
        ticket_qualified: bool,
        candidate_ids_preserved: bool,
        candidate_order_preserved: bool,
        candidate_digests_preserved: bool,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RankedFtsHydrationSample {
        selection_ms: f64,
        hydration_ms: f64,
        unattributed_overhead_ms: f64,
        end_to_end_ms: f64,
        selected_candidate_count: usize,
        selected_candidate_digest: String,
        candidate_record_ids: Vec<String>,
        candidate_record_digest: String,
        result_ids: Vec<String>,
        result_digest: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RankedFtsHydrationPercentiles {
        selection_ms: f64,
        hydration_ms: f64,
        unattributed_overhead_ms: f64,
        end_to_end_ms: f64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProfileTimingEvidence {
        sample_count: usize,
        samples_ms: Vec<f64>,
        p50_ms: f64,
        p95_ms: f64,
        min_ms: f64,
        max_ms: f64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct SearchEvidence {
        ordinary_result_count: usize,
        filtered_result_count: usize,
        ordered_result_ids: Vec<String>,
        result_digest: String,
        filtered_ordered_result_ids: Vec<String>,
        filtered_result_digest: String,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct EventWindowEvidence {
        target_event_id: String,
        ids: Vec<String>,
        count: usize,
        bound: usize,
        contains_target: bool,
        duration_ms: f64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct EventWindowEvidenceStable {
        target_event_id: String,
        ids: Vec<String>,
        count: usize,
        bound: usize,
        contains_target: bool,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CheckpointEvidence {
        duration_ms: f64,
        pre: StorageStageEvidence,
        post: StorageStageEvidence,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ReopenEvidence {
        cache_state: String,
        ordered_result_ids: Vec<String>,
        result_digest: String,
        reopen_ms: f64,
    }
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct ReopenEvidenceStable {
        cache_state: String,
        ordered_result_ids: Vec<String>,
        result_digest: String,
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RssEvidence {
        peak_bytes: Option<u64>,
        source: String,
        semantics: String,
    }

    impl LargeProfileArtifactV1 {
        fn stable_projection(&self) -> LargeProfileStableProjection {
            LargeProfileStableProjection {
                schema_version: self.schema_version,
                profile: self.profile.clone(),
                mode: self.mode.clone(),
                requested: self.requested.clone(),
                achieved: self.achieved.clone(),
                config: self.config.clone(),
                counts: self.counts,
                search: self.search.clone(),
                event_window: EventWindowEvidenceStable {
                    target_event_id: self.event_window.target_event_id.clone(),
                    ids: self.event_window.ids.clone(),
                    count: self.event_window.count,
                    bound: self.event_window.bound,
                    contains_target: self.event_window.contains_target,
                },
                reopen: ReopenEvidenceStable {
                    cache_state: self.reopen.cache_state.clone(),
                    ordered_result_ids: self.reopen.ordered_result_ids.clone(),
                    result_digest: self.reopen.result_digest.clone(),
                },
            }
        }

        fn validate(&self) -> std::result::Result<(), String> {
            if !matches!(self.schema_version, 1 | 2) {
                return Err("wrong schema_version".into());
            }
            if self.profile != "ctx-large-index-profile" {
                return Err("wrong profile".into());
            }
            if self.mode != "smoke" && self.mode != "manual" {
                return Err("wrong mode".into());
            }
            if self.achieved.incremental_records != 1
                || self.achieved.incremental_events != self.config.events_per_record
            {
                return Err("bad incremental counts".into());
            }
            assert_expected_counts(
                "artifact",
                self.counts,
                (self.achieved.baseline_records + 1) as u64,
                (self.achieved.baseline_events + self.achieved.incremental_events) as u64,
            )?;
            if !self.measurements.noop_counts_unchanged {
                return Err("noop changed counts".into());
            }
            if self.search.ordinary_result_count == 0 || self.search.filtered_result_count == 0 {
                return Err("empty search evidence".into());
            }
            if self.search.result_digest != digest_strings(&self.search.ordered_result_ids) {
                return Err("search digest mismatch".into());
            }
            if self.search.filtered_result_digest
                != digest_strings(&self.search.filtered_ordered_result_ids)
            {
                return Err("filtered search digest mismatch".into());
            }
            for timing in [
                &self.measurements.noop_replay,
                &self.measurements.warm_search,
                &self.measurements.filtered_search,
            ] {
                if timing.sample_count != self.config.measurement_repeats
                    || timing.samples_ms.len() != self.config.measurement_repeats
                {
                    return Err("measurement repeat count mismatch".into());
                }
            }
            let current_format = self.schema_version == 2;
            let phase_sets = match (
                self.measurements.ordinary_search_phases.as_ref(),
                self.measurements.filtered_search_phases.as_ref(),
            ) {
                (Some(ordinary), Some(filtered)) if current_format => Some([ordinary, filtered]),
                (None, None) if !current_format => None,
                _ => return Err("incomplete current search phase evidence".into()),
            };
            for phases in phase_sets.into_iter().flatten() {
                if phases.samples.len() != self.config.measurement_repeats
                    || phases.samples.iter().any(|sample| {
                        !sample.reconciles()
                            || !matches!(sample.path.as_str(), "fast_event" | "fallback_ranked_fts")
                    })
                {
                    return Err("search phase evidence mismatch".into());
                }
            }
            if let Some(revision) = self.measurements.ranked_fts_attribution_revision {
                if revision != 1 {
                    return Err(format!(
                        "unsupported ranked FTS attribution revision: {revision}"
                    ));
                }
                if self.measurements.ranked_fts_hydration.is_none() {
                    return Err("current artifact is missing ranked FTS attribution".into());
                }
            } else if current_format {
                return Err("current artifact is missing ranked FTS attribution revision".into());
            }
            if current_format
                && (self.environment.command.is_none()
                    || self.measurements.ordinary_search_phases.is_none()
                    || self.measurements.filtered_search_phases.is_none()
                    || self.measurements.ranked_fts_attribution_revision != Some(1)
                    || self.measurements.ranked_fts_hydration.is_none())
            {
                return Err("schema version 2 requires current profile evidence".into());
            }
            if !current_format
                && (self.environment.command.is_some()
                    || self.measurements.ordinary_search_phases.is_some()
                    || self.measurements.filtered_search_phases.is_some()
                    || self.measurements.ranked_fts_attribution_revision.is_some()
                    || self.measurements.ranked_fts_hydration.is_some())
            {
                return Err("schema version 1 only supports the legacy profile shape".into());
            }
            if let Some(ranked) = &self.measurements.ranked_fts_hydration {
                if ranked.samples.len() != self.config.measurement_repeats
                    || !ranked.candidate_ids_preserved
                    || !ranked.candidate_order_preserved
                    || !ranked.candidate_digests_preserved
                    || ranked.samples.iter().any(|sample| {
                        sample.candidate_record_digest
                            != digest_strings(&sample.candidate_record_ids)
                            || sample.result_digest != digest_strings(&sample.result_ids)
                            || !sample.reconciles()
                    })
                {
                    return Err("ranked FTS two-stage evidence mismatch".into());
                }
                if self.measurements.ranked_fts_attribution_revision == Some(1) {
                    if ranked.attribution_route != "fallback_ranked_fts"
                        || ranked.production_reference != "production_ranked_fts_fallback"
                        || !matches!(
                            ranked.default_filtered_route.as_str(),
                            "fast_event" | "fallback_ranked_fts"
                        )
                        || ranked.default_filtered_result_digest
                            != self.search.filtered_result_digest
                        || self
                            .measurements
                            .filtered_search_phases
                            .as_ref()
                            .is_none_or(|phases| {
                                phases
                                    .samples
                                    .iter()
                                    .any(|sample| sample.path != ranked.default_filtered_route)
                            })
                    {
                        return Err("ranked FTS route attribution mismatch".into());
                    }
                    if self.mode == "manual"
                        && (self.config.measurement_repeats != 5
                            || !ranked.ticket_qualified
                            || ranked.samples.len() != 5)
                    {
                        return Err(
                            "manual ranked FTS evidence requires exactly five warm samples".into(),
                        );
                    }
                }
            }
            if self.event_window.count != self.event_window.ids.len()
                || self.event_window.count > self.event_window.bound
                || !self.event_window.contains_target
                || !self
                    .event_window
                    .ids
                    .contains(&self.event_window.target_event_id)
            {
                return Err("bad event window".into());
            }
            if self.reopen.ordered_result_ids != self.search.ordered_result_ids
                || self.reopen.result_digest != self.search.result_digest
            {
                return Err("reopen mismatch".into());
            }
            Ok(())
        }
    }

    fn parse_artifact_v1(
        value: serde_json::Value,
    ) -> std::result::Result<LargeProfileArtifactV1, String> {
        let artifact: LargeProfileArtifactV1 =
            serde_json::from_value(value).map_err(|e| e.to_string())?;
        artifact.validate()?;
        Ok(artifact)
    }

    impl ProfileCounts {
        fn to_json(self) -> serde_json::Value {
            serde_json::json!({"records": self.records, "capture_sources": self.capture_sources, "sessions": self.sessions, "runs": self.runs, "events": self.events, "summaries": self.summaries, "files_touched": self.files_touched, "record_fts": self.record_fts, "event_fts": self.event_fts, "artifact_fts": self.artifact_fts})
        }
    }

    fn assert_expected_counts(
        label: &str,
        counts: ProfileCounts,
        records: u64,
        events: u64,
    ) -> std::result::Result<(), String> {
        let expected = ProfileCounts {
            records,
            capture_sources: records,
            sessions: records,
            runs: records,
            events,
            summaries: records,
            files_touched: records,
            record_fts: records,
            event_fts: events,
            artifact_fts: 0,
        };
        if counts != expected {
            return Err(format!(
                "{label} counts mismatch: got {counts:?}, expected {expected:?}"
            ));
        }
        Ok(())
    }

    fn assert_fts_projection_parity(
        store: &ctx_history_store::Store,
        counts: ProfileCounts,
        seed: u64,
        baseline_records: usize,
        baseline_events: usize,
        incremental_start_event: usize,
    ) -> std::result::Result<(), String> {
        if counts.records != counts.record_fts || counts.events != counts.event_fts {
            return Err(format!(
                "base/FTS cardinality mismatch: records {} vs {}, events {} vs {}",
                counts.records, counts.record_fts, counts.events, counts.event_fts
            ));
        }
        let incremental_record =
            incremental_start_event / baseline_events.div_ceil(baseline_records);
        let record_sentinels = bounded_sample_indexes(baseline_records, incremental_record);
        for record_index in record_sentinels {
            let record_id = perf_uuid_checked(
                0x7000,
                (record_index as u64)
                    .checked_add(seed)
                    .ok_or("uuid index overflow")?,
            )?;
            let base_label = format!("record sample {record_index} base row {record_id}");
            assert_scalar_count_eq(
                store,
                &base_label,
                &format!("SELECT COUNT(*) FROM history_records WHERE id = '{record_id}'"),
                1,
            )?;
            assert_sampled_record_projection(store, record_index, record_id)?;
        }
        let event_sentinels = bounded_sample_indexes(
            baseline_events,
            incremental_start_event + baseline_events.div_ceil(baseline_records) - 1,
        );
        for event_index in event_sentinels {
            let event_id = synthetic_event_id(event_index, seed)?;
            let base_label = format!("event sample {event_index} base row {event_id}");
            assert_scalar_count_eq(
                store,
                &base_label,
                &format!("SELECT COUNT(*) FROM events WHERE id = '{event_id}'"),
                1,
            )?;
            assert_sampled_event_projection(
                store,
                &format!("event sample {event_index}"),
                synthetic_event_rowid(event_index, baseline_events, incremental_start_event)?,
                event_id,
            )?;
        }
        Ok(())
    }

    fn assert_sampled_record_projection(
        store: &ctx_history_store::Store,
        record_index: usize,
        record_id: Uuid,
    ) -> std::result::Result<(), String> {
        let label = format!("record sample {record_index} FTS perfneedle projection {record_id}");
        assert_scalar_count_eq(
            store,
            &label,
            &format!("SELECT COUNT(*) FROM ctx_history_search WHERE record_id = '{record_id}' AND ctx_history_search MATCH 'perfneedle'"),
            1,
        )
    }

    fn assert_sampled_event_projection(
        store: &ctx_history_store::Store,
        label_prefix: &str,
        rowid: u64,
        event_id: Uuid,
    ) -> std::result::Result<(), String> {
        // Synthetic profile invariant only: events are inserted exactly once in
        // deterministic batch order, so FTS5 rowid follows insertion order. The
        // incremental record may follow a partial final baseline batch, so its
        // rowid is derived from baseline cardinality, not necessarily index+1.
        // This is not a production storage-contract assumption.
        let label = format!("{label_prefix} FTS rowid {rowid} perfneedle projection {event_id}");
        assert_scalar_count_eq(
            store,
            &label,
            &format!("SELECT COUNT(*) FROM event_search WHERE rowid = {rowid} AND event_id = '{event_id}' AND event_search MATCH 'perfneedle'"),
            1,
        )
    }

    fn checked_sample_rowid(index: usize) -> std::result::Result<u64, String> {
        (index as u64)
            .checked_add(1)
            .ok_or_else(|| "sample rowid overflow".to_string())
    }

    fn synthetic_event_rowid(
        event_index: usize,
        baseline_events: usize,
        incremental_start_event: usize,
    ) -> std::result::Result<u64, String> {
        let insertion_index = if event_index < baseline_events {
            event_index
        } else {
            baseline_events
                .checked_add(
                    event_index
                        .checked_sub(incremental_start_event)
                        .ok_or("event sample before incremental start")?,
                )
                .ok_or("event rowid insertion index overflow")?
        };
        checked_sample_rowid(insertion_index)
    }

    fn bounded_sample_indexes(baseline_len: usize, incremental_index: usize) -> Vec<usize> {
        let last = baseline_len.saturating_sub(1);
        let mut indexes = std::collections::BTreeSet::new();
        indexes.insert(0);
        indexes.insert(baseline_len / 4);
        indexes.insert(baseline_len / 2);
        indexes.insert((baseline_len * 3) / 4);
        indexes.insert(last);
        indexes.insert(incremental_index);
        indexes.into_iter().collect()
    }

    fn all_base_identities(
        store: &ctx_history_store::Store,
    ) -> std::result::Result<Vec<(String, Vec<String>)>, String> {
        Ok(vec![
            (
                "history_records".into(),
                small_test_table_ids(store, "history_records", "id")?,
            ),
            (
                "capture_sources".into(),
                small_test_table_ids(store, "capture_sources", "id")?,
            ),
            (
                "sessions".into(),
                small_test_table_ids(store, "sessions", "id")?,
            ),
            ("runs".into(), small_test_table_ids(store, "runs", "id")?),
            (
                "events".into(),
                small_test_table_ids(store, "events", "id")?,
            ),
            (
                "summaries".into(),
                small_test_table_ids(store, "summaries", "id")?,
            ),
            (
                "files_touched".into(),
                small_test_table_ids(store, "files_touched", "id")?,
            ),
        ])
    }

    fn write_synthetic_batch(
        store: &ctx_history_store::Store,
        archive: &SessionHistoryArchive,
    ) -> ctx_history_store::Result<()> {
        store.begin_immediate_batch()?;
        let result = write_synthetic_batch_inner(store, archive, None);
        match result {
            Ok(()) => store.commit_batch(),
            Err(error) => {
                let _ = store.rollback_batch();
                Err(error)
            }
        }
    }

    fn write_synthetic_batch_injected_failure(
        store: &ctx_history_store::Store,
        archive: &SessionHistoryArchive,
        fail_after: usize,
    ) -> ctx_history_store::Result<()> {
        store.begin_immediate_batch()?;
        let result = write_synthetic_batch_inner(store, archive, Some(fail_after));
        match result {
            Ok(()) => store.commit_batch(),
            Err(error) => {
                let _ = store.rollback_batch();
                Err(error)
            }
        }
    }

    fn write_synthetic_batch_inner(
        store: &ctx_history_store::Store,
        archive: &SessionHistoryArchive,
        fail_after: Option<usize>,
    ) -> ctx_history_store::Result<()> {
        let mut writes = 0usize;
        for workspace in &archive.vcs_workspaces {
            store.upsert_vcs_workspace(workspace)?;
        }
        writes += 1;
        if fail_after == Some(writes) {
            return Err(ctx_history_store::StoreError::NumericOutOfRange {
                field: "injected profile failure",
            });
        }
        for record in &archive.records {
            store.upsert_record(record)?;
        }
        writes += 1;
        if fail_after == Some(writes) {
            return Err(ctx_history_store::StoreError::NumericOutOfRange {
                field: "injected profile failure",
            });
        }
        for source in &archive.capture_sources {
            store.upsert_capture_source(source)?;
        }
        for session in &archive.sessions {
            store.upsert_session(session)?;
        }
        for run in &archive.runs {
            store.insert_run_if_absent(run)?;
        }
        for summary in &archive.summaries {
            store.upsert_summary(summary)?;
        }
        for file in &archive.files_touched {
            store.upsert_file_touched(file)?;
        }
        for event in &archive.events {
            store.insert_event_if_absent(event)?;
        }
        Ok(())
    }

    fn small_test_fts_ids(
        store: &ctx_history_store::Store,
        table: &str,
        column: &str,
    ) -> std::result::Result<Vec<String>, String> {
        small_test_table_ids(store, table, column)
    }

    fn small_test_table_ids(
        store: &ctx_history_store::Store,
        table: &str,
        column: &str,
    ) -> std::result::Result<Vec<String>, String> {
        let sql = format!("SELECT {column} FROM {table} ORDER BY {column}");
        let result = store
            .raw_sql_query(&sql, Default::default())
            .map_err(|e| e.to_string())?;
        result
            .rows
            .iter()
            .map(|row| {
                row.first()
                    .and_then(raw_str)
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| "missing fts id".to_string())
            })
            .collect()
    }

    fn validate_uuid_capacity(
        total_events: usize,
        events_per_record: usize,
        seed: u64,
    ) -> std::result::Result<(), String> {
        if total_events == 0 || events_per_record == 0 {
            return Err("counts must be positive".into());
        }
        let final_record = (total_events - 1) / events_per_record;
        let inc_start = total_events
            .div_ceil(events_per_record)
            .checked_mul(events_per_record)
            .ok_or("incremental start overflow")?;
        let final_inc_event = inc_start
            .checked_add(events_per_record)
            .and_then(|v| v.checked_sub(1))
            .ok_or("event overflow")?;
        for index in [final_record as u64, final_inc_event as u64] {
            for ns in [0x7000, 0x7100, 0x7200, 0x7300, 0x7400, 0x7500, 0x7600] {
                let _ =
                    perf_uuid_checked(ns, index.checked_add(seed).ok_or("uuid index overflow")?)?;
            }
        }
        Ok(())
    }

    fn profile_counts(
        store: &ctx_history_store::Store,
    ) -> std::result::Result<ProfileCounts, String> {
        let counts = store.profile_table_counts().map_err(|e| e.to_string())?;
        Ok(ProfileCounts {
            records: counts.records,
            capture_sources: counts.capture_sources,
            sessions: counts.sessions,
            runs: counts.runs,
            events: counts.events,
            summaries: counts.summaries,
            files_touched: counts.files_touched,
            record_fts: counts.record_fts,
            event_fts: counts.event_fts,
            artifact_fts: counts.artifact_fts,
        })
    }

    fn scalar_sql_count_labeled(
        store: &ctx_history_store::Store,
        label: &str,
        sql: &str,
    ) -> std::result::Result<u64, String> {
        let result = store
            .raw_sql_query(sql, Default::default())
            .map_err(|e| format!("{label}: {e}"))?;
        result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(raw_i64)
            .map(|v| v as u64)
            .ok_or_else(|| format!("{label}: missing scalar count"))
    }

    fn assert_scalar_count_eq(
        store: &ctx_history_store::Store,
        label: &str,
        sql: &str,
        expected: u64,
    ) -> std::result::Result<(), String> {
        let actual = scalar_sql_count_labeled(store, label, sql)?;
        if actual != expected {
            return Err(format!("{label}: expected {expected}, got {actual}"));
        }
        Ok(())
    }

    fn sqlite_metadata(
        store: &ctx_history_store::Store,
    ) -> std::result::Result<serde_json::Value, String> {
        let metadata = store.sqlite_profile_metadata().map_err(|e| e.to_string())?;
        Ok(
            serde_json::json!({"version": metadata.version, "journal_mode": metadata.journal_mode, "synchronous": metadata.synchronous, "page_size": metadata.page_size, "foreign_keys": metadata.foreign_keys, "user_version": metadata.user_version}),
        )
    }
    fn raw_i64(value: &ctx_history_store::RawSqlValue) -> Option<i64> {
        match value {
            ctx_history_store::RawSqlValue::Integer(v) => Some(*v),
            _ => None,
        }
    }
    fn raw_str(value: &ctx_history_store::RawSqlValue) -> Option<&str> {
        match value {
            ctx_history_store::RawSqlValue::Text { value, .. } => Some(value.as_str()),
            _ => None,
        }
    }

    #[derive(Clone)]
    struct StorageSnapshot {
        db: FileState,
        wal: FileState,
        shm: FileState,
    }
    #[derive(Clone)]
    struct FileState {
        path: String,
        bytes: Option<u64>,
        error: Option<String>,
    }
    impl StorageSnapshot {
        fn to_json(&self) -> serde_json::Value {
            serde_json::json!({"db": self.db.to_json(), "wal": self.wal.to_json(), "shm": self.shm.to_json(), "total_present_bytes": self.total_present_bytes()})
        }
        fn total_present_bytes(&self) -> u64 {
            self.db.bytes.unwrap_or(0) + self.wal.bytes.unwrap_or(0) + self.shm.bytes.unwrap_or(0)
        }
    }
    impl FileState {
        fn to_json(&self) -> serde_json::Value {
            serde_json::json!({"path": self.path, "bytes": self.bytes, "error": self.error, "present": self.bytes.is_some()})
        }
    }

    fn storage_snapshot(db: &std::path::Path) -> StorageSnapshot {
        StorageSnapshot {
            db: file_state(db),
            wal: file_state(&db.with_extension("sqlite-wal")),
            shm: file_state(&db.with_extension("sqlite-shm")),
        }
    }
    fn file_state(path: &std::path::Path) -> FileState {
        match std::fs::metadata(path) {
            Ok(m) => FileState {
                path: canonical_display(path),
                bytes: Some(m.len()),
                error: None,
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileState {
                path: path.display().to_string(),
                bytes: None,
                error: None,
            },
            Err(e) => FileState {
                path: path.display().to_string(),
                bytes: None,
                error: Some(e.to_string()),
            },
        }
    }

    fn prepare_profile_output(
        cfg: &LargeProfileConfig,
    ) -> std::result::Result<std::path::PathBuf, String> {
        prepare_profile_output_with_protected_root(cfg, None)
    }

    fn prepare_profile_output_with_protected_root(
        cfg: &LargeProfileConfig,
        protected_root: Option<std::path::PathBuf>,
    ) -> std::result::Result<std::path::PathBuf, String> {
        let out = cfg
            .output_dir
            .clone()
            .ok_or("manual large profile requires explicit output")?;
        if cfg.manual && !out.is_absolute() {
            return Err("manual output must be absolute".into());
        }
        let parent = out.parent().ok_or("output must have parent")?;
        let parent = parent.canonicalize().map_err(|e| e.to_string())?;
        let out = parent.join(out.file_name().ok_or("output must have final component")?);
        reject_protected_data_root(&out, protected_root.as_deref())?;
        let marker = out.join(".ctx-large-profile-owned");
        if out.exists() {
            let canon = out.canonicalize().map_err(|e| e.to_string())?;
            reject_protected_data_root(&canon, protected_root.as_deref())?;
            let marker_text = std::fs::read_to_string(&marker).map_err(|_| {
                "pre-existing output lacks valid .ctx-large-profile-owned marker".to_string()
            })?;
            let expected = marker_contents(&canon);
            if marker_text != expected {
                return Err("pre-existing output marker is stale or foreign".into());
            }
            for name in [
                "synthetic-large-profile.sqlite",
                "synthetic-large-profile.sqlite-wal",
                "synthetic-large-profile.sqlite-shm",
                "ctx-large-index-profile-v1.json",
            ] {
                let path = out.join(name);
                if path.exists() {
                    std::fs::remove_file(&path).map_err(|e| e.to_string())?;
                }
            }
        } else {
            std::fs::create_dir_all(&out).map_err(|e| e.to_string())?;
        }
        let canon = out.canonicalize().map_err(|e| e.to_string())?;
        reject_protected_data_root(&canon, protected_root.as_deref())?;
        std::fs::write(&marker, marker_contents(&canon)).map_err(|e| e.to_string())?;
        Ok(out)
    }

    fn marker_contents(canonical_output: &std::path::Path) -> String {
        format!(
            "ctx-large-profile-owned-v1\n{}\n",
            canonical_output.display()
        )
    }

    fn reject_protected_data_root(
        path: &std::path::Path,
        protected_root: Option<&std::path::Path>,
    ) -> std::result::Result<(), String> {
        let protected = match protected_root {
            Some(root) => root.to_path_buf(),
            None => default_data_root().map_err(|e| format!("cannot resolve data root: {e}"))?,
        };
        let protected = canonicalize_existing_ancestor(&protected)
            .map_err(|e| format!("cannot canonicalize data root: {e}"))?;
        let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        if candidate == protected || candidate.starts_with(&protected) {
            return Err("output overlaps protected ctx data root".into());
        }
        Ok(())
    }

    fn canonicalize_existing_ancestor(
        path: &std::path::Path,
    ) -> std::io::Result<std::path::PathBuf> {
        if path.exists() {
            return path.canonicalize();
        }
        let name = path
            .file_name()
            .ok_or_else(|| std::io::Error::other("missing file name"))?;
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("missing parent"))?;
        Ok(parent.canonicalize()?.join(name))
    }

    fn result_ids(packet: &SearchPacket) -> Vec<String> {
        packet
            .results
            .iter()
            .map(|r| r.record_id.to_string())
            .collect()
    }
    fn digest_strings(values: &[String]) -> String {
        let mut h = 0xcbf29ce484222325u64;
        for value in values {
            for byte in value.as_bytes().iter().copied().chain([0]) {
                h ^= byte as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
        }
        format!("{h:016x}")
    }
    fn synthetic_event_id(index: usize, seed: u64) -> std::result::Result<Uuid, String> {
        perf_uuid_checked(
            0x7600,
            (index as u64)
                .checked_add(seed)
                .ok_or("uuid index overflow")?,
        )
    }
    fn perf_uuid_checked(namespace: u16, index: u64) -> std::result::Result<Uuid, String> {
        if index > 0x0000_ffff_ffff {
            return Err("uuid index out of deterministic range".into());
        }
        Uuid::parse_str(&format!("018f45d0-{namespace:04x}-7000-8000-{index:012x}"))
            .map_err(|e| e.to_string())
    }
    fn canonical_display(path: &std::path::Path) -> String {
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .display()
            .to_string()
    }

    #[test]
    #[ignore = "manual perf benchmark; private release gates run scripts/public-ctx/perf-smoke.sh from ctx-private"]
    fn synthetic_search_perf_records_thresholded_evidence() {
        let out_dir = std::env::var_os("CTX_ARTIFACT_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .ancestors()
                    .nth(2)
                    .unwrap()
                    .join("target/ctx-artifacts/synthetic_search_perf")
            });
        std::fs::create_dir_all(&out_dir).unwrap();
        let artifact_path = out_dir.join("synthetic-search-perf.json");

        let event_count = perf_event_count();
        let events_per_record = perf_events_per_record();
        let search_repeats = perf_repeats("CTX_SEARCH_PERF_SEARCH_REPEATS", 9);
        let filtered_search_repeats = perf_repeats("CTX_SEARCH_PERF_FILTERED_SEARCH_REPEATS", 5);
        let thresholds = perf_thresholds(event_count);

        let generation_started = std::time::Instant::now();
        let archive = synthetic_perf_archive(event_count, events_per_record);
        let generation_ms = elapsed_ms(generation_started.elapsed());
        let corpus = PerfCorpus {
            records: archive.records.len(),
            capture_sources: archive.capture_sources.len(),
            sessions: archive.sessions.len(),
            runs: archive.runs.len(),
            events: archive.events.len(),
            summaries: archive.summaries.len(),
            files_touched: archive.files_touched.len(),
        };

        let (_temp, mut store) = test_store();
        let import_started = std::time::Instant::now();
        store.import_archive(&archive, false).unwrap();
        let import_ms = elapsed_ms(import_started.elapsed());
        let import_secs = (import_ms / 1000.0).max(0.001);
        let import_events_per_sec = corpus.events as f64 / import_secs;

        let search_options = PacketOptions {
            limit: 24,
            snippet_chars: 320,
            filters: SearchFilters::default(),
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        };
        let filtered_search_options = PacketOptions {
            limit: 24,
            snippet_chars: 320,
            filters: SearchFilters {
                provider: Some(CaptureProvider::Codex),
                repo: Some("ctx".into()),
                event_type: Some(EventType::ToolCall),
                file: Some("perf_profile.rs".into()),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        };

        let search_warmup = search_packet(&store, "perfneedle", &search_options).unwrap();
        assert_perf_results("search warmup", search_warmup.results.len());
        let filtered_search_warmup =
            search_packet(&store, "perfneedle", &filtered_search_options).unwrap();
        assert_perf_results(
            "filtered search warmup",
            filtered_search_warmup.results.len(),
        );

        let mut search_samples = Vec::new();
        let mut last_search_results = 0;
        let mut last_search_citations = 0;
        for _ in 0..search_repeats {
            let started = std::time::Instant::now();
            let packet = search_packet(&store, "perfneedle", &search_options).unwrap();
            let elapsed = elapsed_ms(started.elapsed());
            assert_perf_results("search sample", packet.results.len());
            last_search_results = packet.results.len();
            last_search_citations = packet
                .results
                .iter()
                .map(|result| result.citations.len())
                .sum();
            search_samples.push(elapsed);
        }

        let mut filtered_search_samples = Vec::new();
        let mut last_filtered_search_results = 0;
        let mut last_filtered_search_citations = 0;
        for _ in 0..filtered_search_repeats {
            let started = std::time::Instant::now();
            let packet = search_packet(&store, "perfneedle", &filtered_search_options).unwrap();
            let elapsed = elapsed_ms(started.elapsed());
            assert_perf_results("filtered search sample", packet.results.len());
            last_filtered_search_results = packet.results.len();
            last_filtered_search_citations = packet
                .results
                .iter()
                .map(|result| result.citations.len())
                .sum();
            filtered_search_samples.push(elapsed);
        }

        let db_path = store.path().to_path_buf();
        drop(store);
        let db_bytes = sqlite_footprint_bytes(&db_path);
        let main_db_bytes = std::fs::metadata(&db_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        let import_stats = timing_stats(&[import_ms]);
        let search_stats = timing_stats(&search_samples);
        let filtered_search_stats = timing_stats(&filtered_search_samples);
        let max_db_bytes = thresholds.max_db_bytes_per_event * corpus.events as u64;
        let checks = vec![
            serde_json::json!({
                "name": "corpus_events_at_least_10000",
                "passed": corpus.events >= 10_000,
                "actual": corpus.events,
                "threshold": 10_000
            }),
            serde_json::json!({
                "name": "import_events_per_sec",
                "passed": import_events_per_sec >= thresholds.import_min_events_per_sec,
                "actual": rounded(import_events_per_sec),
                "threshold": thresholds.import_min_events_per_sec
            }),
            serde_json::json!({
                "name": "search_p95_ms",
                "passed": search_stats.p95_ms <= thresholds.search_p95_ms,
                "actual": search_stats.p95_ms,
                "threshold": thresholds.search_p95_ms
            }),
            serde_json::json!({
                "name": "filtered_search_p95_ms",
                "passed": filtered_search_stats.p95_ms <= thresholds.filtered_search_p95_ms,
                "actual": filtered_search_stats.p95_ms,
                "threshold": thresholds.filtered_search_p95_ms
            }),
            serde_json::json!({
                "name": "db_footprint_bytes",
                "passed": db_bytes <= max_db_bytes,
                "actual": db_bytes,
                "threshold": max_db_bytes
            }),
        ];
        let passed = checks
            .iter()
            .all(|check| check["passed"].as_bool().unwrap_or(false));

        let artifact = serde_json::json!({
            "schema_version": 1,
            "profile": "synthetic-search-perf",
            "mode": if event_count >= 100_000 { "slow" } else { "standard" },
            "status": if passed { "passed" } else { "failed" },
            "corpus": {
                "records": corpus.records,
                "capture_sources": corpus.capture_sources,
                "sessions": corpus.sessions,
                "runs": corpus.runs,
                "events": corpus.events,
                "summaries": corpus.summaries,
                "files_touched": corpus.files_touched,
                "events_per_record": events_per_record,
                "query": "perfneedle"
            },
            "thresholds": {
                "import_min_events_per_sec": thresholds.import_min_events_per_sec,
                "search_p95_ms": thresholds.search_p95_ms,
                "filtered_search_p95_ms": thresholds.filtered_search_p95_ms,
                "max_db_bytes_per_event": thresholds.max_db_bytes_per_event,
                "env_overrides": [
                    "CTX_SEARCH_PERF_IMPORT_MIN_EVENTS_PER_SEC",
                    "CTX_SEARCH_PERF_SEARCH_P95_MS",
                    "CTX_SEARCH_PERF_FILTERED_SEARCH_P95_MS",
                    "CTX_SEARCH_PERF_MAX_DB_BYTES_PER_EVENT"
                ]
            },
            "profiles": {
                "generation": {
                    "duration_ms": generation_ms
                },
                "import": {
                    "timings": import_stats.to_json(),
                    "events_per_sec": rounded(import_events_per_sec)
                },
                "search": {
                    "timings": search_stats.to_json(),
                    "result_count": last_search_results,
                    "citation_count": last_search_citations,
                    "repeats": search_repeats
                },
                "filtered_search": {
                    "timings": filtered_search_stats.to_json(),
                    "result_count": last_filtered_search_results,
                    "citation_count": last_filtered_search_citations,
                    "repeats": filtered_search_repeats
                }
            },
            "storage": {
                "main_db_bytes": main_db_bytes,
                "db_footprint_bytes": db_bytes,
                "db_bytes_per_event": rounded(db_bytes as f64 / corpus.events as f64)
            },
            "checks": checks
        });

        std::fs::write(
            &artifact_path,
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .unwrap();
        println!(
            "synthetic search perf artifact: {}",
            artifact_path.display()
        );

        assert!(
            passed,
            "synthetic search perf thresholds failed; see {}",
            artifact_path.display()
        );
    }

    const SQLITE_SEARCH_BASELINE_COMMAND: &str = "cargo test -q -p ctx-history-search --release tests::synthetic_search_sqlite_baseline_evidence -- --ignored --exact --nocapture";

    const SQLITE_CACHE_TUNING_COMMAND: &str = "cargo test -q -p ctx-history-search --release tests::synthetic_search_sqlite_cache_tuning_evidence -- --ignored --exact --nocapture";

    // These are the two SQLite phases used by the large-event search path,
    // reduced to their database work so each arm can use an explicitly tuned,
    // disposable read-only connection. Keep the SQL here byte-for-byte
    // representative of the production candidate ordering and joins; this is
    // evidence code, not an alternate production query implementation.
    const SQLITE_TUNING_UNFILTERED_SQL: &str = r#"
        SELECT event_search.event_id
        FROM event_search
        JOIN events e ON e.id = event_search.event_id
        WHERE event_search MATCH ?1
        ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
        LIMIT ?2 OFFSET ?3
    "#;

    const SQLITE_TUNING_FILTERED_SQL: &str = r#"
        SELECT event_search.event_id
        FROM event_search
        JOIN events e ON e.id = event_search.event_id
        LEFT JOIN runs r ON r.id = e.run_id
        LEFT JOIN sessions s ON s.id = COALESCE(e.session_id, event_search.session_id)
        LEFT JOIN sessions rs ON rs.id = r.session_id
        LEFT JOIN capture_sources event_source ON event_source.id = e.capture_source_id
        LEFT JOIN capture_sources session_source ON session_source.id = COALESCE(s.capture_source_id, rs.capture_source_id)
        LEFT JOIN capture_sources run_source ON run_source.id = r.source_id
        WHERE event_search MATCH ?1
          AND COALESCE(s.provider, rs.provider, event_source.provider, session_source.provider, run_source.provider) = ?4
          AND e.event_type = ?5
        ORDER BY bm25(event_search), e.occurred_at_ms DESC, e.seq DESC, event_search.event_id
        LIMIT ?2 OFFSET ?3
    "#;

    #[test]
    #[ignore = "evidence-only SQLite cache-size tuning; intentionally has no performance gates"]
    fn synthetic_search_sqlite_cache_tuning_evidence() {
        let provenance = local_jj_provenance().unwrap_or_else(|error| {
            panic!("SQLite tuning requires jj commit/change provenance: {error}")
        });
        let evidence = sqlite_cache_tuning_evidence(200_000, 50, 5, &provenance);
        println!(
            "sqlite cache tuning evidence: {}",
            serde_json::to_string(&evidence).unwrap()
        );
    }

    fn sqlite_cache_tuning_evidence(
        event_count: usize,
        events_per_record: usize,
        sample_count: usize,
        provenance: &BaselineProvenance,
    ) -> serde_json::Value {
        assert_eq!(event_count, 200_000, "this spike is fixed at 200k events");
        assert_eq!(sample_count, 5, "this spike is fixed at five warm samples");
        let archive = synthetic_perf_archive(event_count, events_per_record);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let db_path = store.path().to_path_buf();
        drop(store);

        let plans = sqlite_tuning_plans(&db_path);
        let opening_a = sqlite_cache_tuning_arm(&db_path, -32_768, sample_count);
        let b = sqlite_cache_tuning_arm(&db_path, -131_072, sample_count);
        let closing_a = sqlite_cache_tuning_arm(&db_path, -32_768, sample_count);
        let integrity = sqlite_tuning_integrity(&db_path);
        assert_eq!(opening_a["effective_cache_size_kib"], -32_768);
        assert_eq!(b["effective_cache_size_kib"], -131_072);
        assert_eq!(closing_a["effective_cache_size_kib"], -32_768);
        assert_eq!(opening_a["result_summary"], b["result_summary"]);
        assert_eq!(opening_a["result_summary"], closing_a["result_summary"]);
        assert_eq!(integrity, "ok");

        serde_json::json!({
            "profile": "sqlite-cache-size-tuning-v1",
            "hypothesis": "32 MiB SQLite page cache versus 128 MiB on disposable read-only connections",
            "corpus": {
                "events": event_count,
                "events_per_record": events_per_record,
                "records": archive.records.len(),
                "capture_sources": archive.capture_sources.len(),
                "sessions": archive.sessions.len(),
                "runs": archive.runs.len(),
                "summaries": archive.summaries.len(),
                "files_touched": archive.files_touched.len()
            },
            "db_size_bytes": sqlite_footprint_bytes(&db_path),
            "schema_integrity_check": integrity,
            "plans": plans,
            "arms_in_order": {
                "opening_a_32_mib": opening_a,
                "b_128_mib": b,
                "closing_a_32_mib": closing_a
            },
            "warm_samples": sample_count,
            "sample_order": "A/B/A; five warm samples per arm",
            "canonical_reproduction_command": SQLITE_CACHE_TUNING_COMMAND,
            "executed_test_invocation": current_test_invocation(),
            "jj_commit_id": provenance.commit_id,
            "jj_change_id": provenance.change_id,
            "index_hypothesis": "not tested; this bounded spike used only the cache-size hypothesis"
        })
    }

    fn sqlite_tuning_connection(path: &Path, cache_size_kib: i64) -> rusqlite::Connection {
        let conn =
            rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        conn.execute_batch(&format!(
            "PRAGMA query_only = ON; PRAGMA temp_store = MEMORY; PRAGMA cache_size = {cache_size_kib};"
        ))
        .unwrap();
        conn
    }

    fn sqlite_tuning_plans(path: &Path) -> serde_json::Value {
        let conn = sqlite_tuning_connection(path, -32_768);
        serde_json::json!({
            "unfiltered": sqlite_tuning_explain(&conn, SQLITE_TUNING_UNFILTERED_SQL, false),
            "filtered": sqlite_tuning_explain(&conn, SQLITE_TUNING_FILTERED_SQL, true)
        })
    }

    fn sqlite_tuning_explain(
        conn: &rusqlite::Connection,
        sql: &str,
        filtered: bool,
    ) -> Vec<String> {
        let explain = format!("EXPLAIN QUERY PLAN {sql}");
        let mut stmt = conn.prepare(&explain).unwrap();
        let params: &[&dyn rusqlite::ToSql] = if filtered {
            &[&"perfneedle", &192_i64, &0_i64, &"codex", &"tool_call"]
        } else {
            &[&"perfneedle", &192_i64, &0_i64]
        };
        stmt.query_map(params, |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    }

    fn sqlite_cache_tuning_arm(
        path: &Path,
        cache_size_kib: i64,
        sample_count: usize,
    ) -> serde_json::Value {
        let conn = sqlite_tuning_connection(path, cache_size_kib);
        let effective_cache_size_kib: i64 = conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .unwrap();
        let warm_params_unfiltered: [&dyn rusqlite::ToSql; 3] = [&"perfneedle", &192_i64, &0_i64];
        let warm_params_filtered: [&dyn rusqlite::ToSql; 5] =
            [&"perfneedle", &192_i64, &0_i64, &"codex", &"tool_call"];
        sqlite_tuning_query(&conn, SQLITE_TUNING_UNFILTERED_SQL, &warm_params_unfiltered);
        sqlite_tuning_query(&conn, SQLITE_TUNING_FILTERED_SQL, &warm_params_filtered);

        let mut unfiltered_samples = Vec::with_capacity(sample_count);
        let mut filtered_samples = Vec::with_capacity(sample_count);
        let mut total_samples = Vec::with_capacity(sample_count);
        let mut unfiltered_ids = Vec::new();
        let mut filtered_ids = Vec::new();
        for _ in 0..sample_count {
            let total_started = std::time::Instant::now();
            let started = std::time::Instant::now();
            let current_unfiltered =
                sqlite_tuning_query(&conn, SQLITE_TUNING_UNFILTERED_SQL, &warm_params_unfiltered);
            unfiltered_samples.push(elapsed_ms(started.elapsed()));
            let started = std::time::Instant::now();
            let current_filtered =
                sqlite_tuning_query(&conn, SQLITE_TUNING_FILTERED_SQL, &warm_params_filtered);
            filtered_samples.push(elapsed_ms(started.elapsed()));
            total_samples.push(elapsed_ms(total_started.elapsed()));
            if unfiltered_ids.is_empty() {
                unfiltered_ids = current_unfiltered;
                filtered_ids = current_filtered;
            } else {
                assert_eq!(unfiltered_ids, current_unfiltered);
                assert_eq!(filtered_ids, current_filtered);
            }
        }

        serde_json::json!({
            "requested_cache_size_kib": cache_size_kib,
            "effective_cache_size_kib": effective_cache_size_kib,
            "effective_cache_size_mib": (-effective_cache_size_kib) as f64 / 1024.0,
            "unfiltered": timing_stats(&unfiltered_samples).to_json(),
            "filtered": timing_stats(&filtered_samples).to_json(),
            "total": timing_stats(&total_samples).to_json(),
            "result_summary": {
                "unfiltered": sqlite_tuning_result_summary(&unfiltered_ids),
                "filtered": sqlite_tuning_result_summary(&filtered_ids)
            }
        })
    }

    fn sqlite_tuning_result_summary(ids: &[String]) -> serde_json::Value {
        serde_json::json!({
            "count": ids.len(),
            "first": ids.first(),
            "last": ids.last()
        })
    }

    fn sqlite_tuning_query(
        conn: &rusqlite::Connection,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Vec<String> {
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map(params, |row| row.get(0))
            .unwrap()
            .map(|row| row.unwrap())
            .collect()
    }

    fn sqlite_tuning_integrity(path: &Path) -> String {
        let conn = sqlite_tuning_connection(path, -32_768);
        conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    #[ignore = "evidence-only SQLite search baseline; intentionally has no performance gates"]
    fn synthetic_search_sqlite_baseline_evidence() {
        let provenance = local_jj_provenance().unwrap_or_else(|error| {
            panic!("SQLite baseline requires jj commit/change provenance: {error}")
        });
        let evidence = sqlite_search_baseline_evidence(200_000, 50, 5, &provenance);
        println!(
            "sqlite search baseline evidence: {}",
            serde_json::to_string(&evidence).unwrap()
        );
    }

    #[test]
    #[ignore = "ticket #265 file-scope pushdown benchmark evidence"]
    fn synthetic_search_file_scope_pushdown_evidence() {
        let archive = synthetic_perf_archive(200_000, 50);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let options = PacketOptions {
            limit: 24,
            snippet_chars: 320,
            filters: SearchFilters {
                file: Some("profile_00/perf_profile.rs".into()),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Events,
            match_mode: SearchMatchMode::All,
        };
        let plain = PacketOptions {
            filters: SearchFilters::default(),
            ..options.clone()
        };

        fn measured(
            store: &Store,
            options: &PacketOptions,
            disabled: bool,
        ) -> (f64, Vec<String>, [u64; 3]) {
            DISABLE_FILE_SCOPE_PUSHDOWN.set(disabled);
            let before = RESIDUAL_COUNTS.get();
            let started = std::time::Instant::now();
            let packet = search_packet(store, "perfneedle", options).unwrap();
            (
                elapsed_ms(started.elapsed()),
                result_ids(&packet),
                std::array::from_fn(|index| RESIDUAL_COUNTS.get()[index] - before[index]),
            )
        }

        // Five warmups per shape, then five paired samples, matching #262.
        for index in 0..5 {
            for disabled in if index % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                measured(&store, &options, disabled);
                measured(&store, &plain, disabled);
            }
        }
        let mut residual_ms = Vec::new();
        let mut pushed_ms = Vec::new();
        let mut plain_before_ms = Vec::new();
        let mut plain_after_ms = Vec::new();
        let mut residual_counts = [0_u64; 3];
        let mut pushed_counts = [0_u64; 3];
        for index in 0..5 {
            let (residual, pushed) = if index % 2 == 0 {
                (
                    measured(&store, &options, true),
                    measured(&store, &options, false),
                )
            } else {
                let pushed = measured(&store, &options, false);
                let residual = measured(&store, &options, true);
                (residual, pushed)
            };
            let (before_ms, before_ids, examined_before) = residual;
            let (after_ms, after_ids, examined_after) = pushed;
            assert_eq!(before_ids, after_ids);
            assert_eq!(before_ids.len(), 24);
            residual_ms.push(before_ms);
            pushed_ms.push(after_ms);
            for index in 0..3 {
                residual_counts[index] += examined_before[index];
                pushed_counts[index] += examined_after[index];
            }

            let plain_pair = if index % 2 == 0 {
                (
                    measured(&store, &plain, true).0,
                    measured(&store, &plain, false).0,
                )
            } else {
                let after = measured(&store, &plain, false).0;
                let before = measured(&store, &plain, true).0;
                (before, after)
            };
            plain_before_ms.push(plain_pair.0);
            plain_after_ms.push(plain_pair.1);
        }
        DISABLE_FILE_SCOPE_PUSHDOWN.set(false);
        println!(
            "file-scope pushdown evidence: {}",
            serde_json::json!({
                "fixture_events": 200_000,
                "warmups_per_shape": 5,
                "samples": 5,
                "residual_only": { "timings": timing_stats(&residual_ms).to_json(), "examined": residual_counts[0], "rejected": residual_counts[1], "accepted": residual_counts[2] },
                "sql_plus_residual": { "timings": timing_stats(&pushed_ms).to_json(), "examined": pushed_counts[0], "rejected": pushed_counts[1], "accepted": pushed_counts[2] },
                "unfiltered_before": timing_stats(&plain_before_ms).to_json(),
                "unfiltered_after": timing_stats(&plain_after_ms).to_json(),
            })
        );
    }

    fn sqlite_search_baseline_evidence(
        event_count: usize,
        events_per_record: usize,
        sample_count: usize,
        provenance: &BaselineProvenance,
    ) -> serde_json::Value {
        let archive = synthetic_perf_archive(event_count, events_per_record);
        let corpus = PerfCorpus {
            records: archive.records.len(),
            capture_sources: archive.capture_sources.len(),
            sessions: archive.sessions.len(),
            runs: archive.runs.len(),
            events: archive.events.len(),
            summaries: archive.summaries.len(),
            files_touched: archive.files_touched.len(),
        };
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();

        let unfiltered_options = PacketOptions {
            limit: 24,
            snippet_chars: 320,
            filters: SearchFilters::default(),
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        };
        let filtered_options = PacketOptions {
            filters: SearchFilters {
                provider: Some(CaptureProvider::Codex),
                repo: Some("ctx".into()),
                event_type: Some(EventType::ToolCall),
                file: Some("perf_profile.rs".into()),
                ..SearchFilters::default()
            },
            ..unfiltered_options.clone()
        };

        // Warm both query shapes before collecting exactly `sample_count` paired samples.
        assert_perf_results(
            "unfiltered baseline warmup",
            search_packet(&store, "perfneedle", &unfiltered_options)
                .unwrap()
                .results
                .len(),
        );
        assert_perf_results(
            "filtered baseline warmup",
            search_packet(&store, "perfneedle", &filtered_options)
                .unwrap()
                .results
                .len(),
        );

        let mut unfiltered_samples = Vec::with_capacity(sample_count);
        let mut filtered_samples = Vec::with_capacity(sample_count);
        let mut total_samples = Vec::with_capacity(sample_count);
        let mut unfiltered_ids = Vec::new();
        let mut filtered_ids = Vec::new();
        for _ in 0..sample_count {
            // This wall-clock sample is the elapsed time for the complete ordered
            // unfiltered-then-filtered query pair, including the tiny call boundary.
            let pair_started = std::time::Instant::now();
            let started = std::time::Instant::now();
            let unfiltered = search_packet(&store, "perfneedle", &unfiltered_options).unwrap();
            let unfiltered_ms = elapsed_ms(started.elapsed());
            assert_perf_results("unfiltered baseline sample", unfiltered.results.len());

            let started = std::time::Instant::now();
            let filtered = search_packet(&store, "perfneedle", &filtered_options).unwrap();
            let filtered_ms = elapsed_ms(started.elapsed());
            assert_perf_results("filtered baseline sample", filtered.results.len());

            let current_unfiltered_ids = result_ids(&unfiltered);
            let current_filtered_ids = result_ids(&filtered);
            if unfiltered_ids.is_empty() {
                unfiltered_ids = current_unfiltered_ids;
                filtered_ids = current_filtered_ids;
            } else {
                assert_eq!(unfiltered_ids, current_unfiltered_ids);
                assert_eq!(filtered_ids, current_filtered_ids);
            }
            unfiltered_samples.push(unfiltered_ms);
            filtered_samples.push(filtered_ms);
            total_samples.push(elapsed_ms(pair_started.elapsed()));
        }

        let db_path = store.path().to_path_buf();
        drop(store);
        serde_json::json!({
            "profile": "sqlite-search-baseline-v1",
            "corpus_count": corpus.events,
            "corpus": {
                "records": corpus.records,
                "capture_sources": corpus.capture_sources,
                "sessions": corpus.sessions,
                "runs": corpus.runs,
                "events": corpus.events,
                "summaries": corpus.summaries,
                "files_touched": corpus.files_touched,
                "events_per_record": events_per_record,
            },
            "db_size_bytes": sqlite_footprint_bytes(&db_path),
            "canonical_reproduction_command": SQLITE_SEARCH_BASELINE_COMMAND,
            "executed_test_invocation": current_test_invocation(),
            "jj_commit_id": provenance.commit_id,
            "jj_change_id": provenance.change_id,
            "warm_samples": sample_count,
            "unfiltered": {
                "timings": timing_stats(&unfiltered_samples).to_json(),
                "ordered_result_ids": unfiltered_ids,
            },
            "filtered": {
                "timings": timing_stats(&filtered_samples).to_json(),
                "ordered_result_ids": filtered_ids,
            },
            "paired_queries_wall_clock": {
                "meaning": "elapsed wall-clock time enclosing each ordered unfiltered-then-filtered query pair",
                "timings": timing_stats(&total_samples).to_json(),
            },
        })
    }

    struct BaselineProvenance {
        commit_id: String,
        change_id: String,
    }

    impl BaselineProvenance {
        fn new(
            commit_id: impl Into<String>,
            change_id: impl Into<String>,
        ) -> std::result::Result<Self, String> {
            let provenance = Self {
                commit_id: commit_id.into(),
                change_id: change_id.into(),
            };
            if provenance.commit_id.trim().is_empty() {
                return Err("commit ID must not be empty".into());
            }
            if provenance.change_id.trim().is_empty() {
                return Err("change ID must not be empty".into());
            }
            Ok(provenance)
        }
    }

    fn current_test_invocation() -> serde_json::Value {
        serde_json::json!({
            "executable": std::env::current_exe().ok().map(|path| path.display().to_string()),
            "args": std::env::args_os()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        })
    }

    #[test]
    fn sqlite_search_baseline_has_complete_repeatable_ungated_evidence() {
        let provenance = BaselineProvenance::new("fixture-commit-id", "fixture-change-id").unwrap();
        let first = sqlite_search_baseline_evidence(40, 10, 5, &provenance);
        let second = sqlite_search_baseline_evidence(40, 10, 5, &provenance);

        for evidence in [&first, &second] {
            assert_eq!(evidence["profile"], "sqlite-search-baseline-v1");
            assert_eq!(evidence["corpus_count"], 40);
            assert_eq!(evidence["corpus"]["events"], 40);
            assert_eq!(evidence["corpus"]["records"], 4);
            assert_eq!(evidence["corpus"]["events_per_record"], 10);
            assert!(evidence["db_size_bytes"].as_u64().unwrap() > 0);
            assert_eq!(
                evidence["canonical_reproduction_command"],
                SQLITE_SEARCH_BASELINE_COMMAND
            );
            assert!(evidence["executed_test_invocation"]["args"].is_array());
            assert_eq!(evidence["jj_commit_id"], "fixture-commit-id");
            assert_eq!(evidence["jj_change_id"], "fixture-change-id");
            assert!(!evidence["jj_commit_id"].as_str().unwrap().is_empty());
            assert!(!evidence["jj_change_id"].as_str().unwrap().is_empty());
            assert_eq!(evidence["warm_samples"], 5);
            for profile in ["unfiltered", "filtered"] {
                assert_eq!(evidence[profile]["timings"]["sample_count"], 5);
                assert_eq!(
                    evidence[profile]["timings"]["samples_ms"]
                        .as_array()
                        .unwrap()
                        .len(),
                    5
                );
                assert!(evidence[profile]["timings"]["p50_ms"].is_number());
                assert!(evidence[profile]["timings"]["p95_ms"].is_number());
                assert!(!evidence[profile]["ordered_result_ids"]
                    .as_array()
                    .unwrap()
                    .is_empty());
            }
            let paired = &evidence["paired_queries_wall_clock"];
            assert!(paired["meaning"].as_str().unwrap().contains("wall-clock"));
            assert_eq!(paired["timings"]["sample_count"], 5);
            assert_eq!(paired["timings"]["samples_ms"].as_array().unwrap().len(), 5);
            assert!(paired["timings"]["p50_ms"].is_number());
            assert!(paired["timings"]["p95_ms"].is_number());
            assert!(evidence.get("thresholds").is_none());
            assert!(evidence.get("checks").is_none());
        }
        assert_eq!(
            first["unfiltered"]["ordered_result_ids"],
            second["unfiltered"]["ordered_result_ids"]
        );
        assert_eq!(
            first["filtered"]["ordered_result_ids"],
            second["filtered"]["ordered_result_ids"]
        );
    }

    #[test]
    fn sqlite_search_baseline_rejects_incomplete_provenance() {
        assert!(BaselineProvenance::new("", "change-id").is_err());
        assert!(BaselineProvenance::new("commit-id", "  ").is_err());
    }

    struct PerfCorpus {
        records: usize,
        capture_sources: usize,
        sessions: usize,
        runs: usize,
        events: usize,
        summaries: usize,
        files_touched: usize,
    }

    #[derive(Clone, Copy)]
    struct PerfThresholds {
        import_min_events_per_sec: f64,
        search_p95_ms: f64,
        filtered_search_p95_ms: f64,
        max_db_bytes_per_event: u64,
    }

    struct PerfTimingStats {
        samples_ms: Vec<f64>,
        p50_ms: f64,
        p95_ms: f64,
        min_ms: f64,
        max_ms: f64,
    }

    impl PerfTimingStats {
        fn to_json(&self) -> serde_json::Value {
            serde_json::json!({
                "sample_count": self.samples_ms.len(),
                "samples_ms": self.samples_ms,
                "p50_ms": self.p50_ms,
                "p95_ms": self.p95_ms,
                "min_ms": self.min_ms,
                "max_ms": self.max_ms
            })
        }
    }

    fn synthetic_perf_archive(
        event_count: usize,
        events_per_record: usize,
    ) -> SessionHistoryArchive {
        let mut archive = SessionHistoryArchive::default();
        let record_count = event_count.div_ceil(events_per_record);
        let workspace_id = perf_uuid(0x5000, 0);
        archive.vcs_workspaces.push(VcsWorkspace {
            id: workspace_id,
            kind: VcsKind::Git,
            root_path: "/workspace/ctx".into(),
            repo_fingerprint: "git:ctx-search-perf".into(),
            primary_remote_url_normalized: Some("https://github.com/ctxrs/ctx".into()),
            host: VcsHost::Github,
            owner: Some("ctxrs".into()),
            name: Some("ctx".into()),
            monorepo_subpath: None,
            timestamps: timestamps(),
            source_id: None,
            sync: sync_metadata(),
        });

        for record_index in 0..record_count {
            let record_id = perf_uuid(0x1000, record_index as u64);
            let source_id = perf_uuid(0x1100, record_index as u64);
            let session_id = perf_uuid(0x2000, record_index as u64);
            let run_id = perf_uuid(0x3000, record_index as u64);
            let summary_id = perf_uuid(0x4000, record_index as u64);
            let file_id = perf_uuid(0x4100, record_index as u64);
            let time = fixed_time() + chrono::Duration::seconds(record_index as i64);

            let mut record = HistoryRecord::new(
                format!("Synthetic perf profile {record_index:05}"),
                format!(
                    "perfneedle import search retrieval profile record {record_index:05}; \
                     routing storage ranking citations threshold evidence {}",
                    "detail ".repeat(8)
                ),
                vec![
                    "perf".into(),
                    "synthetic".into(),
                    format!("bucket-{:02}", record_index % 32),
                ],
                "task",
                Some("/workspace/ctx".into()),
            );
            record.id = record_id;
            record.created_at = time;
            record.updated_at = time;
            archive.records.push(record);

            archive.capture_sources.push(CaptureSource {
                id: source_id,
                descriptor: CaptureSourceDescriptor {
                    kind: CaptureSourceKind::ProviderImport,
                    provider: CaptureProvider::Codex,
                    machine_id: "synthetic-perf-host".into(),
                    process_id: None,
                    cwd: Some("/workspace/ctx".into()),
                    raw_source_path: Some(format!(
                        "/workspace/ctx/.ctx/synthetic/perf-session-{record_index:05}.jsonl"
                    )),
                    external_session_id: Some(format!("perf-session-{record_index:05}")),
                },
                started_at: time,
                ended_at: Some(time + chrono::Duration::seconds(events_per_record as i64)),
                sync: SyncMetadata {
                    metadata: serde_json::json!({
                        "source_format": "synthetic_perf_jsonl",
                        "cursor": {
                            "after": {
                                "stream": "provider:codex:synthetic_perf_jsonl",
                                "cursor": format!("line:{}", record_index * events_per_record),
                                "observed_at": time.to_rfc3339()
                            }
                        }
                    }),
                    ..sync_metadata()
                },
            });

            archive.sessions.push(Session {
                id: session_id,
                history_record_id: Some(record_id),
                parent_session_id: None,
                root_session_id: None,
                capture_source_id: Some(source_id),
                provider: CaptureProvider::Codex,
                external_session_id: Some(format!("perf-session-{record_index:05}")),
                external_agent_id: Some(format!("agent-{record_index:05}")),
                agent_type: AgentType::Primary,
                role_hint: Some("implementation-worker".into()),
                is_primary: true,
                status: SessionStatus::Imported,
                transcript_blob_id: None,
                started_at: time,
                ended_at: Some(time + chrono::Duration::seconds(events_per_record as i64)),
                timestamps: EntityTimestamps {
                    created_at: time,
                    updated_at: time,
                },
                sync: sync_metadata(),
            });

            archive.runs.push(Run {
                id: run_id,
                history_record_id: Some(record_id),
                session_id: Some(session_id),
                run_type: RunType::Command,
                status: RunStatus::Succeeded,
                started_at: time,
                ended_at: Some(time + chrono::Duration::seconds(1)),
                exit_code: Some(0),
                cwd: Some("/workspace/ctx".into()),
                command_preview: Some(format!(
                    "ctx search perfneedle --refresh off --limit 5 # synthetic record {record_index:05}"
                )),
                input_blob_id: None,
                output_blob_id: None,
                timestamps: EntityTimestamps {
                    created_at: time,
                    updated_at: time,
                },
                source_id: Some(source_id),
                sync: sync_metadata(),
            });

            archive.summaries.push(Summary {
                id: summary_id,
                history_record_id: Some(record_id),
                session_id: Some(session_id),
                kind: SummaryKind::ImportedProviderSummary,
                model_or_source: Some("synthetic-perf".into()),
                text: format!(
                    "perfneedle summary for import search retrieval record {record_index:05}; \
                     captures commands, files, and citations"
                ),
                citations: Vec::new(),
                timestamps: EntityTimestamps {
                    created_at: time,
                    updated_at: time,
                },
                source_id: Some(source_id),
                sync: sync_metadata(),
            });

            archive.files_touched.push(FileTouched {
                id: file_id,
                history_record_id: Some(record_id),
                run_id: Some(run_id),
                event_id: None,
                vcs_workspace_id: Some(workspace_id),
                path: format!(
                    "crates/perf/profile_{:02}/perf_profile.rs",
                    record_index % 24
                ),
                change_kind: Some(FileChangeKind::Modified),
                old_path: None,
                line_count_delta: Some((record_index % 17) as i64 - 3),
                confidence: Confidence::Explicit,
                timestamps: EntityTimestamps {
                    created_at: time,
                    updated_at: time,
                },
                source_id: Some(source_id),
                sync: sync_metadata(),
            });

            let event_start = record_index * events_per_record;
            let event_end = event_count.min(event_start + events_per_record);
            for event_index in event_start..event_end {
                let local_index = event_index - event_start;
                let event_time = time + chrono::Duration::milliseconds(local_index as i64);
                let event_type = match local_index % 5 {
                    0 => EventType::ToolCall,
                    1 => EventType::ToolOutput,
                    2 => EventType::Message,
                    3 => EventType::CommandOutput,
                    _ => EventType::Notice,
                };
                let role = match event_type {
                    EventType::Message => Some(EventRole::User),
                    EventType::ToolOutput | EventType::CommandOutput => Some(EventRole::Tool),
                    EventType::ToolCall => Some(EventRole::Assistant),
                    _ => Some(EventRole::System),
                };
                let event_id = perf_uuid(0x6000, event_index as u64);
                archive.events.push(Event {
                    id: event_id,
                    seq: (event_index + 1) as u64,
                    history_record_id: Some(record_id),
                    session_id: Some(session_id),
                    run_id: Some(run_id),
                    event_type,
                    role,
                    occurred_at: event_time,
                    capture_source_id: Some(source_id),
                    payload: serde_json::json!({
                        "cursor": format!("line:{}", local_index + 1),
                        "body": {
                            "text": format!(
                                "perfneedle import search retrieval profile record {record_index:05} event {local_index:02} indexed event {event_index:06}"
                            )
                        }
                    }),
                    payload_blob_id: None,
                    dedupe_key: (local_index == 0).then(|| {
                        format!("provider:codex:s{record_index:05}:{local_index}:h{event_index:06}")
                    }),
                    redaction_state: RedactionState::SafePreview,
                    sync: sync_metadata(),
                });
            }
        }

        archive
    }

    fn append_synthetic_perf_record(
        archive: &mut SessionHistoryArchive,
        record_index: usize,
        _batch_start_event: usize,
        total_events: usize,
        events_per_record: usize,
        seed: u64,
    ) -> std::result::Result<(), String> {
        if archive.vcs_workspaces.is_empty() {
            archive.vcs_workspaces.push(VcsWorkspace {
                id: perf_uuid_checked(0x5000, seed)?,
                kind: VcsKind::Git,
                root_path: "/workspace/ctx".into(),
                repo_fingerprint: "git:ctx-large-profile".into(),
                primary_remote_url_normalized: None,
                host: VcsHost::Unknown,
                owner: Some("synthetic".into()),
                name: Some("ctx".into()),
                monorepo_subpath: None,
                timestamps: timestamps(),
                source_id: None,
                sync: sync_metadata(),
            });
        }
        let id_index = (record_index as u64)
            .checked_add(seed)
            .ok_or("uuid index overflow")?;
        let record_id = perf_uuid_checked(0x7000, id_index)?;
        let source_id = perf_uuid_checked(0x7100, id_index)?;
        let session_id = perf_uuid_checked(0x7200, id_index)?;
        let run_id = perf_uuid_checked(0x7300, id_index)?;
        let summary_id = perf_uuid_checked(0x7400, id_index)?;
        let file_id = perf_uuid_checked(0x7500, id_index)?;
        let time = fixed_time() + chrono::Duration::seconds(record_index as i64);
        let mut record = HistoryRecord::new(
            format!("Large synthetic profile {record_index:08}"),
            format!(
                "perfneedle deterministic large profile record {record_index:08} seed {seed}; {}",
                "payload ".repeat(16)
            ),
            vec![
                "large-profile".into(),
                format!("bucket-{:02}", record_index % 64),
            ],
            "task",
            Some("/workspace/ctx".into()),
        );
        record.id = record_id;
        record.created_at = time;
        record.updated_at = time;
        archive.records.push(record);
        archive.capture_sources.push(CaptureSource {
            id: source_id,
            descriptor: CaptureSourceDescriptor {
                kind: CaptureSourceKind::ProviderImport,
                provider: CaptureProvider::Codex,
                machine_id: "synthetic-large-profile".into(),
                process_id: None,
                cwd: Some("/workspace/ctx".into()),
                raw_source_path: Some(format!(
                    "/synthetic/ctx-large-profile-{record_index:08}.jsonl"
                )),
                external_session_id: Some(format!("large-profile-{record_index:08}")),
            },
            started_at: time,
            ended_at: Some(time),
            sync: sync_metadata(),
        });
        archive.sessions.push(Session {
            id: session_id,
            history_record_id: Some(record_id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: Some(source_id),
            provider: CaptureProvider::Codex,
            external_session_id: Some(format!("large-profile-{record_index:08}")),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: Some("synthetic-profile".into()),
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: time,
            ended_at: Some(time),
            timestamps: timestamps(),
            sync: sync_metadata(),
        });
        archive.runs.push(Run {
            id: run_id,
            history_record_id: Some(record_id),
            session_id: Some(session_id),
            run_type: RunType::Command,
            status: RunStatus::Succeeded,
            started_at: time,
            ended_at: Some(time),
            exit_code: Some(0),
            cwd: Some("/workspace/ctx".into()),
            command_preview: Some("ctx profile synthetic".into()),
            input_blob_id: None,
            output_blob_id: None,
            timestamps: timestamps(),
            source_id: Some(source_id),
            sync: sync_metadata(),
        });
        archive.summaries.push(Summary {
            id: summary_id,
            history_record_id: Some(record_id),
            session_id: Some(session_id),
            kind: SummaryKind::ImportedProviderSummary,
            model_or_source: Some("synthetic-large-profile".into()),
            text: format!("perfneedle summary deterministic record {record_index:08}"),
            citations: Vec::new(),
            timestamps: timestamps(),
            source_id: Some(source_id),
            sync: sync_metadata(),
        });
        archive.files_touched.push(FileTouched {
            id: file_id,
            history_record_id: Some(record_id),
            run_id: Some(run_id),
            event_id: None,
            vcs_workspace_id: archive.vcs_workspaces.first().map(|w| w.id),
            path: format!(
                "crates/perf/profile_{:02}/perf_profile.rs",
                record_index % 24
            ),
            change_kind: Some(FileChangeKind::Modified),
            old_path: None,
            line_count_delta: Some(1),
            confidence: Confidence::Explicit,
            timestamps: timestamps(),
            source_id: Some(source_id),
            sync: sync_metadata(),
        });
        let event_start = record_index * events_per_record;
        let event_end = total_events.min(event_start + events_per_record);
        for event_index in event_start..event_end {
            let local_index = event_index - event_start;
            archive.events.push(Event { id: perf_uuid_checked(0x7600, (event_index as u64).checked_add(seed).ok_or("uuid index overflow")?)?, seq: (event_index + 1) as u64, history_record_id: Some(record_id), session_id: Some(session_id), run_id: Some(run_id), event_type: if local_index % 2 == 0 { EventType::ToolCall } else { EventType::Message }, role: Some(if local_index % 2 == 0 { EventRole::Assistant } else { EventRole::User }), occurred_at: time + chrono::Duration::milliseconds(local_index as i64), capture_source_id: Some(source_id), payload: serde_json::json!({"body":{"text":format!("perfneedle deterministic event {event_index:012} record {record_index:08} seed {seed}")}}), payload_blob_id: None, dedupe_key: Some(format!("ctx-large-profile:{seed}:{event_index}")), redaction_state: RedactionState::SafePreview, sync: sync_metadata() });
        }
        Ok(())
    }

    fn peak_rss() -> serde_json::Value {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        if rc != 0 {
            return serde_json::json!({"peak_bytes": null, "source": "getrusage", "semantics": "unavailable"});
        }
        let usage = unsafe { usage.assume_init() };
        #[cfg(target_os = "macos")]
        let (bytes, semantics) = (
            usage.ru_maxrss as u64,
            "getrusage ru_maxrss high-water bytes on macOS",
        );
        #[cfg(target_os = "linux")]
        let (bytes, semantics) = (
            (usage.ru_maxrss as u64).saturating_mul(1024),
            "getrusage ru_maxrss high-water KiB converted to bytes on Linux",
        );
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let (bytes, semantics) = (usage.ru_maxrss as u64, "getrusage ru_maxrss platform units");
        serde_json::json!({"peak_bytes": bytes, "source": "getrusage(RUSAGE_SELF).ru_maxrss", "semantics": semantics})
    }
    fn local_jj_provenance() -> std::result::Result<BaselineProvenance, String> {
        let output = std::process::Command::new("jj")
            .args([
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\" ++ change_id ++ \"\\n\"",
            ])
            .output()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    "jj executable is unavailable".to_owned()
                } else {
                    format!("failed to execute jj: {error}")
                }
            })?;
        if !output.status.success() {
            return Err(format!(
                "jj command failed with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let stdout = String::from_utf8(output.stdout)
            .map_err(|error| format!("jj returned non-UTF-8 provenance: {error}"))?;
        let mut ids = stdout.lines().map(str::trim).filter(|id| !id.is_empty());
        let commit_id = ids
            .next()
            .ok_or_else(|| "jj returned an empty commit ID".to_owned())?;
        let change_id = ids
            .next()
            .ok_or_else(|| "jj returned an empty change ID".to_owned())?;
        BaselineProvenance::new(commit_id, change_id)
    }

    fn local_jj_id(kind: &str) -> Option<String> {
        let template = if kind == "commit" {
            "commit_id"
        } else {
            "change_id"
        };
        std::process::Command::new("jj")
            .args(["log", "-r", "@", "--no-graph", "-T", template])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|id| id.trim().to_owned())
            .filter(|id| !id.is_empty())
    }

    fn perf_uuid(namespace: u16, index: u64) -> Uuid {
        Uuid::parse_str(&format!("018f45d0-{namespace:04x}-7000-8000-{index:012x}")).unwrap()
    }

    fn perf_event_count() -> usize {
        let requested = env_usize("CTX_SEARCH_PERF_EVENTS").unwrap_or_else(|| {
            if env_flag("CTX_SEARCH_PERF_SLOW") {
                100_000
            } else {
                10_000
            }
        });
        requested.max(10_000)
    }

    fn perf_events_per_record() -> usize {
        env_usize("CTX_SEARCH_PERF_EVENTS_PER_RECORD")
            .unwrap_or(50)
            .clamp(1, 50)
    }

    fn perf_repeats(name: &str, default: usize) -> usize {
        env_usize(name).unwrap_or(default).clamp(1, 50)
    }

    fn perf_thresholds(event_count: usize) -> PerfThresholds {
        let slow = event_count >= 100_000;
        PerfThresholds {
            import_min_events_per_sec: env_f64("CTX_SEARCH_PERF_IMPORT_MIN_EVENTS_PER_SEC")
                .unwrap_or(if slow { 25.0 } else { 40.0 }),
            search_p95_ms: env_f64("CTX_SEARCH_PERF_SEARCH_P95_MS").unwrap_or(if slow {
                2_500.0
            } else {
                1_500.0
            }),
            filtered_search_p95_ms: env_f64("CTX_SEARCH_PERF_FILTERED_SEARCH_P95_MS")
                .unwrap_or(if slow { 8_000.0 } else { 5_000.0 }),
            max_db_bytes_per_event: env_u64("CTX_SEARCH_PERF_MAX_DB_BYTES_PER_EVENT")
                .unwrap_or(if slow { 10_240 } else { 12_288 }),
        }
    }

    fn env_flag(name: &str) -> bool {
        std::env::var(name).is_ok_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on" | "slow"
            )
        })
    }

    fn env_usize(name: &str) -> Option<usize> {
        std::env::var(name).ok()?.parse().ok()
    }

    fn env_u64(name: &str) -> Option<u64> {
        std::env::var(name).ok()?.parse().ok()
    }

    fn env_f64(name: &str) -> Option<f64> {
        std::env::var(name).ok()?.parse().ok()
    }

    fn assert_perf_results(label: &str, result_count: usize) {
        assert!(result_count > 0, "{label} returned no results");
    }

    fn elapsed_ms(duration: std::time::Duration) -> f64 {
        rounded(duration.as_secs_f64() * 1000.0)
    }

    struct MeasuredProfileSearch {
        packet: SearchPacket,
        phases: SearchPhaseEvidence,
    }

    fn measured_profile_search(
        store: &Store,
        query: &str,
        options: &PacketOptions,
    ) -> std::result::Result<MeasuredProfileSearch, String> {
        let event_statements = store.event_search_page_executions();
        let rows = store.event_search_rows_hydrated();
        let relations = store.relation_batch_executions();
        let loaders = store.search_hydration_loader_executions();
        let residual = RESIDUAL_COUNTS.get();
        let started = std::time::Instant::now();
        let (packet, capture) = capture_search_phases(|| search_packet(store, query, options));
        let total_ns = duration_ns(started.elapsed());
        let packet = packet.map_err(|error| error.to_string())?;
        let accounted_ns = capture
            .preparation_ns
            .saturating_add(capture.execution_hydration_ns)
            .saturating_add(capture.assembly_ns);
        let residual_after = RESIDUAL_COUNTS.get();
        let loaders_after = store.search_hydration_loader_executions();
        let phases = SearchPhaseEvidence {
            path: match capture.path {
                SearchProfilePath::FastEvent => "fast_event",
                SearchProfilePath::FallbackRanked => "fallback_ranked_fts",
                SearchProfilePath::Unknown => "unknown",
            }
            .to_owned(),
            filter_source_preparation_ms: ns_ms(capture.preparation_ns),
            execution_candidate_paging_and_base_context_hydration_ms: ns_ms(
                capture.execution_hydration_ns,
            ),
            result_assembly_clustering_sorting_projection_ms: ns_ms(capture.assembly_ns),
            unattributed_overhead_ms: ns_ms(total_ns.saturating_sub(accounted_ns)),
            end_to_end_ms: ns_ms(total_ns),
            event_search_statements: store
                .event_search_page_executions()
                .saturating_sub(event_statements),
            candidate_rows_hydrated: store.event_search_rows_hydrated().saturating_sub(rows),
            candidates_examined: residual_after[0].saturating_sub(residual[0]),
            candidates_rejected: residual_after[1].saturating_sub(residual[1]),
            candidates_accepted: residual_after[2].saturating_sub(residual[2]),
            relation_hydration_statements: store
                .relation_batch_executions()
                .saturating_sub(relations),
            hydration_loader_statements: [
                loaders_after[0].saturating_sub(loaders[0]),
                loaders_after[1].saturating_sub(loaders[1]),
            ],
        };
        Ok(MeasuredProfileSearch { packet, phases })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RankedFtsHydrationIdentity {
        selected_candidate_count: usize,
        selected_candidate_digest: String,
        candidate_record_ids: Vec<String>,
        candidate_record_digest: String,
        result_ids: Vec<String>,
        result_digest: String,
    }

    #[derive(Debug, Clone, PartialEq)]
    struct RankedFtsHydrationMeasurement {
        identity: RankedFtsHydrationIdentity,
        selection_ms: f64,
        hydration_ms: f64,
        unattributed_overhead_ms: f64,
        end_to_end_ms: f64,
    }

    fn measured_ranked_fts_hydration(
        store: &Store,
        query: &str,
        options: &PacketOptions,
    ) -> std::result::Result<RankedFtsHydrationMeasurement, String> {
        let options = normalized_options(options);
        let file_scope = file_filter_scope(store, &options.filters).map_err(|e| e.to_string())?;
        let plan = SearchQueryPlan::new(options.match_mode, [query]);
        let target_candidates = options.limit.saturating_add(1);
        let page_size = FILTERED_SEARCH_PAGE_SIZE.max(target_candidates);
        let selection_started = std::time::Instant::now();
        let ranked_hits = store
            .search_record_hits_fts_plan(
                &plan,
                page_size.saturating_mul(FILTERED_SEARCH_MAX_PAGES),
                0,
            )
            .map_err(|e| e.to_string())?
            .ok_or("ranked FTS projection unavailable")?;
        let selection_ms = elapsed_ms(selection_started.elapsed());
        let selected_candidate_ids = ranked_hits
            .iter()
            .map(|hit| hit.record_id.to_string())
            .collect::<Vec<_>>();

        let hydration_started = std::time::Instant::now();
        let terms = plan
            .clauses
            .iter()
            .flat_map(|clause| clause.terms.clone())
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();
        let mut seen = BTreeSet::new();
        for (page_index, hits) in ranked_hits.chunks(page_size).enumerate() {
            let records = hits
                .iter()
                .map(|hit| store.get_record(hit.record_id))
                .collect::<ctx_history_store::Result<Vec<_>>>()
                .map_err(|e| e.to_string())?;
            let page_len = records.len();
            let page_records = records
                .into_iter()
                .filter(|record| {
                    seen.insert(record.id)
                        && file_scope.as_ref().map_or(true, |scope| {
                            scope.history_record_ids.is_empty()
                                || scope.history_record_ids.contains(&record.id)
                        })
                })
                .collect();
            candidates.extend(
                candidates_for_records(
                    store,
                    page_records,
                    Some(&plan),
                    &terms,
                    &options.filters,
                    file_scope.as_ref(),
                    HydrationIntent::Full,
                )
                .map_err(|e| e.to_string())?,
            );
            if candidates.len() >= target_candidates || page_len < page_size {
                break;
            }
            if page_index + 1 >= FILTERED_SEARCH_MAX_PAGES {
                break;
            }
        }
        normalize_scores(&mut candidates);
        candidates.sort_by(compare_candidates);
        if candidates.len() > target_candidates {
            candidates.truncate(target_candidates);
        }
        let hydration_ms = elapsed_ms(hydration_started.elapsed());
        let end_to_end_ms = elapsed_ms(selection_started.elapsed());
        let accounted_ms = selection_ms + hydration_ms;
        let unattributed_overhead_ms = rounded((end_to_end_ms - accounted_ms).max(0.0));
        let candidate_record_ids = candidates
            .iter()
            .map(|candidate| candidate.record.id.to_string())
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        push_candidate_results(&mut results, &candidates, query, &options);
        let result_ids = results
            .iter()
            .map(|result| result.record_id.to_string())
            .collect::<Vec<_>>();
        Ok(RankedFtsHydrationMeasurement {
            identity: RankedFtsHydrationIdentity {
                selected_candidate_count: selected_candidate_ids.len(),
                selected_candidate_digest: digest_strings(&selected_candidate_ids),
                candidate_record_digest: digest_strings(&candidate_record_ids),
                result_digest: digest_strings(&result_ids),
                candidate_record_ids,
                result_ids,
            },
            selection_ms,
            hydration_ms,
            unattributed_overhead_ms,
            end_to_end_ms,
        })
    }

    fn ranked_fts_production_identity(
        store: &Store,
        query: &str,
        options: &PacketOptions,
    ) -> std::result::Result<RankedFtsHydrationIdentity, String> {
        let options = normalized_options(options);
        let file_scope = file_filter_scope(store, &options.filters).map_err(|e| e.to_string())?;
        let plan = SearchQueryPlan::new(options.match_mode, [query]);
        let page_size = FILTERED_SEARCH_PAGE_SIZE.max(options.limit.saturating_add(1));
        let selected_candidate_ids = store
            .search_record_hits_fts_plan(
                &plan,
                page_size.saturating_mul(FILTERED_SEARCH_MAX_PAGES),
                0,
            )
            .map_err(|e| e.to_string())?
            .ok_or("ranked FTS projection unavailable")?
            .into_iter()
            .map(|hit| hit.record_id.to_string())
            .collect::<Vec<_>>();
        let CandidateSearch { mut candidates, .. } = ranked_candidates(
            store,
            Some(&plan),
            &options,
            file_scope.as_ref(),
            HydrationIntent::Full,
        )
        .map_err(|e| e.to_string())?;
        normalize_scores(&mut candidates);
        candidates.sort_by(compare_candidates);
        let target_candidates = options.limit.saturating_add(1);
        if candidates.len() > target_candidates {
            candidates.truncate(target_candidates);
        }
        let candidate_record_ids = candidates
            .iter()
            .map(|candidate| candidate.record.id.to_string())
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        push_candidate_results(&mut results, &candidates, query, &options);
        let result_ids = results
            .iter()
            .map(|result| result.record_id.to_string())
            .collect::<Vec<_>>();
        Ok(RankedFtsHydrationIdentity {
            selected_candidate_count: selected_candidate_ids.len(),
            selected_candidate_digest: digest_strings(&selected_candidate_ids),
            candidate_record_digest: digest_strings(&candidate_record_ids),
            result_digest: digest_strings(&result_ids),
            candidate_record_ids,
            result_ids,
        })
    }

    fn ranked_fts_hydration_stats(
        samples: Vec<RankedFtsHydrationMeasurement>,
        default_filtered_route: String,
        default_filtered_result_digest: String,
        manual: bool,
    ) -> std::result::Result<RankedFtsHydrationStats, String> {
        let Some(first) = samples.first() else {
            return Err("ranked FTS hydration samples must not be empty".into());
        };
        let first_candidate_ids = first
            .identity
            .candidate_record_ids
            .iter()
            .collect::<BTreeSet<_>>();
        let candidate_ids_preserved = samples.iter().all(|sample| {
            sample
                .identity
                .candidate_record_ids
                .iter()
                .collect::<BTreeSet<_>>()
                == first_candidate_ids
        });
        let candidate_order_preserved = samples.iter().all(|sample| {
            sample.identity.candidate_record_ids == first.identity.candidate_record_ids
        });
        let candidate_digests_preserved = samples.iter().all(|sample| {
            sample.identity.selected_candidate_digest == first.identity.selected_candidate_digest
                && sample.identity.candidate_record_digest == first.identity.candidate_record_digest
                && sample.identity.result_digest == first.identity.result_digest
        });
        let calculate = |select: fn(&RankedFtsHydrationMeasurement) -> f64, percentile: f64| {
            let mut values = samples.iter().map(select).collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            percentile_sorted(&values, percentile)
        };
        let percentiles = |percentile| RankedFtsHydrationPercentiles {
            selection_ms: calculate(|sample| sample.selection_ms, percentile),
            hydration_ms: calculate(|sample| sample.hydration_ms, percentile),
            unattributed_overhead_ms: calculate(
                |sample| sample.unattributed_overhead_ms,
                percentile,
            ),
            end_to_end_ms: calculate(|sample| sample.end_to_end_ms, percentile),
        };
        let p50_ms = percentiles(50.0);
        let p95_ms = percentiles(95.0);
        let ticket_qualified = manual && samples.len() == 5;
        let samples = samples
            .into_iter()
            .map(|sample| RankedFtsHydrationSample {
                selection_ms: sample.selection_ms,
                hydration_ms: sample.hydration_ms,
                unattributed_overhead_ms: sample.unattributed_overhead_ms,
                end_to_end_ms: sample.end_to_end_ms,
                selected_candidate_count: sample.identity.selected_candidate_count,
                selected_candidate_digest: sample.identity.selected_candidate_digest,
                candidate_record_ids: sample.identity.candidate_record_ids,
                candidate_record_digest: sample.identity.candidate_record_digest,
                result_ids: sample.identity.result_ids,
                result_digest: sample.identity.result_digest,
            })
            .collect();
        Ok(RankedFtsHydrationStats {
            p50_ms,
            p95_ms,
            samples,
            attribution_route: "fallback_ranked_fts".into(),
            production_reference: "production_ranked_fts_fallback".into(),
            default_filtered_route,
            default_filtered_result_digest,
            ticket_qualified,
            candidate_ids_preserved,
            candidate_order_preserved,
            candidate_digests_preserved,
        })
    }

    impl RankedFtsHydrationSample {
        fn reconciles(&self) -> bool {
            let values = [
                self.selection_ms,
                self.hydration_ms,
                self.unattributed_overhead_ms,
                self.end_to_end_ms,
            ];
            if values
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
            {
                return false;
            }
            (self.selection_ms + self.hydration_ms + self.unattributed_overhead_ms
                - self.end_to_end_ms)
                .abs()
                < 0.002
        }
    }

    fn ns_ms(ns: u64) -> f64 {
        rounded(ns as f64 / 1_000_000.0)
    }

    impl SearchPhaseEvidence {
        fn reconciles(&self) -> bool {
            let thousandths = |milliseconds: f64| {
                if !milliseconds.is_finite() || milliseconds < 0.0 {
                    return None;
                }
                let value = (milliseconds * 1000.0).round();
                if !value.is_finite() || value >= u64::MAX as f64 {
                    return None;
                }
                Some(value as u64)
            };
            let sum = [
                self.filter_source_preparation_ms,
                self.execution_candidate_paging_and_base_context_hydration_ms,
                self.result_assembly_clustering_sorting_projection_ms,
                self.unattributed_overhead_ms,
            ]
            .into_iter()
            .try_fold(0u128, |sum, milliseconds| {
                sum.checked_add(u128::from(thousandths(milliseconds)?))
            });
            let Some((sum, end_to_end)) = sum.zip(thousandths(self.end_to_end_ms)) else {
                return false;
            };
            // Four independently rounded phases can differ from their rounded total by 0.002 ms.
            sum.abs_diff(u128::from(end_to_end)) <= 2
        }
    }

    fn search_phase_stats(samples: Vec<SearchPhaseEvidence>) -> SearchPhaseStats {
        let calculate = |select: fn(&SearchPhaseEvidence) -> f64, percentile: f64| {
            let mut values = samples.iter().map(select).collect::<Vec<_>>();
            values.sort_by(f64::total_cmp);
            percentile_sorted(&values, percentile)
        };
        let at = |percentile: f64| SearchPhasePercentiles {
            filter_source_preparation_ms: calculate(
                |sample| sample.filter_source_preparation_ms,
                percentile,
            ),
            execution_candidate_paging_and_base_context_hydration_ms: calculate(
                |sample| sample.execution_candidate_paging_and_base_context_hydration_ms,
                percentile,
            ),
            result_assembly_clustering_sorting_projection_ms: calculate(
                |sample| sample.result_assembly_clustering_sorting_projection_ms,
                percentile,
            ),
            unattributed_overhead_ms: calculate(
                |sample| sample.unattributed_overhead_ms,
                percentile,
            ),
            end_to_end_ms: calculate(|sample| sample.end_to_end_ms, percentile),
        };
        SearchPhaseStats {
            p50_ms: at(50.0),
            p95_ms: at(95.0),
            samples,
        }
    }

    fn profile_command(cfg: &LargeProfileConfig) -> String {
        format!(
            "CTX_LARGE_PROFILE_EVENTS={} CTX_LARGE_PROFILE_EVENTS_PER_RECORD={} CTX_LARGE_PROFILE_BATCH_RECORDS={} CTX_LARGE_PROFILE_SEED=0x{:x} CTX_LARGE_PROFILE_MEASUREMENT_REPEATS={} CTX_LARGE_PROFILE_OUTPUT={} cargo test --release -p ctx-history-search streaming_large_profile_manual_release -- --ignored --nocapture",
            cfg.total_events,
            cfg.events_per_record,
            cfg.batch_records,
            cfg.seed,
            cfg.measurement_repeats,
            cfg.output_dir.as_ref().map_or("<unset>".into(), |path| path.display().to_string()),
        )
    }

    fn timing_stats(samples: &[f64]) -> PerfTimingStats {
        assert!(!samples.is_empty(), "perf timing samples must not be empty");
        let mut sorted = samples.to_vec();
        sorted.sort_by(|left, right| left.total_cmp(right));
        PerfTimingStats {
            samples_ms: samples.iter().copied().map(rounded).collect(),
            p50_ms: percentile_sorted(&sorted, 50.0),
            p95_ms: percentile_sorted(&sorted, 95.0),
            min_ms: rounded(*sorted.first().unwrap()),
            max_ms: rounded(*sorted.last().unwrap()),
        }
    }

    fn percentile_sorted(sorted: &[f64], percentile: f64) -> f64 {
        let rank = ((percentile / 100.0) * (sorted.len().saturating_sub(1) as f64)).ceil();
        rounded(sorted[rank as usize])
    }

    fn rounded(value: f64) -> f64 {
        (value * 1000.0).round() / 1000.0
    }

    fn sqlite_footprint_bytes(path: &Path) -> u64 {
        let main = std::fs::metadata(path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        main + sqlite_sidecar_bytes(path, "-wal") + sqlite_sidecar_bytes(path, "-shm")
    }

    fn sqlite_sidecar_bytes(path: &Path, suffix: &str) -> u64 {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            return 0;
        };
        let sidecar = path.with_file_name(format!("{file_name}{suffix}"));
        std::fs::metadata(sidecar)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }

    #[test]
    fn search_packet_is_deterministic_for_large_history_and_equal_ties_use_record_id() {
        let (_temp, store) = test_store();
        for id in [
            "018f45d0-0000-7000-8000-000000010004",
            "018f45d0-0000-7000-8000-000000010001",
            "018f45d0-0000-7000-8000-000000010003",
            "018f45d0-0000-7000-8000-000000010002",
        ] {
            store.insert_record(&deterministic_tie_record(id)).unwrap();
        }

        let expected_order = vec![
            Uuid::parse_str("018f45d0-0000-7000-8000-000000010001").unwrap(),
            Uuid::parse_str("018f45d0-0000-7000-8000-000000010002").unwrap(),
            Uuid::parse_str("018f45d0-0000-7000-8000-000000010003").unwrap(),
            Uuid::parse_str("018f45d0-0000-7000-8000-000000010004").unwrap(),
        ];
        let options = PacketOptions {
            limit: 10,
            snippet_chars: 160,
            ..PacketOptions::default()
        };

        let first_search = search_packet(&store, "stabletie", &options).unwrap();
        let second_search = search_packet(&store, "stabletie", &options).unwrap();
        assert_eq!(
            first_search
                .results
                .iter()
                .map(|result| result.record_id)
                .collect::<Vec<_>>(),
            expected_order
        );
        assert_eq!(
            packet_without_generated_at(&first_search),
            packet_without_generated_at(&second_search)
        );
    }

    /// Frozen mirror of `fast_event_search_packet` that pages the
    /// *unfiltered* plan-ranked stream and applies `event_hit_matches_filters`
    /// in Rust, with the fast path's collection-pool sizing and re-sort but no
    /// upper page bound (so it is an exhaustive reference when the budget
    /// would not have been exhausted). Returns `None` exactly when the fast
    /// path would decline (zero hits) and fall back to the record-section
    /// path, plus the number of ranked page queries it needed.
    fn reference_fast_event_packet(
        store: &ctx_history_store::Store,
        query: &str,
        options: &PacketOptions,
    ) -> (Option<SearchPacket>, usize) {
        let options = normalized_options(options);
        let plan = SearchQueryPlan::new(options.match_mode, [query]);
        let target_results = options.limit.saturating_add(1);
        let filtered = has_filters(&options.filters);
        // Mirror of the fast path's bounded rerank pool: penalties (and
        // `any`-mode token rewards) reorder every match mode, so the pool is
        // collected unconditionally before the final sort.
        let collection_target = options.limit.saturating_mul(8).max(50).max(target_results);
        let clustered = options.result_mode == SearchResultMode::Sessions;
        let page_size = if clustered || filtered {
            FILTERED_SEARCH_PAGE_SIZE.max(collection_target)
        } else {
            collection_target
        };
        let mut results = Vec::new();
        let mut clustered_results = Vec::<SearchPacketResult>::new();
        let mut clustered_index = BTreeMap::<Uuid, usize>::new();
        let mut offset = 0_usize;
        let mut pages_scanned = 0_usize;

        loop {
            pages_scanned += 1;
            let hits = store
                .search_event_hits_plan_page(&plan, page_size, offset)
                .unwrap();
            let page_len = hits.len();
            for hit in hits {
                if !plan.matches_text(&hit.preview) {
                    continue;
                }
                if !event_hit_matches_filters(&hit, &options.filters, None) {
                    continue;
                }
                if clustered {
                    let cluster_id = hit.session_id.unwrap_or(hit.event_id);
                    if let Some(index) = clustered_index.get(&cluster_id).copied() {
                        let mut candidate =
                            event_search_result(&hit, query, &plan, options.snippet_chars);
                        candidate.result_scope = if candidate.session_id.is_some() {
                            SearchResultScope::Session
                        } else {
                            SearchResultScope::Event
                        };
                        let existing = &mut clustered_results[index];
                        let more = existing.more_matches_in_session.saturating_add(1);
                        if compare_search_results(&candidate, existing).is_lt() {
                            *existing = candidate;
                        }
                        existing.more_matches_in_session = more;
                        existing.session_importance = session_importance(existing.rank, more);
                    } else {
                        let mut result =
                            event_search_result(&hit, query, &plan, options.snippet_chars);
                        result.result_scope = if result.session_id.is_some() {
                            SearchResultScope::Session
                        } else {
                            SearchResultScope::Event
                        };
                        result.session_importance = session_importance(result.rank, 0);
                        clustered_index.insert(cluster_id, clustered_results.len());
                        clustered_results.push(result);
                    }
                } else {
                    let result = event_search_result(&hit, query, &plan, options.snippet_chars);
                    results.push(result);
                }
            }
            let enough_results = if clustered {
                clustered_results.len() >= collection_target
            } else {
                results.len() >= collection_target
            };
            if enough_results || page_len < page_size {
                break;
            }
            offset += page_size;
        }

        if clustered {
            results = clustered_results;
        }
        results.sort_by(compare_search_results);
        if results.is_empty() {
            return (None, pages_scanned);
        }
        let has_more = results.len() > options.limit;
        if results.len() > options.limit {
            results.truncate(options.limit);
        }
        normalize_search_result_ranks(&mut results);
        let truncation = if has_more {
            ContextTruncation {
                truncated: true,
                reason: Some("limit".to_owned()),
                omitted_results: 1,
                omitted_results_is_lower_bound: true,
            }
        } else {
            ContextTruncation::default()
        };
        let cursor_offset = results.len();
        (
            Some(SearchPacket {
                schema_version: SEARCH_PACKET_SCHEMA_VERSION,
                query: query.to_owned(),
                query_plan: plan,
                filters: options.filters.clone(),
                generated_at: utc_now(),
                results,
                pagination: pagination(Some(cursor_offset), has_more),
                truncation,
            }),
            pages_scanned,
        )
    }

    struct PushdownSearchCorpus {
        _temp: tempfile::TempDir,
        store: ctx_history_store::Store,
        primary_session: Uuid,
        subagent_session: Uuid,
        target_event: Uuid,
        user_event: Uuid,
        tool_call_event: Uuid,
        shell_tool_event: Uuid,
        ctx_command_event: Uuid,
        base: chrono::DateTime<Utc>,
        decoys: usize,
    }

    /// At least `LARGE_EVENT_CORPUS_THRESHOLD` events so the fast event path
    /// runs. 2,000 subagent decoys outrank (newer, equal bm25) a single rare
    /// primary-scope target event, three sessionless events, one primary
    /// tool-call event, and a tail of role-varied primary events (user
    /// message, tool-role shell/ctx tool and command events with structured
    /// payload executables, NULL-role and system-role messages), so
    /// default-scope matches are deeply ranked behind the decoys in the
    /// unfiltered stream and every role/tool-noise filter axis has both
    /// matching and non-matching rows.
    fn pushdown_search_corpus() -> PushdownSearchCorpus {
        let (temp, store) = test_store();
        let base = fixed_time();
        let record_id = Uuid::parse_str("018f45d0-0000-7000-8000-0000000a0001").unwrap();
        let mut record = HistoryRecord::new(
            "Pushdown search record",
            "no needle in record body",
            Vec::new(),
            "task",
            Some("/workspace/pushdown".into()),
        );
        record.id = record_id;
        record.created_at = base;
        record.updated_at = base;
        store.insert_record(&record).unwrap();

        let session = |id: &str,
                       provider: CaptureProvider,
                       agent_type: AgentType,
                       is_primary: bool| Session {
            id: Uuid::parse_str(id).unwrap(),
            history_record_id: Some(record_id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider,
            external_session_id: Some(format!("external-{id}")),
            external_agent_id: None,
            agent_type,
            role_hint: None,
            is_primary,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: base,
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        let primary_session = session(
            "018f45d0-0000-7000-8000-0000000a0011",
            CaptureProvider::Codex,
            AgentType::Primary,
            true,
        );
        let subagent_session = session(
            "018f45d0-0000-7000-8000-0000000a0012",
            CaptureProvider::Claude,
            AgentType::Subagent,
            false,
        );
        store.upsert_session(&primary_session).unwrap();
        store.upsert_session(&subagent_session).unwrap();

        let event = |index: u64,
                     session: Option<Uuid>,
                     event_type: EventType,
                     at: chrono::DateTime<Utc>,
                     text: String| Event {
            id: Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000ee{index:06x}")).unwrap(),
            seq: index,
            history_record_id: Some(record_id),
            session_id: session,
            run_id: None,
            event_type,
            role: Some(EventRole::Assistant),
            occurred_at: at,
            capture_source_id: None,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };

        let decoys = 2_000_usize;
        store.begin_immediate_batch().unwrap();
        for index in 0..decoys as u64 {
            store
                .upsert_event(&event(
                    index,
                    Some(subagent_session.id),
                    EventType::Message,
                    base + chrono::Duration::milliseconds(index as i64),
                    format!("pushdownneedle decoy {index:06}"),
                ))
                .unwrap();
        }
        // Sessionless events between decoys and target in the ranking.
        for index in 0..3_u64 {
            store
                .upsert_event(&event(
                    100_000 + index,
                    None,
                    EventType::Message,
                    base - chrono::Duration::milliseconds(500),
                    format!("pushdownneedle sessionless {index:06}"),
                ))
                .unwrap();
        }
        // A primary tool call for event_type/provider combinations.
        let tool_call = event(
            100_010,
            Some(primary_session.id),
            EventType::ToolCall,
            base - chrono::Duration::seconds(2),
            "pushdownneedle tooling 000000".to_owned(),
        );
        store.upsert_event(&tool_call).unwrap();
        // The rare default-scope target, ranked below every decoy.
        let target = event(
            100_011,
            Some(primary_session.id),
            EventType::Message,
            base - chrono::Duration::seconds(4),
            "pushdownneedle target 000000".to_owned(),
        );
        store.upsert_event(&target).unwrap();
        // Role-varied primary tail, older than the target so the existing
        // rank expectations (equal bm25, timestamp-desc tie-break) hold.
        let mut user_message = event(
            100_020,
            Some(primary_session.id),
            EventType::Message,
            base - chrono::Duration::seconds(10),
            "pushdownneedle userdecision 000000".to_owned(),
        );
        user_message.role = Some(EventRole::User);
        store.upsert_event(&user_message).unwrap();
        let mut shell_tool = event(
            100_021,
            Some(primary_session.id),
            EventType::ToolOutput,
            base - chrono::Duration::seconds(11),
            "pushdownneedle toolshell 000000".to_owned(),
        );
        shell_tool.role = Some(EventRole::Tool);
        shell_tool.payload =
            serde_json::json!({ "text": "pushdownneedle toolshell 000000", "tool": "shell" });
        store.upsert_event(&shell_tool).unwrap();
        let mut ctx_command = event(
            100_022,
            Some(primary_session.id),
            EventType::CommandStarted,
            base - chrono::Duration::seconds(12),
            "pushdownneedle ctxcommand 000000".to_owned(),
        );
        ctx_command.role = Some(EventRole::Tool);
        ctx_command.payload = serde_json::json!({
            "text": "pushdownneedle ctxcommand 000000",
            "command": "ctx search pushdownneedle",
        });
        store.upsert_event(&ctx_command).unwrap();
        let mut null_role = event(
            100_023,
            Some(primary_session.id),
            EventType::Message,
            base - chrono::Duration::seconds(13),
            "pushdownneedle nullrole 000000".to_owned(),
        );
        null_role.role = None;
        store.upsert_event(&null_role).unwrap();
        let mut system_message = event(
            100_024,
            Some(primary_session.id),
            EventType::Message,
            base - chrono::Duration::seconds(14),
            "pushdownneedle systemrole 000000".to_owned(),
        );
        system_message.role = Some(EventRole::System);
        store.upsert_event(&system_message).unwrap();
        store.commit_batch().unwrap();

        assert!(store
            .has_at_least_events(LARGE_EVENT_CORPUS_THRESHOLD)
            .unwrap());
        PushdownSearchCorpus {
            _temp: temp,
            store,
            primary_session: primary_session.id,
            subagent_session: subagent_session.id,
            target_event: target.id,
            user_event: user_message.id,
            tool_call_event: tool_call.id,
            shell_tool_event: shell_tool.id,
            ctx_command_event: ctx_command.id,
            base,
            decoys,
        }
    }

    #[test]
    fn file_scope_sql_pushdown_matches_residual_only_packets() {
        #[derive(Clone, Copy, Debug)]
        enum Ownership {
            Event,
            Run,
            SessionFallback,
            HistoryRecordFallback,
        }

        fn packet(store: &Store, options: &PacketOptions, residual_only: bool) -> SearchPacket {
            DISABLE_FILE_SCOPE_PUSHDOWN.set(residual_only);
            let packet = search_packet(store, "perfneedle", options).unwrap();
            DISABLE_FILE_SCOPE_PUSHDOWN.set(false);
            packet
        }

        for ownership in [
            Ownership::Event,
            Ownership::Run,
            Ownership::SessionFallback,
            Ownership::HistoryRecordFallback,
        ] {
            let mut archive = synthetic_perf_archive(1_200, 50);
            archive.files_touched.clear();
            let target_id = archive.events[0].id;
            let anchor_id = archive.events[1].id;
            let auxiliary_id = archive.events[2].id;
            let target_record = archive.records[0].id;
            let target_run = archive.runs[0].id;
            let target_session = archive.sessions[0].id;

            // A second identity exercises bound arrays with more than one
            // value without adding another FTS match to the expected packet.
            archive.events[1].payload = serde_json::json!({ "text": "anchor without query" });
            archive.events[1].history_record_id = None;
            archive.events[1].run_id = None;
            archive.events[1].session_id = match ownership {
                Ownership::SessionFallback => Some(target_session),
                _ => None,
            };
            archive.events[2].payload = serde_json::json!({ "text": "second anchor" });
            archive.events[2].history_record_id = None;
            archive.events[2].run_id = None;
            archive.events[2].session_id = None;

            archive.events[0].history_record_id = None;
            archive.events[0].session_id = None;
            archive.events[0].run_id = match ownership {
                Ownership::Run | Ownership::SessionFallback | Ownership::HistoryRecordFallback => {
                    Some(target_run)
                }
                Ownership::Event => None,
            };
            archive.runs[0].history_record_id = None;
            archive.runs[0].session_id = match ownership {
                Ownership::SessionFallback | Ownership::HistoryRecordFallback => {
                    Some(target_session)
                }
                _ => None,
            };
            archive.sessions[0].history_record_id = match ownership {
                Ownership::HistoryRecordFallback => Some(target_record),
                _ => None,
            };

            let touched = |id: Uuid, event_id, run_id, history_record_id| FileTouched {
                id,
                history_record_id,
                run_id,
                event_id,
                vcs_workspace_id: None,
                path: "src/selected.rs".into(),
                change_kind: Some(FileChangeKind::Modified),
                old_path: None,
                line_count_delta: None,
                confidence: Confidence::Explicit,
                timestamps: timestamps(),
                source_id: None,
                sync: sync_metadata(),
            };
            archive.files_touched.push(match ownership {
                Ownership::Event => touched(perf_uuid(0x7100, 0), Some(target_id), None, None),
                Ownership::Run => touched(perf_uuid(0x7100, 1), None, Some(target_run), None),
                Ownership::SessionFallback => {
                    touched(perf_uuid(0x7100, 2), Some(anchor_id), None, None)
                }
                Ownership::HistoryRecordFallback => {
                    touched(perf_uuid(0x7100, 3), None, None, Some(target_record))
                }
            });
            archive.files_touched.push(touched(
                perf_uuid(0x7200, ownership as u64),
                Some(auxiliary_id),
                None,
                None,
            ));

            let (_temp, mut store) = test_store();
            store.import_archive(&archive, false).unwrap();
            let options = PacketOptions {
                limit: 60,
                snippet_chars: 200,
                filters: SearchFilters {
                    file: Some("selected.rs".into()),
                    ..SearchFilters::default()
                },
                result_mode: SearchResultMode::Events,
                match_mode: SearchMatchMode::All,
            };
            let before_pages = store.event_search_page_executions();
            let old = with_paged_ranked_reference(true, || packet(&store, &options, true));
            let old_pages = store.event_search_page_executions() - before_pages;
            let before_pages = store.event_search_page_executions();
            let residual = with_paged_ranked_reference(false, || packet(&store, &options, true));
            let new_pages = store.event_search_page_executions() - before_pages;
            let pushed = packet(&store, &options, false);

            assert!(old_pages >= 3, "{ownership:?}: old={old_pages}");
            assert_eq!(new_pages, 1, "{ownership:?}: new ranked statement count");
            assert_eq!(
                packet_without_generated_at(&old),
                packet_without_generated_at(&residual),
                "{ownership:?}: paged/streamed packet equivalence"
            );
            assert!(
                pushed
                    .results
                    .iter()
                    .any(|result| result.event_id == Some(target_id)),
                "{ownership:?}"
            );
            assert_eq!(
                packet_without_generated_at(&pushed),
                packet_without_generated_at(&residual),
                "{ownership:?}: complete packet, including order/ranks/pagination/truncation"
            );
        }

        let archive = synthetic_perf_archive(1_024, 50);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        for file in [None, Some("   "), Some("does-not-exist.rs")] {
            let options = PacketOptions {
                limit: 10,
                filters: SearchFilters {
                    file: file.map(str::to_owned),
                    ..SearchFilters::default()
                },
                result_mode: SearchResultMode::Events,
                ..PacketOptions::default()
            };
            let pushed = packet(&store, &options, false);
            let residual = packet(&store, &options, true);
            assert_eq!(
                packet_without_generated_at(&pushed),
                packet_without_generated_at(&residual)
            );
        }
    }

    /// The default include_subagents=false search must find a rare
    /// primary-scope result ranked behind 2,000 subagent decoys with a single
    /// ranked page query (filters pushed into SQL), while producing exactly
    /// the packet the legacy Rust-filtered page loop produces. Stable page
    /// execution counters are asserted instead of timing.
    #[test]
    fn fast_event_search_pushdown_finds_deep_default_scope_result_in_one_page() {
        let corpus = pushdown_search_corpus();
        let store = &corpus.store;

        // Sessions (default/clustered) mode.
        let options = PacketOptions {
            limit: 10,
            snippet_chars: 200,
            ..PacketOptions::default()
        };
        let before = store.event_search_page_executions();
        let packet = search_packet(store, "pushdownneedle", &options).unwrap();
        let pushdown_pages = store.event_search_page_executions() - before;

        assert!(packet
            .results
            .iter()
            .any(|result| result.session_id == Some(corpus.primary_session)));
        assert!(packet
            .results
            .iter()
            .all(|result| result.session_id != Some(corpus.subagent_session)));
        assert_eq!(
            pushdown_pages, 1,
            "pushdown must locate the deep result in one ranked page query"
        );

        let before = store.event_search_page_executions();
        let (reference, reference_pages) =
            reference_fast_event_packet(store, "pushdownneedle", &options);
        let reference = reference.expect("reference stream must produce results");
        let after = store.event_search_page_executions();
        assert_eq!((after - before) as usize, reference_pages);
        assert_eq!(
            reference_pages,
            corpus.decoys.div_ceil(FILTERED_SEARCH_PAGE_SIZE) + 1,
            "reference loop must page across every unfiltered candidate"
        );
        assert_eq!(
            packet_without_generated_at(&packet),
            packet_without_generated_at(&reference)
        );

        // Events (unclustered) mode: the pushdown page is sized to the
        // bounded rerank pool but still resolves in a single SQL page.
        let options = PacketOptions {
            limit: 5,
            snippet_chars: 200,
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };
        let before = store.event_search_page_executions();
        let packet = search_packet(store, "pushdownneedle", &options).unwrap();
        assert_eq!(store.event_search_page_executions() - before, 1);
        assert!(
            packet
                .results
                .iter()
                .any(|result| result.event_id == Some(corpus.target_event)),
            "the deeply ranked primary target must surface in the first page"
        );
        let (reference, _) = reference_fast_event_packet(store, "pushdownneedle", &options);
        let reference = reference.expect("reference stream must produce results");
        assert_eq!(
            packet_without_generated_at(&packet),
            packet_without_generated_at(&reference)
        );
    }

    /// Role and tool-noise filters must be pushed into the ranked SQL page
    /// while `exclude_tool_names` stays Rust-side: a role-filtered search
    /// whose only match is ranked behind 2,000 differently-roled decoys
    /// completes in one ranked page query (no scan-budget spend), the
    /// tool-noise exclusion likewise stays single-page, the payload-derived
    /// executable exclusion drops exactly the named command event, and the
    /// relevance penalty demotes tool/command matches below equivalent
    /// messages inside the fast path.
    #[test]
    fn fast_event_search_role_and_tool_filters_push_down_and_penalize() {
        let corpus = pushdown_search_corpus();
        let store = &corpus.store;
        let events_options = |filters: SearchFilters, limit: usize| PacketOptions {
            limit,
            snippet_chars: 200,
            filters,
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };

        // Pushed role include: one ranked page finds the single user-role
        // match behind every assistant decoy.
        let before = store.event_search_page_executions();
        let users = search_packet(
            store,
            "pushdownneedle",
            &events_options(
                SearchFilters {
                    roles: vec![EventRole::User],
                    ..SearchFilters::default()
                },
                5,
            ),
        )
        .unwrap();
        assert_eq!(
            store.event_search_page_executions() - before,
            1,
            "role include must be answered by one ranked page query"
        );
        assert_eq!(
            users
                .results
                .iter()
                .map(|result| result.event_id)
                .collect::<Vec<_>>(),
            vec![Some(corpus.user_event)]
        );
        assert!(users.results[0]
            .why_matched
            .iter()
            .any(|why| why == "role:user"));

        // Pushed tool-noise exclusion: one ranked page, no tool/command rows.
        let before = store.event_search_page_executions();
        let no_noise = search_packet(
            store,
            "pushdownneedle",
            &events_options(
                SearchFilters {
                    exclude_tool_noise: true,
                    ..SearchFilters::default()
                },
                10,
            ),
        )
        .unwrap();
        assert_eq!(
            store.event_search_page_executions() - before,
            1,
            "tool-noise exclusion must be answered by one ranked page query"
        );
        assert!(!no_noise.results.is_empty());
        assert!(no_noise.results.iter().all(|result| {
            result.event_id != Some(corpus.tool_call_event)
                && result.event_id != Some(corpus.ctx_command_event)
        }));

        // Residual executable exclusion: Rust drops exactly the ctx command
        // event while the shell tool event survives.
        let no_ctx = search_packet(
            store,
            "pushdownneedle",
            &events_options(
                SearchFilters {
                    exclude_tool_names: vec!["ctx".into()],
                    ..SearchFilters::default()
                },
                10,
            ),
        )
        .unwrap();
        let ids: Vec<_> = no_ctx
            .results
            .iter()
            .map(|result| result.event_id)
            .collect();
        assert!(!ids.contains(&Some(corpus.ctx_command_event)));
        assert!(ids.contains(&Some(corpus.shell_tool_event)));
        assert!(ids.contains(&Some(corpus.tool_call_event)));
        assert!(ids.contains(&Some(corpus.target_event)));

        // Repeated residual exclusion: every named executable is dropped
        // (OR union), while unnamed tool events and messages survive.
        let no_ctx_or_shell = search_packet(
            store,
            "pushdownneedle",
            &events_options(
                SearchFilters {
                    exclude_tool_names: vec!["ctx".into(), "shell".into()],
                    ..SearchFilters::default()
                },
                10,
            ),
        )
        .unwrap();
        let ids: Vec<_> = no_ctx_or_shell
            .results
            .iter()
            .map(|result| result.event_id)
            .collect();
        assert!(!ids.contains(&Some(corpus.ctx_command_event)));
        assert!(!ids.contains(&Some(corpus.shell_tool_event)));
        assert!(ids.contains(&Some(corpus.tool_call_event)));
        assert!(ids.contains(&Some(corpus.target_event)));

        // Ranking penalty inside the fast path: the newer tool call ranks
        // behind the older target and user messages, and explains itself.
        let ranked = search_packet(
            store,
            "pushdownneedle",
            &events_options(SearchFilters::default(), 10),
        )
        .unwrap();
        let position = |id: Uuid| {
            ranked
                .results
                .iter()
                .position(|result| result.event_id == Some(id))
                .unwrap_or_else(|| panic!("event {id} must be in the ranked packet"))
        };
        assert!(position(corpus.target_event) < position(corpus.tool_call_event));
        assert!(position(corpus.user_event) < position(corpus.tool_call_event));
        let tool_call = &ranked.results[position(corpus.tool_call_event)];
        assert!(tool_call
            .why_matched
            .iter()
            .any(|why| why.starts_with("relevance_penalty:")));
    }

    /// Differential equality between the pushdown fast path and the legacy
    /// Rust-filtered reference loop across every pushdown-relevant filter
    /// shape: provider, fractional/exact since boundaries, event_type,
    /// explicit session (including the subagent session whose scope check is
    /// vacuous), primary_only, include_subagents, and combinations, in both
    /// result modes.
    #[test]
    fn fast_event_search_pushdown_packets_match_reference_loop_across_filters() {
        let corpus = pushdown_search_corpus();
        let store = &corpus.store;
        let sessionless_at = corpus.base - chrono::Duration::milliseconds(500);

        let filter_cases = vec![
            ("default", SearchFilters::default()),
            (
                "provider codex",
                SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    ..SearchFilters::default()
                },
            ),
            (
                "provider claude default scope",
                SearchFilters {
                    provider: Some(CaptureProvider::Claude),
                    ..SearchFilters::default()
                },
            ),
            (
                "provider claude include subagents",
                SearchFilters {
                    provider: Some(CaptureProvider::Claude),
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "explicit subagent session keeps subagent rows",
                SearchFilters {
                    session: Some(corpus.subagent_session),
                    ..SearchFilters::default()
                },
            ),
            (
                "primary only",
                SearchFilters {
                    primary_only: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "primary only overrides include subagents",
                SearchFilters {
                    primary_only: true,
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "explicit session makes primary only scope vacuous",
                SearchFilters {
                    session: Some(corpus.subagent_session),
                    primary_only: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "include subagents",
                SearchFilters {
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "since exact millisecond boundary",
                SearchFilters {
                    since: Some(sessionless_at),
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "since fractional millisecond boundary",
                SearchFilters {
                    since: Some(sessionless_at + chrono::Duration::microseconds(500)),
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "event type + provider + since",
                SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    event_type: Some(EventType::ToolCall),
                    since: Some(corpus.base - chrono::Duration::seconds(3)),
                    ..SearchFilters::default()
                },
            ),
            (
                "role user",
                SearchFilters {
                    roles: vec![EventRole::User],
                    ..SearchFilters::default()
                },
            ),
            (
                "roles user+assistant include subagents",
                SearchFilters {
                    roles: vec![EventRole::User, EventRole::Assistant],
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "role tool only",
                SearchFilters {
                    roles: vec![EventRole::Tool],
                    ..SearchFilters::default()
                },
            ),
            (
                "exclude role tool",
                SearchFilters {
                    exclude_roles: vec![EventRole::Tool],
                    ..SearchFilters::default()
                },
            ),
            (
                "exclude role assistant drops every decoy",
                SearchFilters {
                    exclude_roles: vec![EventRole::Assistant],
                    include_subagents: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "exclude tool noise",
                SearchFilters {
                    exclude_tool_noise: true,
                    ..SearchFilters::default()
                },
            ),
            (
                "exclude tool noise contradicts tool_call event type",
                SearchFilters {
                    exclude_tool_noise: true,
                    event_type: Some(EventType::ToolCall),
                    ..SearchFilters::default()
                },
            ),
            (
                "residual exclude tool name ctx",
                SearchFilters {
                    exclude_tool_names: vec!["ctx".into()],
                    ..SearchFilters::default()
                },
            ),
            (
                "residual exclude tool name normalizes case",
                SearchFilters {
                    exclude_tool_names: vec![" SHELL ".into()],
                    ..SearchFilters::default()
                },
            ),
            (
                "repeated residual exclude tool names",
                SearchFilters {
                    exclude_tool_names: vec!["ctx".into(), "shell".into()],
                    ..SearchFilters::default()
                },
            ),
            (
                "pushed role include + residual tool name",
                SearchFilters {
                    roles: vec![EventRole::Tool],
                    exclude_tool_names: vec!["ctx".into()],
                    ..SearchFilters::default()
                },
            ),
            (
                "role + event type + provider",
                SearchFilters {
                    roles: vec![EventRole::User],
                    event_type: Some(EventType::Message),
                    provider: Some(CaptureProvider::Codex),
                    ..SearchFilters::default()
                },
            ),
        ];

        // Every pushdown-relevant filter shape crossed with every match mode
        // (all/any/phrase, single- and multi-word, including a phrase whose
        // token order matches nothing): the pushed-filter fast path must
        // produce exactly the packet the Rust-filtered unfiltered plan stream
        // produces, and when the fast path declines (zero hits) the reference
        // must decline identically so the record-section fallback engages on
        // both sides of the differential.
        let mode_queries = [
            (SearchMatchMode::All, "pushdownneedle"),
            (SearchMatchMode::All, "decoy pushdownneedle"),
            (SearchMatchMode::Any, "pushdownneedle qzzqx"),
            (SearchMatchMode::Phrase, "pushdownneedle decoy"),
            (SearchMatchMode::Phrase, "decoy pushdownneedle"),
        ];
        for (name, filters) in &filter_cases {
            for result_mode in [SearchResultMode::Sessions, SearchResultMode::Events] {
                for (match_mode, query) in mode_queries {
                    let options = PacketOptions {
                        limit: 10,
                        snippet_chars: 200,
                        filters: filters.clone(),
                        result_mode,
                        match_mode,
                    };
                    let packet = search_packet(store, query, &options).unwrap();
                    let (reference, _) = reference_fast_event_packet(store, query, &options);
                    match reference {
                        Some(reference) => assert_eq!(
                            packet_without_generated_at(&packet),
                            packet_without_generated_at(&reference),
                            "case: {name} ({result_mode:?}, {match_mode:?}, {query:?})"
                        ),
                        None => {
                            let normalized = normalized_options(&options);
                            let plan = SearchQueryPlan::new(match_mode, [query]);
                            let fast =
                                fast_event_search_packet(store, &plan, query, &normalized, None)
                                    .unwrap();
                            assert!(
                                fast.is_none(),
                                "fast path must decline with the reference: {name} \
                                 ({result_mode:?}, {match_mode:?}, {query:?})"
                            );
                            assert_eq!(packet.query_plan.mode, match_mode);
                        }
                    }
                }
            }
        }

        // Guard against vacuity: the explicit-session case must actually
        // return subagent rows (scope pushdown disabled), and the exact vs
        // fractional since cases must differ at the boundary.
        let subagent_packet = search_packet(
            store,
            "pushdownneedle",
            &PacketOptions {
                limit: 10,
                snippet_chars: 200,
                filters: SearchFilters {
                    session: Some(corpus.subagent_session),
                    ..SearchFilters::default()
                },
                result_mode: SearchResultMode::Events,
                ..PacketOptions::default()
            },
        )
        .unwrap();
        assert!(!subagent_packet.results.is_empty());
        assert!(subagent_packet
            .results
            .iter()
            .all(|result| result.session_id == Some(corpus.subagent_session)));

        let since_events = |since: chrono::DateTime<Utc>| {
            search_packet(
                store,
                "pushdownneedle",
                &PacketOptions {
                    limit: 200,
                    snippet_chars: 200,
                    filters: SearchFilters {
                        since: Some(since),
                        include_subagents: true,
                        ..SearchFilters::default()
                    },
                    result_mode: SearchResultMode::Events,
                    ..PacketOptions::default()
                },
            )
            .unwrap()
            .results
            .len()
        };
        // Decoys sit at whole milliseconds base+0..base+1999ms. A fractional
        // since between base+1998ms and base+1999ms must behave exactly like
        // base+1999ms (ceil), not like base+1998ms (floor).
        let penultimate = corpus.base + chrono::Duration::milliseconds(1998);
        assert_eq!(since_events(penultimate), 2);
        assert_eq!(
            since_events(penultimate + chrono::Duration::microseconds(500)),
            1,
            "fractional since must exclude the boundary-millisecond event"
        );
        assert_eq!(
            since_events(corpus.base + chrono::Duration::milliseconds(1999)),
            1
        );
    }

    /// `any`-mode must collect the documented bounded rerank pool
    /// (`max(limit * 8, 50)`) even when every active filter is pushed into
    /// the exact-semantics SQL page. The corpus pins a two-token hit at
    /// pushed-stream position 30: its FTS postings are identical to 30 newer
    /// one-token hits (unicode61 folds the query's `café` onto their `cafe`,
    /// while ctx literal tokens do not), so bm25 ties and recency bury it —
    /// beyond the narrow `limit + 1` page that pre-pool filtered collection
    /// used (this test fails there), but inside the pool. With `--provider`
    /// pushed, the hit must be reranked to the top from one ranked SQL page,
    /// `event_hit_matches_filters` stays final authority, and the packet
    /// must equal the Rust-filtered reference stream.
    #[test]
    fn any_mode_pushdown_reranks_varied_token_counts_across_bounded_pool() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "any pool record",
            "no needle here",
            Vec::new(),
            "agent_history",
            Some("/workspace/anypool".into()),
        );
        store.insert_record(&record).unwrap();
        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-0000000b0001").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("any-pool".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: None,
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let base = fixed_time();
        let event = |index: u64, at: chrono::DateTime<Utc>, text: &str| Event {
            id: Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000eb{index:06x}")).unwrap(),
            seq: index,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: at,
            capture_source_id: None,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: Some(format!("any-pool-{index}")),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.begin_immediate_batch().unwrap();
        for index in 0..30_u64 {
            store
                .upsert_event(&event(
                    index,
                    base + chrono::Duration::milliseconds(index as i64 + 1),
                    "bonus cafe",
                ))
                .unwrap();
        }
        let deep = event(1_000, base - chrono::Duration::seconds(10), "bonus café");
        store.upsert_event(&deep).unwrap();
        for index in 0..LARGE_EVENT_CORPUS_THRESHOLD as u64 {
            store
                .upsert_event(&event(
                    10_000 + index,
                    base - chrono::Duration::seconds(100),
                    "ordinary background",
                ))
                .unwrap();
        }
        store.commit_batch().unwrap();
        store.refresh_search_index().unwrap();

        let options = PacketOptions {
            limit: 5,
            snippet_chars: 200,
            filters: SearchFilters {
                provider: Some(CaptureProvider::Codex),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Events,
            match_mode: SearchMatchMode::Any,
        };

        let plan = SearchQueryPlan::new(SearchMatchMode::Any, ["bonus café"]);
        let stream = store.search_event_hits_plan_page(&plan, 100, 0).unwrap();
        assert_eq!(stream.len(), 31);
        let deep_position = stream
            .iter()
            .position(|hit| hit.event_id == deep.id)
            .unwrap();
        assert_eq!(
            deep_position, 30,
            "the two-token hit must be buried behind every one-token hit"
        );
        assert!(
            deep_position > options.limit + 1,
            "corpus must place the hit beyond the narrow limit+1 collection"
        );

        let before = store.event_search_page_executions();
        let packet = search_packet(&store, "bonus café", &options).unwrap();
        assert_eq!(
            store.event_search_page_executions() - before,
            1,
            "pool collection must keep a single exact-filter ranked SQL page"
        );
        assert_eq!(
            packet.results.first().and_then(|result| result.event_id),
            Some(deep.id),
            "any-mode must rerank the two-token hit to the top: {packet:?}"
        );
        assert!(packet
            .results
            .iter()
            .all(|result| result.provider == Some(CaptureProvider::Codex)));
        let (reference, _) = reference_fast_event_packet(&store, "bonus café", &options);
        assert_eq!(
            packet_without_generated_at(&packet),
            packet_without_generated_at(&reference.unwrap())
        );

        // Clustered mode: the single session cluster's best representative
        // must also be the reranked two-token hit.
        let clustered = search_packet(
            &store,
            "bonus café",
            &PacketOptions {
                result_mode: SearchResultMode::Sessions,
                ..options.clone()
            },
        )
        .unwrap();
        assert_eq!(
            clustered.results.first().and_then(|result| result.event_id),
            Some(deep.id)
        );
    }

    /// The role/event-type relevance penalty reorders *every* match mode, so
    /// pushdown-only and default-scope filtered searches must also collect
    /// the bounded rerank pool (`max(limit * 8, 50, limit + 1)`). The corpus
    /// pins a user-role decision at pushed-stream position 30: its text is
    /// byte-identical to 30 newer tool-output hits, so bm25 ties and recency
    /// bury it — beyond the `limit + 1` page that pre-pool filtered `all`
    /// collection used (this test fails there), but inside the pool. The
    /// penalty (`tool_output` × 0.55 vs user message × 1.0) must promote it
    /// to the top from a single exact-filter ranked SQL page, in both the
    /// `--provider` pushdown-only case and the default-scope case, matching
    /// the Rust-filtered reference stream.
    #[test]
    fn all_mode_penalty_promotes_deep_user_hit_over_in_pool_tool_noise() {
        let (_temp, store) = test_store();
        let record = HistoryRecord::new(
            "penalty pool record",
            "no needle here",
            Vec::new(),
            "agent_history",
            Some("/workspace/penaltypool".into()),
        );
        store.insert_record(&record).unwrap();
        let session = Session {
            id: Uuid::parse_str("018f45d0-0000-7000-8000-0000000c0001").unwrap(),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("penalty-pool".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: None,
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let base = fixed_time();
        let event = |index: u64,
                     event_type: EventType,
                     role: EventRole,
                     at: chrono::DateTime<Utc>,
                     text: &str| Event {
            id: Uuid::parse_str(&format!("018f45d0-0000-7000-8000-0000ec{index:06x}")).unwrap(),
            seq: index,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type,
            role: Some(role),
            occurred_at: at,
            capture_source_id: None,
            payload: serde_json::json!({ "text": text }),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.begin_immediate_batch().unwrap();
        for index in 0..30_u64 {
            store
                .upsert_event(&event(
                    index,
                    EventType::ToolOutput,
                    EventRole::Tool,
                    base + chrono::Duration::milliseconds(index as i64 + 1),
                    "penaltyneedle outcome",
                ))
                .unwrap();
        }
        let decision = event(
            1_000,
            EventType::Message,
            EventRole::User,
            base - chrono::Duration::seconds(10),
            "penaltyneedle outcome",
        );
        store.upsert_event(&decision).unwrap();
        for index in 0..LARGE_EVENT_CORPUS_THRESHOLD as u64 {
            store
                .upsert_event(&event(
                    10_000 + index,
                    EventType::Message,
                    EventRole::Assistant,
                    base - chrono::Duration::seconds(100),
                    "ordinary background",
                ))
                .unwrap();
        }
        store.commit_batch().unwrap();
        store.refresh_search_index().unwrap();

        // The corpus really buries the user decision: identical text means
        // bm25 ties, so recency ranks all 30 tool hits ahead of it, deeper
        // than the pre-pool `limit + 1` collection but inside the pool.
        let plan = SearchQueryPlan::new(SearchMatchMode::All, ["penaltyneedle"]);
        let stream = store.search_event_hits_plan_page(&plan, 100, 0).unwrap();
        assert_eq!(stream.len(), 31);
        let deep_position = stream
            .iter()
            .position(|hit| hit.event_id == decision.id)
            .unwrap();
        assert_eq!(
            deep_position, 30,
            "the user decision must be buried behind every tool hit"
        );

        let limit = 5_usize;
        assert!(deep_position > limit + 1);
        for (name, filters) in [
            (
                "pushdown-only provider filter",
                SearchFilters {
                    provider: Some(CaptureProvider::Codex),
                    ..SearchFilters::default()
                },
            ),
            ("default scope", SearchFilters::default()),
        ] {
            let options = PacketOptions {
                limit,
                snippet_chars: 200,
                filters,
                result_mode: SearchResultMode::Events,
                match_mode: SearchMatchMode::All,
            };
            let before = store.event_search_page_executions();
            let packet = search_packet(&store, "penaltyneedle", &options).unwrap();
            assert_eq!(
                store.event_search_page_executions() - before,
                1,
                "{name}: pool collection must keep a single ranked SQL page"
            );
            assert_eq!(
                packet.results.first().and_then(|result| result.event_id),
                Some(decision.id),
                "{name}: the penalty must promote the deep user hit: {packet:?}"
            );
            assert!(packet.results[0]
                .why_matched
                .iter()
                .all(|why| !why.starts_with("relevance_penalty:")));
            assert!(packet.results[1..].iter().all(|result| {
                result
                    .why_matched
                    .iter()
                    .any(|why| why.starts_with("relevance_penalty:"))
            }));
            let (reference, _) = reference_fast_event_packet(&store, "penaltyneedle", &options);
            assert_eq!(
                packet_without_generated_at(&packet),
                packet_without_generated_at(&reference.unwrap()),
                "{name}: fast path must equal the Rust-filtered reference"
            );

            // Clustered mode: the session's best representative must be the
            // promoted user decision, not the newest tool hit.
            let clustered = search_packet(
                &store,
                "penaltyneedle",
                &PacketOptions {
                    result_mode: SearchResultMode::Sessions,
                    ..options.clone()
                },
            )
            .unwrap();
            assert_eq!(
                clustered.results.first().and_then(|result| result.event_id),
                Some(decision.id),
                "{name}: clustered representative must be the promoted user hit"
            );
        }
    }

    fn ranked_record_stream_corpus() -> (tempfile::TempDir, Store, PacketOptions) {
        let (temp, mut store) = test_store();
        let mut archive = SessionHistoryArchive::default();
        let selected = [
            499_usize, 500, 999, 1_000, 1_499, 1_500, 1_999, 2_000, 4_000,
        ];
        for index in 0..4_201_usize {
            let mut record = HistoryRecord::new(
                format!("ranked record {index:04}"),
                "recordstreamneedle equal score payload",
                Vec::new(),
                "task",
                Some("/workspace/ranked-stream".into()),
            );
            record.id = perf_uuid(0x1a00, index as u64);
            record.created_at = fixed_time();
            record.updated_at = fixed_time();
            archive.records.push(record.clone());
            if selected.contains(&index) {
                archive.files_touched.push(FileTouched {
                    id: perf_uuid(0x1b00, index as u64),
                    history_record_id: Some(record.id),
                    run_id: None,
                    event_id: None,
                    vcs_workspace_id: None,
                    path: "src/ranked-selected.rs".into(),
                    change_kind: Some(FileChangeKind::Modified),
                    old_path: None,
                    line_count_delta: Some(1),
                    confidence: Confidence::Explicit,
                    timestamps: timestamps(),
                    source_id: None,
                    sync: sync_metadata(),
                });
            }
            if index < 5 {
                archive.files_touched.push(FileTouched {
                    id: perf_uuid(0x1c00, index as u64),
                    history_record_id: Some(record.id),
                    run_id: None,
                    event_id: None,
                    vcs_workspace_id: None,
                    path: "src/ranked-control.rs".into(),
                    change_kind: Some(FileChangeKind::Modified),
                    old_path: None,
                    line_count_delta: Some(1),
                    confidence: Confidence::Explicit,
                    timestamps: timestamps(),
                    source_id: None,
                    sync: sync_metadata(),
                });
            }
        }
        store.import_archive(&archive, false).unwrap();
        let options = PacketOptions {
            limit: 8,
            snippet_chars: 160,
            filters: SearchFilters {
                file: Some("ranked-selected.rs".into()),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Sessions,
            match_mode: SearchMatchMode::All,
        };
        (temp, store, options)
    }

    fn ranked_record_control_corpus() -> (tempfile::TempDir, Store, PacketOptions) {
        let (temp, mut store) = test_store();
        let mut archive = SessionHistoryArchive::default();
        for index in 0..100_usize {
            let mut record = HistoryRecord::new(
                format!("control record {index:04}"),
                "recordstreamneedle equal score payload",
                Vec::new(),
                "task",
                Some("/workspace/ranked-stream".into()),
            );
            record.id = perf_uuid(0x1d00, index as u64);
            record.created_at = fixed_time();
            record.updated_at = fixed_time();
            archive.records.push(record.clone());
            if index < 5 {
                archive.files_touched.push(FileTouched {
                    id: perf_uuid(0x1e00, index as u64),
                    history_record_id: Some(record.id),
                    run_id: None,
                    event_id: None,
                    vcs_workspace_id: None,
                    path: "src/ranked-control.rs".into(),
                    change_kind: Some(FileChangeKind::Modified),
                    old_path: None,
                    line_count_delta: Some(1),
                    confidence: Confidence::Explicit,
                    timestamps: timestamps(),
                    source_id: None,
                    sync: sync_metadata(),
                });
            }
        }
        store.import_archive(&archive, false).unwrap();
        (
            temp,
            store,
            PacketOptions {
                limit: 3,
                filters: SearchFilters {
                    file: Some("ranked-control.rs".into()),
                    ..SearchFilters::default()
                },
                ..PacketOptions::default()
            },
        )
    }

    #[test]
    fn fallback_ranked_stream_matches_three_page_reference_at_tie_boundaries() {
        let (_temp, store, options) = ranked_record_stream_corpus();
        let before = store.record_search_page_executions();
        let old = with_paged_ranked_reference(true, || {
            search_packet(&store, "recordstreamneedle", &options).unwrap()
        });
        let old_statements = store.record_search_page_executions() - before;
        let before = store.record_search_page_executions();
        let new = with_paged_ranked_reference(false, || {
            search_packet(&store, "recordstreamneedle", &options).unwrap()
        });
        let new_statements = store.record_search_page_executions() - before;

        assert_eq!(old_statements, 9);
        assert_eq!(new_statements, 1);
        assert_eq!(
            packet_without_generated_at(&old),
            packet_without_generated_at(&new),
            "complete packet equivalence includes ordering, normalized ranks, clustering, pagination, and truncation"
        );
        assert_eq!(
            new.results
                .iter()
                .map(|result| result.record_id)
                .collect::<Vec<_>>(),
            [499_u64, 500, 999, 1_000, 1_499, 1_500, 1_999, 2_000]
                .map(|index| perf_uuid(0x1a00, index))
                .to_vec(),
            "equal-score rows straddling 500-row boundaries must not skip or duplicate"
        );
        assert_eq!(new.truncation.reason.as_deref(), Some("limit"));
    }

    #[test]
    fn event_ranked_stream_preserves_scan_budget_exhaustion() {
        let archive = synthetic_perf_archive(10_001, 50);
        let (_temp, mut store) = test_store();
        store.import_archive(&archive, false).unwrap();
        let options = PacketOptions {
            limit: 5,
            filters: SearchFilters {
                repo: Some("definitely-not-this-workspace".into()),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };
        let before = store.event_search_page_executions();
        let old = with_paged_ranked_reference(true, || {
            search_packet(&store, "perfneedle", &options).unwrap()
        });
        assert_eq!(store.event_search_page_executions() - before, 20);
        let before = store.event_search_page_executions();
        let new = with_paged_ranked_reference(false, || {
            search_packet(&store, "perfneedle", &options).unwrap()
        });
        assert_eq!(store.event_search_page_executions() - before, 1);
        assert_eq!(
            packet_without_generated_at(&old),
            packet_without_generated_at(&new)
        );
        assert!(new.results.is_empty());
        assert_eq!(new.truncation.reason.as_deref(), Some("scan_budget"));
    }

    #[test]
    fn ascii_query_pages_past_unicode61_folded_false_positives() {
        let (_temp, store) = test_store();
        let mut record = HistoryRecord::new(
            "unicode fold stream",
            "no search term in record fallback",
            Vec::new(),
            "agent_history",
            Some("/workspace/unicode-fold".into()),
        );
        record.id = perf_uuid(0x1f00, 0);
        store.insert_record(&record).unwrap();
        let session = Session {
            id: perf_uuid(0x1f10, 0),
            history_record_id: Some(record.id),
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("unicode-fold-stream".into()),
            external_agent_id: None,
            agent_type: AgentType::Primary,
            role_hint: None,
            is_primary: true,
            status: SessionStatus::Imported,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: timestamps(),
            sync: sync_metadata(),
        };
        store.upsert_session(&session).unwrap();
        let event = |index: u64, text: &str, millis: i64| Event {
            id: perf_uuid(0x1f20, index),
            seq: index,
            history_record_id: Some(record.id),
            session_id: Some(session.id),
            run_id: None,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: fixed_time() + chrono::Duration::milliseconds(millis),
            capture_source_id: None,
            payload: serde_json::json!({"text": text}),
            payload_blob_id: None,
            dedupe_key: Some(format!("unicode-fold-{index}")),
            redaction_state: RedactionState::SafePreview,
            sync: sync_metadata(),
        };
        store.begin_immediate_batch().unwrap();
        for index in 0..550_u64 {
            store
                .upsert_event(&event(index, "café folded decoy", 10_000 + index as i64))
                .unwrap();
        }
        let exact_ids = (0..60_u64)
            .map(|index| {
                let event = event(1_000 + index, "cafe exact target", index as i64);
                let id = event.id;
                store.upsert_event(&event).unwrap();
                id
            })
            .collect::<BTreeSet<_>>();
        for index in 0..LARGE_EVENT_CORPUS_THRESHOLD as u64 {
            store
                .upsert_event(&event(
                    10_000 + index,
                    "unrelated background",
                    -(index as i64),
                ))
                .unwrap();
        }
        store.commit_batch().unwrap();

        let options = PacketOptions {
            limit: 5,
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };
        let before = store.event_search_page_executions();
        let old =
            with_paged_ranked_reference(true, || search_packet(&store, "cafe", &options).unwrap());
        let old_statements = store.event_search_page_executions() - before;
        let before = store.event_search_page_executions();
        let new =
            with_paged_ranked_reference(false, || search_packet(&store, "cafe", &options).unwrap());
        let new_statements = store.event_search_page_executions() - before;

        assert!(old_statements >= 12, "old={old_statements}");
        assert!(new_statements >= 12, "new={new_statements}");
        assert_eq!(
            packet_without_generated_at(&old),
            packet_without_generated_at(&new)
        );
        assert_eq!(new.results.len(), 5);
        assert!(new
            .results
            .iter()
            .all(|result| exact_ids.contains(&result.event_id.unwrap())));
    }

    #[test]
    #[ignore = "ticket #266 ranked-stream release gate evidence"]
    fn ranked_stream_old_new_release_evidence() {
        type Samples = (Vec<f64>, Vec<f64>, (u64, u64), (u64, u64));

        fn measured(
            store: &Store,
            query: &str,
            options: &PacketOptions,
            old: bool,
            inner_repetitions: u64,
        ) -> (f64, u64, u64) {
            let event_before = store.event_search_page_executions();
            let record_before = store.record_search_page_executions();
            let started = std::time::Instant::now();
            for _ in 0..inner_repetitions {
                let packet = with_paged_ranked_reference(old, || {
                    search_packet(store, query, options).unwrap()
                });
                std::hint::black_box(packet);
            }
            let event_statements = store.event_search_page_executions() - event_before;
            let record_statements = store.record_search_page_executions() - record_before;
            assert_eq!(event_statements % inner_repetitions, 0);
            assert_eq!(record_statements % inner_repetitions, 0);
            (
                elapsed_ms(started.elapsed()) / inner_repetitions as f64,
                event_statements / inner_repetitions,
                record_statements / inner_repetitions,
            )
        }

        fn samples(
            store: &Store,
            query: &str,
            options: &PacketOptions,
            inner_repetitions: u64,
        ) -> Samples {
            measured(store, query, options, true, inner_repetitions);
            measured(store, query, options, false, inner_repetitions);
            let mut old = Vec::with_capacity(5);
            let mut new = Vec::with_capacity(5);
            let mut old_counts = (0, 0);
            let mut new_counts = (0, 0);
            for index in 0..5 {
                let (old_sample, new_sample) = if index % 2 == 0 {
                    (
                        measured(store, query, options, true, inner_repetitions),
                        measured(store, query, options, false, inner_repetitions),
                    )
                } else {
                    let new = measured(store, query, options, false, inner_repetitions);
                    let old = measured(store, query, options, true, inner_repetitions);
                    (old, new)
                };
                old.push(old_sample.0);
                new.push(new_sample.0);
                old_counts = (old_sample.1, old_sample.2);
                new_counts = (new_sample.1, new_sample.2);
            }
            (old, new, old_counts, new_counts)
        }

        fn p95(samples: &[f64]) -> f64 {
            let mut sorted = samples.to_vec();
            sorted.sort_by(f64::total_cmp);
            // Nearest-rank p95 of exactly five outer samples is necessarily
            // their maximum. It is emitted as requested evidence, but is not
            // used as the stability gate because it is not a robust estimate.
            sorted[4]
        }

        fn median(samples: &[f64]) -> f64 {
            let mut sorted = samples.to_vec();
            sorted.sort_by(f64::total_cmp);
            sorted[2]
        }

        fn improvement(old: f64, new: f64) -> f64 {
            (old - new) / old
        }

        let event_archive = synthetic_perf_archive(10_001, 50);
        let (_event_temp, mut event_store) = test_store();
        event_store.import_archive(&event_archive, false).unwrap();
        let event_selective = PacketOptions {
            limit: 5,
            filters: SearchFilters {
                repo: Some("definitely-not-this-workspace".into()),
                ..SearchFilters::default()
            },
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };
        let event_control = PacketOptions {
            limit: 5,
            result_mode: SearchResultMode::Events,
            ..PacketOptions::default()
        };
        let (event_old, event_new, event_old_counts, event_new_counts) =
            samples(&event_store, "perfneedle", &event_selective, 3);
        let (
            event_control_old,
            event_control_new,
            event_control_old_counts,
            event_control_new_counts,
        ) = samples(&event_store, "perfneedle", &event_control, 25);

        let (_record_temp, record_store, record_selective) = ranked_record_stream_corpus();
        let (record_old, record_new, record_old_counts, record_new_counts) =
            samples(&record_store, "recordstreamneedle", &record_selective, 3);
        let (_record_control_temp, record_control_store, record_control) =
            ranked_record_control_corpus();
        let (
            record_control_old,
            record_control_new,
            record_control_old_counts,
            record_control_new_counts,
        ) = samples(
            &record_control_store,
            "recordstreamneedle",
            &record_control,
            100,
        );

        let event_old_p95 = p95(&event_old);
        let event_new_p95 = p95(&event_new);
        let record_old_p95 = p95(&record_old);
        let record_new_p95 = p95(&record_new);
        let event_control_old_p95 = p95(&event_control_old);
        let event_control_new_p95 = p95(&event_control_new);
        let record_control_old_p95 = p95(&record_control_old);
        let record_control_new_p95 = p95(&record_control_new);
        let event_old_median = median(&event_old);
        let event_new_median = median(&event_new);
        let record_old_median = median(&record_old);
        let record_new_median = median(&record_new);
        let event_control_old_median = median(&event_control_old);
        let event_control_new_median = median(&event_control_new);
        let record_control_old_median = median(&record_control_old);
        let record_control_new_median = median(&record_control_new);

        let evidence = serde_json::json!({
            "profile": "ranked-stream-old-new-v1",
            "warm_measured_samples": 5,
            "order": "alternating old/new; parity reversed each pair; values are per-iteration milliseconds",
            "p95_definition": "nearest-rank p95 over five outer samples (the maximum), evidence only",
            "gate_statistic": "median of five outer samples, each aggregating repeated inner iterations",
            "event_selective": {"inner_repetitions": 3, "old_ms": event_old, "new_ms": event_new, "old_p95_ms": event_old_p95, "new_p95_ms": event_new_p95, "old_median_ms": event_old_median, "new_median_ms": event_new_median, "improvement": improvement(event_old_median, event_new_median), "old_counts": event_old_counts, "new_counts": event_new_counts},
            "event_one_page_control": {"inner_repetitions": 25, "old_ms": event_control_old, "new_ms": event_control_new, "old_p95_ms": event_control_old_p95, "new_p95_ms": event_control_new_p95, "old_median_ms": event_control_old_median, "new_median_ms": event_control_new_median, "old_counts": event_control_old_counts, "new_counts": event_control_new_counts},
            "record_selective": {"inner_repetitions": 3, "old_ms": record_old, "new_ms": record_new, "old_p95_ms": record_old_p95, "new_p95_ms": record_new_p95, "old_median_ms": record_old_median, "new_median_ms": record_new_median, "improvement": improvement(record_old_median, record_new_median), "old_counts": record_old_counts, "new_counts": record_new_counts},
            "record_one_page_control": {"inner_repetitions": 100, "old_ms": record_control_old, "new_ms": record_control_new, "old_p95_ms": record_control_old_p95, "new_p95_ms": record_control_new_p95, "old_median_ms": record_control_old_median, "new_median_ms": record_control_new_median, "old_counts": record_control_old_counts, "new_counts": record_control_new_counts},
            "gates": {"statistic": "median", "selective_improvement_min": 0.20, "one_page_regression_max": 0.05}
        });
        println!("ranked stream evidence: {evidence}");

        assert!(event_old_counts.0 >= 3 && event_new_counts.0 == 1);
        assert!(record_old_counts.1 >= 3 && record_new_counts.1 == 1);
        assert_eq!(event_control_old_counts.0, 1);
        assert_eq!(event_control_new_counts.0, 1);
        assert_eq!(record_control_old_counts.1, 1);
        assert_eq!(record_control_new_counts.1, 1);
        assert!(
            improvement(event_old_median, event_new_median) >= 0.20,
            "{evidence}"
        );
        assert!(
            improvement(record_old_median, record_new_median) >= 0.20,
            "{evidence}"
        );
        assert!(
            event_control_new_median <= event_control_old_median * 1.05,
            "{evidence}"
        );
        assert!(
            record_control_new_median <= record_control_old_median * 1.05,
            "{evidence}"
        );
    }
}
