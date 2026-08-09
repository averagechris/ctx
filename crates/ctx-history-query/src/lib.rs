//! Read-only, transport-neutral query projections and continuation handling.
//!
//! The DTOs in this crate deliberately do not contain core `Event`/`Session`
//! values or search `SearchPacket`s. Their v1 shapes are additive public
//! projections, with structurally distinct full and compact variants.

use chrono::{DateTime, Utc};
use ctx_history_capture::{ProviderImportSupport, ProviderSource, ProviderSourceStatus};
use ctx_history_core::{
    database_path, utc_now, AgentType, CaptureProvider, CaptureSource, CaptureSourceKind,
    ContextCitationType, ContextLinks, Event, EventRole, EventType, Fidelity, ProviderRawRetention,
    RedactionState, SearchMatchMode, SearchQueryPlan, Session, SessionStatus, Visibility,
};
use ctx_history_search::{
    search_packet_terms_with_hydration, search_packet_with_hydration, validate_query_request,
    HydrationIntent, PacketOptions, SearchFilters, SearchPacket, SearchPacketResult,
    SearchResultMode, SearchResultScope, SEARCH_PACKET_SCHEMA_VERSION,
};
use ctx_history_store::{RawSqlOptions, RawSqlResult, RawSqlValue, SelectedEventMode, Store};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

mod evidence;
pub use evidence::*;

pub const QUERY_DTO_SCHEMA_VERSION: u32 = 1;
pub const QUERY_REVISION: u32 = 1;
pub const EVIDENCE_SELECTOR_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SHOW_LIMIT: usize = 200;
pub const MAX_SHOW_LIMIT: usize = 1000;
pub const DEFAULT_ITEM_BYTES: usize = 4096;
pub const MAX_ITEM_BYTES: usize = 1024 * 1024;
pub const DEFAULT_PAGE_BYTES: usize = 256 * 1024;
pub const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_ARTIFACT_BYTES: usize = 1024 * 1024;
pub const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_EVIDENCE_EVENT_IDS: usize = 256;
pub const MAX_EVIDENCE_SEARCH_LIMIT: usize = 200;
/// Compatibility name retained for callers that used the scaffold constant.
pub const MAX_SNIPPET_BYTES: usize = MAX_ITEM_BYTES;
const MAX_TOKEN_BYTES: usize = 4096;
const SEARCH_CONTINUATION_CACHE_CAPACITY: usize = 4;
const SEARCH_CONTINUATION_CACHE_LIFETIME: Duration = Duration::from_secs(5 * 60);
pub const LOW_SPACE_WARNING_BYTES: u64 = 512 * 1024 * 1024;
pub const LOW_SPACE_CRITICAL_BYTES: u64 = 128 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    static FORCE_FULL_SEARCH_HYDRATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn with_full_search_hydration_reference<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            FORCE_FULL_SEARCH_HYDRATION.set(self.0);
        }
    }
    let previous = FORCE_FULL_SEARCH_HYDRATION.replace(true);
    let _reset = Reset(previous);
    run()
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error(transparent)]
    Store(#[from] ctx_history_store::StoreError),
    #[error(transparent)]
    Search(#[from] ctx_history_search::SearchError),
    #[error("invalid byte policy: {field} must be at most {maximum}, got {value}")]
    InvalidBytePolicy {
        field: &'static str,
        value: usize,
        maximum: usize,
    },
    #[error("invalid continuation: {0}")]
    InvalidContinuation(String),
    #[error("continuation does not match this request")]
    ContinuationRequestMismatch,
    #[error("continuation is for {found}, not {expected}")]
    ContinuationKind {
        expected: &'static str,
        found: String,
    },
    #[error("continuation snapshot is stale; rerun without a continuation")]
    StaleContinuation,
    #[error("local store changed while reading; retry the query")]
    SnapshotChanged,
    #[error("query arithmetic overflow")]
    ArithmeticOverflow,
    #[error("page size must be at least 1")]
    InvalidPageSize,
    #[error("query projection serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error(
        "first projected item requires {required_bytes} JSON bytes, exceeding page budget {page_bytes}; increase the page byte limit"
    )]
    ItemExceedsPageBudget {
        required_bytes: usize,
        page_bytes: usize,
    },
    #[error("evidence selector is invalid: {0}")]
    InvalidEvidenceSelector(String),
    #[error("evidence selector combines unsupported options: {0}")]
    UnsupportedEvidenceCombination(String),
    #[error("explicit event target is missing: {id}")]
    MissingEvidenceTarget { id: Uuid },
    #[error("explicit event target is deleted: {id}")]
    DeletedEvidenceTarget { id: Uuid },
    #[error(transparent)]
    Evidence(#[from] EvidenceError),
}

pub type Result<T> = std::result::Result<T, QueryError>;

#[derive(Debug, Clone, Default)]
pub struct StatusSnapshotV1 {
    pub initialized: bool,
    pub data_root: PathBuf,
    pub db_path: PathBuf,
    pub config_path: PathBuf,
    pub counts: StatusCountsV1,
    pub files: StatusFilesV1,
    pub available_space_bytes: Option<u64>,
    pub diagnostics: Vec<String>,
    pub measurement_complete: bool,
}

#[derive(Debug, Clone, Default)]
pub struct StatusCountsV1 {
    pub items: usize,
    pub sessions: usize,
    pub events: usize,
    pub sources: usize,
    pub catalog_total: usize,
    pub catalog_indexed: usize,
    pub catalog_pending: usize,
    pub catalog_failed: usize,
    pub catalog_stale: usize,
}

#[derive(Debug, Clone, Default)]
pub struct StatusFilesV1 {
    pub main_db_bytes: u64,
    pub wal_bytes: u64,
    pub shm_bytes: u64,
    pub objects_bytes: u64,
    pub spool_bytes: u64,
    pub total_data_root_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct HistorySourcePluginSourceProjection {
    pub plugin_name: String,
    pub plugin_display_name: Option<String>,
    pub plugin_version: Option<String>,
    pub manifest_path: PathBuf,
    pub id: String,
    pub display_name: Option<String>,
    pub provider_key: String,
    pub source_id: String,
    pub source_format: String,
    pub enabled: bool,
    pub refresh: &'static str,
}

#[derive(Debug, Clone)]
pub struct HistorySourcePluginFailureProjection {
    pub manifest_path: PathBuf,
    pub error: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocateSessionV1 {
    pub schema_version: u32,
    pub target: &'static str,
    pub item_type: &'static str,
    pub ctx_session_id: Uuid,
    pub provider: CaptureProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_ctx_session_id: Option<Uuid>,
    pub agent_type: AgentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    pub resume: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocateEventV1 {
    pub schema_version: u32,
    pub target: &'static str,
    pub item_type: &'static str,
    pub ctx_event_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<CaptureProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub sequence: u64,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<Value>,
}

pub fn status_snapshot(data_root: &Path, config_file: &str) -> Result<StatusSnapshotV1> {
    let db_path = database_path(data_root.to_path_buf());
    let mut snap = StatusSnapshotV1 {
        initialized: db_path.exists(),
        data_root: data_root.to_path_buf(),
        db_path: db_path.clone(),
        config_path: data_root.join(config_file),
        ..Default::default()
    };
    snap.files.main_db_bytes = file_len(&db_path);
    snap.files.wal_bytes = file_len(db_path.with_extension("sqlite-wal"));
    snap.files.shm_bytes = file_len(db_path.with_extension("sqlite-shm"));
    let root_size = sized_tree(data_root, &mut snap.diagnostics);
    snap.files.total_data_root_bytes = root_size.total_bytes;
    snap.measurement_complete = root_size.complete;
    snap.files.objects_bytes = root_size.child_bytes(&data_root.join("objects"));
    snap.files.spool_bytes = root_size.child_bytes(&data_root.join("spool"));
    snap.available_space_bytes = available_space_bytes(data_root)
        .or_else(|| data_root.parent().and_then(available_space_bytes));
    if snap.initialized {
        let store = Store::open_read_only(&db_path)?;
        let c = store.indexed_history_counts()?;
        snap.counts.items = c.items();
        snap.counts.sessions = c.sessions;
        snap.counts.events = c.events;
        snap.counts.sources = store.capture_source_count()?;
        let c = store.catalog_session_counts()?;
        snap.counts.catalog_total = c.total;
        snap.counts.catalog_indexed = c.indexed;
        snap.counts.catalog_pending = c.pending;
        snap.counts.catalog_failed = c.failed;
        snap.counts.catalog_stale = c.stale;
    }
    Ok(snap)
}

pub fn status_json(s: &StatusSnapshotV1) -> Value {
    let bytes_per_event = if s.counts.events > 0 {
        Some(s.files.total_data_root_bytes / s.counts.events as u64)
    } else {
        None
    };
    json!({
        "schema_version": 1,
        "initialized": s.initialized,
        "data_root": s.data_root,
        "database_path": s.db_path,
        "config_path": s.config_path,
        "indexed_items": s.counts.items,
        "indexed_sessions": s.counts.sessions,
        "indexed_events": s.counts.events,
        "indexed_sources": s.counts.sources,
        "cataloged_sessions": s.counts.catalog_total,
        "indexed_catalog_sessions": s.counts.catalog_indexed,
        "pending_catalog_sessions": s.counts.catalog_pending,
        "failed_catalog_sessions": s.counts.catalog_failed,
        "stale_catalog_sessions": s.counts.catalog_stale,
        "storage": {
            "main_db_bytes": s.files.main_db_bytes,
            "wal_bytes": s.files.wal_bytes,
            "shm_bytes": s.files.shm_bytes,
            "objects_bytes": s.files.objects_bytes,
            "spool_bytes": s.files.spool_bytes,
            "total_data_root_bytes": s.files.total_data_root_bytes,
            "approx_bytes_per_event": bytes_per_event,
            "available_space_bytes": s.available_space_bytes,
            "low_space": low_space(s.available_space_bytes),
            "warnings": status_warnings(s.available_space_bytes),
            "measurement_complete": s.measurement_complete,
        },
        "local_only": true,
        "read_only": true,
        "private": true,
        "share_safe": false,
        "diagnostics": s.diagnostics,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSet {
    #[default]
    Full,
    Compact,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptMode {
    #[default]
    Full,
    Lite,
    Log,
}

impl TranscriptMode {
    fn store_mode(self) -> SelectedEventMode {
        match self {
            Self::Full => SelectedEventMode::Full,
            Self::Lite => SelectedEventMode::Lite,
            Self::Log => SelectedEventMode::Log,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BytePolicy {
    pub per_item_bytes: usize,
    pub page_bytes: usize,
}

impl BytePolicy {
    pub fn validate(self) -> Result<Self> {
        if self.per_item_bytes > MAX_ITEM_BYTES {
            return Err(QueryError::InvalidBytePolicy {
                field: "per_item_bytes",
                value: self.per_item_bytes,
                maximum: MAX_ITEM_BYTES,
            });
        }
        if self.page_bytes > MAX_PAGE_BYTES {
            return Err(QueryError::InvalidBytePolicy {
                field: "page_bytes",
                value: self.page_bytes,
                maximum: MAX_PAGE_BYTES,
            });
        }
        Ok(self)
    }
}

impl Default for BytePolicy {
    fn default() -> Self {
        Self {
            per_item_bytes: DEFAULT_ITEM_BYTES,
            page_bytes: DEFAULT_PAGE_BYTES,
        }
    }
}

/// Output format is part of evidence continuation identity even though this
/// crate deliberately does not render either format yet.  Binding it here
/// prevents a future renderer from accidentally replaying a token in another
/// contract domain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceFormat {
    #[default]
    Jsonl,
    Markdown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceRefresh {
    #[default]
    Off,
    Auto,
    Strict,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "domain", rename_all = "snake_case")]
pub enum EvidenceSelector {
    SessionPage {
        ctx_session_id: Uuid,
        mode: TranscriptMode,
    },
    SearchPage {
        query: String,
        #[serde(default)]
        terms: Vec<String>,
        #[serde(default)]
        options: Box<PacketOptions>,
    },
    EventIds {
        event_ids: Vec<Uuid>,
    },
}

/// Explicit evidence-owned search wire fields.  This is deliberately not a
/// serialization of `SearchFilters`: every effective #195 field, including
/// `primary_only`, is named here so continuation arguments cannot silently
/// lose a skipped or future filter field.
#[derive(Debug, Clone, Serialize)]
struct EvidenceSearchOptionsV1<'a> {
    limit: usize,
    snippet_chars: usize,
    filters: EvidenceSearchFiltersV1<'a>,
    result_mode: SearchResultMode,
    match_mode: SearchMatchMode,
}

#[derive(Debug, Clone, Serialize)]
struct EvidenceSearchFiltersV1<'a> {
    session: Option<Uuid>,
    provider: Option<CaptureProvider>,
    history_source: &'a Option<String>,
    provider_key: &'a Option<String>,
    source_id: &'a Option<String>,
    source_format: &'a Option<String>,
    workspace: &'a Option<String>,
    since: Option<DateTime<Utc>>,
    primary_only: bool,
    include_subagents: bool,
    event_type: Option<EventType>,
    roles: &'a [EventRole],
    exclude_roles: &'a [EventRole],
    exclude_tool_noise: bool,
    exclude_tool_names: &'a [String],
    file: &'a Option<String>,
    exclude_provider_session: Option<EvidenceProviderSessionFilterV1<'a>>,
}

#[derive(Debug, Clone, Serialize)]
struct EvidenceProviderSessionFilterV1<'a> {
    provider: CaptureProvider,
    provider_session_id: &'a str,
    session_id: Option<Uuid>,
}

#[derive(Serialize)]
#[serde(tag = "domain", rename_all = "snake_case")]
enum EvidenceSelectorWire<'a> {
    SessionPage {
        ctx_session_id: Uuid,
        mode: TranscriptMode,
    },
    SearchPage {
        query: &'a str,
        terms: &'a [String],
        options: Box<EvidenceSearchOptionsV1<'a>>,
    },
    EventIds {
        event_ids: &'a [Uuid],
    },
}

impl Serialize for EvidenceSelector {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let wire = match self {
            Self::SessionPage {
                ctx_session_id,
                mode,
            } => EvidenceSelectorWire::SessionPage {
                ctx_session_id: *ctx_session_id,
                mode: *mode,
            },
            Self::SearchPage {
                query,
                terms,
                options,
            } => {
                let filters = &options.filters;
                EvidenceSelectorWire::SearchPage {
                    query,
                    terms,
                    options: Box::new(EvidenceSearchOptionsV1 {
                        limit: options.limit,
                        snippet_chars: options.snippet_chars,
                        filters: EvidenceSearchFiltersV1 {
                            session: filters.session,
                            provider: filters.provider,
                            history_source: &filters.history_source,
                            provider_key: &filters.provider_key,
                            source_id: &filters.source_id,
                            source_format: &filters.source_format,
                            workspace: &filters.repo,
                            since: filters.since,
                            primary_only: filters.primary_only,
                            include_subagents: filters.include_subagents,
                            event_type: filters.event_type,
                            roles: &filters.roles,
                            exclude_roles: &filters.exclude_roles,
                            exclude_tool_noise: filters.exclude_tool_noise,
                            exclude_tool_names: &filters.exclude_tool_names,
                            file: &filters.file,
                            exclude_provider_session: filters
                                .exclude_provider_session
                                .as_ref()
                                .map(|value| EvidenceProviderSessionFilterV1 {
                                    provider: value.provider,
                                    provider_session_id: &value.provider_session_id,
                                    session_id: value.session_id,
                                }),
                        },
                        result_mode: options.result_mode,
                        match_mode: options.match_mode,
                    }),
                }
            }
            Self::EventIds { event_ids } => EvidenceSelectorWire::EventIds { event_ids },
        };
        wire.serialize(serializer)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceSelectorRequest {
    pub selector: EvidenceSelector,
    pub limit: usize,
    #[serde(default)]
    pub fields: FieldSet,
    #[serde(default)]
    pub byte_policy: BytePolicy,
    #[serde(default = "default_artifact_bytes")]
    pub artifact_bytes: usize,
    #[serde(default)]
    pub format: EvidenceFormat,
    #[serde(default)]
    pub refresh: EvidenceRefresh,
    /// The effective evidence rule is always true.  It is explicit in the
    /// replay arguments so a continuation never depends on ambient agent
    /// environment such as CODEX_THREAD_ID.
    #[serde(default = "default_include_current_session")]
    pub include_current_session: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
}

fn default_include_current_session() -> bool {
    true
}

fn default_artifact_bytes() -> usize {
    DEFAULT_ARTIFACT_BYTES
}

impl Default for EvidenceSelectorRequest {
    fn default() -> Self {
        Self {
            selector: EvidenceSelector::EventIds { event_ids: vec![] },
            limit: 1,
            fields: FieldSet::default(),
            byte_policy: BytePolicy::default(),
            artifact_bytes: DEFAULT_ARTIFACT_BYTES,
            format: EvidenceFormat::default(),
            refresh: EvidenceRefresh::default(),
            include_current_session: true,
            continuation: None,
        }
    }
}

impl EvidenceSelectorRequest {
    /// Normalize caller ordering and all bounded page defaults.  The returned
    /// request is the one that belongs in a token and in `next_arguments`.
    pub fn canonicalized(&self) -> Result<Self> {
        let mut request = self.clone();
        request.byte_policy = request.byte_policy.validate()?;
        if request.artifact_bytes == 0 || request.artifact_bytes > MAX_ARTIFACT_BYTES {
            return Err(QueryError::InvalidBytePolicy {
                field: "artifact_bytes",
                value: request.artifact_bytes,
                maximum: MAX_ARTIFACT_BYTES,
            });
        }
        if !request.include_current_session {
            return Err(QueryError::UnsupportedEvidenceCombination(
                "include_current_session must be true".to_owned(),
            ));
        }
        if request.refresh != EvidenceRefresh::Off {
            return Err(QueryError::UnsupportedEvidenceCombination(
                "evidence selection requires refresh=off".to_owned(),
            ));
        }
        if request.limit == 0 {
            return Err(QueryError::InvalidPageSize);
        }
        request.limit = match &request.selector {
            EvidenceSelector::SessionPage { .. } => request.limit.min(MAX_SHOW_LIMIT),
            EvidenceSelector::SearchPage { .. } | EvidenceSelector::EventIds { .. } => {
                request.limit.min(MAX_EVIDENCE_SEARCH_LIMIT)
            }
        };
        if let EvidenceSelector::SearchPage { options, .. } = &mut request.selector {
            options.limit = request.limit;
        }
        if let EvidenceSelector::EventIds { event_ids } = &mut request.selector {
            if event_ids.is_empty() {
                return Err(QueryError::InvalidEvidenceSelector(
                    "event_ids must not be empty".to_owned(),
                ));
            }
            if event_ids.len() > MAX_EVIDENCE_EVENT_IDS {
                return Err(QueryError::InvalidEvidenceSelector(format!(
                    "event_ids exceeds the maximum of {MAX_EVIDENCE_EVENT_IDS}"
                )));
            }
            let mut sorted = event_ids.clone();
            sorted.sort_unstable();
            if sorted.windows(2).any(|ids| ids[0] == ids[1]) {
                return Err(QueryError::InvalidEvidenceSelector(
                    "event_ids must not contain duplicates".to_owned(),
                ));
            }
            *event_ids = sorted;
        }
        if let EvidenceSelector::SearchPage {
            query,
            terms,
            options,
        } = &request.selector
        {
            validate_query_request(query, terms)?;
            // Search itself has no refresh operation in QueryService.  Keep a
            // defensive normalization here so a caller-provided ambient
            // exclusion cannot turn an evidence request into a hidden current
            // session filter.
            let _ = options;
        }
        request.continuation = self.continuation.clone();
        Ok(request)
    }

    fn without_continuation(&self) -> Self {
        let mut value = self.clone();
        value.continuation = None;
        value
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceCountV1 {
    pub kind: &'static str,
    pub value: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceSearchTruncationV1 {
    pub truncated: bool,
    pub omitted_results: usize,
    pub omitted_results_exact: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceWorkBoundsV1 {
    pub selector_limit: usize,
    pub explicit_event_id_limit: usize,
    pub search_candidate_limit: usize,
    pub per_item_bytes: usize,
    pub page_bytes: usize,
    pub artifact_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
pub enum EvidenceItemV1 {
    Session { value: SessionProjectionV1 },
    Event { value: EventProjectionV1 },
    Result { value: SearchResultProjectionV1 },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceContinuationV1 {
    pub token: String,
    pub next_arguments: EvidenceSelectorRequest,
}

/// A bounded source lookup carried alongside a selection page.  A missing
/// lookup row is represented by `None`; it is not permission to reread an
/// ambient source or inspect the filesystem later.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceSourceLookupV1 {
    pub capture_source_id: Uuid,
    #[serde(skip)]
    pub(crate) visibility: Option<Visibility>,
    pub source: Option<SourceFullV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceEventSourceRefsV1 {
    pub ctx_event_id: Uuid,
    #[serde(skip)]
    pub(crate) redaction_state: RedactionState,
    #[serde(skip)]
    pub(crate) visibility: Visibility,
    pub event_capture_source_id: Option<Uuid>,
    /// Only a distinct owning-session source is repeated here.  When the
    /// source IDs are equal, the event reference and lookup row are shared.
    pub session_capture_source_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceSearchSourceIdentityV1 {
    pub result_id: Uuid,
    pub history_source: Option<String>,
    pub provider_key: Option<String>,
    pub source_id: Option<String>,
    pub source_format: Option<String>,
}

/// Provenance selected by one admitted search result. Event references follow
/// the result's primary event and then its event citations, with duplicates
/// removed in first-occurrence order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EvidenceSearchSourceRefsV1 {
    pub result_id: Uuid,
    pub session_capture_source_id: Option<Uuid>,
    pub events: Vec<EvidenceEventSourceRefsV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceSelectionPageV1 {
    pub schema_version: u32,
    pub domain: &'static str,
    pub ordering: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionProjectionV1>,
    pub source_lookup: Vec<EvidenceSourceLookupV1>,
    pub event_source_refs: Vec<EvidenceEventSourceRefsV1>,
    pub search_source_identity: Vec<EvidenceSearchSourceIdentityV1>,
    pub search_source_refs: Vec<EvidenceSearchSourceRefsV1>,
    pub items: Vec<EvidenceItemV1>,
    pub selected_total: Option<usize>,
    pub retained_pool_total: Option<usize>,
    pub corpus_count: Option<EvidenceCountV1>,
    pub omitted: OmittedCountsV1,
    pub pagination: PaginationV1,
    pub bytes: PageBytesV1,
    pub work: EvidenceWorkBoundsV1,
    pub search_truncation: Option<EvidenceSearchTruncationV1>,
    pub continuation: Option<EvidenceContinuationV1>,
    pub format: EvidenceFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextTruncationV1 {
    pub original_bytes: usize,
    pub returned_bytes: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaginationV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation: Option<String>,
    pub has_more: bool,
    pub offset: usize,
    pub page_size: usize,
    pub returned_items: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OmittedCountsV1 {
    pub before: usize,
    pub after: usize,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PageBytesV1 {
    pub policy: BytePolicy,
    /// Exact sum of selected projection JSON bytes for admitted item DTOs.
    pub item_json_bytes: usize,
    pub page_budget_exhausted: bool,
    pub item_text_truncated: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum SessionProjectionV1 {
    Full(Box<SessionFullV1>),
    Compact(SessionCompactV1),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionCompactV1 {
    pub ctx_session_id: Uuid,
    pub provider: CaptureProvider,
    pub agent_type: AgentType,
    pub status: SessionStatus,
    pub is_primary: bool,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub(crate) fidelity: Fidelity,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionFullV1 {
    pub id: Uuid,
    pub item_id: Uuid,
    pub item_type: &'static str,
    pub ctx_session_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_record_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_source_id: Option<Uuid>,
    pub provider: CaptureProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    pub agent_type: AgentType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_exists: Option<bool>,
    pub is_primary: bool,
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub visibility: Visibility,
    pub fidelity: Fidelity,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum EventProjectionV1 {
    Full(Box<EventFullV1>),
    Compact(EventCompactV1),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EventCompactV1 {
    pub ctx_event_id: Uuid,
    #[serde(skip)]
    pub(crate) ctx_session_id: Option<Uuid>,
    pub seq: u64,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    pub text: String,
    pub text_truncation: TextTruncationV1,
    #[serde(skip)]
    pub(crate) redaction_state: RedactionState,
    #[serde(skip)]
    pub(crate) visibility: Visibility,
    #[serde(skip)]
    pub(crate) fidelity: Fidelity,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EventFullV1 {
    pub item_id: Uuid,
    pub item_type: &'static str,
    pub ctx_event_id: Uuid,
    pub sequence: u64,
    pub seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_record_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_source_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_exists: Option<bool>,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    pub text: String,
    pub preview: String,
    pub text_truncation: TextTruncationV1,
    pub redaction_state: RedactionState,
    pub visibility: Visibility,
    pub fidelity: Fidelity,
    pub provider: CaptureProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceFullV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EventPageV1 {
    pub schema_version: u32,
    pub session: SessionProjectionV1,
    pub provider: CaptureProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceFullV1>,
    pub mode: TranscriptMode,
    pub events: Vec<EventProjectionV1>,
    pub selected_total: usize,
    pub omitted: OmittedCountsV1,
    pub pagination: PaginationV1,
    pub bytes: PageBytesV1,
    pub fields: FieldSet,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceFullV1 {
    pub source_id: Uuid,
    pub kind: CaptureSourceKind,
    pub provider: CaptureProvider,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchFiltersFullV1 {
    pub session: Option<Uuid>,
    pub provider: Option<CaptureProvider>,
    pub history_source: Option<String>,
    pub provider_key: Option<String>,
    pub source_id: Option<String>,
    pub source_format: Option<String>,
    pub workspace: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub primary_only: bool,
    pub include_subagents: bool,
    pub event_type: Option<EventType>,
    pub roles: Vec<EventRole>,
    pub exclude_roles: Vec<EventRole>,
    pub exclude_tool_noise: bool,
    pub exclude_tool_names: Vec<String>,
    pub file: Option<String>,
    pub exclude_provider_session: Option<ExcludedProviderSessionV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExcludedProviderSessionV1 {
    pub provider: CaptureProvider,
    pub provider_session_id: String,
    pub ctx_session_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum SearchContextProjectionV1 {
    Full(Box<SearchContextFullV1>),
    Compact(SearchContextCompactV1),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchContextCompactV1 {
    pub query: String,
    pub terms: Vec<String>,
    pub result_mode: &'static str,
    pub match_mode: SearchMatchMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchContextFullV1 {
    pub query: String,
    pub terms: Vec<String>,
    pub result_mode: &'static str,
    pub match_mode: SearchMatchMode,
    pub filters: SearchFiltersFullV1,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum SearchResultProjectionV1 {
    Full(Box<SearchResultFullV1>),
    Compact(SearchResultCompactV1),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchResultCompactV1 {
    pub item_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_seq: Option<u64>,
    pub title: String,
    pub snippet: String,
    pub snippet_truncation: TextTruncationV1,
    pub rank: f32,
    pub result_scope: SearchResultScope,
    pub more_matches_in_session: usize,
    pub session_importance: f32,
    pub timestamp: Option<DateTime<Utc>>,
    pub why_matched: Vec<String>,
    pub visibility: Visibility,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchResultFullV1 {
    pub item_id: Uuid,
    pub item_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_seq: Option<u64>,
    pub title: String,
    pub snippet: String,
    pub snippet_truncation: TextTruncationV1,
    pub rank: f32,
    pub result_scope: SearchResultScope,
    pub more_matches_in_session: usize,
    pub session_importance: f32,
    pub provider: Option<CaptureProvider>,
    pub provider_session_id: Option<String>,
    pub history_source: Option<String>,
    pub history_source_plugin: Option<String>,
    pub provider_key: Option<String>,
    pub source_id: Option<String>,
    pub source_format: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub cwd: Option<String>,
    pub source_path: Option<String>,
    pub source_exists: Option<bool>,
    pub source_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub why_matched: Vec<String>,
    pub citations: Vec<SearchCitationV1>,
    pub links: ContextLinks,
    pub suggested_next_commands: Vec<String>,
    pub visibility: Visibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchCitationV1 {
    #[serde(rename = "type")]
    pub citation_type: ContextCitationType,
    pub id: Uuid,
    pub item_id: Uuid,
    pub item_type: &'static str,
    pub label: String,
    pub time: DateTime<Utc>,
    pub provider: Option<CaptureProvider>,
    pub ctx_session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx_event_id: Option<Uuid>,
    pub event_seq: Option<u64>,
    pub source_path: Option<String>,
    pub source_exists: Option<bool>,
    pub source_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceTruncationV1 {
    pub truncated: bool,
    pub omitted_results: usize,
    /// False means `omitted_results` is only a lower bound from search-core.
    pub omitted_results_exact: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchPageV1 {
    pub schema_version: u32,
    /// Existing schema-v1 display spelling from search-core. The CLI exposes
    /// this only for the full projection as the top-level `query` field.
    #[serde(skip)]
    pub legacy_query: String,
    pub context: SearchContextProjectionV1,
    pub query_plan: SearchQueryPlan,
    pub generated_at: DateTime<Utc>,
    pub results: Vec<SearchResultProjectionV1>,
    #[serde(skip)]
    pub admitted_results: Vec<SearchPacketResult>,
    /// Exact size of the stable, bounded candidate pool used by all pages.
    pub pool_total: usize,
    pub omitted: OmittedCountsV1,
    pub source_truncation: SourceTruncationV1,
    pub pagination: PaginationV1,
    pub bytes: PageBytesV1,
    pub fields: FieldSet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Token {
    v: u32,
    kind: String,
    request: String,
    snapshot: String,
    offset: u64,
    seq: Option<u64>,
    id: Option<Uuid>,
}

enum QueryStore<'a> {
    Borrowed(&'a Store),
    Owned(Box<Store>),
}

struct SearchContinuationEntry {
    request: String,
    snapshot: String,
    inserted_at: Instant,
    packet: Arc<SearchPacket>,
}

pub struct QueryService<'a> {
    store: QueryStore<'a>,
    search_continuations: std::cell::RefCell<VecDeque<SearchContinuationEntry>>,
}

impl<'a> QueryService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self {
            store: QueryStore::Borrowed(store),
            search_continuations: std::cell::RefCell::new(VecDeque::new()),
        }
    }

    pub fn from_store(store: Store) -> QueryService<'static> {
        QueryService {
            store: QueryStore::Owned(Box::new(store)),
            search_continuations: std::cell::RefCell::new(VecDeque::new()),
        }
    }

    pub fn store(&self) -> &Store {
        match &self.store {
            QueryStore::Borrowed(store) => store,
            QueryStore::Owned(store) => store,
        }
    }

    fn cached_search_packet(&self, request: &str, snapshot: &str) -> Option<Arc<SearchPacket>> {
        self.cached_search_packet_at(request, snapshot, Instant::now())
    }

    fn cached_search_packet_at(
        &self,
        request: &str,
        snapshot: &str,
        now: Instant,
    ) -> Option<Arc<SearchPacket>> {
        let mut entries = self.search_continuations.borrow_mut();
        entries.retain(|entry| {
            now.duration_since(entry.inserted_at) < SEARCH_CONTINUATION_CACHE_LIFETIME
        });
        let position = entries
            .iter()
            .position(|entry| entry.request == request && entry.snapshot == snapshot)?;
        let entry = entries.remove(position).expect("cache position exists");
        let packet = Arc::clone(&entry.packet);
        entries.push_back(entry);
        Some(packet)
    }

    fn cache_search_packet(&self, request: String, snapshot: String, packet: Arc<SearchPacket>) {
        self.cache_search_packet_at(request, snapshot, packet, Instant::now());
    }

    fn cache_search_packet_at(
        &self,
        request: String,
        snapshot: String,
        packet: Arc<SearchPacket>,
        now: Instant,
    ) {
        let mut entries = self.search_continuations.borrow_mut();
        entries.retain(|entry| {
            now.duration_since(entry.inserted_at) < SEARCH_CONTINUATION_CACHE_LIFETIME
        });
        entries.retain(|entry| entry.request != request || entry.snapshot != snapshot);
        while entries.len() >= SEARCH_CONTINUATION_CACHE_CAPACITY {
            entries.pop_front();
        }
        entries.push_back(SearchContinuationEntry {
            request,
            snapshot,
            inserted_at: now,
            packet,
        });
    }

    #[cfg(test)]
    fn search_continuation_cache_len(&self) -> usize {
        self.search_continuations.borrow().len()
    }

    pub fn raw_sql(&self, sql: &str, options: RawSqlOptions) -> Result<RawSqlResult> {
        Ok(self.store().raw_sql_query(sql, options)?)
    }

    pub fn locate_session(&self, session: &Session) -> Result<LocateSessionV1> {
        let source = session
            .capture_source_id
            .map(|id| {
                self.store()
                    .get_capture_source(id)
                    .map(|source| source_location_json(&source))
            })
            .transpose()?;
        Ok(LocateSessionV1 {
            schema_version: QUERY_DTO_SCHEMA_VERSION,
            target: "session",
            item_type: "session_location",
            ctx_session_id: session.id,
            provider: session.provider,
            provider_session_id: session.external_session_id.clone(),
            parent_ctx_session_id: session.parent_session_id,
            root_ctx_session_id: session.root_session_id,
            agent_type: session.agent_type,
            role: session.role_hint.clone(),
            status: session.status,
            started_at: session.started_at,
            ended_at: session.ended_at,
            source,
            resume: provider_resume_json(session.provider, session.external_session_id.as_deref()),
        })
    }

    pub fn locate_event(&self, event: &Event) -> Result<LocateEventV1> {
        let session = event
            .session_id
            .map(|id| self.store().get_session(id))
            .transpose()?;
        let source = event
            .capture_source_id
            .map(|id| {
                self.store()
                    .get_capture_source(id)
                    .map(|source| source_location_json(&source))
            })
            .transpose()?;
        Ok(LocateEventV1 {
            schema_version: QUERY_DTO_SCHEMA_VERSION,
            target: "event",
            item_type: "event_location",
            ctx_event_id: event.id,
            ctx_session_id: event.session_id,
            provider: session.as_ref().map(|session| session.provider),
            provider_session_id: session
                .as_ref()
                .and_then(|session| session.external_session_id.clone()),
            sequence: event.seq,
            event_type: event.event_type,
            role: event.role,
            occurred_at: event.occurred_at,
            source,
            cursor: event_cursor(event),
            resume: session.as_ref().map(|session| {
                provider_resume_json(session.provider, session.external_session_id.as_deref())
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn session_events(
        &self,
        session: Session,
        mode: TranscriptMode,
        limit: usize,
        continuation: Option<&str>,
        fields: FieldSet,
        byte_policy: BytePolicy,
    ) -> Result<EventPageV1> {
        let byte_policy = byte_policy.validate()?;
        if limit == 0 {
            return Err(QueryError::InvalidPageSize);
        }
        let page_size = limit.min(MAX_SHOW_LIMIT);
        let request = show_request_hash(session.id, mode, page_size, fields, byte_policy)?;
        self.session_events_bound(
            session,
            mode,
            page_size,
            continuation,
            fields,
            byte_policy,
            request,
            "show_session",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn session_events_bound(
        &self,
        session: Session,
        mode: TranscriptMode,
        page_size: usize,
        continuation: Option<&str>,
        fields: FieldSet,
        byte_policy: BytePolicy,
        request: String,
        token_kind: &'static str,
    ) -> Result<EventPageV1> {
        let snapshot = self.store().snapshot_fingerprint()?;
        let token = continuation
            .map(|raw| decode_token(raw, token_kind, &request, &snapshot))
            .transpose()?;
        let offset = token_offset(token.as_ref())?;
        let after = token.as_ref().and_then(|value| value.seq.zip(value.id));
        let selected_total = self
            .store()
            .selected_event_count_for_session(session.id, mode.store_mode())?;
        if let (Some(token), Some(key)) = (token.as_ref(), after) {
            if offset == 0 || offset >= selected_total {
                return Err(QueryError::InvalidContinuation(
                    "show offset is outside the resumable range".to_owned(),
                ));
            }
            let position = self
                .store()
                .selected_event_cursor_position(session.id, mode.store_mode(), key)?
                .ok_or_else(|| {
                    QueryError::InvalidContinuation(
                        "show cursor is not a selected event in this session".to_owned(),
                    )
                })?;
            if position.checked_add(1) != Some(offset)
                || usize::try_from(token.offset).ok() != Some(offset)
            {
                return Err(QueryError::InvalidContinuation(
                    "show cursor and offset are inconsistent".to_owned(),
                ));
            }
        }
        let fetch_limit = page_size
            .checked_add(1)
            .ok_or(QueryError::ArithmeticOverflow)?;
        let raw_events = self.store().selected_events_for_session_after(
            session.id,
            mode.store_mode(),
            after,
            fetch_limit,
        )?;
        let candidate_events = raw_events.iter().take(page_size).collect::<Vec<_>>();
        let source_ids = candidate_events
            .iter()
            .filter_map(|event| event.capture_source_id)
            .chain(session.capture_source_id)
            .collect::<BTreeSet<_>>();
        let source_cache = if fields == FieldSet::Full {
            self.store()
                .capture_sources_for_ids(&source_ids.into_iter().collect::<Vec<_>>())?
                .into_iter()
                .map(|(id, source)| (id, Some(project_source(source))))
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };
        let source = session
            .capture_source_id
            .and_then(|id| source_cache.get(&id))
            .and_then(Option::clone);

        let mut events = Vec::new();
        let mut compact_json_bytes = 0usize;
        let mut item_text_truncated = 0usize;
        let mut page_budget_exhausted = false;
        let mut last_key = None;
        for event in candidate_events {
            let event_source = event
                .capture_source_id
                .and_then(|source_id| source_cache.get(&source_id))
                .and_then(Option::as_ref);
            let projection = project_event(
                event,
                byte_policy.per_item_bytes,
                session.provider,
                session.external_session_id.as_deref(),
                event_source,
                fields,
            );
            // The page budget applies to the exact selected projection. Full
            // records are intentionally larger than compact records, so using
            // the compact shape here would make --max-page-bytes misleading.
            let item_bytes = serde_json::to_vec(&projection)?.len();
            let next_bytes = compact_json_bytes
                .checked_add(item_bytes)
                .ok_or(QueryError::ArithmeticOverflow)?;
            if next_bytes > byte_policy.page_bytes {
                if events.is_empty() {
                    return Err(QueryError::ItemExceedsPageBudget {
                        required_bytes: item_bytes,
                        page_bytes: byte_policy.page_bytes,
                    });
                }
                page_budget_exhausted = true;
                break;
            }
            item_text_truncated += usize::from(match &projection {
                EventProjectionV1::Full(value) => value.text_truncation.truncated,
                EventProjectionV1::Compact(value) => value.text_truncation.truncated,
            });
            compact_json_bytes = next_bytes;
            last_key = Some((event.seq, event.id));
            events.push(projection);
        }

        let returned = events.len();
        let next_offset = offset
            .checked_add(returned)
            .ok_or(QueryError::ArithmeticOverflow)?;
        // A zero-item page is terminal by invariant; never mint a looping
        // continuation from a cursor that made no progress.
        let has_more = returned > 0 && next_offset < selected_total;
        let after_snapshot = self.store().snapshot_fingerprint()?;
        if after_snapshot != snapshot {
            return Err(QueryError::SnapshotChanged);
        }
        let next_continuation = if has_more {
            let cursor_key = last_key.or(after);
            Some(encode_token(&Token {
                v: QUERY_DTO_SCHEMA_VERSION,
                kind: token_kind.to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: u64::try_from(next_offset).map_err(|_| QueryError::ArithmeticOverflow)?,
                seq: cursor_key.map(|value| value.0),
                id: cursor_key.map(|value| value.1),
            })?)
        } else {
            None
        };
        Ok(EventPageV1 {
            schema_version: QUERY_DTO_SCHEMA_VERSION,
            session: project_session(&session, fields, source.as_ref()),
            provider: session.provider,
            provider_session_id: (fields == FieldSet::Full)
                .then(|| session.external_session_id.clone())
                .flatten(),
            source,
            mode,
            events,
            selected_total,
            omitted: OmittedCountsV1 {
                before: offset,
                after: selected_total.saturating_sub(next_offset),
                exact: true,
            },
            pagination: PaginationV1 {
                continuation: next_continuation,
                has_more,
                offset,
                page_size,
                returned_items: returned,
            },
            bytes: PageBytesV1 {
                policy: byte_policy,
                item_json_bytes: compact_json_bytes,
                page_budget_exhausted,
                item_text_truncated,
            },
            fields,
        })
    }

    pub fn search(
        &self,
        query: &str,
        terms: &[String],
        options: PacketOptions,
        continuation: Option<&str>,
        fields: FieldSet,
        byte_policy: BytePolicy,
    ) -> Result<SearchPageV1> {
        validate_query_request(query, terms)?;
        let byte_policy = byte_policy.validate()?;
        if options.limit == 0 {
            return Err(QueryError::InvalidPageSize);
        }
        let page_size = options.limit.min(ctx_history_search::MAX_RESULT_LIMIT);
        let request = search_request_hash(query, terms, &options, page_size, fields, byte_policy)?;
        self.search_bound(
            query,
            terms,
            options,
            page_size,
            continuation,
            fields,
            byte_policy,
            request,
            "search",
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn search_bound(
        &self,
        query: &str,
        terms: &[String],
        options: PacketOptions,
        page_size: usize,
        continuation: Option<&str>,
        fields: FieldSet,
        byte_policy: BytePolicy,
        request: String,
        token_kind: &'static str,
    ) -> Result<SearchPageV1> {
        let snapshot = self.store().snapshot_fingerprint()?;
        let token = continuation
            .map(|raw| decode_token(raw, token_kind, &request, &snapshot))
            .transpose()?;
        let offset = token_offset(token.as_ref())?;

        // Every page slices the same fixed candidate pool. A live service can
        // reuse it; a cache miss deterministically regenerates it.
        let mut pool_options = options.clone();
        pool_options.limit = ctx_history_search::MAX_RESULT_LIMIT;
        let hydration = match fields {
            FieldSet::Full => HydrationIntent::Full,
            FieldSet::Compact => HydrationIntent::Compact,
        };
        #[cfg(test)]
        let hydration = if FORCE_FULL_SEARCH_HYDRATION.get() {
            HydrationIntent::Full
        } else {
            hydration
        };
        let packet = if token.is_some() {
            self.cached_search_packet(&request, &snapshot)
        } else {
            None
        };
        let packet = if let Some(packet) = packet {
            packet
        } else if terms.is_empty() {
            Arc::new(search_packet_with_hydration(
                self.store(),
                query,
                &pool_options,
                hydration,
            )?)
        } else {
            // Do not pre-deduplicate: the canonical request preserves the exact
            // repeated term vector even though search-core may normalize it.
            Arc::new(search_packet_terms_with_hydration(
                self.store(),
                query,
                terms,
                &pool_options,
                hydration,
            )?)
        };
        let pool_total = packet.results.len();
        if token.is_some() && (offset == 0 || offset >= pool_total) {
            return Err(QueryError::InvalidContinuation(
                "offset is outside the candidate pool".to_owned(),
            ));
        }

        let mut results = Vec::new();
        let mut admitted_results = Vec::new();
        let mut compact_json_bytes = 0usize;
        let mut item_text_truncated = 0usize;
        let mut page_budget_exhausted = false;
        for result in packet.results.iter().skip(offset).take(page_size) {
            let projection = project_search_result(
                self.store(),
                result,
                byte_policy.per_item_bytes,
                query,
                terms,
                &options,
                fields,
            );
            let item_bytes = serde_json::to_vec(&projection)?.len();
            let next_bytes = compact_json_bytes
                .checked_add(item_bytes)
                .ok_or(QueryError::ArithmeticOverflow)?;
            if next_bytes > byte_policy.page_bytes {
                if results.is_empty() {
                    return Err(QueryError::ItemExceedsPageBudget {
                        required_bytes: item_bytes,
                        page_bytes: byte_policy.page_bytes,
                    });
                }
                page_budget_exhausted = true;
                break;
            }
            item_text_truncated += usize::from(match &projection {
                SearchResultProjectionV1::Full(value) => value.snippet_truncation.truncated,
                SearchResultProjectionV1::Compact(value) => value.snippet_truncation.truncated,
            });
            compact_json_bytes = next_bytes;
            results.push(projection);
            admitted_results.push(result.clone());
        }
        let returned = results.len();
        let next_offset = offset
            .checked_add(returned)
            .ok_or(QueryError::ArithmeticOverflow)?;
        let has_more = returned > 0 && next_offset < pool_total;
        let after_snapshot = self.store().snapshot_fingerprint()?;
        if after_snapshot != snapshot {
            return Err(QueryError::SnapshotChanged);
        }
        if has_more {
            self.cache_search_packet(request.clone(), snapshot.clone(), Arc::clone(&packet));
        }
        let next_continuation = if has_more {
            Some(encode_token(&Token {
                v: QUERY_DTO_SCHEMA_VERSION,
                kind: token_kind.to_owned(),
                request,
                snapshot,
                offset: u64::try_from(next_offset).map_err(|_| QueryError::ArithmeticOverflow)?,
                seq: None,
                id: None,
            })?)
        } else {
            None
        };
        let source_truncation = SourceTruncationV1 {
            truncated: packet.truncation.truncated,
            omitted_results: packet.truncation.omitted_results as usize,
            // A plain result-limit truncation carries the exact difference
            // computed by search-core. Scan/source budget reasons use sentinel
            // lower bounds and must not be presented as exact.
            omitted_results_exact: !packet.truncation.omitted_results_is_lower_bound,
            reason: packet.truncation.reason.clone(),
        };
        Ok(SearchPageV1 {
            schema_version: QUERY_DTO_SCHEMA_VERSION,
            legacy_query: packet.query.clone(),
            context: project_search_context(query, terms, &options, fields),
            query_plan: packet.query_plan.clone(),
            // This is response metadata, not candidate-pool identity. Emitting
            // it per request keeps cache hits indistinguishable from misses.
            generated_at: utc_now(),
            results,
            admitted_results,
            pool_total,
            omitted: OmittedCountsV1 {
                before: offset,
                after: pool_total.saturating_sub(next_offset),
                exact: true,
            },
            source_truncation,
            pagination: PaginationV1 {
                continuation: next_continuation,
                has_more,
                offset,
                page_size,
                returned_items: returned,
            },
            bytes: PageBytesV1 {
                policy: byte_policy,
                item_json_bytes: compact_json_bytes,
                page_budget_exhausted,
                item_text_truncated,
            },
            fields,
        })
    }

    /// Execute exactly one evidence selector domain.  This is intentionally a
    /// query-core API: it has no CLI/MCP state, reads no ambient session
    /// variables, and does not render or write an artifact.
    pub(crate) fn select_evidence(
        &self,
        request: EvidenceSelectorRequest,
    ) -> Result<EvidenceSelectionPageV1> {
        let request = request.canonicalized()?;
        let request_hash = evidence_request_hash(&request.without_continuation())?;
        let continuation = request.continuation.as_deref();
        match &request.selector {
            EvidenceSelector::SessionPage {
                ctx_session_id,
                mode,
            } => self.select_evidence_session(
                &request,
                *ctx_session_id,
                *mode,
                continuation,
                request_hash,
            ),
            EvidenceSelector::SearchPage {
                query,
                terms,
                options,
            } => self.select_evidence_search(
                &request,
                query,
                terms,
                options,
                continuation,
                request_hash,
            ),
            EvidenceSelector::EventIds { event_ids } => {
                self.select_evidence_events(&request, event_ids, continuation, request_hash)
            }
        }
    }

    /// Select and normalize through the only public evidence output boundary.
    pub fn evidence(&self, request: EvidenceSelectorRequest) -> Result<NormalizedEvidencePageV1> {
        let fields = request.fields;
        let per_item_bytes = request.byte_policy.per_item_bytes;
        let selected = self.select_evidence(request)?;
        normalize_evidence(selected, fields, per_item_bytes).map_err(QueryError::Evidence)
    }

    fn select_evidence_session(
        &self,
        request: &EvidenceSelectorRequest,
        session_id: Uuid,
        mode: TranscriptMode,
        continuation: Option<&str>,
        request_hash: String,
    ) -> Result<EvidenceSelectionPageV1> {
        let session = match self.store().get_session(session_id) {
            Ok(session) if session.sync.deleted_at.is_none() => session,
            Ok(_) => return Err(QueryError::DeletedEvidenceTarget { id: session_id }),
            Err(ctx_history_store::StoreError::NotFound(_)) => {
                return Err(QueryError::MissingEvidenceTarget { id: session_id })
            }
            Err(error) => return Err(error.into()),
        };
        let page = self.session_events_bound(
            session,
            mode,
            request.limit,
            continuation,
            request.fields,
            request.byte_policy,
            request_hash,
            "evidence_session_page",
        )?;
        let admitted_event_ids = page
            .events
            .iter()
            .map(event_projection_id)
            .collect::<Vec<_>>();
        let (event_source_refs, source_lookup) =
            self.evidence_event_provenance(&admitted_event_ids, request.fields)?;
        let items = page
            .events
            .into_iter()
            .map(|value| EvidenceItemV1::Event { value })
            .collect::<Vec<_>>();
        let continuation =
            page.pagination
                .continuation
                .clone()
                .map(|token| EvidenceContinuationV1 {
                    token: token.clone(),
                    next_arguments: next_evidence_arguments(request, token),
                });
        Ok(EvidenceSelectionPageV1 {
            schema_version: EVIDENCE_SELECTOR_SCHEMA_VERSION,
            domain: "session_page",
            ordering: "session_seq_id_asc",
            session: Some(page.session),
            source_lookup,
            event_source_refs,
            search_source_identity: Vec::new(),
            search_source_refs: Vec::new(),
            items,
            selected_total: Some(page.selected_total),
            retained_pool_total: None,
            corpus_count: None,
            omitted: page.omitted,
            pagination: page.pagination,
            bytes: page.bytes,
            work: evidence_work_bounds(request),
            search_truncation: None,
            continuation,
            format: request.format,
        })
    }

    fn select_evidence_search(
        &self,
        request: &EvidenceSelectorRequest,
        query: &str,
        terms: &[String],
        options: &PacketOptions,
        continuation: Option<&str>,
        request_hash: String,
    ) -> Result<EvidenceSelectionPageV1> {
        let page = self.search_bound(
            query,
            terms,
            options.clone(),
            request.limit,
            continuation,
            request.fields,
            request.byte_policy,
            request_hash,
            "evidence_search_page",
        )?;
        let search_truncation = EvidenceSearchTruncationV1 {
            truncated: page.source_truncation.truncated,
            omitted_results: page.source_truncation.omitted_results,
            omitted_results_exact: page.source_truncation.omitted_results_exact,
            reason: page.source_truncation.reason.clone(),
        };
        let search_source_identity = page
            .results
            .iter()
            .filter_map(project_search_source_identity)
            .collect::<Vec<_>>();
        let result_event_ids = page
            .admitted_results
            .iter()
            .flat_map(search_result_event_ids)
            .collect::<Vec<_>>();
        let (all_event_refs, mut source_lookup) =
            self.evidence_event_provenance(&result_event_ids, request.fields)?;
        let event_refs_by_id = all_event_refs
            .into_iter()
            .map(|refs| (refs.ctx_event_id, refs))
            .collect::<HashMap<_, _>>();
        let result_session_ids = page
            .admitted_results
            .iter()
            .filter_map(|result| result.session_id)
            .collect::<Vec<_>>();
        let result_sessions = self.store().live_sessions_for_ids(&result_session_ids)?;
        let search_source_refs = page
            .admitted_results
            .iter()
            .map(|result| EvidenceSearchSourceRefsV1 {
                result_id: result.record_id,
                session_capture_source_id: result
                    .session_id
                    .and_then(|id| result_sessions.get(&id))
                    .and_then(|session| session.capture_source_id),
                events: search_result_event_ids(result)
                    .into_iter()
                    .filter_map(|id| event_refs_by_id.get(&id).cloned())
                    .collect(),
            })
            .collect::<Vec<_>>();
        let existing_source_ids = source_lookup
            .iter()
            .map(|source| source.capture_source_id)
            .collect::<BTreeSet<_>>();
        let session_source_ids = search_source_refs
            .iter()
            .filter_map(|refs| refs.session_capture_source_id)
            .filter(|id| !existing_source_ids.contains(id))
            .collect::<BTreeSet<_>>();
        source_lookup.extend(self.evidence_source_lookup(&session_source_ids, request.fields)?);
        source_lookup.sort_by_key(|source| source.capture_source_id);
        let corpus_count = if search_truncation.truncated {
            Some(EvidenceCountV1 {
                kind: "lower_bound",
                value: page.pool_total,
            })
        } else {
            Some(EvidenceCountV1 {
                kind: "exact",
                value: page.pool_total,
            })
        };
        let items = page
            .results
            .into_iter()
            .map(|value| EvidenceItemV1::Result { value })
            .collect::<Vec<_>>();
        let continuation =
            page.pagination
                .continuation
                .clone()
                .map(|token| EvidenceContinuationV1 {
                    token: token.clone(),
                    next_arguments: next_evidence_arguments(request, token),
                });
        Ok(EvidenceSelectionPageV1 {
            schema_version: EVIDENCE_SELECTOR_SCHEMA_VERSION,
            domain: "search_page",
            ordering: "search_ranked_v1",
            session: None,
            source_lookup,
            event_source_refs: Vec::new(),
            search_source_identity,
            search_source_refs,
            items,
            selected_total: None,
            retained_pool_total: Some(page.pool_total),
            corpus_count,
            omitted: page.omitted,
            pagination: page.pagination,
            bytes: page.bytes,
            work: evidence_work_bounds(request),
            search_truncation: Some(search_truncation),
            continuation,
            format: request.format,
        })
    }

    fn select_evidence_events(
        &self,
        request: &EvidenceSelectorRequest,
        event_ids: &[Uuid],
        continuation: Option<&str>,
        request_hash: String,
    ) -> Result<EvidenceSelectionPageV1> {
        let snapshot = self.store().snapshot_fingerprint()?;
        let offset = if let Some(token) = continuation {
            let token = decode_token(token, "evidence_event_ids", &request_hash, &snapshot)?;
            token_offset(Some(&token))?
        } else {
            0
        };
        let statuses = self.store().event_target_statuses(event_ids)?;
        if let Some(id) = event_ids.iter().find(|id| {
            matches!(
                statuses.get(id),
                Some(ctx_history_store::EventTargetStatus::Missing)
            )
        }) {
            return Err(QueryError::MissingEvidenceTarget { id: *id });
        }
        if let Some(id) = event_ids.iter().find(|id| {
            matches!(
                statuses.get(id),
                Some(ctx_history_store::EventTargetStatus::Deleted)
            )
        }) {
            return Err(QueryError::DeletedEvidenceTarget { id: *id });
        }
        let events = self.store().live_events_for_ids(event_ids)?;
        if offset > events.len() || (continuation.is_some() && offset == events.len()) {
            return Err(QueryError::InvalidContinuation(
                "event_ids offset is outside the selected set".to_owned(),
            ));
        }
        let fetch_end = offset
            .checked_add(request.limit)
            .ok_or(QueryError::ArithmeticOverflow)?
            .min(events.len());
        let page_events = events
            .iter()
            .skip(offset)
            .take(fetch_end.saturating_sub(offset))
            .collect::<Vec<_>>();
        let mut items = Vec::new();
        let mut item_bytes = 0usize;
        let mut page_budget_exhausted = false;
        let session_ids = page_events
            .iter()
            .filter_map(|event| event.session_id)
            .collect::<Vec<_>>();
        let sessions = self.store().live_sessions_for_ids(&session_ids)?;
        let mut source_ids = BTreeSet::new();
        for event in &page_events {
            if let Some(source_id) = event.capture_source_id {
                source_ids.insert(source_id);
            }
            if let Some(session_id) = event.session_id {
                if let Some(source_id) = sessions
                    .get(&session_id)
                    .and_then(|session| session.capture_source_id)
                {
                    source_ids.insert(source_id);
                }
            }
        }
        let source_ids = source_ids.into_iter().collect::<Vec<_>>();
        let sources = self.store().capture_sources_for_ids(&source_ids)?;
        let projected_sources = sources
            .into_iter()
            .map(|(id, source)| (id, project_source(source)))
            .collect::<HashMap<_, _>>();
        let mut admitted_events = Vec::new();
        for event in &page_events {
            let session = event.session_id.and_then(|id| sessions.get(&id));
            let provider = session
                .map(|session| session.provider)
                .unwrap_or(CaptureProvider::Unknown);
            let provider_session_id =
                session.and_then(|session| session.external_session_id.as_deref());
            let projection = project_event(
                event,
                request.byte_policy.per_item_bytes,
                provider,
                provider_session_id,
                event
                    .capture_source_id
                    .and_then(|id| projected_sources.get(&id)),
                request.fields,
            );
            let bytes = serde_json::to_vec(&projection)?.len();
            let next_bytes = item_bytes
                .checked_add(bytes)
                .ok_or(QueryError::ArithmeticOverflow)?;
            if next_bytes > request.byte_policy.page_bytes {
                if items.is_empty() {
                    return Err(QueryError::ItemExceedsPageBudget {
                        required_bytes: bytes,
                        page_bytes: request.byte_policy.page_bytes,
                    });
                }
                page_budget_exhausted = true;
                break;
            }
            item_bytes = next_bytes;
            items.push(EvidenceItemV1::Event { value: projection });
            admitted_events.push(*event);
        }
        let event_source_refs = admitted_events
            .iter()
            .map(|event| {
                let session_source_id = event
                    .session_id
                    .and_then(|session_id| sessions.get(&session_id))
                    .and_then(|session| session.capture_source_id)
                    .filter(|source_id| Some(*source_id) != event.capture_source_id);
                EvidenceEventSourceRefsV1 {
                    ctx_event_id: event.id,
                    redaction_state: event.redaction_state,
                    visibility: event.sync.visibility,
                    event_capture_source_id: event.capture_source_id,
                    session_capture_source_id: session_source_id,
                }
            })
            .collect::<Vec<_>>();
        let admitted_source_ids = event_source_refs
            .iter()
            .flat_map(|refs| {
                [refs.event_capture_source_id, refs.session_capture_source_id]
                    .into_iter()
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        let source_lookup = self.evidence_source_lookup(&admitted_source_ids, request.fields)?;
        let returned = items.len();
        let next_offset = offset
            .checked_add(returned)
            .ok_or(QueryError::ArithmeticOverflow)?;
        let has_more = returned > 0 && next_offset < events.len();
        let after_snapshot = self.store().snapshot_fingerprint()?;
        if after_snapshot != snapshot {
            return Err(QueryError::SnapshotChanged);
        }
        let next_token = if has_more {
            Some(encode_token(&Token {
                v: QUERY_DTO_SCHEMA_VERSION,
                kind: "evidence_event_ids".to_owned(),
                request: request_hash,
                snapshot,
                offset: u64::try_from(next_offset).map_err(|_| QueryError::ArithmeticOverflow)?,
                seq: None,
                id: None,
            })?)
        } else {
            None
        };
        let pagination = PaginationV1 {
            continuation: next_token.clone(),
            has_more,
            offset,
            page_size: request.limit,
            returned_items: returned,
        };
        let continuation = next_token.map(|token| EvidenceContinuationV1 {
            token: token.clone(),
            next_arguments: next_evidence_arguments(request, token),
        });
        Ok(EvidenceSelectionPageV1 {
            schema_version: EVIDENCE_SELECTOR_SCHEMA_VERSION,
            domain: "event_ids",
            ordering: "event_occurred_at_id_asc",
            session: None,
            source_lookup,
            event_source_refs,
            search_source_identity: Vec::new(),
            search_source_refs: Vec::new(),
            items,
            selected_total: Some(events.len()),
            retained_pool_total: None,
            corpus_count: None,
            omitted: OmittedCountsV1 {
                before: offset,
                after: events.len().saturating_sub(next_offset),
                exact: true,
            },
            pagination,
            bytes: PageBytesV1 {
                policy: request.byte_policy,
                item_json_bytes: item_bytes,
                page_budget_exhausted,
                item_text_truncated: 0,
            },
            work: evidence_work_bounds(request),
            search_truncation: None,
            continuation,
            format: request.format,
        })
    }

    fn evidence_event_provenance(
        &self,
        event_ids: &[Uuid],
        fields: FieldSet,
    ) -> Result<(Vec<EvidenceEventSourceRefsV1>, Vec<EvidenceSourceLookupV1>)> {
        let events = self.store().live_events_for_ids(event_ids)?;
        let events = events
            .into_iter()
            .map(|event| (event.id, event))
            .collect::<HashMap<_, _>>();
        let session_ids = events
            .values()
            .filter_map(|event| event.session_id)
            .collect::<Vec<_>>();
        let sessions = self.store().live_sessions_for_ids(&session_ids)?;
        let refs = event_ids
            .iter()
            .filter_map(|event_id| events.get(event_id))
            .map(|event| {
                let session_capture_source_id = event
                    .session_id
                    .and_then(|id| sessions.get(&id))
                    .and_then(|session| session.capture_source_id)
                    .filter(|id| Some(*id) != event.capture_source_id);
                EvidenceEventSourceRefsV1 {
                    ctx_event_id: event.id,
                    redaction_state: event.redaction_state,
                    visibility: event.sync.visibility,
                    event_capture_source_id: event.capture_source_id,
                    session_capture_source_id,
                }
            })
            .collect::<Vec<_>>();
        let source_ids = refs
            .iter()
            .flat_map(|refs| {
                [refs.event_capture_source_id, refs.session_capture_source_id]
                    .into_iter()
                    .flatten()
            })
            .collect::<BTreeSet<_>>();
        let lookup = self.evidence_source_lookup(&source_ids, fields)?;
        Ok((refs, lookup))
    }

    /// Shared bounded projection boundary for every evidence domain.
    fn evidence_source_lookup(
        &self,
        source_ids: &BTreeSet<Uuid>,
        fields: FieldSet,
    ) -> Result<Vec<EvidenceSourceLookupV1>> {
        let sources = if fields == FieldSet::Full {
            self.store()
                .capture_sources_for_ids(&source_ids.iter().copied().collect::<Vec<_>>())?
        } else {
            BTreeMap::new()
        };
        Ok(source_ids
            .iter()
            .copied()
            .map(|capture_source_id| EvidenceSourceLookupV1 {
                capture_source_id,
                visibility: sources
                    .get(&capture_source_id)
                    .map(|source| source.sync.visibility),
                source: sources
                    .get(&capture_source_id)
                    .cloned()
                    .map(project_evidence_source),
            })
            .collect())
    }
}

fn evidence_work_bounds(request: &EvidenceSelectorRequest) -> EvidenceWorkBoundsV1 {
    EvidenceWorkBoundsV1 {
        selector_limit: request.limit,
        explicit_event_id_limit: MAX_EVIDENCE_EVENT_IDS,
        search_candidate_limit: ctx_history_search::MAX_RESULT_LIMIT,
        per_item_bytes: request.byte_policy.per_item_bytes,
        page_bytes: request.byte_policy.page_bytes,
        artifact_bytes: request.artifact_bytes,
    }
}

fn next_evidence_arguments(
    request: &EvidenceSelectorRequest,
    token: String,
) -> EvidenceSelectorRequest {
    let mut next = request.without_continuation();
    next.continuation = Some(token);
    next
}

#[derive(Serialize)]
struct EvidenceRequestBinding<'a> {
    kind: &'static str,
    query_revision: u32,
    schema_version: u32,
    evidence_schema_version: u32,
    search_packet_schema_version: u32,
    request: &'a EvidenceSelectorRequest,
}

fn evidence_request_hash(request: &EvidenceSelectorRequest) -> Result<String> {
    hash_serializable(&EvidenceRequestBinding {
        kind: "evidence_selector",
        query_revision: QUERY_REVISION,
        schema_version: QUERY_DTO_SCHEMA_VERSION,
        evidence_schema_version: EVIDENCE_SELECTOR_SCHEMA_VERSION,
        search_packet_schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        request,
    })
}

fn project_search_source_identity(
    result: &SearchResultProjectionV1,
) -> Option<EvidenceSearchSourceIdentityV1> {
    match result {
        SearchResultProjectionV1::Full(value) => Some(EvidenceSearchSourceIdentityV1 {
            result_id: value.item_id,
            history_source: value.history_source.clone(),
            provider_key: value.provider_key.clone(),
            source_id: value.source_id.clone(),
            source_format: value.source_format.clone(),
        }),
        SearchResultProjectionV1::Compact(_) => None,
    }
}

pub fn sources_response_json<I>(sources: I, read_only: bool) -> Value
where
    I: IntoIterator<Item = Value>,
{
    let mut value = json!({
        "schema_version": QUERY_DTO_SCHEMA_VERSION,
        "sources": sources.into_iter().collect::<Vec<_>>(),
    });
    if read_only {
        value
            .as_object_mut()
            .expect("object")
            .insert("read_only".to_owned(), Value::Bool(true));
    }
    value
}

pub fn sources_json(
    native_sources: &[ProviderSource],
    plugin_sources: &[HistorySourcePluginSourceProjection],
    plugin_failures: &[HistorySourcePluginFailureProjection],
    read_only: bool,
) -> Value {
    let mut rows = native_sources_json(native_sources);
    rows.extend(plugin_sources_json(plugin_sources));
    rows.extend(plugin_manifest_failures_json(plugin_failures));
    sources_response_json(rows, read_only)
}

pub fn native_sources_json(sources: &[ProviderSource]) -> Vec<Value> {
    sources.iter().map(native_source_json).collect()
}

fn native_source_json(source: &ProviderSource) -> Value {
    json!({
        "provider": source.provider.as_str(),
        "path": source.path,
        "exists": source.exists,
        "source_format": source.source_format,
        "status": provider_source_status_json(source.status),
        "import_support": import_support_json(source.import_support),
        "native_import": source.import_support.is_auto_importable(),
        "importable": source.status == ProviderSourceStatus::Available && source.import_support.is_importable(),
        "raw_retention": raw_retention_json(source.raw_retention),
        "unsupported_reason": source.unsupported_reason,
    })
}

pub fn plugin_sources_json(sources: &[HistorySourcePluginSourceProjection]) -> Vec<Value> {
    sources.iter().map(|source| json!({
        "provider": CaptureProvider::Custom.as_str(), "kind": "history_source_plugin", "plugin": source.plugin_name,
        "plugin_display_name": source.plugin_display_name, "plugin_version": source.plugin_version,
        "history_source": format!("{}/{}", source.plugin_name, source.id), "history_source_id": source.id,
        "display_name": source.display_name, "provider_key": source.provider_key, "source_id": source.source_id,
        "source_format": source.source_format, "manifest_path": source.manifest_path, "enabled": source.enabled,
        "refresh": source.refresh, "status": "available", "import_support": "history_source_plugin", "native_import": false,
        "importable": true, "raw_retention": "metadata_only", "unsupported_reason": null,
    })).collect()
}

pub fn plugin_manifest_failures_json(
    failures: &[HistorySourcePluginFailureProjection],
) -> Vec<Value> {
    failures.iter().map(|failure| json!({
        "provider": CaptureProvider::Custom.as_str(), "kind": "history_source_plugin", "plugin": null,
        "plugin_display_name": null, "plugin_version": null, "history_source": null, "history_source_id": null,
        "display_name": null, "provider_key": null, "source_id": null, "source_format": null,
        "manifest_path": failure.manifest_path, "enabled": false, "refresh": null, "status": "invalid",
        "import_support": "history_source_plugin", "native_import": false, "importable": false,
        "raw_retention": "metadata_only", "unsupported_reason": failure.error, "error": failure.error,
    })).collect()
}

fn provider_source_status_json(status: ProviderSourceStatus) -> &'static str {
    status.as_str()
}
fn import_support_json(support: ProviderImportSupport) -> &'static str {
    match support {
        ProviderImportSupport::Native => "native",
        ProviderImportSupport::Preview => "preview",
        ProviderImportSupport::Unsupported => "unsupported",
    }
}
fn raw_retention_json(retention: ProviderRawRetention) -> &'static str {
    match retention {
        ProviderRawRetention::None => "none",
        ProviderRawRetention::PathReference => "path_reference",
        ProviderRawRetention::MetadataOnly => "metadata_only",
        ProviderRawRetention::LocalBlob => "local_blob",
        ProviderRawRetention::Withheld => "withheld",
    }
}

pub fn raw_sql_result_json(result: &RawSqlResult) -> Value {
    compact_json_value(json!({
        "schema_version": QUERY_DTO_SCHEMA_VERSION, "item_type": "sql_result", "read_only": true,
        "columns": result.columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>(),
        "rows": result.rows.iter().map(|row| row.iter().map(raw_sql_value_json).collect::<Vec<_>>()).collect::<Vec<_>>(),
        "returned_rows": result.returned_rows,
        "truncated": { "rows": result.truncated.rows, "values": result.truncated.values },
        "limits": { "max_rows": result.limits.max_rows, "max_columns": result.limits.max_columns, "max_value_bytes": result.limits.max_value_bytes, "max_sql_bytes": result.limits.max_sql_bytes, "timeout_ms": result.limits.timeout_ms },
        "elapsed_ms": result.elapsed.as_millis(),
    }))
}

pub fn raw_sql_value_json(value: &RawSqlValue) -> Value {
    match value {
        RawSqlValue::Null => Value::Null,
        RawSqlValue::Integer(value) => json!(value),
        RawSqlValue::Real(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        RawSqlValue::Text {
            value,
            bytes,
            truncated,
        } if *truncated => json!({"type":"text","value":value,"bytes":bytes,"truncated":true}),
        RawSqlValue::Text { value, .. } => Value::String(value.clone()),
        RawSqlValue::Blob {
            bytes,
            preview_hex,
            truncated,
        } => json!({"type":"blob","bytes":bytes,"preview_hex":preview_hex,"truncated":truncated}),
    }
}

/// Truncate to a UTF-8 byte cap and report exact byte accounting.
///
/// U+2026 is appended only when its three bytes fit. The returned string never
/// splits a code point, including for caps 0, 1, and 2.
pub fn truncate_utf8_bytes(value: &str, max: usize) -> (String, TextTruncationV1) {
    let original_bytes = value.len();
    if original_bytes <= max {
        return (
            value.to_owned(),
            TextTruncationV1 {
                original_bytes,
                returned_bytes: original_bytes,
                truncated: false,
            },
        );
    }
    const ELLIPSIS: &str = "…";
    let content_cap = if max >= ELLIPSIS.len() {
        max - ELLIPSIS.len()
    } else {
        max
    };
    let mut end = content_cap.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut output = value[..end].to_owned();
    if max >= ELLIPSIS.len() {
        output.push_str(ELLIPSIS);
    }
    let returned_bytes = output.len();
    debug_assert!(returned_bytes <= max);
    (
        output,
        TextTruncationV1 {
            original_bytes,
            returned_bytes,
            truncated: true,
        },
    )
}

fn project_session(
    session: &Session,
    fields: FieldSet,
    source: Option<&SourceFullV1>,
) -> SessionProjectionV1 {
    match fields {
        FieldSet::Compact => SessionProjectionV1::Compact(SessionCompactV1 {
            ctx_session_id: session.id,
            provider: session.provider,
            agent_type: session.agent_type,
            status: session.status,
            is_primary: session.is_primary,
            started_at: session.started_at,
            ended_at: session.ended_at,
            fidelity: session.sync.fidelity,
        }),
        FieldSet::Full => SessionProjectionV1::Full(Box::new(SessionFullV1 {
            id: session.id,
            item_id: session.id,
            item_type: "session",
            ctx_session_id: session.id,
            history_record_id: session.history_record_id,
            parent_session_id: session.parent_session_id,
            root_session_id: session.root_session_id,
            capture_source_id: session.capture_source_id,
            provider: session.provider,
            external_session_id: session.external_session_id.clone(),
            provider_session_id: session.external_session_id.clone(),
            agent_type: session.agent_type,
            role_hint: session.role_hint.clone(),
            role: session.role_hint.clone(),
            source_id: session.capture_source_id,
            source_path: source.and_then(|value| value.path.clone()),
            source_exists: source.and_then(|value| value.exists),
            is_primary: session.is_primary,
            status: session.status,
            started_at: session.started_at,
            ended_at: session.ended_at,
            created_at: session.timestamps.created_at,
            updated_at: session.timestamps.updated_at,
            visibility: session.sync.visibility,
            fidelity: session.sync.fidelity,
        })),
    }
}

fn project_source(source: CaptureSource) -> SourceFullV1 {
    let source_format = [
        "/source_format",
        "/format",
        "/provider/source_format",
        "/source/source_format",
    ]
    .into_iter()
    .find_map(|pointer| {
        source
            .sync
            .metadata
            .pointer(pointer)
            .and_then(|value| value.as_str())
    })
    .map(str::to_owned);
    let source_cursor = source
        .sync
        .metadata
        .pointer("/cursor/after/cursor")
        .and_then(|value| value.as_str())
        .or_else(|| {
            source
                .sync
                .metadata
                .pointer("/cursor")
                .and_then(|value| value.as_str())
        })
        .map(str::to_owned);
    let path = source.descriptor.raw_source_path;
    let exists = path.as_deref().map(|value| Path::new(value).exists());
    SourceFullV1 {
        source_id: source.id,
        kind: source.descriptor.kind,
        provider: source.descriptor.provider,
        provider_session_id: source.descriptor.external_session_id,
        cwd: source.descriptor.cwd,
        path,
        exists,
        started_at: source.started_at,
        ended_at: source.ended_at,
        source_format,
        cursor: source_cursor.clone(),
        source_cursor,
    }
}

fn project_evidence_source(source: CaptureSource) -> SourceFullV1 {
    let mut projected = project_source(source);
    projected.cwd = None;
    projected.path = None;
    projected.exists = None;
    projected.source_cursor = None;
    projected.cursor = None;
    projected
}

fn event_projection_id(event: &EventProjectionV1) -> Uuid {
    match event {
        EventProjectionV1::Full(value) => value.ctx_event_id,
        EventProjectionV1::Compact(value) => value.ctx_event_id,
    }
}

fn search_result_event_ids(result: &SearchPacketResult) -> Vec<Uuid> {
    let mut seen = BTreeSet::new();
    result
        .event_id
        .into_iter()
        .chain(
            result
                .citations
                .iter()
                .filter(|citation| citation.citation_type == ContextCitationType::Event)
                .map(|citation| citation.id),
        )
        .filter(|id| seen.insert(*id))
        .collect()
}

fn project_event(
    event: &Event,
    cap: usize,
    provider: CaptureProvider,
    provider_session_id: Option<&str>,
    source: Option<&SourceFullV1>,
    fields: FieldSet,
) -> EventProjectionV1 {
    let preview = event_projection_text(event);
    let (text, text_truncation) = truncate_utf8_bytes(&preview, cap);
    match fields {
        FieldSet::Compact => EventProjectionV1::Compact(EventCompactV1 {
            ctx_event_id: event.id,
            ctx_session_id: event.session_id,
            seq: event.seq,
            event_type: event.event_type,
            role: event.role,
            occurred_at: event.occurred_at,
            text,
            text_truncation,
            redaction_state: event.redaction_state,
            visibility: event.sync.visibility,
            fidelity: event.sync.fidelity,
        }),
        FieldSet::Full => EventProjectionV1::Full(Box::new(EventFullV1 {
            item_id: event.id,
            item_type: "event",
            ctx_event_id: event.id,
            seq: event.seq,
            sequence: event.seq,
            history_record_id: event.history_record_id,
            ctx_session_id: event.session_id,
            run_id: event.run_id,
            capture_source_id: event.capture_source_id,
            source_id: event.capture_source_id,
            source_path: source.and_then(|value| value.path.clone()),
            source_exists: source.and_then(|value| value.exists),
            event_type: event.event_type,
            role: event.role,
            occurred_at: event.occurred_at,
            preview: text.clone(),
            text,
            text_truncation,
            redaction_state: event.redaction_state,
            visibility: event.sync.visibility,
            fidelity: event.sync.fidelity,
            provider,
            provider_session_id: provider_session_id.map(str::to_owned),
            source: source.cloned(),
            cursor: event_cursor(event),
        })),
    }
}

fn event_cursor(event: &Event) -> Option<String> {
    event
        .payload
        .get("cursor")
        .and_then(|value| value.as_str())
        .or_else(|| {
            event
                .payload
                .get("body")
                .and_then(|body| body.get("cursor"))
                .and_then(|value| value.as_str())
        })
        .map(str::to_owned)
}

fn event_projection_text(event: &Event) -> String {
    if matches!(
        event.redaction_state,
        RedactionState::Raw | RedactionState::Withheld
    ) {
        return "raw event payload withheld".to_owned();
    }
    if let Some(body) = event.payload.get("body") {
        if let Some(preview) = event_value_text(body) {
            return preview;
        }
    }
    if let Some(preview) = event_value_text(&event.payload) {
        return preview;
    }
    String::new()
}

fn event_value_text(value: &serde_json::Value) -> Option<String> {
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
        "last_agent_message",
    ] {
        if let Some(value) = object.get(key).and_then(event_preview_fragment) {
            return Some(value);
        }
    }
    for key in ["body", "payload", "data"] {
        if let Some(value) = object.get(key).and_then(event_value_text) {
            return Some(value);
        }
    }
    let structured = ["tool", "name", "arguments_preview", "status"]
        .into_iter()
        .filter_map(|key| {
            object
                .get(key)
                .and_then(event_preview_fragment)
                .map(|value| format!("{key}: {value}"))
        })
        .collect::<Vec<_>>();
    (!structured.is_empty()).then(|| structured.join(" | "))
}

fn event_preview_fragment(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => non_blank(value),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn non_blank(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn project_search_result(
    store: &Store,
    result: &SearchPacketResult,
    cap: usize,
    query: &str,
    terms: &[String],
    options: &PacketOptions,
    fields: FieldSet,
) -> SearchResultProjectionV1 {
    let (snippet, snippet_truncation) = truncate_utf8_bytes(&result.snippet, cap);
    match fields {
        FieldSet::Compact => SearchResultProjectionV1::Compact(SearchResultCompactV1 {
            item_id: result.record_id,
            ctx_session_id: result.session_id,
            ctx_event_id: result.event_id,
            event_seq: result.event_seq,
            title: result.title.clone(),
            snippet,
            snippet_truncation,
            rank: result.rank,
            result_scope: result.result_scope,
            more_matches_in_session: result.more_matches_in_session,
            session_importance: result.session_importance,
            timestamp: result.timestamp,
            why_matched: result.why_matched.clone(),
            visibility: result.visibility,
        }),
        FieldSet::Full => SearchResultProjectionV1::Full(Box::new(SearchResultFullV1 {
            item_id: result.record_id,
            item_type: search_result_item_type(store, result),
            ctx_session_id: result.session_id,
            ctx_event_id: result.event_id,
            session_id: result.session_id,
            event_id: result.event_id,
            event_seq: result.event_seq,
            title: result.title.clone(),
            snippet,
            snippet_truncation,
            rank: result.rank,
            result_scope: result.result_scope,
            more_matches_in_session: result.more_matches_in_session,
            session_importance: result.session_importance,
            provider: result.provider,
            provider_session_id: result.provider_session_id.clone(),
            history_source: result.history_source.clone(),
            history_source_plugin: result.history_source_plugin.clone(),
            provider_key: result.provider_key.clone(),
            source_id: result.source_id.clone(),
            source_format: result.source_format.clone(),
            timestamp: result.timestamp,
            cwd: result.cwd.clone(),
            source_path: result.raw_source_path.clone(),
            source_exists: result.raw_source_exists,
            source_cursor: result.cursor.clone(),
            cursor: result.cursor.clone(),
            why_matched: result.why_matched.clone(),
            citations: result
                .citations
                .iter()
                .map(|citation| SearchCitationV1 {
                    citation_type: citation.citation_type,
                    id: citation.id,
                    item_id: citation.id,
                    item_type: citation_item_type(citation.citation_type),
                    label: citation.label.clone(),
                    time: citation.time,
                    provider: citation.provider,
                    ctx_session_id: if citation.citation_type == ContextCitationType::Session {
                        Some(citation.id)
                    } else {
                        citation.session_id
                    },
                    session_id: citation.session_id,
                    ctx_event_id: (citation.citation_type == ContextCitationType::Event)
                        .then_some(citation.id),
                    event_seq: citation.event_seq,
                    source_path: citation.raw_source_path.clone(),
                    source_exists: citation.raw_source_exists,
                    source_cursor: citation.cursor.clone(),
                    cursor: citation.cursor.clone(),
                })
                .collect(),
            links: result.links.clone(),
            suggested_next_commands: suggested_next_commands(
                result,
                query,
                terms,
                options.match_mode,
            ),
            visibility: result.visibility,
        })),
    }
}

fn search_result_item_type(store: &Store, result: &SearchPacketResult) -> String {
    if result.result_scope == SearchResultScope::Session {
        return "session_result".to_owned();
    }
    if result.event_id == Some(result.record_id) {
        return "event".to_owned();
    }
    if result.session_id == Some(result.record_id) {
        return "session".to_owned();
    }
    if let Ok(record) = store.get_record(result.record_id) {
        return match record.kind.trim() {
            "" | "record" => "indexed_item".to_owned(),
            kind => kind.to_owned(),
        };
    }
    if store.get_event(result.record_id).is_ok() {
        return "event".to_owned();
    }
    if store.get_session(result.record_id).is_ok() {
        return "session".to_owned();
    }
    if store.get_run(result.record_id).is_ok() {
        return "run".to_owned();
    }
    "indexed_item".to_owned()
}

fn citation_item_type(citation_type: ContextCitationType) -> &'static str {
    match citation_type {
        ContextCitationType::HistoryRecord => "indexed_item",
        ContextCitationType::Session => "session",
        ContextCitationType::Run => "run",
        ContextCitationType::Event => "event",
        ContextCitationType::Artifact => "artifact",
        ContextCitationType::VcsChange => "vcs_change",
        ContextCitationType::Summary => "summary",
        ContextCitationType::File => "file",
    }
}

fn suggested_next_commands(
    result: &SearchPacketResult,
    query: &str,
    terms: &[String],
    match_mode: SearchMatchMode,
) -> Vec<String> {
    let mut commands = Vec::new();
    if let Some(event_id) = result.event_id {
        commands.push(format!("ctx show event {event_id} --window 10"));
    }
    if let Some(session_id) = result.session_id {
        // Repeated clauses cannot be represented by the historical scoped
        // suggestion without changing their OR semantics; preserve the parent
        // command only for the single-query spelling.
        if terms.is_empty() && !query.trim().is_empty() {
            let mut command = "ctx search".to_owned();
            if match_mode != SearchMatchMode::All {
                command.push_str(&format!(" --match {}", match_mode.as_str()));
            }
            command.push_str(&format!(
                " --session {session_id} -- {}",
                shell_quote_arg(query)
            ));
            commands.push(command);
        }
        commands.push(format!("ctx show session {session_id}"));
        commands.push(format!("ctx locate session {session_id}"));
    }
    if let Some(event_id) = result.event_id {
        commands.push(format!("ctx locate event {event_id}"));
    }
    commands
}

fn shell_quote_arg(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '@'))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn project_search_context(
    query: &str,
    terms: &[String],
    options: &PacketOptions,
    fields: FieldSet,
) -> SearchContextProjectionV1 {
    let compact = SearchContextCompactV1 {
        query: query.to_owned(),
        terms: terms.to_vec(),
        result_mode: result_mode_str(options.result_mode),
        match_mode: options.match_mode,
    };
    match fields {
        FieldSet::Compact => SearchContextProjectionV1::Compact(compact),
        FieldSet::Full => SearchContextProjectionV1::Full(Box::new(SearchContextFullV1 {
            query: compact.query,
            terms: compact.terms,
            result_mode: compact.result_mode,
            match_mode: compact.match_mode,
            filters: project_filters(&options.filters),
        })),
    }
}

fn project_filters(filters: &SearchFilters) -> SearchFiltersFullV1 {
    SearchFiltersFullV1 {
        session: filters.session,
        provider: filters.provider,
        history_source: filters.history_source.clone(),
        provider_key: filters.provider_key.clone(),
        source_id: filters.source_id.clone(),
        source_format: filters.source_format.clone(),
        workspace: filters.repo.clone(),
        since: filters.since,
        primary_only: filters.primary_only,
        include_subagents: filters.include_subagents,
        event_type: filters.event_type,
        roles: filters.roles.clone(),
        exclude_roles: filters.exclude_roles.clone(),
        exclude_tool_noise: filters.exclude_tool_noise,
        exclude_tool_names: filters.exclude_tool_names.clone(),
        file: filters.file.clone(),
        exclude_provider_session: filters.exclude_provider_session.as_ref().map(|value| {
            ExcludedProviderSessionV1 {
                provider: value.provider,
                provider_session_id: value.provider_session_id.clone(),
                ctx_session_id: value.session_id,
            }
        }),
    }
}

#[derive(Serialize)]
struct ShowRequestV1 {
    kind: &'static str,
    query_revision: u32,
    schema_version: u32,
    session_id: Uuid,
    mode: TranscriptMode,
    page_size: usize,
    fields: FieldSet,
    byte_policy: BytePolicy,
}

fn show_request_hash(
    session_id: Uuid,
    mode: TranscriptMode,
    page_size: usize,
    fields: FieldSet,
    byte_policy: BytePolicy,
) -> Result<String> {
    hash_serializable(&ShowRequestV1 {
        kind: "show_session",
        query_revision: QUERY_REVISION,
        schema_version: QUERY_DTO_SCHEMA_VERSION,
        session_id,
        mode,
        page_size,
        fields,
        byte_policy,
    })
}

#[derive(Serialize)]
struct SearchRequestV1<'a> {
    kind: &'static str,
    query_revision: u32,
    schema_version: u32,
    search_packet_schema_version: u32,
    query: &'a str,
    terms: &'a [String],
    page_size: usize,
    fields: FieldSet,
    byte_policy: BytePolicy,
    result_mode: &'static str,
    match_mode: SearchMatchMode,
    snippet_chars: usize,
    filters: SearchFiltersRequestV1<'a>,
}

#[derive(Serialize)]
struct SearchFiltersRequestV1<'a> {
    session: Option<Uuid>,
    provider: Option<CaptureProvider>,
    history_source: &'a Option<String>,
    provider_key: &'a Option<String>,
    source_id: &'a Option<String>,
    source_format: &'a Option<String>,
    workspace: &'a Option<String>,
    since: Option<DateTime<Utc>>,
    primary_only: bool,
    include_subagents: bool,
    event_type: Option<EventType>,
    roles: &'a [EventRole],
    exclude_roles: &'a [EventRole],
    exclude_tool_noise: bool,
    exclude_tool_names: &'a [String],
    file: &'a Option<String>,
    exclude_provider_session: Option<ExcludedProviderSessionRequestV1<'a>>,
}

#[derive(Serialize)]
struct ExcludedProviderSessionRequestV1<'a> {
    provider: CaptureProvider,
    provider_session_id: &'a str,
    session_id: Option<Uuid>,
}

fn search_request_hash(
    query: &str,
    terms: &[String],
    options: &PacketOptions,
    page_size: usize,
    fields: FieldSet,
    byte_policy: BytePolicy,
) -> Result<String> {
    let filters = &options.filters;
    hash_serializable(&SearchRequestV1 {
        kind: "search",
        query_revision: QUERY_REVISION,
        schema_version: QUERY_DTO_SCHEMA_VERSION,
        search_packet_schema_version: SEARCH_PACKET_SCHEMA_VERSION,
        query,
        terms,
        page_size,
        fields,
        byte_policy,
        result_mode: result_mode_str(options.result_mode),
        match_mode: options.match_mode,
        snippet_chars: options.snippet_chars,
        filters: SearchFiltersRequestV1 {
            session: filters.session,
            provider: filters.provider,
            history_source: &filters.history_source,
            provider_key: &filters.provider_key,
            source_id: &filters.source_id,
            source_format: &filters.source_format,
            workspace: &filters.repo,
            since: filters.since,
            primary_only: filters.primary_only,
            include_subagents: filters.include_subagents,
            event_type: filters.event_type,
            roles: &filters.roles,
            exclude_roles: &filters.exclude_roles,
            exclude_tool_noise: filters.exclude_tool_noise,
            exclude_tool_names: &filters.exclude_tool_names,
            file: &filters.file,
            exclude_provider_session: filters.exclude_provider_session.as_ref().map(|value| {
                ExcludedProviderSessionRequestV1 {
                    provider: value.provider,
                    provider_session_id: &value.provider_session_id,
                    session_id: value.session_id,
                }
            }),
        },
    })
}

fn result_mode_str(mode: SearchResultMode) -> &'static str {
    match mode {
        SearchResultMode::Sessions => "sessions",
        SearchResultMode::Events => "events",
    }
}

fn hash_serializable(value: &impl Serialize) -> Result<String> {
    Ok(hex(&Sha256::digest(serde_json::to_vec(value)?)))
}

fn encode_token(token: &Token) -> Result<String> {
    let bytes = serde_json::to_vec(token)?;
    if bytes.len() > MAX_TOKEN_BYTES {
        return Err(QueryError::InvalidContinuation(
            "encoded token exceeds size limit".to_owned(),
        ));
    }
    Ok(hex(&bytes))
}

fn decode_token(raw: &str, kind: &'static str, request: &str, snapshot: &str) -> Result<Token> {
    if raw.len() > MAX_TOKEN_BYTES.saturating_mul(2) {
        return Err(QueryError::InvalidContinuation(
            "token exceeds size limit".to_owned(),
        ));
    }
    let bytes = unhex(raw)?;
    let token: Token = serde_json::from_slice(&bytes)
        .map_err(|error| QueryError::InvalidContinuation(error.to_string()))?;
    if token.v != QUERY_DTO_SCHEMA_VERSION {
        return Err(QueryError::InvalidContinuation(
            "unsupported token schema".to_owned(),
        ));
    }
    if token.kind != kind {
        return Err(QueryError::ContinuationKind {
            expected: kind,
            found: token.kind,
        });
    }
    match kind {
        "show_session" | "evidence_session_page"
            if !matches!((token.offset, token.seq, token.id), (1.., Some(_), Some(_))) =>
        {
            return Err(QueryError::InvalidContinuation(
                "show token requires both seq and id after the first item".to_owned(),
            ));
        }
        "search" | "evidence_search_page" | "evidence_event_ids"
            if token.offset == 0 || token.seq.is_some() || token.id.is_some() =>
        {
            return Err(QueryError::InvalidContinuation(
                "search token forbids seq and id".to_owned(),
            ));
        }
        _ => {}
    }
    if token.request != request {
        return Err(QueryError::ContinuationRequestMismatch);
    }
    if token.snapshot != snapshot {
        return Err(QueryError::StaleContinuation);
    }
    Ok(token)
}

fn token_offset(token: Option<&Token>) -> Result<usize> {
    token.map_or(Ok(0), |value| {
        usize::try_from(value.offset).map_err(|_| {
            QueryError::InvalidContinuation("offset is outside the supported range".to_owned())
        })
    })
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn unhex(value: &str) -> Result<Vec<u8>> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(QueryError::InvalidContinuation(
            "hex length must be even".to_owned(),
        ));
    }
    let mut output = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(QueryError::InvalidContinuation(
            "token contains a non-hex character".to_owned(),
        )),
    }
}

fn source_location_json(source: &CaptureSource) -> Value {
    let path = source.descriptor.raw_source_path.clone();
    compact_json_value(
        json!({"source_id": source.id,"provider": source.descriptor.provider,"provider_session_id": source.descriptor.external_session_id,"path": path,"exists": path.as_deref().map(|path| Path::new(path).exists()),"cwd": source.descriptor.cwd,"started_at": source.started_at,"ended_at": source.ended_at,"source_format": source_format(&source.sync.metadata),"cursor": source_cursor(&source.sync.metadata)}),
    )
}

fn source_format(metadata: &Value) -> Option<String> {
    [
        "/source_format",
        "/format",
        "/provider/source_format",
        "/source/source_format",
    ]
    .iter()
    .find_map(|pointer| {
        metadata
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}
fn source_cursor(metadata: &Value) -> Option<String> {
    metadata
        .pointer("/cursor/after/cursor")
        .and_then(Value::as_str)
        .or_else(|| metadata.pointer("/cursor").and_then(Value::as_str))
        .map(str::to_owned)
}
fn provider_resume_json(provider: CaptureProvider, provider_session_id: Option<&str>) -> Value {
    let (command, argv) = match (provider, provider_session_id) {
        (CaptureProvider::Codex, Some(session_id)) => (
            Some(format!("codex resume {}", shell_quote_arg(session_id))),
            Some(vec![
                "codex".to_owned(),
                "resume".to_owned(),
                session_id.to_owned(),
            ]),
        ),
        _ => (None, None),
    };
    compact_json_value(json!({ "available": command.is_some(), "command": command, "argv": argv }))
}
fn compact_json_value(mut value: Value) -> Value {
    prune_null_json_value(&mut value);
    value
}
fn prune_null_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, nested| {
                prune_null_json_value(nested);
                !nested.is_null()
            });
        }
        Value::Array(items) => items.iter_mut().for_each(prune_null_json_value),
        _ => {}
    }
}
fn file_len(path: impl AsRef<Path>) -> u64 {
    fs::symlink_metadata(path)
        .ok()
        .filter(|m| m.file_type().is_file())
        .map(|m| m.len())
        .unwrap_or(0)
}
#[derive(Default)]
struct TreeSize {
    total_bytes: u64,
    children: Vec<(PathBuf, u64)>,
    complete: bool,
    omitted: usize,
}

impl TreeSize {
    fn child_bytes(&self, path: &Path) -> u64 {
        self.children
            .iter()
            .find_map(|(child, bytes)| (child == path).then_some(*bytes))
            .unwrap_or(0)
    }
}

fn sized_tree(path: &Path, diagnostics: &mut Vec<String>) -> TreeSize {
    let mut tree = TreeSize {
        complete: true,
        ..Default::default()
    };
    let Ok(entries) = fs::read_dir(path) else {
        if path.exists() {
            tree.complete = false;
            push_measurement_diag(
                diagnostics,
                &mut tree.omitted,
                "could not read data-root directory",
            );
        }
        return tree;
    };
    for entry in entries {
        let Ok(entry) = entry else {
            tree.complete = false;
            push_measurement_diag(
                diagnostics,
                &mut tree.omitted,
                "could not read a data-root entry",
            );
            continue;
        };
        let child_path = entry.path();
        let bytes = dir_size(
            &child_path,
            diagnostics,
            &mut tree.complete,
            &mut tree.omitted,
        );
        tree.total_bytes = tree.total_bytes.saturating_add(bytes);
        tree.children.push((child_path, bytes));
    }
    if tree.omitted > 0 {
        diagnostics.push(format!("measurement diagnostics omitted: {}", tree.omitted));
    }
    tree
}

fn dir_size(
    path: &Path,
    diagnostics: &mut Vec<String>,
    complete: &mut bool,
    omitted: &mut usize,
) -> u64 {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(_) => {
            *complete = false;
            push_measurement_diag(diagnostics, omitted, "could not measure data-root entry");
            return 0;
        }
    };
    if meta.file_type().is_symlink() {
        return 0;
    }
    if meta.is_file() {
        return meta.len();
    }
    let mut total = 0u64;
    let rd = match fs::read_dir(path) {
        Ok(rd) => rd,
        Err(_) => {
            *complete = false;
            push_measurement_diag(
                diagnostics,
                omitted,
                "could not read data-root directory entry",
            );
            return 0;
        }
    };
    for entry in rd {
        match entry {
            Ok(entry) => {
                total =
                    total.saturating_add(dir_size(&entry.path(), diagnostics, complete, omitted))
            }
            Err(_) => {
                *complete = false;
                push_measurement_diag(
                    diagnostics,
                    omitted,
                    "could not read data-root directory entry",
                );
            }
        }
    }
    total
}

fn push_measurement_diag(diagnostics: &mut Vec<String>, omitted: &mut usize, message: &str) {
    const CAP: usize = 20;
    if diagnostics.len() < CAP {
        diagnostics.push(message.to_owned());
    } else {
        *omitted += 1;
    }
}
/// Authoritative low-space classification shared by status/doctor
/// surfaces: `critical`, `warning`, `ok`, or `unknown`.
pub fn low_space(v: Option<u64>) -> &'static str {
    match v {
        Some(x) if x < LOW_SPACE_CRITICAL_BYTES => "critical",
        Some(x) if x < LOW_SPACE_WARNING_BYTES => "warning",
        Some(_) => "ok",
        None => "unknown",
    }
}

/// Authoritative low-free-space warning messages shared by the status JSON
/// projection and the CLI's human status/doctor output.
pub fn status_warnings(v: Option<u64>) -> Vec<String> {
    match v {
        Some(x) if x < LOW_SPACE_CRITICAL_BYTES => vec![format!(
            "critical low free space: {x} bytes available; imports can need temporary space and may fail"
        )],
        Some(x) if x < LOW_SPACE_WARNING_BYTES => vec![format!(
            "low free space: {x} bytes available; imports can need temporary space"
        )],
        _ => Vec::new(),
    }
}

#[cfg(unix)]
fn available_space_bytes(path: &Path) -> Option<u64> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    fn statvfs_field_to_u64<T>(value: T) -> Option<u64>
    where
        T: TryInto<u64>,
    {
        value.try_into().ok()
    }
    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stat = unsafe { stat.assume_init() };
    let available_blocks = statvfs_field_to_u64(stat.f_bavail)?;
    let fragment_size = statvfs_field_to_u64(stat.f_frsize)?;
    Some(available_blocks.saturating_mul(fragment_size))
}
#[cfg(not(unix))]
fn available_space_bytes(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctx_history_core::{
        CaptureSourceDescriptor, ContextCitation, EntityTimestamps, HistoryRecord, SyncMetadata,
        SyncState,
    };
    use ctx_history_search::ProviderSessionFilter;
    use std::time::{Duration, Instant};

    trait EventProjectionTestExt {
        fn full_for_test(self) -> EventFullV1;
        fn compact_for_test(self) -> EventCompactV1;
    }

    impl EventProjectionTestExt for EventProjectionV1 {
        fn full_for_test(self) -> EventFullV1 {
            match self {
                EventProjectionV1::Full(value) => *value,
                EventProjectionV1::Compact(_) => panic!("expected full event projection"),
            }
        }

        fn compact_for_test(self) -> EventCompactV1 {
            match self {
                EventProjectionV1::Compact(value) => value,
                EventProjectionV1::Full(_) => panic!("expected compact event projection"),
            }
        }
    }

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn status_snapshot_reports_absent_store_without_initializing() {
        let temp = tempfile::tempdir().unwrap();
        let snap = status_snapshot(temp.path(), "config.json").unwrap();
        assert!(!snap.initialized);
        assert_eq!(snap.counts.items, 0);
        assert!(!snap.db_path.exists());
        let value = status_json(&snap);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["initialized"], false);
        assert_eq!(value["read_only"], true);
        assert_eq!(value["share_safe"], false);
        assert!(value["storage"]
            .as_object()
            .unwrap()
            .contains_key("approx_bytes_per_event"));
        assert!(value["storage"]
            .as_object()
            .unwrap()
            .contains_key("available_space_bytes"));
        assert!(value["storage"]["approx_bytes_per_event"].is_null());
    }

    #[test]
    fn status_snapshot_uses_single_traversal_child_totals() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir(temp.path().join("objects")).unwrap();
        fs::create_dir(temp.path().join("spool")).unwrap();
        fs::write(temp.path().join("objects").join("a"), [0u8; 3]).unwrap();
        fs::write(temp.path().join("spool").join("b"), [0u8; 5]).unwrap();
        fs::write(temp.path().join("c"), [0u8; 7]).unwrap();
        let snap = status_snapshot(temp.path(), "config.json").unwrap();
        assert_eq!(snap.files.objects_bytes, 3);
        assert_eq!(snap.files.spool_bytes, 5);
        assert_eq!(snap.files.total_data_root_bytes, 15);
        assert!(snap.measurement_complete);
    }

    #[test]
    fn sources_response_preserves_caller_projected_sources_and_read_only_flag() {
        let source = json!({"provider": "codex", "status": "available", "path": "/tmp/codex"});
        let value = sources_response_json([source.clone()], true);
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["read_only"], true);
        assert_eq!(value["sources"].as_array().unwrap(), &[source]);

        let cli_value = sources_response_json(Vec::<Value>::new(), false);
        assert!(cli_value.get("read_only").is_none());
    }

    #[test]
    fn source_rows_project_native_plugin_and_failure_shapes() {
        let native = ProviderSource {
            provider: CaptureProvider::Codex,
            path: PathBuf::from("/tmp/codex"),
            exists: true,
            source_format: "codex_session_jsonl_tree",
            source_kind: ctx_history_capture::ProviderSourceKind::NativeHistory,
            import_support: ProviderImportSupport::Native,
            catalog_support: ctx_history_capture::ProviderCatalogSupport::None,
            status: ProviderSourceStatus::Available,
            raw_retention: ProviderRawRetention::PathReference,
            redaction_boundary: ctx_history_core::ProviderRedactionBoundary::ManualReview,
            unsupported_reason: None,
        };
        let plugin = HistorySourcePluginSourceProjection {
            plugin_name: "plug".into(),
            plugin_display_name: Some("Plug".into()),
            plugin_version: Some("1".into()),
            manifest_path: PathBuf::from("/tmp/manifest.json"),
            id: "src".into(),
            display_name: Some("Source".into()),
            provider_key: "agent".into(),
            source_id: "sid".into(),
            source_format: "fmt".into(),
            enabled: true,
            refresh: "auto",
        };
        let failure = HistorySourcePluginFailureProjection {
            manifest_path: PathBuf::from("/bad.json"),
            error: "bad manifest".into(),
        };
        let value = sources_json(&[native], &[plugin], &[failure], true);
        let rows = value["sources"].as_array().unwrap();
        assert_eq!(rows[0]["provider"], "codex");
        assert_eq!(rows[0]["native_import"], true);
        assert_eq!(rows[1]["history_source"], "plug/src");
        assert_eq!(rows[1]["refresh"], "auto");
        assert_eq!(rows[2]["status"], "invalid");
        assert_eq!(rows[2]["error"], "bad manifest");
    }

    #[test]
    fn raw_sql_values_project_scalars_and_truncation() {
        assert_eq!(raw_sql_value_json(&RawSqlValue::Null), Value::Null);
        assert_eq!(raw_sql_value_json(&RawSqlValue::Integer(7)), json!(7));
        assert_eq!(
            raw_sql_value_json(&RawSqlValue::Text {
                value: "abc".into(),
                bytes: 10,
                truncated: true
            }),
            json!({"type":"text","value":"abc","bytes":10,"truncated":true})
        );
        assert_eq!(
            raw_sql_value_json(&RawSqlValue::Blob {
                bytes: 4,
                preview_hex: "ffee".into(),
                truncated: false
            }),
            json!({"type":"blob","bytes":4,"preview_hex":"ffee","truncated":false})
        );
    }

    #[test]
    fn locate_dtos_serialize_with_omitted_optional_fields() {
        let dto = LocateSessionV1 {
            schema_version: 1,
            target: "session",
            item_type: "session_location",
            ctx_session_id: Uuid::nil(),
            provider: CaptureProvider::Codex,
            provider_session_id: Some("s1".into()),
            parent_ctx_session_id: None,
            root_ctx_session_id: None,
            agent_type: AgentType::Primary,
            role: None,
            status: SessionStatus::Imported,
            started_at: fixed_time(),
            ended_at: None,
            source: None,
            resume: json!({"available": true, "command": "codex resume s1", "argv": ["codex", "resume", "s1"]}),
        };
        let value = serde_json::to_value(dto).unwrap();
        assert_eq!(value["target"], "session");
        assert!(value.get("role").is_none());
        assert_eq!(value["resume"]["argv"][2], "s1");

        let event = LocateEventV1 {
            schema_version: 1,
            target: "event",
            item_type: "event_location",
            ctx_event_id: Uuid::nil(),
            ctx_session_id: None,
            provider: None,
            provider_session_id: None,
            sequence: 3,
            event_type: EventType::Message,
            role: None,
            occurred_at: fixed_time(),
            source: None,
            cursor: Some("cur".into()),
            resume: None,
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["sequence"], 3);
        assert!(value.get("provider").is_none());
        assert_eq!(value["cursor"], "cur");
    }

    #[test]
    fn locate_event_distinguishes_absent_relationships_from_store_errors() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        Store::open(&path).unwrap();
        let store = Store::open_read_only(&path).unwrap();
        let mut ev = event(Uuid::nil(), 9, EventType::Message, None, "body");
        ev.session_id = None;
        ev.capture_source_id = None;
        let dto = QueryService::new(&store).locate_event(&ev).unwrap();
        assert!(dto.provider.is_none());
        assert!(dto.source.is_none());

        ev.capture_source_id = Some(Uuid::from_u128(999));
        assert!(QueryService::new(&store).locate_event(&ev).is_err());
    }

    fn sync() -> SyncMetadata {
        SyncMetadata {
            visibility: Visibility::LocalOnly,
            fidelity: Fidelity::Imported,
            sync_state: SyncState::LocalOnly,
            sync_version: 0,
            deleted_at: None,
            metadata: serde_json::json!({"private": "must-not-project"}),
        }
    }

    fn session(id: Uuid) -> Session {
        Session {
            id,
            history_record_id: None,
            parent_session_id: None,
            root_session_id: None,
            capture_source_id: None,
            provider: CaptureProvider::Codex,
            external_session_id: Some("provider-session".to_owned()),
            external_agent_id: Some("external-agent".to_owned()),
            agent_type: AgentType::Primary,
            role_hint: Some("primary".to_owned()),
            is_primary: true,
            status: SessionStatus::Completed,
            transcript_blob_id: None,
            started_at: fixed_time(),
            ended_at: None,
            timestamps: EntityTimestamps {
                created_at: fixed_time(),
                updated_at: fixed_time(),
            },
            sync: sync(),
        }
    }

    fn event(
        session_id: Uuid,
        seq: u64,
        event_type: EventType,
        role: Option<EventRole>,
        text: &str,
    ) -> Event {
        Event {
            id: Uuid::from_u128(10_000 + seq as u128),
            seq,
            history_record_id: None,
            session_id: Some(session_id),
            run_id: None,
            event_type,
            role,
            occurred_at: fixed_time(),
            capture_source_id: None,
            payload: serde_json::json!({"text": text, "raw_secret": "do-not-project"}),
            payload_blob_id: None,
            dedupe_key: None,
            redaction_state: RedactionState::LocalPreview,
            sync: sync(),
        }
    }

    fn transcript_fixture(path: &std::path::Path) -> (Session, Vec<Event>) {
        let store = Store::open(path).unwrap();
        let session = session(Uuid::from_u128(42));
        store.upsert_session(&session).unwrap();
        let events = vec![
            event(
                session.id,
                0,
                EventType::Message,
                Some(EventRole::System),
                "sys",
            ),
            event(
                session.id,
                1,
                EventType::Message,
                Some(EventRole::User),
                "u1",
            ),
            event(
                session.id,
                2,
                EventType::Message,
                Some(EventRole::Assistant),
                "a1",
            ),
            event(
                session.id,
                3,
                EventType::ToolCall,
                Some(EventRole::Tool),
                "tool",
            ),
            event(
                session.id,
                4,
                EventType::Message,
                Some(EventRole::Assistant),
                "a2",
            ),
            event(
                session.id,
                5,
                EventType::Message,
                Some(EventRole::System),
                "sys2",
            ),
            event(
                session.id,
                6,
                EventType::Message,
                Some(EventRole::User),
                "u2",
            ),
            event(
                session.id,
                7,
                EventType::Message,
                Some(EventRole::Assistant),
                "a3",
            ),
            event(
                session.id,
                8,
                EventType::ToolOutput,
                Some(EventRole::Tool),
                "output",
            ),
        ];
        for event in &events {
            store.upsert_event(event).unwrap();
        }
        drop(store);
        (session, events)
    }

    fn bytes() -> BytePolicy {
        BytePolicy {
            per_item_bytes: MAX_ITEM_BYTES,
            page_bytes: MAX_PAGE_BYTES,
        }
    }

    fn event_sequences(events: &[EventProjectionV1]) -> Vec<u64> {
        events
            .iter()
            .map(|event| match event {
                EventProjectionV1::Full(value) => value.seq,
                EventProjectionV1::Compact(value) => value.seq,
            })
            .collect()
    }

    fn reference_project_event_pair(
        event: &Event,
        cap: usize,
        provider: CaptureProvider,
        provider_session_id: Option<&str>,
        source: Option<&SourceFullV1>,
    ) -> (EventFullV1, EventCompactV1) {
        let preview = event_projection_text(event);
        let (text, text_truncation) = truncate_utf8_bytes(&preview, cap);
        let compact = EventCompactV1 {
            ctx_event_id: event.id,
            ctx_session_id: event.session_id,
            seq: event.seq,
            event_type: event.event_type,
            role: event.role,
            occurred_at: event.occurred_at,
            text: text.clone(),
            text_truncation: text_truncation.clone(),
            redaction_state: event.redaction_state,
            visibility: event.sync.visibility,
            fidelity: event.sync.fidelity,
        };
        let full = EventFullV1 {
            item_id: event.id,
            item_type: "event",
            ctx_event_id: event.id,
            seq: event.seq,
            sequence: event.seq,
            history_record_id: event.history_record_id,
            ctx_session_id: event.session_id,
            run_id: event.run_id,
            capture_source_id: event.capture_source_id,
            source_id: event.capture_source_id,
            source_path: source.and_then(|value| value.path.clone()),
            source_exists: source.and_then(|value| value.exists),
            event_type: event.event_type,
            role: event.role,
            occurred_at: event.occurred_at,
            preview: text.clone(),
            text,
            text_truncation,
            redaction_state: event.redaction_state,
            visibility: event.sync.visibility,
            fidelity: event.sync.fidelity,
            provider,
            provider_session_id: provider_session_id.map(str::to_owned),
            source: source.cloned(),
            cursor: event_cursor(event),
        };
        (full, compact)
    }

    #[inline(never)]
    fn eager_event_compact_for_measurement(event: &Event, cap: usize) -> EventProjectionV1 {
        let (full, compact) = reference_project_event_pair(
            std::hint::black_box(event),
            std::hint::black_box(cap),
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
        );
        std::hint::black_box(full);
        EventProjectionV1::Compact(compact)
    }

    #[inline(never)]
    fn selected_event_compact_for_measurement(event: &Event, cap: usize) -> EventProjectionV1 {
        std::hint::black_box(project_event(
            std::hint::black_box(event),
            std::hint::black_box(cap),
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
            FieldSet::Compact,
        ))
    }

    fn search_result(i: usize, text: &str) -> SearchPacketResult {
        let session_id = Uuid::from_u128(90_000 + i as u128);
        let event_id = Uuid::from_u128(100_000 + i as u128);
        let shape = i % 4;
        let snippet = if i % 5 == 0 {
            format!("short snippet {i} λ")
        } else {
            format!("{text} -- snippet {i} -- {}", "λ".repeat(128))
        };
        SearchPacketResult {
            // Synthetic distribution: 25% event shortcut, 25% session scope,
            // 25% generic/fallback store lookup by event id, 25% absent indexed-item fallback.
            record_id: match shape {
                0 => event_id,
                1 => session_id,
                2 => event_id,
                _ => Uuid::from_u128(200_000 + i as u128),
            },
            session_id: (i % 6 != 0).then_some(session_id),
            event_id: (shape != 1).then_some(event_id),
            event_seq: (i % 7 != 0).then_some(i as u64),
            title: format!("synthetic title {i} 😃"),
            snippet,
            rank: 1.0 / (i as f32 + 1.0),
            result_scope: if shape == 1 {
                SearchResultScope::Session
            } else {
                SearchResultScope::Event
            },
            more_matches_in_session: i % 7,
            session_importance: (i % 11) as f32,
            provider: Some(CaptureProvider::Codex),
            provider_session_id: (i % 3 != 0).then(|| format!("provider-session-{i}")),
            history_source: (i % 4 != 0).then(|| "jsonl".to_owned()),
            history_source_plugin: (i % 4 != 0).then(|| "synthetic-plugin".to_owned()),
            provider_key: (i % 5 != 0).then(|| "codex/default".to_owned()),
            source_id: (i % 3 != 1).then(|| format!("source-{i}")),
            source_format: (i % 3 != 1).then(|| "ctx-history-jsonl-v1".to_owned()),
            timestamp: (i % 8 != 0).then(fixed_time),
            cwd: (i % 3 != 2).then(|| format!("/tmp/synthetic/{i}")),
            raw_source_path: (i % 3 != 2).then(|| format!("/tmp/synthetic/{i}/transcript.jsonl")),
            raw_source_exists: Some(i % 2 == 0),
            cursor: (i % 4 != 2).then(|| format!("cursor-{i}:{}", "界".repeat(16))),
            why_matched: vec!["title".into(), "snippet".into(), format!("term-{i}")],
            citations: vec![
                ContextCitation {
                    citation_type: ContextCitationType::Session,
                    id: session_id,
                    label: "session citation".into(),
                    time: fixed_time(),
                    provider: Some(CaptureProvider::Codex),
                    session_id: Some(session_id),
                    event_seq: None,
                    raw_source_path: Some("/tmp/session.jsonl".into()),
                    raw_source_exists: Some(true),
                    cursor: Some("session-cursor".into()),
                },
                ContextCitation {
                    citation_type: ContextCitationType::Event,
                    id: event_id,
                    label: "event citation".into(),
                    time: fixed_time(),
                    provider: Some(CaptureProvider::Codex),
                    session_id: Some(session_id),
                    event_seq: Some(i as u64),
                    raw_source_path: Some("/tmp/event.jsonl".into()),
                    raw_source_exists: Some(false),
                    cursor: Some("event-cursor".into()),
                },
            ],
            links: ContextLinks::default(),
            visibility: Visibility::LocalOnly,
        }
    }

    fn reference_project_search_result_pair(
        store: &Store,
        result: &SearchPacketResult,
        cap: usize,
        query: &str,
        terms: &[String],
        options: &PacketOptions,
    ) -> (SearchResultFullV1, SearchResultCompactV1) {
        let (snippet, snippet_truncation) = truncate_utf8_bytes(&result.snippet, cap);
        let compact = SearchResultCompactV1 {
            item_id: result.record_id,
            ctx_session_id: result.session_id,
            ctx_event_id: result.event_id,
            event_seq: result.event_seq,
            title: result.title.clone(),
            snippet: snippet.clone(),
            snippet_truncation: snippet_truncation.clone(),
            rank: result.rank,
            result_scope: result.result_scope,
            more_matches_in_session: result.more_matches_in_session,
            session_importance: result.session_importance,
            timestamp: result.timestamp,
            why_matched: result.why_matched.clone(),
            visibility: result.visibility,
        };
        let full = SearchResultFullV1 {
            item_id: result.record_id,
            item_type: search_result_item_type(store, result),
            ctx_session_id: result.session_id,
            ctx_event_id: result.event_id,
            session_id: result.session_id,
            event_id: result.event_id,
            event_seq: result.event_seq,
            title: result.title.clone(),
            snippet,
            snippet_truncation,
            rank: result.rank,
            result_scope: result.result_scope,
            more_matches_in_session: result.more_matches_in_session,
            session_importance: result.session_importance,
            provider: result.provider,
            provider_session_id: result.provider_session_id.clone(),
            history_source: result.history_source.clone(),
            history_source_plugin: result.history_source_plugin.clone(),
            provider_key: result.provider_key.clone(),
            source_id: result.source_id.clone(),
            source_format: result.source_format.clone(),
            timestamp: result.timestamp,
            cwd: result.cwd.clone(),
            source_path: result.raw_source_path.clone(),
            source_exists: result.raw_source_exists,
            source_cursor: result.cursor.clone(),
            cursor: result.cursor.clone(),
            why_matched: result.why_matched.clone(),
            citations: result
                .citations
                .iter()
                .map(|citation| SearchCitationV1 {
                    citation_type: citation.citation_type,
                    id: citation.id,
                    item_id: citation.id,
                    item_type: citation_item_type(citation.citation_type),
                    label: citation.label.clone(),
                    time: citation.time,
                    provider: citation.provider,
                    ctx_session_id: if citation.citation_type == ContextCitationType::Session {
                        Some(citation.id)
                    } else {
                        citation.session_id
                    },
                    session_id: citation.session_id,
                    ctx_event_id: (citation.citation_type == ContextCitationType::Event)
                        .then_some(citation.id),
                    event_seq: citation.event_seq,
                    source_path: citation.raw_source_path.clone(),
                    source_exists: citation.raw_source_exists,
                    source_cursor: citation.cursor.clone(),
                    cursor: citation.cursor.clone(),
                })
                .collect(),
            links: result.links.clone(),
            suggested_next_commands: suggested_next_commands(
                result,
                query,
                terms,
                options.match_mode,
            ),
            visibility: result.visibility,
        };
        (full, compact)
    }

    #[inline(never)]
    fn eager_search_compact_for_measurement(
        store: &Store,
        result: &SearchPacketResult,
        cap: usize,
    ) -> SearchResultProjectionV1 {
        let terms = ["alpha".to_owned(), "beta".to_owned()];
        let options = PacketOptions::default();
        let (full, compact) = reference_project_search_result_pair(
            std::hint::black_box(store),
            std::hint::black_box(result),
            std::hint::black_box(cap),
            "query",
            &terms,
            &options,
        );
        std::hint::black_box(full);
        SearchResultProjectionV1::Compact(compact)
    }

    #[inline(never)]
    fn selected_search_compact_for_measurement(
        store: &Store,
        result: &SearchPacketResult,
        cap: usize,
    ) -> SearchResultProjectionV1 {
        std::hint::black_box(project_search_result(
            std::hint::black_box(store),
            std::hint::black_box(result),
            std::hint::black_box(cap),
            "query",
            &["alpha".into(), "beta".into()],
            &PacketOptions::default(),
            FieldSet::Compact,
        ))
    }

    #[inline(never)]
    fn release_benchmark_build() -> bool {
        !cfg!(debug_assertions)
    }

    #[test]
    fn selected_only_projections_match_reference_serialization() {
        let source = SourceFullV1 {
            source_id: Uuid::from_u128(7),
            kind: CaptureSourceKind::ProviderImport,
            provider: CaptureProvider::Codex,
            provider_session_id: Some("provider-session".into()),
            cwd: Some("/tmp/ctx".into()),
            path: Some("/tmp/ctx/transcript.jsonl".into()),
            exists: Some(true),
            started_at: fixed_time(),
            ended_at: None,
            source_format: Some("ctx-history-jsonl-v1".into()),
            cursor: Some("source-cursor".into()),
            source_cursor: Some("source-cursor".into()),
        };
        let mut ev = event(
            Uuid::from_u128(42),
            10,
            EventType::Message,
            Some(EventRole::Assistant),
            "short λ",
        );
        ev.capture_source_id = Some(source.source_id);
        ev.payload = json!({"body": {"text": "🙂x", "cursor": "event-cursor"}});
        for (cap, fields) in [
            (5, FieldSet::Full),
            (5, FieldSet::Compact),
            (64, FieldSet::Full),
            (64, FieldSet::Compact),
        ] {
            let (old_full, old_compact) = reference_project_event_pair(
                &ev,
                cap,
                CaptureProvider::Codex,
                Some("provider-session"),
                Some(&source),
            );
            let old = match fields {
                FieldSet::Full => EventProjectionV1::Full(Box::new(old_full)),
                FieldSet::Compact => EventProjectionV1::Compact(old_compact),
            };
            let selected = project_event(
                &ev,
                cap,
                CaptureProvider::Codex,
                Some("provider-session"),
                Some(&source),
                fields,
            );
            assert_eq!(
                serde_json::to_value(old).unwrap(),
                serde_json::to_value(selected).unwrap()
            );
        }

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store_rw = Store::open(&path).unwrap();
        let session = session(Uuid::from_u128(90_001));
        store_rw.upsert_session(&session).unwrap();
        let stored_event = event(
            session.id,
            3,
            EventType::Message,
            Some(EventRole::User),
            "stored event",
        );
        store_rw.upsert_event(&stored_event).unwrap();
        drop(store_rw);
        let store = Store::open_read_only(&path).unwrap();
        let cases = vec![
            search_result(0, "short λ"),
            search_result(1, &"long🙂".repeat(1_000)),
            search_result(2, &"fallback界".repeat(800)),
            search_result(3, "absent optional"),
        ];
        let terms = ["alpha".to_owned(), "beta".to_owned()];
        let options = PacketOptions::default();
        for result in &cases {
            for (cap, fields) in [
                (12, FieldSet::Full),
                (12, FieldSet::Compact),
                (MAX_ITEM_BYTES, FieldSet::Full),
                (MAX_ITEM_BYTES, FieldSet::Compact),
            ] {
                let (old_full, old_compact) = reference_project_search_result_pair(
                    &store, result, cap, "query", &terms, &options,
                );
                let old = match fields {
                    FieldSet::Full => SearchResultProjectionV1::Full(Box::new(old_full)),
                    FieldSet::Compact => SearchResultProjectionV1::Compact(old_compact),
                };
                let selected =
                    project_search_result(&store, result, cap, "query", &terms, &options, fields);
                assert_eq!(
                    serde_json::to_value(old).unwrap(),
                    serde_json::to_value(selected).unwrap()
                );
            }
        }
    }

    #[test]
    #[ignore = "release-mode synthetic benchmark; run manually with --release -- --ignored --nocapture"]
    fn compact_projection_selected_only_benchmark() {
        if !release_benchmark_build() {
            panic!("compact projection benchmark must be run with --release");
        }
        const ROWS: usize = 2_000;
        const REPS: usize = 80;
        const CAP: usize = 4_096;
        let threshold = 0.90_f64; // candidate p95 must be at least 10% faster than eager.
        let long = format!(
            "{}{}{}",
            "ASCII ".repeat(256),
            "Здравствуйте 🌍 ".repeat(128),
            "終".repeat(256)
        );
        let mut events = (0..ROWS).map(|i| {
            let mut ev = event(Uuid::from_u128(42), i as u64, EventType::Message, Some(if i % 2 == 0 { EventRole::User } else { EventRole::Assistant }), &format!("{long} event {i}"));
            ev.payload = json!({"body": {"text": format!("{long} body {i}"), "cursor": format!("event-cursor-{i}")}});
            ev
        }).collect::<Vec<_>>();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store_rw = Store::open(&path).unwrap();
        // Populate the temp store for full-projection fallback lookup paths:
        // event shortcut (25%), session scope (25%), event lookup by record_id
        // (25%), and absent generic indexed-item fallback (25%).
        for i in 0..ROWS {
            let session_id = Uuid::from_u128(90_000 + i as u128);
            let event_id = Uuid::from_u128(100_000 + i as u128);
            let mut s = session(session_id);
            s.external_session_id = Some(format!("provider-session-{i}"));
            store_rw.upsert_session(&s).unwrap();
            let mut ev = event(
                session_id,
                i as u64,
                EventType::Message,
                Some(EventRole::User),
                &long,
            );
            ev.id = event_id;
            store_rw.upsert_event(&ev).unwrap();
        }
        drop(store_rw);
        let store = Store::open_read_only(&path).unwrap();
        let results = (0..ROWS)
            .map(|i| search_result(i, &long))
            .collect::<Vec<_>>();

        let base_event_json =
            serde_json::to_vec(&eager_event_compact_for_measurement(&events[0], CAP)).unwrap();
        let cand_event_json = serde_json::to_vec(&project_event(
            &events[0],
            CAP,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
            FieldSet::Compact,
        ))
        .unwrap();
        assert_eq!(base_event_json, cand_event_json);
        let base_search_json = serde_json::to_vec(&eager_search_compact_for_measurement(
            &store,
            &results[0],
            CAP,
        ))
        .unwrap();
        let cand_search_json = serde_json::to_vec(&project_search_result(
            &store,
            &results[0],
            CAP,
            "query",
            &["alpha".into(), "beta".into()],
            &PacketOptions::default(),
            FieldSet::Compact,
        ))
        .unwrap();
        assert_eq!(base_search_json, cand_search_json);

        for ev in events.iter().take(128) {
            std::hint::black_box(eager_event_compact_for_measurement(ev, CAP));
            std::hint::black_box(selected_event_compact_for_measurement(ev, CAP));
        }
        for result in results.iter().take(128) {
            std::hint::black_box(eager_search_compact_for_measurement(&store, result, CAP));
            std::hint::black_box(selected_search_compact_for_measurement(&store, result, CAP));
        }

        fn run_pair(
            mut eager: impl FnMut(),
            mut candidate: impl FnMut(),
        ) -> (Vec<Duration>, Vec<Duration>) {
            let mut eager_times = Vec::with_capacity(REPS);
            let mut candidate_times = Vec::with_capacity(REPS);
            for i in 0..REPS {
                if i % 2 == 0 {
                    let start = Instant::now();
                    eager();
                    eager_times.push(start.elapsed());
                    let start = Instant::now();
                    candidate();
                    candidate_times.push(start.elapsed());
                } else {
                    let start = Instant::now();
                    candidate();
                    candidate_times.push(start.elapsed());
                    let start = Instant::now();
                    eager();
                    eager_times.push(start.elapsed());
                }
            }
            eager_times.sort();
            candidate_times.sort();
            (eager_times, candidate_times)
        }

        fn project_all_events_eager(events: &[Event]) {
            for ev in events {
                std::hint::black_box(eager_event_compact_for_measurement(ev, CAP));
            }
        }
        fn project_all_events_candidate(events: &[Event]) {
            for ev in events {
                std::hint::black_box(selected_event_compact_for_measurement(ev, CAP));
            }
        }
        fn project_all_search_eager(store: &Store, results: &[SearchPacketResult]) {
            for result in results {
                std::hint::black_box(eager_search_compact_for_measurement(store, result, CAP));
            }
        }
        fn project_all_search_candidate(store: &Store, results: &[SearchPacketResult]) {
            for result in results {
                std::hint::black_box(selected_search_compact_for_measurement(store, result, CAP));
            }
        }

        let (event_eager, event_candidate) = run_pair(
            || project_all_events_eager(&events),
            || project_all_events_candidate(&events),
        );
        let (search_eager, search_candidate) = run_pair(
            || project_all_search_eager(&store, &results),
            || project_all_search_candidate(&store, &results),
        );
        let p50 = |v: &[Duration]| v[v.len() / 2].as_secs_f64() * 1000.0;
        let p95 = |v: &[Duration]| v[v.len() * 95 / 100].as_secs_f64() * 1000.0;
        println!("compact projection benchmark threshold: candidate p95/eager p95 <= {threshold:.2}; rows={ROWS}; reps={REPS}; cap={CAP}; release={}", !cfg!(debug_assertions));
        println!(
            "search shape distribution: event-shortcut=25%, session-scope=25%, event-store-lookup=25%, absent-indexed-item-fallback=25%; short-snippet=20%; optional fields mixed present/absent"
        );
        for (name, eager, candidate) in [
            ("event", &event_eager, &event_candidate),
            ("search", &search_eager, &search_candidate),
        ] {
            let ratio = p95(candidate) / p95(eager);
            println!("{name}: eager p50={:.3}ms p95={:.3}ms; candidate p50={:.3}ms p95={:.3}ms; p95_ratio={ratio:.3}", p50(eager), p95(eager), p50(candidate), p95(candidate));
            assert!(
                ratio <= threshold,
                "{name} candidate p95 ratio {ratio:.3} exceeded threshold {threshold:.2}"
            );
        }
        events.clear();
    }

    #[test]
    fn utf8_byte_caps_cover_zero_one_two_and_emoji() {
        for (cap, expected) in [(0, ""), (1, "a"), (2, "ab"), (3, "…")] {
            let (value, metadata) = truncate_utf8_bytes("abcdef", cap);
            assert_eq!(value, expected);
            assert_eq!(metadata.returned_bytes, expected.len());
            assert_eq!(metadata.original_bytes, 6);
            assert!(metadata.truncated);
            assert!(value.len() <= cap);
        }
        for (cap, expected) in [(0, ""), (1, ""), (2, ""), (3, "…"), (4, "…")] {
            let (value, metadata) = truncate_utf8_bytes("🙂x", cap);
            assert_eq!(value, expected);
            assert_eq!(metadata.returned_bytes, expected.len());
            assert!(value.is_char_boundary(value.len()));
            assert!(value.len() <= cap);
        }
        let (value, metadata) = truncate_utf8_bytes("🙂x", 5);
        assert_eq!(value, "🙂x");
        assert!(!metadata.truncated);
    }

    #[test]
    fn byte_policy_allows_zero_and_rejects_values_above_caps() {
        assert!(BytePolicy {
            per_item_bytes: 0,
            page_bytes: 0
        }
        .validate()
        .is_ok());
        assert!(matches!(
            BytePolicy {
                per_item_bytes: MAX_ITEM_BYTES + 1,
                page_bytes: 0
            }
            .validate(),
            Err(QueryError::InvalidBytePolicy {
                field: "per_item_bytes",
                ..
            })
        ));
    }

    #[test]
    fn transcript_modes_select_before_pagination_and_page_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (session, _) = transcript_fixture(&path);
        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);

        let full = service
            .session_events(
                session.clone(),
                TranscriptMode::Full,
                20,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(full.selected_total, 7);
        assert_eq!(full.events.len(), 7);
        assert_eq!(event_sequences(&full.events), vec![0, 1, 2, 4, 5, 6, 7]);
        assert_eq!(full.omitted.after, 0);

        let log = service
            .session_events(
                session.clone(),
                TranscriptMode::Log,
                20,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(log.selected_total, 9);
        assert_eq!(event_sequences(&log.events), (0..9).collect::<Vec<_>>());

        let page1 = service
            .session_events(
                session.clone(),
                TranscriptMode::Lite,
                2,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(page1.selected_total, 4);
        assert_eq!(page1.events.len(), 2);
        assert_eq!(event_sequences(&page1.events), vec![1, 4]);
        assert_eq!(page1.omitted.after, 2);
        assert!(page1.pagination.has_more);
        let token = page1.pagination.continuation.clone().unwrap();
        let before_reopen = store.snapshot_fingerprint().unwrap();
        drop(store);
        // Continuations are intentionally ephemeral but must work across a
        // normal read-only process reopen while the physical store is stable.
        let store = Store::open_read_only(&path).unwrap();
        let after_reopen = store.snapshot_fingerprint().unwrap();
        assert_eq!(before_reopen, after_reopen, "stable read-only reopen");
        let service = QueryService::new(&store);
        let page2 = service
            .session_events(
                session.clone(),
                TranscriptMode::Lite,
                2,
                Some(&token),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(page2.events.len(), 2);
        assert_eq!(event_sequences(&page2.events), vec![6, 7]);
        assert_eq!(page2.omitted.before, 2);
        assert_eq!(page2.omitted.after, 0);
        assert!(!page2.pagination.has_more);

        let zero = service.session_events(
            session,
            TranscriptMode::Lite,
            0,
            None,
            FieldSet::Compact,
            bytes(),
        );
        assert!(matches!(zero, Err(QueryError::InvalidPageSize)));
    }

    #[test]
    fn event_projection_redacts_raw_and_has_structural_compact_shape() {
        let mut raw = event(
            Uuid::from_u128(42),
            1,
            EventType::Message,
            Some(EventRole::User),
            "secret payload",
        );
        raw.redaction_state = RedactionState::Raw;
        raw.dedupe_key = Some("private-dedupe".to_owned());
        let full = match project_event(
            &raw,
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
            FieldSet::Full,
        ) {
            EventProjectionV1::Full(value) => *value,
            EventProjectionV1::Compact(_) => unreachable!(),
        };
        let compact = match project_event(
            &raw,
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
            FieldSet::Compact,
        ) {
            EventProjectionV1::Compact(value) => value,
            EventProjectionV1::Full(_) => unreachable!(),
        };
        assert_eq!(full.text, "raw event payload withheld");
        assert_eq!(compact.text, "raw event payload withheld");
        raw.redaction_state = RedactionState::Withheld;
        assert_eq!(
            project_event(
                &raw,
                DEFAULT_ITEM_BYTES,
                CaptureProvider::Codex,
                Some("provider-session"),
                None,
                FieldSet::Full,
            )
            .full_for_test()
            .text,
            "raw event payload withheld"
        );
        let compact_json = serde_json::to_value(compact).unwrap();
        for forbidden in [
            "provider_session_id",
            "capture_source_id",
            "cwd",
            "cursor",
            "citations",
            "payload",
            "suggested_commands",
        ] {
            assert!(compact_json.get(forbidden).is_none(), "{forbidden}");
        }
        let full_json = serde_json::to_string(&full).unwrap();
        assert!(!full_json.contains("raw_secret"));
        assert!(!full_json.contains("private-dedupe"));

        raw.redaction_state = RedactionState::LocalPreview;
        raw.payload = serde_json::json!({"text": "🙂".repeat(2_000)});
        let long = project_event(&raw, 10, CaptureProvider::Codex, None, None, FieldSet::Full)
            .full_for_test();
        assert_eq!(long.text_truncation.original_bytes, 8_000);
        assert_eq!(long.text_truncation.returned_bytes, 7);
        assert!(long.text_truncation.truncated);

        raw.payload = serde_json::json!({"api_key": "must-never-be-a-text-fallback"});
        let unknown = project_event(
            &raw,
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            None,
            None,
            FieldSet::Compact,
        )
        .compact_for_test();
        assert!(unknown.text.is_empty());
        assert!(!serde_json::to_string(&unknown)
            .unwrap()
            .contains("must-never-be-a-text-fallback"));
    }

    #[test]
    fn token_shape_kind_request_bytes_and_staleness_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (session, events) = transcript_fixture(&path);
        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);
        let first = service
            .session_events(
                session.clone(),
                TranscriptMode::Log,
                1,
                None,
                FieldSet::Full,
                BytePolicy::default(),
            )
            .unwrap();
        let token = first.pagination.continuation.as_deref().unwrap();
        assert!(matches!(
            service.session_events(
                session.clone(),
                TranscriptMode::Log,
                1,
                Some("é"),
                FieldSet::Full,
                BytePolicy::default(),
            ),
            Err(QueryError::InvalidContinuation(_))
        ));
        assert!(matches!(
            service.session_events(
                session.clone(),
                TranscriptMode::Full,
                1,
                Some(token),
                FieldSet::Full,
                BytePolicy::default(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        assert!(matches!(
            service.session_events(
                session.clone(),
                TranscriptMode::Log,
                1,
                Some(token),
                FieldSet::Full,
                BytePolicy {
                    per_item_bytes: 1,
                    ..BytePolicy::default()
                },
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));

        let snapshot = store.snapshot_fingerprint().unwrap();
        let request = show_request_hash(
            session.id,
            TranscriptMode::Log,
            1,
            FieldSet::Full,
            BytePolicy::default(),
        )
        .unwrap();
        let wrong_kind = encode_token(&Token {
            v: 1,
            kind: "search".to_owned(),
            request: request.clone(),
            snapshot: snapshot.clone(),
            offset: 1,
            seq: None,
            id: None,
        })
        .unwrap();
        assert!(matches!(
            service.session_events(
                session.clone(),
                TranscriptMode::Log,
                1,
                Some(&wrong_kind),
                FieldSet::Full,
                BytePolicy::default(),
            ),
            Err(QueryError::ContinuationKind { .. })
        ));
        let malformed_shape = encode_token(&Token {
            v: 1,
            kind: "show_session".to_owned(),
            request: request.clone(),
            snapshot: snapshot.clone(),
            offset: 1,
            seq: None,
            id: None,
        })
        .unwrap();
        assert!(matches!(
            service.session_events(
                session.clone(),
                TranscriptMode::Log,
                1,
                Some(&malformed_shape),
                FieldSet::Full,
                BytePolicy::default(),
            ),
            Err(QueryError::InvalidContinuation(_))
        ));

        for forged in [
            Token {
                v: 1,
                kind: "show_session".to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: 0,
                seq: None,
                id: None,
            },
            Token {
                v: 1,
                kind: "show_session".to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: 0,
                seq: Some(events[0].seq),
                id: Some(events[0].id),
            },
            Token {
                v: 1,
                kind: "show_session".to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: 2,
                seq: Some(events[0].seq),
                id: Some(events[0].id),
            },
            Token {
                v: 1,
                kind: "show_session".to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: 99,
                seq: Some(events[0].seq),
                id: Some(events[0].id),
            },
            Token {
                v: 1,
                kind: "show_session".to_owned(),
                request: request.clone(),
                snapshot: snapshot.clone(),
                offset: events.len() as u64,
                seq: Some(events.last().unwrap().seq),
                id: Some(events.last().unwrap().id),
            },
        ] {
            let forged = encode_token(&forged).unwrap();
            assert!(matches!(
                service.session_events(
                    session.clone(),
                    TranscriptMode::Log,
                    1,
                    Some(&forged),
                    FieldSet::Full,
                    BytePolicy::default(),
                ),
                Err(QueryError::InvalidContinuation(_))
            ));
        }
        drop(store);
        let writable = Store::open(&path).unwrap();
        writable
            .upsert_event(&event(
                session.id,
                99,
                EventType::Message,
                Some(EventRole::User),
                "new",
            ))
            .unwrap();
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        assert!(matches!(
            QueryService::new(&store).session_events(
                session,
                TranscriptMode::Log,
                1,
                Some(token),
                FieldSet::Full,
                BytePolicy::default(),
            ),
            Err(QueryError::StaleContinuation)
        ));
    }

    #[test]
    fn page_budget_counts_compact_json_and_never_partially_emits_an_item() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (session, raw_events) = transcript_fixture(&path);
        let first_full = project_event(
            &raw_events[0],
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
            FieldSet::Full,
        )
        .full_for_test();
        let exact = serde_json::to_vec(&EventProjectionV1::Full(Box::new(first_full)))
            .unwrap()
            .len();
        let store = Store::open_read_only(&path).unwrap();
        let page = QueryService::new(&store)
            .session_events(
                session.clone(),
                TranscriptMode::Log,
                2,
                None,
                FieldSet::Full,
                BytePolicy {
                    per_item_bytes: DEFAULT_ITEM_BYTES,
                    page_bytes: exact,
                },
            )
            .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.bytes.item_json_bytes, exact);
        assert!(page.bytes.page_budget_exhausted);
        assert!(page.pagination.continuation.is_some());

        let none = QueryService::new(&store).session_events(
            session,
            TranscriptMode::Log,
            2,
            None,
            FieldSet::Compact,
            BytePolicy {
                per_item_bytes: 0,
                page_bytes: 0,
            },
        );
        assert!(matches!(
            none,
            Err(QueryError::ItemExceedsPageBudget { page_bytes: 0, .. })
        ));
    }

    #[test]
    fn long_transcript_uses_bounded_public_pages_without_loading_the_session() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        let session = session(Uuid::from_u128(4242));
        store.upsert_session(&session).unwrap();
        for seq in 0..1_201_u64 {
            store
                .upsert_event(&event(
                    session.id,
                    seq,
                    EventType::Message,
                    Some(EventRole::User),
                    "bounded",
                ))
                .unwrap();
        }
        drop(store);
        let store = Store::open_read_only(&path).unwrap();
        let first = QueryService::new(&store)
            .session_events(
                session.clone(),
                TranscriptMode::Full,
                MAX_SHOW_LIMIT,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(first.selected_total, 1_201);
        assert_eq!(first.events.len(), 1_000);
        assert_eq!(first.omitted.after, 201);
        let second = QueryService::new(&store)
            .session_events(
                session,
                TranscriptMode::Full,
                MAX_SHOW_LIMIT,
                first.pagination.continuation.as_deref(),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(second.events.len(), 201);
        assert_eq!(event_sequences(&second.events)[0], 1_000);
        assert_eq!(second.omitted.before, 1_000);
        assert_eq!(second.omitted.after, 0);
        assert!(!second.pagination.has_more);
    }

    #[test]
    fn search_uses_stable_pool_and_binds_repeated_terms_and_filters() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        for index in 0..5_u128 {
            store
                .insert_record(&HistoryRecord {
                    id: Uuid::from_u128(100 + index),
                    title: format!("record-{index}"),
                    body: "needle repeated search text".to_owned(),
                    tags: vec![],
                    kind: "test".to_owned(),
                    workspace: None,
                    created_at: fixed_time(),
                    updated_at: fixed_time(),
                })
                .unwrap();
        }
        drop(store);
        let store = Store::open_read_only(&path).unwrap();
        let mut options = PacketOptions {
            limit: 2,
            ..PacketOptions::default()
        };
        options.filters.primary_only = true;
        let terms = vec!["needle".to_owned(), "needle".to_owned()];
        let page1 = QueryService::new(&store)
            .search(
                "needle",
                &terms,
                options.clone(),
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(page1.pool_total, 5);
        assert_eq!(page1.results.len(), 2);
        let token = page1.pagination.continuation.clone().unwrap();
        let mut forged_search: Token = serde_json::from_slice(&unhex(&token).unwrap()).unwrap();
        forged_search.offset = 0;
        let zero_search = encode_token(&forged_search).unwrap();
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                options.clone(),
                Some(&zero_search),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::InvalidContinuation(_))
        ));
        forged_search.offset = page1.pool_total as u64;
        let end_search = encode_token(&forged_search).unwrap();
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                options.clone(),
                Some(&end_search),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::InvalidContinuation(_))
        ));
        let page2 = QueryService::new(&store)
            .search(
                "needle",
                &terms,
                options.clone(),
                Some(&token),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        let page3 = QueryService::new(&store)
            .search(
                "needle",
                &terms,
                options.clone(),
                page2.pagination.continuation.as_deref(),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(page2.results.len(), 2);
        assert_eq!(page3.results.len(), 1);
        assert!(!page3.pagination.has_more);
        assert!(page3.pagination.continuation.is_none());
        assert!(!page3.source_truncation.truncated);
        assert!(page3.source_truncation.omitted_results_exact);
        let ids = page1
            .results
            .iter()
            .chain(&page2.results)
            .chain(&page3.results)
            .map(|result| match result {
                SearchResultProjectionV1::Compact(value) => value.item_id,
                SearchResultProjectionV1::Full(value) => value.item_id,
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 5);

        let zero = QueryService::new(&store).search(
            "needle",
            &terms,
            PacketOptions {
                limit: 0,
                ..options.clone()
            },
            None,
            FieldSet::Compact,
            bytes(),
        );
        assert!(matches!(zero, Err(QueryError::InvalidPageSize)));

        let changed_terms = vec!["needle".to_owned()];
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &changed_terms,
                options.clone(),
                Some(&token),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        options.filters.primary_only = false;
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                options.clone(),
                Some(&token),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));

        let mut changed = options.clone();
        changed.filters.primary_only = true;
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                changed.clone(),
                Some(&token),
                FieldSet::Full,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        let mut changed_snippet_chars = changed.clone();
        changed_snippet_chars.snippet_chars += 1;
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                changed_snippet_chars,
                Some(&token),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                changed,
                Some(&token),
                FieldSet::Compact,
                BytePolicy {
                    per_item_bytes: MAX_ITEM_BYTES - 1,
                    page_bytes: MAX_PAGE_BYTES,
                },
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));

        let empty = QueryService::new(&store)
            .search(
                "definitely-absent-search-term",
                &[],
                PacketOptions {
                    limit: 2,
                    ..PacketOptions::default()
                },
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert!(empty.results.is_empty());
        assert_eq!(empty.pool_total, 0);
        assert!(!empty.pagination.has_more);
        assert!(empty.pagination.continuation.is_none());

        let too_many_terms = vec![String::new(); ctx_history_search::MAX_QUERY_CLAUSES + 1];
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &too_many_terms,
                PacketOptions::default(),
                None,
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::Search(
                ctx_history_search::SearchError::InvalidRequest(_)
            ))
        ));

        drop(store);
        let writable = Store::open(&path).unwrap();
        writable
            .insert_record(&HistoryRecord {
                id: Uuid::from_u128(999),
                title: "new record".to_owned(),
                body: "needle repeated search text".to_owned(),
                tags: vec![],
                kind: "test".to_owned(),
                workspace: None,
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        let mut stale_options = PacketOptions {
            limit: 2,
            ..PacketOptions::default()
        };
        stale_options.filters.primary_only = true;
        assert!(matches!(
            QueryService::new(&store).search(
                "needle",
                &terms,
                stale_options,
                Some(&token),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::StaleContinuation)
        ));
    }

    #[test]
    fn compact_fallback_uses_narrow_hydration_and_matches_full_reference() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        let record = HistoryRecord {
            id: Uuid::from_u128(26_400),
            title: "compact hydration oracle".into(),
            body: "fallbackneedle body".into(),
            tags: vec!["fallbackneedle-tag".into()],
            kind: "test".into(),
            workspace: Some("/repo/compact".into()),
            created_at: fixed_time(),
            updated_at: fixed_time(),
        };
        store.insert_record(&record).unwrap();
        let source_id = Uuid::from_u128(26_401);
        store
            .upsert_capture_source(&CaptureSource {
                id: source_id,
                descriptor: CaptureSourceDescriptor {
                    kind: CaptureSourceKind::ProviderImport,
                    provider: CaptureProvider::Codex,
                    machine_id: "test-machine".into(),
                    process_id: None,
                    cwd: Some("/repo/compact".into()),
                    raw_source_path: None,
                    external_session_id: Some("compact-session".into()),
                },
                started_at: fixed_time(),
                ended_at: None,
                sync: SyncMetadata {
                    metadata: json!({"source_metadata":{"ctx_history_plugin":{"plugin_name":"plugin","plugin_source_id":"fixture","history_source":"plugin/fixture"}},"cursor":{"after":{"cursor":"cursor-1"}}}),
                    ..sync()
                },
            })
            .unwrap();
        let mut fixture_session = session(Uuid::from_u128(26_402));
        fixture_session.history_record_id = Some(record.id);
        fixture_session.capture_source_id = Some(source_id);
        store.upsert_session(&fixture_session).unwrap();
        let mut fixture_event = event(
            fixture_session.id,
            1,
            EventType::Message,
            Some(EventRole::User),
            "fallbackneedle event text",
        );
        fixture_event.id = Uuid::from_u128(26_403);
        fixture_event.history_record_id = Some(record.id);
        fixture_event.capture_source_id = Some(source_id);
        store.upsert_event(&fixture_event).unwrap();

        let options = PacketOptions {
            limit: 1,
            filters: SearchFilters {
                history_source: Some("plugin/fixture".into()),
                repo: Some("compact".into()),
                ..SearchFilters::default()
            },
            ..PacketOptions::default()
        };
        store.reset_search_hydration_loader_executions();
        let narrow = QueryService::new(&store)
            .search(
                "fallbackneedle",
                &[],
                options.clone(),
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(store.search_hydration_loader_executions(), [0, 8]);
        store.reset_search_hydration_loader_executions();
        let mut reference = with_full_search_hydration_reference(|| {
            QueryService::new(&store).search(
                "fallbackneedle",
                &[],
                options.clone(),
                None,
                FieldSet::Compact,
                bytes(),
            )
        })
        .unwrap();
        assert_eq!(store.search_hydration_loader_executions(), [8, 0]);
        reference.generated_at = narrow.generated_at;
        assert_eq!(narrow, reference);
        let compact_json = serde_json::to_value(&narrow).unwrap();
        assert!(compact_json["results"][0].get("citations").is_none());
        assert!(compact_json["results"][0].get("provider").is_none());

        store.reset_search_hydration_loader_executions();
        let full = QueryService::new(&store)
            .search(
                "fallbackneedle",
                &[],
                options,
                None,
                FieldSet::Full,
                bytes(),
            )
            .unwrap();
        assert_eq!(store.search_hydration_loader_executions(), [8, 0]);
        assert_eq!(full.results.len(), narrow.results.len());
    }

    #[test]
    fn search_page_budget_admits_whole_selected_projections_and_progresses() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let store = Store::open(&path).unwrap();
        for index in 0..3_u128 {
            store
                .insert_record(&HistoryRecord {
                    id: Uuid::from_u128(500 + index),
                    title: format!("budget-{index}"),
                    body: "budget needle payload".to_owned(),
                    tags: vec![],
                    kind: "test".to_owned(),
                    workspace: None,
                    created_at: fixed_time(),
                    updated_at: fixed_time(),
                })
                .unwrap();
        }
        drop(store);
        let store = Store::open_read_only(&path).unwrap();
        let options = PacketOptions {
            limit: 2,
            ..PacketOptions::default()
        };
        let roomy = QueryService::new(&store)
            .search(
                "budget needle",
                &[],
                options.clone(),
                None,
                FieldSet::Full,
                bytes(),
            )
            .unwrap();
        let first_bytes = serde_json::to_vec(&roomy.results[0]).unwrap().len();
        let exact = QueryService::new(&store)
            .search(
                "budget needle",
                &[],
                options.clone(),
                None,
                FieldSet::Full,
                BytePolicy {
                    per_item_bytes: MAX_ITEM_BYTES,
                    page_bytes: first_bytes,
                },
            )
            .unwrap();
        assert_eq!(exact.results.len(), 1);
        assert_eq!(exact.bytes.item_json_bytes, first_bytes);
        assert!(exact.pagination.has_more);
        assert_eq!(exact.pagination.offset, 0);
        assert_eq!(exact.pagination.returned_items, 1);
        assert!(exact.pagination.continuation.is_some());

        let too_small = QueryService::new(&store).search(
            "budget needle",
            &[],
            options,
            None,
            FieldSet::Full,
            BytePolicy {
                per_item_bytes: MAX_ITEM_BYTES,
                page_bytes: first_bytes - 1,
            },
        );
        assert!(matches!(
            too_small,
            Err(QueryError::ItemExceedsPageBudget { .. })
        ));
    }

    #[test]
    fn live_search_reuses_one_bounded_candidate_pool_and_evicts_lru() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let writable = Store::open(&path).unwrap();
        for index in 0..10_u128 {
            writable
                .insert_record(&HistoryRecord {
                    id: Uuid::from_u128(index + 1),
                    title: format!("candidate-{index}"),
                    body: "alpha beta gamma delta epsilon".to_owned(),
                    tags: vec![],
                    kind: "cache-test".to_owned(),
                    workspace: None,
                    created_at: fixed_time(),
                    updated_at: fixed_time(),
                })
                .unwrap();
        }
        drop(writable);

        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);
        let options = PacketOptions {
            limit: 1,
            ..PacketOptions::default()
        };
        let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let mut tokens = Vec::new();
        for term in terms {
            let page = service
                .search(term, &[], options.clone(), None, FieldSet::Compact, bytes())
                .unwrap();
            tokens.push((term, page.pagination.continuation.unwrap()));
        }
        assert_eq!(
            service.search_continuation_cache_len(),
            SEARCH_CONTINUATION_CACHE_CAPACITY
        );

        let before = store.record_search_page_executions();
        assert!(matches!(
            service.search(
                tokens[4].0,
                &[],
                options.clone(),
                Some("not-hex"),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::InvalidContinuation(_))
        ));
        assert!(matches!(
            service.search(
                tokens[4].0,
                &[],
                options.clone(),
                Some(&tokens[4].1),
                FieldSet::Full,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        let mut filtered = options.clone();
        filtered.filters.history_source = Some("other-source".to_owned());
        assert!(matches!(
            service.search(
                tokens[4].0,
                &[],
                filtered,
                Some(&tokens[4].1),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        assert_eq!(store.record_search_page_executions(), before);

        Arc::make_mut(
            &mut service
                .search_continuations
                .borrow_mut()
                .back_mut()
                .unwrap()
                .packet,
        )
        .generated_at = DateTime::<Utc>::UNIX_EPOCH;
        let cached = service
            .search(
                tokens[4].0,
                &[],
                options.clone(),
                Some(&tokens[4].1),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_ne!(cached.generated_at, DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(store.record_search_page_executions(), before);

        let mut reference = QueryService::new(&store)
            .search(
                tokens[4].0,
                &[],
                options.clone(),
                Some(&tokens[4].1),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        // Candidate generation timestamps are intentionally observational;
        // normalize that instant to compare the complete public page packet.
        reference.generated_at = cached.generated_at;
        assert_eq!(cached, reference);

        let after_reference = store.record_search_page_executions();
        service
            .search(
                tokens[0].0,
                &[],
                options,
                Some(&tokens[0].1),
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(store.record_search_page_executions(), after_reference + 1);
        assert_eq!(
            service.search_continuation_cache_len(),
            SEARCH_CONTINUATION_CACHE_CAPACITY
        );

        let now = Instant::now();
        let packet = Arc::clone(&service.search_continuations.borrow()[0].packet);
        for entry in service.search_continuations.borrow_mut().iter_mut() {
            entry.inserted_at = now - SEARCH_CONTINUATION_CACHE_LIFETIME;
        }
        assert!(service
            .cached_search_packet_at(tokens[4].0, "unused", now)
            .is_none());
        assert_eq!(service.search_continuation_cache_len(), 0);
        for index in 0..SEARCH_CONTINUATION_CACHE_CAPACITY {
            service.cache_search_packet_at(
                format!("expired-{index}"),
                "expired-snapshot".to_owned(),
                Arc::clone(&packet),
                now - SEARCH_CONTINUATION_CACHE_LIFETIME,
            );
        }
        service.cache_search_packet_at(
            "fresh-request".to_owned(),
            "fresh-snapshot".to_owned(),
            packet,
            now,
        );
        assert_eq!(service.search_continuation_cache_len(), 1);
    }

    #[test]
    fn live_search_rejects_cached_continuation_after_store_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let writable = Store::open(&path).unwrap();
        for index in 0..3_u128 {
            writable
                .insert_record(&HistoryRecord {
                    id: Uuid::from_u128(index + 1),
                    title: format!("mutation-{index}"),
                    body: "snapshot needle".to_owned(),
                    tags: vec![],
                    kind: "cache-test".to_owned(),
                    workspace: None,
                    created_at: fixed_time(),
                    updated_at: fixed_time(),
                })
                .unwrap();
        }
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);
        let options = PacketOptions {
            limit: 1,
            ..PacketOptions::default()
        };
        let first = service
            .search(
                "needle",
                &[],
                options.clone(),
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        let token = first.pagination.continuation.unwrap();

        let writer = Store::open(&path).unwrap();
        writer
            .insert_record(&HistoryRecord {
                id: Uuid::from_u128(99),
                title: "new mutation".to_owned(),
                body: "snapshot needle".to_owned(),
                tags: vec![],
                kind: "cache-test".to_owned(),
                workspace: None,
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        drop(writer);

        let before = store.record_search_page_executions();
        assert!(matches!(
            service.search(
                "needle",
                &[],
                options,
                Some(&token),
                FieldSet::Compact,
                bytes(),
            ),
            Err(QueryError::StaleContinuation)
        ));
        assert_eq!(store.record_search_page_executions(), before);
    }

    #[test]
    #[ignore = "issue #270 release benchmark; run in release mode with --ignored --nocapture"]
    fn issue_270_same_process_query_reuses_fixed_candidate_pool() {
        const RECORD_COUNT: usize = 200_000;
        const PAGE_SIZE: usize = 100;
        const WARMUP_PAIRS: usize = 1;
        const SAMPLES: usize = 5;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let writable = Store::open(&path).unwrap();
        let created_at = fixed_time();
        for start in (0..RECORD_COUNT).step_by(5_000) {
            let records = (start..(start + 5_000).min(RECORD_COUNT))
                .map(|index| HistoryRecord {
                    id: Uuid::from_u128(index as u128 + 1),
                    title: "issue-270 continuation fixture".to_owned(),
                    body: "needle continuation replay fixture".to_owned(),
                    tags: vec![],
                    kind: "issue-270".to_owned(),
                    workspace: None,
                    created_at,
                    updated_at: created_at,
                })
                .collect::<Vec<_>>();
            writable.upsert_records(&records).unwrap();
        }
        drop(writable);

        let store = Store::open_read_only(&path).unwrap();
        let query = QueryService::new(&store);
        let options = PacketOptions {
            limit: PAGE_SIZE,
            ..PacketOptions::default()
        };

        let run_pair = |query: &QueryService<'_>, options: &PacketOptions, forced_miss: bool| {
            let started = Instant::now();
            let page1 = query
                .search(
                    "needle",
                    &[],
                    options.clone(),
                    None,
                    FieldSet::Compact,
                    bytes(),
                )
                .unwrap();
            let page1_elapsed = started.elapsed();
            let token = page1.pagination.continuation.clone().unwrap();
            let page2_started = Instant::now();
            let control;
            let page2_query = if forced_miss {
                control = QueryService::new(&store);
                &control
            } else {
                query
            };
            let page2 = page2_query
                .search(
                    "needle",
                    &[],
                    options.clone(),
                    Some(&token),
                    FieldSet::Compact,
                    bytes(),
                )
                .unwrap();
            let page2_elapsed = page2_started.elapsed();
            (page1, page2, page1_elapsed, page2_elapsed)
        };

        for _ in 0..WARMUP_PAIRS {
            let _ = run_pair(&query, &options, false);
            let _ = run_pair(&query, &options, true);
        }
        let baseline_searches = store.record_search_page_executions();
        let mut samples = Vec::with_capacity(SAMPLES);
        for sample in 1..=SAMPLES {
            let (cached, control) = if sample % 2 == 0 {
                (
                    run_pair(&query, &options, false),
                    run_pair(&query, &options, true),
                )
            } else {
                let control = run_pair(&query, &options, true);
                let cached = run_pair(&query, &options, false);
                (cached, control)
            };
            let (page1, page2, cached_page1, cached_page2) = cached;
            let (control_page1_result, control_page2_result, control_page1, control_page2) =
                control;
            let page1_ids = page1
                .results
                .iter()
                .map(search_projection_id)
                .collect::<Vec<_>>();
            let page2_ids = page2
                .results
                .iter()
                .map(search_projection_id)
                .collect::<Vec<_>>();
            let expected_page1 = (1..=PAGE_SIZE)
                .map(|id| Uuid::from_u128(id as u128))
                .collect::<Vec<_>>();
            let expected_page2 = ((PAGE_SIZE + 1)..=(PAGE_SIZE * 2))
                .map(|id| Uuid::from_u128(id as u128))
                .collect::<Vec<_>>();
            assert_eq!(page1_ids, expected_page1, "page 1 ordering changed");
            assert_eq!(page2_ids, expected_page2, "page 2 ordering changed");
            assert_eq!(page1.results, control_page1_result.results);
            assert_eq!(page2.results, control_page2_result.results);
            assert_eq!(page1.pagination.offset, 0);
            assert_eq!(page2.pagination.offset, PAGE_SIZE);
            assert_eq!(page1.pool_total, 200);
            assert_eq!(page2.pool_total, 200);

            let after_searches = store.record_search_page_executions();
            let reruns = after_searches.saturating_sub(baseline_searches);
            assert_eq!(reruns, (sample * 3) as u64);
            samples.push((
                cached_page1,
                cached_page2,
                control_page1,
                control_page2,
                reruns,
            ));
        }

        let cached_page1_mean = samples
            .iter()
            .map(|sample| sample.0.as_nanos())
            .sum::<u128>()
            / SAMPLES as u128;
        let cached_page2_mean = samples
            .iter()
            .map(|sample| sample.1.as_nanos())
            .sum::<u128>()
            / SAMPLES as u128;
        let control_page1_mean = samples
            .iter()
            .map(|sample| sample.2.as_nanos())
            .sum::<u128>()
            / SAMPLES as u128;
        let control_page2_mean = samples
            .iter()
            .map(|sample| sample.3.as_nanos())
            .sum::<u128>()
            / SAMPLES as u128;
        let median = |field: usize| {
            let mut values = samples
                .iter()
                .map(|sample| match field {
                    0 => sample.0.as_nanos(),
                    1 => sample.1.as_nanos(),
                    2 => sample.2.as_nanos(),
                    _ => sample.3.as_nanos(),
                })
                .collect::<Vec<_>>();
            values.sort_unstable();
            values[SAMPLES / 2]
        };
        let cached_page1_median = median(0);
        let cached_page2_median = median(1);
        let control_page1_median = median(2);
        let control_page2_median = median(3);
        assert!(
            cached_page2_median * 100 < control_page2_median * 80,
            "cached page 2 did not improve by more than 20%"
        );
        assert!(
            cached_page1_median * 100 <= control_page1_median * 105,
            "cached-path page 1 regressed by more than 5%"
        );

        println!(
            "issue #270 fixture records={RECORD_COUNT} page_size={PAGE_SIZE} \
             same_store_handle=true same_query_service=true"
        );
        for (index, (cached_page1, cached_page2, control_page1, control_page2, reruns)) in
            samples.into_iter().enumerate()
        {
            println!(
                "sample={} cached_page1_us={} cached_page2_us={} \
                 forced_miss_page1_us={} forced_miss_page2_us={} \
                  record_search_statements_since_warmup={reruns}",
                index + 1,
                cached_page1.as_micros(),
                cached_page2.as_micros(),
                control_page1.as_micros(),
                control_page2.as_micros(),
            );
        }
        println!(
            "means cached_page1_us={} cached_page2_us={} forced_miss_page1_us={} \
             forced_miss_page2_us={} medians_us={}/{}/{}/{} page2_improvement_percent={:.2} \
             page1_regression_percent={:.2}",
            cached_page1_mean / 1_000,
            cached_page2_mean / 1_000,
            control_page1_mean / 1_000,
            control_page2_mean / 1_000,
            cached_page1_median / 1_000,
            cached_page2_median / 1_000,
            control_page1_median / 1_000,
            control_page2_median / 1_000,
            100.0 * (1.0 - cached_page2_median as f64 / control_page2_median as f64),
            100.0 * (cached_page1_median as f64 / control_page1_median as f64 - 1.0),
        );
    }

    #[test]
    fn evidence_session_selector_replays_without_ambient_state() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (session, _) = transcript_fixture(&path);
        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);
        let request = EvidenceSelectorRequest {
            selector: EvidenceSelector::SessionPage {
                ctx_session_id: session.id,
                mode: TranscriptMode::Log,
            },
            limit: 2,
            fields: FieldSet::Compact,
            format: EvidenceFormat::Markdown,
            ..EvidenceSelectorRequest::default()
        };
        let first = service.select_evidence(request).unwrap();
        assert_eq!(first.domain, "session_page");
        assert_eq!(first.selected_total, Some(9));
        assert_eq!(first.omitted.before, 0);
        assert_eq!(first.omitted.after, 7);
        let continuation = first.continuation.unwrap();
        assert_eq!(continuation.next_arguments.format, EvidenceFormat::Markdown);
        assert!(continuation.next_arguments.include_current_session);
        assert_eq!(
            continuation.next_arguments.continuation.as_deref(),
            Some(continuation.token.as_str())
        );
        let second = QueryService::new(&store)
            .select_evidence(continuation.next_arguments)
            .unwrap();
        assert_eq!(second.domain, "session_page");
        assert_eq!(second.omitted.before, 2);
        assert_eq!(second.items.len(), 2);
        assert!(matches!(second.items[0], EvidenceItemV1::Event { .. }));
    }

    #[test]
    fn evidence_event_ids_are_sorted_bounded_and_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (_session, events) = transcript_fixture(&path);
        let deleted_id = events[6].id;
        let mut deleted = events[6].clone();
        deleted.sync.deleted_at = Some(fixed_time());
        let writable = Store::open(&path).unwrap();
        writable.upsert_event(&deleted).unwrap();
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        let service = QueryService::new(&store);
        let live_log = service
            .session_events(
                session(Uuid::from_u128(42)),
                TranscriptMode::Log,
                20,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(live_log.selected_total, 8);
        let live_lite = service
            .session_events(
                session(Uuid::from_u128(42)),
                TranscriptMode::Lite,
                20,
                None,
                FieldSet::Compact,
                bytes(),
            )
            .unwrap();
        assert_eq!(event_sequences(&live_lite.events), vec![1, 7]);
        let request = EvidenceSelectorRequest {
            selector: EvidenceSelector::EventIds {
                event_ids: vec![events[2].id, events[0].id],
            },
            limit: 1,
            fields: FieldSet::Compact,
            ..EvidenceSelectorRequest::default()
        };
        let first = service.select_evidence(request).unwrap();
        assert_eq!(first.selected_total, Some(2));
        assert_eq!(first.items.len(), 1);
        assert!(first.pagination.has_more);
        let next = first.continuation.unwrap().next_arguments;
        let second = QueryService::new(&store).select_evidence(next).unwrap();
        assert_eq!(second.items.len(), 1);
        assert_eq!(second.omitted.before, 1);

        let missing = EvidenceSelectorRequest {
            selector: EvidenceSelector::EventIds {
                event_ids: vec![Uuid::from_u128(0xdead)],
            },
            ..EvidenceSelectorRequest::default()
        };
        assert!(matches!(
            service.select_evidence(missing),
            Err(QueryError::MissingEvidenceTarget { .. })
        ));
        let deleted = EvidenceSelectorRequest {
            selector: EvidenceSelector::EventIds {
                event_ids: vec![deleted_id],
            },
            ..EvidenceSelectorRequest::default()
        };
        assert!(matches!(
            service.select_evidence(deleted),
            Err(QueryError::DeletedEvidenceTarget { id }) if id == deleted_id
        ));
        let duplicate = EvidenceSelectorRequest {
            selector: EvidenceSelector::EventIds {
                event_ids: vec![events[0].id, events[0].id],
            },
            ..EvidenceSelectorRequest::default()
        };
        assert!(matches!(
            service.select_evidence(duplicate),
            Err(QueryError::InvalidEvidenceSelector(_))
        ));
    }

    #[test]
    fn evidence_event_ids_carry_distinct_event_and_session_sources() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (fixture_session, events) = transcript_fixture(&path);
        let event_source_id = Uuid::from_u128(60_001);
        let session_source_id = Uuid::from_u128(60_002);
        let later_event_source_id = Uuid::from_u128(60_003);
        let writable = Store::open(&path).unwrap();
        for (source_id, external_session_id) in [
            (event_source_id, "event-source"),
            (session_source_id, "session-source"),
            (later_event_source_id, "later-event-source"),
        ] {
            writable
                .upsert_capture_source(&CaptureSource {
                    id: source_id,
                    descriptor: CaptureSourceDescriptor {
                        kind: CaptureSourceKind::ProviderImport,
                        provider: CaptureProvider::Codex,
                        machine_id: "test-machine".into(),
                        process_id: None,
                        cwd: Some("/tmp/evidence".into()),
                        raw_source_path: Some("/private/evidence/source.jsonl".into()),
                        external_session_id: Some(external_session_id.into()),
                    },
                    started_at: fixed_time(),
                    ended_at: None,
                    sync: SyncMetadata {
                        metadata: json!({"cursor": "private-cursor"}),
                        ..sync()
                    },
                })
                .unwrap();
        }
        let mut sourced_session = fixture_session.clone();
        let search_record_id = Uuid::from_u128(60_004);
        writable
            .insert_record(&HistoryRecord {
                id: search_record_id,
                title: "provenance search".into(),
                body: "sys sys2".into(),
                tags: Vec::new(),
                kind: "test".into(),
                workspace: None,
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        sourced_session.history_record_id = Some(search_record_id);
        sourced_session.capture_source_id = Some(session_source_id);
        writable.upsert_session(&sourced_session).unwrap();
        let mut sourced_event = events[0].clone();
        sourced_event.history_record_id = Some(search_record_id);
        sourced_event.capture_source_id = Some(event_source_id);
        writable.upsert_event(&sourced_event).unwrap();
        let mut later_event = events[1].clone();
        later_event.capture_source_id = Some(later_event_source_id);
        writable.upsert_event(&later_event).unwrap();
        let mut later_search_event = events[5].clone();
        later_search_event.history_record_id = Some(search_record_id);
        later_search_event.capture_source_id = Some(later_event_source_id);
        writable.upsert_event(&later_search_event).unwrap();
        let withheld_record_id = Uuid::from_u128(60_005);
        writable
            .insert_record(&HistoryRecord {
                id: withheld_record_id,
                title: "withheld provenance".into(),
                body: "withheldneedle".into(),
                tags: Vec::new(),
                kind: "test".into(),
                workspace: None,
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        let mut withheld_event = event(
            fixture_session.id,
            11,
            EventType::Message,
            Some(EventRole::Assistant),
            "withheldneedle",
        );
        withheld_event.history_record_id = Some(withheld_record_id);
        withheld_event.sync.visibility = Visibility::Withheld;
        writable.upsert_event(&withheld_event).unwrap();
        writable.refresh_search_index().unwrap();
        drop(writable);

        let store = Store::open_read_only(&path).unwrap();
        let probe = QueryService::new(&store)
            .select_evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::EventIds {
                    event_ids: vec![sourced_event.id, later_event.id],
                },
                limit: 1,
                fields: FieldSet::Full,
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        let page = QueryService::new(&store)
            .select_evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::EventIds {
                    event_ids: vec![sourced_event.id, later_event.id],
                },
                limit: 2,
                fields: FieldSet::Full,
                byte_policy: BytePolicy {
                    page_bytes: probe.bytes.item_json_bytes + 16,
                    ..BytePolicy::default()
                },
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        assert!(page.bytes.page_budget_exhausted);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.event_source_refs.len(), 1);
        assert_eq!(
            page.event_source_refs[0].event_capture_source_id,
            Some(event_source_id)
        );
        assert_eq!(
            page.event_source_refs[0].session_capture_source_id,
            Some(session_source_id)
        );
        assert_eq!(page.source_lookup.len(), 2);
        assert_eq!(
            page.source_lookup
                .iter()
                .map(|source| source.capture_source_id)
                .collect::<Vec<_>>(),
            vec![event_source_id, session_source_id]
        );
        assert!(page
            .source_lookup
            .iter()
            .all(|source| source.source.is_some()));
        assert!(!page
            .source_lookup
            .iter()
            .any(|source| source.capture_source_id == later_event_source_id));
        let next = page.continuation.clone().unwrap().next_arguments;
        let replay = QueryService::new(&store)
            .select_evidence(next.clone())
            .unwrap();
        let replay_again = QueryService::new(&store).select_evidence(next).unwrap();
        assert_eq!(replay.event_source_refs, replay_again.event_source_refs);
        assert_eq!(replay.source_lookup, replay_again.source_lookup);
        assert_eq!(replay.items.len(), 1);
        assert_eq!(replay.event_source_refs[0].ctx_event_id, later_event.id);
        assert!(replay
            .source_lookup
            .iter()
            .any(|source| source.capture_source_id == later_event_source_id));

        let session_probe = QueryService::new(&store)
            .select_evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::SessionPage {
                    ctx_session_id: sourced_session.id,
                    mode: TranscriptMode::Log,
                },
                limit: 1,
                fields: FieldSet::Full,
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        let session_page = QueryService::new(&store)
            .select_evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::SessionPage {
                    ctx_session_id: sourced_session.id,
                    mode: TranscriptMode::Log,
                },
                limit: 2,
                fields: FieldSet::Full,
                byte_policy: BytePolicy {
                    page_bytes: session_probe.bytes.item_json_bytes + 16,
                    ..BytePolicy::default()
                },
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        assert_eq!(session_page.items.len(), 1);
        assert!(session_page.bytes.page_budget_exhausted);
        assert_eq!(session_page.event_source_refs.len(), 1);
        assert_eq!(session_page.source_lookup.len(), 2);
        assert!(!session_page
            .source_lookup
            .iter()
            .any(|source| source.capture_source_id == later_event_source_id));
        let session_next = session_page.continuation.clone().unwrap().next_arguments;
        let session_replay = QueryService::new(&store)
            .select_evidence(session_next.clone())
            .unwrap();
        let session_replay_again = QueryService::new(&store)
            .select_evidence(session_next)
            .unwrap();
        assert_eq!(
            session_replay.event_source_refs,
            session_replay_again.event_source_refs
        );
        assert!(session_replay
            .source_lookup
            .iter()
            .any(|source| source.capture_source_id == later_event_source_id));

        let search_request = |limit, page_bytes| EvidenceSelectorRequest {
            selector: EvidenceSelector::SearchPage {
                query: "sys".to_owned(),
                terms: Vec::new(),
                options: Box::new(PacketOptions {
                    result_mode: SearchResultMode::Events,
                    ..PacketOptions::default()
                }),
            },
            limit,
            fields: FieldSet::Full,
            byte_policy: BytePolicy {
                page_bytes,
                ..BytePolicy::default()
            },
            ..EvidenceSelectorRequest::default()
        };
        let search_probe = QueryService::new(&store)
            .select_evidence(search_request(1, MAX_PAGE_BYTES))
            .unwrap();
        let search_page = QueryService::new(&store)
            .select_evidence(search_request(2, search_probe.bytes.item_json_bytes + 16))
            .unwrap();
        assert_eq!(search_page.items.len(), 1);
        assert_eq!(search_page.search_source_refs.len(), 1);
        assert!(!search_page.search_source_refs[0].events.is_empty());
        assert_eq!(
            search_page.search_source_refs[0].events[0].event_capture_source_id,
            Some(event_source_id)
        );
        assert_eq!(search_page.source_lookup.len(), 2);

        let evidence_source = |page: &EvidenceSelectionPageV1| {
            page.source_lookup
                .iter()
                .find(|lookup| lookup.capture_source_id == event_source_id)
                .and_then(|lookup| lookup.source.clone())
                .unwrap()
        };
        let explicit_source = evidence_source(&page);
        let session_source = evidence_source(&session_page);
        let search_source = evidence_source(&search_page);
        assert_eq!(explicit_source, session_source);
        assert_eq!(explicit_source, search_source);
        for source in [explicit_source, session_source, search_source] {
            assert!(source.cwd.is_none());
            assert!(source.path.is_none());
            assert!(source.exists.is_none());
            assert!(source.source_cursor.is_none());
            assert!(source.cursor.is_none());
            let wire = serde_json::to_value(source).unwrap();
            for sensitive in ["cwd", "path", "exists", "source_cursor", "cursor"] {
                assert!(wire.get(sensitive).is_none(), "leaked {sensitive}: {wire}");
            }
        }
        let normalized = QueryService::new(&store)
            .evidence(search_request(1, MAX_PAGE_BYTES))
            .unwrap();
        let normalized_json = serde_json::to_string(&normalized).unwrap();
        assert!(!normalized_json.contains("private-cursor"));
        let normalized_results = normalized
            .records
            .iter()
            .filter_map(|record| match record {
                EvidenceRecordV1::SearchResult(result) => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(normalized_results.len(), 1);
        let positive = normalized_results[0];
        assert!(positive
            .title
            .as_deref()
            .is_some_and(|title| !title.is_empty()));
        assert!(positive
            .content
            .text
            .as_deref()
            .is_some_and(|snippet| !snippet.is_empty()));
        assert_eq!(positive.content.content_state, ContentStateV1::Available);
        assert!(positive.content.suppression_reason.is_none());
        assert!(
            !positive.citations.is_empty(),
            "primary event citation was lost"
        );
        assert!(positive.citations.iter().any(|citation| {
            citation.citation_type == EvidenceCitationTypeV1::Event
                && citation.target_id == sourced_event.id
        }));
        for result in normalized.records.iter().filter_map(|record| match record {
            EvidenceRecordV1::SearchResult(result) => Some(result),
            _ => None,
        }) {
            assert!(
                !result.citations.is_empty(),
                "citation assertion must be non-vacuous"
            );
            assert!(result
                .citations
                .iter()
                .all(|citation| ["event evidence", "session evidence"]
                    .contains(&citation.label.as_str())));
            assert!(result.citations.windows(2).all(|pair| {
                (pair[0].citation_type, pair[0].target_id, &pair[0].time)
                    <= (pair[1].citation_type, pair[1].target_id, &pair[1].time)
            }));
        }

        let withheld = QueryService::new(&store)
            .evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::SearchPage {
                    query: "withheldneedle".into(),
                    terms: vec![],
                    options: Box::default(),
                },
                limit: 1,
                fields: FieldSet::Full,
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        let EvidenceRecordV1::SearchResult(withheld_result) = &withheld.records[0] else {
            panic!("expected withheld search result")
        };
        assert!(withheld_result.title.is_none());
        assert!(withheld_result.content.text.is_none());
        assert_eq!(
            withheld_result.content.content_state,
            ContentStateV1::Withheld
        );
        assert_eq!(
            withheld_result.content.suppression_reason,
            Some(SuppressionReasonV1::WithheldVisibility)
        );
    }

    #[test]
    fn normalized_evidence_real_domains_are_closed_deterministic_and_nonleaky() {
        const SENTINEL: &str = "HOSTILE_LEAK_SENTINEL";
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (fixture_session, mut events) = transcript_fixture(&path);
        let writable = Store::open(&path).unwrap();
        let mut raw = event(
            fixture_session.id,
            9,
            EventType::Message,
            Some(EventRole::Assistant),
            SENTINEL,
        );
        raw.redaction_state = RedactionState::Raw;
        writable.upsert_event(&raw).unwrap();
        let marker = event(
            fixture_session.id,
            10,
            EventType::Message,
            Some(EventRole::Assistant),
            "[content withheld]",
        );
        writable.upsert_event(&marker).unwrap();
        events.extend([raw.clone(), marker.clone()]);
        drop(writable);

        for fields in [FieldSet::Full, FieldSet::Compact] {
            let store = Store::open_read_only(&path).unwrap();
            let service = QueryService::new(&store);
            let requests = [
                EvidenceSelectorRequest {
                    selector: EvidenceSelector::SessionPage {
                        ctx_session_id: fixture_session.id,
                        mode: TranscriptMode::Full,
                    },
                    limit: 20,
                    fields,
                    ..EvidenceSelectorRequest::default()
                },
                EvidenceSelectorRequest {
                    selector: EvidenceSelector::EventIds {
                        event_ids: vec![raw.id, marker.id],
                    },
                    limit: 20,
                    fields,
                    ..EvidenceSelectorRequest::default()
                },
                EvidenceSelectorRequest {
                    selector: EvidenceSelector::SearchPage {
                        query: "a1".into(),
                        terms: vec![],
                        options: Box::default(),
                    },
                    limit: 20,
                    fields,
                    ..EvidenceSelectorRequest::default()
                },
            ];
            for request in requests {
                let first = service.evidence(request.clone()).unwrap();
                let replay = QueryService::new(&store).evidence(request).unwrap();
                let json = serde_json::to_string(&first).unwrap();
                assert_eq!(json, serde_json::to_string(&replay).unwrap());
                assert!(!json.contains(SENTINEL));
                assert!(!json.contains("generated_at"));
                assert!(!json.contains("source_path"));
                assert!(!json.contains("cursor"));
                if !first.records.is_empty() {
                    assert!(json.contains(".000Z"));
                    assert!(first.normalized_item_json_bytes > 0);
                }
            }

            let marker_page = service
                .evidence(EvidenceSelectorRequest {
                    selector: EvidenceSelector::EventIds {
                        event_ids: vec![marker.id],
                    },
                    fields,
                    ..EvidenceSelectorRequest::default()
                })
                .unwrap();
            let EvidenceRecordV1::Event(marker_record) = &marker_page.records[0] else {
                panic!("expected event")
            };
            assert_eq!(
                marker_record.content.content_state,
                ContentStateV1::Available
            );
            assert_eq!(
                marker_record.content.text.as_deref(),
                Some("[content withheld]")
            );
            assert!(marker_record.content.suppression_reason.is_none());
        }
    }

    #[test]
    fn normalized_provenance_distinguishes_live_withheld_missing_and_session_source() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let (fixture_session, events) = transcript_fixture(&path);
        let event_source = Uuid::from_u128(70_001);
        let session_source = Uuid::from_u128(70_002);
        let missing_source = Uuid::from_u128(70_003);
        let writable = Store::open(&path).unwrap();
        let make_source = |id, visibility| CaptureSource {
            id,
            descriptor: CaptureSourceDescriptor {
                kind: CaptureSourceKind::ProviderImport,
                provider: CaptureProvider::Codex,
                machine_id: "HOSTILE_LEAK_SENTINEL".into(),
                process_id: None,
                cwd: Some("/HOSTILE_LEAK_SENTINEL".into()),
                raw_source_path: Some("/HOSTILE_LEAK_SENTINEL/source".into()),
                external_session_id: Some("HOSTILE_LEAK_SENTINEL".into()),
            },
            started_at: fixed_time(),
            ended_at: None,
            sync: SyncMetadata {
                visibility,
                metadata: json!({"source_format":"HOSTILE_LEAK_SENTINEL"}),
                ..sync()
            },
        };
        writable
            .upsert_capture_source(&make_source(event_source, Visibility::Withheld))
            .unwrap();
        writable
            .upsert_capture_source(&make_source(session_source, Visibility::LocalOnly))
            .unwrap();
        writable
            .upsert_capture_source(&make_source(missing_source, Visibility::LocalOnly))
            .unwrap();
        let mut session = fixture_session.clone();
        session.capture_source_id = Some(session_source);
        writable.upsert_session(&session).unwrap();
        let mut withheld_event = events[0].clone();
        withheld_event.capture_source_id = Some(event_source);
        writable.upsert_event(&withheld_event).unwrap();
        let mut missing_event = events[1].clone();
        missing_event.capture_source_id = Some(missing_source);
        writable.upsert_event(&missing_event).unwrap();
        writable
            .orphan_capture_source_for_test(missing_source)
            .unwrap();
        drop(writable);

        let store = Store::open_read_only(&path).unwrap();
        let page = QueryService::new(&store)
            .evidence(EvidenceSelectorRequest {
                selector: EvidenceSelector::EventIds {
                    event_ids: vec![withheld_event.id, missing_event.id],
                },
                limit: 2,
                fields: FieldSet::Full,
                ..EvidenceSelectorRequest::default()
            })
            .unwrap();
        let json = serde_json::to_string(&page).unwrap();
        assert!(!json.contains("HOSTILE_LEAK_SENTINEL"));
        let events = page
            .records
            .iter()
            .filter_map(|record| match record {
                EvidenceRecordV1::Event(event) => Some(event),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            events[0].provenance.as_ref().unwrap().availability,
            SourceAvailabilityV1::Withheld
        );
        assert_eq!(
            events[0].session_provenance.as_ref().unwrap().availability,
            SourceAvailabilityV1::Live
        );
        assert_eq!(
            events[1].provenance.as_ref().unwrap().availability,
            SourceAvailabilityV1::Missing
        );
        for provenance in [
            events[0].provenance.as_ref().unwrap(),
            events[1].provenance.as_ref().unwrap(),
        ] {
            assert!(provenance.provider.is_none());
            assert!(provenance.kind.is_none());
            assert!(provenance.started_at.is_none());
        }
    }

    #[test]
    fn normalized_search_without_primary_event_proof_suppresses_title_and_snippet() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let writable = Store::open(&path).unwrap();
        writable
            .insert_record(&HistoryRecord {
                id: Uuid::from_u128(80_001),
                title: "HOSTILE_LEAK_SENTINEL title".into(),
                body: "prooflessneedle HOSTILE_LEAK_SENTINEL body".into(),
                tags: vec![],
                kind: "note".into(),
                workspace: None,
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        writable.refresh_search_index().unwrap();
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        for fields in [FieldSet::Full, FieldSet::Compact] {
            let page = QueryService::new(&store)
                .evidence(EvidenceSelectorRequest {
                    selector: EvidenceSelector::SearchPage {
                        query: "prooflessneedle".into(),
                        terms: vec![],
                        options: Box::default(),
                    },
                    limit: 10,
                    fields,
                    ..EvidenceSelectorRequest::default()
                })
                .unwrap();
            let json = serde_json::to_string(&page).unwrap();
            assert!(!json.contains("HOSTILE_LEAK_SENTINEL"));
            let EvidenceRecordV1::SearchResult(result) = &page.records[0] else {
                panic!("expected search result")
            };
            assert!(result.title.is_none());
            assert!(result.content.text.is_none());
            assert_eq!(result.content.content_state, ContentStateV1::Withheld);
            assert_eq!(
                result.content.suppression_reason,
                Some(SuppressionReasonV1::MissingProof)
            );
        }
    }

    #[test]
    fn evidence_search_preserves_filters_and_reports_pool_truncation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("work.sqlite");
        let writable = Store::open(&path).unwrap();
        writable
            .insert_record(&HistoryRecord {
                id: Uuid::from_u128(77),
                title: "evidence needle".to_owned(),
                body: "bounded evidence search".to_owned(),
                tags: vec![],
                kind: "test".to_owned(),
                workspace: Some("workspace".to_owned()),
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        writable
            .insert_record(&HistoryRecord {
                id: Uuid::from_u128(78),
                title: "second evidence needle".to_owned(),
                body: "bounded evidence search".to_owned(),
                tags: vec![],
                kind: "test".to_owned(),
                workspace: Some("workspace".to_owned()),
                created_at: fixed_time(),
                updated_at: fixed_time(),
            })
            .unwrap();
        writable.refresh_search_index().unwrap();
        drop(writable);
        let store = Store::open_read_only(&path).unwrap();
        let options = PacketOptions::default();
        let request = EvidenceSelectorRequest {
            selector: EvidenceSelector::SearchPage {
                query: "needle".to_owned(),
                terms: vec!["needle".to_owned(), "needle".to_owned()],
                options: Box::new(options),
            },
            limit: 1,
            fields: FieldSet::Compact,
            ..EvidenceSelectorRequest::default()
        };
        let page = QueryService::new(&store).select_evidence(request).unwrap();
        assert_eq!(page.domain, "search_page");
        assert_eq!(page.retained_pool_total, Some(2));
        assert_eq!(page.corpus_count.as_ref().unwrap().kind, "exact");
        assert!(!page.search_truncation.as_ref().unwrap().truncated);
        let mut tampered = page.continuation.as_ref().unwrap().next_arguments.clone();
        if let EvidenceSelector::SearchPage { options, .. } = &mut tampered.selector {
            options.filters.primary_only = true;
        }
        assert!(matches!(
            QueryService::new(&store).select_evidence(tampered),
            Err(QueryError::ContinuationRequestMismatch)
        ));
        let serialized = serde_json::to_value(page).unwrap();
        assert_eq!(serialized["domain"], "search_page");
        assert!(serialized.to_string().contains("needle"));
    }

    #[test]
    fn evidence_wire_is_strict_and_round_trips_each_domain() {
        let session_id = Uuid::from_u128(501);
        let event_id = Uuid::from_u128(502);
        let search_options = PacketOptions {
            filters: SearchFilters {
                primary_only: true,
                roles: vec![EventRole::User],
                exclude_tool_names: vec!["ctx".to_owned()],
                ..SearchFilters::default()
            },
            ..PacketOptions::default()
        };
        let requests = vec![
            EvidenceSelectorRequest {
                selector: EvidenceSelector::SessionPage {
                    ctx_session_id: session_id,
                    mode: TranscriptMode::Full,
                },
                limit: 3,
                ..EvidenceSelectorRequest::default()
            },
            EvidenceSelectorRequest {
                selector: EvidenceSelector::SearchPage {
                    query: "needle".to_owned(),
                    terms: vec!["term-a".to_owned(), "term-a".to_owned()],
                    options: Box::new(search_options.clone()),
                },
                limit: 3,
                ..EvidenceSelectorRequest::default()
            },
            EvidenceSelectorRequest {
                selector: EvidenceSelector::EventIds {
                    event_ids: vec![event_id],
                },
                limit: 1,
                ..EvidenceSelectorRequest::default()
            },
        ];
        for request in requests {
            let wire = serde_json::to_value(&request).unwrap();
            let round_trip: EvidenceSelectorRequest = serde_json::from_value(wire).unwrap();
            assert_eq!(round_trip, request);
        }

        let session = serde_json::to_value(&requests_for_wire()[0]).unwrap();
        let mut foreign = session.clone();
        foreign["selector"]["query"] = json!("not-a-session-field");
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(foreign).is_err());

        let mut union = session.clone();
        union["selector"]["event_ids"] = json!([event_id]);
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(union).is_err());

        let mut event_foreign = serde_json::to_value(&EvidenceSelectorRequest {
            selector: EvidenceSelector::EventIds {
                event_ids: vec![event_id],
            },
            limit: 1,
            ..EvidenceSelectorRequest::default()
        })
        .unwrap();
        event_foreign["selector"]["query"] = json!("not-an-event-field");
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(event_foreign).is_err());

        let mut search_union = serde_json::to_value(&requests_for_wire()[1]).unwrap();
        search_union["selector"]["event_ids"] = json!([event_id]);
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(search_union).is_err());

        let mut unknown = session.clone();
        unknown["unknown"] = json!(true);
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(unknown).is_err());

        for mut domain in [
            session.clone(),
            serde_json::to_value(&requests_for_wire()[1]).unwrap(),
            serde_json::to_value(&EvidenceSelectorRequest {
                selector: EvidenceSelector::EventIds {
                    event_ids: vec![event_id],
                },
                limit: 1,
                ..EvidenceSelectorRequest::default()
            })
            .unwrap(),
        ] {
            domain["selector"]["unknown"] = json!(true);
            assert!(serde_json::from_value::<EvidenceSelectorRequest>(domain).is_err());
        }

        let mut search = serde_json::to_value(&requests_for_wire()[1]).unwrap();
        search["selector"]["ctx_session_id"] = json!(session_id);
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(search).is_err());

        let mut nested_unknown = serde_json::to_value(&requests_for_wire()[1]).unwrap();
        nested_unknown["selector"]["options"]["unknown"] = json!(true);
        assert!(serde_json::from_value::<EvidenceSelectorRequest>(nested_unknown).is_err());

        let mut primary_true = requests_for_wire()[1].clone();
        let true_wire = serde_json::to_value(&primary_true).unwrap();
        let true_options = true_wire["selector"]["options"]["filters"]["primary_only"].as_bool();
        assert_eq!(true_options, Some(true));
        if let EvidenceSelector::SearchPage { options, .. } = &mut primary_true.selector {
            options.filters.primary_only = false;
        }
        let false_wire = serde_json::to_value(&primary_true).unwrap();
        assert_eq!(
            false_wire["selector"]["options"]["filters"]["primary_only"],
            json!(false)
        );
        let false_round_trip: EvidenceSelectorRequest = serde_json::from_value(false_wire).unwrap();
        assert!(!match false_round_trip.selector {
            EvidenceSelector::SearchPage { options, .. } => options.filters.primary_only,
            _ => unreachable!(),
        });
    }

    fn requests_for_wire() -> Vec<EvidenceSelectorRequest> {
        vec![
            EvidenceSelectorRequest {
                selector: EvidenceSelector::SessionPage {
                    ctx_session_id: Uuid::from_u128(501),
                    mode: TranscriptMode::Full,
                },
                limit: 3,
                ..EvidenceSelectorRequest::default()
            },
            EvidenceSelectorRequest {
                selector: EvidenceSelector::SearchPage {
                    query: "needle".to_owned(),
                    terms: vec!["term".to_owned()],
                    options: Box::new(PacketOptions {
                        filters: SearchFilters {
                            primary_only: true,
                            ..SearchFilters::default()
                        },
                        ..PacketOptions::default()
                    }),
                },
                limit: 3,
                ..EvidenceSelectorRequest::default()
            },
        ]
    }

    #[test]
    fn evidence_search_binding_changes_for_every_effective_search_field() {
        fn with_options(
            mut request: EvidenceSelectorRequest,
            mutate: impl FnOnce(&mut PacketOptions),
        ) -> EvidenceSelectorRequest {
            match &mut request.selector {
                EvidenceSelector::SearchPage { options, .. } => mutate(options),
                _ => unreachable!(),
            }
            request
        }

        let options = PacketOptions {
            filters: SearchFilters {
                roles: vec![EventRole::Assistant],
                exclude_roles: vec![EventRole::Tool],
                exclude_tool_names: vec!["sh".to_owned()],
                ..SearchFilters::default()
            },
            ..PacketOptions::default()
        };
        let base = EvidenceSelectorRequest {
            selector: EvidenceSelector::SearchPage {
                query: "query".to_owned(),
                terms: vec!["term".to_owned(), "term".to_owned()],
                options: Box::new(options),
            },
            limit: 2,
            fields: FieldSet::Full,
            byte_policy: BytePolicy {
                per_item_bytes: 100,
                page_bytes: 1_000,
            },
            artifact_bytes: 2_000,
            format: EvidenceFormat::Markdown,
            ..EvidenceSelectorRequest::default()
        };
        let hash = |request: &EvidenceSelectorRequest| {
            let canonical = request.canonicalized().unwrap();
            evidence_request_hash(&canonical.without_continuation()).unwrap()
        };
        let base_hash = hash(&base);
        let mut variants = Vec::new();

        let mut query = base.clone();
        if let EvidenceSelector::SearchPage { query, .. } = &mut query.selector {
            *query = "other-query".to_owned();
        }
        variants.push(query);
        let mut terms = base.clone();
        if let EvidenceSelector::SearchPage { terms, .. } = &mut terms.selector {
            terms.push("third-term".to_owned());
        }
        variants.push(terms);
        variants.push(with_options(base.clone(), |options| {
            options.snippet_chars += 1;
        }));
        variants.push(with_options(base.clone(), |options| {
            options.result_mode = SearchResultMode::Events;
        }));
        variants.push(with_options(base.clone(), |options| {
            options.match_mode = SearchMatchMode::Phrase;
        }));

        type FilterMutation = Box<dyn Fn(&mut SearchFilters)>;
        let filter_variants: Vec<FilterMutation> = vec![
            Box::new(|filters| filters.session = Some(Uuid::from_u128(1))),
            Box::new(|filters| filters.provider = Some(CaptureProvider::Codex)),
            Box::new(|filters| filters.history_source = Some("plugin/source".into())),
            Box::new(|filters| filters.provider_key = Some("provider".into())),
            Box::new(|filters| filters.source_id = Some("source".into())),
            Box::new(|filters| filters.source_format = Some("jsonl".into())),
            Box::new(|filters| filters.repo = Some("workspace".into())),
            Box::new(|filters| filters.since = Some(fixed_time())),
            Box::new(|filters| filters.primary_only = true),
            Box::new(|filters| filters.include_subagents = true),
            Box::new(|filters| filters.event_type = Some(EventType::Message)),
            Box::new(|filters| filters.roles = vec![EventRole::User]),
            Box::new(|filters| filters.exclude_roles = vec![EventRole::System]),
            Box::new(|filters| filters.exclude_tool_noise = true),
            Box::new(|filters| filters.exclude_tool_names = vec!["ctx".into()]),
            Box::new(|filters| filters.file = Some("src/lib.rs".into())),
            Box::new(|filters| {
                filters.exclude_provider_session = Some(ProviderSessionFilter {
                    provider: CaptureProvider::Codex,
                    provider_session_id: "provider-session".into(),
                    session_id: Some(Uuid::from_u128(2)),
                })
            }),
        ];
        for mutate in filter_variants {
            variants.push(with_options(base.clone(), |options| {
                mutate(&mut options.filters)
            }));
        }

        let mut hashes = std::collections::HashSet::new();
        hashes.insert(base_hash.clone());
        for variant in variants {
            let variant_hash = hash(&variant);
            assert_ne!(variant_hash, base_hash);
            assert!(hashes.insert(variant_hash));
        }
    }

    fn search_projection_id(result: &SearchResultProjectionV1) -> Uuid {
        match result {
            SearchResultProjectionV1::Full(value) => value.item_id,
            SearchResultProjectionV1::Compact(value) => value.item_id,
        }
    }
}
