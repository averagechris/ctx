//! Read-only, transport-neutral query projections and continuation handling.
//!
//! The DTOs in this crate deliberately do not contain core `Event`/`Session`
//! values or search `SearchPacket`s. Their v1 shapes are additive public
//! projections, with structurally distinct full and compact variants.

use chrono::{DateTime, Utc};
use ctx_history_core::{
    AgentType, CaptureProvider, CaptureSource, CaptureSourceKind, ContextCitationType,
    ContextLinks, Event, EventRole, EventType, Fidelity, RedactionState, SearchMatchMode,
    SearchQueryPlan, Session, SessionStatus, Visibility,
};
use ctx_history_search::{
    search_packet, search_packet_terms, validate_query_request, PacketOptions, SearchFilters,
    SearchPacketResult, SearchResultMode, SearchResultScope, SEARCH_PACKET_SCHEMA_VERSION,
};
use ctx_history_store::{SelectedEventMode, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, path::Path};
use uuid::Uuid;

pub const QUERY_DTO_SCHEMA_VERSION: u32 = 1;
pub const QUERY_REVISION: u32 = 1;
pub const DEFAULT_SHOW_LIMIT: usize = 200;
pub const MAX_SHOW_LIMIT: usize = 1000;
pub const DEFAULT_ITEM_BYTES: usize = 4096;
pub const MAX_ITEM_BYTES: usize = 1024 * 1024;
pub const DEFAULT_PAGE_BYTES: usize = 256 * 1024;
pub const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
/// Compatibility name retained for callers that used the scaffold constant.
pub const MAX_SNIPPET_BYTES: usize = MAX_ITEM_BYTES;
const MAX_TOKEN_BYTES: usize = 4096;

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
}

pub type Result<T> = std::result::Result<T, QueryError>;

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
    pub seq: u64,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    pub text: String,
    pub text_truncation: TextTruncationV1,
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
    pub exclude_tool_name: Option<String>,
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

pub struct QueryService<'a> {
    store: &'a Store,
}

impl<'a> QueryService<'a> {
    pub fn new(store: &'a Store) -> Self {
        Self { store }
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
        let snapshot = self.store.snapshot_fingerprint()?;
        let token = continuation
            .map(|raw| decode_token(raw, "show_session", &request, &snapshot))
            .transpose()?;
        let offset = token_offset(token.as_ref())?;
        let after = token.as_ref().and_then(|value| value.seq.zip(value.id));
        let selected_total = self
            .store
            .selected_event_count_for_session(session.id, mode.store_mode())?;
        if let (Some(token), Some(key)) = (token.as_ref(), after) {
            if offset == 0 || offset >= selected_total {
                return Err(QueryError::InvalidContinuation(
                    "show offset is outside the resumable range".to_owned(),
                ));
            }
            let position = self
                .store
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
        let raw_events = self.store.selected_events_for_session_after(
            session.id,
            mode.store_mode(),
            after,
            fetch_limit,
        )?;
        let source = if fields == FieldSet::Full {
            session
                .capture_source_id
                .map(|id| self.store.get_capture_source(id).map(project_source))
                .transpose()?
        } else {
            None
        };
        let mut source_cache = HashMap::<Uuid, Option<SourceFullV1>>::new();
        if let (Some(source_id), Some(source)) = (session.capture_source_id, source.clone()) {
            source_cache.insert(source_id, Some(source));
        }

        let mut events = Vec::new();
        let mut compact_json_bytes = 0usize;
        let mut item_text_truncated = 0usize;
        let mut page_budget_exhausted = false;
        let mut last_key = None;
        for event in raw_events.iter().take(page_size) {
            if fields == FieldSet::Full {
                if let Some(source_id) = event.capture_source_id {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        source_cache.entry(source_id)
                    {
                        let projected = self
                            .store
                            .get_capture_source(source_id)
                            .map(project_source)?;
                        entry.insert(Some(projected));
                    }
                }
            }
            let event_source = event
                .capture_source_id
                .and_then(|source_id| source_cache.get(&source_id))
                .and_then(Option::as_ref);
            let (full, compact) = project_event(
                event,
                byte_policy.per_item_bytes,
                session.provider,
                session.external_session_id.as_deref(),
                event_source,
            );
            let projection = match fields {
                FieldSet::Full => EventProjectionV1::Full(Box::new(full)),
                FieldSet::Compact => EventProjectionV1::Compact(compact),
            };
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
        let after_snapshot = self.store.snapshot_fingerprint()?;
        if after_snapshot != snapshot {
            return Err(QueryError::SnapshotChanged);
        }
        let next_continuation = if has_more {
            let cursor_key = last_key.or(after);
            Some(encode_token(&Token {
                v: QUERY_DTO_SCHEMA_VERSION,
                kind: "show_session".to_owned(),
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
        let snapshot = self.store.snapshot_fingerprint()?;
        let token = continuation
            .map(|raw| decode_token(raw, "search", &request, &snapshot))
            .transpose()?;
        let offset = token_offset(token.as_ref())?;

        // Every page replays and slices this same fixed candidate pool. Neither
        // page offset nor requested page size changes candidate generation.
        let mut pool_options = options.clone();
        pool_options.limit = ctx_history_search::MAX_RESULT_LIMIT;
        let packet = if terms.is_empty() {
            search_packet(self.store, query, &pool_options)?
        } else {
            // Do not pre-deduplicate: the canonical request preserves the exact
            // repeated term vector even though search-core may normalize it.
            search_packet_terms(self.store, query, terms, &pool_options)?
        };
        let pool_total = packet.results.len();
        if token.is_some() && (offset == 0 || offset >= pool_total) {
            return Err(QueryError::InvalidContinuation(
                "offset is outside the candidate pool".to_owned(),
            ));
        }

        let mut results = Vec::new();
        let mut compact_json_bytes = 0usize;
        let mut item_text_truncated = 0usize;
        let mut page_budget_exhausted = false;
        for result in packet.results.iter().skip(offset).take(page_size) {
            let (full, compact) = project_search_result(
                self.store,
                result,
                byte_policy.per_item_bytes,
                query,
                terms,
                &options,
            );
            let projection = match fields {
                FieldSet::Full => SearchResultProjectionV1::Full(Box::new(full)),
                FieldSet::Compact => SearchResultProjectionV1::Compact(compact),
            };
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
        }
        let returned = results.len();
        let next_offset = offset
            .checked_add(returned)
            .ok_or(QueryError::ArithmeticOverflow)?;
        let has_more = returned > 0 && next_offset < pool_total;
        let after_snapshot = self.store.snapshot_fingerprint()?;
        if after_snapshot != snapshot {
            return Err(QueryError::SnapshotChanged);
        }
        let next_continuation = if has_more {
            Some(encode_token(&Token {
                v: QUERY_DTO_SCHEMA_VERSION,
                kind: "search".to_owned(),
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
            legacy_query: packet.query,
            context: project_search_context(query, terms, &options, fields),
            query_plan: packet.query_plan,
            generated_at: packet.generated_at,
            results,
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

fn project_event(
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
        seq: event.seq,
        event_type: event.event_type,
        role: event.role,
        occurred_at: event.occurred_at,
        text: text.clone(),
        text_truncation: text_truncation.clone(),
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
        suggested_next_commands: suggested_next_commands(result, query, terms, options.match_mode),
        visibility: result.visibility,
    };
    (full, compact)
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
        exclude_tool_name: filters.exclude_tool_name.clone(),
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
    exclude_tool_name: &'a Option<String>,
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
            exclude_tool_name: &filters.exclude_tool_name,
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
        "show_session"
            if !matches!((token.offset, token.seq, token.id), (1.., Some(_), Some(_))) =>
        {
            return Err(QueryError::InvalidContinuation(
                "show token requires both seq and id after the first item".to_owned(),
            ));
        }
        "search" if token.offset == 0 || token.seq.is_some() || token.id.is_some() => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use ctx_history_core::{EntityTimestamps, HistoryRecord, SyncMetadata, SyncState};

    fn fixed_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
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
        let (full, compact) = project_event(
            &raw,
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
        );
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
            )
            .0
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
        let long = project_event(&raw, 10, CaptureProvider::Codex, None, None).0;
        assert_eq!(long.text_truncation.original_bytes, 8_000);
        assert_eq!(long.text_truncation.returned_bytes, 7);
        assert!(long.text_truncation.truncated);

        raw.payload = serde_json::json!({"api_key": "must-never-be-a-text-fallback"});
        let unknown = project_event(&raw, DEFAULT_ITEM_BYTES, CaptureProvider::Codex, None, None).1;
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
        let (first_full, _) = project_event(
            &raw_events[0],
            DEFAULT_ITEM_BYTES,
            CaptureProvider::Codex,
            Some("provider-session"),
            None,
        );
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
}
