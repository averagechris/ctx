//! Deterministic, transport-neutral evidence bundle renderers.
//!
//! This module intentionally accepts only [`NormalizedEvidencePageV1`].  It
//! has no store, filesystem, clock, environment, or network access.  The
//! complete output is assembled and bounded before it is returned or handed
//! to a caller-owned writer.

use crate::{
    ContentStateV1, ContentV1, EvidenceContinuationV1, EvidenceCountV1, EvidenceEventV1,
    EvidenceFormat, EvidenceRecordV1, EvidenceSearchResultV1, EvidenceSelectorRequest,
    EvidenceSessionV1, NormalizedEvidencePageV1,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Write;

pub const JSONL_FORMAT: &str = "ctx-evidence-bundle-jsonl-v1";
pub const MARKDOWN_FORMAT: &str = "ctx-evidence-bundle-markdown-v1";
pub const MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RECORD_JSON_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 128 * 1024;
pub const MAX_COMPLETION_BYTES: usize = 128 * 1024;
pub const MAX_NEXT_ARGUMENTS_BYTES: usize = 128 * 1024;
pub const MAX_RECORDS: usize = 1001;

#[derive(Debug, thiserror::Error)]
pub enum EvidenceRenderError {
    #[error("evidence artifact byte limit exceeded")]
    ArtifactLimit,
    #[error("evidence manifest byte limit exceeded")]
    ManifestLimit,
    #[error("evidence completion byte limit exceeded")]
    CompletionLimit,
    #[error("evidence record byte limit exceeded")]
    RecordLimit,
    #[error("evidence record count limit exceeded")]
    RecordCountLimit,
    #[error("evidence continuation arguments byte limit exceeded")]
    NextArgumentsLimit,
    #[error("evidence page byte accounting is inconsistent")]
    PageAccounting,
    #[error("evidence artifact byte policy is invalid")]
    InvalidArtifactPolicy,
    #[error("Markdown scalar contains a disallowed control character")]
    MarkdownScalarLimit,
    #[error("evidence arithmetic overflow")]
    ArithmeticOverflow,
    #[error("evidence serialization failed")]
    Serialization,
    #[error("evidence output write failed")]
    OutputIo,
    #[error("unsupported normalized evidence schema version")]
    SchemaVersion,
    #[error("normalized evidence page invariant failed")]
    Invariant,
}

pub type EvidenceRenderResult<T> = std::result::Result<T, EvidenceRenderError>;

/// Render a normalized page in the format selected by the normalized page.
pub fn render_evidence(page: &NormalizedEvidencePageV1) -> EvidenceRenderResult<Vec<u8>> {
    match page.format {
        EvidenceFormat::Jsonl => render_jsonl(page),
        EvidenceFormat::Markdown => render_markdown(page),
    }
}

/// Render a complete JSONL artifact.  No output is returned until the whole
/// artifact has passed all bounds.
pub fn render_jsonl(page: &NormalizedEvidencePageV1) -> EvidenceRenderResult<Vec<u8>> {
    let parts = preflight_json(page, JSONL_FORMAT)?;
    let mut output = Vec::new();
    append_line(&mut output, &parts.manifest)?;
    for record in &parts.records {
        append_line(&mut output, record)?;
    }
    append_line(&mut output, &parts.completion)?;
    check_artifact_size(output.len(), page.work.artifact_bytes)?;
    Ok(output)
}

/// Render a complete safe-Markdown artifact.  Markdown is a projection of the
/// same records; each record also has a fenced canonical JSON representation so
/// no normalized field is silently lost by the human-readable projection.
pub fn render_markdown(page: &NormalizedEvidencePageV1) -> EvidenceRenderResult<Vec<u8>> {
    let parts = preflight_json(page, MARKDOWN_FORMAT)?;
    let mut sizing = MarkdownOutput::new(page.work.artifact_bytes, false);
    render_markdown_into(&mut sizing, page, &parts)?;
    let expected = sizing.len;
    let mut output = MarkdownOutput::new(page.work.artifact_bytes, true);
    render_markdown_into(&mut output, page, &parts)?;
    if output.len != expected {
        return Err(EvidenceRenderError::Invariant);
    }
    output.finish()
}

/// Preflight and then write a complete artifact to a caller-owned sink.  The
/// renderer performs no writes before the artifact has passed preflight.
pub fn write_evidence<W: Write>(
    page: &NormalizedEvidencePageV1,
    sink: &mut W,
) -> EvidenceRenderResult<usize> {
    let bytes = render_evidence(page)?;
    sink.write_all(&bytes)
        .map_err(|_| EvidenceRenderError::OutputIo)?;
    Ok(bytes.len())
}

struct JsonParts {
    bundle_id: String,
    manifest: Vec<u8>,
    records: Vec<Vec<u8>>,
    completion: Vec<u8>,
}

struct MarkdownOutput {
    bytes: Vec<u8>,
    len: usize,
    cap: usize,
    collect: bool,
}

impl MarkdownOutput {
    fn new(cap: usize, collect: bool) -> Self {
        Self {
            bytes: if collect {
                Vec::with_capacity(cap)
            } else {
                Vec::new()
            },
            len: 0,
            cap,
            collect,
        }
    }

    fn push(&mut self, value: &[u8]) -> EvidenceRenderResult<()> {
        self.len = self
            .len
            .checked_add(value.len())
            .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
        if self.len > self.cap {
            return Err(EvidenceRenderError::ArtifactLimit);
        }
        if self.collect {
            self.bytes.extend_from_slice(value);
        }
        Ok(())
    }

    fn push_repeated(&mut self, byte: u8, count: usize) -> EvidenceRenderResult<()> {
        let value = self
            .len
            .checked_add(count)
            .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
        if value > self.cap {
            return Err(EvidenceRenderError::ArtifactLimit);
        }
        self.len = value;
        if self.collect {
            self.bytes.extend(std::iter::repeat(byte).take(count));
        }
        Ok(())
    }

    fn finish(self) -> EvidenceRenderResult<Vec<u8>> {
        if !self.collect || self.len != self.bytes.len() {
            return Err(EvidenceRenderError::Invariant);
        }
        Ok(self.bytes)
    }
}

fn render_markdown_into(
    output: &mut MarkdownOutput,
    page: &NormalizedEvidencePageV1,
    parts: &JsonParts,
) -> EvidenceRenderResult<()> {
    output.push(
        b"<!-- ctx-evidence-bundle: {\"schema_version\":\"ctx-evidence-bundle-markdown-v1\",\"private\":true,\"share_safe\":false} -->\n",
    )?;
    markdown_summary(output, page, "manifest", &parts.manifest, &parts.bundle_id)?;
    for (record, value) in page.records.iter().zip(&parts.records) {
        markdown_item(output, record, value)?;
    }
    markdown_summary(
        output,
        page,
        "completion",
        &parts.completion,
        &parts.bundle_id,
    )?;
    Ok(())
}

fn preflight_json(
    page: &NormalizedEvidencePageV1,
    schema: &'static str,
) -> EvidenceRenderResult<JsonParts> {
    validate_page(page)?;
    // `records` may contain the session envelope in addition to selected
    // items.  `validate_page` has established that pagination's count is the
    // validated selected-item count, so use it for the bundle summaries.
    let returned = page.pagination.returned_items;
    if (schema == JSONL_FORMAT && page.format != EvidenceFormat::Jsonl)
        || (schema == MARKDOWN_FORMAT && page.format != EvidenceFormat::Markdown)
    {
        return Err(EvidenceRenderError::Invariant);
    }
    if page.schema_version != crate::EVIDENCE_PROJECTION_SCHEMA_VERSION
        || page.records.iter().any(|record| match record {
            EvidenceRecordV1::Session(value) => {
                value.schema_version != crate::EVIDENCE_PROJECTION_SCHEMA_VERSION
            }
            EvidenceRecordV1::Event(value) => {
                value.schema_version != crate::EVIDENCE_PROJECTION_SCHEMA_VERSION
            }
            EvidenceRecordV1::SearchResult(value) => {
                value.schema_version != crate::EVIDENCE_PROJECTION_SCHEMA_VERSION
            }
        })
    {
        return Err(EvidenceRenderError::SchemaVersion);
    }
    if page.work.artifact_bytes == 0 || page.work.artifact_bytes > MAX_ARTIFACT_BYTES {
        return Err(EvidenceRenderError::InvalidArtifactPolicy);
    }
    if page.records.len() > MAX_RECORDS {
        return Err(EvidenceRenderError::RecordCountLimit);
    }

    let next_arguments_bytes = page
        .continuation
        .as_ref()
        .map(|continuation| {
            serde_json::to_vec(&continuation.next_arguments)
                .map(|value| value.len())
                .map_err(|_| EvidenceRenderError::Serialization)
        })
        .transpose()?;
    if next_arguments_bytes.is_some_and(|value| value > MAX_NEXT_ARGUMENTS_BYTES) {
        return Err(EvidenceRenderError::NextArgumentsLimit);
    }

    let mut records = Vec::with_capacity(page.records.len());
    let mut item_bytes = 0usize;
    for record in &page.records {
        let value = item_record(record, schema)?;
        if value.len() > MAX_RECORD_JSON_BYTES {
            return Err(EvidenceRenderError::RecordLimit);
        }
        item_bytes = item_bytes
            .checked_add(value.len())
            .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
        records.push(value);
        let lower_bound = item_bytes
            .checked_add(records.len() + 2)
            .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
        if lower_bound > page.work.artifact_bytes {
            return Err(EvidenceRenderError::ArtifactLimit);
        }
    }
    if item_bytes > page.work.page_bytes {
        return Err(EvidenceRenderError::PageAccounting);
    }

    let bundle_id = bundle_id(page)?;
    let manifest = manifest_record(page, returned, &bundle_id, schema, item_bytes)?;
    if manifest.len() > MAX_MANIFEST_BYTES {
        return Err(EvidenceRenderError::ManifestLimit);
    }

    let completion = completion_record(page, returned, &bundle_id, schema, item_bytes)?;
    if completion.len() > MAX_COMPLETION_BYTES {
        return Err(EvidenceRenderError::CompletionLimit);
    }
    let total = manifest
        .len()
        .checked_add(completion.len())
        .and_then(|n| n.checked_add(item_bytes))
        .and_then(|n| n.checked_add(page.records.len() + 2))
        .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
    check_artifact_size(total, page.work.artifact_bytes)?;
    Ok(JsonParts {
        bundle_id,
        manifest,
        records,
        completion,
    })
}

fn check_artifact_size(size: usize, cap: usize) -> EvidenceRenderResult<()> {
    if size > cap {
        Err(EvidenceRenderError::ArtifactLimit)
    } else {
        Ok(())
    }
}

fn validate_page(page: &NormalizedEvidencePageV1) -> EvidenceRenderResult<()> {
    if page.schema_version != crate::EVIDENCE_PROJECTION_SCHEMA_VERSION
        || page.request_binding.kind != "evidence_selector"
        || page.request_hash.is_empty()
        || page.snapshot_fingerprint.is_empty()
        || page.request_binding.format != page.format
    {
        return Err(EvidenceRenderError::Invariant);
    }
    let binding_domain = match &page.request_binding.selector {
        crate::EvidenceSelectorBindingV1::SessionPage { .. } => "session_page",
        crate::EvidenceSelectorBindingV1::SearchPage { .. } => "search_page",
        crate::EvidenceSelectorBindingV1::EventIds { .. } => "event_ids",
    };
    if binding_domain != page.domain {
        return Err(EvidenceRenderError::Invariant);
    }
    if page.pagination.has_more != page.continuation.is_some()
        || page.pagination.offset != page.omitted.before
    {
        return Err(EvidenceRenderError::Invariant);
    }
    match (&page.pagination.continuation, &page.continuation) {
        (None, None) => {}
        (Some(token), Some(continuation))
            if token == &continuation.token
                && continuation.next_arguments.continuation.as_deref() == Some(token) => {}
        _ => return Err(EvidenceRenderError::Invariant),
    }
    if page.pagination.has_more != (page.omitted.after > 0) {
        return Err(EvidenceRenderError::Invariant);
    }
    if page.work != page.request_binding.work
        || page.work.selector_limit != page.request_binding.limit
        || page.work.per_item_bytes != page.request_binding.byte_policy.per_item_bytes
        || page.work.page_bytes != page.request_binding.byte_policy.page_bytes
        || page.work.artifact_bytes != page.request_binding.artifact_bytes
        || page.pagination.page_size != page.request_binding.limit
        || page.selector_item_json_bytes > page.work.page_bytes
    {
        return Err(EvidenceRenderError::Invariant);
    }
    let recomputed = page.records.iter().try_fold(0usize, |total, record| {
        let bytes = serde_json::to_vec(record)
            .map_err(|_| EvidenceRenderError::Serialization)?
            .len();
        total
            .checked_add(bytes)
            .ok_or(EvidenceRenderError::ArithmeticOverflow)
    })?;
    if recomputed != page.normalized_item_json_bytes {
        return Err(EvidenceRenderError::Invariant);
    }
    let session_records = page
        .records
        .iter()
        .filter(|record| matches!(record, EvidenceRecordV1::Session(_)))
        .count();
    let selected_page_count = page.records.len().saturating_sub(session_records);
    if page.pagination.returned_items != selected_page_count {
        return Err(EvidenceRenderError::Invariant);
    }
    let selected = page.selected_total;
    let retained = page.retained_pool_total;
    let corpus = page.corpus_count.as_ref();
    match page.domain {
        "session_page" => {
            if selected.is_none()
                || retained.is_some()
                || corpus.is_some()
                || session_records != 1
                || !matches!(page.records.first(), Some(EvidenceRecordV1::Session(_)))
                || !page
                    .records
                    .iter()
                    .skip(1)
                    .all(|record| matches!(record, EvidenceRecordV1::Event(_)))
            {
                return Err(EvidenceRenderError::Invariant);
            }
        }
        "event_ids" => {
            if selected.is_none()
                || retained.is_some()
                || corpus.is_some()
                || session_records != 0
                || !page
                    .records
                    .iter()
                    .all(|record| matches!(record, EvidenceRecordV1::Event(_)))
            {
                return Err(EvidenceRenderError::Invariant);
            }
        }
        "search_page" => {
            if selected.is_some()
                || retained.is_none()
                || corpus.is_none()
                || session_records != 0
                || !page
                    .records
                    .iter()
                    .all(|record| matches!(record, EvidenceRecordV1::SearchResult(_)))
            {
                return Err(EvidenceRenderError::Invariant);
            }
            if corpus.is_some_and(|count| {
                count.value != retained.unwrap_or_default()
                    || !matches!(count.kind, "exact" | "lower_bound")
            }) {
                return Err(EvidenceRenderError::Invariant);
            }
        }
        _ => return Err(EvidenceRenderError::Invariant),
    }
    let selected_total = selected
        .or(retained)
        .ok_or(EvidenceRenderError::Invariant)?;
    let omitted_total = page
        .omitted
        .before
        .checked_add(selected_page_count)
        .and_then(|value| value.checked_add(page.omitted.after))
        .ok_or(EvidenceRenderError::ArithmeticOverflow)?;
    if omitted_total != selected_total {
        return Err(EvidenceRenderError::Invariant);
    }
    Ok(())
}

fn append_line(output: &mut Vec<u8>, line: &[u8]) -> EvidenceRenderResult<()> {
    output.extend_from_slice(line);
    output.push(b'\n');
    Ok(())
}

fn bundle_id(page: &NormalizedEvidencePageV1) -> EvidenceRenderResult<String> {
    let binding = serde_json::to_vec(&page.request_binding)
        .map_err(|_| EvidenceRenderError::Serialization)?;
    let mut hasher = Sha256::new();
    hasher.update(b"ctx-evidence-bundle-v1\0");
    hasher.update(binding);
    hasher.update(page.request_hash.as_bytes());
    hasher.update([0]);
    hasher.update(page.snapshot_fingerprint.as_bytes());
    hasher.update([0]);
    for record in &page.records {
        hasher.update(serde_json::to_vec(record).map_err(|_| EvidenceRenderError::Serialization)?);
        hasher.update([0]);
    }
    if let Some(value) = &page.continuation {
        hasher.update(value.token.as_bytes());
        hasher.update([0]);
        hasher.update(
            serde_json::to_vec(&value.next_arguments)
                .map_err(|_| EvidenceRenderError::Serialization)?,
        );
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn serialize_map<F>(entries: usize, write: F) -> EvidenceRenderResult<Vec<u8>>
where
    F: FnOnce(&mut OrderedMap) -> serde_json::Result<()>,
{
    let mut map = OrderedMap {
        entries: Vec::with_capacity(entries),
    };
    write(&mut map).map_err(|_| EvidenceRenderError::Serialization)?;
    let object = map.entries.into_iter().collect::<serde_json::Map<_, _>>();
    serde_json::to_vec(&serde_json::Value::Object(object))
        .map_err(|_| EvidenceRenderError::Serialization)
}

struct OrderedMap {
    entries: Vec<(String, serde_json::Value)>,
}

impl OrderedMap {
    fn serialize_entry<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> serde_json::Result<()> {
        self.entries
            .push((key.to_owned(), serde_json::to_value(value)?));
        Ok(())
    }
}

fn common(
    map: &mut OrderedMap,
    record_type: &'static str,
    schema: &'static str,
) -> serde_json::Result<()> {
    map.serialize_entry("schema_version", schema)?;
    map.serialize_entry("record_type", record_type)?;
    map.serialize_entry("private", &true)?;
    map.serialize_entry("share_safe", &false)
}

fn manifest_record(
    page: &NormalizedEvidencePageV1,
    returned: usize,
    bundle_id: &str,
    schema: &'static str,
    item_json_bytes: usize,
) -> EvidenceRenderResult<Vec<u8>> {
    let counts = Counts {
        selected_total: page.selected_total,
        retained_pool_total: page.retained_pool_total,
        corpus_count: page.corpus_count.as_ref(),
        returned,
        omitted_before: page.omitted.before,
        omitted_after: page.omitted.after,
        omitted_exact: page.omitted.exact,
    };
    let continuation = continuation_value(page.pagination.has_more, page.continuation.as_ref());
    let bytes = ByteSummary {
        item_json_bytes,
        page_budget_exhausted: page.page_budget_exhausted,
    };
    serialize_map(13, |map| {
        common(map, "manifest", schema)?;
        map.serialize_entry("bundle_id", bundle_id)?;
        map.serialize_entry("selector", &page.request_binding.selector)?;
        map.serialize_entry("ordering", page.ordering)?;
        map.serialize_entry("request_binding", &page.request_binding)?;
        map.serialize_entry("request_hash", &page.request_hash)?;
        map.serialize_entry(
            "snapshot",
            &Snapshot {
                schema_version: 1002,
                query_revision: crate::QUERY_REVISION,
                fingerprint: &page.snapshot_fingerprint,
            },
        )?;
        map.serialize_entry("counts", &counts)?;
        map.serialize_entry("search_truncation", &page.search_truncation)?;
        map.serialize_entry("continuation", &continuation)?;
        map.serialize_entry("bytes", &bytes)?;
        map.serialize_entry(
            "format",
            if schema == JSONL_FORMAT {
                "jsonl"
            } else {
                "markdown"
            },
        )
    })
}

fn completion_record(
    page: &NormalizedEvidencePageV1,
    returned: usize,
    _bundle_id: &str,
    schema: &'static str,
    item_json_bytes: usize,
) -> EvidenceRenderResult<Vec<u8>> {
    let counts = Counts {
        selected_total: page.selected_total,
        retained_pool_total: page.retained_pool_total,
        corpus_count: page.corpus_count.as_ref(),
        returned,
        omitted_before: page.omitted.before,
        omitted_after: page.omitted.after,
        omitted_exact: page.omitted.exact,
    };
    let continuation = continuation_value(page.pagination.has_more, page.continuation.as_ref());
    let bytes = ByteSummary {
        item_json_bytes,
        page_budget_exhausted: page.page_budget_exhausted,
    };
    serialize_map(8, |map| {
        common(map, "completion", schema)?;
        map.serialize_entry("counts", &counts)?;
        map.serialize_entry("search_truncation", &page.search_truncation)?;
        map.serialize_entry("continuation", &continuation)?;
        map.serialize_entry("bytes", &bytes)?;
        map.serialize_entry(
            "format",
            if schema == JSONL_FORMAT {
                "jsonl"
            } else {
                "markdown"
            },
        )
    })
}

#[derive(Serialize)]
struct Snapshot<'a> {
    schema_version: u32,
    query_revision: u32,
    fingerprint: &'a str,
}

#[derive(Serialize)]
struct Counts<'a> {
    selected_total: Option<usize>,
    retained_pool_total: Option<usize>,
    corpus_count: Option<&'a EvidenceCountV1>,
    returned: usize,
    omitted_before: usize,
    omitted_after: usize,
    omitted_exact: bool,
}

#[derive(Serialize)]
struct ByteSummary {
    item_json_bytes: usize,
    page_budget_exhausted: bool,
}

#[derive(Serialize)]
struct ContinuationValue<'a> {
    has_more: bool,
    next: Option<&'a str>,
    next_arguments: Option<&'a EvidenceSelectorRequest>,
}

fn continuation_value<'a>(
    has_more: bool,
    value: Option<&'a EvidenceContinuationV1>,
) -> ContinuationValue<'a> {
    ContinuationValue {
        has_more,
        next: value.map(|value| value.token.as_str()),
        next_arguments: value.map(|value| &value.next_arguments),
    }
}

fn item_record(record: &EvidenceRecordV1, schema: &'static str) -> EvidenceRenderResult<Vec<u8>> {
    match record {
        EvidenceRecordV1::Session(value) => session_record(value, schema),
        EvidenceRecordV1::Event(value) => event_record(value, schema),
        EvidenceRecordV1::SearchResult(value) => result_record(value, schema),
    }
}

fn session_record(
    value: &EvidenceSessionV1,
    schema: &'static str,
) -> EvidenceRenderResult<Vec<u8>> {
    serialize_map(16, |map| {
        common(map, "session", schema)?;
        map.serialize_entry("record_id", &value.record_id)?;
        map.serialize_entry("ctx_session_id", &value.ctx_session_id)?;
        map.serialize_entry("provider", &value.provider)?;
        map.serialize_entry("agent_type", &value.agent_type)?;
        map.serialize_entry("status", &value.status)?;
        map.serialize_entry("is_primary", &value.is_primary)?;
        map.serialize_entry("started_at", &value.started_at)?;
        map.serialize_entry("ended_at", &value.ended_at)?;
        map.serialize_entry("fidelity", &value.fidelity)?;
        if let Some(provenance) = &value.provenance {
            map.serialize_entry("provenance", provenance)?;
        }
        Ok(())
    })
}

fn event_record(value: &EvidenceEventV1, schema: &'static str) -> EvidenceRenderResult<Vec<u8>> {
    serialize_map(19, |map| {
        common(map, "event", schema)?;
        map.serialize_entry("record_id", &value.record_id)?;
        map.serialize_entry("ctx_event_id", &value.ctx_event_id)?;
        map.serialize_entry("ctx_session_id", &value.ctx_session_id)?;
        map.serialize_entry("sequence", &value.sequence)?;
        map.serialize_entry("event_type", &value.event_type)?;
        map.serialize_entry("role", &value.role)?;
        map.serialize_entry("occurred_at", &value.occurred_at)?;
        map.serialize_entry("content", &safe_content(&value.content))?;
        map.serialize_entry("fidelity", &value.fidelity)?;
        if let Some(provenance) = &value.provenance {
            map.serialize_entry("provenance", provenance)?;
        }
        if let Some(provenance) = &value.session_provenance {
            map.serialize_entry("session_provenance", provenance)?;
        }
        if !value.citations.is_empty() {
            map.serialize_entry("citations", &value.citations)?;
        }
        if !value.citation_omissions.is_empty() {
            map.serialize_entry("citation_omissions", &value.citation_omissions)?;
        }
        Ok(())
    })
}

fn result_record(
    value: &EvidenceSearchResultV1,
    schema: &'static str,
) -> EvidenceRenderResult<Vec<u8>> {
    serialize_map(20, |map| {
        common(map, "result", schema)?;
        map.serialize_entry("record_id", &value.record_id)?;
        map.serialize_entry("item_id", &value.item_id)?;
        map.serialize_entry("result_scope", &value.result_scope)?;
        map.serialize_entry("ctx_session_id", &value.ctx_session_id)?;
        map.serialize_entry("ctx_event_id", &value.ctx_event_id)?;
        map.serialize_entry("event_seq", &value.event_seq)?;
        let title = if content_is_visible(&value.content) {
            value.title.as_ref()
        } else {
            None
        };
        map.serialize_entry("title", &title)?;
        map.serialize_entry("content", &safe_content(&value.content))?;
        map.serialize_entry("rank", &value.rank)?;
        map.serialize_entry("timestamp", &value.timestamp)?;
        map.serialize_entry("why_matched", &value.why_matched)?;
        if let Some(provenance) = &value.provenance {
            map.serialize_entry("provenance", provenance)?;
        }
        if let Some(provenance) = &value.session_provenance {
            map.serialize_entry("session_provenance", provenance)?;
        }
        if !value.citations.is_empty() {
            map.serialize_entry("citations", &value.citations)?;
        }
        if !value.citation_omissions.is_empty() {
            map.serialize_entry("citation_omissions", &value.citation_omissions)?;
        }
        Ok(())
    })
}

fn markdown_summary(
    output: &mut MarkdownOutput,
    page: &NormalizedEvidencePageV1,
    record_type: &str,
    json_record: &[u8],
    bundle_id: &str,
) -> EvidenceRenderResult<()> {
    output.push(
        format!(
            "<!-- ctx-evidence-record: record_type={} schema_version={} private=true share_safe=false -->\n",
            record_type, MARKDOWN_FORMAT
        )
        .as_bytes(),
    )?;
    push_format(
        output,
        format_args!("## Evidence {}\n\n", scalar(record_type, true)?),
    )?;
    push_format(
        output,
        format_args!("- schema_version: {}\n", scalar(MARKDOWN_FORMAT, true)?),
    )?;
    push_format(
        output,
        format_args!("- private: {}\n", scalar("true", true)?),
    )?;
    push_format(
        output,
        format_args!("- share_safe: {}\n", scalar("false", true)?),
    )?;
    push_format(
        output,
        format_args!("- bundle_id: {}\n", scalar(bundle_id, true)?),
    )?;
    push_format(
        output,
        format_args!("- domain: {}\n", scalar(page.domain, true)?),
    )?;
    push_format(
        output,
        format_args!("- ordering: {}\n\n", scalar(page.ordering, true)?),
    )?;
    output.push(b"### Canonical record\n")?;
    fenced(output, "json", &String::from_utf8_lossy(json_record))?;
    Ok(())
}

fn markdown_item(
    output: &mut MarkdownOutput,
    record: &EvidenceRecordV1,
    json_record: &[u8],
) -> EvidenceRenderResult<()> {
    let (record_type, id) = match record {
        EvidenceRecordV1::Session(value) => ("session", value.record_id),
        EvidenceRecordV1::Event(value) => ("event", value.record_id),
        EvidenceRecordV1::SearchResult(value) => ("result", value.record_id),
    };
    output.push(
        format!(
            "<!-- ctx-evidence-record: record_type={} schema_version={} private=true share_safe=false record_id={} -->\n",
            record_type, MARKDOWN_FORMAT, id
        )
        .as_bytes(),
    )?;
    push_format(
        output,
        format_args!(
            "## Evidence {} {}\n\n",
            record_type,
            scalar(&id.to_string(), true)?
        ),
    )?;
    push_format(
        output,
        format_args!("- record_type: {}\n", scalar(record_type, true)?),
    )?;
    push_format(
        output,
        format_args!("- schema_version: {}\n", scalar(MARKDOWN_FORMAT, true)?),
    )?;
    push_format(
        output,
        format_args!("- private: {}\n", scalar("true", true)?),
    )?;
    push_format(
        output,
        format_args!("- share_safe: {}\n\n", scalar("false", true)?),
    )?;

    match record {
        EvidenceRecordV1::Session(value) => markdown_session(output, value)?,
        EvidenceRecordV1::Event(value) => markdown_event(output, value)?,
        EvidenceRecordV1::SearchResult(value) => markdown_result(output, value)?,
    }
    output.push(b"### Canonical record\n")?;
    fenced(output, "json", &String::from_utf8_lossy(json_record))?;
    Ok(())
}

fn markdown_session(
    output: &mut MarkdownOutput,
    value: &EvidenceSessionV1,
) -> EvidenceRenderResult<()> {
    scalar_line(
        output,
        "ctx_session_id",
        &value.ctx_session_id.to_string(),
        true,
    )?;
    scalar_line(output, "provider", &value.provider.to_string(), true)?;
    scalar_line(output, "agent_type", &value.agent_type.to_string(), true)?;
    scalar_line(output, "status", &value.status.to_string(), true)?;
    scalar_line(
        output,
        "is_primary",
        if value.is_primary { "true" } else { "false" },
        true,
    )?;
    scalar_line(output, "started_at", &value.started_at, true)?;
    if let Some(ended_at) = &value.ended_at {
        scalar_line(output, "ended_at", ended_at, true)?;
    }
    scalar_line(output, "fidelity", &value.fidelity.to_string(), true)?;
    Ok(())
}

fn markdown_event(
    output: &mut MarkdownOutput,
    value: &EvidenceEventV1,
) -> EvidenceRenderResult<()> {
    scalar_line(
        output,
        "ctx_event_id",
        &value.ctx_event_id.to_string(),
        true,
    )?;
    if let Some(session_id) = value.ctx_session_id {
        scalar_line(output, "ctx_session_id", &session_id.to_string(), true)?;
    }
    scalar_line(output, "sequence", &value.sequence.to_string(), true)?;
    scalar_line(output, "event_type", &value.event_type.to_string(), true)?;
    if let Some(role) = value.role {
        scalar_line(output, "role", &role.to_string(), true)?;
    }
    scalar_line(output, "occurred_at", &value.occurred_at, true)?;
    scalar_line(output, "fidelity", &value.fidelity.to_string(), true)?;
    markdown_content(output, &safe_content(&value.content))?;
    Ok(())
}

fn markdown_result(
    output: &mut MarkdownOutput,
    value: &EvidenceSearchResultV1,
) -> EvidenceRenderResult<()> {
    scalar_line(output, "item_id", &value.item_id.to_string(), true)?;
    scalar_line(
        output,
        "result_scope",
        match value.result_scope {
            ctx_history_search::SearchResultScope::Session => "session",
            ctx_history_search::SearchResultScope::Event => "event",
        },
        true,
    )?;
    if let Some(session_id) = value.ctx_session_id {
        scalar_line(output, "ctx_session_id", &session_id.to_string(), true)?;
    }
    if let Some(event_id) = value.ctx_event_id {
        scalar_line(output, "ctx_event_id", &event_id.to_string(), true)?;
    }
    scalar_line(output, "rank", &value.rank.to_string(), true)?;
    if let Some(timestamp) = &value.timestamp {
        scalar_line(output, "timestamp", timestamp, true)?;
    }
    if let Some(title) = value
        .title
        .as_ref()
        .filter(|_| content_is_visible(&value.content))
    {
        output.push(b"### Title\n")?;
        fenced(output, "text", title)?;
    }
    for reason in &value.why_matched {
        scalar_line(output, "why_matched", reason, false)?;
    }
    markdown_content(output, &safe_content(&value.content))?;
    Ok(())
}

fn scalar_line(
    output: &mut MarkdownOutput,
    label: &str,
    value: &str,
    metadata: bool,
) -> EvidenceRenderResult<()> {
    push_format(
        output,
        format_args!("- {}: {}\n", label, scalar(value, metadata)?),
    )?;
    Ok(())
}

fn markdown_content(
    output: &mut MarkdownOutput,
    content: &crate::ContentV1,
) -> EvidenceRenderResult<()> {
    scalar_line(
        output,
        "content_state",
        match content.content_state {
            crate::ContentStateV1::Available => "available",
            crate::ContentStateV1::Truncated => "truncated",
            crate::ContentStateV1::Withheld => "withheld",
            crate::ContentStateV1::MetadataOnly => "metadata_only",
        },
        true,
    )?;
    scalar_line(
        output,
        "original_bytes",
        &content.truncation.original_bytes.to_string(),
        true,
    )?;
    scalar_line(
        output,
        "returned_bytes",
        &content.truncation.returned_bytes.to_string(),
        true,
    )?;
    scalar_line(
        output,
        "truncated",
        if content.truncation.truncated {
            "true"
        } else {
            "false"
        },
        true,
    )?;
    if let Some(reason) = content.suppression_reason {
        scalar_line(
            output,
            "suppression_reason",
            suppression_reason_name(reason),
            true,
        )?;
    }
    output.push(b"### Content\n")?;
    fenced(
        output,
        "text",
        content.text.as_deref().unwrap_or("[content withheld]"),
    )?;
    Ok(())
}

fn content_is_visible(content: &ContentV1) -> bool {
    matches!(
        content.content_state,
        ContentStateV1::Available | ContentStateV1::Truncated
    ) && content.text.is_some()
}

fn safe_content(content: &ContentV1) -> ContentV1 {
    if content_is_visible(content) {
        content.clone()
    } else {
        let mut safe = content.clone();
        safe.text = None;
        safe
    }
}

fn suppression_reason_name(reason: crate::SuppressionReasonV1) -> &'static str {
    match reason {
        crate::SuppressionReasonV1::RawPayload => "raw_payload",
        crate::SuppressionReasonV1::WithheldVisibility => "withheld_visibility",
        crate::SuppressionReasonV1::Unavailable => "unavailable",
        crate::SuppressionReasonV1::MissingProof => "missing_proof",
        crate::SuppressionReasonV1::NotApplicable => "not_applicable",
    }
}

fn scalar(value: &str, metadata: bool) -> EvidenceRenderResult<String> {
    let limit = if metadata { 4096 } else { 512 };
    if value.len() > limit {
        return Err(EvidenceRenderError::MarkdownScalarLimit);
    }
    let normalized = value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .replace('\n', "\\n")
        .replace('\t', "\\t");
    if normalized.chars().any(|c| c == '\0' || c.is_control()) {
        return Err(EvidenceRenderError::MarkdownScalarLimit);
    }
    let run = longest_backtick_run(&normalized);
    let delimiter = "`".repeat(if run == 0 { 1 } else { run + 1 });
    Ok(format!("{delimiter}{normalized}{delimiter}"))
}

fn longest_backtick_run(value: &str) -> usize {
    let mut longest = 0;
    let mut current = 0;
    for c in value.chars() {
        if c == '`' {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
}

fn push_format(
    output: &mut MarkdownOutput,
    value: std::fmt::Arguments<'_>,
) -> EvidenceRenderResult<()> {
    output.push(value.to_string().as_bytes())
}

fn fenced(output: &mut MarkdownOutput, language: &str, value: &str) -> EvidenceRenderResult<()> {
    let (_, longest) = content_stats(value);
    let fence_len = 3.max(longest + 1);
    output.push_repeated(b'`', fence_len)?;
    output.push(language.as_bytes())?;
    output.push(b"\n")?;
    push_content(output, value)?;
    output.push(b"\n")?;
    output.push_repeated(b'`', fence_len)?;
    output.push(b"\n\n")
}

fn content_stats(value: &str) -> (usize, usize) {
    let mut length = 0;
    let mut longest = 0;
    let mut run = 0;
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        let c = if c == '\r' {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            '\n'
        } else {
            c
        };
        if c == '`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
        length += match c {
            '\n' => 1,
            '\t' => 2,
            c if c.is_control() => 8,
            c => c.len_utf8(),
        };
    }
    (length, longest)
}

fn push_content(output: &mut MarkdownOutput, value: &str) -> EvidenceRenderResult<()> {
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        let c = if c == '\r' {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            '\n'
        } else {
            c
        };
        match c {
            '\n' => output.push(b"\n")?,
            '\t' => output.push(b"\\t")?,
            c if c.is_control() => push_format(output, format_args!("\\u{{{:04x}}}", c as u32))?,
            c => {
                let mut bytes = [0; 4];
                output.push(c.encode_utf8(&mut bytes).as_bytes())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BytePolicy, ContentStateV1, ContentV1, EvidenceEventV1, EvidenceRequestBindingV1,
        EvidenceSearchResultV1, EvidenceSelectorRequest, EvidenceWorkBoundsV1, Fidelity,
        OmittedCountsV1, PaginationV1, SuppressionReasonV1, TextTruncationV1,
    };
    use ctx_history_core::{EventRole, EventType};
    use ctx_history_search::{PacketOptions, SearchResultScope};
    use uuid::Uuid;

    fn make_page(content: ContentV1, format: EvidenceFormat) -> NormalizedEvidencePageV1 {
        let event = EvidenceRecordV1::Event(EvidenceEventV1 {
            schema_version: 1,
            private: true,
            share_safe: false,
            record_id: Uuid::from_u128(7),
            ctx_event_id: Uuid::from_u128(7),
            ctx_session_id: Some(Uuid::from_u128(8)),
            sequence: 3,
            event_type: EventType::Message,
            role: Some(EventRole::Assistant),
            occurred_at: "2026-08-09T01:02:03.123Z".into(),
            content,
            fidelity: Fidelity::Imported,
            provenance: None,
            session_provenance: None,
            citations: vec![],
            citation_omissions: vec![],
        });
        let normalized_item_json_bytes = serde_json::to_vec(&event).unwrap().len();
        let request = EvidenceSelectorRequest {
            selector: crate::EvidenceSelector::EventIds { event_ids: vec![] },
            byte_policy: BytePolicy {
                per_item_bytes: 4096,
                page_bytes: 1024 * 1024,
            },
            artifact_bytes: 1024 * 1024,
            format,
            ..EvidenceSelectorRequest::default()
        };
        NormalizedEvidencePageV1 {
            schema_version: 1,
            domain: "event_ids",
            ordering: "event_occurred_at_id_asc",
            records: vec![event],
            omitted: OmittedCountsV1 {
                before: 0,
                after: 0,
                exact: true,
            },
            pagination: PaginationV1 {
                continuation: None,
                has_more: false,
                offset: 0,
                page_size: 1,
                returned_items: 1,
            },
            selected_total: Some(1),
            retained_pool_total: None,
            corpus_count: None,
            search_truncation: None,
            work: EvidenceWorkBoundsV1 {
                selector_limit: 1,
                explicit_event_id_limit: crate::MAX_EVIDENCE_EVENT_IDS,
                search_candidate_limit: ctx_history_search::MAX_RESULT_LIMIT,
                per_item_bytes: 4096,
                page_bytes: 1024 * 1024,
                artifact_bytes: 1024 * 1024,
            },
            page_budget_exhausted: false,
            selector_item_json_bytes: 0,
            normalized_item_json_bytes,
            continuation: None,
            format,
            request_binding: EvidenceRequestBindingV1::from_request(&request),
            request_hash: "request-hash".into(),
            snapshot_fingerprint: "snapshot-fingerprint".into(),
        }
    }

    fn available(text: &str) -> ContentV1 {
        ContentV1 {
            content_state: ContentStateV1::Available,
            text: Some(text.into()),
            truncation: TextTruncationV1 {
                original_bytes: text.len(),
                returned_bytes: text.len(),
                truncated: false,
            },
            suppression_reason: None,
        }
    }

    #[test]
    fn jsonl_is_repeatable_parseable_and_lf_terminated_for_hostile_content() {
        let text = "prefix ```\r\n\0\u{001b}[31m💣\n``` suffix";
        let page = make_page(available(text), EvidenceFormat::Jsonl);
        let first = render_jsonl(&page).unwrap();
        let second = render_jsonl(&page).unwrap();
        assert_eq!(first, second);
        assert!(first.ends_with(b"\n"));
        assert!(first.starts_with(b"{\"schema_version\":\"ctx-evidence-bundle-jsonl-v1\""));
        for line in first
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap();
            assert!(value.get("schema_version").is_some());
            assert!(value.get("private") == Some(&serde_json::Value::Bool(true)));
            assert!(value.get("share_safe") == Some(&serde_json::Value::Bool(false)));
        }
    }

    #[test]
    fn bundle_id_binds_distinct_first_page_requests() {
        let first = make_page(available("same"), EvidenceFormat::Jsonl);
        let mut second = first.clone();
        second.request_hash = "different-canonical-request-hash".into();
        let first_value: serde_json::Value = serde_json::from_slice(
            render_jsonl(&first)
                .unwrap()
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap(),
        )
        .unwrap();
        let second_value: serde_json::Value = serde_json::from_slice(
            render_jsonl(&second)
                .unwrap()
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_ne!(first_value["bundle_id"], second_value["bundle_id"]);
    }

    #[test]
    fn markdown_lengthens_fences_and_escapes_controls_without_leaking_withheld_text() {
        let text = "```\r\nline\0\u{001b} unicode 💣";
        let page = make_page(available(text), EvidenceFormat::Markdown);
        let output = String::from_utf8(render_markdown(&page).unwrap()).unwrap();
        assert!(output.starts_with(
            "<!-- ctx-evidence-bundle: {\"schema_version\":\"ctx-evidence-bundle-markdown-v1\",\"private\":true,\"share_safe\":false} -->\n"
        ));
        assert!(output.contains("\n````text\n"));
        assert!(output.contains("\\u{0000}"));
        assert!(output.contains("\\u{001b}"));
        assert!(!output.contains("\r"));

        let hidden = make_page(
            ContentV1 {
                content_state: ContentStateV1::Withheld,
                text: Some("HOSTILE_LEAK_SENTINEL".into()),
                truncation: TextTruncationV1 {
                    original_bytes: 999,
                    returned_bytes: 0,
                    truncated: false,
                },
                suppression_reason: Some(SuppressionReasonV1::RawPayload),
            },
            EvidenceFormat::Markdown,
        );
        let hidden_output = String::from_utf8(render_markdown(&hidden).unwrap()).unwrap();
        assert!(hidden_output.contains("[content withheld]"));
        assert!(!hidden_output.contains("HOSTILE_LEAK_SENTINEL"));
    }

    #[test]
    fn artifact_preflight_fails_before_sink_write_at_exact_bound() {
        let page = make_page(available("bounded"), EvidenceFormat::Jsonl);
        let bytes = render_jsonl(&page).unwrap();
        let mut limited = page.clone();
        limited.work.artifact_bytes = 1;
        limited.request_binding.artifact_bytes = 1;
        limited.request_binding.work.artifact_bytes = 1;
        assert!(matches!(
            render_jsonl(&limited),
            Err(EvidenceRenderError::ArtifactLimit)
        ));
        let mut sink = b"sentinel".to_vec();
        assert!(matches!(
            write_evidence(&limited, &mut sink),
            Err(EvidenceRenderError::ArtifactLimit)
        ));
        assert_eq!(sink, b"sentinel");

        let mut exact = page;
        let mut bound = bytes.len();
        for _ in 0..4 {
            exact.work.artifact_bytes = bound;
            exact.request_binding.artifact_bytes = bound;
            exact.request_binding.work.artifact_bytes = bound;
            bound = render_jsonl(&exact).unwrap().len();
        }
        exact.work.artifact_bytes = bound;
        exact.request_binding.artifact_bytes = bound;
        exact.request_binding.work.artifact_bytes = bound;
        assert_eq!(render_jsonl(&exact).unwrap().len(), bound);
    }

    #[test]
    fn markdown_rejects_scalar_controls_instead_of_creating_markdown_blocks() {
        let mut page = make_page(available("safe"), EvidenceFormat::Markdown);
        if let EvidenceRecordV1::Event(event) = &mut page.records[0] {
            event.role = Some(EventRole::Assistant);
            event.occurred_at.push('\0');
        }
        page.normalized_item_json_bytes = serde_json::to_vec(&page.records[0]).unwrap().len();
        assert!(matches!(
            render_markdown(&page),
            Err(EvidenceRenderError::MarkdownScalarLimit)
        ));
    }

    #[test]
    fn withheld_result_forces_title_null_in_both_formats() {
        let hidden = ContentV1 {
            content_state: ContentStateV1::Withheld,
            text: Some("HOSTILE_LEAK_SENTINEL".into()),
            truncation: TextTruncationV1 {
                original_bytes: 21,
                returned_bytes: 21,
                truncated: false,
            },
            suppression_reason: Some(SuppressionReasonV1::WithheldVisibility),
        };
        let result = EvidenceRecordV1::SearchResult(EvidenceSearchResultV1 {
            schema_version: 1,
            private: true,
            share_safe: false,
            record_id: Uuid::from_u128(9),
            item_id: Uuid::from_u128(9),
            result_scope: SearchResultScope::Event,
            ctx_session_id: None,
            ctx_event_id: None,
            event_seq: None,
            title: Some("HOSTILE_LEAK_SENTINEL title".into()),
            content: hidden,
            rank: 1.0,
            timestamp: None,
            why_matched: vec!["safe reason".into()],
            provenance: None,
            session_provenance: None,
            citations: vec![],
            citation_omissions: vec![],
        });
        let mut page = make_page(available("unused"), EvidenceFormat::Jsonl);
        page.domain = "search_page";
        page.ordering = "search_ranked_v1";
        page.records = vec![result];
        page.selected_total = None;
        page.retained_pool_total = Some(1);
        page.corpus_count = Some(EvidenceCountV1 {
            kind: "exact",
            value: 1,
        });
        let request = EvidenceSelectorRequest {
            selector: crate::EvidenceSelector::SearchPage {
                query: "q".into(),
                terms: vec![],
                options: Box::new(PacketOptions::default()),
            },
            byte_policy: BytePolicy {
                per_item_bytes: 4096,
                page_bytes: 1024 * 1024,
            },
            artifact_bytes: 1024 * 1024,
            ..EvidenceSelectorRequest::default()
        };
        page.request_binding = EvidenceRequestBindingV1::from_request(&request);
        page.normalized_item_json_bytes = serde_json::to_vec(&page.records[0]).unwrap().len();
        let jsonl = render_jsonl(&page).unwrap();
        let result_line = jsonl
            .split(|byte| *byte == b'\n')
            .filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
            .find(|value| {
                value.get("record_type") == Some(&serde_json::Value::String("result".into()))
            })
            .unwrap();
        assert_eq!(result_line.get("title"), Some(&serde_json::Value::Null));
        assert_eq!(result_line["content"]["text"], serde_json::Value::Null);
        assert!(!jsonl
            .windows(b"HOSTILE_LEAK_SENTINEL".len())
            .any(|window| { window == b"HOSTILE_LEAK_SENTINEL" }));

        page.format = EvidenceFormat::Markdown;
        page.request_binding.format = EvidenceFormat::Markdown;
        let markdown = render_markdown(&page).unwrap();
        assert!(!markdown
            .windows(b"HOSTILE_LEAK_SENTINEL".len())
            .any(|window| window == b"HOSTILE_LEAK_SENTINEL"));
        assert!(String::from_utf8_lossy(&markdown).contains("\"title\":null"));
    }

    #[test]
    fn markdown_expansion_hits_cap_without_writing_to_sink() {
        let mut page = make_page(available(&"`".repeat(20_000)), EvidenceFormat::Markdown);
        let full = render_markdown(&page).unwrap();
        page.work.artifact_bytes = full.len() / 2;
        page.request_binding.artifact_bytes = full.len() / 2;
        page.request_binding.work.artifact_bytes = full.len() / 2;
        let mut sink = b"sentinel".to_vec();
        assert!(matches!(
            write_evidence(&page, &mut sink),
            Err(EvidenceRenderError::ArtifactLimit)
        ));
        assert_eq!(sink, b"sentinel");
    }

    #[test]
    fn invariant_mutations_fail_before_any_sink_write() {
        let page = make_page(available("safe"), EvidenceFormat::Jsonl);
        let mut cases = Vec::new();

        let mut returned = page.clone();
        returned.pagination.returned_items += 1;
        cases.push(returned);

        let mut has_more = page.clone();
        has_more.pagination.has_more = true;
        cases.push(has_more);

        let mut token = page.clone();
        token.pagination.continuation = Some("token".into());
        cases.push(token);

        let mut accounting = page.clone();
        accounting.normalized_item_json_bytes += 1;
        cases.push(accounting);

        let mut selector_accounting = page.clone();
        selector_accounting.selector_item_json_bytes = selector_accounting.work.page_bytes + 1;
        cases.push(selector_accounting);

        let mut counts = page.clone();
        counts.selected_total = Some(2);
        cases.push(counts);

        let mut work = page.clone();
        work.work.page_bytes += 1;
        cases.push(work);

        let mut request = page.clone();
        request.request_binding.format = EvidenceFormat::Markdown;
        cases.push(request);

        for case in cases {
            let mut sink = b"sentinel".to_vec();
            assert!(matches!(
                write_evidence(&case, &mut sink),
                Err(EvidenceRenderError::Invariant)
            ));
            assert_eq!(sink, b"sentinel");
        }
    }
}
