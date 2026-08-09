//! Closed deterministic evidence projection boundary. This module has no
//! store, clock, filesystem, environment, or open metadata input.
use super::*;
use chrono::SecondsFormat;

pub const EVIDENCE_PROJECTION_SCHEMA_VERSION: u32 = 1;
const MAX_METADATA_BYTES: usize = 4096;
const MAX_LABEL_BYTES: usize = 512;
const MAX_NEXT_ARGUMENTS_BYTES: usize = 128 * 1024;
const MAX_CITATIONS: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum EvidenceError {
    #[error("evidence field exceeds its byte limit")]
    StringLimit,
    #[error("evidence array exceeds its item limit")]
    ArrayLimit,
    #[error("normalized evidence record exceeds its byte limit")]
    RecordLimit,
    #[error("evidence serialization failed")]
    Serialization,
    #[error("evidence continuation arguments exceed their byte limit")]
    NextArgumentsLimit,
    #[error("normalized evidence byte accounting overflow")]
    ArithmeticOverflow,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NormalizedEvidencePageV1 {
    pub schema_version: u32,
    pub domain: &'static str,
    pub ordering: &'static str,
    pub records: Vec<EvidenceRecordV1>,
    pub omitted: OmittedCountsV1,
    pub pagination: PaginationV1,
    pub selector_item_json_bytes: usize,
    pub normalized_item_json_bytes: usize,
    pub continuation: Option<EvidenceContinuationV1>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
pub enum EvidenceRecordV1 {
    Session(EvidenceSessionV1),
    Event(EvidenceEventV1),
    SearchResult(EvidenceSearchResultV1),
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceSessionV1 {
    pub schema_version: u32,
    pub private: bool,
    pub share_safe: bool,
    pub record_id: Uuid,
    pub ctx_session_id: Uuid,
    pub provider: CaptureProvider,
    pub agent_type: AgentType,
    pub status: SessionStatus,
    pub is_primary: bool,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub fidelity: Fidelity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ProvenanceV1>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceEventV1 {
    pub schema_version: u32,
    pub private: bool,
    pub share_safe: bool,
    pub record_id: Uuid,
    pub ctx_event_id: Uuid,
    pub ctx_session_id: Option<Uuid>,
    pub sequence: u64,
    pub event_type: EventType,
    pub role: Option<EventRole>,
    pub occurred_at: String,
    pub content: ContentV1,
    pub fidelity: Fidelity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ProvenanceV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_provenance: Option<ProvenanceV1>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub citations: Vec<CitationV1>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub citation_omissions: Vec<CitationOmissionV1>,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EvidenceSearchResultV1 {
    pub schema_version: u32,
    pub private: bool,
    pub share_safe: bool,
    pub record_id: Uuid,
    pub item_id: Uuid,
    pub result_scope: SearchResultScope,
    pub ctx_session_id: Option<Uuid>,
    pub ctx_event_id: Option<Uuid>,
    pub event_seq: Option<u64>,
    pub title: Option<String>,
    pub content: ContentV1,
    pub rank: f32,
    pub timestamp: Option<String>,
    pub why_matched: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ProvenanceV1>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_provenance: Option<ProvenanceV1>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub citations: Vec<CitationV1>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub citation_omissions: Vec<CitationOmissionV1>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentV1 {
    pub content_state: ContentStateV1,
    pub text: Option<String>,
    pub truncation: TextTruncationV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suppression_reason: Option<SuppressionReasonV1>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStateV1 {
    Available,
    Truncated,
    Withheld,
    MetadataOnly,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SuppressionReasonV1 {
    RawPayload,
    WithheldVisibility,
    Unavailable,
    MissingProof,
    NotApplicable,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAvailabilityV1 {
    Live,
    Missing,
    Deleted,
    Withheld,
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProvenanceV1 {
    pub capture_source_id: Uuid,
    pub availability: SourceAvailabilityV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<CaptureProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<CaptureSourceKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceCitationTypeV1 {
    HistoryRecord,
    Session,
    Run,
    Event,
    VcsChange,
    Artifact,
    Summary,
    File,
    Source,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CitationV1 {
    pub citation_type: EvidenceCitationTypeV1,
    pub target_id: Uuid,
    pub target_item_type: &'static str,
    pub label: String,
    pub time: String,
    pub ctx_session_id: Option<Uuid>,
    pub ctx_event_id: Option<Uuid>,
    pub event_seq: Option<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CitationOmissionReasonV1 {
    MissingTarget,
    DeletedTarget,
    AmbiguousTarget,
    WithheldTarget,
    UnsupportedType,
    MissingProof,
    OverLimit,
    InvalidProvenance,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CitationOmissionV1 {
    pub reason: CitationOmissionReasonV1,
    pub count: usize,
}

pub(crate) fn normalize_evidence(
    page: EvidenceSelectionPageV1,
    fields: FieldSet,
    cap: usize,
) -> std::result::Result<NormalizedEvidencePageV1, EvidenceError> {
    for identity in &page.search_source_identity {
        for value in [
            identity.history_source.as_deref(),
            identity.provider_key.as_deref(),
            identity.source_id.as_deref(),
            identity.source_format.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if value.len() > MAX_METADATA_BYTES {
                return Err(EvidenceError::StringLimit);
            }
        }
    }
    let sources = page
        .source_lookup
        .iter()
        .map(|s| (s.capture_source_id, s))
        .collect::<HashMap<_, _>>();
    let refs = page
        .event_source_refs
        .iter()
        .map(|r| (r.ctx_event_id, r))
        .collect::<HashMap<_, _>>();
    let mut records = Vec::new();
    if let Some(s) = &page.session {
        records.push(EvidenceRecordV1::Session(session(s, fields, &sources)));
    }
    for item in &page.items {
        records.push(match item {
            EvidenceItemV1::Event { value } => EvidenceRecordV1::Event(event(
                value,
                fields,
                cap,
                refs.get(&event_projection_id(value)).copied(),
                &sources,
            )),
            EvidenceItemV1::Result { value } => {
                EvidenceRecordV1::SearchResult(result(value, fields, cap, &page, &sources)?)
            }
            EvidenceItemV1::Session { value } => {
                EvidenceRecordV1::Session(session(value, fields, &sources))
            }
        });
    }
    let mut bytes = 0usize;
    for r in &records {
        let n = serde_json::to_vec(r)
            .map_err(|_| EvidenceError::Serialization)?
            .len();
        if n > 2 * 1024 * 1024 {
            return Err(EvidenceError::RecordLimit);
        }
        bytes = bytes
            .checked_add(n)
            .ok_or(EvidenceError::ArithmeticOverflow)?;
    }
    if let Some(continuation) = &page.continuation {
        let bytes = serde_json::to_vec(&continuation.next_arguments)
            .map_err(|_| EvidenceError::Serialization)?
            .len();
        if bytes > MAX_NEXT_ARGUMENTS_BYTES {
            return Err(EvidenceError::NextArgumentsLimit);
        }
    }
    Ok(NormalizedEvidencePageV1 {
        schema_version: 1,
        domain: page.domain,
        ordering: page.ordering,
        records,
        omitted: page.omitted,
        pagination: page.pagination,
        selector_item_json_bytes: page.bytes.item_json_bytes,
        normalized_item_json_bytes: bytes,
        continuation: page.continuation,
    })
}
fn stamp(v: DateTime<Utc>) -> String {
    v.to_rfc3339_opts(SecondsFormat::Millis, true)
}
fn provenance(
    id: Option<Uuid>,
    fields: FieldSet,
    map: &HashMap<Uuid, &EvidenceSourceLookupV1>,
) -> Option<ProvenanceV1> {
    if fields == FieldSet::Compact {
        return None;
    }
    let id = id?;
    let row = map.get(&id)?;
    Some(match (&row.source, row.visibility) {
        (Some(_), Some(Visibility::Withheld)) => ProvenanceV1 {
            capture_source_id: id,
            availability: SourceAvailabilityV1::Withheld,
            provider: None,
            kind: None,
            started_at: None,
            ended_at: None,
        },
        (Some(s), _) => ProvenanceV1 {
            capture_source_id: id,
            availability: SourceAvailabilityV1::Live,
            provider: Some(s.provider),
            kind: Some(s.kind),
            started_at: Some(stamp(s.started_at)),
            ended_at: s.ended_at.map(stamp),
        },
        (None, _) => ProvenanceV1 {
            capture_source_id: id,
            availability: SourceAvailabilityV1::Missing,
            provider: None,
            kind: None,
            started_at: None,
            ended_at: None,
        },
    })
}
fn session(
    v: &SessionProjectionV1,
    fields: FieldSet,
    map: &HashMap<Uuid, &EvidenceSourceLookupV1>,
) -> EvidenceSessionV1 {
    let (id, p, a, s, primary, start, end, fidelity, src) = match v {
        SessionProjectionV1::Full(x) => (
            x.ctx_session_id,
            x.provider,
            x.agent_type,
            x.status,
            x.is_primary,
            x.started_at,
            x.ended_at,
            x.fidelity,
            x.capture_source_id,
        ),
        SessionProjectionV1::Compact(x) => (
            x.ctx_session_id,
            x.provider,
            x.agent_type,
            x.status,
            x.is_primary,
            x.started_at,
            x.ended_at,
            x.fidelity,
            None,
        ),
    };
    EvidenceSessionV1 {
        schema_version: 1,
        private: true,
        share_safe: false,
        record_id: id,
        ctx_session_id: id,
        provider: p,
        agent_type: a,
        status: s,
        is_primary: primary,
        started_at: stamp(start),
        ended_at: end.map(stamp),
        fidelity,
        provenance: provenance(src, fields, map),
    }
}
fn withheld(reason: SuppressionReasonV1) -> ContentV1 {
    ContentV1 {
        content_state: ContentStateV1::Withheld,
        text: None,
        truncation: TextTruncationV1 {
            original_bytes: 0,
            returned_bytes: 0,
            truncated: false,
        },
        suppression_reason: Some(reason),
    }
}
fn available(text: &str, cap: usize) -> ContentV1 {
    let (text, t) = truncate_utf8_bytes(text, cap);
    ContentV1 {
        content_state: if t.truncated {
            ContentStateV1::Truncated
        } else {
            ContentStateV1::Available
        },
        text: Some(text),
        truncation: t,
        suppression_reason: None,
    }
}
fn metadata_only() -> ContentV1 {
    ContentV1 {
        content_state: ContentStateV1::MetadataOnly,
        text: None,
        truncation: TextTruncationV1 {
            original_bytes: 0,
            returned_bytes: 0,
            truncated: false,
        },
        suppression_reason: None,
    }
}
fn event(
    v: &EventProjectionV1,
    fields: FieldSet,
    cap: usize,
    refs: Option<&EvidenceEventSourceRefsV1>,
    map: &HashMap<Uuid, &EvidenceSourceLookupV1>,
) -> EvidenceEventV1 {
    let (id, sid, seq, ty, role, time, text, fidelity, redaction, visibility) = match v {
        EventProjectionV1::Full(x) => (
            x.ctx_event_id,
            x.ctx_session_id,
            x.sequence,
            x.event_type,
            x.role,
            x.occurred_at,
            x.text.as_str(),
            x.fidelity,
            Some(x.redaction_state),
            Some(x.visibility),
        ),
        EventProjectionV1::Compact(x) => (
            x.ctx_event_id,
            x.ctx_session_id,
            x.seq,
            x.event_type,
            x.role,
            x.occurred_at,
            x.text.as_str(),
            x.fidelity,
            Some(x.redaction_state),
            Some(x.visibility),
        ),
    };
    let content = match (redaction, visibility) {
        (Some(RedactionState::Raw), _) => withheld(SuppressionReasonV1::RawPayload),
        (Some(RedactionState::Withheld), _) | (_, Some(Visibility::Withheld)) => {
            withheld(SuppressionReasonV1::WithheldVisibility)
        }
        (Some(RedactionState::LocalPreview | RedactionState::Redacted), Some(_))
            if text.is_empty() =>
        {
            metadata_only()
        }
        (Some(RedactionState::LocalPreview | RedactionState::Redacted), Some(_)) => {
            available(text, cap)
        }
        _ => withheld(SuppressionReasonV1::MissingProof),
    };
    EvidenceEventV1 {
        schema_version: 1,
        private: true,
        share_safe: false,
        record_id: id,
        ctx_event_id: id,
        ctx_session_id: sid,
        sequence: seq,
        event_type: ty,
        role,
        occurred_at: stamp(time),
        content,
        fidelity,
        provenance: refs.and_then(|r| provenance(r.event_capture_source_id, fields, map)),
        session_provenance: refs.and_then(|r| provenance(r.session_capture_source_id, fields, map)),
        citations: vec![],
        citation_omissions: vec![],
    }
}
fn result(
    v: &SearchResultProjectionV1,
    fields: FieldSet,
    cap: usize,
    page: &EvidenceSelectionPageV1,
    map: &HashMap<Uuid, &EvidenceSourceLookupV1>,
) -> std::result::Result<EvidenceSearchResultV1, EvidenceError> {
    let (id, sid, eid, seq, title, snippet, rank, scope, time, reasons, visibility, citations) =
        match v {
            SearchResultProjectionV1::Full(x) => (
                x.item_id,
                x.ctx_session_id,
                x.ctx_event_id,
                x.event_seq,
                x.title.as_str(),
                x.snippet.as_str(),
                x.rank,
                x.result_scope,
                x.timestamp,
                x.why_matched.as_slice(),
                x.visibility,
                Some(x.citations.as_slice()),
            ),
            SearchResultProjectionV1::Compact(x) => (
                x.item_id,
                x.ctx_session_id,
                x.ctx_event_id,
                x.event_seq,
                x.title.as_str(),
                x.snippet.as_str(),
                x.rank,
                x.result_scope,
                x.timestamp,
                x.why_matched.as_slice(),
                x.visibility,
                None,
            ),
        };
    if reasons.len() > 32 {
        return Err(EvidenceError::ArrayLimit);
    }
    for s in reasons {
        if s.len() > MAX_LABEL_BYTES {
            return Err(EvidenceError::StringLimit);
        }
    }
    let proof = page
        .search_source_refs
        .iter()
        .find(|r| r.result_id == id)
        .and_then(|r| eid.and_then(|e| r.events.iter().find(|x| x.ctx_event_id == e)));
    let safe = proof.is_some_and(|p| {
        matches!(
            p.redaction_state,
            RedactionState::LocalPreview | RedactionState::Redacted
        ) && p.visibility != Visibility::Withheld
            && visibility != Visibility::Withheld
    });
    let (content, title) = if safe {
        if title.len() > MAX_LABEL_BYTES {
            return Err(EvidenceError::StringLimit);
        }
        (available(snippet, cap), Some(title.to_owned()))
    } else {
        let withheld_visibility = visibility == Visibility::Withheld
            || proof.is_some_and(|p| p.visibility == Visibility::Withheld);
        (
            withheld(if withheld_visibility {
                SuppressionReasonV1::WithheldVisibility
            } else {
                SuppressionReasonV1::MissingProof
            }),
            None,
        )
    };
    let (citations, omissions) = if fields == FieldSet::Compact {
        (vec![], vec![])
    } else {
        citations_for(citations.unwrap_or(&[]), sid, eid, page)?
    };
    let session_provenance = page
        .search_source_refs
        .iter()
        .find(|r| r.result_id == id)
        .and_then(|r| {
            let event_source = proof.and_then(|p| p.event_capture_source_id);
            (r.session_capture_source_id != event_source)
                .then(|| provenance(r.session_capture_source_id, fields, map))
                .flatten()
        });
    Ok(EvidenceSearchResultV1 {
        schema_version: 1,
        private: true,
        share_safe: false,
        record_id: id,
        item_id: id,
        result_scope: scope,
        ctx_session_id: sid,
        ctx_event_id: eid,
        event_seq: seq,
        title,
        content,
        rank,
        timestamp: time.map(stamp),
        why_matched: reasons.to_vec(),
        provenance: proof.and_then(|p| provenance(p.event_capture_source_id, fields, map)),
        session_provenance,
        citations,
        citation_omissions: omissions,
    })
}
fn citations_for(
    input: &[SearchCitationV1],
    sid: Option<Uuid>,
    eid: Option<Uuid>,
    page: &EvidenceSelectionPageV1,
) -> std::result::Result<(Vec<CitationV1>, Vec<CitationOmissionV1>), EvidenceError> {
    if input.len() > MAX_CITATIONS {
        return Err(EvidenceError::ArrayLimit);
    }
    let mut out = vec![];
    let mut seen = BTreeSet::new();
    let mut omitted = 0;
    for c in input {
        let event_ok = c.citation_type == ContextCitationType::Event
            && Some(c.id) == eid
            && page
                .search_source_refs
                .iter()
                .any(|r| r.events.iter().any(|e| e.ctx_event_id == c.id));
        let session_ok = c.citation_type == ContextCitationType::Session && Some(c.id) == sid;
        if !(event_ok || session_ok) {
            omitted += 1;
            continue;
        }
        let (ty, item, label) = if event_ok {
            (EvidenceCitationTypeV1::Event, "event", "event evidence")
        } else {
            (
                EvidenceCitationTypeV1::Session,
                "session",
                "session evidence",
            )
        };
        if !seen.insert((ty, c.id, stamp(c.time))) {
            continue;
        }
        out.push(CitationV1 {
            citation_type: ty,
            target_id: c.id,
            target_item_type: item,
            label: label.into(),
            time: stamp(c.time),
            ctx_session_id: if session_ok {
                Some(c.id)
            } else {
                c.ctx_session_id
            },
            ctx_event_id: if event_ok { Some(c.id) } else { None },
            event_seq: c.event_seq,
        });
    }
    out.sort_by_key(|c| (c.citation_type, c.target_id, c.time.clone()));
    Ok((
        out,
        if omitted == 0 {
            vec![]
        } else {
            vec![CitationOmissionV1 {
                reason: CitationOmissionReasonV1::MissingProof,
                count: omitted,
            }]
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_state_does_not_treat_marker_text_as_control_data() {
        let marker = "[content withheld] HOSTILE_LEAK_SENTINEL";
        let shown = available(marker, 4096);
        assert_eq!(shown.content_state, ContentStateV1::Available);
        assert_eq!(shown.text.as_deref(), Some(marker));
        assert!(shown.suppression_reason.is_none());

        for reason in [
            SuppressionReasonV1::RawPayload,
            SuppressionReasonV1::WithheldVisibility,
            SuppressionReasonV1::Unavailable,
            SuppressionReasonV1::MissingProof,
        ] {
            let hidden = withheld(reason);
            let json = serde_json::to_string(&hidden).unwrap();
            assert_eq!(hidden.text, None);
            assert!(!json.contains("HOSTILE_LEAK_SENTINEL"));
        }
    }

    #[test]
    fn timestamps_are_millisecond_utc_and_replay_stable() {
        let time = DateTime::parse_from_rfc3339("2026-08-09T01:02:03.123456+02:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(stamp(time), "2026-08-08T23:02:03.123Z");
        assert_eq!(stamp(time), stamp(time));
    }

    #[test]
    fn content_caps_are_utf8_safe_and_exact() {
        let value = available("a💣b", 5);
        assert_eq!(value.text.as_deref(), Some("a…"));
        assert_eq!(value.content_state, ContentStateV1::Truncated);
        assert_eq!(value.truncation.original_bytes, 6);
        assert_eq!(value.truncation.returned_bytes, 4);
    }

    #[test]
    fn errors_are_typed_and_do_not_echo_hostile_input() {
        for error in [EvidenceError::StringLimit, EvidenceError::ArrayLimit] {
            let rendered = error.to_string();
            assert!(!rendered.contains("HOSTILE_LEAK_SENTINEL"));
            assert!(rendered.len() < 128);
        }
    }

    const RECORD_LIMIT: usize = 2 * 1024 * 1024;

    #[derive(Clone, Copy)]
    enum ExpectedError {
        String,
        Array,
        Record,
        NextArguments,
    }

    fn assert_expected_error(error: EvidenceError, expected: ExpectedError) {
        let is_expected = matches!(
            (&error, expected),
            (EvidenceError::StringLimit, ExpectedError::String)
                | (EvidenceError::ArrayLimit, ExpectedError::Array)
                | (EvidenceError::RecordLimit, ExpectedError::Record)
                | (
                    EvidenceError::NextArgumentsLimit,
                    ExpectedError::NextArguments
                )
        );
        assert!(is_expected, "unexpected evidence error: {error:?}");
        let rendered = error.to_string();
        assert!(!rendered.contains("HOSTILE_LEAK_SENTINEL"));
        assert!(rendered.len() < 128);
        assert_eq!(
            rendered,
            match expected {
                ExpectedError::String => "evidence field exceeds its byte limit",
                ExpectedError::Array => "evidence array exceeds its item limit",
                ExpectedError::Record => "normalized evidence record exceeds its byte limit",
                ExpectedError::NextArguments => {
                    "evidence continuation arguments exceed their byte limit"
                }
            }
        );
    }

    fn hostile_text(len: usize) -> String {
        std::iter::repeat("HOSTILE_LEAK_SENTINEL")
            .flat_map(str::bytes)
            .map(char::from)
            .take(len)
            .collect()
    }

    fn base_page(
        items: Vec<EvidenceItemV1>,
        search_source_identity: Vec<EvidenceSearchSourceIdentityV1>,
        continuation: Option<EvidenceContinuationV1>,
    ) -> EvidenceSelectionPageV1 {
        EvidenceSelectionPageV1 {
            schema_version: 1,
            domain: "test",
            ordering: "stable",
            session: None,
            source_lookup: vec![],
            event_source_refs: vec![],
            search_source_identity,
            search_source_refs: vec![],
            items,
            selected_total: None,
            retained_pool_total: None,
            corpus_count: None,
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
                returned_items: 0,
            },
            bytes: PageBytesV1 {
                policy: BytePolicy::default(),
                item_json_bytes: 0,
                page_budget_exhausted: false,
                item_text_truncated: 0,
            },
            work: EvidenceWorkBoundsV1 {
                selector_limit: 1,
                explicit_event_id_limit: 1,
                search_candidate_limit: 1,
                per_item_bytes: 4096,
                page_bytes: 4096,
                artifact_bytes: 4096,
            },
            search_truncation: None,
            continuation,
            format: EvidenceFormat::Jsonl,
        }
    }

    fn proofed_result(
        title: String,
        why_matched: Vec<String>,
        citations: Vec<SearchCitationV1>,
    ) -> EvidenceItemV1 {
        let session_id = Uuid::from_u128(91_001);
        let event_id = Uuid::from_u128(91_002);
        EvidenceItemV1::Result {
            value: SearchResultProjectionV1::Full(Box::new(SearchResultFullV1 {
                item_id: event_id,
                item_type: "event".into(),
                ctx_session_id: Some(session_id),
                ctx_event_id: Some(event_id),
                session_id: Some(session_id),
                event_id: Some(event_id),
                event_seq: Some(1),
                title,
                snippet: "HOSTILE_LEAK_SENTINEL snippet".into(),
                snippet_truncation: TextTruncationV1 {
                    original_bytes: 31,
                    returned_bytes: 31,
                    truncated: false,
                },
                rank: 1.0,
                result_scope: SearchResultScope::Event,
                more_matches_in_session: 0,
                session_importance: 0.0,
                provider: Some(CaptureProvider::Codex),
                provider_session_id: None,
                history_source: None,
                history_source_plugin: None,
                provider_key: None,
                source_id: None,
                source_format: None,
                timestamp: Some(DateTime::<Utc>::from_timestamp(0, 0).unwrap()),
                cwd: None,
                source_path: None,
                source_exists: None,
                source_cursor: None,
                cursor: None,
                why_matched,
                citations,
                links: ContextLinks::default(),
                suggested_next_commands: vec![],
                visibility: Visibility::LocalOnly,
            })),
        }
    }

    fn proofed_result_page(item: EvidenceItemV1) -> EvidenceSelectionPageV1 {
        let result_id = match &item {
            EvidenceItemV1::Result { value } => match value {
                SearchResultProjectionV1::Full(value) => value.item_id,
                SearchResultProjectionV1::Compact(value) => value.item_id,
            },
            _ => panic!("expected a search result"),
        };
        let event_id = Uuid::from_u128(91_002);
        let mut page = base_page(vec![item], vec![], None);
        page.search_source_refs = vec![EvidenceSearchSourceRefsV1 {
            result_id,
            session_capture_source_id: None,
            events: vec![EvidenceEventSourceRefsV1 {
                ctx_event_id: event_id,
                redaction_state: RedactionState::LocalPreview,
                visibility: Visibility::LocalOnly,
                event_capture_source_id: None,
                session_capture_source_id: None,
            }],
        }];
        page
    }

    fn title_page(len: usize) -> EvidenceSelectionPageV1 {
        proofed_result_page(proofed_result(
            hostile_text(len),
            vec!["why".into()],
            vec![],
        ))
    }

    fn why_page(len: usize) -> EvidenceSelectionPageV1 {
        proofed_result_page(proofed_result(
            "title".into(),
            vec![hostile_text(len)],
            vec![],
        ))
    }

    #[derive(Clone, Copy)]
    enum IdentityField {
        HistorySource,
        ProviderKey,
        SourceId,
        SourceFormat,
    }

    fn identity_page(field: IdentityField, len: usize) -> EvidenceSelectionPageV1 {
        let value = Some(hostile_text(len));
        let identity = EvidenceSearchSourceIdentityV1 {
            result_id: Uuid::from_u128(91_003),
            history_source: matches!(field, IdentityField::HistorySource)
                .then(|| value.clone())
                .flatten(),
            provider_key: matches!(field, IdentityField::ProviderKey)
                .then(|| value.clone())
                .flatten(),
            source_id: matches!(field, IdentityField::SourceId)
                .then(|| value.clone())
                .flatten(),
            source_format: matches!(field, IdentityField::SourceFormat)
                .then(|| value.clone())
                .flatten(),
        };
        base_page(vec![], vec![identity], None)
    }

    fn why_array_page(count: usize) -> EvidenceSelectionPageV1 {
        proofed_result_page(proofed_result(
            "title".into(),
            (0..count).map(|_| "why".into()).collect(),
            vec![],
        ))
    }

    fn citation() -> SearchCitationV1 {
        SearchCitationV1 {
            citation_type: ContextCitationType::Event,
            id: Uuid::from_u128(91_002),
            item_id: Uuid::from_u128(91_002),
            item_type: "event",
            label: "event evidence".into(),
            time: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            provider: Some(CaptureProvider::Codex),
            ctx_session_id: Some(Uuid::from_u128(91_001)),
            session_id: Some(Uuid::from_u128(91_001)),
            ctx_event_id: Some(Uuid::from_u128(91_002)),
            event_seq: Some(1),
            source_path: None,
            source_exists: None,
            source_cursor: None,
            cursor: None,
        }
    }

    fn citation_array_page(count: usize) -> EvidenceSelectionPageV1 {
        proofed_result_page(proofed_result(
            "title".into(),
            vec!["why".into()],
            (0..count).map(|_| citation()).collect(),
        ))
    }

    fn event_item(text_len: usize) -> EvidenceItemV1 {
        EvidenceItemV1::Event {
            value: EventProjectionV1::Compact(EventCompactV1 {
                ctx_event_id: Uuid::from_u128(91_004),
                ctx_session_id: Some(Uuid::from_u128(91_005)),
                seq: 1,
                event_type: EventType::Message,
                role: Some(EventRole::Assistant),
                occurred_at: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
                text: "x".repeat(text_len),
                text_truncation: TextTruncationV1 {
                    original_bytes: text_len,
                    returned_bytes: text_len,
                    truncated: false,
                },
                redaction_state: RedactionState::LocalPreview,
                visibility: Visibility::LocalOnly,
                fidelity: Fidelity::Imported,
            }),
        }
    }

    fn event_record_bytes(text_len: usize) -> usize {
        let EvidenceItemV1::Event { value } = event_item(text_len) else {
            unreachable!()
        };
        let value = match value {
            EventProjectionV1::Compact(value) => value,
            EventProjectionV1::Full(_) => unreachable!(),
        };
        let normalized = event(
            &EventProjectionV1::Compact(value),
            FieldSet::Full,
            usize::MAX,
            None,
            &HashMap::new(),
        );
        serde_json::to_vec(&EvidenceRecordV1::Event(normalized))
            .unwrap()
            .len()
    }

    fn record_page(text_len: usize) -> EvidenceSelectionPageV1 {
        base_page(vec![event_item(text_len)], vec![], None)
    }

    fn continuation_for_bytes(target: usize) -> EvidenceContinuationV1 {
        let request = |query: &str| EvidenceSelectorRequest {
            selector: EvidenceSelector::SearchPage {
                query: query.into(),
                terms: vec![],
                options: Box::default(),
            },
            ..EvidenceSelectorRequest::default()
        };
        let baseline = serde_json::to_vec(&request("")).unwrap().len();
        let continuation = EvidenceContinuationV1 {
            token: "token".into(),
            next_arguments: request(&"q".repeat(target - baseline)),
        };
        assert_eq!(
            serde_json::to_vec(&continuation.next_arguments)
                .unwrap()
                .len(),
            target
        );
        continuation
    }

    #[test]
    fn normalizer_boundary_limits_are_exact_typed_and_nonleaky() {
        struct Case {
            name: &'static str,
            at: usize,
            over: usize,
            build: fn(usize) -> EvidenceSelectionPageV1,
            expected: ExpectedError,
        }

        let string_cases = [
            Case {
                name: "title",
                at: 512,
                over: 513,
                build: title_page,
                expected: ExpectedError::String,
            },
            Case {
                name: "why",
                at: 512,
                over: 513,
                build: why_page,
                expected: ExpectedError::String,
            },
        ];
        for case in string_cases {
            assert!(
                normalize_evidence((case.build)(case.at), FieldSet::Full, 4096).is_ok(),
                "{} must accept its limit",
                case.name
            );
            let error = normalize_evidence((case.build)(case.over), FieldSet::Full, 4096)
                .expect_err(case.name);
            assert_expected_error(error, case.expected);
        }

        for field in [
            IdentityField::HistorySource,
            IdentityField::ProviderKey,
            IdentityField::SourceId,
            IdentityField::SourceFormat,
        ] {
            assert!(normalize_evidence(identity_page(field, 4096), FieldSet::Full, 4096).is_ok());
            let error = normalize_evidence(identity_page(field, 4097), FieldSet::Full, 4096)
                .expect_err("identity limit");
            assert_expected_error(error, ExpectedError::String);
        }

        let array_cases = [
            (
                "why array",
                why_array_page as fn(usize) -> _,
                ExpectedError::Array,
            ),
            (
                "citation array",
                citation_array_page as fn(usize) -> _,
                ExpectedError::Array,
            ),
        ];
        for (name, build, expected) in array_cases {
            assert!(
                normalize_evidence(build(32), FieldSet::Full, 4096).is_ok(),
                "{name}"
            );
            let error = normalize_evidence(build(33), FieldSet::Full, 4096).expect_err(name);
            assert_expected_error(error, expected);
        }

        let mut low = 0;
        let mut high = RECORD_LIMIT;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if event_record_bytes(middle) <= RECORD_LIMIT {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        assert_eq!(event_record_bytes(low), RECORD_LIMIT);
        assert!(event_record_bytes(low + 1) > RECORD_LIMIT);

        let at = continuation_for_bytes(128 * 1024);
        let over = continuation_for_bytes(128 * 1024 + 1);
        let byte_cases = [
            (
                "record limit",
                record_page(low),
                record_page(low + 1),
                usize::MAX,
                ExpectedError::Record,
            ),
            (
                "continuation arguments limit",
                base_page(vec![], vec![], Some(at)),
                base_page(vec![], vec![], Some(over)),
                4096,
                ExpectedError::NextArguments,
            ),
        ];
        for (name, at_page, over_page, cap, expected) in byte_cases {
            assert!(
                normalize_evidence(at_page, FieldSet::Full, cap).is_ok(),
                "{name}"
            );
            let error = normalize_evidence(over_page, FieldSet::Full, cap).expect_err(name);
            assert_expected_error(error, expected);
        }
    }
}
