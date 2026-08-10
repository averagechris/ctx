use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    AgentType, ArtifactKind, CaptureProvider, EventRole, EventType, Fidelity, RedactionState,
    SessionStatus,
};

pub const PROVIDER_CAPTURE_ENVELOPE_SCHEMA_VERSION: u32 = 1;
pub const PROVIDER_SUPPORT_MATRIX_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMatrixPriority {
    P0,
    P1,
    #[default]
    P2,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSupportStatus {
    LocalImport,
    LocalImportWhenSupported,
    SupportedLive,
    SupportedImport,
    SupportedWrapper,
    NormalizedImportOnly,
    FixtureOnly,
    DetectedUnsupported,
    #[default]
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum ProviderId {
    Codex,
    #[serde(alias = "claude")]
    ClaudeCode,
    ClaudeCliCrp,
    Pi,
    OpenCode,
    Cursor,
    AntigravityCli,
    GeminiCli,
    Gemini,
    CopilotCli,
    Copilot,
    FactoryAiDroid,
    FactoryDroid,
    DroidFactoryAi,
    #[serde(rename = "openclaw", alias = "open_claw")]
    OpenClaw,
    Hermes,
    #[serde(rename = "nanoclaw", alias = "nano_claw")]
    NanoClaw,
    #[serde(rename = "astrbot", alias = "astr_bot")]
    AstrBot,
    Goose,
    #[serde(rename = "openhands")]
    OpenHands,
    Cagent,
    Qwen,
    Mistral,
    Kimi,
    Aider,
    ClineRoo,
    ContinueCody,
    Auggie,
    Junie,
    Kilo,
    SweAgent,
}

impl ProviderId {
    pub const ALL: [Self; 31] = [
        Self::Codex,
        Self::ClaudeCode,
        Self::ClaudeCliCrp,
        Self::Pi,
        Self::OpenCode,
        Self::Cursor,
        Self::AntigravityCli,
        Self::GeminiCli,
        Self::Gemini,
        Self::CopilotCli,
        Self::Copilot,
        Self::FactoryAiDroid,
        Self::FactoryDroid,
        Self::DroidFactoryAi,
        Self::OpenClaw,
        Self::Hermes,
        Self::NanoClaw,
        Self::AstrBot,
        Self::Goose,
        Self::OpenHands,
        Self::Cagent,
        Self::Qwen,
        Self::Mistral,
        Self::Kimi,
        Self::Aider,
        Self::ClineRoo,
        Self::ContinueCody,
        Self::Auggie,
        Self::Junie,
        Self::Kilo,
        Self::SweAgent,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPathKind {
    NativeImport,
    PassiveCapture,
    Wrapper,
    NormalizedImport,
    Fixture,
    Detection,
    Research,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderSourceTrust {
    ProviderNative,
    ProviderExport,
    WrapperObserved,
    Fixture,
    Synthetic,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRawRetention {
    #[default]
    None,
    PathReference,
    MetadataOnly,
    LocalBlob,
    Withheld,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRedactionBoundary {
    BeforeStore,
    BeforeExport,
    #[default]
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCursorCheckpoint {
    pub stream: String,
    pub cursor: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderCursorRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<ProviderCursorCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<ProviderCursorCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProviderFidelityClaims {
    #[serde(default)]
    pub user_prompts: bool,
    #[serde(default)]
    pub assistant_messages: bool,
    #[serde(default)]
    pub tool_calls: bool,
    #[serde(default)]
    pub tool_output: bool,
    #[serde(default)]
    pub command_output: bool,
    #[serde(default)]
    pub files_touched: bool,
    #[serde(default)]
    pub artifacts: bool,
    #[serde(default)]
    pub model_identity: bool,
    #[serde(default)]
    pub costs: bool,
    #[serde(default)]
    pub token_usage: bool,
    #[serde(default)]
    pub parent_child_session_edges: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderArtifactDescriptor {
    pub provider_artifact_id: String,
    pub kind: ArtifactKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub byte_size: Option<u64>,
    #[serde(default)]
    pub redaction_state: RedactionState,
    #[serde(default = "super::default_metadata")]
    pub metadata: Value,
}

/// Normalize provider artifact descriptors for content identity.
///
/// Provider-local paths describe where an adapter observed an artifact, not
/// the artifact itself.  The descriptor list is therefore treated as a set:
/// descriptors, object keys, and nested arrays are put in canonical order.
/// Sorting uses an explicitly framed representation rather than JSON text so
/// values such as `"ab", "c"` cannot be confused with `"a", "bc"`.
///
/// This deliberately only removes `source_path` from each descriptor's
/// top-level object.  A `path` in artifact metadata, and file-touch paths in
/// the surrounding history model, retain their existing semantics.
pub fn normalize_provider_artifacts<T: Serialize>(artifacts: &[T]) -> serde_json::Result<Value> {
    Ok(normalize_provider_artifacts_value(serde_json::to_value(
        artifacts,
    )?))
}

/// Value form of [`normalize_provider_artifacts`], used by archive readers
/// whose rows already contain decoded JSON.
pub fn normalize_provider_artifacts_value(value: Value) -> Value {
    let Value::Array(artifacts) = value else {
        return value;
    };

    let mut artifacts = artifacts
        .into_iter()
        .map(|mut artifact| {
            if let Value::Object(fields) = &mut artifact {
                fields.remove("source_path");
            }
            canonicalize_json_value(artifact)
        })
        .collect::<Vec<_>>();
    artifacts.sort_by_key(framed_json);
    Value::Array(artifacts)
}

fn canonicalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(fields) => {
            let mut entries = fields
                .into_iter()
                .map(|(key, value)| (key, canonicalize_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(entries.into_iter().collect::<Map<_, _>>())
        }
        Value::Array(values) => {
            let mut values = values
                .into_iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>();
            values.sort_by_key(framed_json);
            Value::Array(values)
        }
        value => value,
    }
}

fn framed_json(value: &Value) -> Vec<u8> {
    fn append(value: &Value, output: &mut Vec<u8>) {
        match value {
            Value::Null => output.extend_from_slice(b"n"),
            Value::Bool(value) => output.extend_from_slice(if *value { b"b1" } else { b"b0" }),
            Value::Number(value) => {
                output.extend_from_slice(b"d");
                let text = value.to_string();
                output.extend_from_slice(&(text.len() as u64).to_be_bytes());
                output.extend_from_slice(text.as_bytes());
            }
            Value::String(value) => {
                output.extend_from_slice(b"s");
                output.extend_from_slice(&(value.len() as u64).to_be_bytes());
                output.extend_from_slice(value.as_bytes());
            }
            Value::Array(values) => {
                output.extend_from_slice(b"a");
                output.extend_from_slice(&(values.len() as u64).to_be_bytes());
                for value in values {
                    append(value, output);
                }
            }
            Value::Object(values) => {
                output.extend_from_slice(b"o");
                output.extend_from_slice(&(values.len() as u64).to_be_bytes());
                let mut fields = values.iter().collect::<Vec<_>>();
                fields.sort_by_key(|(name, _)| *name);
                for (name, value) in fields {
                    output.extend_from_slice(&(name.len() as u64).to_be_bytes());
                    output.extend_from_slice(name.as_bytes());
                    append(value, output);
                }
            }
        }
    }

    let mut output = Vec::new();
    append(value, &mut output);
    output
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSourceEnvelope {
    pub source_format: String,
    pub machine_id: String,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_source_path: Option<String>,
    #[serde(default)]
    pub raw_retention: ProviderRawRetention,
    #[serde(default)]
    pub redaction_boundary: ProviderRedactionBoundary,
    #[serde(default)]
    pub trust: ProviderSourceTrust,
    #[serde(default)]
    pub fidelity: Fidelity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<ProviderCursorRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default = "super::default_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSessionEnvelope {
    pub provider_session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_provider_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_agent_id: Option<String>,
    #[serde(default)]
    pub agent_type: AgentType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_hint: Option<String>,
    #[serde(default)]
    pub is_primary: bool,
    #[serde(default)]
    pub status: SessionStatus,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default)]
    pub fidelity: Fidelity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ProviderArtifactDescriptor>,
    #[serde(default = "super::default_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderEventEnvelope {
    pub provider_event_index: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_event_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub event_type: EventType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<EventRole>,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub fidelity: Fidelity,
    #[serde(default)]
    pub redaction_state: RedactionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ProviderArtifactDescriptor>,
    #[serde(default = "super::default_metadata")]
    pub payload: Value,
    #[serde(default = "super::default_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderCaptureEnvelope {
    #[serde(default = "provider_capture_envelope_schema_version")]
    pub schema_version: u32,
    pub provider: CaptureProvider,
    pub source: ProviderSourceEnvelope,
    pub session: ProviderSessionEnvelope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<ProviderEventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSupportPath {
    pub kind: ProviderPathKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format: Option<String>,
    #[serde(default)]
    pub fidelity: Fidelity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proof: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSupportEntry {
    pub id: ProviderId,
    #[serde(alias = "name", default)]
    pub display_name: String,
    #[serde(default)]
    pub priority: ProviderMatrixPriority,
    #[serde(default)]
    pub status: ProviderSupportStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_provider: Option<CaptureProvider>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub implemented_paths: Vec<ProviderSupportPath>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_detection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_detection: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history_locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hook_options: Vec<String>,
    #[serde(default)]
    pub imports_existing_history: bool,
    #[serde(default)]
    pub captures_new_runs_passively: bool,
    #[serde(default)]
    pub child_sessions_supported: bool,
    #[serde(default)]
    pub fidelity: ProviderFidelityClaims,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redaction_notes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub public_docs: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tests: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixture_paths: Vec<String>,
    #[serde(default = "super::default_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderSupportMatrixDocument {
    #[serde(default = "provider_support_matrix_schema_version", alias = "version")]
    pub schema_version: u32,
    pub providers: Vec<ProviderSupportEntry>,
}

pub const fn provider_capture_envelope_schema_version() -> u32 {
    PROVIDER_CAPTURE_ENVELOPE_SCHEMA_VERSION
}

pub const fn provider_support_matrix_schema_version() -> u32 {
    PROVIDER_SUPPORT_MATRIX_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::PathBuf};

    use super::{
        normalize_provider_artifacts, ProviderCaptureEnvelope, ProviderId,
        ProviderSupportMatrixDocument, ProviderSupportStatus,
    };

    fn workspace_file(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    }

    #[test]
    fn provider_support_matrix_scaffold_parses_current_provider_rows() {
        let matrix = fs::read_to_string(workspace_file("docs/provider-support-matrix.json"))
            .expect("provider support matrix scaffold should exist");
        let parsed: ProviderSupportMatrixDocument =
            serde_json::from_str(&matrix).expect("matrix scaffold should parse");
        let ids = parsed
            .providers
            .iter()
            .map(|entry| entry.id)
            .collect::<BTreeSet<_>>();
        let expected = [
            ProviderId::AntigravityCli,
            ProviderId::AstrBot,
            ProviderId::ClaudeCode,
            ProviderId::Codex,
            ProviderId::Cursor,
            ProviderId::CopilotCli,
            ProviderId::FactoryAiDroid,
            ProviderId::GeminiCli,
            ProviderId::Hermes,
            ProviderId::NanoClaw,
            ProviderId::OpenCode,
            ProviderId::OpenClaw,
            ProviderId::Pi,
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();

        assert_eq!(parsed.schema_version, 1);
        assert_eq!(ids, expected);
    }

    #[test]
    fn provider_support_matrix_records_local_import_statuses() {
        let matrix = fs::read_to_string(workspace_file("docs/provider-support-matrix.json"))
            .expect("provider support matrix scaffold should exist");
        let parsed: ProviderSupportMatrixDocument =
            serde_json::from_str(&matrix).expect("matrix scaffold should parse");

        for (id, status, env_name) in [
            (
                ProviderId::Codex,
                ProviderSupportStatus::LocalImport,
                "Codex",
            ),
            (
                ProviderId::Pi,
                ProviderSupportStatus::LocalImportWhenSupported,
                "Pi",
            ),
        ] {
            let entry = parsed
                .providers
                .iter()
                .find(|entry| entry.id == id)
                .unwrap_or_else(|| panic!("missing provider row for {id:?}"));
            assert_eq!(entry.status, status, "{id:?} support status changed");
            assert_eq!(entry.display_name, env_name);
        }
    }

    #[test]
    fn provider_capture_envelope_round_trips_cursor_and_redaction_fields() {
        let sample = r#"{
          "schema_version": 1,
          "provider": "codex",
          "source": {
            "source_format": "normalized_provider_fixture_jsonl",
            "machine_id": "machine-1",
            "observed_at": "2026-06-23T12:00:00Z",
            "raw_retention": "metadata_only",
            "redaction_boundary": "before_export",
            "trust": "fixture",
            "fidelity": "imported",
            "cursor": {
              "after": {
                "stream": "provider:codex:fixture",
                "cursor": "line:2",
                "observed_at": "2026-06-23T12:00:01Z"
              }
            },
            "metadata": {"source": "fixture"}
          },
          "session": {
            "provider_session_id": "codex-session-1",
            "agent_type": "primary",
            "status": "imported",
            "started_at": "2026-06-23T12:00:00Z",
            "fidelity": "imported",
            "metadata": {"model": "gpt-5-codex"}
          },
          "event": {
            "provider_event_index": 1,
            "cursor": "line:2",
            "event_type": "message",
            "role": "assistant",
            "occurred_at": "2026-06-23T12:00:01Z",
            "fidelity": "imported",
            "redaction_state": "redacted",
            "payload": {"text": "redacted preview only"},
            "metadata": {"token_usage": 42}
          }
        }"#;

        let parsed: ProviderCaptureEnvelope =
            serde_json::from_str(sample).expect("envelope should parse");
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(
            parsed
                .source
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.after.as_ref())
                .map(|checkpoint| checkpoint.cursor.as_str()),
            Some("line:2")
        );
        assert_eq!(
            parsed
                .event
                .as_ref()
                .map(|event| event.redaction_state.as_str()),
            Some("redacted")
        );
    }

    #[test]
    fn provider_artifact_normalization_is_order_and_path_independent() {
        let first = vec![
            serde_json::json!({
                "provider_artifact_id": "b",
                "kind": "markdown",
                "source_path": "/old/export.md",
                "metadata": {
                    "path": "workspace/description.md",
                    "labels": ["z", "a"],
                    "nested": {"second": 2, "first": 1}
                }
            }),
            serde_json::json!({
                "provider_artifact_id": "a",
                "kind": "binary",
                "source_path": "/old/image.bin",
                "metadata": {"nested": {"first": 1, "second": 2}}
            }),
        ];
        let second = vec![
            serde_json::json!({
                "metadata": {"nested": {"second": 2, "first": 1}},
                "source_path": "/new/image.bin",
                "kind": "binary",
                "provider_artifact_id": "a"
            }),
            serde_json::json!({
                "metadata": {
                    "nested": {"first": 1, "second": 2},
                    "labels": ["a", "z"],
                    "path": "workspace/description.md"
                },
                "kind": "markdown",
                "provider_artifact_id": "b",
                "source_path": "/new/export.md"
            }),
        ];

        assert_eq!(
            normalize_provider_artifacts(&first).unwrap(),
            normalize_provider_artifacts(&second).unwrap()
        );
        assert_eq!(
            normalize_provider_artifacts(&first).unwrap()[1]["metadata"]["path"],
            "workspace/description.md"
        );
        assert!(normalize_provider_artifacts(&first)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .all(|artifact| artifact.get("source_path").is_none()));
    }
}
