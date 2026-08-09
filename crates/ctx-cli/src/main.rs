use std::{
    env, fs,
    io::{Cursor, IsTerminal, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod config;
mod docs;
mod history_source_plugins;
mod mcp;
mod storage_status;

use config::CONFIG_FILE;
use ctx_history_capture::{
    catalog_codex_session_tree, discover_provider_sources, discover_provider_sources_for_provider,
    import_antigravity_cli_history, import_astrbot_sqlite, import_claude_projects_jsonl_tree,
    import_codex_history_jsonl, import_codex_session_jsonl, import_codex_session_jsonl_tail,
    import_codex_session_paths, import_codex_session_tree, import_copilot_cli_session_events,
    import_cursor_native_history, import_custom_history_jsonl_v1,
    import_custom_history_jsonl_v1_reader, import_factory_ai_droid_sessions,
    import_gemini_cli_history, import_hermes_sqlite, import_nanoclaw_project,
    import_openclaw_history, import_opencode_sqlite, import_pi_session_jsonl,
    provider_source_for_path, provider_source_spec, stable_capture_uuid,
    validate_custom_history_jsonl_v1, validate_custom_history_jsonl_v1_reader,
    AntigravityCliImportOptions, AstrBotSqliteImportOptions, CatalogSummary,
    ClaudeProjectsImportOptions, CodexEventImportMode, CodexHistoryImportOptions,
    CodexSessionCatalogOptions, CodexSessionImportOptions, CodexSessionImportProgress,
    CodexSessionImportProgressCallback, CodexToolOutputMode, CopilotCliImportOptions,
    CursorNativeImportOptions, CustomHistoryJsonlV1ImportOptions, FactoryAiDroidImportOptions,
    GeminiCliImportOptions, HermesSqliteImportOptions, NanoClawImportOptions,
    OpenClawImportOptions, OpenCodeSqliteImportOptions, PiSessionImportOptions,
    ProviderImportSummary, ProviderImportSupport, ProviderSource, ProviderSourceStatus,
    OPENCODE_CURSOR_V2_PREFIX,
};
use ctx_history_core::{
    database_path, default_data_root, utc_now, CaptureProvider, CtxHistoryJsonlRecord, CtxIdPrefix,
    Event, EventRole, EventType, HistoryRecord, RedactionState, SearchMatchMode, Session,
};
use ctx_history_query::{
    raw_sql_result_json, sources_json as query_sources_json, status_json as query_status_json,
    status_snapshot as query_status_snapshot, BytePolicy, EventPageV1, EventProjectionV1, FieldSet,
    HistorySourcePluginFailureProjection, HistorySourcePluginSourceProjection, QueryError,
    QueryService, SearchContextProjectionV1, SearchPageV1, SearchResultProjectionV1,
    SessionProjectionV1, TranscriptMode as QueryTranscriptMode, DEFAULT_ITEM_BYTES,
    DEFAULT_PAGE_BYTES, DEFAULT_SHOW_LIMIT, MAX_SHOW_LIMIT,
};
use ctx_history_store::{
    archive_verification_error_code, restore_archive_bundle, verify_archive_bundle_with_options,
    ArchiveOptions, ArchiveVerifyOptions, CatalogSession, CatalogSourceIndexUpdate,
    IdPrefixResolution, RawSqlOptions, RawSqlResult, RawSqlValue, SourceHealthClassification,
    SourceImportFile, SourceImportFileIndexUpdate, Store, StoreError, ARCHIVE_MAX_ENTITIES,
    ARCHIVE_MAX_OBJECTS, ARCHIVE_MAX_OBJECT_BYTES, ARCHIVE_MAX_TOTAL_BYTES,
    CATALOG_IMPORT_OUTCOME_UNATTRIBUTED_CODE, RAW_SQL_DEFAULT_MAX_COLUMNS,
    RAW_SQL_DEFAULT_MAX_ROWS, RAW_SQL_DEFAULT_MAX_SQL_BYTES, RAW_SQL_DEFAULT_MAX_VALUE_BYTES,
    RAW_SQL_MAX_TIMEOUT, SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
};
use history_source_plugins::{
    discover_history_source_plugins, discover_history_source_plugins_with_diagnostics,
    run_history_source_plugin, HistorySourcePluginManifestFailure, HistorySourcePluginRefresh,
    HistorySourcePluginRunOptions, HistorySourcePluginSource,
};

const WAL_TRUNCATE_MIN_BYTES: u64 = 64 * 1024 * 1024;
const LARGE_IMPORT_SOURCE_FILES_WARNING: usize = 10_000;
const LARGE_IMPORT_SOURCE_BYTES_WARNING: u64 = 1024 * 1024 * 1024;
const MAX_SEARCH_LIMIT: usize = 200;

#[derive(Debug, Parser)]
#[command(name = "ctx", version, about = "Search local agent history")]
struct Cli {
    #[arg(long, env = "CTX_DATA_ROOT", global = true)]
    data_root: Option<PathBuf>,
    #[command(subcommand)]
    command: CommandRoot,
}

#[derive(Debug, Subcommand)]
enum CommandRoot {
    #[command(about = "Create local ctx storage and index discovered history")]
    Setup(SetupArgs),
    #[command(about = "Show local ctx index status")]
    Status(JsonArgs),
    #[command(about = "List configured and discovered agent history sources")]
    Sources(JsonArgs),
    #[command(about = "Index provider history into local search")]
    Import(ImportArgs),
    #[command(about = "Show an indexed session transcript or event")]
    Show(ShowArgs),
    #[command(about = "Locate provider/source metadata for an indexed session or event")]
    Locate(LocateArgs),
    #[command(about = "Search indexed agent history")]
    Search(Box<SearchArgs>),
    #[command(about = "Run read-only SQL against the local ctx index")]
    Sql(SqlArgs),
    #[command(about = "Read embedded ctx documentation")]
    Docs(docs::DocsArgs),
    #[command(about = "Serve read-only ctx tools over MCP")]
    Mcp(mcp::McpArgs),
    #[command(about = "Check local ctx health")]
    Doctor(DoctorArgs),
    #[command(about = "Create a private, checksummed logical archive")]
    Archive(ArchiveArgs),
}

#[derive(Debug, Args)]
struct SetupArgs {
    #[arg(long, alias = "no-import")]
    catalog_only: bool,
    #[arg(long)]
    json: bool,
    #[arg(long, value_enum, default_value_t = ProgressArg::Auto)]
    progress: ProgressArg,
}

#[derive(Debug, Args, Clone)]
struct JsonArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ArchiveArgs {
    #[command(subcommand)]
    command: ArchiveCommand,
}

#[derive(Debug, Subcommand)]
enum ArchiveCommand {
    #[command(about = "Stream the current data root into an atomic archive bundle")]
    Create(ArchiveCreateArgs),
    #[command(about = "Verify a complete archive bundle without modifying it")]
    Verify(ArchiveVerifyArgs),
    #[command(about = "Restore a verified archive into a strictly absent data root")]
    Restore(ArchiveRestoreArgs),
}

#[derive(Debug, Args)]
struct ArchiveCreateArgs {
    #[arg(help = "Absent destination directory, conventionally ending in .ctxar")]
    target: PathBuf,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ArchiveVerifyArgs {
    #[arg(help = "Published archive bundle directory")]
    bundle: PathBuf,
    #[arg(long)]
    json: bool,
    #[arg(long, default_value_t = ARCHIVE_MAX_ENTITIES)]
    max_entities: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_OBJECTS)]
    max_objects: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_OBJECT_BYTES)]
    max_object_bytes: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_TOTAL_BYTES)]
    max_bytes: u64,
}

#[derive(Debug, Args)]
struct ArchiveRestoreArgs {
    #[arg(help = "Published archive bundle directory")]
    bundle: PathBuf,
    #[arg(help = "Strictly absent destination data-root directory")]
    target: PathBuf,
    #[arg(
        long,
        help = "Emit JSON on success and typed verifier rejection envelopes; destination and restore failures remain ordinary errors"
    )]
    json: bool,
    #[arg(long, default_value_t = ARCHIVE_MAX_ENTITIES)]
    max_entities: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_OBJECTS)]
    max_objects: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_OBJECT_BYTES)]
    max_object_bytes: u64,
    #[arg(long, default_value_t = ARCHIVE_MAX_TOTAL_BYTES)]
    max_bytes: u64,
}

#[derive(Debug, Args, Clone)]
struct DoctorArgs {
    #[arg(long)]
    json: bool,
    #[arg(long, help = "Include read-only storage footprint diagnostics")]
    storage: bool,
    #[arg(
        long,
        help = "Explicitly clear advisory source-health rows; does not alter imported history or search state"
    )]
    acknowledge_source_health: bool,
    #[arg(long, value_enum, default_value_t = ProgressArg::Auto)]
    progress: ProgressArg,
}

#[derive(Debug, Args)]
struct ImportArgs {
    #[arg(long, value_enum)]
    provider: Option<NativeProviderArg>,
    #[arg(
        long,
        help = "Import exactly this path; native provider paths require --provider"
    )]
    path: Option<PathBuf>,
    #[arg(long = "history-source", conflicts_with_all = ["provider", "path", "format", "all"])]
    history_source: Option<String>,
    #[arg(
        long = "history-source-manifest",
        conflicts_with_all = ["provider", "path", "format"]
    )]
    history_source_manifest: Vec<PathBuf>,
    #[arg(long = "reset-cursor")]
    reset_cursor: bool,
    #[arg(
        long,
        value_enum,
        requires = "path",
        conflicts_with_all = ["provider", "all", "history_source"]
    )]
    format: Option<ImportFormatArg>,
    #[arg(long, conflicts_with_all = ["provider", "path", "format", "history_source"])]
    all: bool,
    #[arg(long)]
    resume: bool,
    #[arg(long)]
    json: bool,
    #[arg(
        long,
        help = "Exit nonzero after reporting if import health detects a zero-yield anomaly"
    )]
    strict: bool,
    #[arg(long, value_enum, default_value_t = ProgressArg::Auto)]
    progress: ProgressArg,
}

#[derive(Debug, Args)]
struct ShowArgs {
    #[command(subcommand)]
    target: ShowTarget,
}

#[derive(Debug, Subcommand)]
enum ShowTarget {
    #[command(about = "Show a session transcript")]
    Session(ShowSessionArgs),
    #[command(about = "Show one event or a surrounding event window")]
    Event(ShowEventArgs),
}

#[derive(Debug, Args)]
struct ShowSessionArgs {
    #[arg(help = "ctx session UUID or unambiguous 8+ hex UUID prefix (compact or canonical)")]
    id: Option<String>,
    #[arg(long, value_enum)]
    provider: Option<ProviderArg>,
    #[arg(long = "provider-session")]
    provider_session: Option<String>,
    #[arg(long, value_enum, default_value_t = TranscriptMode::Lite)]
    mode: TranscriptMode,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    #[arg(long)]
    json: bool,
    #[arg(long, default_value_t = DEFAULT_SHOW_LIMIT, value_parser = parse_show_limit, help = "Maximum events to emit for this page (1..1000)")]
    limit: usize,
    #[arg(
        long = "continue",
        help = "Opaque continuation from a previous show session page"
    )]
    continuation: Option<String>,
    #[arg(long, value_enum, default_value_t = FieldArg::Full, help = "Output full or compact event fields")]
    fields: FieldArg,
    #[arg(long, aliases = ["event-bytes", "item-bytes"], default_value_t = DEFAULT_ITEM_BYTES, help = "Maximum UTF-8 bytes per event (0 allowed)")]
    max_event_bytes: usize,
    #[arg(long, alias = "page-bytes", default_value_t = DEFAULT_PAGE_BYTES, help = "Maximum selected event projection JSON bytes per page (0 allowed)")]
    max_page_bytes: usize,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ShowEventArgs {
    #[arg(help = "ctx event UUID or unambiguous 8+ hex UUID prefix (compact or canonical)")]
    id: String,
    #[arg(long, default_value_t = 0)]
    before: usize,
    #[arg(long, default_value_t = 0)]
    after: usize,
    #[arg(long)]
    window: Option<usize>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    format: OutputFormat,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct LocateArgs {
    #[command(subcommand)]
    target: LocateTarget,
}

#[derive(Debug, Subcommand)]
enum LocateTarget {
    #[command(about = "Locate provider/source metadata for a session")]
    Session(LocateSessionArgs),
    #[command(about = "Locate provider/source metadata for an event")]
    Event(LocateEventArgs),
}

#[derive(Debug, Args)]
struct LocateSessionArgs {
    #[arg(help = "ctx session UUID or unambiguous 8+ hex UUID prefix (compact or canonical)")]
    id: Option<String>,
    #[arg(long, value_enum)]
    provider: Option<ProviderArg>,
    #[arg(long = "provider-session")]
    provider_session: Option<String>,
    #[arg(long, value_enum, default_value_t = LocateFormat::Text)]
    format: LocateFormat,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct LocateEventArgs {
    #[arg(help = "ctx event UUID or unambiguous 8+ hex UUID prefix (compact or canonical)")]
    id: String,
    #[arg(long, value_enum, default_value_t = LocateFormat::Text)]
    format: LocateFormat,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Args)]
struct SearchArgs {
    #[arg(help = "Natural-language query to search local agent history")]
    query: Option<String>,
    #[arg(
        long,
        help = "Add another search query or keyword; repeat to broaden with OR-style merged results (maximum 32 clauses, 4096 bytes each, 65536 aggregate query bytes)"
    )]
    term: Vec<String>,
    #[arg(
        long,
        value_enum,
        default_value_t = SearchMatchArg::All,
        help = "How words inside each query clause match: all (default), any, or phrase"
    )]
    r#match: SearchMatchArg,
    #[arg(
        long,
        default_value_t = 20,
        value_parser = parse_search_limit,
        help = "Maximum results to return, from 1 to 200"
    )]
    limit: usize,
    #[arg(long, help = "Search only one provider")]
    provider: Option<ProviderArg>,
    #[arg(
        long = "history-source",
        help = "Filter custom history imports by plugin/source or provider_key/source_id"
    )]
    history_source: Option<String>,
    #[arg(
        long = "provider-key",
        help = "Filter custom history imports by provider_key"
    )]
    provider_key: Option<String>,
    #[arg(
        long = "source-id",
        help = "Filter custom history imports by source_id"
    )]
    source_id: Option<String>,
    #[arg(
        long = "source-format",
        help = "Filter custom history imports by source_format"
    )]
    source_format: Option<String>,
    #[arg(
        long,
        help = "Filter by stored workspace, cwd, source path, or repo-name text"
    )]
    workspace: Option<String>,
    #[arg(
        long,
        help = "Filter to recent history, as RFC3339 or a day window like 30d"
    )]
    since: Option<String>,
    #[arg(
        long,
        hide = true,
        help = "Deprecated alias for the default primary-agent search scope"
    )]
    primary_only: bool,
    #[arg(
        long,
        help = "Include subagent sessions in addition to primary-agent sessions"
    )]
    include_subagents: bool,
    #[arg(
        long,
        help = "Filter by event type: message, tool_call, tool_output, command_started, command_output, command_finished, file_touched, vcs_change, artifact, summary, or notice"
    )]
    event_type: Option<String>,
    #[arg(
        long = "role",
        help = "Include only events with this role: user, assistant, or tool; repeatable"
    )]
    role: Vec<String>,
    #[arg(
        long = "exclude-role",
        help = "Exclude events with this role: user, assistant, or tool; repeatable"
    )]
    exclude_role: Vec<String>,
    #[arg(
        long,
        help = "Exclude tool invocations and command output from search results"
    )]
    exclude_tool_noise: bool,
    #[arg(
        long = "exclude-tool",
        help = "Exclude tool/command events whose structured tool or command executable is this name, for example ctx; repeatable"
    )]
    exclude_tool_name: Vec<String>,
    #[arg(
        long,
        help = "Filter by indexed touched-file path metadata, not the current filesystem"
    )]
    file: Option<PathBuf>,
    #[arg(
        long,
        help = "Search event hits within one ctx session id or unambiguous id prefix"
    )]
    session: Option<String>,
    #[arg(
        long,
        help = "Return dense event-level results instead of diverse session results"
    )]
    events: bool,
    #[arg(
        long,
        value_enum,
        default_value_t = RefreshArg::Auto,
        help = "Pre-search refresh behavior: auto, off, or strict",
        long_help = "Pre-search refresh behavior. auto best-effort refreshes discovered native provider sources and enabled auto history-source plugins, then serves the existing index if refresh fails; off searches the existing index only; strict fails if the refresh cannot run or import successfully."
    )]
    refresh: RefreshArg,
    #[arg(
        long,
        help = "Include the active Codex session tree when CODEX_THREAD_ID is set"
    )]
    include_current_session: bool,
    #[arg(long, help = "Print machine-readable JSON")]
    json: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text, help = "Output format: text, markdown, json, or jsonl/NDJSON")]
    format: OutputFormat,
    #[arg(
        long = "continue",
        help = "Opaque continuation from a previous search page; requires --refresh off"
    )]
    continuation: Option<String>,
    #[arg(long, value_enum, default_value_t = FieldArg::Full, help = "Output full or compact result fields")]
    fields: FieldArg,
    #[arg(long, aliases = ["snippet-bytes", "item-bytes"], default_value_t = DEFAULT_ITEM_BYTES, help = "Maximum UTF-8 bytes per result snippet (0 allowed)")]
    max_snippet_bytes: usize,
    #[arg(long, alias = "page-bytes", default_value_t = DEFAULT_PAGE_BYTES, help = "Maximum selected result projection JSON bytes per page (0 allowed)")]
    max_page_bytes: usize,
    #[arg(
        long,
        help = "Print expanded text details such as full ids, provider ids, citations, and next commands"
    )]
    verbose: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SearchMatchArg {
    All,
    Any,
    Phrase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FieldArg {
    Full,
    Compact,
}

impl From<FieldArg> for FieldSet {
    fn from(value: FieldArg) -> Self {
        match value {
            FieldArg::Full => FieldSet::Full,
            FieldArg::Compact => FieldSet::Compact,
        }
    }
}

fn parse_show_limit(value: &str) -> std::result::Result<usize, String> {
    let limit = value
        .parse::<usize>()
        .map_err(|_| "limit must be a number".to_string())?;
    if (1..=MAX_SHOW_LIMIT).contains(&limit) {
        Ok(limit)
    } else {
        Err(format!("limit must be between 1 and {MAX_SHOW_LIMIT}"))
    }
}

impl From<SearchMatchArg> for SearchMatchMode {
    fn from(value: SearchMatchArg) -> Self {
        match value {
            SearchMatchArg::All => Self::All,
            SearchMatchArg::Any => Self::Any,
            SearchMatchArg::Phrase => Self::Phrase,
        }
    }
}

#[derive(Debug, Args)]
struct SqlArgs {
    #[arg(help = "Read-only SQL statement to run; pass '-' to read SQL from stdin")]
    sql: Option<String>,
    #[arg(long, conflicts_with = "sql", help = "Read SQL from a file")]
    file: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = SqlFormat::Table)]
    format: SqlFormat,
    #[arg(long, help = "Alias for --format json")]
    json: bool,
    #[arg(long, default_value_t = RAW_SQL_DEFAULT_MAX_ROWS)]
    max_rows: usize,
    #[arg(long, default_value_t = RAW_SQL_DEFAULT_MAX_COLUMNS)]
    max_columns: usize,
    #[arg(long, default_value_t = RAW_SQL_DEFAULT_MAX_VALUE_BYTES)]
    max_value_bytes: usize,
    #[arg(long, default_value_t = RAW_SQL_DEFAULT_MAX_SQL_BYTES)]
    max_sql_bytes: usize,
    #[arg(long, default_value = "10s", value_parser = parse_sql_timeout)]
    timeout: StdDuration,
    #[arg(long, help = "Omit the header row for CSV output")]
    no_header: bool,
}

impl SqlArgs {
    fn output_format(&self) -> SqlFormat {
        if self.json {
            SqlFormat::Json
        } else {
            self.format
        }
    }
}

pub(crate) struct SearchFilterInput {
    session: Option<String>,
    provider: Option<ProviderArg>,
    source_identity: SourceIdentityFilterArgs,
    workspace: Option<String>,
    since: Option<String>,
    primary_only: bool,
    include_subagents: bool,
    event_type: Option<String>,
    role: Vec<String>,
    exclude_role: Vec<String>,
    exclude_tool_noise: bool,
    exclude_tool_name: Vec<String>,
    file: Option<PathBuf>,
    include_current_session: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceIdentityFilterArgs {
    history_source: Option<String>,
    provider_key: Option<String>,
    source_id: Option<String>,
    source_format: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct SourceIdentityFilters {
    history_source: Option<String>,
    provider_key: Option<String>,
    source_id: Option<String>,
    source_format: Option<String>,
}

impl SourceIdentityFilters {
    fn is_empty(&self) -> bool {
        self.history_source.is_none()
            && self.provider_key.is_none()
            && self.source_id.is_none()
            && self.source_format.is_none()
    }

    fn matches_plugin_source(&self, source: &HistorySourcePluginSource) -> bool {
        if let Some(selector) = &self.history_source {
            if !source.matches_selector(selector) {
                return false;
            }
        }
        if let Some(provider_key) = &self.provider_key {
            if source.provider_key != *provider_key {
                return false;
            }
        }
        if let Some(source_id) = &self.source_id {
            if source.source_id != *source_id {
                return false;
            }
        }
        if let Some(source_format) = &self.source_format {
            if source.source_format != *source_format {
                return false;
            }
        }
        true
    }
}

impl From<&SearchArgs> for SourceIdentityFilterArgs {
    fn from(args: &SearchArgs) -> Self {
        Self {
            history_source: args.history_source.clone(),
            provider_key: args.provider_key.clone(),
            source_id: args.source_id.clone(),
            source_format: args.source_format.clone(),
        }
    }
}

pub(crate) struct SearchIntentInput<'a> {
    query: Option<&'a str>,
    terms: &'a [String],
    file: Option<&'a Path>,
}

pub(crate) fn search_has_intent(input: SearchIntentInput<'_>) -> bool {
    input.query.is_some_and(has_search_token)
        || input.terms.iter().any(|term| has_search_token(term))
        || input
            .file
            .and_then(|path| path.to_str())
            .is_some_and(|file| !file.trim().is_empty())
}

fn has_search_token(value: &str) -> bool {
    value.split_whitespace().any(|term| {
        term.trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
            .chars()
            .any(char::is_alphanumeric)
    })
}

pub(crate) fn missing_search_intent_error() -> anyhow::Error {
    anyhow!(
        "search needs a query, --term, or --file\n\nTry:\n  ctx search \"failed migration\"\n  ctx search --term \"failed migration\" --term rollback\n  ctx search --file crates/foo/src/lib.rs"
    )
}

fn search_no_results_target(query: &str, terms: &[String]) -> String {
    if !query.trim().is_empty() {
        return shell_quote_arg(query);
    }
    let rendered_terms = terms
        .iter()
        .filter(|term| !term.trim().is_empty())
        .map(|term| format!("--term {}", shell_quote_arg(term)))
        .collect::<Vec<_>>();
    if rendered_terms.is_empty() {
        "search".to_owned()
    } else {
        rendered_terms.join(" ")
    }
}

fn broader_search_command(args: &SearchArgs, query: &str) -> Option<String> {
    broader_search_argv(args, query).map(|parts| {
        parts
            .iter()
            .map(|part| shell_quote_arg(part))
            .collect::<Vec<_>>()
            .join(" ")
    })
}

fn broader_search_argv(args: &SearchArgs, query: &str) -> Option<Vec<String>> {
    let has_multiword_clause = std::iter::once(query)
        .chain(args.term.iter().map(String::as_str))
        .any(|clause| ctx_history_core::search_query_terms(clause).len() >= 2);
    if !has_multiword_clause {
        return None;
    }
    let next_match = match args.r#match {
        SearchMatchArg::Phrase => SearchMatchArg::All,
        SearchMatchArg::All => SearchMatchArg::Any,
        SearchMatchArg::Any => return None,
    };
    let mut parts = vec!["ctx".to_owned()];
    parts.push("search".to_owned());
    parts.push("--match".to_owned());
    parts.push(next_match.as_str().to_owned());
    parts.push("--limit".to_owned());
    parts.push(args.limit.to_string());
    if let Some(provider) = args.provider {
        parts.push(format!("--provider={}", provider.cli_name()));
    }
    for (flag, value) in [
        ("--history-source", args.history_source.as_deref()),
        ("--provider-key", args.provider_key.as_deref()),
        ("--source-id", args.source_id.as_deref()),
        ("--source-format", args.source_format.as_deref()),
        ("--workspace", args.workspace.as_deref()),
        ("--since", args.since.as_deref()),
        ("--event-type", args.event_type.as_deref()),
        ("--session", args.session.as_deref()),
    ] {
        if let Some(value) = value {
            parts.push(format!("{flag}={value}"));
        }
    }
    // Repeatable filters are preserved one flag per value, in the order they
    // were given, so the broadened suggestion keeps the exact filter scope.
    for (flag, values) in [
        ("--role", &args.role),
        ("--exclude-role", &args.exclude_role),
        ("--exclude-tool", &args.exclude_tool_name),
    ] {
        for value in values {
            if !value.trim().is_empty() {
                parts.push(format!("{flag}={value}"));
            }
        }
    }
    if let Some(file) = &args.file {
        parts.push(format!("--file={}", file.display()));
    }
    for term in &args.term {
        if !term.trim().is_empty() {
            parts.push(format!("--term={term}"));
        }
    }
    for (enabled, flag) in [
        (args.exclude_tool_noise, "--exclude-tool-noise"),
        (args.include_subagents, "--include-subagents"),
        (args.primary_only, "--primary-only"),
        (args.events, "--events"),
        (args.include_current_session, "--include-current-session"),
        (args.verbose, "--verbose"),
        (args.json, "--json"),
    ] {
        if enabled {
            parts.push(flag.to_owned());
        }
    }
    if args.refresh != RefreshArg::Auto {
        parts.push(format!("--refresh={}", args.refresh.as_str()));
    }
    if !query.trim().is_empty() {
        parts.push("--".to_owned());
        parts.push(query.to_owned());
    }
    Some(parts)
}

fn broadened_search_json(args: &SearchArgs, query: &str) -> Value {
    let Some(argv) = broader_search_argv(args, query) else {
        return Value::Null;
    };
    let command = argv
        .iter()
        .map(|part| shell_quote_arg(part))
        .collect::<Vec<_>>()
        .join(" ");
    let to_match = match args.r#match {
        SearchMatchArg::Phrase => SearchMatchArg::All,
        SearchMatchArg::All => SearchMatchArg::Any,
        SearchMatchArg::Any => return Value::Null,
    };
    compact_json(json!({
        "executed": false,
        "from_match": args.r#match.as_str(),
        "to_match": to_match.as_str(),
        "command": command,
        "argv": argv,
    }))
}

impl SearchMatchArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
            Self::Phrase => "phrase",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TranscriptMode {
    Full,
    Lite,
    Log,
}

impl TranscriptMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Lite => "lite",
            Self::Log => "log",
        }
    }
}

impl From<TranscriptMode> for QueryTranscriptMode {
    fn from(value: TranscriptMode) -> Self {
        match value {
            TranscriptMode::Full => Self::Full,
            TranscriptMode::Lite => Self::Lite,
            TranscriptMode::Log => Self::Log,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RefreshArg {
    Auto,
    Off,
    Strict,
}

impl RefreshArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Off => "off",
            Self::Strict => "strict",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Text,
    Markdown,
    Json,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum LocateFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SqlFormat {
    Table,
    Json,
    Csv,
    Raw,
}

impl OutputFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Markdown => "markdown",
            Self::Json => "json",
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum NativeProviderArg {
    Codex,
    Pi,
    #[value(alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    #[value(alias = "antigravity-cli")]
    Antigravity,
    #[value(alias = "gemini-cli")]
    Gemini,
    Cursor,
    #[value(alias = "copilot", alias = "copilot_cli")]
    CopilotCli,
    #[value(
        alias = "factoryai-droid",
        alias = "factory-droid",
        alias = "factory_ai_droid"
    )]
    FactoryAiDroid,
    #[value(name = "openclaw", alias = "open-claw", alias = "open_claw")]
    OpenClaw,
    Hermes,
    #[value(name = "nanoclaw", alias = "nano-claw", alias = "nano_claw")]
    NanoClaw,
    #[value(name = "astrbot", alias = "astr-bot", alias = "astr_bot")]
    AstrBot,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProviderArg {
    Codex,
    Pi,
    #[value(alias = "claude-code")]
    Claude,
    #[value(name = "opencode", alias = "open-code")]
    OpenCode,
    #[value(alias = "antigravity-cli")]
    Antigravity,
    #[value(alias = "gemini-cli")]
    Gemini,
    Cursor,
    #[value(alias = "copilot", alias = "copilot_cli")]
    CopilotCli,
    #[value(
        alias = "factoryai-droid",
        alias = "factory-droid",
        alias = "factory_ai_droid"
    )]
    FactoryAiDroid,
    #[value(name = "openclaw", alias = "open-claw", alias = "open_claw")]
    OpenClaw,
    Hermes,
    #[value(name = "nanoclaw", alias = "nano-claw", alias = "nano_claw")]
    NanoClaw,
    #[value(name = "astrbot", alias = "astr-bot", alias = "astr_bot")]
    AstrBot,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ImportFormatArg {
    #[value(name = "ctx-history-jsonl-v1", alias = "custom-history-jsonl-v1")]
    CtxHistoryJsonlV1,
}

impl ImportFormatArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::CtxHistoryJsonlV1 => "ctx-history-jsonl-v1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProgressArg {
    Auto,
    Plain,
    Json,
    None,
}

impl NativeProviderArg {
    fn capture_provider(self) -> CaptureProvider {
        match self {
            Self::Codex => CaptureProvider::Codex,
            Self::Pi => CaptureProvider::Pi,
            Self::Claude => CaptureProvider::Claude,
            Self::OpenCode => CaptureProvider::OpenCode,
            Self::Antigravity => CaptureProvider::Antigravity,
            Self::Gemini => CaptureProvider::Gemini,
            Self::Cursor => CaptureProvider::Cursor,
            Self::CopilotCli => CaptureProvider::CopilotCli,
            Self::FactoryAiDroid => CaptureProvider::FactoryAiDroid,
            Self::OpenClaw => CaptureProvider::OpenClaw,
            Self::Hermes => CaptureProvider::Hermes,
            Self::NanoClaw => CaptureProvider::NanoClaw,
            Self::AstrBot => CaptureProvider::AstrBot,
        }
    }
}

impl ProviderArg {
    fn capture_provider(self) -> CaptureProvider {
        match self {
            Self::Codex => CaptureProvider::Codex,
            Self::Pi => CaptureProvider::Pi,
            Self::Claude => CaptureProvider::Claude,
            Self::OpenCode => CaptureProvider::OpenCode,
            Self::Antigravity => CaptureProvider::Antigravity,
            Self::Gemini => CaptureProvider::Gemini,
            Self::Cursor => CaptureProvider::Cursor,
            Self::CopilotCli => CaptureProvider::CopilotCli,
            Self::FactoryAiDroid => CaptureProvider::FactoryAiDroid,
            Self::OpenClaw => CaptureProvider::OpenClaw,
            Self::Hermes => CaptureProvider::Hermes,
            Self::NanoClaw => CaptureProvider::NanoClaw,
            Self::AstrBot => CaptureProvider::AstrBot,
            Self::Custom => CaptureProvider::Custom,
        }
    }

    pub(crate) fn cli_name(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Pi => "pi",
            Self::Claude => "claude",
            Self::OpenCode => "opencode",
            Self::Antigravity => "antigravity",
            Self::Gemini => "gemini",
            Self::Cursor => "cursor",
            Self::CopilotCli => "copilot-cli",
            Self::FactoryAiDroid => "factory-ai-droid",
            Self::OpenClaw => "openclaw",
            Self::Hermes => "hermes",
            Self::NanoClaw => "nanoclaw",
            Self::AstrBot => "astrbot",
            Self::Custom => "custom",
        }
    }
}

type SourceInfo = ProviderSource;

#[derive(Debug, Clone, Default)]
struct ImportTotals {
    source_files: usize,
    source_bytes: u64,
    imported_sources: usize,
    failed_sources: usize,
    imported_sessions: usize,
    imported_events: usize,
    imported_edges: usize,
    skipped: usize,
    failed: usize,
    unchanged_sources: usize,
    zero_yield_anomaly_sources: usize,
    health_persistence_failures: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportHealthClassification {
    Success,
    PartialSuccess,
    Unchanged,
    AllSkipped,
    Empty,
    UnsupportedOrMalformed,
    ZeroYieldAnomaly,
    Failed,
}

impl ImportHealthClassification {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::PartialSuccess => "partial_success",
            Self::Unchanged => "unchanged",
            Self::AllSkipped => "all_skipped",
            Self::Empty => "empty",
            Self::UnsupportedOrMalformed => "unsupported_or_malformed",
            Self::ZeroYieldAnomaly => "zero_yield_anomaly",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
struct ImportHealth {
    classification: ImportHealthClassification,
    reason_counts: Value,
    zero_yield_anomaly_count: usize,
}

impl ImportHealth {
    fn zero_yield_anomaly(&self) -> bool {
        self.zero_yield_anomaly_count > 0
    }

    fn to_json(&self) -> Value {
        json!({
            "classification": self.classification.as_str(),
            "reason_counts": self.reason_counts.clone(),
        })
    }
}

#[derive(Debug)]
struct ImportSourceReport {
    health: ImportHealth,
    json: Value,
}

struct HistorySourcePluginImportOutcome {
    summary: ProviderImportSummary,
    stats: SourceStats,
    source_only: bool,
}

#[derive(Debug)]
struct ImportReport {
    resume: bool,
    totals: ImportTotals,
    sources: Vec<ImportSourceReport>,
    health_persistence_failures: usize,
}

impl ImportReport {
    fn empty(resume: bool) -> Self {
        Self {
            resume,
            totals: ImportTotals::default(),
            sources: Vec::new(),
            health_persistence_failures: 0,
        }
    }

    fn resume_mode(&self) -> &'static str {
        resume_mode_name(self.resume)
    }

    fn has_zero_yield_anomaly(&self) -> bool {
        self.sources
            .iter()
            .any(|source| source.zero_yield_anomaly_count() > 0)
    }
}

impl ImportSourceReport {
    fn zero_yield_anomaly_count(&self) -> u64 {
        self.health.zero_yield_anomaly_count as u64
    }
}

#[derive(Debug, Clone, Copy)]
struct ImportRunOptions {
    progress: ProgressArg,
    json: bool,
    print_human: bool,
    allow_empty_sources: bool,
    include_history_source_plugins: bool,
    operation: &'static str,
}

fn resume_mode_name(resume: bool) -> &'static str {
    if resume {
        "idempotent_rescan"
    } else {
        "normal_scan"
    }
}

impl ImportTotals {
    fn add(&mut self, summary: &ProviderImportSummary, stats: &SourceStats) {
        let health = classify_import_health(stats, summary);
        self.add_with_health(summary, stats, &health);
    }

    fn add_with_health(
        &mut self,
        summary: &ProviderImportSummary,
        stats: &SourceStats,
        health: &ImportHealth,
    ) {
        self.source_files += stats.files;
        self.source_bytes = self.source_bytes.saturating_add(stats.bytes);
        self.imported_sources += 1;
        self.imported_sessions += summary.imported_sessions;
        self.imported_events += summary.imported_events;
        self.imported_edges += summary.imported_edges;
        self.skipped += summary.skipped;
        self.failed += summary.failed;
        self.unchanged_sources += summary.unchanged_sources;
        if health.zero_yield_anomaly_count > 0 {
            self.zero_yield_anomaly_sources += 1;
        }
    }

    fn add_source_failure(&mut self, stats: &SourceStats) {
        self.source_files += stats.files;
        self.source_bytes = self.source_bytes.saturating_add(stats.bytes);
        self.failed_sources += 1;
    }
}

#[derive(Debug, Default)]
struct CatalogTotals {
    sources: usize,
    source_files: usize,
    source_bytes: u64,
    cataloged_sessions: usize,
    cached_sessions: usize,
    parsed_sessions: usize,
    skipped_sessions: usize,
    failed_sessions: usize,
}

impl CatalogTotals {
    fn add(&mut self, summary: &CatalogSummary) {
        self.sources += 1;
        self.source_files += summary.source_files;
        self.source_bytes = self.source_bytes.saturating_add(summary.source_bytes);
        self.cataloged_sessions += summary.cataloged_sessions;
        self.cached_sessions += summary.cached_sessions;
        self.parsed_sessions += summary.parsed_sessions;
        self.skipped_sessions += summary.skipped_sessions;
        self.failed_sessions += summary.failed_sessions;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SourceStats {
    files: usize,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SourceProgressSnapshot {
    completed_bytes: u64,
    total_bytes: u64,
}

#[derive(Debug, Clone)]
struct SearchRefreshReport {
    mode: RefreshArg,
    status: &'static str,
    source_count: usize,
    totals: ImportTotals,
    duration_ms: u128,
    index_age_seconds: Option<i64>,
    reason: &'static str,
    error: Option<String>,
}

struct DelayedRefreshProgress {
    cancel: Arc<AtomicBool>,
}

impl DelayedRefreshProgress {
    fn start(refresh: RefreshArg) -> Option<Self> {
        if refresh == RefreshArg::Off {
            return None;
        }
        let delay_ms = env::var("CTX_TEST_REFRESH_PROGRESS_DELAY_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1500);
        let cancel = Arc::new(AtomicBool::new(false));
        let thread_cancel = Arc::clone(&cancel);
        thread::spawn(move || {
            thread::sleep(StdDuration::from_millis(delay_ms));
            if !thread_cancel.load(Ordering::Relaxed) {
                eprintln!(
                    "{}",
                    json!({"type":"ctx_progress","operation":"search-refresh","phase":"refreshing","message":"refresh still running","done":false})
                );
            }
        });
        Some(Self { cancel })
    }
}

impl Drop for DelayedRefreshProgress {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

impl SearchRefreshReport {
    fn skipped(mode: RefreshArg, status: &'static str) -> Self {
        Self {
            mode,
            status,
            source_count: 0,
            totals: ImportTotals::default(),
            duration_ms: 0,
            index_age_seconds: None,
            reason: status,
            error: None,
        }
    }

    fn completed(
        mode: RefreshArg,
        source_count: usize,
        totals: ImportTotals,
        duration_ms: u128,
    ) -> Self {
        let status = if totals.zero_yield_anomaly_sources > 0 {
            "degraded_zero_yield"
        } else if totals.health_persistence_failures > 0 {
            "degraded_health_persistence"
        } else {
            "completed"
        };
        Self {
            mode,
            status,
            source_count,
            totals,
            duration_ms,
            index_age_seconds: Some(0),
            reason: match status {
                "completed" => "refreshed",
                "degraded_zero_yield" => "zero_yield_anomaly",
                _ => "health_persistence_failed",
            },
            error: None,
        }
    }

    fn failed(mode: RefreshArg, source_count: usize, error: String, duration_ms: u128) -> Self {
        Self {
            mode,
            status: "failed",
            source_count,
            totals: ImportTotals::default(),
            duration_ms,
            index_age_seconds: None,
            reason: "refresh_failed",
            error: Some(error),
        }
    }

    fn with_index_age(mut self, store: Option<&Store>) -> Self {
        self.index_age_seconds = store
            .and_then(|store| store.latest_indexed_source_at_ms().ok().flatten())
            .map(|ms| (utc_now().timestamp_millis().saturating_sub(ms)) / 1000);
        self
    }

    fn to_json(&self) -> Value {
        compact_json(json!({
            "mode": self.mode.as_str(),
            "status": self.status,
            "source_count": self.source_count,
            "ran": self.status == "completed" || self.status == "degraded_zero_yield" || self.status == "failed",
            "duration_ms": self.duration_ms,
            "index_age_seconds": self.index_age_seconds,
            "reason": self.reason,
            "totals": import_totals_json(&self.totals),
            "error": self.error,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressRenderMode {
    None,
    Plain { interactive: bool },
    Json,
}

#[derive(Debug)]
struct ProgressState {
    started: Instant,
    last_emit: Option<Instant>,
    last_line_len: usize,
}

#[derive(Clone)]
struct ProgressReporter {
    mode: ProgressRenderMode,
    operation: &'static str,
    total_bytes: u64,
    state: Arc<Mutex<ProgressState>>,
}

impl ProgressReporter {
    fn new(arg: ProgressArg, json_output: bool, operation: &'static str, total_bytes: u64) -> Self {
        let stderr_is_terminal = std::io::stderr().is_terminal();
        let mode = match arg {
            ProgressArg::None => ProgressRenderMode::None,
            ProgressArg::Json => ProgressRenderMode::Json,
            ProgressArg::Plain => ProgressRenderMode::Plain {
                interactive: stderr_is_terminal,
            },
            ProgressArg::Auto if json_output || !stderr_is_terminal => ProgressRenderMode::None,
            ProgressArg::Auto => ProgressRenderMode::Plain { interactive: true },
        };
        Self {
            mode,
            operation,
            total_bytes,
            state: Arc::new(Mutex::new(ProgressState {
                started: Instant::now(),
                last_emit: None,
                last_line_len: 0,
            })),
        }
    }

    fn is_enabled(&self) -> bool {
        self.mode != ProgressRenderMode::None
    }

    fn message(&self, phase: &'static str, message: impl Into<String>) {
        if !self.is_enabled() {
            return;
        }
        let message = message.into();
        self.emit(ProgressLine {
            phase,
            message,
            completed_bytes: 0,
            total_bytes: self.total_bytes,
            completed_files: None,
            total_files: None,
            imported_events: None,
            done: false,
            force: true,
        });
    }

    fn done(&self, phase: &'static str, message: impl Into<String>, completed_bytes: u64) {
        if !self.is_enabled() {
            return;
        }
        self.emit(ProgressLine {
            phase,
            message: message.into(),
            completed_bytes,
            total_bytes: self.total_bytes.max(completed_bytes),
            completed_files: None,
            total_files: None,
            imported_events: None,
            done: true,
            force: true,
        });
    }

    fn finish_line(&self) {
        let mut state = self.state.lock().expect("progress state poisoned");
        if matches!(self.mode, ProgressRenderMode::Plain { interactive: true })
            && state.last_line_len > 0
        {
            eprintln!();
            state.last_line_len = 0;
        }
    }

    fn warning(&self, message: impl AsRef<str>) {
        if matches!(self.mode, ProgressRenderMode::None) {
            return;
        }
        self.finish_line();
        match self.mode {
            ProgressRenderMode::Json => {
                eprintln!(
                    "{}",
                    progress_event_json(self.operation, "warning", message.as_ref(), false, None)
                );
            }
            ProgressRenderMode::Plain { .. } => eprintln!("warning: {}", message.as_ref()),
            ProgressRenderMode::None => {}
        }
    }

    fn codex_import_callback(
        &self,
        source: &SourceInfo,
        source_offset_bytes: u64,
    ) -> Option<CodexSessionImportProgressCallback> {
        if !self.is_enabled() || source.provider != CaptureProvider::Codex {
            return None;
        }
        let reporter = self.clone();
        let provider = source.provider.as_str().to_owned();
        Some(Arc::new(move |progress: CodexSessionImportProgress| {
            let completed_bytes = source_offset_bytes.saturating_add(progress.completed_bytes);
            reporter.emit(ProgressLine {
                phase: "indexing",
                message: provider.clone(),
                completed_bytes,
                total_bytes: reporter.total_bytes.max(completed_bytes),
                completed_files: Some(progress.completed_files),
                total_files: Some(progress.total_files),
                imported_events: Some(progress.imported_events),
                done: progress.done,
                force: progress.done,
            });
        }))
    }

    fn parallel_codex_import_callback(
        &self,
        source: &SourceInfo,
        source_index: usize,
        source_states: Arc<Mutex<Vec<SourceProgressSnapshot>>>,
    ) -> Option<CodexSessionImportProgressCallback> {
        if !self.is_enabled() || source.provider != CaptureProvider::Codex {
            return None;
        }
        let reporter = self.clone();
        let provider = source.provider.as_str().to_owned();
        Some(Arc::new(move |progress: CodexSessionImportProgress| {
            let (completed_bytes, total_bytes) = {
                let mut states = source_states
                    .lock()
                    .expect("parallel progress state poisoned");
                if let Some(state) = states.get_mut(source_index) {
                    state.total_bytes = state.total_bytes.max(progress.total_bytes);
                    state.completed_bytes = progress
                        .completed_bytes
                        .min(state.total_bytes.max(progress.completed_bytes));
                }
                aggregate_source_progress(&states)
            };
            reporter.emit(ProgressLine {
                phase: "indexing",
                message: provider.clone(),
                completed_bytes,
                total_bytes: reporter.total_bytes.max(total_bytes).max(completed_bytes),
                completed_files: Some(progress.completed_files),
                total_files: Some(progress.total_files),
                imported_events: Some(progress.imported_events),
                done: progress.done,
                force: progress.done,
            });
        }))
    }

    fn parallel_source_done(
        &self,
        source: &SourceInfo,
        source_index: usize,
        source_states: &Arc<Mutex<Vec<SourceProgressSnapshot>>>,
        stats: SourceStats,
        summary: &ProviderImportSummary,
    ) {
        if !self.is_enabled() {
            return;
        }
        let (completed_bytes, total_bytes) = {
            let mut states = source_states
                .lock()
                .expect("parallel progress state poisoned");
            if let Some(state) = states.get_mut(source_index) {
                state.total_bytes = state.total_bytes.max(stats.bytes);
                state.completed_bytes = state.total_bytes;
            }
            aggregate_source_progress(&states)
        };
        self.emit(ProgressLine {
            phase: "indexing",
            message: format!("imported {}", source.provider.as_str()),
            completed_bytes,
            total_bytes: self.total_bytes.max(total_bytes).max(completed_bytes),
            completed_files: Some(stats.files),
            total_files: Some(stats.files),
            imported_events: Some(summary.imported_events),
            done: true,
            force: true,
        });
    }

    fn parallel_source_failed(
        &self,
        source: &SourceInfo,
        source_index: usize,
        source_states: &Arc<Mutex<Vec<SourceProgressSnapshot>>>,
        stats: SourceStats,
        error: &str,
    ) {
        if !self.is_enabled() {
            return;
        }
        let (completed_bytes, total_bytes) = {
            let mut states = source_states
                .lock()
                .expect("parallel progress state poisoned");
            if let Some(state) = states.get_mut(source_index) {
                state.total_bytes = state.total_bytes.max(stats.bytes);
                state.completed_bytes = state.total_bytes;
            }
            aggregate_source_progress(&states)
        };
        self.emit(ProgressLine {
            phase: "indexing",
            message: format!(
                "skipped {}: {}",
                source.provider.as_str(),
                source_error_reason(source, error)
            ),
            completed_bytes,
            total_bytes: self.total_bytes.max(total_bytes).max(completed_bytes),
            completed_files: Some(stats.files),
            total_files: Some(stats.files),
            imported_events: Some(0),
            done: true,
            force: true,
        });
    }

    fn emit(&self, line: ProgressLine) {
        let mut state = self.state.lock().expect("progress state poisoned");
        let now = Instant::now();
        if !line.force
            && state
                .last_emit
                .is_some_and(|last| now.duration_since(last) < StdDuration::from_millis(900))
        {
            return;
        }
        state.last_emit = Some(now);
        let elapsed = now.duration_since(state.started);
        match self.mode {
            ProgressRenderMode::None => {}
            ProgressRenderMode::Json => {
                let value = json!({
                    "type": "ctx_progress",
                    "operation": self.operation,
                    "phase": line.phase,
                    "message": line.message,
                    "completed_bytes": line.completed_bytes,
                    "total_bytes": line.total_bytes,
                    "percent": progress_percent(line.completed_bytes, line.total_bytes),
                    "elapsed_seconds": elapsed.as_secs_f64(),
                    "eta_seconds": eta_seconds(line.completed_bytes, line.total_bytes, elapsed),
                    "completed_files": line.completed_files,
                    "total_files": line.total_files,
                    "imported_events": line.imported_events,
                    "done": line.done,
                });
                eprintln!("{value}");
            }
            ProgressRenderMode::Plain { interactive } => {
                let rendered = render_progress_line(&line, elapsed);
                if interactive {
                    let padding = state.last_line_len.saturating_sub(rendered.len());
                    eprint!("\r{}{}", rendered, " ".repeat(padding));
                    if line.done {
                        eprintln!();
                        state.last_line_len = 0;
                    } else {
                        state.last_line_len = rendered.len();
                        let _ = std::io::stderr().flush();
                    }
                } else {
                    eprintln!("{rendered}");
                }
            }
        }
    }
}

fn aggregate_source_progress(states: &[SourceProgressSnapshot]) -> (u64, u64) {
    states
        .iter()
        .fold((0u64, 0u64), |(completed, total), state| {
            let source_total = state.total_bytes.max(state.completed_bytes);
            (
                completed.saturating_add(state.completed_bytes.min(source_total)),
                total.saturating_add(source_total),
            )
        })
}

struct ProgressLine {
    phase: &'static str,
    message: String,
    completed_bytes: u64,
    total_bytes: u64,
    completed_files: Option<usize>,
    total_files: Option<usize>,
    imported_events: Option<usize>,
    done: bool,
    force: bool,
}

fn render_progress_line(line: &ProgressLine, elapsed: StdDuration) -> String {
    let percent = progress_percent(line.completed_bytes, line.total_bytes);
    let bar = progress_bar(percent, 20);
    let eta = eta_seconds(line.completed_bytes, line.total_bytes, elapsed)
        .map(format_seconds)
        .unwrap_or_else(|| "estimating".to_owned());
    let files = match (line.completed_files, line.total_files) {
        (Some(done), Some(total)) if total > 0 => format!(" {done}/{total} files"),
        _ => String::new(),
    };
    let events = line
        .imported_events
        .map(|events| format!(" {events} events"))
        .unwrap_or_default();
    let remaining = if line.done {
        "done".to_owned()
    } else {
        format!("{eta} left")
    };
    format!(
        "{:<10} [{}] {:>5.1}% {}/{}{}{} {} - {}",
        line.phase,
        bar,
        percent,
        format_bytes(line.completed_bytes),
        format_bytes(line.total_bytes),
        files,
        events,
        remaining,
        line.message
    )
}

fn progress_percent(completed: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    ((completed as f64 / total as f64) * 100.0).clamp(0.0, 100.0)
}

fn eta_seconds(completed: u64, total: u64, elapsed: StdDuration) -> Option<f64> {
    if completed == 0 || total <= completed {
        return None;
    }
    let rate = completed as f64 / elapsed.as_secs_f64().max(0.001);
    if rate <= 0.0 {
        return None;
    }
    Some((total - completed) as f64 / rate)
}

fn progress_bar(percent: f64, width: usize) -> String {
    let filled = ((percent / 100.0) * width as f64).round() as usize;
    format!(
        "{}{}",
        "#".repeat(filled.min(width)),
        "-".repeat(width.saturating_sub(filled))
    )
}

fn format_seconds(seconds: f64) -> String {
    let seconds = seconds.max(0.0).round() as u64;
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        let minutes = seconds / 60;
        let rem = seconds % 60;
        format!("{minutes}m{rem:02}s")
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

#[derive(Debug)]
struct SilentExit {
    code: i32,
}

impl std::fmt::Display for SilentExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "silent exit {}", self.code)
    }
}

impl std::error::Error for SilentExit {}

fn main() {
    match main_result() {
        Ok(()) => {}
        Err(err) => {
            if let Some(silent) = err.downcast_ref::<SilentExit>() {
                std::process::exit(silent.code);
            }
            eprintln!("Error: {err:?}");
            std::process::exit(1);
        }
    }
}

fn main_result() -> Result<()> {
    let cli = Cli::parse();
    let data_root = cli
        .data_root
        .clone()
        .map(Ok)
        .unwrap_or_else(default_data_root)
        .context("resolve ctx data root")?;

    match cli.command {
        CommandRoot::Setup(args) => run_setup(args, data_root.clone()),
        CommandRoot::Status(args) => run_status(args, data_root.clone()),
        CommandRoot::Sources(args) => run_sources(args, data_root.clone()),
        CommandRoot::Import(args) => run_import(args, data_root.clone()),
        CommandRoot::Show(args) => run_show(args, data_root.clone()),
        CommandRoot::Locate(args) => run_locate(args, data_root.clone()),
        CommandRoot::Search(args) => run_search(*args, data_root.clone()),
        CommandRoot::Sql(args) => run_sql(args, data_root.clone()),
        CommandRoot::Docs(args) => docs::run(args),
        CommandRoot::Mcp(args) => mcp::run(args, data_root.clone()),
        CommandRoot::Doctor(args) => run_doctor(args, data_root.clone()),
        CommandRoot::Archive(args) => run_archive(args, data_root),
    }
}

fn run_archive(args: ArchiveArgs, data_root: PathBuf) -> Result<()> {
    match args.command {
        ArchiveCommand::Create(create) => run_archive_create(create, data_root),
        ArchiveCommand::Verify(verify) => run_archive_verify(verify),
        ArchiveCommand::Restore(restore) => run_archive_restore(restore),
    }
}

fn run_archive_restore(args: ArchiveRestoreArgs) -> Result<()> {
    let options = ArchiveVerifyOptions {
        max_entities: args.max_entities,
        max_objects: args.max_objects,
        max_object_bytes: args.max_object_bytes,
        max_total_bytes: args.max_bytes,
    };
    match restore_archive_bundle(&args.bundle, &args.target, options) {
        Ok(report) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "format": "ctx-archive", "format_version": 1,
                        "archive_id": report.archive_id, "source_schema_version": report.source_schema_version,
                        "path": report.path, "restored": true, "entity_count": report.entity_count,
                        "objects": {"count": report.object_count, "total_bytes": report.object_bytes},
                    }))?
                );
            } else {
                println!(
                    "restored archive {} ({} entities, {} objects): {}",
                    report.archive_id,
                    report.entity_count,
                    report.object_count,
                    report.path.display()
                );
            }
            Ok(())
        }
        Err(error) => {
            if let Some(code) = archive_verification_error_code(&error) {
                let message = archive_verification_message(code);
                if args.json {
                    eprintln!(
                        "{}",
                        serde_json::to_string(
                            &json!({"error":{"code":code,"message":message,"path":args.bundle}})
                        )?
                    );
                } else {
                    eprintln!(
                        "archive verification failed [{code}] for {}: {message}",
                        args.bundle.display()
                    );
                }
                Err(anyhow::Error::new(SilentExit { code: 1 }))
            } else {
                Err(error.into())
            }
        }
    }
}

fn run_archive_create(args: ArchiveCreateArgs, data_root: PathBuf) -> Result<()> {
    let db_path = database_path(data_root);
    let mut store = Store::open_read_only(&db_path)
        .with_context(|| format!("open ctx store read-only: {}", db_path.display()))?;
    let report = store.create_archive(&args.target, ArchiveOptions::default())?;
    if args.json {
        let streams = report
            .streams
            .iter()
            .map(|stream| {
                json!({
                    "name": stream.name,
                    "path": stream.path,
                    "count": stream.count,
                    "bytes": stream.bytes,
                    "sha256": stream.sha256,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string(&json!({
                "format": "ctx-archive",
                "format_version": 1,
                "archive_id": report.archive_id,
                "created_at_ms": report.created_at_ms,
                "path": report.path,
                "verified": true,
                "streams": streams,
                "objects": {"count": report.object_count, "total_bytes": report.object_bytes},
                "entity_count": report.entity_count,
            }))?
        );
    } else {
        println!(
            "created archive {} ({} entities, {} objects): {}",
            report.archive_id,
            report.entity_count,
            report.object_count,
            report.path.display()
        );
    }
    Ok(())
}

fn archive_verification_message(code: &str) -> &'static str {
    match code {
        "marker_missing" => "the bundle has no completion marker",
        "marker_invalid" => "the completion marker is malformed or unsupported",
        "manifest_digest_mismatch" => "the completion marker does not authenticate the manifest",
        "manifest_too_large" => "manifest.json exceeds the v1 size bound",
        "format_unsupported" => "the bundle format or version is not supported",
        "unknown_field" => "a manifest or stream record contains an unknown field",
        "layout_mismatch" => "the bundle layout is not the exact v1 layout",
        "stream_integrity_mismatch" => {
            "a stream count, size, or digest does not match the manifest"
        }
        "stream_truncated" => "a stream is truncated or missing its final newline",
        "line_too_long" => "a JSONL record exceeds the v1 line bound",
        "record_malformed" => "a stream record is malformed or noncanonical",
        "vocabulary_unknown" => "a stream enum value is outside the v1 vocabulary",
        "duplicate_id" => "the bundle contains a duplicate entity ID",
        "natural_key_conflict" => "the bundle contains a conflicting natural key",
        "stream_unsorted" => "a stream is not in canonical order",
        "dangling_reference" => "a stream reference does not resolve",
        "blob_missing" => "a referenced object blob is missing",
        "blob_unreferenced" => "the bundle contains an unreferenced object blob",
        "blob_mismatch" => "an object blob path, size, or checksum is invalid",
        "special_file" => "the bundle contains a symlink, hard link, or non-regular entry",
        "permissions_writable" => "a bundle entry is group- or world-writable",
        "size_cap_exceeded" => "the bundle exceeds a verifier size or count cap",
        _ => "the bundle could not be verified",
    }
}

fn run_archive_verify(args: ArchiveVerifyArgs) -> Result<()> {
    let options = ArchiveVerifyOptions {
        max_entities: args.max_entities,
        max_objects: args.max_objects,
        max_object_bytes: args.max_object_bytes,
        max_total_bytes: args.max_bytes,
    };
    match verify_archive_bundle_with_options(&args.bundle, options) {
        Ok(report) => {
            if args.json {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "format": "ctx-archive",
                        "format_version": 1,
                        "path": report.path,
                        "verified": true,
                        "entity_count": report.entity_count,
                        "objects": {
                            "count": report.object_count,
                            "total_bytes": report.object_bytes,
                        },
                    }))?
                );
            } else {
                println!(
                    "verified archive ({} entities, {} objects): {}",
                    report.entity_count,
                    report.object_count,
                    report.path.display()
                );
            }
            Ok(())
        }
        Err(error) => {
            let Some(code) = archive_verification_error_code(&error) else {
                return Err(error.into());
            };
            let message = archive_verification_message(code);
            if args.json {
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "error": {
                            "code": code,
                            "message": message,
                            "path": args.bundle,
                        }
                    }))?
                );
            } else {
                eprintln!(
                    "archive verification failed [{}] for {}: {}",
                    code,
                    args.bundle.display(),
                    message
                );
            }
            Err(anyhow::Error::new(SilentExit { code: 1 }))
        }
    }
}

fn progress_mode_name(progress: ProgressArg) -> &'static str {
    match progress {
        ProgressArg::Auto => "auto",
        ProgressArg::Plain => "plain",
        ProgressArg::Json => "json",
        ProgressArg::None => "none",
    }
}

fn run_setup(args: SetupArgs, data_root: PathBuf) -> Result<()> {
    fs::create_dir_all(&data_root)?;
    let db_path = database_path(data_root.clone());
    let store = Store::open(&db_path)?;
    let config_path = data_root.join(CONFIG_FILE);
    config::write_default_config(&data_root)?;
    let sources = discovered_sources();
    let progress = ProgressReporter::new(args.progress, args.json, "setup", 0);
    progress.message("cataloging", "cataloging discovered Codex sessions");
    let (catalog, catalog_sources) = catalog_available_sources(&store, &sources)?;
    progress.done(
        "cataloging",
        format!("cataloged {} Codex sessions", catalog.cataloged_sessions),
        catalog.source_bytes,
    );
    let import_report = if args.catalog_only {
        None
    } else {
        drop(store);
        let import_args = ImportArgs {
            provider: None,
            path: None,
            history_source: None,
            history_source_manifest: Vec::new(),
            reset_cursor: false,
            format: None,
            all: true,
            resume: false,
            json: args.json,
            strict: false,
            progress: args.progress,
        };
        Some(run_import_internal(
            &import_args,
            data_root.clone(),
            ImportRunOptions {
                progress: args.progress,
                json: args.json,
                print_human: !args.json,
                allow_empty_sources: true,
                include_history_source_plugins: false,
                operation: "setup",
            },
        )?)
    };
    let setup_store = Store::open(&db_path)?;
    let catalog_counts = setup_store.catalog_session_counts()?;
    let indexed_items = indexed_history_item_count(&setup_store)?;

    if args.json {
        print_json(json!({
            "schema_version": 1,
            "data_root": data_root,
            "database_path": db_path,
            "config_path": config_path,
            "mode": if args.catalog_only { "catalog_only" } else { "ready" },
            "indexed_items": indexed_items,
            "sources": ctx_history_query::native_sources_json(&sources),
            "catalog": {
                "sources": catalog.sources,
                "source_files": catalog.source_files,
                "source_bytes": catalog.source_bytes,
                "cataloged_sessions": catalog.cataloged_sessions,
                "cached_sessions": catalog.cached_sessions,
                "parsed_sessions": catalog.parsed_sessions,
                "indexed_sessions": catalog_counts.indexed,
                "pending_sessions": catalog_counts.pending,
                "skipped_sessions": catalog.skipped_sessions,
                "failed_sessions": catalog.failed_sessions,
                "failed_index_sessions": catalog_counts.failed,
                "stale_sessions": catalog_counts.stale,
            },
            "catalog_sources": catalog_sources,
            "import": setup_import_json(import_report.as_ref()),
            "network_required": false,
            "repo_writes": false,
        }))?;
    } else {
        progress.finish_line();
        print_setup_status_line(
            import_report.as_ref(),
            args.catalog_only,
            catalog_counts.pending,
            indexed_items,
        );
        println!("data_root: {}", data_root.display());
        println!("database_path: {}", db_path.display());
        println!("config_path: {}", config_path.display());
        println!("indexed_items: {indexed_items}");
        println!("cataloged_sessions: {}", catalog.cataloged_sessions);
        println!("cached_catalog_sessions: {}", catalog.cached_sessions);
        println!("parsed_catalog_sessions: {}", catalog.parsed_sessions);
        println!("indexed_catalog_sessions: {}", catalog_counts.indexed);
        println!("pending_catalog_sessions: {}", catalog_counts.pending);
        println!("failed_catalog_sessions: {}", catalog_counts.failed);
        println!("stale_catalog_sessions: {}", catalog_counts.stale);
        println!("catalog_source_files: {}", catalog.source_files);
        println!("catalog_source_bytes: {}", catalog.source_bytes);
        if let Some(report) = &import_report {
            println!("imported_sources: {}", report.totals.imported_sources);
            println!("failed_sources: {}", report.totals.failed_sources);
            println!("imported_sessions: {}", report.totals.imported_sessions);
            println!("imported_events: {}", report.totals.imported_events);
            println!("imported_edges: {}", report.totals.imported_edges);
        }
        println!("next_steps:");
        if args.catalog_only {
            println!("  ctx import --all");
            println!("  ctx sources");
        } else if setup_has_indexed_content(indexed_items) {
            println!("  ctx search \"what failed before\"");
            println!("  ctx sources");
            if setup_has_failed_sources(import_report.as_ref()) {
                println!("  ctx import --provider <provider>");
            }
        } else {
            println!("  ctx sources");
            println!("  ctx import --all");
        }
    }
    Ok(())
}

fn setup_import_json(report: Option<&ImportReport>) -> Value {
    match report {
        Some(report) => json!({
            "ran": true,
            "resume": report.resume,
            "resume_mode": report.resume_mode(),
            "totals": import_totals_json(&report.totals),
            "sources": report.sources.iter().map(|source| source.json.clone()).collect::<Vec<_>>(),
        }),
        None => json!({
            "ran": false,
            "reason": "catalog_only",
        }),
    }
}

fn print_setup_status_line(
    report: Option<&ImportReport>,
    catalog_only: bool,
    pending_catalog_sessions: usize,
    indexed_items: usize,
) {
    if catalog_only {
        if pending_catalog_sessions > 0 {
            println!("ctx catalog is ready; import is still pending");
        } else {
            println!("ctx catalog is ready");
        }
        return;
    }
    let Some(report) = report else {
        println!("ctx is initialized; no local history was indexed");
        return;
    };
    if setup_has_indexed_content(indexed_items) && report.totals.failed_sources > 0 {
        println!("ctx indexed available local agent history; some sources were skipped");
    } else if setup_has_indexed_content(indexed_items) {
        println!("ctx local agent history search is ready");
    } else {
        println!("ctx is initialized; no local history was indexed");
    }
}

fn setup_has_indexed_content(indexed_items: usize) -> bool {
    indexed_items > 0
}

fn indexed_history_item_count(store: &Store) -> Result<usize> {
    Ok(store.indexed_history_item_count()?)
}

fn setup_has_failed_sources(report: Option<&ImportReport>) -> bool {
    report.is_some_and(|report| report.totals.failed_sources > 0)
}

fn run_status(args: JsonArgs, data_root: PathBuf) -> Result<()> {
    // Preserve the version-aware guidance of the pre-extraction status
    // path: unsupported schema versions keep their migrate/upgrade advice.
    let db_path = database_path(data_root.clone());
    let snapshot = query_status_snapshot(&data_root, CONFIG_FILE).map_err(|err| match err {
        ctx_history_query::QueryError::Store(StoreError::UnsupportedSchemaVersion(version)) => {
            unsupported_schema_version_error(version, "ctx status")
        }
        err => anyhow!(err).context(format!(
            "read `ctx status` storage snapshot from read-only ctx store {}",
            db_path.display()
        )),
    })?;

    if args.json {
        print_json(query_status_json(&snapshot))?;
    } else {
        println!("data_root: {}", snapshot.data_root.display());
        println!("database_path: {}", snapshot.db_path.display());
        println!("config_path: {}", snapshot.config_path.display());
        println!("initialized: {}", snapshot.initialized);
        println!("indexed_items: {}", snapshot.counts.items);
        println!("indexed_sources: {}", snapshot.counts.sources);
        println!("cataloged_sessions: {}", snapshot.counts.catalog_total);
        println!(
            "indexed_catalog_sessions: {}",
            snapshot.counts.catalog_indexed
        );
        println!(
            "pending_catalog_sessions: {}",
            snapshot.counts.catalog_pending
        );
        println!(
            "failed_catalog_sessions: {}",
            snapshot.counts.catalog_failed
        );
        println!("stale_catalog_sessions: {}", snapshot.counts.catalog_stale);
        println!("{}", storage_status::human_total_query(&snapshot));
        for warning in storage_status::status_warnings(snapshot.available_space_bytes) {
            println!("warning: {warning}");
        }
        for diagnostic in &snapshot.diagnostics {
            println!("diagnostic: {diagnostic}");
        }
        println!("local_only: true");
    }
    Ok(())
}

fn run_sources(args: JsonArgs, data_root: PathBuf) -> Result<()> {
    let sources = discovered_sources();
    let plugin_discovery = discover_history_source_plugins_with_diagnostics(&data_root, &[])?;
    let plugin_sources = plugin_discovery.sources;
    let plugin_failures = plugin_discovery.failures;
    if args.json {
        print_json(query_sources_json(
            &sources,
            &plugin_source_projections(&plugin_sources),
            &plugin_failure_projections(&plugin_failures),
            false,
        ))?;
    } else {
        for source in sources {
            println!(
                "{} {} {} ({})",
                source.provider.as_str(),
                source.path.display(),
                source.status.as_str(),
                source.source_format
            );
        }
        for failure in plugin_failures {
            println!(
                "custom history-source-plugin invalid: {}: {}",
                failure.manifest_path.display(),
                failure.error
            );
        }
        for source in plugin_sources {
            println!(
                "custom {} available (history-source-plugin:{})",
                source.label(),
                source.source_format
            );
        }
    }
    Ok(())
}

fn catalog_available_sources(
    store: &Store,
    sources: &[SourceInfo],
) -> Result<(CatalogTotals, Vec<Value>)> {
    let mut totals = CatalogTotals::default();
    let mut catalog_sources = Vec::new();
    for source in sources {
        if source.provider != CaptureProvider::Codex
            || source.source_format != "codex_session_jsonl_tree"
            || !source.exists
            || source.status != ProviderSourceStatus::Available
        {
            continue;
        }
        let summary = catalog_codex_session_tree(
            &source.path,
            store,
            CodexSessionCatalogOptions {
                source_root: Some(source.path.clone()),
                allow_partial_failures: true,
                ..CodexSessionCatalogOptions::default()
            },
        )
        .with_context(|| format!("catalog Codex sessions from {}", source.path.display()))?;
        totals.add(&summary);
        catalog_sources.push(json!({
            "provider": source.provider.as_str(),
            "path": source.path,
            "source_format": source.source_format,
            "source_files": summary.source_files,
            "source_bytes": summary.source_bytes,
            "cataloged_sessions": summary.cataloged_sessions,
            "cached_sessions": summary.cached_sessions,
            "parsed_sessions": summary.parsed_sessions,
            "skipped_sessions": summary.skipped_sessions,
            "failed_sessions": summary.failed_sessions,
        }));
    }
    Ok((totals, catalog_sources))
}

fn run_import(args: ImportArgs, data_root: PathBuf) -> Result<()> {
    let json = args.json;
    let progress = args.progress;
    let report = run_import_internal(
        &args,
        data_root,
        ImportRunOptions {
            progress,
            json,
            print_human: !json,
            allow_empty_sources: false,
            include_history_source_plugins: true,
            operation: "import",
        },
    )?;
    emit_import_health_warnings(&report, progress);
    print_import_report(&report, json)?;
    if args.strict && (report.has_zero_yield_anomaly() || report.health_persistence_failures > 0) {
        if progress == ProgressArg::Json {
            eprintln!(
                "{}",
                progress_event_json("import", "error", "ctx import --strict detected zero-yield anomaly; report was printed, retry an explicit provider or run `ctx doctor`", true, Some("zero_yield_anomaly_strict"))
            );
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
            return Err(SilentExit { code: 1 }.into());
        }
        return Err(anyhow!(
            "ctx import --strict detected zero-yield anomaly; report was printed, retry an explicit provider or run `ctx doctor`"
        ));
    }
    Ok(())
}

fn run_import_internal(
    args: &ImportArgs,
    data_root: PathBuf,
    options: ImportRunOptions,
) -> Result<ImportReport> {
    validate_import_args(args)?;
    fs::create_dir_all(&data_root)?;
    config::write_default_config(&data_root)?;
    let db_path = database_path(data_root.clone());
    let mut store = Store::open(&db_path)?;
    let mut totals = ImportTotals::default();
    let mut imported_sources = Vec::new();
    let mut health_persistence_failures = 0usize;

    if let Some(format) = args.format {
        return run_explicit_format_import(args, format, db_path, store, options);
    }

    let requests = import_requests(args)?;
    let plugin_requests = history_source_plugin_import_requests(
        args,
        &data_root,
        options.include_history_source_plugins,
    )?;
    if requests.is_empty() && plugin_requests.is_empty() {
        if options.allow_empty_sources {
            return Ok(ImportReport::empty(args.resume));
        }
        return Err(anyhow!(
            "no importable provider history sources found; use --path, --history-source, or run `ctx sources`"
        ));
    }

    let mut planned_sources = Vec::new();
    let mut planned_total_bytes = 0u64;
    for source in requests {
        let stats = source_stats(&source.path)
            .with_context(|| format!("scan import source {}", source.path.display()))?;
        planned_total_bytes = planned_total_bytes.saturating_add(stats.bytes);
        planned_sources.push((source, stats));
    }

    let progress = ProgressReporter::new(
        options.progress,
        options.json,
        options.operation,
        planned_total_bytes,
    );
    let allow_source_failures = args.all && args.path.is_none();
    progress.message(
        "discovering",
        format!(
            "found {} import source(s), {}",
            planned_sources.len().saturating_add(plugin_requests.len()),
            format_bytes(planned_total_bytes)
        ),
    );
    if let Some(warning) = low_disk_space_warning(&db_path, planned_total_bytes) {
        progress.warning(warning);
    }
    if let Some(warning) = large_import_warning(&planned_sources, planned_total_bytes) {
        progress.warning(warning);
    }

    for plugin_source in plugin_requests {
        if options.print_human {
            progress.finish_line();
            println!("importing history source plugin {}", plugin_source.label());
        }
        progress.message(
            "indexing",
            format!("running history source plugin {}", plugin_source.label()),
        );
        match import_history_source_plugin(
            &mut store,
            &plugin_source,
            &data_root,
            args.reset_cursor,
        ) {
            Ok(outcome) => {
                let summary = outcome.summary;
                let stats = outcome.stats;
                let health = history_source_plugin_health(&stats, &summary, outcome.source_only);
                if let Err(err) = persist_source_health(
                    &store,
                    &plugin_source.provider_key,
                    &plugin_source.source_format,
                    &plugin_source.manifest_dir,
                    &plugin_source.source_id,
                    &health,
                    true,
                ) {
                    health_persistence_failures += 1;
                    emit_health_persistence_warning(options.progress, &err);
                }
                totals.add_with_health(&summary, &stats, &health);
                progress.done(
                    "indexing",
                    format!("imported history source plugin {}", plugin_source.label()),
                    planned_total_bytes,
                );
                if options.print_human {
                    progress.finish_line();
                    print_history_source_plugin_imported(&plugin_source, &summary);
                }
                imported_sources.push(history_source_plugin_import_json_with_source_only(
                    &plugin_source,
                    &stats,
                    &summary,
                    outcome.source_only,
                ));
            }
            Err(err) => {
                let error = error_summary(&err);
                if allow_source_failures && !import_error_is_systemic(&error) {
                    totals.add_source_failure(&SourceStats::default());
                    progress.done(
                        "indexing",
                        format!(
                            "skipped history source plugin {}: {}",
                            plugin_source.label(),
                            one_line_error(&error)
                        ),
                        planned_total_bytes,
                    );
                    if options.print_human {
                        progress.finish_line();
                        print_history_source_plugin_failed(&plugin_source, &error);
                    }
                    imported_sources
                        .push(history_source_plugin_failure_json(&plugin_source, &error));
                } else {
                    return Err(err);
                }
            }
        }
    }

    let native_import_requested = !planned_sources.is_empty();
    if should_parallelize_import(&planned_sources) {
        let final_refresh_required = store.event_search_projection_needs_backfill()?
            || planned_sources
                .iter()
                .any(|(source, _)| !source_uses_incremental_event_search(source));
        drop(store);

        if options.print_human {
            progress.finish_line();
            println!("sources:");
            for (source, stats) in &planned_sources {
                println!(
                    "  {} {} ({} files, {})",
                    source.provider.as_str(),
                    source.path.display(),
                    stats.files,
                    format_bytes(stats.bytes)
                );
            }
        }

        let source_states = Arc::new(Mutex::new(
            planned_sources
                .iter()
                .map(|(_, stats)| SourceProgressSnapshot {
                    completed_bytes: 0,
                    total_bytes: stats.bytes,
                })
                .collect::<Vec<_>>(),
        ));
        let handles = planned_sources
            .into_iter()
            .enumerate()
            .map(|(index, (source, stats))| {
                let db_path = db_path.clone();
                let progress_callback = progress.parallel_codex_import_callback(
                    &source,
                    index,
                    Arc::clone(&source_states),
                );
                let full_rescan = args.resume;
                let join_source = source.clone();
                let join_stats = stats;
                let handle = thread::spawn(move || -> ImportSourceRun {
                    let result = (|| -> Result<ProviderImportSummary> {
                        let mut store = Store::open(&db_path)?;
                        import_one_source_without_search_refresh(
                            &mut store,
                            &source,
                            progress_callback,
                            full_rescan,
                        )
                        .with_context(|| {
                            format!(
                                "import {} source {}",
                                source.provider.as_str(),
                                source.path.display()
                            )
                        })
                    })();
                    match result {
                        Ok(summary) => ImportSourceRun::Imported(ImportSourceOutcome {
                            index,
                            source,
                            stats,
                            summary,
                        }),
                        Err(err) => {
                            let error = error_summary(&err);
                            ImportSourceRun::Failed(ImportSourceFailure {
                                index,
                                source,
                                stats,
                                error,
                            })
                        }
                    }
                });
                (index, join_source, join_stats, handle)
            })
            .collect::<Vec<_>>();

        let mut runs = Vec::with_capacity(handles.len());
        let mut first_error = None;
        for (index, source, stats, handle) in handles {
            match handle.join() {
                Ok(ImportSourceRun::Imported(outcome)) => {
                    runs.push(ImportSourceRun::Imported(outcome))
                }
                Ok(ImportSourceRun::Failed(failure)) => {
                    if !allow_source_failures || import_error_is_systemic(&failure.error) {
                        first_error.get_or_insert_with(|| {
                            anyhow!(
                                "import {} source {}: {}",
                                failure.source.provider.as_str(),
                                failure.source.path.display(),
                                failure.error
                            )
                        });
                    }
                    runs.push(ImportSourceRun::Failed(failure));
                }
                Err(_) => {
                    let failure = ImportSourceFailure {
                        index,
                        source,
                        stats,
                        error: "provider import worker panicked".to_owned(),
                    };
                    if !allow_source_failures {
                        first_error.get_or_insert_with(|| anyhow!("{}", failure.error));
                    }
                    runs.push(ImportSourceRun::Failed(failure));
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }

        runs.sort_by_key(ImportSourceRun::index);
        for run in runs {
            match run {
                ImportSourceRun::Imported(outcome) => {
                    let health = classify_import_health(&outcome.stats, &outcome.summary);
                    let health_store = Store::open(&db_path)?;
                    if let Err(err) = persist_source_health(
                        &health_store,
                        outcome.source.provider.as_str(),
                        outcome.source.source_format,
                        &outcome.source.path,
                        "",
                        &health,
                        args.resume || !source_uses_import_file_manifest(&outcome.source),
                    ) {
                        health_persistence_failures += 1;
                        emit_health_persistence_warning(options.progress, &err);
                    }
                    totals.add(&outcome.summary, &outcome.stats);
                    progress.parallel_source_done(
                        &outcome.source,
                        outcome.index,
                        &source_states,
                        outcome.stats,
                        &outcome.summary,
                    );
                    if options.print_human {
                        progress.finish_line();
                        print_source_imported(&outcome.source, &outcome.summary);
                    }
                    imported_sources.push(source_import_json(
                        &outcome.source,
                        &outcome.stats,
                        &outcome.summary,
                    ));
                }
                ImportSourceRun::Failed(failure) => {
                    totals.add_source_failure(&failure.stats);
                    progress.parallel_source_failed(
                        &failure.source,
                        failure.index,
                        &source_states,
                        failure.stats,
                        &failure.error,
                    );
                    if options.print_human {
                        progress.finish_line();
                        print_source_failed(&failure);
                    }
                    imported_sources.push(source_failure_json(&failure));
                }
            }
        }

        if final_refresh_required {
            progress.message("finalizing", "refreshing search index");
            let store = Store::open(&db_path)?;
            store.refresh_search_index()?;
        }
    } else {
        let mut completed_source_bytes = 0u64;
        for (source, stats) in planned_sources {
            if options.print_human {
                progress.finish_line();
                println!(
                    "importing {} {} ({} files, {})",
                    source.provider.as_str(),
                    source.path.display(),
                    stats.files,
                    format_bytes(stats.bytes)
                );
            }
            let source_progress = progress.codex_import_callback(&source, completed_source_bytes);
            completed_source_bytes = completed_source_bytes.saturating_add(stats.bytes);
            match import_one_source(&mut store, &source, source_progress, args.resume) {
                Ok(summary) => {
                    let health = classify_import_health(&stats, &summary);
                    if let Err(err) = persist_source_health(
                        &store,
                        source.provider.as_str(),
                        source.source_format,
                        &source.path,
                        "",
                        &health,
                        args.resume || !source_uses_import_file_manifest(&source),
                    ) {
                        health_persistence_failures += 1;
                        emit_health_persistence_warning(options.progress, &err);
                    }
                    totals.add(&summary, &stats);
                    progress.done(
                        "indexing",
                        format!("imported {}", source.provider.as_str()),
                        completed_source_bytes,
                    );
                    if options.print_human {
                        progress.finish_line();
                        print_source_imported(&source, &summary);
                    }
                    imported_sources.push(source_import_json(&source, &stats, &summary));
                }
                Err(err) => {
                    let error = error_summary(&err);
                    if allow_source_failures && !import_error_is_systemic(&error) {
                        let failure = ImportSourceFailure {
                            index: imported_sources.len(),
                            source,
                            stats,
                            error,
                        };
                        totals.add_source_failure(&failure.stats);
                        progress.done(
                            "indexing",
                            format!(
                                "skipped {}: {}",
                                failure.source.provider.as_str(),
                                source_error_reason(&failure.source, &failure.error)
                            ),
                            completed_source_bytes,
                        );
                        if options.print_human {
                            progress.finish_line();
                            print_source_failed(&failure);
                        }
                        imported_sources.push(source_failure_json(&failure));
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    if totals.imported_sessions > 0 || totals.imported_events > 0 || totals.imported_edges > 0 {
        progress.message("finalizing", "compacting search index");
        // One fixed positive FTS merge request instead of a full `optimize`:
        // ask SQLite for roughly 256 pages of work per existing projection.
        Store::open(&db_path)?.merge_search_index_bounded()?;
    }

    progress.message("finalizing", "checkpointing search database");
    Store::open(&db_path)?.checkpoint_wal_truncate_if_larger_than(WAL_TRUNCATE_MIN_BYTES)?;

    if options.print_human {
        progress.finish_line();
    }
    progress.done(
        "finalizing",
        format!("indexed {} source file(s)", totals.source_files),
        totals.source_bytes,
    );
    if totals.imported_sources == 0 && totals.failed_sources > 0 {
        let detail = imported_sources
            .iter()
            .find_map(|source| source.json.get("error").and_then(Value::as_str))
            .map(|error| format!("; first failure: {error}"))
            .unwrap_or_default();
        return Err(anyhow!("all import sources failed{detail}"));
    }
    Ok(ImportReport {
        resume: args.resume && native_import_requested,
        totals,
        sources: imported_sources,
        health_persistence_failures,
    })
}

fn run_explicit_format_import(
    args: &ImportArgs,
    format: ImportFormatArg,
    db_path: PathBuf,
    mut store: Store,
    options: ImportRunOptions,
) -> Result<ImportReport> {
    let path = args
        .path
        .as_ref()
        .context("--format requires an explicit --path")?;
    let stats =
        source_stats(path).with_context(|| format!("scan import source {}", path.display()))?;

    let progress = ProgressReporter::new(
        options.progress,
        options.json,
        options.operation,
        stats.bytes,
    );
    progress.message(
        "discovering",
        format!(
            "found 1 {} source, {}",
            format.as_str(),
            format_bytes(stats.bytes)
        ),
    );
    if let Some(warning) = low_disk_space_warning(&db_path, stats.bytes) {
        progress.warning(warning);
    }
    if (stats.files >= LARGE_IMPORT_SOURCE_FILES_WARNING
        || stats.bytes >= LARGE_IMPORT_SOURCE_BYTES_WARNING)
        && stats.files > 0
    {
        let warning = format!(
            "large import: {} source file(s), {}; initial indexing may use sustained CPU and disk",
            stats.files,
            format_bytes(stats.bytes)
        );
        progress.warning(warning);
    }

    let validation = match format {
        ImportFormatArg::CtxHistoryJsonlV1 => {
            validate_custom_history_jsonl_v1(path).map_err(anyhow::Error::from)?
        }
    };
    if validation.failed > 0 {
        return Err(explicit_format_import_failure(format, &validation));
    }

    let record = import_record_for_custom_history(path, format);
    let record_id = record.id;
    store.upsert_record(&record)?;
    progress.message("indexing", format!("importing {}", format.as_str()));
    let summary = match format {
        ImportFormatArg::CtxHistoryJsonlV1 => import_custom_history_jsonl_v1(
            path,
            &mut store,
            CustomHistoryJsonlV1ImportOptions {
                source_path: Some(path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: false,
                ..CustomHistoryJsonlV1ImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from)?,
    };
    if summary.failed > 0 {
        return Err(explicit_format_import_failure(format, &summary));
    }

    let health = classify_import_health(&stats, &summary);
    let mut health_persistence_failures = 0;
    if let Err(err) =
        persist_source_health(&store, "custom", format.as_str(), path, "", &health, true)
    {
        health_persistence_failures = 1;
        emit_health_persistence_warning(options.progress, &err);
    }
    let mut totals = ImportTotals::default();
    totals.add_with_health(&summary, &stats, &health);
    if totals.imported_sessions > 0 || totals.imported_events > 0 || totals.imported_edges > 0 {
        progress.message("finalizing", "compacting search index");
        // One fixed positive FTS merge request (see the normal import path).
        Store::open(&db_path)?.merge_search_index_bounded()?;
    }
    progress.message("finalizing", "checkpointing search database");
    Store::open(&db_path)?.checkpoint_wal_truncate_if_larger_than(WAL_TRUNCATE_MIN_BYTES)?;
    if options.print_human {
        progress.finish_line();
    }
    progress.done(
        "finalizing",
        format!("indexed 1 {} source file", format.as_str()),
        stats.bytes,
    );
    Ok(ImportReport {
        resume: args.resume,
        totals,
        sources: vec![custom_format_import_json(format, path, &stats, &summary)],
        health_persistence_failures,
    })
}

fn explicit_format_import_failure(
    format: ImportFormatArg,
    summary: &ProviderImportSummary,
) -> anyhow::Error {
    let detail = summary
        .failures
        .first()
        .map(|failure| format!("line {}: {}", failure.line, failure.error))
        .unwrap_or_else(|| "unknown validation failure".to_owned());
    anyhow!(
        "{} import failed with {} failure(s); first failure: {detail}",
        format.as_str(),
        summary.failed
    )
}

fn print_import_report(report: &ImportReport, json_output: bool) -> Result<()> {
    if json_output {
        print_json(import_report_json(report))
    } else {
        print_import_report_human(report);
        Ok(())
    }
}

fn import_report_json(report: &ImportReport) -> Value {
    json!({
        "schema_version": 1,
        "resume": report.resume,
        "resume_mode": report.resume_mode(),
        "totals": import_totals_json(&report.totals),
        "sources": report.sources.iter().map(|source| source.json.clone()).collect::<Vec<_>>(),
        "health_persistence_failed": report.health_persistence_failures,
    })
}

fn persist_source_health(
    store: &Store,
    provider: &str,
    format: &str,
    path: &Path,
    logical_id: &str,
    health: &ImportHealth,
    source_level_coverage: bool,
) -> Result<()> {
    if health.zero_yield_anomaly() {
        if source_level_coverage {
            store.upsert_source_health(
                provider,
                format,
                path,
                logical_id,
                SourceHealthClassification::ZeroYieldAnomaly,
            )?;
        }
        return Ok(());
    }
    let positive_health = matches!(
        health.classification,
        ImportHealthClassification::Success
            | ImportHealthClassification::PartialSuccess
            | ImportHealthClassification::AllSkipped
            | ImportHealthClassification::Empty
    );
    if !positive_health {
        return Ok(());
    }
    if source_level_coverage {
        store.upsert_source_health(
            provider,
            format,
            path,
            logical_id,
            SourceHealthClassification::Healthy,
        )?;
    } else {
        store.heal_source_health_if_present(provider, format, path, logical_id)?;
    }
    Ok(())
}

fn emit_health_persistence_warning(progress: ProgressArg, err: &anyhow::Error) {
    let message = format!(
        "source health could not be persisted after import: {}",
        one_line_error(&error_summary(err))
    );
    if progress == ProgressArg::Json {
        eprintln!(
            "{}",
            progress_event_json(
                "import",
                "warning",
                &message,
                false,
                Some("health_persistence_failed")
            )
        );
    } else {
        eprintln!("warning: health_persistence_failed: {message}");
    }
}

fn import_totals_json(totals: &ImportTotals) -> Value {
    json!({
        "source_files": totals.source_files,
        "source_bytes": totals.source_bytes,
        "imported_sources": totals.imported_sources,
        "failed_sources": totals.failed_sources,
        "imported_sessions": totals.imported_sessions,
        "imported_events": totals.imported_events,
        "imported_edges": totals.imported_edges,
        "skipped": totals.skipped,
        "failed": totals.failed,
        "unchanged_sources": totals.unchanged_sources,
        "zero_yield_anomaly_sources": totals.zero_yield_anomaly_sources,
        "health_persistence_failed": totals.health_persistence_failures,
    })
}

fn emit_import_health_warnings(report: &ImportReport, progress: ProgressArg) {
    for source in &report.sources {
        let count = source.zero_yield_anomaly_count();
        if count > 0 {
            let provider = source
                .json
                .get("provider")
                .and_then(Value::as_str)
                .unwrap_or("source");
            let message = format!("import health detected zero-yield anomaly for {provider}; {count} import unit(s) yielded no sessions, events, or edges without a safe skip/empty reason. Next steps: run `ctx sources`, retry with an explicit provider, or run `ctx doctor`.");
            if progress == ProgressArg::Json {
                eprintln!(
                    "{}",
                    progress_event_json(
                        "import",
                        "warning",
                        &message,
                        false,
                        Some("zero_yield_anomaly")
                    )
                );
            } else {
                eprintln!("warning: {message}");
            }
        }
    }
}

fn progress_event_json(
    operation: &str,
    level: &str,
    message: &str,
    done: bool,
    code: Option<&str>,
) -> Value {
    json!({
        "type": "ctx_progress",
        "operation": operation,
        "phase": "import_health",
        "level": level,
        "code": code,
        "message": message,
        "completed_bytes": 0,
        "total_bytes": 0,
        "percent": 100.0,
        "elapsed_seconds": 0.0,
        "eta_seconds": null,
        "completed_files": null,
        "total_files": null,
        "imported_events": null,
        "done": done,
    })
}

fn print_import_report_human(report: &ImportReport) {
    println!("source_files: {}", report.totals.source_files);
    println!("source_bytes: {}", report.totals.source_bytes);
    println!("imported_sources: {}", report.totals.imported_sources);
    println!("failed_sources: {}", report.totals.failed_sources);
    println!("imported_sessions: {}", report.totals.imported_sessions);
    println!("imported_events: {}", report.totals.imported_events);
    println!("imported_edges: {}", report.totals.imported_edges);
    println!("skipped: {}", report.totals.skipped);
    println!("failed: {}", report.totals.failed);
    println!(
        "zero_yield_anomaly_sources: {}",
        report.totals.zero_yield_anomaly_sources
    );
    println!("resume: {}", report.resume);
    println!("resume_mode: {}", report.resume_mode());
}

#[derive(Debug)]
struct ImportSourceOutcome {
    index: usize,
    source: SourceInfo,
    stats: SourceStats,
    summary: ProviderImportSummary,
}

#[derive(Debug)]
struct ImportSourceFailure {
    index: usize,
    source: SourceInfo,
    stats: SourceStats,
    error: String,
}

#[derive(Debug)]
enum ImportSourceRun {
    Imported(ImportSourceOutcome),
    Failed(ImportSourceFailure),
}

impl ImportSourceRun {
    fn index(&self) -> usize {
        match self {
            Self::Imported(outcome) => outcome.index,
            Self::Failed(failure) => failure.index,
        }
    }
}

fn should_parallelize_import(planned_sources: &[(SourceInfo, SourceStats)]) -> bool {
    let _ = planned_sources;
    false
}

fn large_import_warning(
    planned_sources: &[(SourceInfo, SourceStats)],
    planned_total_bytes: u64,
) -> Option<String> {
    let planned_total_files = planned_sources
        .iter()
        .map(|(_, stats)| stats.files)
        .sum::<usize>();
    if planned_total_files < LARGE_IMPORT_SOURCE_FILES_WARNING
        && planned_total_bytes < LARGE_IMPORT_SOURCE_BYTES_WARNING
    {
        return None;
    }
    Some(format!(
        "large import: {} source file(s), {}; initial indexing may use sustained CPU and disk",
        planned_total_files,
        format_bytes(planned_total_bytes)
    ))
}

fn source_import_json(
    source: &SourceInfo,
    stats: &SourceStats,
    summary: &ProviderImportSummary,
) -> ImportSourceReport {
    let health = classify_import_health(stats, summary);
    let json = json!({
        "status": "imported",
        "health": health.to_json(),
        "provider": source.provider.as_str(),
        "path": source.path,
        "source_format": source.source_format,
        "source_files": stats.files,
        "source_bytes": stats.bytes,
        "scanned_files": stats.files,
        "scanned_bytes": stats.bytes,
        "imported_sessions": summary.imported_sessions,
        "imported_events": summary.imported_events,
        "imported_edges": summary.imported_edges,
        "skipped": summary.skipped,
        "skipped_reasons": skipped_reasons_json(summary),
        "failed": summary.failed,
        "malformed_or_unsupported_count": summary.failed,
        "failures": provider_failures_json(summary),
        "notes": summary.notes,
    });
    ImportSourceReport { health, json }
}

fn custom_format_import_json(
    format: ImportFormatArg,
    path: &Path,
    stats: &SourceStats,
    summary: &ProviderImportSummary,
) -> ImportSourceReport {
    let health = classify_import_health(stats, summary);
    let json = json!({
        "status": "imported",
        "health": health.to_json(),
        "provider": CaptureProvider::Custom.as_str(),
        "path": path,
        "format": format.as_str(),
        "source_format": format.as_str(),
        "source_files": stats.files,
        "source_bytes": stats.bytes,
        "scanned_files": stats.files,
        "scanned_bytes": stats.bytes,
        "imported_sessions": summary.imported_sessions,
        "imported_events": summary.imported_events,
        "imported_edges": summary.imported_edges,
        "skipped": summary.skipped,
        "skipped_reasons": skipped_reasons_json(summary),
        "failed": summary.failed,
        "malformed_or_unsupported_count": summary.failed,
        "failures": provider_failures_json(summary),
    });
    ImportSourceReport { health, json }
}

fn history_source_plugin_import_json_with_source_only(
    source: &HistorySourcePluginSource,
    stats: &SourceStats,
    summary: &ProviderImportSummary,
    source_only: bool,
) -> ImportSourceReport {
    let health = history_source_plugin_health(stats, summary, source_only);
    let json = json!({
        "status": "imported",
        "health": health.to_json(),
        "provider": CaptureProvider::Custom.as_str(),
        "kind": "history_source_plugin",
        "plugin": source.plugin_name,
        "history_source": source.label(),
        "provider_key": source.provider_key,
        "source_id": source.source_id,
        "source_format": source.source_format,
        "manifest_path": source.manifest_path,
        "source_files": stats.files,
        "source_bytes": stats.bytes,
        "scanned_files": stats.files,
        "scanned_bytes": stats.bytes,
        "imported_sessions": summary.imported_sessions,
        "imported_events": summary.imported_events,
        "imported_edges": summary.imported_edges,
        "skipped": summary.skipped,
        "skipped_reasons": skipped_reasons_json(summary),
        "failed": summary.failed,
        "malformed_or_unsupported_count": summary.failed,
        "failures": provider_failures_json(summary),
    });
    ImportSourceReport { health, json }
}

fn provider_failures_json(summary: &ProviderImportSummary) -> Vec<Value> {
    summary
        .failures
        .iter()
        .take(5)
        .map(|failure| {
            json!({
                "line": failure.line,
                "error": failure.error,
            })
        })
        .collect()
}

fn history_source_plugin_health(
    stats: &SourceStats,
    summary: &ProviderImportSummary,
    source_only: bool,
) -> ImportHealth {
    if history_source_plugin_cursor_only(summary, source_only) {
        ImportHealth {
            classification: ImportHealthClassification::Unchanged,
            reason_counts: json!({"plugin_cursor_only": 1}),
            zero_yield_anomaly_count: 0,
        }
    } else {
        classify_import_health(stats, summary)
    }
}

fn history_source_plugin_cursor_only(summary: &ProviderImportSummary, source_only: bool) -> bool {
    let imported = summary.imported_sessions + summary.imported_events + summary.imported_edges;
    imported == 0 && summary.failed == 0 && summary.skipped == 0 && source_only
}

fn classify_import_health(stats: &SourceStats, summary: &ProviderImportSummary) -> ImportHealth {
    let imported = summary.imported_sessions + summary.imported_events + summary.imported_edges;
    let classification = if imported > 0 && (summary.failed > 0 || summary.zero_yield_anomalies > 0)
    {
        ImportHealthClassification::PartialSuccess
    } else if summary.unchanged_sources > 0
        && imported == 0
        && summary.failed == 0
        && summary.skipped == 0
        && summary.zero_yield_anomalies == 0
    {
        ImportHealthClassification::Unchanged
    } else if summary.zero_yield_anomalies > 0 {
        ImportHealthClassification::ZeroYieldAnomaly
    } else if summary.failed > 0 {
        ImportHealthClassification::UnsupportedOrMalformed
    } else if imported > 0 {
        ImportHealthClassification::Success
    } else if summary.empty_sources > 0 || summary.empty_files > 0 {
        ImportHealthClassification::Empty
    } else if summary.skipped > 0 {
        ImportHealthClassification::AllSkipped
    } else if stats.bytes == 0 {
        ImportHealthClassification::Empty
    } else {
        ImportHealthClassification::ZeroYieldAnomaly
    };
    let mut reasons = serde_json::Map::new();
    if summary.skipped > 0 {
        reasons.insert("unspecified".to_owned(), json!(summary.skipped));
    }
    if summary.failed > 0 {
        reasons.insert("malformed_or_unsupported".to_owned(), json!(summary.failed));
    }
    if summary.zero_yield_anomalies > 0 {
        reasons.insert(
            "zero_yield_anomaly".to_owned(),
            json!(summary.zero_yield_anomalies),
        );
    }
    if summary.unchanged_sources > 0 {
        reasons.insert(
            "no_pending_files".to_owned(),
            json!(summary.unchanged_sources),
        );
    }
    if summary.empty_sources > 0 || summary.empty_files > 0 {
        reasons.insert(
            "empty".to_owned(),
            json!(summary.empty_sources + summary.empty_files),
        );
    }
    let zero_yield_anomaly_count = if summary.zero_yield_anomalies > 0 {
        summary.zero_yield_anomalies
    } else if classification == ImportHealthClassification::ZeroYieldAnomaly {
        1
    } else {
        0
    };
    ImportHealth {
        classification,
        reason_counts: Value::Object(reasons),
        zero_yield_anomaly_count,
    }
}

fn skipped_reasons_json(summary: &ProviderImportSummary) -> Value {
    if summary.skipped == 0 {
        json!({})
    } else {
        json!({"unspecified": summary.skipped})
    }
}

fn source_failure_json(failure: &ImportSourceFailure) -> ImportSourceReport {
    let health = ImportHealth {
        classification: ImportHealthClassification::Failed,
        reason_counts: json!({"failure": 1}),
        zero_yield_anomaly_count: 0,
    };
    let json = json!({
        "status": "failed",
        "health": health.to_json(),
        "provider": failure.source.provider.as_str(),
        "path": failure.source.path,
        "source_format": failure.source.source_format,
        "source_files": failure.stats.files,
        "source_bytes": failure.stats.bytes,
        "scanned_files": failure.stats.files,
        "scanned_bytes": failure.stats.bytes,
        "imported_sessions": 0,
        "imported_events": 0,
        "imported_edges": 0,
        "skipped": 0,
        "failed": 1,
        "skipped_reasons": {},
        "malformed_or_unsupported_count": 1,
        "error": source_error_reason(&failure.source, &failure.error),
    });
    ImportSourceReport { health, json }
}

fn history_source_plugin_failure_json(
    source: &HistorySourcePluginSource,
    error: &str,
) -> ImportSourceReport {
    let health = ImportHealth {
        classification: ImportHealthClassification::Failed,
        reason_counts: json!({"failure": 1}),
        zero_yield_anomaly_count: 0,
    };
    let json = json!({
        "status": "failed",
        "health": health.to_json(),
        "provider": CaptureProvider::Custom.as_str(),
        "kind": "history_source_plugin",
        "plugin": source.plugin_name,
        "history_source": source.label(),
        "provider_key": source.provider_key,
        "source_id": source.source_id,
        "source_format": source.source_format,
        "manifest_path": source.manifest_path,
        "source_files": 0,
        "source_bytes": 0,
        "scanned_files": 0,
        "scanned_bytes": 0,
        "imported_sessions": 0,
        "imported_events": 0,
        "imported_edges": 0,
        "skipped": 0,
        "failed": 1,
        "skipped_reasons": {},
        "malformed_or_unsupported_count": 1,
        "error": one_line_error(error),
    });
    ImportSourceReport { health, json }
}

fn print_source_imported(source: &SourceInfo, summary: &ProviderImportSummary) {
    println!(
        "imported {}: sessions={} events={} edges={} skipped={} failed={}",
        source.provider.as_str(),
        summary.imported_sessions,
        summary.imported_events,
        summary.imported_edges,
        summary.skipped,
        summary.failed
    );
    for note in &summary.notes {
        println!("  note: {note}");
    }
}

fn print_history_source_plugin_imported(
    source: &HistorySourcePluginSource,
    summary: &ProviderImportSummary,
) {
    println!(
        "imported history source plugin {}: sessions={} events={} edges={} skipped={} failed={}",
        source.label(),
        summary.imported_sessions,
        summary.imported_events,
        summary.imported_edges,
        summary.skipped,
        summary.failed
    );
}

fn print_source_failed(failure: &ImportSourceFailure) {
    println!(
        "skipped {}: {}",
        failure.source.provider.as_str(),
        source_error_reason(&failure.source, &failure.error)
    );
    println!("  path: {}", failure.source.path.display());
}

fn print_history_source_plugin_failed(source: &HistorySourcePluginSource, error: &str) {
    println!(
        "skipped history source plugin {}: {}",
        source.label(),
        one_line_error(error)
    );
    println!("  manifest: {}", source.manifest_path.display());
}

fn source_error_reason(source: &SourceInfo, error: &str) -> String {
    let error = one_line_error(error);
    let prefix = format!(
        "import {} source {}: ",
        source.provider.as_str(),
        source.path.display()
    );
    error.strip_prefix(&prefix).unwrap_or(&error).to_owned()
}

fn one_line_error(error: &str) -> String {
    error
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("unknown error")
        .to_owned()
}

fn error_summary(error: &anyhow::Error) -> String {
    let top = error.to_string();
    let root = error
        .chain()
        .last()
        .map(ToString::to_string)
        .unwrap_or_else(|| top.clone());
    if is_sqlite_busy_text(&top) || is_sqlite_busy_text(&root) {
        return "ctx index is busy because another ctx import or search refresh is writing to the local database; retry in a moment, or rerun the search with `--refresh off` to use the existing index".to_owned();
    }
    if root == top || top.contains(&root) {
        top
    } else {
        format!("{top}: {root}")
    }
}

fn is_sqlite_busy_text(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database is locked") || lower.contains("database table is locked")
}

fn import_error_is_systemic(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database or disk is full")
        || lower.contains("ctx index is busy")
        || lower.contains("database is locked")
        || lower.contains("readonly database")
        || lower.contains("disk i/o error")
        || lower.contains("out of memory")
}

fn low_disk_space_warning(db_path: &Path, planned_total_bytes: u64) -> Option<String> {
    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    let available = available_space_bytes(parent)?;
    let recommended = (planned_total_bytes / 4).clamp(1 << 30, 20 * (1 << 30));
    if available < recommended {
        Some(format!(
            "low disk space: {} available near {}, {} recommended before indexing {}",
            format_bytes(available),
            parent.display(),
            format_bytes(recommended),
            format_bytes(planned_total_bytes)
        ))
    } else {
        None
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

fn run_show(args: ShowArgs, data_root: PathBuf) -> Result<()> {
    let db_path = database_path(data_root);
    match args.target {
        ShowTarget::Session(mut args) => {
            let format = effective_format(args.format, args.json);
            let store = match open_existing_store_read_only(&db_path, "ctx show") {
                Ok(store) => store,
                Err(error) => return paged_request_error(format, "store_error", error),
            };
            let session = match resolve_session(
                &store,
                args.id.clone(),
                args.provider.map(ProviderArg::capture_provider),
                args.provider_session.as_deref(),
            ) {
                Ok(session) => session,
                Err(error) => return paged_request_error(format, "session_lookup_error", error),
            };
            args.id = Some(session.id.to_string());
            let byte_policy = BytePolicy {
                per_item_bytes: args.max_event_bytes,
                page_bytes: args.max_page_bytes,
            };
            let page = QueryService::new(&store).session_events(
                session.clone(),
                args.mode.into(),
                args.limit,
                args.continuation.as_deref(),
                args.fields.into(),
                byte_policy,
            );
            let page = match page {
                Ok(page) => page,
                Err(err) => return paged_query_error(format, err),
            };
            let result = write_rendered_session_page(&page, &args, format);
            if args.out.is_none() {
                finish_paged_stdout(result)?;
            } else {
                // Explicit destinations (including FIFOs) are caller-owned;
                // never reinterpret their I/O failures as stdout early-close.
                result?;
            }
        }
        ShowTarget::Event(args) => {
            let format = effective_format(args.format, args.json);
            let store = match open_existing_store_read_only(&db_path, "ctx show") {
                Ok(store) => store,
                Err(error) => return paged_request_error(format, "store_error", error),
            };
            let event = match resolve_event(&store, &args.id) {
                Ok(event) => event,
                Err(error) => return paged_request_error(format, "event_lookup_error", error),
            };
            let events = match event_window(&store, &event, args.before, args.after, args.window) {
                Ok(events) => events,
                Err(error) => return paged_request_error(format, "event_window_error", error),
            };
            write_rendered_events(&store, &event, &events, format, None)?;
        }
    }
    Ok(())
}

fn event_preview(event: &Event) -> String {
    let preview = ctx_history_search::event_preview_text(event);
    if preview.trim().is_empty() {
        format!("{} event", event.event_type.as_str())
    } else {
        ctx_history_search::display_snippet(&preview, 120)
    }
}

fn run_locate(args: LocateArgs, data_root: PathBuf) -> Result<()> {
    let store = open_existing_store_read_only(&database_path(data_root), "ctx locate")?;
    match args.target {
        LocateTarget::Session(args) => {
            let session = resolve_session(
                &store,
                args.id,
                args.provider.map(ProviderArg::capture_provider),
                args.provider_session.as_deref(),
            )?;
            let value = serde_json::to_value(QueryService::new(&store).locate_session(&session)?)?;
            if locate_json_output(args.format, args.json) {
                print_json(value)?;
            } else {
                print_locate_session_text(&value)?;
            }
        }
        LocateTarget::Event(args) => {
            let event = resolve_event(&store, &args.id)?;
            let value = serde_json::to_value(QueryService::new(&store).locate_event(&event)?)?;
            if locate_json_output(args.format, args.json) {
                print_json(value)?;
            } else {
                print_locate_event_text(&value)?;
            }
        }
    }
    Ok(())
}

fn effective_format(format: OutputFormat, json: bool) -> OutputFormat {
    if json {
        OutputFormat::Json
    } else {
        format
    }
}

fn locate_json_output(format: LocateFormat, json: bool) -> bool {
    json || format == LocateFormat::Json
}

fn resolve_session(
    store: &Store,
    id: Option<String>,
    provider: Option<CaptureProvider>,
    provider_session: Option<&str>,
) -> Result<Session> {
    if let Some(id) = id {
        return resolve_session_by_id_text(store, &id);
    }
    let provider = provider.ok_or_else(|| {
        anyhow!(
            "session lookup requires either a ctx session id or --provider with --provider-session"
        )
    })?;
    let provider_session = provider_session
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!("session lookup requires --provider-session when no ctx session id is provided")
        })?;
    let matches = store.sessions_by_external_session(provider, provider_session, 2)?;
    match matches.as_slice() {
        [session] => Ok(session.clone()),
        [] => Err(anyhow!(
            "no {provider} session with provider_session_id {provider_session:?} is indexed"
        )),
        _ => Err(anyhow!(
            "multiple {provider} sessions with provider_session_id {provider_session:?} are indexed; use ctx_session_id"
        )),
    }
}

fn event_window(
    store: &Store,
    event: &Event,
    before: usize,
    after: usize,
    window: Option<usize>,
) -> Result<Vec<Event>> {
    let (before, after) = window
        .map(|window| (window, window))
        .unwrap_or((before, after));
    Ok(store.event_window_bounded(event.id, before, after)?)
}

fn write_rendered_session_page(
    page: &EventPageV1,
    args: &ShowSessionArgs,
    format: OutputFormat,
) -> Result<()> {
    if let Some(path) = &args.out {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let file = fs::File::create(path).with_context(|| format!("write {}", path.display()))?;
        let mut writer = std::io::BufWriter::new(file);
        return write_session_page(&mut writer, page, args, format);
    }
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    write_session_page(&mut writer, page, args, format)
}

fn write_session_page<W: Write + ?Sized>(
    writer: &mut W,
    page: &EventPageV1,
    args: &ShowSessionArgs,
    format: OutputFormat,
) -> Result<()> {
    let next_argv = show_session_next_argv(args, format, page.pagination.continuation.as_deref());
    let next_command = next_argv.as_ref().map(|argv| command_from_argv(argv));
    match format {
        OutputFormat::Json => {
            let value = event_page_json(page, format, next_command, next_argv)?;
            writer.write_all(&serde_json::to_vec_pretty(&value)?)?;
            writer.write_all(b"\n")?;
        }
        OutputFormat::Jsonl => write_session_jsonl(writer, page, next_command, next_argv)?,
        OutputFormat::Text | OutputFormat::Markdown => {
            let markdown = format == OutputFormat::Markdown;
            let rendered = render_event_page(page, markdown, next_command.as_deref());
            writer.write_all(rendered.as_bytes())?;
        }
    }
    writer.flush()?;
    Ok(())
}

pub(crate) fn event_page_json(
    page: &EventPageV1,
    format: OutputFormat,
    next_command: Option<String>,
    next_argv: Option<Vec<String>>,
) -> Result<Value> {
    let mut value = serde_json::to_value(page)?;
    let object = value
        .as_object_mut()
        .expect("query event page serializes as an object");
    object.insert("target".into(), json!("session"));
    object.insert("item_type".into(), json!("session_transcript"));
    object.insert("format".into(), json!(format.as_str()));
    object.insert("next".into(), json!(page.pagination.continuation));
    object.insert("next_command".into(), json!(next_command));
    object.insert("next_argv".into(), json!(next_argv));
    object.insert("total_events".into(), json!(page.selected_total));
    object.insert(
        "omitted_events".into(),
        json!(page.omitted.before.saturating_add(page.omitted.after)),
    );
    if let Some(pagination) = object.get_mut("pagination").and_then(Value::as_object_mut) {
        pagination.insert("cursor".into(), json!(page.pagination.continuation));
    }
    object.insert("share_safe".into(), Value::Bool(false));
    if let Some(session) = object.get("session") {
        if let Some(id) = session.get("ctx_session_id").cloned() {
            object.insert("ctx_session_id".into(), id);
        }
    }
    Ok(value)
}

fn render_event_page(page: &EventPageV1, markdown: bool, next_command: Option<&str>) -> String {
    let mut out = String::new();
    if markdown {
        out.push_str("# Session transcript\n\n");
    }
    render_session_projection(&mut out, &page.session, markdown);
    if markdown {
        out.push_str(&format!("- mode: `{:?}`\n\n", page.mode).to_lowercase());
    } else {
        out.push_str(&format!("mode: {:?}\n\n", page.mode).to_lowercase());
    }
    for event in &page.events {
        render_event_projection(&mut out, event, markdown);
    }
    render_page_summary(
        &mut out,
        PageSummary {
            noun: "events",
            offset: page.pagination.offset,
            returned: page.pagination.returned_items,
            omitted_before: page.omitted.before,
            omitted_after: page.omitted.after,
            exact: page.omitted.exact,
            next_command,
        },
        markdown,
    );
    out
}

fn render_session_projection(out: &mut String, session: &SessionProjectionV1, markdown: bool) {
    let (id, provider, provider_session) = match session {
        SessionProjectionV1::Full(session) => (
            session.ctx_session_id,
            session.provider,
            session.provider_session_id.as_deref(),
        ),
        SessionProjectionV1::Compact(session) => (session.ctx_session_id, session.provider, None),
    };
    if markdown {
        out.push_str(&format!(
            "- ctx_session_id: `{id}`\n- provider: `{provider}`\n"
        ));
        if let Some(value) = provider_session {
            out.push_str(&format!("- provider_session_id: `{value}`\n"));
        }
        out.push('\n');
    } else {
        out.push_str(&format!("ctx_session_id: {id}\nprovider: {provider}\n"));
        if let Some(value) = provider_session {
            out.push_str(&format!("provider_session_id: {value}\n"));
        }
        out.push('\n');
    }
}

fn render_event_projection(out: &mut String, event: &EventProjectionV1, markdown: bool) {
    let (id, seq, event_type, role, occurred_at, text) = match event {
        EventProjectionV1::Full(event) => (
            event.ctx_event_id,
            event.seq,
            event.event_type,
            event.role,
            event.occurred_at,
            event.text.as_str(),
        ),
        EventProjectionV1::Compact(event) => (
            event.ctx_event_id,
            event.seq,
            event.event_type,
            event.role,
            event.occurred_at,
            event.text.as_str(),
        ),
    };
    let role = role.map(|role| role.as_str()).unwrap_or("-");
    if markdown {
        out.push_str(&format!(
            "## {role} - {} - {occurred_at}\n\nctx_event_id: `{id}`  \nsequence: `{seq}`\n\n{text}\n\n",
            event_type.as_str()
        ));
    } else {
        out.push_str(&format!(
            "[{occurred_at}] {role} {} {id} seq={seq}\n{text}\n\n",
            event_type.as_str()
        ));
    }
}

fn write_rendered_events(
    store: &Store,
    selected: &Event,
    events: &[Event],
    format: OutputFormat,
    out: Option<PathBuf>,
) -> Result<()> {
    let body = match format {
        OutputFormat::Text => render_events_text(store, selected, events),
        OutputFormat::Markdown => render_events_markdown(store, selected, events),
        OutputFormat::Json => {
            serde_json::to_string_pretty(&event_window_json(store, selected, events, format))?
        }
        OutputFormat::Jsonl => render_events_jsonl(store, events)?,
    };
    write_output(body, out)
}

fn write_output(body: String, out: Option<PathBuf>) -> Result<()> {
    if let Some(out) = out {
        if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        fs::write(&out, body).with_context(|| format!("write {}", out.display()))?;
    } else {
        print!("{body}");
        if !body.ends_with('\n') {
            println!();
        }
    }
    Ok(())
}

fn event_content(event: &Event) -> String {
    if matches!(
        event.redaction_state,
        RedactionState::Raw | RedactionState::Withheld
    ) {
        return "raw event payload withheld".to_owned();
    }
    if let Some(value) = event.payload.get("body").and_then(event_value_text) {
        return ctx_history_search::display_snippet(&value, 16_000);
    }
    if let Some(value) = event_value_text(&event.payload) {
        return ctx_history_search::display_snippet(&value, 16_000);
    }
    let preview = ctx_history_search::event_preview_text(event);
    if preview.trim().is_empty() {
        format!("{} event", event.event_type.as_str())
    } else {
        ctx_history_search::display_snippet(&preview, 16_000)
    }
}

fn event_value_text(value: &Value) -> Option<String> {
    if let Some(value) = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(value.to_owned());
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
        if let Some(value) = object
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(value.to_owned());
        }
    }
    let structured = ["tool", "name", "arguments_preview", "status"]
        .into_iter()
        .filter_map(|key| object.get(key).and_then(|value| value.as_str()))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if structured.is_empty() {
        None
    } else {
        Some(structured.join(" "))
    }
}

fn resolve_session_by_id_text(store: &Store, value: &str) -> Result<Session> {
    let prefix = parse_id_prefix(value, "session")?;
    if let Some(id) = prefix.full_uuid() {
        return store.get_session(id).with_context(|| {
            format!("session {id} was not found; rerun the search that found it with `--verbose` to get ctx_session_id")
        });
    }
    match store.resolve_session_by_id_prefix(&prefix)? {
        IdPrefixResolution::Found(session) => Ok(session),
        IdPrefixResolution::NotFound => Err(anyhow!(
            "session id prefix {:?} was not found; rerun the search that found it with `--verbose` to get ctx_session_id",
            prefix.canonical()
        )),
        IdPrefixResolution::Ambiguous(ambiguity) => Err(anyhow!(ambiguity.message("session", &prefix))),
    }
}

fn resolve_session_id(store: &Store, value: &str) -> Result<Uuid> {
    Ok(resolve_session_by_id_text(store, value)?.id)
}

fn resolve_event(store: &Store, value: &str) -> Result<Event> {
    let prefix = parse_id_prefix(value, "event")?;
    if let Some(id) = prefix.full_uuid() {
        return store.get_event(id).with_context(|| {
            format!(
                "event {id} was not found; rerun the event search with `--events --verbose` to get ctx_event_id"
            )
        });
    }
    match store.resolve_event_by_id_prefix(&prefix)? {
        IdPrefixResolution::Found(event) => Ok(event),
        IdPrefixResolution::NotFound => Err(anyhow!(
            "event id prefix {:?} was not found; rerun the event search with `--events --verbose` to get ctx_event_id",
            prefix.canonical()
        )),
        IdPrefixResolution::Ambiguous(ambiguity) => Err(anyhow!(ambiguity.message("event", &prefix))),
    }
}

fn parse_id_prefix(value: &str, kind: &str) -> Result<CtxIdPrefix> {
    CtxIdPrefix::parse(value).map_err(|err| anyhow!("{kind} {err}"))
}

fn push_event_text_block(out: &mut String, event: &Event) {
    let role = event.role.map(|role| role.as_str()).unwrap_or("-");
    out.push_str(&format!(
        "[{}] {} {} {}\n",
        event.occurred_at,
        role,
        event.event_type.as_str(),
        event.id
    ));
    out.push_str(&event_content(event));
    out.push_str("\n\n");
}

fn render_events_text(store: &Store, selected: &Event, events: &[Event]) -> String {
    let mut out = String::new();
    out.push_str(&format!("ctx_event_id: {}\n", selected.id));
    if let Some(session_id) = selected.session_id {
        out.push_str(&format!("ctx_session_id: {session_id}\n"));
        if let Ok(session) = store.get_session(session_id) {
            out.push_str(&format!("provider: {}\n", session.provider));
            if let Some(provider_session_id) = session.external_session_id {
                out.push_str(&format!("provider_session_id: {provider_session_id}\n"));
            }
        }
    }
    out.push('\n');
    for event in events {
        push_event_text_block(&mut out, event);
    }
    out
}

fn render_events_markdown(store: &Store, selected: &Event, events: &[Event]) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Event {}\n\n", selected.id));
    if let Some(session_id) = selected.session_id {
        out.push_str(&format!("- ctx_session_id: `{session_id}`\n"));
        if let Ok(session) = store.get_session(session_id) {
            out.push_str(&format!("- provider: `{}`\n", session.provider));
            if let Some(provider_session_id) = session.external_session_id {
                out.push_str(&format!("- provider_session_id: `{provider_session_id}`\n"));
            }
        }
    }
    for event in events {
        let role = event.role.map(|role| role.as_str()).unwrap_or("-");
        out.push_str(&format!(
            "\n## {} - {} - {}\n\n",
            role,
            event.event_type.as_str(),
            event.occurred_at
        ));
        out.push_str(&format!("ctx_event_id: `{}`\n\n", event.id));
        out.push_str(&event_content(event));
        out.push('\n');
    }
    out
}

fn event_window_json(
    store: &Store,
    selected: &Event,
    events: &[Event],
    format: OutputFormat,
) -> Value {
    compact_json(json!({
        "schema_version": 1,
        "target": "event",
        "item_type": "event_window",
        "ctx_event_id": selected.id,
        "ctx_session_id": selected.session_id,
        "format": format.as_str(),
        "event": transcript_event_json(store, selected),
        "events": events
            .iter()
            .map(|event| transcript_event_json(store, event))
            .collect::<Vec<_>>(),
    }))
}

fn transcript_event_json(store: &Store, event: &Event) -> Value {
    let session = event.session_id.and_then(|id| store.get_session(id).ok());
    compact_json(json!({
        "ctx_event_id": event.id,
        "item_id": event.id,
        "item_type": "event",
        "ctx_session_id": event.session_id,
        "provider": session.as_ref().map(|session| session.provider),
        "provider_session_id": session
            .as_ref()
            .and_then(|session| session.external_session_id.clone()),
        "sequence": event.seq,
        "event_type": event.event_type,
        "role": event.role,
        "occurred_at": event.occurred_at,
        "source_id": event.capture_source_id,
        "source_path": source_path_for(store, event.capture_source_id),
        "source_exists": source_path_exists(source_path_for(store, event.capture_source_id).as_deref()),
        "source": source_json_for(store, event.capture_source_id),
        "cursor": event_cursor(event),
        "preview": event_preview(event),
        "text": event_content(event),
        "redaction_state": event.redaction_state,
    }))
}

fn render_events_jsonl(store: &Store, events: &[Event]) -> Result<String> {
    let mut lines = Vec::new();
    for event in events {
        lines.push(serde_json::to_string(&transcript_event_json(store, event))?);
    }
    Ok(lines.join("\n") + "\n")
}

fn source_json_for(store: &Store, source_id: Option<Uuid>) -> Option<Value> {
    let source = source_id.and_then(|source_id| store.get_capture_source(source_id).ok())?;
    let path = source.descriptor.raw_source_path.clone();
    Some(compact_json(json!({
        "source_id": source.id,
        "provider": source.descriptor.provider,
        "provider_session_id": source.descriptor.external_session_id,
        "path": path,
        "exists": source_path_exists(path.as_deref()),
        "cwd": source.descriptor.cwd,
        "started_at": source.started_at,
        "ended_at": source.ended_at,
        "source_format": source_format(&source.sync.metadata),
        "cursor": source_cursor(&source.sync.metadata),
    })))
}

fn source_format(metadata: &Value) -> Option<String> {
    for pointer in [
        "/source_format",
        "/format",
        "/provider/source_format",
        "/source/source_format",
    ] {
        if let Some(value) = metadata.pointer(pointer).and_then(|value| value.as_str()) {
            return Some(value.to_owned());
        }
    }
    None
}

fn source_cursor(metadata: &Value) -> Option<String> {
    metadata
        .pointer("/cursor/after/cursor")
        .and_then(|value| value.as_str())
        .or_else(|| metadata.pointer("/cursor").and_then(|value| value.as_str()))
        .map(str::to_owned)
}

fn shell_quote_arg(value: &str) -> String {
    if !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '@'))
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn print_locate_session_text(value: &Value) -> Result<()> {
    println!(
        "ctx_session_id: {}",
        value["ctx_session_id"].as_str().unwrap_or("")
    );
    print_optional_json_str(value, "provider");
    print_optional_json_str(value, "provider_session_id");
    if let Some(source) = value.get("source") {
        print_optional_json_str(source, "path");
        print_optional_json_str(source, "source_format");
        if let Some(exists) = source.get("exists").and_then(|value| value.as_bool()) {
            println!("source_exists: {exists}");
        }
    }
    if let Some(command) = value
        .get("resume")
        .and_then(|resume| resume.get("command"))
        .and_then(|value| value.as_str())
    {
        println!("resume_command: {command}");
    }
    Ok(())
}

fn print_locate_event_text(value: &Value) -> Result<()> {
    println!(
        "ctx_event_id: {}",
        value["ctx_event_id"].as_str().unwrap_or("")
    );
    print_optional_json_str(value, "ctx_session_id");
    print_optional_json_str(value, "provider");
    print_optional_json_str(value, "provider_session_id");
    print_optional_json_str(value, "event_type");
    print_optional_json_str(value, "role");
    print_optional_json_str(value, "cursor");
    if let Some(source) = value.get("source") {
        print_optional_json_str(source, "path");
    }
    Ok(())
}

fn print_optional_json_str(value: &Value, key: &str) {
    if let Some(text) = value.get(key).and_then(|value| value.as_str()) {
        println!("{key}: {text}");
    }
}

fn write_json_record<W: Write + ?Sized>(writer: &mut W, value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_session_jsonl<W: Write + ?Sized>(
    writer: &mut W,
    page: &EventPageV1,
    next_command: Option<String>,
    next_argv: Option<Vec<String>>,
) -> Result<()> {
    for event in &page.events {
        let mut record = json!({
            "schema_version": page.schema_version,
            "record_type": "event",
            "item_type": "session_transcript_event",
            "mode": page.mode,
            "ctx_session_id": match &page.session {
                SessionProjectionV1::Full(session) => session.ctx_session_id,
                SessionProjectionV1::Compact(session) => session.ctx_session_id,
            },
            "provider": page.provider,
            "event": event,
        });
        if let (Some(object), Some(provider_session_id)) =
            (record.as_object_mut(), page.provider_session_id.as_ref())
        {
            object.insert("provider_session_id".to_owned(), json!(provider_session_id));
        }
        write_json_record(writer, &record)?;
    }
    write_json_record(
        writer,
        &json!({
            "schema_version": page.schema_version,
            "record_type": "completion",
            "returned": page.pagination.returned_items,
            "range": page_range_json(page.pagination.offset, page.pagination.returned_items),
            "omitted": page.omitted,
            "omitted_before": page.omitted.before,
            "omitted_after": page.omitted.after,
            "omitted_exact": page.omitted.exact,
            "has_more": page.pagination.has_more,
            "next": page.pagination.continuation,
            "next_command": next_command,
            "next_argv": next_argv,
            "bytes": page.bytes,
            "fields": page.fields,
            "selected_total": page.selected_total,
        }),
    )?;
    Ok(())
}

fn write_search_jsonl<W: Write + ?Sized>(
    writer: &mut W,
    page: &SearchPageV1,
    next_command: Option<String>,
    next_argv: Option<Vec<String>>,
) -> Result<()> {
    for result in &page.results {
        write_json_record(
            writer,
            &json!({
                "schema_version": page.schema_version,
                "record_type": "result",
                "result": result,
            }),
        )?;
    }
    write_json_record(
        writer,
        &json!({
            "schema_version": page.schema_version,
            "record_type": "completion",
            "returned": page.pagination.returned_items,
            "range": page_range_json(page.pagination.offset, page.pagination.returned_items),
            "omitted": page.omitted,
            "omitted_before": page.omitted.before,
            "omitted_after": page.omitted.after,
            "omitted_exact": page.omitted.exact,
            "has_more": page.pagination.has_more,
            "next": page.pagination.continuation,
            "next_command": next_command,
            "next_argv": next_argv,
            "bytes": page.bytes,
            "fields": page.fields,
            "pool_total": page.pool_total,
            "source_truncation": page.source_truncation,
        }),
    )?;
    Ok(())
}

fn page_range_json(offset: usize, returned: usize) -> Value {
    if returned == 0 {
        json!({"start": null, "end": null})
    } else {
        json!({"start": offset + 1, "end": offset + returned})
    }
}

struct PageSummary<'a> {
    noun: &'a str,
    offset: usize,
    returned: usize,
    omitted_before: usize,
    omitted_after: usize,
    exact: bool,
    next_command: Option<&'a str>,
}

fn render_page_summary(out: &mut String, summary: PageSummary<'_>, markdown: bool) {
    let PageSummary {
        noun,
        offset,
        returned,
        omitted_before,
        omitted_after,
        exact,
        next_command,
    } = summary;
    let prefix = if markdown {
        "## Page summary\n\n"
    } else {
        "page: "
    };
    out.push_str(prefix);
    if returned == 0 {
        out.push_str(&format!("returned 0 {noun}"));
    } else {
        out.push_str(&format!(
            "returned {} {noun} (range {}-{})",
            returned,
            offset + 1,
            offset + returned
        ));
    }
    out.push_str(&format!(
        "; omitted before {omitted_before}, after {omitted_after} ({}); {} total {}\n",
        if exact { "exact" } else { "lower-bound" },
        if noun == "events" { "selected" } else { "pool" },
        omitted_before
            .saturating_add(returned)
            .saturating_add(omitted_after),
    ));
    if let Some(command) = next_command {
        out.push_str(&format!("continuation: {command}\n"));
        if noun == "events" {
            out.push_str("events omitted; continue with --continue\n");
        }
    } else {
        out.push_str(&format!("no more {noun}\n"));
    }
}

pub(crate) fn command_from_argv(argv: &[String]) -> String {
    argv.iter()
        .map(|part| shell_quote_arg(part))
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn show_session_next_argv(
    args: &ShowSessionArgs,
    format: OutputFormat,
    continuation: Option<&str>,
) -> Option<Vec<String>> {
    let continuation = continuation?;
    let id = args.id.as_ref()?;
    Some(vec![
        "ctx".to_owned(),
        "show".to_owned(),
        "session".to_owned(),
        id.clone(),
        "--mode".to_owned(),
        args.mode.as_str().to_owned(),
        "--fields".to_owned(),
        args.fields.as_str().to_owned(),
        "--limit".to_owned(),
        args.limit.to_string(),
        "--max-event-bytes".to_owned(),
        args.max_event_bytes.to_string(),
        "--max-page-bytes".to_owned(),
        args.max_page_bytes.to_string(),
        "--format".to_owned(),
        format.as_str().to_owned(),
        "--continue".to_owned(),
        continuation.to_owned(),
    ])
}

impl FieldArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Compact => "compact",
        }
    }
}

fn query_error_kind(error: &QueryError) -> &'static str {
    match error {
        QueryError::InvalidBytePolicy { .. } => "invalid_byte_policy",
        QueryError::InvalidContinuation(_) => "invalid_continuation",
        QueryError::ContinuationRequestMismatch => "continuation_request_mismatch",
        QueryError::ContinuationKind { .. } => "continuation_kind_mismatch",
        QueryError::StaleContinuation => "stale_continuation",
        QueryError::SnapshotChanged => "snapshot_changed",
        QueryError::Store(_) => "store_error",
        QueryError::Search(_) => "search_error",
        QueryError::ArithmeticOverflow => "arithmetic_overflow",
        QueryError::InvalidPageSize => "invalid_page_size",
        QueryError::Serialization(_) => "serialization_error",
        QueryError::ItemExceedsPageBudget { .. } => "item_exceeds_page_budget",
    }
}

fn write_ndjson_error<W: Write + ?Sized>(writer: &mut W, kind: &str, message: &str) -> Result<()> {
    write_json_record(
        writer,
        &json!({
            "schema_version": 1,
            "record_type": "error",
            "kind": kind,
            "message": message,
        }),
    )?;
    writer.flush()?;
    Ok(())
}

fn is_broken_pipe(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::BrokenPipe)
    })
}

fn finish_paged_stdout(result: Result<()>) -> Result<()> {
    match result {
        Err(error) if is_broken_pipe(&error) => Ok(()),
        other => other,
    }
}

fn paged_query_error<T>(format: OutputFormat, error: QueryError) -> Result<T> {
    if format == OutputFormat::Jsonl {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        if let Err(write_error) =
            write_ndjson_error(&mut writer, query_error_kind(&error), &error.to_string())
        {
            if is_broken_pipe(&write_error) {
                return Err(SilentExit { code: 0 }.into());
            }
            return Err(write_error);
        }
    }
    Err(error.into())
}

fn paged_request_error<T>(format: OutputFormat, kind: &str, error: anyhow::Error) -> Result<T> {
    if format == OutputFormat::Jsonl {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        if let Err(write_error) = write_ndjson_error(&mut writer, kind, &error.to_string()) {
            if is_broken_pipe(&write_error) {
                return Err(SilentExit { code: 0 }.into());
            }
            return Err(write_error);
        }
    }
    Err(error)
}

fn source_path_for(store: &Store, source_id: Option<Uuid>) -> Option<String> {
    source_id
        .and_then(|source_id| store.get_capture_source(source_id).ok())
        .and_then(|source| source.descriptor.raw_source_path)
}

fn source_path_exists(source_path: Option<&str>) -> Option<bool> {
    source_path.map(|path| Path::new(path).exists())
}

fn event_cursor(event: &Event) -> Option<String> {
    if let Some(cursor) = event.payload.get("cursor").and_then(|value| value.as_str()) {
        return Some(cursor.to_owned());
    }
    event
        .payload
        .get("body")
        .and_then(|body| body.get("cursor"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

fn compact_json(mut value: Value) -> Value {
    prune_null_json(&mut value);
    value
}

fn parse_search_limit(value: &str) -> std::result::Result<usize, String> {
    let limit = value
        .parse::<usize>()
        .map_err(|err| format!("invalid search limit: {err}"))?;
    if !(1..=MAX_SEARCH_LIMIT).contains(&limit) {
        return Err(format!(
            "search limit must be between 1 and {MAX_SEARCH_LIMIT}"
        ));
    }
    Ok(limit)
}

fn parse_sql_timeout(value: &str) -> std::result::Result<StdDuration, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("timeout must not be empty".to_owned());
    }
    let (number, multiplier_ms) = if let Some(number) = trimmed.strip_suffix("ms") {
        (number, 1.0)
    } else if let Some(number) = trimmed.strip_suffix('s') {
        (number, 1_000.0)
    } else if let Some(number) = trimmed.strip_suffix('m') {
        (number, 60_000.0)
    } else {
        (trimmed, 1_000.0)
    };
    let amount = number
        .parse::<f64>()
        .map_err(|err| format!("invalid timeout: {err}"))?;
    if !amount.is_finite() || amount <= 0.0 {
        return Err("timeout must be greater than zero".to_owned());
    }
    let millis = (amount * multiplier_ms).round();
    let max_millis = RAW_SQL_MAX_TIMEOUT.as_millis() as f64;
    if millis < 1.0 || millis > max_millis {
        return Err(format!(
            "timeout must be between 1ms and {}ms",
            RAW_SQL_MAX_TIMEOUT.as_millis()
        ));
    }
    Ok(StdDuration::from_millis(millis as u64))
}

fn prune_null_json(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, nested| {
                prune_null_json(nested);
                !nested.is_null()
            });
        }
        Value::Array(items) => {
            for item in items {
                prune_null_json(item);
            }
        }
        _ => {}
    }
}

fn open_existing_store_read_only(db_path: &Path, command: &str) -> Result<Store> {
    if !db_path.exists() {
        return Err(anyhow!(
            "ctx store is not initialized at {}; run `ctx setup` or `ctx import` first",
            db_path.display()
        ));
    }
    match Store::open_read_only(db_path) {
        Ok(store) => Ok(store),
        Err(StoreError::UnsupportedSchemaVersion(version)) => {
            Err(unsupported_schema_version_error(version, command))
        }
        Err(err) => {
            Err(err).with_context(|| format!("open read-only ctx store {}", db_path.display()))
        }
    }
}

/// Version-aware guidance for [`StoreError::UnsupportedSchemaVersion`].
/// Only versions this binary's chain can migrate — the ported upstream
/// chain (≤ v15) and the reviewed fork version v1000 — get "run a writable
/// command" advice; everything else (the unreviewed 16–999 gap or versions
/// newer than this binary) needs a newer ctx or a matching database, and
/// telling the user to "migrate" would be advising the impossible.
pub(crate) fn unsupported_schema_version_error(version: i64, command: &str) -> anyhow::Error {
    if ctx_history_store::schema_version_is_migratable(version) {
        anyhow!(
            "ctx store schema version {version} is older than this ctx binary; run a writable command such as `ctx setup` or `ctx import` once to migrate before using `{command}`"
        )
    } else {
        anyhow!(
            "ctx store schema version {version} is newer than or incompatible with this ctx binary and cannot be migrated by it; upgrade ctx, or restore a database backup that matches this version, before using `{command}`"
        )
    }
}

fn run_sql(args: SqlArgs, data_root: PathBuf) -> Result<()> {
    let sql = read_sql_input(&args)?;
    let db_path = database_path(data_root);
    let store = open_existing_store_read_only(&db_path, "ctx sql")?;
    let result = QueryService::new(&store).raw_sql(
        &sql,
        RawSqlOptions {
            max_rows: args.max_rows,
            max_columns: args.max_columns,
            max_value_bytes: args.max_value_bytes,
            max_sql_bytes: args.max_sql_bytes,
            timeout: args.timeout,
        },
    )?;

    match args.output_format() {
        SqlFormat::Table => print_sql_table(&result),
        SqlFormat::Json => print_share_safe_value(raw_sql_result_json(&result)),
        SqlFormat::Csv => print_sql_csv(&result, args.no_header),
        SqlFormat::Raw => print_sql_raw(&result),
    }
}

fn read_sql_input(args: &SqlArgs) -> Result<String> {
    match (&args.sql, &args.file) {
        (Some(sql), None) if sql == "-" => {
            let mut input = String::new();
            std::io::stdin()
                .read_to_string(&mut input)
                .context("read SQL from stdin")?;
            Ok(input)
        }
        (Some(sql), None) => Ok(sql.clone()),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("read SQL from {}", path.display()))
        }
        (None, None) => Err(anyhow!(
            "SQL is required; pass a statement, --file <path>, or '-' for stdin"
        )),
        (Some(_), Some(_)) => unreachable!("clap rejects --file with inline SQL"),
    }
}

fn print_sql_table(result: &RawSqlResult) -> Result<()> {
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(sql_table_cell).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut widths = result
        .columns
        .iter()
        .map(|column| column.name.chars().count())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }

    let headers = result
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| pad_table_cell(&column.name, widths[index]))
        .collect::<Vec<_>>();
    println!("{}", headers.join(" | "));
    let separators = widths
        .iter()
        .map(|width| "-".repeat(*width))
        .collect::<Vec<_>>();
    println!("{}", separators.join(" | "));
    for row in &rows {
        let cells = row
            .iter()
            .enumerate()
            .map(|(index, cell)| pad_table_cell(cell, widths[index]))
            .collect::<Vec<_>>();
        println!("{}", cells.join(" | "));
    }
    if result.rows.is_empty() {
        println!("(0 rows)");
    }
    print_sql_truncation_notice(result);
    Ok(())
}

fn print_sql_csv(result: &RawSqlResult, no_header: bool) -> Result<()> {
    if !no_header {
        println!(
            "{}",
            result
                .columns
                .iter()
                .map(|column| csv_escape(&column.name))
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    for row in &result.rows {
        println!(
            "{}",
            row.iter()
                .map(sql_csv_cell)
                .map(|cell| csv_escape(&cell))
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    print_sql_truncation_notice(result);
    Ok(())
}

fn print_sql_raw(result: &RawSqlResult) -> Result<()> {
    if result.columns.len() != 1 {
        return Err(anyhow!(
            "--format raw requires exactly one selected column; got {}",
            result.columns.len()
        ));
    }
    for row in &result.rows {
        println!("{}", sql_raw_cell(&row[0]));
    }
    print_sql_truncation_notice(result);
    Ok(())
}

fn print_sql_truncation_notice(result: &RawSqlResult) {
    if result.truncated.rows {
        eprintln!(
            "warning: rows truncated at {}; rerun with --max-rows for more",
            result.limits.max_rows
        );
    }
    if result.truncated.values {
        eprintln!(
            "warning: values truncated at {} bytes; rerun with --max-value-bytes for more",
            result.limits.max_value_bytes
        );
    }
}

fn sql_table_cell(value: &RawSqlValue) -> String {
    truncate_table_cell(&sql_display_cell(value), 96)
}

fn sql_csv_cell(value: &RawSqlValue) -> String {
    sql_display_cell(value)
}

fn sql_raw_cell(value: &RawSqlValue) -> String {
    match value {
        RawSqlValue::Null => String::new(),
        RawSqlValue::Integer(value) => value.to_string(),
        RawSqlValue::Real(value) => value.to_string(),
        RawSqlValue::Text { value, .. } => value.clone(),
        RawSqlValue::Blob { preview_hex, .. } => preview_hex.clone(),
    }
}

fn sql_display_cell(value: &RawSqlValue) -> String {
    match value {
        RawSqlValue::Null => "NULL".to_owned(),
        RawSqlValue::Integer(value) => value.to_string(),
        RawSqlValue::Real(value) => value.to_string(),
        RawSqlValue::Text {
            value, truncated, ..
        } => {
            let mut value = value.replace('\n', "\\n").replace('\r', "\\r");
            if *truncated {
                value.push_str("...");
            }
            value
        }
        RawSqlValue::Blob {
            bytes,
            preview_hex,
            truncated,
        } => {
            if *truncated {
                format!("[blob {bytes} bytes {preview_hex}...]")
            } else {
                format!("[blob {bytes} bytes {preview_hex}]")
            }
        }
    }
}

fn truncate_table_cell(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let keep = max_chars.saturating_sub(3);
    let mut truncated = value.chars().take(keep).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn pad_table_cell(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        value.to_owned()
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

fn run_search(args: SearchArgs, data_root: PathBuf) -> Result<()> {
    let search_format = effective_format(args.format, args.json);
    if args.continuation.is_some() && args.refresh != RefreshArg::Off {
        return paged_request_error(
            search_format,
            "continuation_requires_refresh_off",
            anyhow!(
                "search continuations require --refresh off so the read-only snapshot is stable"
            ),
        );
    }
    let byte_policy = match (BytePolicy {
        per_item_bytes: args.max_snippet_bytes,
        page_bytes: args.max_page_bytes,
    })
    .validate()
    {
        Ok(policy) => policy,
        Err(err) => return paged_query_error(search_format, err),
    };
    if !search_has_intent(SearchIntentInput {
        query: args.query.as_deref(),
        terms: &args.term,
        file: args.file.as_deref(),
    }) {
        return paged_request_error(
            search_format,
            "missing_search_intent",
            missing_search_intent_error(),
        );
    }
    let query = args.query.clone().unwrap_or_default();
    if let Err(error) = ctx_history_search::validate_query_request(&query, &args.term) {
        return paged_request_error(search_format, "invalid_query", error.into());
    }

    let db_path = database_path(data_root.clone());
    let had_existing_store = db_path.exists();
    let refresh = match refresh_before_search(&args, &data_root) {
        Ok(refresh) => refresh,
        Err(error) => return paged_request_error(search_format, "refresh_error", error),
    };
    if refresh.status == "failed" && args.refresh == RefreshArg::Auto && !had_existing_store {
        return paged_request_error(
            search_format,
            "refresh_error",
            anyhow!(
            "search refresh failed and no existing ctx index is available; run `ctx import` first or retry with `--refresh strict`: {}",
            refresh.error.as_deref().unwrap_or("unknown refresh error")
            ),
        );
    }
    if !db_path.exists() && args.refresh == RefreshArg::Auto {
        // Preserve the existing empty-index first-run behavior, but confine
        // initialization to the refresh phase and reopen read-only to query.
        if let Err(error) = Store::open(&db_path) {
            return paged_request_error(search_format, "store_initialization_error", error.into());
        }
    }
    // Refresh owns every intentional write. The query phase always reopens
    // the resulting store read-only, including a store created by refresh.
    let store = match open_existing_store_read_only(&db_path, "ctx search") {
        Ok(store) => store,
        Err(error) => return paged_request_error(search_format, "store_error", error),
    };
    let refresh = refresh.with_index_age(Some(&store));
    let source_identity = SourceIdentityFilterArgs::from(&args);
    let event_results = args.events || args.session.is_some();
    let filters = match search_filters(
        SearchFilterInput {
            session: args.session.clone(),
            provider: args.provider,
            source_identity,
            workspace: args.workspace.clone(),
            since: args.since.clone(),
            primary_only: args.primary_only,
            include_subagents: args.include_subagents,
            event_type: args.event_type.clone(),
            role: args.role.clone(),
            exclude_role: args.exclude_role.clone(),
            exclude_tool_noise: args.exclude_tool_noise,
            exclude_tool_name: args.exclude_tool_name.clone(),
            file: args.file.clone(),
            include_current_session: args.include_current_session,
        },
        Some(&store),
    ) {
        Ok(filters) => filters,
        Err(error) => return paged_request_error(search_format, "invalid_filters", error),
    };
    let options = ctx_history_search::PacketOptions {
        limit: args.limit,
        filters,
        result_mode: if event_results {
            ctx_history_search::SearchResultMode::Events
        } else {
            ctx_history_search::SearchResultMode::Sessions
        },
        match_mode: args.r#match.into(),
        ..ctx_history_search::PacketOptions::default()
    };
    // Freeze relative windows (for example `30d`) only in replay argv.
    // Resolving them again in a later process would produce a different bound
    // request, while broader-search suggestions should preserve user spelling.
    let mut replay_args = args.clone();
    replay_args.since = options.filters.since.map(|value| value.to_rfc3339());
    let uses_composed_terms = args.term.iter().any(|term| !term.trim().is_empty());
    let page = QueryService::new(&store).search(
        &query,
        &args.term,
        options,
        args.continuation.as_deref(),
        args.fields.into(),
        byte_policy,
    );
    let page = match page {
        Ok(page) => page,
        Err(err) => return paged_query_error(search_format, err),
    };
    let next_argv = search_next_argv(
        &replay_args,
        search_format,
        page.pagination.continuation.as_deref(),
    );
    let next_command = next_argv.as_ref().map(|argv| command_from_argv(argv));
    if search_format == OutputFormat::Jsonl {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        finish_paged_stdout((|| -> Result<()> {
            write_search_jsonl(&mut writer, &page, next_command, next_argv)?;
            writer.flush()?;
            Ok(())
        })())?;
    } else if search_format == OutputFormat::Json {
        let broadened = if page.results.is_empty() {
            broadened_search_json(&args, &query)
        } else {
            Value::Null
        };
        let value = search_page_json(&page, &refresh, broadened, next_command, next_argv)?;
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        finish_paged_stdout((|| -> Result<()> {
            writer.write_all(&serde_json::to_vec_pretty(&value)?)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            Ok(())
        })())?;
    } else {
        if refresh.status == "failed" && args.refresh == RefreshArg::Auto {
            if let Some(error) = &refresh.error {
                eprintln!(
                    "warning: search refresh failed; serving existing index; use --refresh strict to fail instead: {error}"
                );
            }
        } else if refresh.status == "degraded_zero_yield" && args.refresh == RefreshArg::Auto {
            eprintln!(
                "warning: search refresh detected zero-yield import health anomalies; serving search results; run `ctx doctor` or `ctx import --strict`"
            );
        }
        let mut output = format!(
            "freshness: refresh {} ({}), duration {}ms, index age {}, imported sessions/events/edges {}/{}/{}, skipped {}, unchanged {}, failed {}\n",
            refresh.status,
            refresh.reason,
            refresh.duration_ms,
            refresh
                .index_age_seconds
                .map(|seconds| format!("{seconds}s"))
                .unwrap_or_else(|| "unknown".to_owned()),
            refresh.totals.imported_sessions,
            refresh.totals.imported_events,
            refresh.totals.imported_edges,
            refresh.totals.skipped,
            refresh.totals.unchanged_sources,
            refresh.totals.failed,
        );
        if page.results.is_empty() {
            if let Some(file) = args
                .file
                .as_deref()
                .filter(|_| query.trim().is_empty() && !uses_composed_terms)
            {
                output.push_str(&format!("no indexed events touched {}\n", file.display()));
                let indexed_items = indexed_history_item_count(&store)?;
                if indexed_items == 0 {
                    output.push_str("next: ctx import --all\n");
                } else {
                    output.push_str(&format!(
                        "next: ctx search {}\n",
                        shell_quote_arg(&file.display().to_string())
                    ));
                }
            } else {
                output.push_str(&format!(
                    "no results for {}\n",
                    search_no_results_target(&query, &args.term)
                ));
                let indexed_items = indexed_history_item_count(&store)?;
                if indexed_items == 0 {
                    output.push_str("next: ctx import --all\n");
                } else {
                    output.push_str("hint: default matching is --match all: all words in one query/--term must appear in one indexed section; repeated --term clauses are OR; filters are AND. Use --match phrase for adjacent ordered words or --match any to broaden.\n");
                    if let Some(command) = broader_search_command(&args, &query) {
                        output.push_str(&format!("suggestion (not run): {command}\n"));
                    } else {
                        output.push_str("next: try another query or remove filters\n");
                    }
                }
            }
        }
        let markdown = search_format == OutputFormat::Markdown;
        for (index, result) in page.results.iter().enumerate() {
            render_search_result(
                &mut output,
                page.pagination.offset + index + 1,
                result,
                args.verbose,
                markdown,
                args.term.is_empty().then_some(query.as_str()),
                args.r#match.into(),
            );
        }
        render_page_summary(
            &mut output,
            PageSummary {
                noun: "results",
                offset: page.pagination.offset,
                returned: page.pagination.returned_items,
                omitted_before: page.omitted.before,
                omitted_after: page.omitted.after,
                exact: page.omitted.exact,
                next_command: next_command.as_deref(),
            },
            markdown,
        );
        output.push_str(&format!(
            "search source truncation: omitted {} ({}){}\n",
            page.source_truncation.omitted_results,
            if page.source_truncation.omitted_results_exact {
                "exact"
            } else {
                "lower-bound"
            },
            page.source_truncation
                .reason
                .as_deref()
                .map(|reason| format!(", reason {reason}"))
                .unwrap_or_default()
        ));
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        finish_paged_stdout((|| -> Result<()> {
            writer.write_all(output.as_bytes())?;
            writer.flush()?;
            Ok(())
        })())?;
    }
    Ok(())
}

pub(crate) fn search_page_json(
    page: &SearchPageV1,
    refresh: &SearchRefreshReport,
    broadened_search: Value,
    next_command: Option<String>,
    next_argv: Option<Vec<String>>,
) -> Result<Value> {
    let mut value = serde_json::to_value(page)?;
    let object = value
        .as_object_mut()
        .expect("query search page serializes as an object");
    object.insert("freshness".into(), refresh.to_json());
    object.insert("broadened_search".into(), broadened_search);
    object.insert("next".into(), json!(page.pagination.continuation));
    object.insert("next_command".into(), json!(next_command));
    object.insert("next_argv".into(), json!(next_argv));
    object.insert("share_safe".into(), Value::Bool(false));
    if let Some(pagination) = object.get_mut("pagination").and_then(Value::as_object_mut) {
        pagination.insert("cursor".into(), json!(page.pagination.continuation));
    }
    object.insert(
        "truncation".into(),
        json!({
            "truncated": page.source_truncation.truncated,
            "omitted_results": page.source_truncation.omitted_results,
            "omitted_results_exact": page.source_truncation.omitted_results_exact,
            "reason": page.source_truncation.reason,
        }),
    );
    match &page.context {
        SearchContextProjectionV1::Full(context) => {
            object.insert("query".into(), json!(page.legacy_query));
            object.insert("terms".into(), json!(context.terms));
            object.insert("filters".into(), serde_json::to_value(&context.filters)?);
            add_full_search_page_compatibility(
                object,
                &context.query,
                context.match_mode,
                context.terms.is_empty(),
            );
        }
        SearchContextProjectionV1::Compact(_) => {}
    }
    Ok(value)
}

fn add_full_search_page_compatibility(
    object: &mut serde_json::Map<String, Value>,
    _query: &str,
    _match_mode: SearchMatchMode,
    _include_scoped_search: bool,
) {
    if let Some(filters) = object.get_mut("filters").and_then(Value::as_object_mut) {
        filters.insert("primary_only".into(), Value::Null);
        if let Some(excluded) = filters
            .get_mut("exclude_provider_session")
            .and_then(Value::as_object_mut)
        {
            if let Some(id) = excluded.get("ctx_session_id").cloned() {
                excluded.insert("session_id".into(), id);
            }
        }
    }
}

fn render_search_result(
    out: &mut String,
    index: usize,
    result: &SearchResultProjectionV1,
    verbose: bool,
    markdown: bool,
    query: Option<&str>,
    match_mode: SearchMatchMode,
) {
    let (title, snippet, rank, scope, importance, session_id, event_id, why, full) = match result {
        SearchResultProjectionV1::Full(result) => (
            result.title.as_str(),
            result.snippet.as_str(),
            result.rank,
            result.result_scope,
            result.session_importance,
            result.ctx_session_id,
            result.ctx_event_id,
            result.why_matched.as_slice(),
            Some(result.as_ref()),
        ),
        SearchResultProjectionV1::Compact(result) => (
            result.title.as_str(),
            result.snippet.as_str(),
            result.rank,
            result.result_scope,
            result.session_importance,
            result.ctx_session_id,
            result.ctx_event_id,
            result.why_matched.as_slice(),
            None,
        ),
    };
    if markdown {
        out.push_str(&format!("\n## {index}. {title}\n\n"));
    } else {
        out.push_str(&format!("{index}. {title}\n"));
    }
    if scope == ctx_history_search::SearchResultScope::Session {
        out.push_str(&format!("   importance {importance:.2}"));
    } else {
        out.push_str(&format!("   rank {rank:.2}"));
    }
    if let Some(id) = session_id {
        out.push_str(&format!(" | session {}", short_uuid(id)));
    }
    if let Some(id) = event_id {
        out.push_str(&format!(" | event {}", short_uuid(id)));
    }
    out.push('\n');
    if !snippet.trim().is_empty() {
        out.push_str(&format!("   {}\n", snippet.trim()));
    }
    if full.is_some() {
        if let Some(id) = event_id {
            out.push_str(&format!("   inspect: ctx show event {id} --window 10\n"));
        } else if let Some(id) = session_id {
            out.push_str(&format!("   inspect: ctx show session {id}\n"));
        }
    }
    if verbose {
        if let Some(id) = event_id {
            out.push_str(&format!("   ctx_event_id: {id}\n"));
        }
        if let Some(id) = session_id {
            out.push_str(&format!("   ctx_session_id: {id}\n"));
        }
        if !why.is_empty() {
            out.push_str(&format!("   why_matched: {}\n", why.join(", ")));
        }
        if scope == ctx_history_search::SearchResultScope::Session {
            out.push_str(&format!("   session_importance: {importance:.2}\n"));
        }
        if let Some(result) = full {
            if let Some(value) = &result.provider_session_id {
                out.push_str(&format!("   provider_session_id: {value}\n"));
            }
            if let Some(value) = &result.history_source {
                out.push_str(&format!("   history_source: {value}\n"));
            }
            if let Some(value) = &result.source_path {
                out.push_str(&format!("   source_path: {value}\n"));
            }
        }
        if full.is_some() {
            if let Some(id) = event_id {
                out.push_str(&format!("   next: ctx show event {id} --window 10\n"));
            }
            if let Some(id) = session_id {
                if let Some(query) = query.filter(|query| !query.trim().is_empty()) {
                    let mut scoped = vec!["ctx".to_owned(), "search".to_owned(), query.to_owned()];
                    if match_mode != SearchMatchMode::All {
                        scoped.extend(["--match".to_owned(), match_mode.as_str().to_owned()]);
                    }
                    scoped.extend(["--session".to_owned(), id.to_string()]);
                    out.push_str(&format!("   next: {}\n", command_from_argv(&scoped)));
                }
                out.push_str(&format!("   next: ctx show session {id}\n"));
            }
        }
    }
}

fn short_uuid(id: Uuid) -> String {
    id.to_string().chars().take(8).collect()
}

pub(crate) fn search_next_argv(
    args: &SearchArgs,
    format: OutputFormat,
    continuation: Option<&str>,
) -> Option<Vec<String>> {
    let continuation = continuation?;
    let mut argv = vec!["ctx".to_owned(), "search".to_owned()];
    for term in &args.term {
        argv.extend(["--term".to_owned(), term.clone()]);
    }
    argv.extend(["--match".to_owned(), args.r#match.as_str().to_owned()]);
    if let Some(provider) = args.provider {
        argv.extend(["--provider".to_owned(), provider.cli_name().to_owned()]);
    }
    for (flag, value) in [
        ("--history-source", args.history_source.as_deref()),
        ("--provider-key", args.provider_key.as_deref()),
        ("--source-id", args.source_id.as_deref()),
        ("--source-format", args.source_format.as_deref()),
        ("--workspace", args.workspace.as_deref()),
        ("--since", args.since.as_deref()),
        ("--event-type", args.event_type.as_deref()),
        ("--session", args.session.as_deref()),
    ] {
        if let Some(value) = value {
            argv.extend([flag.to_owned(), value.to_owned()]);
        }
    }
    for role in &args.role {
        argv.extend(["--role".to_owned(), role.clone()]);
    }
    for role in &args.exclude_role {
        argv.extend(["--exclude-role".to_owned(), role.clone()]);
    }
    for tool in &args.exclude_tool_name {
        argv.extend(["--exclude-tool".to_owned(), tool.clone()]);
    }
    if let Some(file) = &args.file {
        argv.extend(["--file".to_owned(), file.to_string_lossy().into_owned()]);
    }
    for (enabled, flag) in [
        (args.primary_only, "--primary-only"),
        (args.include_subagents, "--include-subagents"),
        (args.exclude_tool_noise, "--exclude-tool-noise"),
        (args.events, "--events"),
        (args.include_current_session, "--include-current-session"),
        (args.verbose, "--verbose"),
    ] {
        if enabled {
            argv.push(flag.to_owned());
        }
    }
    argv.extend([
        "--fields".to_owned(),
        args.fields.as_str().to_owned(),
        "--limit".to_owned(),
        args.limit.to_string(),
        "--max-snippet-bytes".to_owned(),
        args.max_snippet_bytes.to_string(),
        "--max-page-bytes".to_owned(),
        args.max_page_bytes.to_string(),
        "--format".to_owned(),
        format.as_str().to_owned(),
        "--refresh".to_owned(),
        "off".to_owned(),
        "--continue".to_owned(),
        continuation.to_owned(),
    ]);
    if let Some(query) = &args.query {
        argv.extend(["--".to_owned(), query.clone()]);
    }
    Some(argv)
}

fn refresh_before_search(args: &SearchArgs, data_root: &Path) -> Result<SearchRefreshReport> {
    let started = Instant::now();
    if args.refresh == RefreshArg::Off {
        let mut report = SearchRefreshReport::skipped(RefreshArg::Off, "skipped");
        report.reason = "refresh_off";
        return Ok(report);
    }
    let _delayed_progress = DelayedRefreshProgress::start(args.refresh);
    let source_identity = normalize_source_identity_filters(SourceIdentityFilterArgs::from(args))?;
    if !source_identity.is_empty()
        && args
            .provider
            .is_some_and(|provider| !matches!(provider, ProviderArg::Custom))
    {
        return Err(anyhow!(
            "custom history source filters can only be combined with --provider custom"
        ));
    }
    let sources = if source_identity.is_empty() {
        search_refresh_sources(args.provider)
    } else {
        Vec::new()
    };
    let plugin_sources =
        match search_refresh_plugin_sources(data_root, args.provider, &source_identity) {
            Ok(sources) => sources,
            Err(err) if args.refresh == RefreshArg::Auto => {
                return Ok(SearchRefreshReport::failed(
                    RefreshArg::Auto,
                    sources.len(),
                    error_summary(&err),
                    started.elapsed().as_millis(),
                ));
            }
            Err(err) => return Err(err.context("search refresh failed")),
        };
    if sources.is_empty() && plugin_sources.is_empty() {
        if args.refresh == RefreshArg::Strict {
            return Err(anyhow!(
                "strict search refresh found no supported discovered native provider or enabled auto history-source plugin sources; rerun the search with --refresh off to use the existing index"
            ));
        }
        let mut report = SearchRefreshReport::skipped(args.refresh, "no_sources");
        report.reason = "no_sources";
        report.duration_ms = started.elapsed().as_millis();
        return Ok(report);
    }
    let source_count = sources.len().saturating_add(plugin_sources.len());
    match refresh_sources_for_search(data_root, sources, plugin_sources, args.refresh, args.json) {
        Ok(totals) => {
            if args.refresh == RefreshArg::Strict && totals.zero_yield_anomaly_sources > 0 {
                return Err(anyhow!("strict search refresh detected zero-yield import anomaly; run `ctx import --strict` or `ctx doctor`"));
            }
            Ok(SearchRefreshReport::completed(
                args.refresh,
                source_count,
                totals,
                started.elapsed().as_millis(),
            ))
        }
        Err(err) if args.refresh == RefreshArg::Auto => Ok(SearchRefreshReport::failed(
            RefreshArg::Auto,
            source_count,
            error_summary(&err),
            started.elapsed().as_millis(),
        )),
        Err(err) => Err(err.context("search refresh failed")),
    }
}

fn search_refresh_sources(provider: Option<ProviderArg>) -> Vec<SourceInfo> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    let mut sources = if let Some(provider) = provider {
        discover_provider_sources_for_provider(&home, provider.capture_provider())
    } else {
        discovered_sources()
    };
    sources
        .drain(..)
        .filter(|source| {
            source.exists
                && source.import_support.is_auto_importable()
                && source.status == ProviderSourceStatus::Available
                && source.source_format != "codex_history_jsonl"
        })
        .collect()
}

fn search_refresh_plugin_sources(
    data_root: &Path,
    provider: Option<ProviderArg>,
    source_identity: &SourceIdentityFilters,
) -> Result<Vec<HistorySourcePluginSource>> {
    if !matches!(provider, None | Some(ProviderArg::Custom)) {
        return Ok(Vec::new());
    }
    Ok(discover_history_source_plugins(data_root, &[])?
        .into_iter()
        .filter(|source| {
            source.enabled
                && source.refresh == HistorySourcePluginRefresh::Auto
                && source_identity.matches_plugin_source(source)
        })
        .collect())
}

fn refresh_sources_for_search(
    data_root: &Path,
    sources: Vec<SourceInfo>,
    plugin_sources: Vec<HistorySourcePluginSource>,
    refresh: RefreshArg,
    json_output: bool,
) -> Result<ImportTotals> {
    fs::create_dir_all(data_root)?;
    config::write_default_config(data_root)?;
    let db_path = database_path(data_root.to_path_buf());
    let planned_sources = sources
        .into_iter()
        .map(|source| (source, SourceStats::default()))
        .collect::<Vec<_>>();
    if planned_sources.is_empty() && plugin_sources.is_empty() {
        return Ok(ImportTotals::default());
    }

    let progress_arg = match refresh {
        RefreshArg::Strict if json_output => ProgressArg::Json,
        RefreshArg::Strict => ProgressArg::Auto,
        RefreshArg::Auto | RefreshArg::Off => ProgressArg::None,
    };
    let progress = ProgressReporter::new(progress_arg, json_output, "search-refresh", 0);
    let mut totals = ImportTotals::default();
    if should_parallelize_import(&planned_sources) {
        let source_states = Arc::new(Mutex::new(
            planned_sources
                .iter()
                .map(|(_, stats)| SourceProgressSnapshot {
                    completed_bytes: 0,
                    total_bytes: stats.bytes,
                })
                .collect::<Vec<_>>(),
        ));
        let handles = planned_sources
            .into_iter()
            .enumerate()
            .map(|(index, (source, stats))| {
                let db_path = db_path.clone();
                let progress_callback = progress.parallel_codex_import_callback(
                    &source,
                    index,
                    Arc::clone(&source_states),
                );
                thread::spawn(move || -> Result<ImportSourceOutcome> {
                    let mut store = Store::open(&db_path)?;
                    let summary = import_one_source_without_search_refresh(
                        &mut store,
                        &source,
                        progress_callback,
                        false,
                    )?;
                    Ok(ImportSourceOutcome {
                        index,
                        source,
                        stats,
                        summary,
                    })
                })
            })
            .collect::<Vec<_>>();

        let mut outcomes = Vec::with_capacity(handles.len());
        for handle in handles {
            let outcome = handle
                .join()
                .map_err(|_| anyhow!("provider import worker panicked"))??;
            outcomes.push(outcome);
        }
        outcomes.sort_by_key(|outcome| outcome.index);
        for outcome in outcomes {
            let health = classify_import_health(&outcome.stats, &outcome.summary);
            totals.add_with_health(&outcome.summary, &outcome.stats, &health);
            let health_store = Store::open(&db_path)?;
            if let Err(err) = persist_source_health(
                &health_store,
                outcome.source.provider.as_str(),
                outcome.source.source_format,
                &outcome.source.path,
                "",
                &health,
                !source_uses_import_file_manifest(&outcome.source),
            ) {
                totals.health_persistence_failures += 1;
                emit_health_persistence_warning(progress_arg, &err);
            }
        }
    } else {
        let mut store = Store::open(&db_path)?;
        let mut completed_source_bytes = 0u64;
        for (source, stats) in planned_sources {
            progress.message(
                "refreshing",
                format!("importing {}", source.provider.as_str()),
            );
            let source_progress = progress.codex_import_callback(&source, completed_source_bytes);
            completed_source_bytes = completed_source_bytes.saturating_add(stats.bytes);
            let summary = import_one_source_without_search_refresh(
                &mut store,
                &source,
                source_progress,
                false,
            )?;
            let health = classify_import_health(&stats, &summary);
            totals.add_with_health(&summary, &stats, &health);
            if let Err(err) = persist_source_health(
                &store,
                source.provider.as_str(),
                source.source_format,
                &source.path,
                "",
                &health,
                !source_uses_import_file_manifest(&source),
            ) {
                totals.health_persistence_failures += 1;
                emit_health_persistence_warning(progress_arg, &err);
            }
            progress.done(
                "refreshing",
                format!("refreshed {}", source.provider.as_str()),
                completed_source_bytes,
            );
        }
    }

    if !plugin_sources.is_empty() {
        let mut store = Store::open(&db_path)?;
        for plugin_source in plugin_sources {
            progress.message(
                "refreshing",
                format!("running history source plugin {}", plugin_source.label()),
            );
            let outcome =
                import_history_source_plugin(&mut store, &plugin_source, data_root, false)
                    .with_context(|| {
                        format!("refresh history source plugin {}", plugin_source.label())
                    })?;
            let health =
                history_source_plugin_health(&outcome.stats, &outcome.summary, outcome.source_only);
            totals.add_with_health(&outcome.summary, &outcome.stats, &health);
            if let Err(err) = persist_source_health(
                &store,
                &plugin_source.provider_key,
                &plugin_source.source_format,
                &plugin_source.manifest_dir,
                &plugin_source.source_id,
                &health,
                true,
            ) {
                totals.health_persistence_failures += 1;
                emit_health_persistence_warning(progress_arg, &err);
            }
            progress.done(
                "refreshing",
                format!("refreshed history source plugin {}", plugin_source.label()),
                0,
            );
        }
    }

    Store::open(&db_path)?.checkpoint_wal_truncate_if_larger_than(WAL_TRUNCATE_MIN_BYTES)?;
    Ok(totals)
}

fn run_doctor(args: DoctorArgs, data_root: PathBuf) -> Result<()> {
    let progress = ProgressReporter::new(args.progress, args.json, "doctor", 0);
    progress.message("opening", "opening ctx store");
    let db_path = database_path(data_root.clone());
    let mut findings = Vec::new();
    let mut import_health_zero_yield_anomalies = 0usize;
    let mut source_health_zero_yield_anomalies = 0usize;
    let mut source_health_acknowledged = 0usize;
    if !data_root.exists() {
        findings.push(format!("data root does not exist: {}", data_root.display()));
    }
    if !db_path.exists() {
        findings.push(format!(
            "ctx store is not initialized at {}; run `ctx setup` or `ctx import` first",
            db_path.display()
        ));
    } else {
        if args.acknowledge_source_health {
            source_health_acknowledged = Store::open(&db_path)?.acknowledge_source_health()?;
        }
        let store = open_existing_store_read_only(&db_path, "ctx doctor")?;
        progress.message(
            "checking",
            "running sqlite integrity and foreign key checks",
        );
        findings.extend(store.validate()?);
        import_health_zero_yield_anomalies = store.count_source_import_zero_yield_anomalies()?;
        source_health_zero_yield_anomalies = store.source_health_counts()?.zero_yield_anomaly;
        if import_health_zero_yield_anomalies > 0 {
            findings.push(format!(
                "import health: {import_health_zero_yield_anomalies} ledger-backed source file(s) previously produced zero imported entities without a safe skip/empty reason; rerun `ctx import` after the source changes or retry with an explicit provider"
            ));
        }
        if source_health_zero_yield_anomalies > 0 {
            findings.push(format!("import health: {source_health_zero_yield_anomalies} logical source(s) currently have a zero-yield anomaly; rerun `ctx import` after the source changes"));
        }
    }
    let storage = if args.storage {
        Some(storage_status::snapshot_deep(&data_root, CONFIG_FILE)?)
    } else {
        None
    };
    let storage_findings = storage
        .as_ref()
        .map(storage_status::findings)
        .unwrap_or_default();
    let ok = findings.is_empty() && storage_findings.is_empty();
    progress.done(
        "done",
        if ok {
            "ctx doctor passed"
        } else {
            "ctx doctor found issues"
        },
        0,
    );
    if args.json {
        let mut all_findings = findings.clone();
        all_findings.extend(storage_findings);
        print_json(json!({
            "schema_version": 1,
            "ok": ok,
            "progress": progress_mode_name(args.progress),
            "private": true,
            "share_safe": false,
            "storage": storage.as_ref().map(storage_status::storage_json),
            "storage_optional_diagnostics": storage.as_ref().map(storage_status::optional_diagnostic_messages).unwrap_or_default(),
            "import_health": {
                "ledger_backed_zero_yield_anomalies": import_health_zero_yield_anomalies,
                "source_level_anomalies": source_health_zero_yield_anomalies,
                "source_level_class_breakdown": { "zero_yield_anomaly": source_health_zero_yield_anomalies },
                "coverage": "all_import_paths_since_v1002",
                "not_persisted_for": [],
                "acknowledged": source_health_acknowledged,
            },
            "findings": all_findings,
        }))?;
    } else if ok {
        println!("ok");
        if args.acknowledge_source_health {
            println!("source_health_acknowledged: {source_health_acknowledged}");
        }
        if let Some(snapshot) = storage {
            for line in storage_status::human_storage_lines(&snapshot) {
                println!("{line}");
            }
        }
    } else {
        if args.acknowledge_source_health {
            println!("source_health_acknowledged: {source_health_acknowledged}");
        }
        for finding in findings {
            println!("{finding}");
        }
        for finding in storage_findings {
            println!("{finding}");
        }
        if let Some(snapshot) = storage {
            for line in storage_status::human_storage_lines(&snapshot) {
                println!("{line}");
            }
        }
    }
    Ok(())
}

fn validate_import_args(args: &ImportArgs) -> Result<()> {
    if args.path.is_some() && args.format.is_none() && args.provider.is_none() {
        return Err(anyhow!(
            "ctx import --path requires --provider for native provider history; use `ctx import --provider codex --path <path>` or `ctx import --format ctx-history-jsonl-v1 --path <file>`"
        ));
    }
    Ok(())
}

fn import_requests(args: &ImportArgs) -> Result<Vec<SourceInfo>> {
    if args.history_source.is_some() || !args.history_source_manifest.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(path) = &args.path {
        let provider = args
            .provider
            .context("ctx import --path requires --provider for native provider history")?
            .capture_provider();
        let source = explicit_path_source(provider, path.clone());
        if !source
            .path
            .try_exists()
            .with_context(|| format!("check import path {}", source.path.display()))?
        {
            return Err(anyhow!(
                "import path does not exist: {}",
                source.path.display()
            ));
        }
        validate_source_import_supported(&source)?;
        return Ok(vec![source]);
    }
    if args.all || args.provider.is_none() {
        return Ok(discovered_sources()
            .into_iter()
            .filter(|source| {
                source.exists
                    && source.import_support.is_auto_importable()
                    && source.status == ProviderSourceStatus::Available
            })
            .collect());
    }
    let provider = args.provider.expect("checked provider").capture_provider();
    let discovered = discovered_sources_for_provider(provider);
    let sources = discovered
        .iter()
        .filter(|source| {
            source.provider == provider
                && source.exists
                && source.import_support.is_importable()
                && source.status == ProviderSourceStatus::Available
        })
        .cloned()
        .collect::<Vec<_>>();
    if sources.is_empty() {
        let spec = provider_source_spec(provider);
        if spec
            .is_some_and(|spec| matches!(spec.import_support, ProviderImportSupport::Unsupported))
        {
            let reason = spec
                .and_then(|spec| spec.unsupported_reason)
                .unwrap_or("no native local-history parser is implemented");
            return Err(anyhow!(
                "{} native import is unsupported: {reason}",
                provider.as_str()
            ));
        }
        return Err(no_importable_provider_sources_error(provider, &discovered));
    }
    for source in &sources {
        validate_source_import_supported(source)?;
    }
    Ok(sources)
}

fn no_importable_provider_sources_error(
    provider: CaptureProvider,
    sources: &[SourceInfo],
) -> anyhow::Error {
    let mut message = format!("no importable {} history found", provider.as_str());
    if sources.is_empty() {
        message.push_str("; no default paths are registered for this provider");
    } else {
        message.push_str("\nchecked paths:");
        for source in sources {
            message.push_str(&format!(
                "\n  {} ({})",
                source.path.display(),
                source.status.as_str()
            ));
            if let Some(reason) = source.unsupported_reason {
                message.push_str(&format!(" - {reason}"));
            }
        }
    }
    message.push_str("\nuse `ctx sources` to inspect discovery, or pass --path");
    anyhow!(message)
}

fn history_source_plugin_import_requests(
    args: &ImportArgs,
    data_root: &Path,
    include_plugins: bool,
) -> Result<Vec<HistorySourcePluginSource>> {
    if !include_plugins {
        return Ok(Vec::new());
    }
    if !args.all && args.history_source.is_none() && args.history_source_manifest.is_empty() {
        return Ok(Vec::new());
    }
    let sources = discover_history_source_plugins(data_root, &args.history_source_manifest)?;
    if let Some(selector) = &args.history_source {
        let matches = sources
            .into_iter()
            .filter(|source| source.matches_selector(selector))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err(anyhow!(
                "no history source plugin matched `{selector}`; use `ctx sources` to inspect configured plugins"
            ));
        }
        if matches.len() > 1 {
            let labels = matches
                .iter()
                .map(HistorySourcePluginSource::label)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!(
                "history source plugin selector `{selector}` matched multiple sources ({labels}); use plugin/source or provider_key/source_id"
            ));
        }
        return Ok(matches);
    }
    if args.all {
        return Ok(sources
            .into_iter()
            .filter(|source| source.enabled)
            .collect());
    }
    Ok(sources
        .into_iter()
        .filter(|source| {
            args.history_source_manifest
                .iter()
                .any(|path| manifest_arg_matches_source(path, &source.manifest_path))
        })
        .collect())
}

fn manifest_arg_matches_source(arg: &Path, manifest_path: &Path) -> bool {
    if arg.is_file() {
        return same_pathish(arg, manifest_path);
    }
    if arg.is_dir() {
        return manifest_path.starts_with(arg);
    }
    same_pathish(arg, manifest_path)
}

fn same_pathish(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    let left = fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    left == right
}

fn import_history_source_plugin(
    store: &mut Store,
    source: &HistorySourcePluginSource,
    data_root: &Path,
    full_rescan: bool,
) -> Result<HistorySourcePluginImportOutcome> {
    let record = import_record_for_history_source_plugin(source);
    let record_id = record.id;
    let options = CustomHistoryJsonlV1ImportOptions::default();
    let machine_id = options.machine_id.clone();
    let cursor_stream = source.cursor_stream();
    let previous_cursor = if full_rescan {
        None
    } else {
        store
            .get_sync_cursor(None, &machine_id, &cursor_stream)?
            .map(|cursor| cursor.cursor)
    };
    let run = run_history_source_plugin(
        source,
        HistorySourcePluginRunOptions {
            data_root,
            machine_id: &machine_id,
            cursor: previous_cursor.as_deref(),
            cursor_stream: &cursor_stream,
            full_rescan,
        },
    )?;
    let _plugin_stderr = &run.stderr;
    validate_history_source_plugin_output(source, &run.stdout, &machine_id, full_rescan)?;
    let stdout = annotate_history_source_plugin_output(source, &run.stdout)?;
    let source_only = history_source_plugin_output_is_source_only(&stdout)?;
    let validation = validate_custom_history_jsonl_v1_reader(Cursor::new(stdout.as_slice()))
        .map_err(anyhow::Error::from)?;
    if validation.failed > 0 {
        return Err(history_source_plugin_import_failure(source, &validation));
    }
    let stats = SourceStats {
        files: 1,
        bytes: stdout.len() as u64,
    };
    store.upsert_record(&record)?;
    let summary = import_custom_history_jsonl_v1_reader(
        Cursor::new(stdout),
        store,
        CustomHistoryJsonlV1ImportOptions {
            machine_id,
            source_path: Some(source.manifest_path.clone()),
            history_record_id: Some(record_id),
            allow_partial_failures: false,
            ..options
        },
    )
    .map_err(anyhow::Error::from)?;
    if summary.failed > 0 {
        return Err(history_source_plugin_import_failure(source, &summary));
    }
    Ok(HistorySourcePluginImportOutcome {
        summary,
        stats,
        source_only,
    })
}

fn history_source_plugin_output_is_source_only(stdout: &[u8]) -> Result<bool> {
    let text = std::str::from_utf8(stdout).context("history source plugin output is not UTF-8")?;
    let mut saw_source_with_after_cursor = false;
    let mut saw_history_entity = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        match serde_json::from_str::<CtxHistoryJsonlRecord>(line)? {
            CtxHistoryJsonlRecord::Source(source) => {
                saw_source_with_after_cursor = source
                    .cursor
                    .as_ref()
                    .and_then(|cursor| cursor.after.as_ref())
                    .is_some();
            }
            CtxHistoryJsonlRecord::Session(_)
            | CtxHistoryJsonlRecord::Event(_)
            | CtxHistoryJsonlRecord::FileTouch(_)
            | CtxHistoryJsonlRecord::Edge(_) => saw_history_entity = true,
            CtxHistoryJsonlRecord::Manifest(_) => {}
        }
    }
    Ok(saw_source_with_after_cursor && !saw_history_entity)
}

fn annotate_history_source_plugin_output(
    source: &HistorySourcePluginSource,
    stdout: &[u8],
) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(stdout).with_context(|| {
        format!(
            "history source plugin {} emitted non-UTF-8 ctx-history-jsonl-v1 output",
            source.label()
        )
    })?;
    let mut out = Vec::with_capacity(stdout.len());
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let mut record: CtxHistoryJsonlRecord = serde_json::from_str(line).with_context(|| {
            format!(
                "history source plugin {} emitted invalid ctx-history-jsonl-v1 at line {line_number}",
                source.label()
            )
        })?;
        if let CtxHistoryJsonlRecord::Source(source_record) = &mut record {
            let mut metadata = match std::mem::take(&mut source_record.metadata) {
                Value::Object(map) => map,
                Value::Null => serde_json::Map::new(),
                other => {
                    let mut map = serde_json::Map::new();
                    map.insert("metadata".to_owned(), other);
                    map
                }
            };
            metadata.insert(
                "ctx_history_plugin".to_owned(),
                json!({
                    "plugin_name": source.plugin_name,
                    "plugin_source_id": source.id,
                    "history_source": source.label(),
                    "plugin_display_name": source.plugin_display_name,
                    "plugin_version": source.plugin_version,
                    "manifest_path": source.manifest_path,
                    "provider_key": source.provider_key,
                    "source_id": source.source_id,
                    "source_format": source.source_format,
                }),
            );
            source_record.metadata = Value::Object(metadata);
        }
        serde_json::to_writer(&mut out, &record).with_context(|| {
            format!(
                "serialize annotated history source plugin {} record at line {line_number}",
                source.label()
            )
        })?;
        out.push(b'\n');
    }
    Ok(out)
}

fn validate_history_source_plugin_output(
    source: &HistorySourcePluginSource,
    stdout: &[u8],
    machine_id: &str,
    require_after_cursor: bool,
) -> Result<()> {
    let text = std::str::from_utf8(stdout).with_context(|| {
        format!(
            "history source plugin {} emitted non-UTF-8 ctx-history-jsonl-v1 output",
            source.label()
        )
    })?;
    let mut saw_source = false;
    let mut saw_after_cursor = false;
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let record: CtxHistoryJsonlRecord = serde_json::from_str(line).with_context(|| {
            format!(
                "history source plugin {} emitted invalid ctx-history-jsonl-v1 at line {line_number}",
                source.label()
            )
        })?;
        let CtxHistoryJsonlRecord::Source(source_record) = record else {
            continue;
        };
        saw_source = true;
        if source_record
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.after.as_ref())
            .is_some()
        {
            saw_after_cursor = true;
        }
        if source_record.provider_key != source.provider_key
            || source_record.source_id != source.source_id
            || source_record.source_format != source.source_format
        {
            return Err(anyhow!(
                "history source plugin {} emitted source identity {}/{}/{} but manifest declares {}/{}/{}",
                source.label(),
                source_record.provider_key,
                source_record.source_id,
                source_record.source_format,
                source.provider_key,
                source.source_id,
                source.source_format
            ));
        }
        if let Some(source_machine_id) = source_record.machine_id {
            if source_machine_id != machine_id {
                return Err(anyhow!(
                    "history source plugin {} emitted machine_id `{source_machine_id}` but ctx is importing as `{machine_id}`; omit machine_id or set it to CTX_HISTORY_MACHINE_ID",
                    source.label()
                ));
            }
        }
    }
    if !saw_source {
        return Err(anyhow!(
            "history source plugin {} emitted no source record",
            source.label()
        ));
    }
    if require_after_cursor && !saw_after_cursor {
        return Err(anyhow!(
            "history source plugin {} was reset but emitted no source.cursor.after checkpoint; emit a fresh cursor after a full rescan",
            source.label()
        ));
    }
    Ok(())
}

fn history_source_plugin_import_failure(
    source: &HistorySourcePluginSource,
    summary: &ProviderImportSummary,
) -> anyhow::Error {
    let detail = summary
        .failures
        .first()
        .map(|failure| format!("line {}: {}", failure.line, failure.error))
        .unwrap_or_else(|| "unknown validation failure".to_owned());
    anyhow!(
        "history source plugin {} import failed with {} failure(s); first failure: {detail}",
        source.label(),
        summary.failed
    )
}

fn validate_source_import_supported(source: &SourceInfo) -> Result<()> {
    match source.import_support {
        ProviderImportSupport::Native => Ok(()),
        ProviderImportSupport::Preview => Ok(()),
        ProviderImportSupport::Unsupported => {
            let reason = source
                .unsupported_reason
                .unwrap_or("no native local-history parser is implemented");
            Err(anyhow!(
                "{} native import is unsupported: {reason}",
                source.provider.as_str()
            ))
        }
    }
}

fn import_one_source(
    store: &mut Store,
    source: &SourceInfo,
    progress: Option<CodexSessionImportProgressCallback>,
    full_rescan: bool,
) -> Result<ProviderImportSummary> {
    let event_search_needs_backfill = store.event_search_projection_needs_backfill()?;
    let refresh_search_after_import =
        event_search_needs_backfill || !source_uses_incremental_event_search(source);
    import_one_source_inner(
        store,
        source,
        progress,
        refresh_search_after_import,
        full_rescan,
    )
}

fn import_one_source_without_search_refresh(
    store: &mut Store,
    source: &SourceInfo,
    progress: Option<CodexSessionImportProgressCallback>,
    full_rescan: bool,
) -> Result<ProviderImportSummary> {
    import_one_source_inner(store, source, progress, false, full_rescan)
}

fn import_one_source_inner(
    store: &mut Store,
    source: &SourceInfo,
    progress: Option<CodexSessionImportProgressCallback>,
    refresh_search_after_import: bool,
    full_rescan: bool,
) -> Result<ProviderImportSummary> {
    let record = import_record_for_source(source);
    let record_id = record.id;
    store.upsert_record(&record)?;
    let tool_output_mode = codex_tool_output_mode()?;
    let event_mode = codex_event_import_mode()?;
    let include_notices = codex_include_notices();
    let full_rescan = full_rescan || source_cursor_requires_rescan(store, source)?;
    if !full_rescan && source_uses_import_file_manifest(source) {
        return import_manifested_source(
            store,
            source,
            record_id,
            tool_output_mode,
            event_mode,
            include_notices,
            progress,
        );
    }
    let summary = match source.provider {
        CaptureProvider::Codex => {
            if source.path.is_dir() {
                if full_rescan {
                    import_codex_session_tree(
                        &source.path,
                        store,
                        CodexSessionImportOptions {
                            source_path: Some(source.path.clone()),
                            history_record_id: Some(record_id),
                            allow_partial_failures: true,
                            tool_output_mode,
                            event_mode,
                            include_notices,
                            progress: progress.clone(),
                            ..CodexSessionImportOptions::default()
                        },
                    )
                    .map_err(anyhow::Error::from)
                } else {
                    import_incremental_codex_session_tree(
                        store,
                        source,
                        record_id,
                        tool_output_mode,
                        event_mode,
                        include_notices,
                        progress.clone(),
                    )
                }
            } else if source
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == "history.jsonl")
            {
                import_codex_history_jsonl(
                    &source.path,
                    store,
                    CodexHistoryImportOptions {
                        source_path: Some(source.path.clone()),
                        history_record_id: Some(record_id),
                        allow_partial_failures: true,
                        ..CodexHistoryImportOptions::default()
                    },
                )
                .map_err(anyhow::Error::from)
            } else {
                import_codex_session_jsonl(
                    &source.path,
                    store,
                    CodexSessionImportOptions {
                        source_path: Some(source.path.clone()),
                        history_record_id: Some(record_id),
                        allow_partial_failures: true,
                        tool_output_mode,
                        event_mode,
                        include_notices,
                        progress,
                        ..CodexSessionImportOptions::default()
                    },
                )
                .map_err(anyhow::Error::from)
            }
        }
        CaptureProvider::Pi => import_pi_session_jsonl(
            &source.path,
            store,
            PiSessionImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..PiSessionImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::Claude => import_claude_projects_jsonl_tree(
            &source.path,
            store,
            ClaudeProjectsImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..ClaudeProjectsImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::OpenCode => import_opencode_sqlite(
            &source.path,
            store,
            OpenCodeSqliteImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..OpenCodeSqliteImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::OpenClaw => import_openclaw_history(
            &source.path,
            store,
            OpenClawImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..OpenClawImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::Hermes => import_hermes_sqlite(
            &source.path,
            store,
            HermesSqliteImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..HermesSqliteImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::NanoClaw => import_nanoclaw_project(
            &source.path,
            store,
            NanoClawImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..NanoClawImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::AstrBot => import_astrbot_sqlite(
            &source.path,
            store,
            AstrBotSqliteImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..AstrBotSqliteImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::Gemini => import_gemini_cli_history(
            &source.path,
            store,
            GeminiCliImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..GeminiCliImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::Cursor => import_cursor_native_history(
            &source.path,
            store,
            CursorNativeImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..CursorNativeImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::CopilotCli => import_copilot_cli_session_events(
            &source.path,
            store,
            CopilotCliImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..CopilotCliImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::FactoryAiDroid => import_factory_ai_droid_sessions(
            &source.path,
            store,
            FactoryAiDroidImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..FactoryAiDroidImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        CaptureProvider::Antigravity => import_antigravity_cli_history(
            &source.path,
            store,
            AntigravityCliImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                ..AntigravityCliImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from),
        other => Err(anyhow!(
            "{} is not registered for provider history import",
            other.as_str()
        )),
    }?;
    if refresh_search_after_import {
        store.refresh_search_index()?;
    }
    Ok(summary)
}

fn import_manifested_source(
    store: &mut Store,
    source: &SourceInfo,
    record_id: Uuid,
    tool_output_mode: CodexToolOutputMode,
    event_mode: CodexEventImportMode,
    include_notices: bool,
    progress: Option<CodexSessionImportProgressCallback>,
) -> Result<ProviderImportSummary> {
    let source_root = source.path.display().to_string();
    let files = collect_source_import_files(source)
        .with_context(|| format!("catalog import files from {}", source.path.display()))?;
    if files.is_empty() {
        return Err(anyhow!(
            "no importable {} history files found under {}",
            source.provider.as_str(),
            source.path.display()
        ));
    }
    let current_paths = files
        .iter()
        .map(|file| file.source_path.clone())
        .collect::<Vec<_>>();
    let observed_at_ms = utc_now().timestamp_millis();
    store.begin_immediate_batch()?;
    let persist = (|| -> Result<()> {
        store.upsert_source_import_files(&files)?;
        store.mark_source_import_missing_paths_stale(
            source.provider,
            &source_root,
            &current_paths,
            observed_at_ms,
        )?;
        Ok(())
    })();
    match persist {
        Ok(()) => store.commit_batch()?,
        Err(err) => {
            let _ = store.rollback_batch();
            return Err(err);
        }
    }

    let pending = store.list_pending_source_import_files(source.provider, &source_root)?;
    if pending.is_empty() {
        return Ok(ProviderImportSummary {
            unchanged_sources: 1,
            ..ProviderImportSummary::default()
        });
    }

    let mut summary = ProviderImportSummary::default();
    for pending_file in pending {
        let path = PathBuf::from(&pending_file.source_path);
        let mut pending_source = explicit_path_source(source.provider, path);
        pending_source.source_format = source.source_format;
        let imported =
            import_one_source_inner(store, &pending_source, progress.clone(), false, true);
        match imported {
            Ok(mut file_summary) => {
                let file_stats = SourceStats {
                    files: 1,
                    bytes: pending_file.file_size_bytes,
                };
                if pending_file.file_size_bytes == 0 {
                    file_summary.empty_files = 1;
                }
                let health = classify_import_health(&file_stats, &file_summary);
                if health.zero_yield_anomaly() {
                    file_summary.zero_yield_anomalies = 1;
                    store.mark_source_import_file_failed(
                        source.provider,
                        &source_root,
                        &pending_file.source_path,
                        SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
                        utc_now().timestamp_millis(),
                    )?;
                } else {
                    store.mark_source_import_file_indexed(
                        source.provider,
                        SourceImportFileIndexUpdate {
                            source_root: &source_root,
                            source_path: &pending_file.source_path,
                            file_size_bytes: pending_file.file_size_bytes,
                            file_modified_at_ms: pending_file.file_modified_at_ms,
                            indexed_at_ms: utc_now().timestamp_millis(),
                        },
                    )?;
                }
                merge_provider_import_summary(&mut summary, file_summary);
            }
            Err(err) => {
                store.mark_source_import_file_failed(
                    source.provider,
                    &source_root,
                    &pending_file.source_path,
                    &err.to_string(),
                    utc_now().timestamp_millis(),
                )?;
                return Err(err);
            }
        }
    }

    let _ = record_id;
    let _ = tool_output_mode;
    let _ = event_mode;
    let _ = include_notices;
    Ok(summary)
}

/// Detect stores written by an older adapter whose sync cursor format has
/// since changed, and force one full rescan so history the old adapter
/// missed is picked up.
///
/// OpenCode: cursors written before the message/part-aware adapter look like
/// `session_message:<session_id>:seq:<n>`. The new adapter prefixes all
/// cursors with `opencode-v2:`. An old-format cursor means the store may only
/// contain the (nearly empty) session_message projection, so re-scan the
/// database once; event-level dedupe keeps this idempotent.
fn source_cursor_requires_rescan(store: &Store, source: &SourceInfo) -> Result<bool> {
    if source.provider != CaptureProvider::OpenCode {
        return Ok(false);
    }
    let machine_id = OpenCodeSqliteImportOptions::default().machine_id;
    let stream = format!(
        "provider:{}:{}",
        source.provider.as_str(),
        source.source_format
    );
    let Some(cursor) = store.get_sync_cursor(None, &machine_id, &stream)? else {
        return Ok(false);
    };
    Ok(!cursor.cursor.starts_with(OPENCODE_CURSOR_V2_PREFIX))
}

fn source_uses_import_file_manifest(source: &SourceInfo) -> bool {
    !matches!(
        source.source_format,
        "codex_session_jsonl_tree"
            | "openclaw_session_jsonl_tree"
            | "hermes_state_sqlite"
            | "nanoclaw_project"
            | "astrbot_data_v4_sqlite"
    )
}

fn merge_provider_import_summary(
    summary: &mut ProviderImportSummary,
    other: ProviderImportSummary,
) {
    summary.imported += other.imported;
    summary.skipped += other.skipped;
    summary.failed += other.failed;
    summary.redacted += other.redacted;
    summary.imported_sessions += other.imported_sessions;
    summary.skipped_sessions += other.skipped_sessions;
    summary.imported_events += other.imported_events;
    summary.skipped_events += other.skipped_events;
    summary.imported_edges += other.imported_edges;
    summary.skipped_edges += other.skipped_edges;
    summary.unchanged_sources += other.unchanged_sources;
    summary.zero_yield_anomalies += other.zero_yield_anomalies;
    summary.empty_sources += other.empty_sources;
    summary.empty_files += other.empty_files;
    summary.failures.extend(other.failures);
    summary.notes.extend(other.notes);
}

fn collect_source_import_files(source: &SourceInfo) -> Result<Vec<SourceImportFile>> {
    let paths = collect_source_import_paths(source)?;
    let source_root = source.path.display().to_string();
    let observed_at_ms = utc_now().timestamp_millis();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("stat import source file {}", path.display()))?;
        files.push(SourceImportFile {
            provider: source.provider,
            source_format: source.source_format.to_owned(),
            source_root: source_root.clone(),
            source_path: path.display().to_string(),
            file_size_bytes: metadata.len(),
            file_modified_at_ms: system_time_ms(metadata.modified().unwrap_or(UNIX_EPOCH)),
            observed_at_ms,
            metadata: json!({}),
        });
    }
    Ok(files)
}

fn collect_source_import_paths(source: &SourceInfo) -> Result<Vec<PathBuf>> {
    let metadata = fs::symlink_metadata(&source.path)
        .with_context(|| format!("stat import source {}", source.path.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "symlinked provider transcript roots are rejected: {}",
            source.path.display()
        ));
    }
    if metadata.file_type().is_file() {
        return Ok(if source_import_file_matches(source, &source.path) {
            vec![source.path.clone()]
        } else {
            Vec::new()
        });
    }
    if !metadata.file_type().is_dir() {
        return Ok(Vec::new());
    }

    let mut paths = Vec::new();
    let mut stack = vec![source.path.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("read import source directory {}", dir.display()))?
        {
            let entry = entry
                .with_context(|| format!("read import source entry under {}", dir.display()))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat import source entry {}", path.display()))?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() && source_import_file_matches(source, &path) {
                paths.push(path);
            }
        }
    }
    paths.sort();
    Ok(paths)
}

fn source_import_file_matches(source: &SourceInfo, path: &Path) -> bool {
    match source.provider {
        CaptureProvider::OpenCode => path == source.path,
        CaptureProvider::CopilotCli => {
            path.file_name().and_then(|name| name.to_str()) == Some("events.jsonl")
        }
        CaptureProvider::Antigravity => matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("transcript_full.jsonl" | "transcript.jsonl")
        ),
        CaptureProvider::Gemini => {
            path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "chats")
        }
        CaptureProvider::Cursor => {
            path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "agent-transcripts")
        }
        _ => path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"),
    }
}

fn system_time_ms(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn import_incremental_codex_session_tree(
    store: &mut Store,
    source: &SourceInfo,
    record_id: Uuid,
    tool_output_mode: CodexToolOutputMode,
    event_mode: CodexEventImportMode,
    include_notices: bool,
    progress: Option<CodexSessionImportProgressCallback>,
) -> Result<ProviderImportSummary> {
    let source_root = source.path.display().to_string();
    catalog_codex_session_tree(
        &source.path,
        store,
        CodexSessionCatalogOptions {
            source_root: Some(source.path.clone()),
            allow_partial_failures: true,
            ..CodexSessionCatalogOptions::default()
        },
    )
    .with_context(|| format!("catalog Codex sessions from {}", source.path.display()))?;

    let pending = store.list_pending_catalog_sessions(CaptureProvider::Codex, &source_root)?;
    if pending.is_empty() {
        return Ok(ProviderImportSummary {
            unchanged_sources: 1,
            ..ProviderImportSummary::default()
        });
    }

    let mut summary = ProviderImportSummary::default();
    let mut full_import_sessions = Vec::new();
    for session in &pending {
        let state = store.catalog_source_index_state(
            CaptureProvider::Codex,
            &source_root,
            &session.source_path,
        )?;
        let tail_start = state
            .as_ref()
            .and_then(|state| state.last_imported_file_size_bytes)
            .filter(|indexed_size| *indexed_size > 0 && *indexed_size < session.file_size_bytes);
        if let Some(start_offset) = tail_start {
            let checkpoint_hash = state
                .as_ref()
                .and_then(|state| state.last_imported_file_sha256.as_deref());
            if !catalog_import_checkpoint_matches(
                Path::new(&session.source_path),
                start_offset,
                checkpoint_hash,
            )? {
                full_import_sessions.push(session.clone());
                continue;
            }
            let tail_summary = match import_codex_session_jsonl_tail(
                PathBuf::from(&session.source_path),
                start_offset,
                store,
                CodexSessionImportOptions {
                    source_path: Some(source.path.clone()),
                    history_record_id: Some(record_id),
                    allow_partial_failures: true,
                    tool_output_mode,
                    event_mode,
                    include_notices,
                    progress: progress.clone(),
                    ..CodexSessionImportOptions::default()
                },
            )
            .map_err(anyhow::Error::from)
            {
                Ok(summary) => summary,
                Err(err) => {
                    mark_catalog_sessions_failed(
                        store,
                        std::slice::from_ref(session),
                        &err.to_string(),
                    )?;
                    return Err(err);
                }
            };
            if tail_summary.failed > 0 {
                mark_catalog_sessions_failed(
                    store,
                    std::slice::from_ref(session),
                    "tail import failed for one or more appended events",
                )?;
                merge_provider_import_summary(&mut summary, tail_summary);
                continue;
            }
            let tail_entities = tail_summary.imported_sessions
                + tail_summary.imported_events
                + tail_summary.imported_edges
                + tail_summary.skipped_sessions
                + tail_summary.skipped_events
                + tail_summary.skipped_edges;
            if tail_entities == 0 {
                store.mark_catalog_source_failed(
                    CaptureProvider::Codex,
                    &session.source_root,
                    &session.source_path,
                    SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
                    utc_now().timestamp_millis(),
                )?;
                summary.zero_yield_anomalies += 1;
                continue;
            }
            let tail_event_count = tail_summary
                .imported_events
                .saturating_add(tail_summary.skipped_events)
                as u64;
            let event_count = state
                .and_then(|state| state.last_imported_event_count)
                .map(|event_count| event_count.saturating_add(tail_event_count));
            mark_catalog_session_indexed(
                store,
                session,
                event_count,
                utc_now().timestamp_millis(),
            )?;
            merge_provider_import_summary(&mut summary, tail_summary);
        } else {
            full_import_sessions.push(session.clone());
        }
    }

    if !full_import_sessions.is_empty() {
        let paths = full_import_sessions
            .iter()
            .map(|session| PathBuf::from(&session.source_path))
            .collect::<Vec<_>>();
        let full_summary = match import_codex_session_paths(
            paths,
            store,
            CodexSessionImportOptions {
                source_path: Some(source.path.clone()),
                history_record_id: Some(record_id),
                allow_partial_failures: true,
                tool_output_mode,
                event_mode,
                include_notices,
                progress,
                ..CodexSessionImportOptions::default()
            },
        )
        .map_err(anyhow::Error::from)
        {
            Ok(summary) => summary,
            Err(err) => {
                mark_catalog_sessions_failed(store, &full_import_sessions, &err.to_string())?;
                return Err(err);
            }
        };
        let (anomalies, empty_files) = mark_catalog_sessions_indexed_or_anomalous(
            store,
            &full_import_sessions,
            &full_summary,
        )?;
        if anomalies > 0 {
            summary.zero_yield_anomalies += anomalies;
        }
        if empty_files > 0 {
            summary.empty_files += empty_files;
        }
        merge_provider_import_summary(&mut summary, full_summary);
    }
    Ok(summary)
}

fn mark_catalog_sessions_indexed_or_anomalous(
    store: &Store,
    sessions: &[CatalogSession],
    summary: &ProviderImportSummary,
) -> Result<(usize, usize)> {
    let batch_entities = summary.imported_sessions
        + summary.imported_events
        + summary.imported_edges
        + summary.skipped_sessions
        + summary.skipped_events
        + summary.skipped_edges;
    let mut anomalies = 0usize;
    let mut empty_files = 0usize;
    let has_failures_or_skips = summary.failed > 0
        || summary.skipped > 0
        || summary.skipped_sessions > 0
        || summary.skipped_events > 0
        || summary.skipped_edges > 0;
    let indexed_at_ms = utc_now().timestamp_millis();
    let external_session_ids = sessions
        .iter()
        .filter_map(|session| session.external_session_id.clone())
        .collect::<Vec<_>>();
    let existing_external_session_ids =
        store.existing_external_session_ids(CaptureProvider::Codex, &external_session_ids)?;
    for session in sessions {
        if session.file_size_bytes == 0 {
            mark_catalog_session_indexed(store, session, None, indexed_at_ms)?;
            empty_files += 1;
            continue;
        }
        let has_indexed_session = match session.external_session_id.as_deref() {
            Some(external_session_id) => {
                existing_external_session_ids.contains(external_session_id)
            }
            None => batch_entities > 0,
        };
        if has_indexed_session {
            let event_count = (sessions.len() == 1).then_some(
                summary
                    .imported_events
                    .saturating_add(summary.skipped_events) as u64,
            );
            mark_catalog_session_indexed(store, session, event_count, indexed_at_ms)?;
        } else if has_failures_or_skips {
            store.mark_catalog_source_failed(
                CaptureProvider::Codex,
                &session.source_root,
                &session.source_path,
                CATALOG_IMPORT_OUTCOME_UNATTRIBUTED_CODE,
                indexed_at_ms,
            )?;
        } else if batch_entities == 0 || session.external_session_id.is_some() {
            store.mark_catalog_source_failed(
                CaptureProvider::Codex,
                &session.source_root,
                &session.source_path,
                SOURCE_IMPORT_ZERO_YIELD_ANOMALY_CODE,
                indexed_at_ms,
            )?;
            anomalies += 1;
        } else {
            mark_catalog_session_indexed(store, session, None, indexed_at_ms)?;
        }
    }
    Ok((anomalies, empty_files))
}

fn mark_catalog_session_indexed(
    store: &Store,
    session: &CatalogSession,
    event_count: Option<u64>,
    indexed_at_ms: i64,
) -> Result<()> {
    let file_sha256 =
        sha256_file_prefix_hex(Path::new(&session.source_path), session.file_size_bytes)
            .with_context(|| format!("hash checkpoint prefix for {}", session.source_path))?;
    store.mark_catalog_source_indexed(
        session.provider,
        CatalogSourceIndexUpdate {
            source_root: &session.source_root,
            source_path: &session.source_path,
            file_size_bytes: session.file_size_bytes,
            file_modified_at_ms: session.file_modified_at_ms,
            file_sha256: Some(&file_sha256),
            event_count,
            indexed_at_ms,
        },
    )?;
    Ok(())
}

fn catalog_import_checkpoint_matches(
    path: &Path,
    byte_count: u64,
    expected_sha256: Option<&str>,
) -> Result<bool> {
    let Some(expected_sha256) = expected_sha256 else {
        return Ok(true);
    };
    let actual_sha256 = sha256_file_prefix_hex(path, byte_count)?;
    Ok(actual_sha256 == expected_sha256)
}

fn sha256_file_prefix_hex(path: &Path, byte_count: u64) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut remaining = byte_count;
    let mut buffer = [0_u8; 8192];
    while remaining > 0 {
        let to_read = buffer.len().min(remaining as usize);
        let read = file.read(&mut buffer[..to_read])?;
        if read == 0 {
            return Err(anyhow!(
                "file ended before checkpoint byte offset {byte_count}: {}",
                path.display()
            ));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn mark_catalog_sessions_failed(
    store: &Store,
    sessions: &[CatalogSession],
    error: &str,
) -> Result<()> {
    let indexed_at_ms = utc_now().timestamp_millis();
    for session in sessions {
        store.mark_catalog_source_failed(
            session.provider,
            &session.source_root,
            &session.source_path,
            error,
            indexed_at_ms,
        )?;
    }
    Ok(())
}

fn source_uses_incremental_event_search(source: &SourceInfo) -> bool {
    matches!(
        source.provider,
        CaptureProvider::Codex
            | CaptureProvider::Claude
            | CaptureProvider::Pi
            | CaptureProvider::Cursor
            | CaptureProvider::OpenCode
            | CaptureProvider::Antigravity
            | CaptureProvider::Gemini
            | CaptureProvider::CopilotCli
            | CaptureProvider::FactoryAiDroid
    )
}

fn codex_tool_output_mode() -> Result<CodexToolOutputMode> {
    if let Some(raw) = env::var_os("CTX_CODEX_TOOL_OUTPUT_MODE") {
        let raw = raw.to_string_lossy();
        return match raw.as_ref() {
            "full" => Ok(CodexToolOutputMode::Full),
            "metadata" => Ok(CodexToolOutputMode::Metadata),
            "failures" | "failure" | "errors" | "error" => Ok(CodexToolOutputMode::Failures),
            "skip" => Ok(CodexToolOutputMode::Skip),
            other => Err(anyhow!(
                "unsupported CTX_CODEX_TOOL_OUTPUT_MODE={other:?}; expected full, metadata, failures, or skip"
            )),
        };
    }
    if env::var_os("CTX_EXPERIMENTAL_SKIP_TOOL_OUTPUTS").is_some() {
        return Ok(CodexToolOutputMode::Skip);
    }
    Ok(CodexToolOutputMode::Skip)
}

fn codex_event_import_mode() -> Result<CodexEventImportMode> {
    if let Some(raw) = env::var_os("CTX_CODEX_EVENT_MODE") {
        let raw = raw.to_string_lossy();
        return match raw.as_ref() {
            "search" | "message" | "messages" => Ok(CodexEventImportMode::Search),
            "rich" | "full" => Ok(CodexEventImportMode::Rich),
            other => Err(anyhow!(
                "unsupported CTX_CODEX_EVENT_MODE={other:?}; expected search or rich"
            )),
        };
    }
    Ok(CodexEventImportMode::Search)
}

fn codex_include_notices() -> bool {
    env::var_os("CTX_CODEX_INCLUDE_NOTICES").is_some()
}

fn source_stats(path: &Path) -> Result<SourceStats> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("stat import source {}", path.display()))?;
    if metadata.file_type().is_file() {
        return Ok(SourceStats {
            files: 1,
            bytes: metadata.len(),
        });
    }
    if !metadata.file_type().is_dir() {
        return Ok(SourceStats::default());
    }

    let mut stats = SourceStats::default();
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("read import source directory {}", dir.display()))?
        {
            let entry = entry
                .with_context(|| format!("read import source entry under {}", dir.display()))?;
            let entry_path = entry.path();
            let file_type = entry
                .file_type()
                .with_context(|| format!("stat import source entry {}", entry_path.display()))?;
            if file_type.is_dir() {
                stack.push(entry_path);
            } else if file_type.is_file() {
                let metadata = entry
                    .metadata()
                    .with_context(|| format!("stat import source file {}", entry_path.display()))?;
                stats.files += 1;
                stats.bytes = stats.bytes.saturating_add(metadata.len());
            }
        }
    }
    Ok(stats)
}

fn import_record_for_source(source: &SourceInfo) -> HistoryRecord {
    let key = format!(
        "agent-history:{}:{}",
        source.provider.as_str(),
        source.path.display()
    );
    let mut record = HistoryRecord::new(
        format!("{} agent history", source.provider.as_str()),
        format!(
            "Indexed local agent history from {} ({})",
            source.path.display(),
            source.source_format
        ),
        vec!["agent-history".into(), source.provider.as_str().into()],
        "agent_history",
        source.path.parent().map(|path| path.display().to_string()),
    );
    record.id = stable_capture_uuid(&key, "record");
    record
}

fn import_record_for_custom_history(path: &Path, format: ImportFormatArg) -> HistoryRecord {
    let key = format!("custom-history:{}:{}", format.as_str(), path.display());
    let mut record = HistoryRecord::new(
        "custom agent history".to_owned(),
        format!(
            "Indexed custom agent history from {} ({})",
            path.display(),
            format.as_str()
        ),
        vec![
            "agent-history".into(),
            "custom".into(),
            format.as_str().into(),
        ],
        "agent_history",
        path.parent().map(|path| path.display().to_string()),
    );
    record.id = stable_capture_uuid(&key, "record");
    record
}

fn import_record_for_history_source_plugin(source: &HistorySourcePluginSource) -> HistoryRecord {
    let key = format!(
        "history-source-plugin:{}:{}:{}:{}:{}",
        source.plugin_name, source.id, source.provider_key, source.source_id, source.source_format
    );
    let mut record = HistoryRecord::new(
        format!("history source plugin {}", source.label()),
        format!(
            "Indexed custom agent history from history source plugin {} ({})",
            source.label(),
            source.source_format
        ),
        vec![
            "agent-history".into(),
            "custom".into(),
            "history-source-plugin".into(),
            source.provider_key.clone(),
            source.source_format.clone(),
        ],
        "agent_history",
        source
            .manifest_path
            .parent()
            .map(|path| path.display().to_string()),
    );
    record.id = stable_capture_uuid(&key, "record");
    record
}

fn discovered_sources() -> Vec<SourceInfo> {
    home_dir()
        .as_deref()
        .map(discover_provider_sources)
        .unwrap_or_default()
}

fn discovered_sources_for_provider(provider: CaptureProvider) -> Vec<SourceInfo> {
    home_dir()
        .as_deref()
        .map(|home| discover_provider_sources_for_provider(home, provider))
        .unwrap_or_default()
}

fn explicit_path_source(provider: CaptureProvider, path: PathBuf) -> SourceInfo {
    source_for_path(provider, path)
}

fn source_for_path(provider: CaptureProvider, path: PathBuf) -> SourceInfo {
    provider_source_for_path(provider, path)
}

pub(crate) fn plugin_source_projections(
    sources: &[HistorySourcePluginSource],
) -> Vec<HistorySourcePluginSourceProjection> {
    sources
        .iter()
        .map(|source| HistorySourcePluginSourceProjection {
            plugin_name: source.plugin_name.clone(),
            plugin_display_name: source.plugin_display_name.clone(),
            plugin_version: source.plugin_version.clone(),
            manifest_path: source.manifest_path.clone(),
            id: source.id.clone(),
            display_name: source.display_name.clone(),
            provider_key: source.provider_key.clone(),
            source_id: source.source_id.clone(),
            source_format: source.source_format.clone(),
            enabled: source.enabled,
            refresh: history_source_plugin_refresh_json(source.refresh),
        })
        .collect()
}

pub(crate) fn plugin_failure_projections(
    failures: &[HistorySourcePluginManifestFailure],
) -> Vec<HistorySourcePluginFailureProjection> {
    failures
        .iter()
        .map(|failure| HistorySourcePluginFailureProjection {
            manifest_path: failure.manifest_path.clone(),
            error: failure.error.clone(),
        })
        .collect()
}

fn history_source_plugin_refresh_json(refresh: HistorySourcePluginRefresh) -> &'static str {
    match refresh {
        HistorySourcePluginRefresh::Manual => "manual",
        HistorySourcePluginRefresh::Auto => "auto",
    }
}

fn search_filters(
    input: SearchFilterInput,
    store: Option<&Store>,
) -> Result<ctx_history_search::SearchFilters> {
    let source_identity = normalize_source_identity_filters(input.source_identity)?;
    if !source_identity.is_empty()
        && input
            .provider
            .is_some_and(|provider| !matches!(provider, ProviderArg::Custom))
    {
        return Err(anyhow!(
            "custom history source filters can only be combined with --provider custom"
        ));
    }
    let provider = if !source_identity.is_empty() {
        Some(CaptureProvider::Custom)
    } else {
        input.provider.map(ProviderArg::capture_provider)
    };
    let session = input
        .session
        .as_deref()
        .map(|value| {
            let store = store.ok_or_else(|| {
                anyhow!("session id prefix resolution requires an open ctx store")
            })?;
            resolve_session_id(store, value)
        })
        .transpose()?;
    let exclude_provider_session = if input.include_current_session || session.is_some() {
        None
    } else {
        current_codex_provider_session_filter(store)
    };
    Ok(ctx_history_search::SearchFilters {
        session,
        provider,
        history_source: source_identity.history_source,
        provider_key: source_identity.provider_key,
        source_id: source_identity.source_id,
        source_format: source_identity.source_format,
        repo: input.workspace,
        since: input.since.as_deref().map(parse_since_filter).transpose()?,
        primary_only: input.primary_only,
        include_subagents: input.include_subagents && !input.primary_only,
        event_type: input
            .event_type
            .as_deref()
            .map(EventType::from_str)
            .transpose()
            .map_err(|err| anyhow!("{err}"))?,
        roles: parse_event_roles("--role", input.role)?,
        exclude_roles: parse_event_roles("--exclude-role", input.exclude_role)?,
        exclude_tool_noise: input.exclude_tool_noise,
        exclude_tool_names: normalize_cli_filter_list("--exclude-tool", input.exclude_tool_name)?,
        file: input.file.map(|path| path.display().to_string()),
        exclude_provider_session,
    })
}

fn parse_event_roles(label: &str, values: Vec<String>) -> Result<Vec<EventRole>> {
    values
        .into_iter()
        .map(|value| EventRole::from_str(value.trim()).map_err(|err| anyhow!("{label}: {err}")))
        .collect()
}

/// Normalizes a repeatable name-valued filter: values are trimmed, empty
/// values are rejected, and duplicates (ASCII case-insensitive, matching the
/// tool-name normalization applied at match time) are dropped while
/// preserving first-occurrence order.
fn normalize_cli_filter_list(label: &str, values: Vec<String>) -> Result<Vec<String>> {
    let mut normalized = Vec::<String>::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            return Err(anyhow!("{label} cannot be empty"));
        }
        if !normalized
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(value))
        {
            normalized.push(value.to_owned());
        }
    }
    Ok(normalized)
}

fn normalize_source_identity_filters(
    input: SourceIdentityFilterArgs,
) -> Result<SourceIdentityFilters> {
    let history_source = normalize_source_identity_filter("history-source", input.history_source)?;
    if history_source
        .as_deref()
        .is_some_and(|value| !value.contains('/'))
    {
        return Err(anyhow!(
            "--history-source expects plugin/source or provider_key/source_id"
        ));
    }
    Ok(SourceIdentityFilters {
        history_source,
        provider_key: normalize_source_identity_filter("provider-key", input.provider_key)?,
        source_id: normalize_source_identity_filter("source-id", input.source_id)?,
        source_format: normalize_source_identity_filter("source-format", input.source_format)?,
    })
}

fn normalize_source_identity_filter(label: &str, value: Option<String>) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("--{label} cannot be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(anyhow!("--{label} cannot contain control characters"));
    }
    Ok(Some(value.to_owned()))
}

fn current_codex_provider_session_filter(
    store: Option<&Store>,
) -> Option<ctx_history_search::ProviderSessionFilter> {
    let provider_session_id = std::env::var("CODEX_THREAD_ID").ok()?;
    let provider_session_id = provider_session_id.trim();
    if provider_session_id.is_empty() {
        return None;
    }
    let session_id = store
        .and_then(|store| {
            store
                .session_by_external_session(CaptureProvider::Codex, provider_session_id)
                .ok()
                .flatten()
        })
        .map(|session| session.id);
    Some(ctx_history_search::ProviderSessionFilter {
        provider: CaptureProvider::Codex,
        provider_session_id: provider_session_id.to_owned(),
        session_id,
    })
}

fn parse_since_filter(value: &str) -> Result<chrono::DateTime<Utc>> {
    let trimmed = value.trim();
    if let Some(days) = trimmed.strip_suffix('d') {
        let days: i64 = days
            .parse()
            .with_context(|| format!("invalid --since day window: {value}"))?;
        return Ok(utc_now() - Duration::days(days));
    }
    Ok(chrono::DateTime::parse_from_rfc3339(trimmed)
        .with_context(|| format!("invalid --since value: {value}"))?
        .with_timezone(&Utc))
}

fn print_json(value: Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn print_share_safe_value(mut value: Value) -> Result<()> {
    mark_share_safe(&mut value);
    print_json(value)
}

fn mark_share_safe(value: &mut Value) {
    if let Value::Object(map) = value {
        map.entry("share_safe").or_insert(Value::Bool(false));
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_import_checkpoint_matches, classify_import_health,
        history_source_plugin_cursor_only, persist_source_health, sha256_file_prefix_hex,
        shell_quote_arg, write_json_record, ImportHealth, ImportHealthClassification, ImportReport,
        ImportSourceReport, ImportTotals, SourceStats,
    };
    use ctx_history_capture::ProviderImportSummary;
    use ctx_history_store::{SourceHealthClassification, Store};
    use std::{fs, io::Write};
    use tempfile::tempdir;

    #[test]
    fn shell_quote_arg_uses_single_quotes_for_shell_metacharacters() {
        assert_eq!(shell_quote_arg("onboarding"), "onboarding");
        assert_eq!(
            shell_quote_arg("$(touch /tmp/ctx-owned)'s"),
            "'$(touch /tmp/ctx-owned)'\\''s'"
        );
    }

    struct OtherIoFailure;

    impl Write for OtherIoFailure {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("not a broken pipe"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn ndjson_writer_propagates_non_broken_pipe_io_errors() {
        let error =
            write_json_record(&mut OtherIoFailure, &serde_json::json!({"ok": true})).unwrap_err();
        assert!(error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::Other));
    }

    #[test]
    fn catalog_import_checkpoint_requires_matching_hash() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        {
            let mut file = fs::File::create(&path).unwrap();
            writeln!(file, "prefix").unwrap();
        }
        let prefix_hash = sha256_file_prefix_hex(&path, 7).unwrap();
        assert!(catalog_import_checkpoint_matches(&path, 7, Some(&prefix_hash)).unwrap());
        assert!(catalog_import_checkpoint_matches(&path, 7, None).unwrap());

        fs::write(&path, "mutated\n").unwrap();
        assert!(!catalog_import_checkpoint_matches(&path, 7, Some(&prefix_hash)).unwrap());
    }

    #[test]
    fn import_health_classifies_zero_yield_without_safe_reason_as_anomaly() {
        let stats = SourceStats {
            files: 1,
            bytes: 42,
        };
        let summary = ProviderImportSummary::default();
        assert_eq!(
            classify_import_health(&stats, &summary).classification,
            ImportHealthClassification::ZeroYieldAnomaly
        );
    }

    #[test]
    fn import_health_does_not_flag_empty_or_all_skipped() {
        assert_eq!(
            classify_import_health(&SourceStats::default(), &ProviderImportSummary::default())
                .classification,
            ImportHealthClassification::Empty
        );
        let summary = ProviderImportSummary {
            skipped: 3,
            skipped_events: 3,
            ..ProviderImportSummary::default()
        };
        assert_eq!(
            classify_import_health(
                &SourceStats {
                    files: 1,
                    bytes: 42
                },
                &summary
            )
            .classification,
            ImportHealthClassification::AllSkipped
        );
    }

    #[test]
    fn import_health_classifies_partial_and_malformed() {
        let stats = SourceStats {
            files: 1,
            bytes: 42,
        };
        let partial = ProviderImportSummary {
            imported_events: 1,
            failed: 1,
            ..ProviderImportSummary::default()
        };
        assert_eq!(
            classify_import_health(&stats, &partial).classification,
            ImportHealthClassification::PartialSuccess
        );

        let malformed = ProviderImportSummary {
            failed: 1,
            ..ProviderImportSummary::default()
        };
        assert_eq!(
            classify_import_health(&stats, &malformed).classification,
            ImportHealthClassification::UnsupportedOrMalformed
        );
    }

    #[test]
    fn plugin_cursor_only_is_not_anomaly() {
        assert!(history_source_plugin_cursor_only(
            &ProviderImportSummary::default(),
            true
        ));
    }

    #[test]
    fn unchanged_health_does_not_heal_existing_source_anomaly() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::write(&source, "source").unwrap();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        store
            .upsert_source_health(
                "custom",
                "format",
                &source,
                "id",
                SourceHealthClassification::ZeroYieldAnomaly,
            )
            .unwrap();
        let unchanged = ImportHealth {
            classification: ImportHealthClassification::Unchanged,
            reason_counts: serde_json::json!({}),
            zero_yield_anomaly_count: 0,
        };
        persist_source_health(&store, "custom", "format", &source, "id", &unchanged, true).unwrap();
        assert_eq!(store.source_health_counts().unwrap().zero_yield_anomaly, 1);
    }

    #[test]
    fn report_detects_zero_yield_anomaly_without_path_content() {
        let report = ImportReport {
            resume: false,
            totals: ImportTotals::default(),
            sources: vec![ImportSourceReport {
                health: ImportHealth {
                    classification: ImportHealthClassification::ZeroYieldAnomaly,
                    reason_counts: serde_json::json!({}),
                    zero_yield_anomaly_count: 1,
                },
                json: serde_json::json!({"provider": "codex"}),
            }],
            health_persistence_failures: 0,
        };
        assert!(report.has_zero_yield_anomaly());
    }

    #[test]
    fn aggregate_counts_source_rows_and_anomalous_files_separately() {
        let summary = ProviderImportSummary {
            imported_events: 1,
            zero_yield_anomalies: 2,
            ..ProviderImportSummary::default()
        };
        let stats = SourceStats { files: 3, bytes: 9 };
        let health = classify_import_health(&stats, &summary);
        assert_eq!(
            health.classification,
            ImportHealthClassification::PartialSuccess
        );
        assert_eq!(health.zero_yield_anomaly_count, 2);
        let mut totals = ImportTotals::default();
        totals.add_with_health(&summary, &stats, &health);
        assert_eq!(totals.zero_yield_anomaly_sources, 1);
    }

    #[test]
    fn empty_file_bookkeeping_is_safe_with_imported_siblings() {
        let summary = ProviderImportSummary {
            imported_events: 1,
            empty_files: 1,
            ..ProviderImportSummary::default()
        };
        let health = classify_import_health(
            &SourceStats {
                files: 2,
                bytes: 42,
            },
            &summary,
        );
        assert_eq!(health.classification, ImportHealthClassification::Success);
        assert_eq!(health.zero_yield_anomaly_count, 0);
        let mut totals = ImportTotals::default();
        totals.add_with_health(
            &summary,
            &SourceStats {
                files: 2,
                bytes: 42,
            },
            &health,
        );
        assert_eq!(totals.zero_yield_anomaly_sources, 0);
    }

    #[test]
    fn search_refresh_completed_degrades_when_import_health_has_anomaly() {
        use super::{RefreshArg, SearchRefreshReport};
        let totals = ImportTotals {
            zero_yield_anomaly_sources: 1,
            ..ImportTotals::default()
        };
        let report = SearchRefreshReport::completed(RefreshArg::Auto, 1, totals, 7);
        assert_eq!(report.status, "degraded_zero_yield");
        assert_eq!(report.reason, "zero_yield_anomaly");
        assert_eq!(report.to_json()["status"], "degraded_zero_yield");
        assert_eq!(report.to_json()["reason"], "zero_yield_anomaly");
    }

    #[test]
    fn opencode_old_format_cursor_forces_full_rescan_once() {
        use super::{provider_source_for_path, source_cursor_requires_rescan};
        use ctx_history_capture::{OpenCodeSqliteImportOptions, OPENCODE_CURSOR_V2_PREFIX};
        use ctx_history_core::{utc_now, CaptureProvider, EntityTimestamps, SyncCursor};
        use ctx_history_store::Store;
        use uuid::Uuid;

        let temp = tempdir().unwrap();
        let store = Store::open(temp.path().join("work.sqlite")).unwrap();
        let source =
            provider_source_for_path(CaptureProvider::OpenCode, temp.path().join("opencode.db"));
        let machine_id = OpenCodeSqliteImportOptions::default().machine_id;

        // No cursor yet: nothing to migrate.
        assert!(!source_cursor_requires_rescan(&store, &source).unwrap());

        let mut cursor = SyncCursor {
            id: Uuid::new_v4(),
            team_id: None,
            device_id: machine_id,
            stream: format!("provider:opencode:{}", source.source_format),
            cursor: "session_message:ses_123:seq:1".to_owned(),
            last_synced_at: None,
            timestamps: EntityTimestamps {
                created_at: utc_now(),
                updated_at: utc_now(),
            },
        };
        store.upsert_sync_cursor(&cursor).unwrap();
        // Old-format cursor written by the pre-message/part adapter: rescan.
        assert!(source_cursor_requires_rescan(&store, &source).unwrap());

        cursor.cursor = format!("{OPENCODE_CURSOR_V2_PREFIX}message_part:ses_123:prt_1");
        store.upsert_sync_cursor(&cursor).unwrap();
        // New-format cursor: no rescan needed.
        assert!(!source_cursor_requires_rescan(&store, &source).unwrap());
    }
}
