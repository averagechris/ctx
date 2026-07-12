use std::{
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use ctx_history_core::{database_path, CtxIdPrefix, EventType, SearchMatchMode};
use ctx_history_query::{
    raw_sql_result_json, sources_json as query_sources_json, status_json as query_status_json,
    status_snapshot as query_status_snapshot, BytePolicy, FieldSet, QueryService,
    TranscriptMode as QueryTranscriptMode, DEFAULT_ITEM_BYTES, DEFAULT_PAGE_BYTES, MAX_ITEM_BYTES,
    MAX_PAGE_BYTES, MAX_SHOW_LIMIT,
};
use ctx_history_store::{
    IdPrefixResolution, RawSqlOptions, Store, RAW_SQL_DEFAULT_MAX_COLUMNS,
    RAW_SQL_DEFAULT_MAX_ROWS, RAW_SQL_DEFAULT_MAX_SQL_BYTES, RAW_SQL_DEFAULT_MAX_VALUE_BYTES,
    RAW_SQL_DEFAULT_TIMEOUT, RAW_SQL_MAX_COLUMNS_CAP, RAW_SQL_MAX_ROWS_CAP,
    RAW_SQL_MAX_SQL_BYTES_CAP, RAW_SQL_MAX_TIMEOUT, RAW_SQL_MAX_VALUE_BYTES_CAP,
};
use serde_json::{json, Value};
use uuid::Uuid;

use super::{
    command_from_argv, compact_json, config::CONFIG_FILE, discovered_sources, event_page_json,
    event_window, event_window_json, mark_share_safe, plugin_failure_projections,
    plugin_source_projections, search_filters, search_has_intent, search_next_argv,
    search_page_json, show_session_next_argv, FieldArg, OutputFormat, ProviderArg, RefreshArg,
    SearchArgs, SearchFilterInput, SearchIntentInput, SearchMatchArg, SearchRefreshReport,
    ShowSessionArgs, SourceIdentityFilterArgs, TranscriptMode, MAX_SEARCH_LIMIT,
};

const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const MCP_MAX_EVENT_WINDOW: usize = 50;

#[derive(Debug, Args)]
pub(crate) struct McpArgs {
    #[command(subcommand)]
    command: McpCommand,
}

#[derive(Debug, Subcommand)]
enum McpCommand {
    #[command(
        about = "Serve a read-only MCP server over stdio",
        long_about = "Serve a read-only MCP server over newline-delimited stdio JSON-RPC.\n\nExample:\n  printf '%s\\n' '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-11-25\",\"capabilities\":{},\"clientInfo\":{\"name\":\"client\",\"version\":\"0\"}}}' | ctx mcp serve"
    )]
    Serve(McpServeArgs),
}

#[derive(Debug, Args)]
struct McpServeArgs {}

pub(crate) fn run(args: McpArgs, data_root: PathBuf) -> Result<()> {
    match args.command {
        McpCommand::Serve(_) => serve_stdio(data_root),
    }
}

fn serve_stdio(data_root: PathBuf) -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    let mut initialized = false;

    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(response) = handle_line(line, &data_root, &mut initialized) {
            writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
            stdout.flush()?;
        }
    }
    Ok(())
}

fn handle_line(line: &str, data_root: &Path, initialized: &mut bool) -> Option<Value> {
    let message = match serde_json::from_str::<Value>(line) {
        Ok(message) => message,
        Err(err) => {
            return Some(error_response(
                Value::Null,
                -32700,
                "Parse error",
                Some(json!({ "error": err.to_string() })),
            ));
        }
    };
    handle_message(message, data_root, initialized)
}

fn handle_message(message: Value, data_root: &Path, initialized: &mut bool) -> Option<Value> {
    let Some(object) = message.as_object() else {
        return Some(error_response(Value::Null, -32600, "Invalid Request", None));
    };
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        let id = object.get("id").cloned().unwrap_or(Value::Null);
        return Some(error_response(id, -32600, "Invalid Request", None));
    }
    let id = message
        .as_object()
        .and_then(|object| object.get("id"))
        .cloned();
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return id.map(|id| error_response(id, -32600, "Invalid Request", None));
    };
    if matches!(id, Some(Value::Null | Value::Array(_) | Value::Object(_))) {
        return Some(error_response(Value::Null, -32600, "Invalid Request", None));
    }
    if id.is_none() {
        if method == "notifications/initialized" {
            *initialized = true;
        }
        return None;
    }
    let id = id?;
    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return Some(error_response(
            id,
            -32602,
            "Invalid params",
            Some(json!({ "error": "params must be an object" })),
        ));
    }
    if method != "initialize" && !*initialized {
        return Some(error_response(
            id,
            -32002,
            "Server not initialized",
            Some(json!({ "error": "send initialize before calling ctx MCP tools" })),
        ));
    }
    let result = match method {
        "initialize" => {
            *initialized = true;
            Ok(initialize_result())
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => handle_tools_call(params, data_root),
        _ => Err(json_rpc_error(-32601, "Method not found", None)),
    };
    Some(match result {
        Ok(result) => success_response(id, result),
        Err(error) => {
            if let Some(object) = error.as_object() {
                let code = object.get("code").and_then(Value::as_i64).unwrap_or(-32603);
                let message = object
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Internal error");
                let data = object.get("data").cloned();
                error_response(id, code, message, data)
            } else {
                error_response(id, -32603, "Internal error", Some(error))
            }
        }
    })
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": "ctx",
            "version": env!("CARGO_PKG_VERSION")
        },
        "instructions": "Read-only access to the local ctx index. Tool output is private local history and may include absolute paths, source metadata, snippets, transcript text, and raw SQL query results; MCP hosts may log or forward it. This minimal server supports initialize, ping, tools/list, and tools/call over newline-delimited stdio. It does not expose MCP resources or prompts, and tools do not import provider history, write provider files, or write repositories."
    })
}

fn handle_tools_call(params: Value, data_root: &Path) -> Result<Value, Value> {
    let name = params.get("name").and_then(Value::as_str).ok_or_else(|| {
        json_rpc_error(
            -32602,
            "Invalid params",
            Some(json!({ "error": "tools/call requires params.name" })),
        )
    })?;
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err(json_rpc_error(
            -32602,
            "Invalid params",
            Some(json!({ "error": "tools/call params.arguments must be an object" })),
        ));
    }

    let result = match name {
        "status" => {
            validate_argument_keys(&arguments, &[])?;
            tool_status(data_root)
        }
        "sources" => {
            validate_argument_keys(&arguments, &[])?;
            tool_sources(data_root)
        }
        "search" => {
            validate_argument_keys(
                &arguments,
                &[
                    "query",
                    "terms",
                    "limit",
                    "provider",
                    "history_source",
                    "provider_key",
                    "source_id",
                    "source_format",
                    "workspace",
                    "since",
                    "primary_only",
                    "include_subagents",
                    "event_type",
                    "roles",
                    "exclude_roles",
                    "exclude_tool_noise",
                    "exclude_tool_name",
                    "file",
                    "session",
                    "events",
                    "include_current_session",
                    "match",
                    "continue",
                    "fields",
                    "max_snippet_bytes",
                    "max_page_bytes",
                ],
            )?;
            tool_search(&arguments, data_root)
        }
        "sql" => {
            validate_argument_keys(
                &arguments,
                &[
                    "sql",
                    "max_rows",
                    "max_columns",
                    "max_value_bytes",
                    "max_sql_bytes",
                    "timeout_ms",
                ],
            )?;
            tool_sql(&arguments, data_root)
        }
        "show_session" => {
            validate_argument_keys(
                &arguments,
                &[
                    "ctx_session_id",
                    "mode",
                    "limit",
                    "continue",
                    "fields",
                    "max_event_bytes",
                    "max_page_bytes",
                ],
            )?;
            tool_show_session(&arguments, data_root)
        }
        "show_event" => {
            validate_argument_keys(&arguments, &["ctx_event_id", "before", "after", "window"])?;
            tool_show_event(&arguments, data_root)
        }
        _ => {
            return Err(json_rpc_error(
                -32602,
                "Invalid params",
                Some(json!({ "error": format!("unknown tool {name}") })),
            ))
        }
    };

    Ok(match result {
        Ok(value) => tool_result(value),
        Err(err) => tool_error_result(err),
    })
}

fn tool_status(data_root: &Path) -> Result<Value> {
    Ok(query_status_json(&query_status_snapshot(data_root, CONFIG_FILE).map_err(|_| {
        anyhow!("read-only store is unavailable; run `ctx setup` or `ctx import` to migrate writable storage if needed")
    })?))
}

fn tool_sources(data_root: &Path) -> Result<Value> {
    let sources = discovered_sources();
    let plugin_discovery = super::discover_history_source_plugins_with_diagnostics(data_root, &[])?;
    Ok(query_sources_json(
        &sources,
        &plugin_source_projections(&plugin_discovery.sources),
        &plugin_failure_projections(&plugin_discovery.failures),
        true,
    ))
}

fn tool_search(arguments: &Value, data_root: &Path) -> Result<Value> {
    let query_input = optional_string(arguments, "query")?;
    let query = query_input.clone().unwrap_or_default();
    let terms = optional_string_array(arguments, "terms")?.unwrap_or_default();
    let limit = optional_usize(arguments, "limit")?.unwrap_or(20);
    if !(1..=MAX_SEARCH_LIMIT).contains(&limit) {
        return Err(anyhow!("limit must be between 1 and {MAX_SEARCH_LIMIT}"));
    }
    let provider = optional_provider(arguments, "provider")?;
    let history_source = optional_string(arguments, "history_source")?;
    let provider_key = optional_string(arguments, "provider_key")?;
    let source_id = optional_string(arguments, "source_id")?;
    let source_format = optional_string(arguments, "source_format")?;
    let session = optional_string(arguments, "session")?;
    let workspace = optional_string(arguments, "workspace")?;
    let since = optional_string(arguments, "since")?;
    let primary_only = optional_bool(arguments, "primary_only")?.unwrap_or(false);
    let include_subagents = optional_bool(arguments, "include_subagents")?.unwrap_or(false);
    let event_type = optional_string(arguments, "event_type")?;
    let roles = optional_string_array(arguments, "roles")?.unwrap_or_default();
    let exclude_roles = optional_string_array(arguments, "exclude_roles")?.unwrap_or_default();
    let exclude_tool_noise = optional_bool(arguments, "exclude_tool_noise")?.unwrap_or(false);
    let exclude_tool_name = optional_string(arguments, "exclude_tool_name")?;
    let file = optional_string(arguments, "file")?.map(PathBuf::from);
    if !search_has_intent(SearchIntentInput {
        query: Some(&query),
        terms: &terms,
        file: file.as_deref(),
    }) {
        return Err(anyhow!("search needs a query or file"));
    }
    let store = open_existing_store(data_root)?;
    let events = optional_bool(arguments, "events")?.unwrap_or(false) || session.is_some();
    let include_current_session =
        optional_bool(arguments, "include_current_session")?.unwrap_or(false);
    let match_name = optional_string(arguments, "match")?;
    let match_name = match_name.as_deref().unwrap_or("all");
    let (match_mode, match_arg) = match match_name {
        "all" => (SearchMatchMode::All, SearchMatchArg::All),
        "any" => (SearchMatchMode::Any, SearchMatchArg::Any),
        "phrase" => (SearchMatchMode::Phrase, SearchMatchArg::Phrase),
        _ => return Err(anyhow!("match must be one of all, any, phrase")),
    };
    let (fields, fields_arg) =
        optional_fields(arguments, "fields")?.unwrap_or((FieldSet::Full, FieldArg::Full));
    let max_snippet_bytes =
        optional_usize(arguments, "max_snippet_bytes")?.unwrap_or(DEFAULT_ITEM_BYTES);
    let max_page_bytes = optional_usize(arguments, "max_page_bytes")?.unwrap_or(DEFAULT_PAGE_BYTES);
    let byte_policy = BytePolicy {
        per_item_bytes: max_snippet_bytes,
        page_bytes: max_page_bytes,
    }
    .validate()?;
    let continuation = optional_string(arguments, "continue")?;

    let options = ctx_history_search::PacketOptions {
        limit,
        filters: search_filters(
            SearchFilterInput {
                session: session.clone(),
                provider,
                source_identity: SourceIdentityFilterArgs {
                    history_source: history_source.clone(),
                    provider_key: provider_key.clone(),
                    source_id: source_id.clone(),
                    source_format: source_format.clone(),
                },
                workspace: workspace.clone(),
                since: since.clone(),
                primary_only,
                include_subagents,
                event_type: event_type.clone(),
                role: roles.clone(),
                exclude_role: exclude_roles.clone(),
                exclude_tool_noise,
                exclude_tool_name: exclude_tool_name.clone(),
                file: file.clone(),
                include_current_session,
            },
            Some(&store),
        )?,
        result_mode: if events {
            ctx_history_search::SearchResultMode::Events
        } else {
            ctx_history_search::SearchResultMode::Sessions
        },
        match_mode,
        ..ctx_history_search::PacketOptions::default()
    };
    let canonical_since = options.filters.since.map(|value| value.to_rfc3339());
    let page = QueryService::new(&store).search(
        &query,
        &terms,
        options,
        continuation.as_deref(),
        fields,
        byte_policy,
    )?;
    let cli_args = SearchArgs {
        query: query_input,
        term: terms,
        r#match: match_arg,
        limit,
        provider,
        history_source,
        provider_key,
        source_id,
        source_format,
        workspace,
        since: canonical_since,
        primary_only,
        include_subagents,
        event_type,
        role: roles,
        exclude_role: exclude_roles,
        exclude_tool_noise,
        exclude_tool_name,
        file,
        session,
        events: optional_bool(arguments, "events")?.unwrap_or(false),
        refresh: RefreshArg::Off,
        include_current_session,
        json: false,
        format: OutputFormat::Json,
        continuation,
        fields: fields_arg,
        max_snippet_bytes,
        max_page_bytes,
        verbose: false,
    };
    let next_argv = search_next_argv(
        &cli_args,
        OutputFormat::Json,
        page.pagination.continuation.as_deref(),
    );
    let next_command = next_argv.as_ref().map(|argv| command_from_argv(argv));
    let mut refresh = SearchRefreshReport::skipped(RefreshArg::Off, "skipped");
    refresh.reason = "refresh_off";
    let refresh = refresh.with_index_age(Some(&store));
    let next_arguments = page
        .pagination
        .continuation
        .as_deref()
        .map(|continuation| search_next_arguments(&cli_args, continuation));
    let mut value = search_page_json(&page, &refresh, Value::Null, next_command, next_argv)?;
    value
        .as_object_mut()
        .expect("search page is an object")
        .insert("next_arguments".to_owned(), json!(next_arguments));
    Ok(value)
}

fn tool_sql(arguments: &Value, data_root: &Path) -> Result<Value> {
    let store = open_existing_store(data_root)?;
    let sql = optional_string(arguments, "sql")?.ok_or_else(|| anyhow!("sql is required"))?;
    let max_rows = optional_usize(arguments, "max_rows")?.unwrap_or(RAW_SQL_DEFAULT_MAX_ROWS);
    let max_columns =
        optional_usize(arguments, "max_columns")?.unwrap_or(RAW_SQL_DEFAULT_MAX_COLUMNS);
    let max_value_bytes =
        optional_usize(arguments, "max_value_bytes")?.unwrap_or(RAW_SQL_DEFAULT_MAX_VALUE_BYTES);
    let max_sql_bytes =
        optional_usize(arguments, "max_sql_bytes")?.unwrap_or(RAW_SQL_DEFAULT_MAX_SQL_BYTES);
    let timeout_ms = optional_usize(arguments, "timeout_ms")?
        .map(|value| u64::try_from(value).map_err(|_| anyhow!("timeout_ms is too large")))
        .transpose()?
        .unwrap_or_else(|| duration_millis_u64(RAW_SQL_DEFAULT_TIMEOUT));
    let result = QueryService::new(&store).raw_sql(
        &sql,
        RawSqlOptions {
            max_rows,
            max_columns,
            max_value_bytes,
            max_sql_bytes,
            timeout: Duration::from_millis(timeout_ms),
        },
    )?;
    let mut value = raw_sql_result_json(&result);
    mark_share_safe(&mut value);
    Ok(value)
}

fn tool_show_session(arguments: &Value, data_root: &Path) -> Result<Value> {
    let store = open_existing_store(data_root)?;
    let session_id = resolve_session_id_arg(&store, arguments, "ctx_session_id")?;
    let mode = optional_transcript_mode(arguments, "mode")?.unwrap_or(TranscriptMode::Lite);
    let limit =
        optional_usize(arguments, "limit")?.unwrap_or(ctx_history_query::DEFAULT_SHOW_LIMIT);
    if !(1..=MAX_SHOW_LIMIT).contains(&limit) {
        return Err(anyhow!(
            "show_session limit must be between 1 and {MAX_SHOW_LIMIT}"
        ));
    }
    let continuation = optional_string(arguments, "continue")?;
    let (fields, fields_arg) =
        optional_fields(arguments, "fields")?.unwrap_or((FieldSet::Full, FieldArg::Full));
    let max_event_bytes =
        optional_usize(arguments, "max_event_bytes")?.unwrap_or(DEFAULT_ITEM_BYTES);
    let max_page_bytes = optional_usize(arguments, "max_page_bytes")?.unwrap_or(DEFAULT_PAGE_BYTES);
    let byte_policy = BytePolicy {
        per_item_bytes: max_event_bytes,
        page_bytes: max_page_bytes,
    }
    .validate()?;
    let session = store.get_session(session_id)?;
    let page = QueryService::new(&store).session_events(
        session,
        QueryTranscriptMode::from(mode),
        limit,
        continuation.as_deref(),
        fields,
        byte_policy,
    )?;
    let cli_args = ShowSessionArgs {
        id: Some(session_id.to_string()),
        provider: None,
        provider_session: None,
        mode,
        format: OutputFormat::Json,
        json: false,
        limit,
        continuation,
        fields: fields_arg,
        max_event_bytes,
        max_page_bytes,
        out: None,
    };
    let next_argv = show_session_next_argv(
        &cli_args,
        OutputFormat::Json,
        page.pagination.continuation.as_deref(),
    );
    let next_command = next_argv.as_ref().map(|argv| command_from_argv(argv));
    let next_arguments = page.pagination.continuation.as_deref().map(|continuation| {
        json!({
            "ctx_session_id": session_id,
            "mode": mode.as_str(),
            "limit": limit,
            "continue": continuation,
            "fields": fields_arg.as_str(),
            "max_event_bytes": max_event_bytes,
            "max_page_bytes": max_page_bytes,
        })
    });
    let mut value = event_page_json(&page, OutputFormat::Json, next_command, next_argv)?;
    value
        .as_object_mut()
        .expect("event page is an object")
        .insert("next_arguments".to_owned(), json!(next_arguments));
    Ok(value)
}

fn search_next_arguments(args: &SearchArgs, continuation: &str) -> Value {
    compact_json(json!({
        "query": args.query,
        "terms": args.term,
        "match": args.r#match.as_str(),
        "limit": args.limit,
        "provider": args.provider.map(ProviderArg::cli_name),
        "history_source": args.history_source,
        "provider_key": args.provider_key,
        "source_id": args.source_id,
        "source_format": args.source_format,
        "workspace": args.workspace,
        // Relative windows are frozen by tool_search before this projection.
        "since": args.since,
        "primary_only": args.primary_only,
        "include_subagents": args.include_subagents,
        "event_type": args.event_type,
        "roles": args.role,
        "exclude_roles": args.exclude_role,
        "exclude_tool_noise": args.exclude_tool_noise,
        "exclude_tool_name": args.exclude_tool_name,
        "file": args.file.as_ref().map(|path| path.to_string_lossy()),
        "session": args.session,
        "events": args.events,
        "include_current_session": args.include_current_session,
        "continue": continuation,
        "fields": args.fields.as_str(),
        "max_snippet_bytes": args.max_snippet_bytes,
        "max_page_bytes": args.max_page_bytes,
    }))
}

fn tool_show_event(arguments: &Value, data_root: &Path) -> Result<Value> {
    let store = open_existing_store(data_root)?;
    let event_id = resolve_event_id_arg(&store, arguments, "ctx_event_id")?;
    let before = optional_usize(arguments, "before")?.unwrap_or(0);
    let after = optional_usize(arguments, "after")?.unwrap_or(0);
    let window = optional_usize(arguments, "window")?;
    if before > MCP_MAX_EVENT_WINDOW
        || after > MCP_MAX_EVENT_WINDOW
        || window.is_some_and(|window| window > MCP_MAX_EVENT_WINDOW)
    {
        return Err(anyhow!(
            "show_event before/after/window must be {MCP_MAX_EVENT_WINDOW} or less"
        ));
    }
    let event = store.get_event(event_id)?;
    let events = event_window(&store, &event, before, after, window)?;
    Ok(event_window_json(
        &store,
        &event,
        &events,
        OutputFormat::Json,
    ))
}

fn open_existing_store(data_root: &Path) -> Result<Store> {
    let db_path = database_path(data_root.to_path_buf());
    if !db_path.exists() {
        return Err(anyhow!(
            "ctx store is not initialized at {}; run `ctx setup` or `ctx import` first",
            db_path.display()
        ));
    }
    Store::open_read_only(&db_path)
        .with_context(|| format!("open read-only ctx store {}", db_path.display()))
}

fn tool_result(structured: Value) -> Value {
    json!({
        "content": [
            {
                "type": "text",
                "text": "ctx returned structured JSON in structuredContent. Treat it as private local history.",
            }
        ],
        "structuredContent": structured,
    })
}

fn tool_error_result(err: anyhow::Error) -> Value {
    let error = format!("{err:#}");
    json!({
        "isError": true,
        "content": [
            {
                "type": "text",
                "text": error.clone(),
            }
        ],
        "structuredContent": {
            "error": error,
        }
    })
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "status",
            "title": "Status",
            "description": "Return local ctx index status without writing to provider history or repositories.",
            "inputSchema": object_schema(json!({}), vec![]),
            "annotations": { "readOnlyHint": true },
        }),
        json!({
            "name": "sources",
            "title": "Sources",
            "description": "List discovered local agent history sources.",
            "inputSchema": object_schema(json!({}), vec![]),
            "annotations": { "readOnlyHint": true },
        }),
        json!({
            "name": "search",
            "title": "Search",
            "description": "Search the existing local ctx index by query text or touched-file path. This does not refresh or import provider history.",
            "inputSchema": object_schema(json!({
                "query": { "type": "string", "maxLength": ctx_history_search::MAX_QUERY_CLAUSE_BYTES, "description": "Non-empty text query. Required unless file is provided." },
                "terms": { "type": "array", "maxItems": ctx_history_search::MAX_QUERY_CLAUSES, "items": { "type": "string", "maxLength": ctx_history_search::MAX_QUERY_CLAUSE_BYTES }, "default": [], "description": "Additional OR-style query clauses; order and duplicates are preserved. Aggregate query text is limited to 65536 UTF-8 bytes at runtime." },
                "match": { "type": "string", "enum": ["all", "any", "phrase"], "default": "all", "description": "Within-query word matching: all tokens in one indexed section, any token, or adjacent ordered phrase. Punctuation is normalized as separators and input is literal, not FTS syntax." },
                "limit": { "type": "integer", "minimum": 1, "maximum": MAX_SEARCH_LIMIT, "default": 20 },
                "continue": { "type": "string", "description": "Opaque continuation from a previous search response." },
                "fields": { "type": "string", "enum": ["full", "compact"], "default": "full" },
                "max_snippet_bytes": { "type": "integer", "minimum": 0, "maximum": MAX_ITEM_BYTES, "default": DEFAULT_ITEM_BYTES },
                "max_page_bytes": { "type": "integer", "minimum": 0, "maximum": MAX_PAGE_BYTES, "default": DEFAULT_PAGE_BYTES },
                "provider": { "type": "string", "enum": provider_names() },
                "history_source": { "type": "string", "description": "Custom history source selector as plugin/source or provider_key/source_id." },
                "provider_key": { "type": "string", "description": "Custom history provider_key." },
                "source_id": { "type": "string", "description": "Custom history source_id." },
                "source_format": { "type": "string", "description": "Custom history source_format." },
                "workspace": { "type": "string", "description": "Workspace path or name text." },
                "since": { "type": "string", "description": "RFC3339 timestamp or day window such as 30d." },
                "primary_only": { "type": "boolean", "default": false, "description": "Deprecated compatibility flag for the default primary-agent scope." },
                "include_subagents": { "type": "boolean", "default": false, "description": "Include subagent sessions in addition to primary-agent sessions." },
                "event_type": { "type": "string", "enum": event_type_names() },
                "roles": { "type": "array", "items": { "type": "string", "enum": ["user", "assistant", "tool"] }, "default": [] },
                "exclude_roles": { "type": "array", "items": { "type": "string", "enum": ["user", "assistant", "tool"] }, "default": [] },
                "exclude_tool_noise": { "type": "boolean", "default": false },
                "exclude_tool_name": { "type": "string" },
                "file": { "type": "string", "description": "Indexed touched-file path. Required unless query is provided." },
                "session": { "type": "string", "description": "ctx session UUID or unambiguous 8+ hex UUID prefix (compact or canonical-hyphenated)." },
                "events": { "type": "boolean", "default": false },
                "include_current_session": { "type": "boolean", "default": false, "description": "Include the active Codex session tree when CODEX_THREAD_ID is set." }
            }), vec![]),
            "annotations": { "readOnlyHint": true },
        }),
        json!({
            "name": "sql",
            "title": "SQL",
            "description": "Run one read-only SQL statement against the existing local ctx index. Prefer stable ctx_* views for scripts.",
            "inputSchema": object_schema(json!({
                "sql": { "type": "string", "description": "Single read-only SQL statement." },
                "max_rows": { "type": "integer", "minimum": 1, "maximum": RAW_SQL_MAX_ROWS_CAP, "default": RAW_SQL_DEFAULT_MAX_ROWS },
                "max_columns": { "type": "integer", "minimum": 1, "maximum": RAW_SQL_MAX_COLUMNS_CAP, "default": RAW_SQL_DEFAULT_MAX_COLUMNS },
                "max_value_bytes": { "type": "integer", "minimum": 1, "maximum": RAW_SQL_MAX_VALUE_BYTES_CAP, "default": RAW_SQL_DEFAULT_MAX_VALUE_BYTES },
                "max_sql_bytes": { "type": "integer", "minimum": 1, "maximum": RAW_SQL_MAX_SQL_BYTES_CAP, "default": RAW_SQL_DEFAULT_MAX_SQL_BYTES },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": duration_millis_u64(RAW_SQL_MAX_TIMEOUT),
                    "default": duration_millis_u64(RAW_SQL_DEFAULT_TIMEOUT)
                }
            }), vec!["sql"]),
            "annotations": { "readOnlyHint": true },
        }),
        json!({
            "name": "show_session",
            "title": "Show Session",
            "description": "Return an indexed session transcript by ctx session id.",
            "inputSchema": object_schema(json!({
                "ctx_session_id": { "type": "string", "description": "ctx session UUID or unambiguous 8+ hex UUID prefix (compact or canonical-hyphenated)." },
                "mode": { "type": "string", "enum": ["full", "lite", "log"], "default": "lite" },
                "limit": { "type": "integer", "minimum": 1, "maximum": MAX_SHOW_LIMIT, "default": ctx_history_query::DEFAULT_SHOW_LIMIT },
                "continue": { "type": "string", "description": "Opaque continuation from a previous show_session response." },
                "fields": { "type": "string", "enum": ["full", "compact"], "default": "full" },
                "max_event_bytes": { "type": "integer", "minimum": 0, "maximum": MAX_ITEM_BYTES, "default": DEFAULT_ITEM_BYTES },
                "max_page_bytes": { "type": "integer", "minimum": 0, "maximum": MAX_PAGE_BYTES, "default": DEFAULT_PAGE_BYTES }
            }), vec!["ctx_session_id"]),
            "annotations": { "readOnlyHint": true },
        }),
        json!({
            "name": "show_event",
            "title": "Show Event",
            "description": "Return an indexed event and optional surrounding event window by ctx event id.",
            "inputSchema": object_schema(json!({
                "ctx_event_id": { "type": "string", "description": "ctx event UUID or unambiguous 8+ hex UUID prefix (compact or canonical-hyphenated)." },
                "before": { "type": "integer", "minimum": 0, "default": 0 },
                "after": { "type": "integer", "minimum": 0, "default": 0 },
                "window": { "type": "integer", "minimum": 0 }
            }), vec!["ctx_event_id"]),
            "annotations": { "readOnlyHint": true },
        }),
    ]
}

fn object_schema(properties: Value, required: Vec<&str>) -> Value {
    compact_json(json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    }))
}

fn provider_names() -> Vec<&'static str> {
    let mut names = vec![
        ProviderArg::Codex.cli_name(),
        ProviderArg::Pi.cli_name(),
        ProviderArg::Claude.cli_name(),
        ProviderArg::OpenCode.cli_name(),
        ProviderArg::Antigravity.cli_name(),
        ProviderArg::Gemini.cli_name(),
        ProviderArg::Cursor.cli_name(),
        ProviderArg::CopilotCli.cli_name(),
        "copilot_cli",
        ProviderArg::FactoryAiDroid.cli_name(),
        "factory_ai_droid",
        ProviderArg::OpenClaw.cli_name(),
        ProviderArg::Hermes.cli_name(),
        ProviderArg::NanoClaw.cli_name(),
        ProviderArg::AstrBot.cli_name(),
        ProviderArg::Custom.cli_name(),
    ];
    names.sort_unstable();
    names
}

fn event_type_names() -> Vec<&'static str> {
    vec![
        EventType::Message.as_str(),
        EventType::ToolCall.as_str(),
        EventType::ToolOutput.as_str(),
        EventType::CommandStarted.as_str(),
        EventType::CommandOutput.as_str(),
        EventType::CommandFinished.as_str(),
        EventType::FileTouched.as_str(),
        EventType::VcsChange.as_str(),
        EventType::Artifact.as_str(),
        EventType::Summary.as_str(),
        EventType::Notice.as_str(),
    ]
}

fn optional_string(arguments: &Value, key: &str) -> Result<Option<String>> {
    match arguments.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(anyhow!("{key} must be a string")),
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn optional_bool(arguments: &Value, key: &str) -> Result<Option<bool>> {
    match arguments.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(anyhow!("{key} must be a boolean")),
    }
}

fn optional_usize(arguments: &Value, key: &str) -> Result<Option<usize>> {
    match arguments.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let value = value
                .as_u64()
                .ok_or_else(|| anyhow!("{key} must be a non-negative integer"))?;
            usize::try_from(value)
                .map(Some)
                .map_err(|_| anyhow!("{key} is too large"))
        }
        Some(_) => Err(anyhow!("{key} must be a non-negative integer")),
    }
}

fn optional_string_array(arguments: &Value, key: &str) -> Result<Option<Vec<String>>> {
    match arguments.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("{key} must contain only strings"))
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(_) => Err(anyhow!("{key} must be an array of strings")),
    }
}

fn optional_fields(arguments: &Value, key: &str) -> Result<Option<(FieldSet, FieldArg)>> {
    let Some(fields) = optional_string(arguments, key)? else {
        return Ok(None);
    };
    match fields.as_str() {
        "full" => Ok(Some((FieldSet::Full, FieldArg::Full))),
        "compact" => Ok(Some((FieldSet::Compact, FieldArg::Compact))),
        _ => Err(anyhow!("fields must be one of full, compact")),
    }
}

fn resolve_session_id_arg(store: &Store, arguments: &Value, key: &str) -> Result<Uuid> {
    let value = optional_string(arguments, key)?.ok_or_else(|| anyhow!("{key} is required"))?;
    let prefix = CtxIdPrefix::parse(&value).map_err(|err| anyhow!("session {err}"))?;
    if let Some(id) = prefix.full_uuid() {
        store.get_session(id)?;
        return Ok(id);
    }
    match store.resolve_session_by_id_prefix(&prefix)? {
        IdPrefixResolution::Found(session) => Ok(session.id),
        IdPrefixResolution::NotFound => Err(anyhow!(
            "session id prefix {:?} was not found",
            prefix.canonical()
        )),
        IdPrefixResolution::Ambiguous(ambiguity) => {
            Err(anyhow!(ambiguity.message("session", &prefix)))
        }
    }
}

fn resolve_event_id_arg(store: &Store, arguments: &Value, key: &str) -> Result<Uuid> {
    let value = optional_string(arguments, key)?.ok_or_else(|| anyhow!("{key} is required"))?;
    let prefix = CtxIdPrefix::parse(&value).map_err(|err| anyhow!("event {err}"))?;
    if let Some(id) = prefix.full_uuid() {
        store.get_event(id)?;
        return Ok(id);
    }
    match store.resolve_event_by_id_prefix(&prefix)? {
        IdPrefixResolution::Found(event) => Ok(event.id),
        IdPrefixResolution::NotFound => Err(anyhow!(
            "event id prefix {:?} was not found",
            prefix.canonical()
        )),
        IdPrefixResolution::Ambiguous(ambiguity) => {
            Err(anyhow!(ambiguity.message("event", &prefix)))
        }
    }
}

fn optional_provider(arguments: &Value, key: &str) -> Result<Option<ProviderArg>> {
    let Some(provider) = optional_string(arguments, key)? else {
        return Ok(None);
    };
    match provider.as_str() {
        "codex" => Ok(Some(ProviderArg::Codex)),
        "pi" => Ok(Some(ProviderArg::Pi)),
        "claude" => Ok(Some(ProviderArg::Claude)),
        "opencode" => Ok(Some(ProviderArg::OpenCode)),
        "antigravity" => Ok(Some(ProviderArg::Antigravity)),
        "gemini" => Ok(Some(ProviderArg::Gemini)),
        "cursor" => Ok(Some(ProviderArg::Cursor)),
        "copilot-cli" | "copilot_cli" => Ok(Some(ProviderArg::CopilotCli)),
        "factory-ai-droid" | "factory_ai_droid" => Ok(Some(ProviderArg::FactoryAiDroid)),
        "openclaw" => Ok(Some(ProviderArg::OpenClaw)),
        "hermes" => Ok(Some(ProviderArg::Hermes)),
        "nanoclaw" => Ok(Some(ProviderArg::NanoClaw)),
        "astrbot" => Ok(Some(ProviderArg::AstrBot)),
        "custom" => Ok(Some(ProviderArg::Custom)),
        _ => Err(anyhow!(
            "provider must be one of {}",
            provider_names().join(", ")
        )),
    }
}

fn validate_argument_keys(arguments: &Value, allowed: &[&str]) -> std::result::Result<(), Value> {
    let Some(object) = arguments.as_object() else {
        return Err(json_rpc_error(
            -32602,
            "Invalid params",
            Some(json!({ "error": "tools/call params.arguments must be an object" })),
        ));
    };
    if let Some(key) = object
        .keys()
        .find(|key| !allowed.iter().any(|allowed| allowed == &key.as_str()))
    {
        return Err(json_rpc_error(
            -32602,
            "Invalid params",
            Some(json!({ "error": format!("unknown argument {key}") })),
        ));
    }
    Ok(())
}

fn optional_transcript_mode(arguments: &Value, key: &str) -> Result<Option<TranscriptMode>> {
    let Some(mode) = optional_string(arguments, key)? else {
        return Ok(None);
    };
    match mode.as_str() {
        "full" => Ok(Some(TranscriptMode::Full)),
        "lite" => Ok(Some(TranscriptMode::Lite)),
        "log" => Ok(Some(TranscriptMode::Log)),
        _ => Err(anyhow!("mode must be one of full, lite, log")),
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn error_response(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    compact_json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
            "data": data,
        }
    }))
}

fn json_rpc_error(code: i64, message: &str, data: Option<Value>) -> Value {
    compact_json(json!({
        "code": code,
        "message": message,
        "data": data,
    }))
}
