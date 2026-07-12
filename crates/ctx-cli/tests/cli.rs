use assert_cmd::Command;
use predicates::prelude::*;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::{Builder, TempDir};

fn tempdir() -> TempDir {
    Builder::new().prefix("ctx-search-mvp-").tempdir().unwrap()
}

fn ctx(temp: &TempDir) -> Command {
    let mut command = Command::cargo_bin("ctx").unwrap();
    command.env("CTX_DATA_ROOT", temp.path());
    command.env("HOME", temp.path());
    command
}

fn provider_history_fixture(name: &str) -> String {
    materialized_fixture("provider-history", name)
}

fn custom_history_fixture(name: &str) -> String {
    materialized_fixture("custom-history-jsonl", name)
}

#[derive(Debug)]
struct HistorySourcePluginFixture {
    manifest_dir: PathBuf,
    run_marker: PathBuf,
}

fn write_history_source_plugin(
    temp: &TempDir,
    provider: &str,
    enabled: bool,
    cursor_log: Option<&Path>,
) -> HistorySourcePluginFixture {
    write_history_source_plugin_with_refresh(temp, provider, enabled, None, cursor_log)
}

fn write_history_source_plugin_with_refresh(
    temp: &TempDir,
    provider: &str,
    enabled: bool,
    refresh: Option<&str>,
    cursor_log: Option<&Path>,
) -> HistorySourcePluginFixture {
    write_history_source_plugin_at_with_refresh(
        &temp.path().join("history-plugins"),
        provider,
        enabled,
        refresh,
        cursor_log,
    )
}

fn write_history_source_plugin_at(
    root: &Path,
    provider: &str,
    enabled: bool,
    cursor_log: Option<&Path>,
) -> HistorySourcePluginFixture {
    write_history_source_plugin_at_with_refresh(root, provider, enabled, None, cursor_log)
}

fn write_history_source_plugin_at_with_refresh(
    root: &Path,
    provider: &str,
    enabled: bool,
    refresh: Option<&str>,
    cursor_log: Option<&Path>,
) -> HistorySourcePluginFixture {
    let manifest_dir = root.join(provider);
    fs::create_dir_all(&manifest_dir).unwrap();
    let script = manifest_dir.join("export.py");
    let run_marker = manifest_dir.join("ran");
    let run_marker_json = Value::String(run_marker.display().to_string());
    let cursor_log_py = cursor_log
        .map(|path| {
            serde_json::to_string(&path.display().to_string())
                .expect("cursor log path is JSON-serializable")
        })
        .unwrap_or_else(|| "None".to_owned());
    let script_body = format!(
        r#"#!/usr/bin/env python3
import json
import os
import pathlib
import sys

provider = sys.argv[1]
source_id = os.environ["CTX_HISTORY_SOURCE_ID"]
provider_key = os.environ["CTX_HISTORY_PROVIDER_KEY"]
source_format = os.environ["CTX_HISTORY_SOURCE_FORMAT"]
cursor_stream = os.environ["CTX_HISTORY_CURSOR_STREAM"]
cursor_inline = os.environ.get("CTX_HISTORY_CURSOR")
cursor_file = os.environ.get("CTX_HISTORY_CURSOR_FILE")
pathlib.Path({run_marker_json}).write_text("ran\n")
cursor_log = {cursor_log_py}
cursor_text = cursor_inline
if not cursor_text and cursor_file:
    cursor_text = pathlib.Path(cursor_file).read_text()
if cursor_log and cursor_text:
    file_text = pathlib.Path(cursor_file).read_text() if cursor_file else ""
    with open(cursor_log, "a", encoding="utf-8") as handle:
        handle.write(cursor_text + "\n")
        handle.write("cursor_file=" + file_text + "\n")

cursor_shapes = {{
    "dorkos": {{"files": {{"/tmp/dorkos.jsonl": {{"offset": 128, "size": 128, "mtimeMs": 1}}}}}},
    "disabled-dorkos": {{"files": {{"/tmp/disabled-dorkos.jsonl": {{"offset": 128, "size": 128, "mtimeMs": 1}}}}}},
    "openclaw": {{"backend": "openclaw-file", "transcripts": {{"/tmp/openclaw.jsonl": {{"offset": 256, "size": 256, "lastRecordId": "rec-1"}}}}}},
    "hermes": {{"message_id": 7}},
    "nanoclaw": {{"sessions": {{"sess-1": 42}}}},
}}
next_cursor = cursor_shapes[provider]
if cursor_text:
    if provider == "hermes":
        next_cursor = {{"message_id": 8}}
    elif provider == "nanoclaw":
        next_cursor = {{"sessions": {{"sess-1": 44}}}}
    elif provider == "openclaw":
        next_cursor = {{"backend": "openclaw-file", "transcripts": {{"/tmp/openclaw.jsonl": {{"offset": 512, "size": 512, "lastRecordId": "rec-2"}}}}}}
    else:
        next_cursor = {{"files": {{"/tmp/" + provider + ".jsonl": {{"offset": 256, "size": 256, "mtimeMs": 2}}}}}}

event_index = 1 if cursor_text else 0
phase = "incremental" if cursor_text else "initial"
observed = "2026-07-01T12:00:00Z"
cursor = {{
    "after": {{
        "stream": cursor_stream,
        "cursor": json.dumps(next_cursor, separators=(",", ":")),
        "observed_at": observed,
    }}
}}
if cursor_text:
    cursor["before"] = {{
        "stream": cursor_stream,
        "cursor": cursor_text,
        "observed_at": observed,
    }}

records = [
    {{"record_type": "manifest", "schema_version": "ctx-history-jsonl-v1", "producer": provider + "-fixture"}},
    {{"record_type": "source", "source_id": source_id, "provider_key": provider_key, "source_format": source_format, "observed_at": observed, "cursor": cursor, "metadata": {{"fixture_provider": provider}}}},
    {{"record_type": "session", "source_id": source_id, "session_id": provider + "-session", "started_at": "2026-07-01T11:59:00Z", "cwd": "/workspace/" + provider, "agent_type": "primary", "is_primary": True, "status": "completed"}},
    {{"record_type": "event", "source_id": source_id, "session_id": provider + "-session", "event_index": event_index, "event_id": provider + "-event-" + str(event_index), "native_cursor": phase, "event_type": "message", "role": "assistant", "occurred_at": observed, "payload": {{"text": provider + " plugin " + phase + " marker"}}, "preview": provider + " plugin " + phase + " marker"}},
]
for record in records:
    print(json.dumps(record, separators=(",", ":")))
"#,
        run_marker_json = run_marker_json,
        cursor_log_py = cursor_log_py
    );
    fs::write(&script, script_body).unwrap();
    let mut source_manifest = json!({
        "id": "default",
        "provider_key": provider,
        "source_id": "default",
        "source_format": format!("{provider}-history-v1"),
        "enabled": enabled,
        "command": [python_command(), script.display().to_string(), provider],
        "timeout_seconds": 10
    });
    if let Some(refresh) = refresh {
        source_manifest["refresh"] = json!(refresh);
    }
    let manifest = json!({
        "schema_version": 1,
        "name": provider,
        "display_name": format!("{provider} history"),
        "version": "0.1.0",
        "history_sources": [source_manifest]
    });
    fs::write(
        manifest_dir.join("ctx-history-plugin.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    HistorySourcePluginFixture {
        manifest_dir,
        run_marker,
    }
}

fn python_command() -> String {
    std::env::var("PYTHON").unwrap_or_else(|_| "python3".to_owned())
}

fn write_raw_history_source_plugin(
    temp: &TempDir,
    provider: &str,
    script_body: &str,
) -> HistorySourcePluginFixture {
    write_raw_history_source_plugin_with_options(temp, provider, script_body, false, None)
}

fn write_raw_history_source_plugin_with_options(
    temp: &TempDir,
    provider: &str,
    script_body: &str,
    enabled: bool,
    refresh: Option<&str>,
) -> HistorySourcePluginFixture {
    write_raw_history_source_plugin_with_options_and_timeout(
        temp,
        provider,
        script_body,
        enabled,
        refresh,
        10,
    )
}

fn write_raw_history_source_plugin_with_options_and_timeout(
    temp: &TempDir,
    provider: &str,
    script_body: &str,
    enabled: bool,
    refresh: Option<&str>,
    timeout_seconds: u64,
) -> HistorySourcePluginFixture {
    let manifest_dir = temp.path().join("history-plugins").join(provider);
    fs::create_dir_all(&manifest_dir).unwrap();
    let script = manifest_dir.join("export.py");
    let run_marker = manifest_dir.join("ran");
    fs::write(&script, script_body).unwrap();
    let mut source_manifest = json!({
        "id": "default",
        "provider_key": provider,
        "source_id": "default",
        "source_format": format!("{provider}-history-v1"),
        "enabled": enabled,
        "command": [python_command(), script.display().to_string()],
        "timeout_seconds": timeout_seconds
    });
    if let Some(refresh) = refresh {
        source_manifest["refresh"] = json!(refresh);
    }
    let manifest = json!({
        "schema_version": 1,
        "name": provider,
        "history_sources": [source_manifest]
    });
    fs::write(
        manifest_dir.join("ctx-history-plugin.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    HistorySourcePluginFixture {
        manifest_dir,
        run_marker,
    }
}

fn redaction_fixture(name: &str) -> String {
    materialized_fixture("redaction", name)
}

fn materialized_fixture(category: &str, name: &str) -> String {
    let source = match category {
        "provider-history" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/provider-history")
            .join(name),
        "custom-history-jsonl" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/custom-history-jsonl")
            .join(name),
        "provider" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/provider")
            .join(name),
        "redaction" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/redaction")
            .join(name),
        _ => panic!("unknown fixture category {category}"),
    };
    let materialized_root = std::env::current_dir()
        .unwrap()
        .join("target/test-data/materialized-fixtures");
    fs::create_dir_all(&materialized_root).unwrap();
    let unique = format!(
        "{}-{}-{}-{}",
        category,
        name.replace(['/', '\\', '.'], "_"),
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut target = materialized_root.join(unique);
    if source.is_file() {
        if let Some(extension) = source.extension() {
            target.set_extension(extension);
        }
    }
    if source.is_dir() {
        copy_dir_all(&source, &target);
    } else {
        fs::copy(&source, &target).unwrap();
    }
    target.to_str().unwrap().to_owned()
}

fn copy_dir_all(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let entry_path = entry.path();
        let target = to.join(entry.file_name());
        if entry_path.is_dir() {
            copy_dir_all(&entry_path, &target);
        } else {
            fs::copy(entry_path, target).unwrap();
        }
    }
}

fn json_output(command: &mut Command) -> Value {
    let output = command.assert().success().get_output().stdout.clone();
    serde_json::from_slice(&output).unwrap()
}

fn failure_stderr(command: &mut Command) -> String {
    let stderr = command.assert().failure().get_output().stderr.clone();
    String::from_utf8(stderr).unwrap()
}

fn mcp_roundtrip(temp: &TempDir, messages: &[Value]) -> Vec<Value> {
    mcp_roundtrip_with_env(temp, messages, &[])
}

fn mcp_roundtrip_with_env(temp: &TempDir, messages: &[Value], envs: &[(&str, &str)]) -> Vec<Value> {
    let mut stdin = String::new();
    for message in messages {
        stdin.push_str(&serde_json::to_string(message).unwrap());
        stdin.push('\n');
    }
    let mut command = ctx(temp);
    command.args(["mcp", "serve"]);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command
        .write_stdin(stdin)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn assert_omits_keys(value: &Value, forbidden_keys: &[&str]) {
    match value {
        Value::Object(map) => {
            for key in forbidden_keys {
                assert!(
                    !map.contains_key(*key),
                    "forbidden JSON key {key} appeared in {value:#}"
                );
            }
            for nested in map.values() {
                assert_omits_keys(nested, forbidden_keys);
            }
        }
        Value::Array(items) => {
            for item in items {
                assert_omits_keys(item, forbidden_keys);
            }
        }
        _ => {}
    }
}

fn assert_contains_markers(label: &str, value: &str, expected_markers: &[&str]) {
    for expected in expected_markers {
        assert!(
            value.contains(expected),
            "{label} did not preserve local marker {expected} in {value}"
        );
    }
}

fn local_cli_markers() -> &'static [&'static str] {
    &[
        "sk-fake00000000000000000000000000000000000000000000",
        "AKIAFAKE000000000000",
        "fake.jwt.token",
        "fake_password",
        "fake_secret_value",
        "fake-password-123",
        "fake_token@git.example.com",
        "person@example.invalid",
    ]
}

fn local_sqlite_markers() -> &'static [&'static str] {
    &[
        "sk-fake00000000000000000000000000000000000000000000",
        "ghp_fake000000000000000000000000000000000000",
        "AKIAFAKE000000000000",
        "fake.jwt.token",
        "fake_password",
        "fake_secret_value",
        "fake-password-123",
        "fake_token@git.example.com",
        "person@example.invalid",
    ]
}

fn sqlite_column_text(conn: &Connection, sql: &str) -> String {
    let mut statement = conn.prepare(sql).unwrap();
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap();
    let mut text = String::new();
    for row in rows {
        text.push_str(&row.unwrap());
        text.push('\n');
    }
    text
}

fn sqlite_count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn assert_search_provider_oracle(
    packet: &Value,
    provider: &str,
    query: &str,
    expected_results: usize,
    expected_match_reason: &str,
) {
    assert_search_provider_oracle_with_scope(
        packet,
        provider,
        query,
        expected_results,
        expected_match_reason,
        "session_result",
        "session",
    );
}

fn assert_event_search_provider_oracle(
    packet: &Value,
    provider: &str,
    query: &str,
    expected_results: usize,
    expected_match_reason: &str,
) {
    assert_search_provider_oracle_with_scope(
        packet,
        provider,
        query,
        expected_results,
        expected_match_reason,
        "event",
        "event",
    );
}

fn assert_search_provider_oracle_with_scope(
    packet: &Value,
    provider: &str,
    query: &str,
    expected_results: usize,
    expected_match_reason: &str,
    expected_item_type: &str,
    expected_scope: &str,
) {
    assert_eq!(packet["schema_version"], 1);
    assert_eq!(packet["query"], query);
    assert_eq!(packet["filters"]["provider"], provider);
    let results = packet["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        expected_results,
        "unexpected search result count in {packet:#}"
    );

    for result in results {
        assert_eq!(result["provider"], provider, "provider filter failed");
        assert_eq!(result["source_exists"], true, "source_exists failed");
        assert_eq!(result["item_type"], expected_item_type);
        assert_eq!(result["result_scope"], expected_scope);
        assert!(result["ctx_event_id"].is_string());
        assert!(result["ctx_session_id"].is_string());
        assert!(result["provider_session_id"].is_string());
        assert!(result["source_path"].is_string());
        assert!(result["cursor"].is_string());
        if expected_scope == "session" {
            assert!(result["session_importance"].is_number());
            assert!(result["more_matches_in_session"].is_number());
            assert_session_suggested_next_commands(result);
        } else {
            assert_eq!(result.get("session_importance"), None);
            assert_eq!(result.get("more_matches_in_session"), None);
            assert_event_suggested_next_commands(result);
        }
        assert!(result["why_matched"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == expected_match_reason));
        assert_provider_citations(result, provider);
    }
}

fn assert_provider_citations(result: &Value, provider: &str) {
    let citations = result["citations"].as_array().unwrap();
    assert!(!citations.is_empty(), "missing citations in {result:#}");
    for citation in citations {
        assert!(
            citation["ctx_event_id"].is_string() || citation["ctx_session_id"].is_string(),
            "citation needs a ctx-owned event or session id in {citation:#}"
        );
        assert_eq!(citation["provider"], provider, "citation provider failed");
        assert_eq!(
            citation["source_exists"], true,
            "citation source_exists failed"
        );
        assert!(citation["source_path"].is_string());
        assert!(citation["cursor"].is_string());
    }
}

fn assert_session_suggested_next_commands(result: &Value) {
    let commands = result["suggested_next_commands"].as_array().unwrap();
    assert!(
        commands
            .iter()
            .all(|command| !command.as_str().unwrap_or("").contains("--mode lite")),
        "lite default should not be restated in suggestions: {result:#}"
    );
    assert!(
        commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx show session ")),
        "missing show session suggestion in {result:#}"
    );
    assert!(
        commands.iter().any(|command| {
            let command = command.as_str().unwrap_or("");
            command.starts_with("ctx search ") && command.contains(" --session ")
        }),
        "missing session event drilldown suggestion in {result:#}"
    );
    assert!(
        commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx locate session ")),
        "missing locate session suggestion in {result:#}"
    );
}

fn assert_event_suggested_next_commands(result: &Value) {
    let commands = result["suggested_next_commands"].as_array().unwrap();
    assert!(
        commands
            .iter()
            .all(|command| !command.as_str().unwrap_or("").contains("--mode lite")),
        "lite default should not be restated in suggestions: {result:#}"
    );
    assert!(
        commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx show event ")),
        "missing show event suggestion in {result:#}"
    );
    assert!(
        commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx show session ")),
        "missing show session suggestion in {result:#}"
    );
    assert!(
        !commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx export session ")),
        "search should not suggest exporting transcripts by default in {result:#}"
    );
    assert!(
        commands.iter().any(|command| command
            .as_str()
            .unwrap_or("")
            .starts_with("ctx locate event ")),
        "missing locate event suggestion in {result:#}"
    );
}

#[test]
fn help_exposes_session_retrieval_commands() {
    let temp = tempdir();
    let output = ctx(&temp)
        .arg("--help")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let help = String::from_utf8(output).unwrap();
    let commands = help
        .split("Commands:")
        .nth(1)
        .and_then(|tail| tail.split("Options:").next())
        .unwrap_or(&help);

    for expected in [
        "setup", "status", "sources", "import", "show", "search", "docs", "locate", "mcp", "sql",
        "doctor",
    ] {
        assert!(
            commands.contains(expected),
            "missing command {expected} in\n{help}"
        );
    }
    for forbidden in [
        "dashboard",
        "shim",
        "evidence",
        "publish",
        "link-pr",
        "record",
        "research",
        "list",
        "export",
        "validate",
        "report",
        "schema",
        "workspace",
        "work",
        "service",
        "capture",
        "vcs",
        "pr",
        "repair",
        "watch",
        "context",
        "update",
        "uninstall",
        "upgrade",
        "analytics",
    ] {
        assert!(
            !commands.contains(&format!("  {forbidden}")),
            "forbidden command {forbidden} appeared in\n{help}"
        );
    }
}

#[test]
fn root_version_reports_package_version() {
    let temp = tempdir();
    ctx(&temp)
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn removed_commands_are_rejected() {
    let temp = tempdir();
    for command in [
        "dashboard",
        "shim",
        "evidence",
        "publish",
        "link-pr",
        "record",
        "report",
        "schema",
        "workspace",
        "work",
        "service",
        "capture",
        "vcs",
        "pr",
        "repair",
        "watch",
        "context",
        "update",
        "uninstall",
        "upgrade",
        "analytics",
    ] {
        ctx(&temp)
            .arg(command)
            .assert()
            .failure()
            .stderr(predicate::str::contains("unrecognized subcommand"));
    }
}

#[test]
fn setup_does_not_migrate_legacy_shim_directory() {
    let temp = tempdir();
    let legacy_shims = temp.path().join("legacy-history").join("shims");
    fs::create_dir_all(&legacy_shims).unwrap();
    fs::write(legacy_shims.join("git"), "#!/bin/sh\n").unwrap();

    ctx(&temp).arg("setup").assert().success();

    assert!(
        !temp.path().join("shims").exists(),
        "setup must not create or migrate shim directories"
    );
    assert!(
        legacy_shims.join("git").exists(),
        "legacy shim files should be left in place instead of installed"
    );
}

#[test]
fn setup_writes_day_one_config_contract_without_overwriting_existing_config() {
    let temp = tempdir();
    let config_path = temp.path().join("config.toml");

    ctx(&temp).arg("setup").assert().success();
    let default_config = fs::read_to_string(&config_path).unwrap();
    assert!(default_config.contains("# ctx configuration"));
    assert!(!default_config.contains("[upgrade]"));
    assert!(!default_config.contains("[analytics]"));

    let user_config = "# user managed ctx config\n";
    fs::write(&config_path, user_config).unwrap();

    ctx(&temp).arg("setup").assert().success();
    assert_eq!(
        fs::read_to_string(&config_path).unwrap(),
        user_config,
        "setup must not overwrite an existing user config"
    );
}

#[test]
fn setup_catalog_only_catalogs_codex_sessions_without_import() {
    let temp = tempdir();
    let sessions = temp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026/06/24");
    fs::create_dir_all(&sessions).unwrap();
    fs::write(
        sessions.join("rollout-2026-06-24T10-00-00-codex-session-setup.jsonl"),
        r#"{"timestamp":"2026-06-24T10:00:00.000Z","type":"session_meta","payload":{"id":"codex-session-setup","timestamp":"2026-06-24T10:00:00.000Z","cwd":"/repo/app","originator":"codex-cli","cli_version":"0.200.0","source":"cli","model_provider":"openai"}}"#,
    )
    .unwrap();

    let setup = json_output(ctx(&temp).args(["setup", "--catalog-only", "--json"]));
    assert_eq!(setup["catalog"]["cataloged_sessions"], 1);
    assert_eq!(setup["catalog"]["source_files"], 1);
    assert_eq!(setup["catalog"]["failed_sessions"], 0);
    assert_eq!(setup["import"]["ran"], false);

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["cataloged_sessions"], 1);
    assert_eq!(status["indexed_catalog_sessions"], 0);
    assert_eq!(status["indexed_items"], 0);

    let human_setup = ctx(&temp)
        .args(["setup", "--catalog-only", "--progress", "none"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let human_setup = String::from_utf8(human_setup).unwrap();
    assert!(human_setup.contains("ctx catalog is ready; import is still pending"));
    assert!(human_setup.contains("  ctx import --all"));
    assert!(!human_setup.contains("ctx search \"what failed before\""));
}

#[test]
fn setup_imports_discovered_codex_sessions_by_default() {
    let temp = tempdir();
    let sessions = temp
        .path()
        .join(".codex")
        .join("sessions")
        .join("2026/06/24");
    fs::create_dir_all(&sessions).unwrap();
    fs::write(
        sessions.join("rollout-2026-06-24T10-00-00-codex-session-setup.jsonl"),
        concat!(
            r#"{"timestamp":"2026-06-24T10:00:00.000Z","type":"session_meta","payload":{"id":"codex-session-setup","timestamp":"2026-06-24T10:00:00.000Z","cwd":"/repo/app","originator":"codex-cli","cli_version":"0.200.0","source":"cli","model_provider":"openai"}}"#,
            "\n",
            r#"{"timestamp":"2026-06-24T10:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"setup should import"}]}}"#,
            "\n"
        ),
    )
    .unwrap();

    let setup = json_output(ctx(&temp).args(["setup", "--json", "--progress", "none"]));
    assert_eq!(setup["catalog"]["cataloged_sessions"], 1);
    assert_eq!(setup["import"]["ran"], true);
    assert_eq!(setup["import"]["totals"]["failed_sources"], 0);
    assert_eq!(setup["import"]["totals"]["imported_sessions"], 1);
    assert!(
        setup["import"]["totals"]["imported_events"]
            .as_u64()
            .unwrap()
            >= 1
    );

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["cataloged_sessions"], 1);
    assert_eq!(status["indexed_catalog_sessions"], 1);
    assert_eq!(status["pending_catalog_sessions"], 0);
    assert!(status["indexed_items"].as_u64().unwrap() > 0);

    let human_setup = ctx(&temp)
        .args(["setup", "--progress", "none"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let human_setup = String::from_utf8(human_setup).unwrap();
    assert!(human_setup.contains("ctx local agent history search is ready"));
    assert!(human_setup.contains("imported_sources: 1"));
    assert!(human_setup.contains("  ctx search \"what failed before\""));
}

#[test]
fn setup_skips_empty_codex_session_tree() {
    let temp = tempdir();
    fs::create_dir_all(temp.path().join(".codex").join("sessions")).unwrap();

    let setup = json_output(ctx(&temp).args(["setup", "--json", "--progress", "none"]));
    assert_eq!(setup["catalog"]["cataloged_sessions"], 0);
    assert_eq!(setup["catalog"]["source_files"], 0);
    assert_eq!(setup["import"]["totals"]["imported_sources"], 0);

    let sources = json_output(ctx(&temp).args(["sources", "--json"]));
    let codex_sessions = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| {
            source["provider"] == "codex" && source["source_format"] == "codex_session_jsonl_tree"
        })
        .unwrap();
    assert_eq!(codex_sessions["status"], "empty");
    assert_eq!(codex_sessions["importable"], false);
}

#[test]
fn import_progress_json_goes_to_stderr_without_polluting_stdout() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    let output = ctx(&temp)
        .args([
            "import",
            "--provider",
            "codex",
            "--path",
            &fixture,
            "--json",
            "--progress",
            "json",
        ])
        .assert()
        .success()
        .get_output()
        .clone();

    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["schema_version"], 1);
    assert!(stdout["totals"]["imported_sessions"].as_u64().unwrap() > 0);

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(r#""type":"ctx_progress""#), "{stderr}");
    assert!(stderr.contains(r#""operation":"import""#), "{stderr}");
}

#[test]
fn import_custom_history_jsonl_format_is_searchable_and_idempotent() {
    let temp = tempdir();
    let fixture = custom_history_fixture("basic.jsonl");

    let first = json_output(ctx(&temp).args([
        "import",
        "--format",
        "ctx-history-jsonl-v1",
        "--path",
        &fixture,
        "--json",
        "--progress",
        "none",
    ]));
    assert_eq!(first["totals"]["imported_sessions"], 2);
    assert_eq!(first["totals"]["imported_events"], 2);
    assert_eq!(first["totals"]["imported_edges"], 2);
    assert_eq!(first["sources"][0]["provider"], "custom");
    assert_eq!(first["sources"][0]["format"], "ctx-history-jsonl-v1");

    let search = json_output(ctx(&temp).args([
        "search",
        "parser test",
        "--provider",
        "custom",
        "--refresh",
        "off",
        "--json",
    ]));
    assert!(
        !search["results"].as_array().unwrap().is_empty(),
        "custom import was not searchable: {search:#}"
    );

    let second = json_output(ctx(&temp).args([
        "import",
        "--format",
        "ctx-history-jsonl-v1",
        "--path",
        &fixture,
        "--json",
        "--progress",
        "none",
    ]));
    assert_eq!(second["totals"]["imported_sessions"], 0);
    assert_eq!(second["totals"]["imported_events"], 0);
    assert_eq!(second["totals"]["imported_edges"], 0);
    assert_eq!(second["totals"]["skipped"], 6);
}

#[test]
fn search_refresh_off_reports_additive_freshness_without_plugins_or_writes() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    let plugin = write_history_source_plugin(&temp, "noexec", true, None);
    let db_path = temp.path().join("work.sqlite");
    let before = fs::metadata(&db_path).unwrap().modified().unwrap();

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["search", "onboarding", "--refresh", "off", "--json"]),
    );

    assert_eq!(search["schema_version"], 1);
    assert_eq!(search["freshness"]["mode"], "off");
    assert_eq!(search["freshness"]["status"], "skipped");
    assert_eq!(search["freshness"]["ran"], false);
    assert_eq!(search["freshness"]["reason"], "refresh_off");
    assert_eq!(search["freshness"]["duration_ms"], 0);
    assert!(search["freshness"]["index_age_seconds"].is_number());
    assert_eq!(search["freshness"]["totals"]["unchanged_sources"], 0);
    assert_eq!(search["freshness"]["totals"]["imported_events"], 0);
    assert!(!search["results"].as_array().unwrap().is_empty());
    assert!(!plugin.run_marker.exists());
    assert_eq!(fs::metadata(&db_path).unwrap().modified().unwrap(), before);

    let stdout = ctx(&temp)
        .args(["search", "onboarding", "--refresh", "off"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8(stdout).unwrap();
    assert!(stdout.contains("freshness: refresh skipped"), "{stdout}");
    assert!(stdout.contains("unchanged 0"), "{stdout}");
}

#[test]
fn search_freshness_reports_persisted_index_age() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    let fresh =
        json_output(ctx(&temp).args(["search", "onboarding", "--refresh", "off", "--json"]));
    assert!(fresh["freshness"]["index_age_seconds"].as_i64().unwrap() >= 0);

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    conn.execute("UPDATE sessions SET updated_at_ms = 1000", [])
        .unwrap();
    conn.execute("UPDATE history_records SET updated_at_ms = 1000", [])
        .unwrap();
    let stale =
        json_output(ctx(&temp).args(["search", "onboarding", "--refresh", "off", "--json"]));
    assert!(stale["freshness"]["index_age_seconds"].as_i64().unwrap() > 1_000_000);
}

#[test]
fn import_custom_history_jsonl_format_rejects_malformed_atomically() {
    let temp = tempdir();
    let fixture = custom_history_fixture("malformed-partial.jsonl");

    let stderr = failure_stderr(ctx(&temp).args([
        "import",
        "--format",
        "ctx-history-jsonl-v1",
        "--path",
        &fixture,
        "--progress",
        "none",
    ]));
    assert!(
        stderr.contains("ctx-history-jsonl-v1 import failed"),
        "{stderr}"
    );

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["indexed_items"], 0);
    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM history_records"),
        0
    );
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM ctx_history_search"),
        0
    );
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM capture_sources"),
        0
    );
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM sessions"), 0);
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM events"), 0);
}

#[test]
fn import_custom_history_format_is_not_a_native_provider_importer() {
    let temp = tempdir();
    let stderr = failure_stderr(ctx(&temp).args(["import", "--provider", "custom"]));
    assert!(stderr.contains("invalid value 'custom'"), "{stderr}");

    let fixture = custom_history_fixture("basic.jsonl");
    let stderr = failure_stderr(ctx(&temp).args([
        "import",
        "--format",
        "ctx-history-jsonl-v1",
        "--path",
        &fixture,
        "--all",
    ]));
    assert!(stderr.contains("--format"), "{stderr}");
    assert!(stderr.contains("--all"), "{stderr}");
}

#[test]
fn history_source_plugins_are_listed_without_running() {
    let temp = tempdir();
    let plugin = write_history_source_plugin(&temp, "dorkos", false, None);

    let sources = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["sources", "--json"]),
    );
    let plugin_source = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["history_source"] == "dorkos/default")
        .unwrap();
    assert_eq!(plugin_source["kind"], "history_source_plugin");
    assert_eq!(plugin_source["provider_key"], "dorkos");
    assert_eq!(plugin_source["enabled"], false);
    assert!(!plugin.run_marker.exists());
}

#[test]
fn invalid_installed_history_source_plugin_is_listed_as_invalid() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let bad_dir = plugin_root.join("bad");
    fs::create_dir_all(&bad_dir).unwrap();
    fs::write(bad_dir.join("ctx-history-plugin.json"), "{not-json").unwrap();

    let sources = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args(["sources", "--json"]),
    );
    let invalid = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["kind"] == "history_source_plugin" && source["status"] == "invalid")
        .unwrap();
    assert_eq!(invalid["importable"], false);
    assert_eq!(invalid["enabled"], false);
    assert!(invalid["error"]
        .as_str()
        .unwrap()
        .contains("parse history source plugin manifest"));
}

#[test]
fn invalid_installed_history_source_plugin_does_not_block_valid_import() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let good = write_history_source_plugin_at(&plugin_root, "dorkos", false, None);
    let bad_dir = plugin_root.join("bad");
    fs::create_dir_all(&bad_dir).unwrap();
    fs::write(bad_dir.join("ctx-history-plugin.json"), "{not-json").unwrap();

    let imported = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args([
                "import",
                "--history-source",
                "dorkos/default",
                "--json",
                "--progress",
                "none",
            ]),
    );

    assert_eq!(imported["totals"]["imported_sources"], 1);
    assert!(good.run_marker.exists());
}

#[test]
fn removed_history_source_plugin_aliases_and_legacy_discovery_are_ignored() {
    let temp = tempdir();
    let plugin = write_history_source_plugin(&temp, "dorkos", false, None);

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["import", "--plugin", "dorkos/default"]),
    );
    assert!(stderr.contains("--plugin"), "{stderr}");

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["import", "--plugin-manifest", "ctx-history-plugin.json"]),
    );
    assert!(stderr.contains("--plugin-manifest"), "{stderr}");

    let sources = json_output(
        ctx(&temp)
            .env_remove("CTX_HISTORY_PLUGIN_PATH")
            .env("CTX_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["sources", "--json"]),
    );
    assert!(!sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|source| source["history_source"] == "dorkos/default"));

    let legacy_dir = temp.path().join("legacy-plugin");
    fs::create_dir_all(&legacy_dir).unwrap();
    fs::copy(
        plugin.manifest_dir.join("ctx-history-plugin.json"),
        legacy_dir.join("plugin.json"),
    )
    .unwrap();
    let sources = json_output(
        ctx(&temp)
            .env_remove("CTX_PLUGIN_PATH")
            .env("CTX_HISTORY_PLUGIN_PATH", &legacy_dir)
            .args(["sources", "--json"]),
    );
    assert!(!sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|source| source["history_source"] == "dorkos/default"));
}

#[test]
fn setup_does_not_execute_enabled_history_source_plugins() {
    let temp = tempdir();
    let plugin = write_history_source_plugin(&temp, "dorkos", true, None);

    json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["setup", "--json", "--progress", "none"]),
    );

    assert!(!plugin.run_marker.exists());
}

#[test]
fn bare_history_source_plugin_selector_fails_before_execution() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let dorkos = write_history_source_plugin_at(&plugin_root, "dorkos", false, None);
    let hermes = write_history_source_plugin_at(&plugin_root, "hermes", false, None);

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args(["import", "--history-source", "dorkos", "--progress", "none"]),
    );

    assert!(
        stderr.contains("no history source plugin matched"),
        "{stderr}"
    );
    assert!(!dorkos.run_marker.exists());
    assert!(!hermes.run_marker.exists());
}

#[test]
fn explicit_history_source_manifest_reports_parse_errors() {
    let temp = tempdir();
    let bad_manifest = temp.path().join("bad-plugin.json");
    fs::write(&bad_manifest, "{not-json").unwrap();

    let stderr = failure_stderr(ctx(&temp).args([
        "import",
        "--history-source-manifest",
        bad_manifest.to_str().unwrap(),
        "--progress",
        "none",
    ]));

    assert!(
        stderr.contains("parse history source plugin manifest"),
        "{stderr}"
    );
}

#[test]
fn failed_history_source_plugin_import_does_not_leave_record_metadata() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import json
provider = "badplugin"
records = [
  {"record_type":"manifest","schema_version":"ctx-history-jsonl-v1"},
  {"record_type":"source","source_id":"default","provider_key":provider,"source_format":"badplugin-history-v1"},
  {"record_type":"event","source_id":"default","session_id":"missing","event_index":0,"event_type":"message","role":"assistant","occurred_at":"2026-07-01T12:00:00Z","preview":"should not import"}
]
for record in records:
    print(json.dumps(record))
"#;
    let plugin = write_raw_history_source_plugin(&temp, "badplugin", script);

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "badplugin/default",
                "--progress",
                "none",
            ]),
    );

    assert!(stderr.contains("import failed"), "{stderr}");
    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM history_records"),
        0
    );
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM sessions"), 0);
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM events"), 0);
}

#[test]
fn history_source_plugin_rejects_mismatched_machine_id_before_import() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import json
records = [
  {"record_type":"manifest","schema_version":"ctx-history-jsonl-v1"},
  {"record_type":"source","source_id":"default","provider_key":"machineplugin","source_format":"machineplugin-history-v1","machine_id":"other-machine"},
  {"record_type":"session","source_id":"default","session_id":"run","started_at":"2026-07-01T12:00:00Z"},
]
for record in records:
    print(json.dumps(record))
"#;
    let plugin = write_raw_history_source_plugin(&temp, "machineplugin", script);

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "machineplugin/default",
                "--progress",
                "none",
            ]),
    );

    assert!(stderr.contains("machine_id"), "{stderr}");
    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM history_records"),
        0
    );
}

#[test]
fn history_source_plugin_reset_requires_fresh_after_cursor() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import json
records = [
  {"record_type":"manifest","schema_version":"ctx-history-jsonl-v1"},
  {"record_type":"source","source_id":"default","provider_key":"nocursor","source_format":"nocursor-history-v1"},
  {"record_type":"session","source_id":"default","session_id":"run","started_at":"2026-07-01T12:00:00Z"},
]
for record in records:
    print(json.dumps(record))
"#;
    let plugin = write_raw_history_source_plugin(&temp, "nocursor", script);

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "nocursor/default",
                "--reset-cursor",
                "--progress",
                "none",
            ]),
    );

    assert!(stderr.contains("source.cursor.after"), "{stderr}");
    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(&conn, "SELECT COUNT(*) FROM history_records"),
        0
    );
}

#[test]
fn large_history_source_plugin_cursor_uses_cursor_file_without_inline_env() {
    let temp = tempdir();
    let log = temp.path().join("large-cursor.log");
    let log_json = serde_json::to_string(&log.display().to_string()).unwrap();
    let script = format!(
        r#"#!/usr/bin/env python3
import json
import os
import pathlib

cursor_file = os.environ.get("CTX_HISTORY_CURSOR_FILE")
inline = os.environ.get("CTX_HISTORY_CURSOR")
cursor_text = pathlib.Path(cursor_file).read_text() if cursor_file else inline
if cursor_text:
    with open({log_json}, "a", encoding="utf-8") as handle:
        handle.write("inline=" + ("1" if inline else "0") + "\n")
        handle.write("file_len=" + str(len(cursor_text)) + "\n")
next_cursor = "x" * 9000 if not cursor_text else "done"
observed = "2026-07-01T12:00:00Z"
records = [
  {{"record_type":"manifest","schema_version":"ctx-history-jsonl-v1"}},
  {{"record_type":"source","source_id":"default","provider_key":"largecursor","source_format":"largecursor-history-v1","cursor":{{"after":{{"stream":os.environ["CTX_HISTORY_CURSOR_STREAM"],"cursor":next_cursor,"observed_at":observed}}}}}},
  {{"record_type":"session","source_id":"default","session_id":"run","started_at":"2026-07-01T12:00:00Z"}},
  {{"record_type":"event","source_id":"default","session_id":"run","event_index":1 if cursor_text else 0,"event_type":"message","role":"assistant","occurred_at":observed,"preview":"large cursor marker"}},
]
for record in records:
    print(json.dumps(record))
"#
    );
    let plugin = write_raw_history_source_plugin(&temp, "largecursor", &script);

    json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "largecursor/default",
                "--json",
                "--progress",
                "none",
            ]),
    );
    json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "largecursor/default",
                "--json",
                "--progress",
                "none",
            ]),
    );

    let log = fs::read_to_string(log).unwrap();
    assert!(log.contains("inline=0"), "{log}");
    assert!(log.contains("file_len=9000"), "{log}");
}

#[test]
fn import_history_source_plugin_is_searchable_and_receives_cursor() {
    let temp = tempdir();
    let cursor_log = temp.path().join("cursor-log.txt");
    let plugin = write_history_source_plugin(&temp, "hermes", false, Some(&cursor_log));

    let first = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "hermes/default",
                "--resume",
                "--json",
                "--progress",
                "none",
            ]),
    );
    assert_eq!(first["totals"]["imported_sessions"], 1);
    assert_eq!(first["totals"]["imported_events"], 1);
    assert_eq!(first["sources"][0]["history_source"], "hermes/default");

    let initial = json_output(ctx(&temp).args([
        "search",
        "hermes plugin initial marker",
        "--provider",
        "custom",
        "--refresh",
        "off",
        "--json",
    ]));
    assert!(
        !initial["results"].as_array().unwrap().is_empty(),
        "initial plugin import was not searchable: {initial:#}"
    );
    let initial_by_history_source = json_output(ctx(&temp).args([
        "search",
        "hermes plugin initial marker",
        "--history-source",
        "hermes/default",
        "--refresh",
        "off",
        "--json",
    ]));
    let source_filtered_result = &initial_by_history_source["results"][0];
    assert_eq!(source_filtered_result["provider"], "custom");
    assert_eq!(source_filtered_result["history_source"], "hermes/default");
    assert_eq!(source_filtered_result["history_source_plugin"], "hermes");
    assert_eq!(source_filtered_result["provider_key"], "hermes");
    assert_eq!(source_filtered_result["source_id"], "default");
    assert_eq!(source_filtered_result["source_format"], "hermes-history-v1");

    let second = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "hermes/default",
                "--json",
                "--progress",
                "none",
            ]),
    );
    assert_eq!(second["totals"]["imported_sessions"], 0);
    assert_eq!(second["totals"]["imported_events"], 1);
    assert_eq!(second["resume"], false);
    assert_eq!(second["resume_mode"], "normal_scan");

    let incremental = json_output(ctx(&temp).args([
        "search",
        "hermes plugin incremental marker",
        "--provider",
        "custom",
        "--refresh",
        "off",
        "--json",
    ]));
    assert!(
        !incremental["results"].as_array().unwrap().is_empty(),
        "incremental plugin import was not searchable: {incremental:#}"
    );
    let cursor_log = fs::read_to_string(cursor_log).unwrap();
    assert!(cursor_log.contains(r#""message_id":7"#), "{cursor_log}");
    assert!(cursor_log.contains("cursor_file="), "{cursor_log}");
}

#[test]
fn import_all_runs_enabled_history_source_plugins_for_external_shapes() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let providers = ["dorkos", "openclaw", "hermes", "nanoclaw"];
    for provider in providers {
        write_history_source_plugin_at(&plugin_root, provider, true, None);
    }
    write_history_source_plugin_at(&plugin_root, "disabled-dorkos", false, None);

    let imported = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args(["import", "--all", "--json", "--progress", "none"]),
    );
    assert_eq!(imported["totals"]["imported_sources"], 4);
    assert_eq!(imported["totals"]["imported_sessions"], 4);
    assert_eq!(imported["totals"]["imported_events"], 4);
    let sources = imported["sources"].as_array().unwrap();
    for provider in providers {
        assert!(
            sources
                .iter()
                .any(|source| source["history_source"] == format!("{provider}/default")),
            "missing import source for {provider}: {sources:#?}"
        );
        let search = json_output(ctx(&temp).args([
            "search",
            &format!("{provider} plugin initial marker"),
            "--provider",
            "custom",
            "--refresh",
            "off",
            "--json",
        ]));
        assert!(
            !search["results"].as_array().unwrap().is_empty(),
            "{provider} plugin result was not searchable: {search:#}"
        );
    }
    assert!(!sources
        .iter()
        .any(|source| source["history_source"] == "disabled-dorkos/default"));
}

#[test]
fn import_all_discovers_and_imports_providers_together() {
    let temp = tempdir();
    copy_dir_all(
        Path::new(&provider_history_fixture("codex-sessions")),
        &temp.path().join(".codex").join("sessions"),
    );
    let pi_home = temp.path().join(".pi");
    fs::create_dir_all(&pi_home).unwrap();
    fs::copy(
        provider_history_fixture("pi-session.jsonl"),
        pi_home.join("sessions.jsonl"),
    )
    .unwrap();

    let output = ctx(&temp)
        .args(["import", "--all", "--json", "--progress", "json"])
        .assert()
        .success()
        .get_output()
        .clone();

    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["schema_version"], 1);
    assert!(stdout["totals"]["imported_sessions"].as_u64().unwrap() >= 3);
    let sources = stdout["sources"].as_array().unwrap();
    assert_eq!(sources.len(), 2);
    assert!(sources.iter().any(|source| source["provider"] == "codex"));
    assert!(sources.iter().any(|source| source["provider"] == "pi"));

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(r#""type":"ctx_progress""#), "{stderr}");
    assert!(stderr.contains(r#""phase":"finalizing""#), "{stderr}");
}

#[test]
fn import_all_without_sources_does_not_report_missing_explicit_path() {
    let temp = tempdir();
    let stderr = failure_stderr(ctx(&temp).args(["import", "--all", "--json"]));

    assert!(stderr.contains("no importable provider history sources found"));
    assert!(!stderr.contains("import path does not exist"), "{stderr}");
}

#[test]
fn import_all_skips_empty_gemini_source() {
    let temp = tempdir();
    copy_dir_all(
        Path::new(&provider_history_fixture("codex-sessions")),
        &temp.path().join(".codex").join("sessions"),
    );
    fs::create_dir_all(temp.path().join(".gemini")).unwrap();

    let sources = json_output(ctx(&temp).args(["sources", "--json"]));
    let gemini = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["provider"] == "gemini")
        .unwrap();
    assert_eq!(gemini["status"], "empty");
    assert_eq!(gemini["native_import"], true);
    assert_eq!(gemini["importable"], false);

    let imported =
        json_output(ctx(&temp).args(["import", "--all", "--json", "--progress", "none"]));
    assert_eq!(imported["totals"]["imported_sources"], 1);
    assert_eq!(imported["totals"]["failed_sources"], 0);
    assert!(imported["sources"]
        .as_array()
        .unwrap()
        .iter()
        .all(|source| source["provider"] != "gemini"));
}

#[test]
fn sources_lists_personal_agent_provider_defaults() {
    let temp = tempdir();
    install_default_openclaw_fixture(&temp, "openclaw-sources-oracle");
    install_default_hermes_fixture(&temp, "hermes-sources-oracle");
    install_default_astrbot_fixture(&temp, "astrbot-sources-oracle");

    let sources = json_output(ctx(&temp).args(["sources", "--json"]));
    for (provider, source_format, import_support, native_import) in [
        ("openclaw", "openclaw_session_jsonl_tree", "native", true),
        ("hermes", "hermes_state_sqlite", "native", true),
        ("astrbot", "astrbot_data_v4_sqlite", "preview", false),
    ] {
        let source = sources["sources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|source| {
                source["provider"] == provider && source["source_format"] == source_format
            })
            .unwrap_or_else(|| panic!("missing {provider} source in {sources:#}"));
        assert_eq!(source["status"], "available");
        assert_eq!(source["import_support"], import_support);
        assert_eq!(source["native_import"], native_import);
        assert_eq!(source["importable"], true);
        assert!(source["unsupported_reason"].is_null());
    }
}

#[test]
fn preview_native_sources_are_listed_but_not_auto_imported() {
    let temp = tempdir();
    let query = "nanoclaw-preview-auto-refresh-oracle";
    let project = PathBuf::from(write_native_nanoclaw_fixture(&temp, query));

    let mut sources_command = ctx(&temp);
    sources_command.current_dir(&project);
    let sources = json_output(sources_command.args(["sources", "--json"]));
    let nanoclaw = sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|source| source["provider"] == "nanoclaw")
        .unwrap();
    assert_eq!(nanoclaw["status"], "available");
    assert_eq!(nanoclaw["import_support"], "preview");
    assert_eq!(nanoclaw["native_import"], false);
    assert_eq!(nanoclaw["importable"], true);
    assert!(nanoclaw["unsupported_reason"].is_null());

    let mut search_command = ctx(&temp);
    search_command.current_dir(&project);
    let search =
        json_output(search_command.args(["search", query, "--provider", "nanoclaw", "--json"]));
    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "no_sources");
    assert_eq!(search["freshness"]["source_count"], 0);
    assert!(search["results"].as_array().unwrap().is_empty());

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "nanoclaw",
        "--path",
        project.to_str().unwrap(),
        "--json",
    ]));
    assert_eq!(imported["totals"]["failed"], 0);
    assert_eq!(imported["totals"]["imported_sources"], 1);

    let search_after_import =
        json_output(ctx(&temp).args(["search", query, "--provider", "nanoclaw", "--json"]));
    assert_search_provider_oracle(&search_after_import, "nanoclaw", query, 1, "message");
}

#[test]
fn import_all_reports_source_failure_without_losing_successes() {
    let temp = tempdir();
    copy_dir_all(
        Path::new(&provider_history_fixture("codex-sessions")),
        &temp.path().join(".codex").join("sessions"),
    );
    let opencode_dir = temp.path().join(".local/share/opencode");
    fs::create_dir_all(&opencode_dir).unwrap();
    fs::write(opencode_dir.join("opencode.db"), b"not sqlite").unwrap();

    let output = ctx(&temp)
        .args(["import", "--all", "--json", "--progress", "none"])
        .assert()
        .success()
        .get_output()
        .clone();
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["schema_version"], 1);
    assert_eq!(stdout["totals"]["imported_sources"], 1);
    assert_eq!(stdout["totals"]["failed_sources"], 1);
    assert!(stdout["totals"]["imported_sessions"].as_u64().unwrap() > 0);
    let sources = stdout["sources"].as_array().unwrap();
    assert!(sources
        .iter()
        .any(|source| source["provider"] == "codex" && source["status"] == "imported"));
    assert!(sources
        .iter()
        .any(|source| source["provider"] == "opencode" && source["status"] == "failed"));
    let opencode_failure = sources
        .iter()
        .find(|source| source["provider"] == "opencode")
        .unwrap();
    assert!(
        opencode_failure["error"]
            .as_str()
            .unwrap()
            .contains("not a database"),
        "{opencode_failure}"
    );
}

#[test]
fn failed_import_attempt_does_not_count_as_indexed_history() {
    let temp = tempdir();
    let opencode_dir = temp.path().join(".local/share/opencode");
    fs::create_dir_all(&opencode_dir).unwrap();
    fs::write(opencode_dir.join("opencode.db"), b"not sqlite").unwrap();

    ctx(&temp)
        .args(["import", "--all", "--json", "--progress", "none"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("all import sources failed"));

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["indexed_items"], 0);
    assert_eq!(status["indexed_sources"], 0);
}

#[test]
fn provider_help_matches_implemented_importers() {
    let temp = tempdir();
    let output = ctx(&temp)
        .args(["import", "--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let help = String::from_utf8(output).unwrap();

    for value in [
        "codex",
        "pi",
        "claude",
        "opencode",
        "openclaw",
        "hermes",
        "nanoclaw",
        "astrbot",
        "antigravity",
        "gemini",
        "cursor",
        "copilot-cli",
        "factory-ai-droid",
    ] {
        assert!(help.contains(value), "provider {value} missing in\n{help}");
    }
}

#[test]
fn provider_json_names_are_accepted_as_cli_filter_aliases() {
    let temp = tempdir();
    // `--refresh off` requires an initialized store, so set one up first.
    ctx(&temp)
        .args(["setup", "--json", "--progress", "none"])
        .assert()
        .success();
    for (provider, expected) in [
        ("copilot_cli", "copilot_cli"),
        ("factory_ai_droid", "factory_ai_droid"),
        ("open_claw", "openclaw"),
        ("nano_claw", "nanoclaw"),
        ("astr_bot", "astrbot"),
    ] {
        let search = json_output(ctx(&temp).args([
            "search",
            "anything",
            "--provider",
            provider,
            "--refresh",
            "off",
            "--json",
        ]));
        assert_eq!(search["filters"]["provider"], expected);
    }
}

#[test]
fn search_excludes_active_codex_session_by_default_when_available() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
        "--progress",
        "none",
    ]));

    let excluded = json_output(
        ctx(&temp)
            .env("CODEX_THREAD_ID", "codex-session-root")
            .args([
                "search",
                "onboarding",
                "--provider",
                "codex",
                "--refresh",
                "off",
                "--json",
            ]),
    );
    assert_eq!(excluded["results"].as_array().unwrap().len(), 0);
    assert_eq!(
        excluded["filters"]["exclude_provider_session"]["provider"],
        "codex"
    );
    assert_eq!(
        excluded["filters"]["exclude_provider_session"]["provider_session_id"],
        "codex-session-root"
    );
    assert!(excluded["filters"]["exclude_provider_session"]["session_id"].is_string());

    let excluded_tree = json_output(
        ctx(&temp)
            .env("CODEX_THREAD_ID", "codex-session-root")
            .args([
                "search",
                "local history search",
                "--provider",
                "codex",
                "--refresh",
                "off",
                "--json",
            ]),
    );
    assert_eq!(
        excluded_tree["results"].as_array().unwrap().len(),
        0,
        "active session tree was not excluded: {excluded_tree:#}"
    );

    let included = json_output(
        ctx(&temp)
            .env("CODEX_THREAD_ID", "codex-session-root")
            .args([
                "search",
                "onboarding",
                "--provider",
                "codex",
                "--refresh",
                "off",
                "--include-current-session",
                "--json",
            ]),
    );
    assert_search_provider_oracle(&included, "codex", "onboarding", 1, "message");
    assert!(included["filters"]["exclude_provider_session"].is_null());

    let included_tree = json_output(
        ctx(&temp)
            .env("CODEX_THREAD_ID", "codex-session-root")
            .args([
                "search",
                "local history search",
                "--provider",
                "codex",
                "--refresh",
                "off",
                "--include-current-session",
                "--json",
            ]),
    );
    assert!(!included_tree["results"].as_array().unwrap().is_empty());
}

#[test]
fn public_subcommand_help_is_golden_enough_for_session_retrieval() {
    let temp = tempdir();
    for (command, required) in [
        ("setup", vec!["Usage: ctx setup", "--json"]),
        ("status", vec!["Usage: ctx status", "--json"]),
        ("sources", vec!["Usage: ctx sources", "--json"]),
        (
            "import",
            vec![
                "Usage: ctx import",
                "--provider <PROVIDER>",
                "[possible values: codex, pi, claude, opencode, antigravity, gemini, cursor, copilot-cli, factory-ai-droid, openclaw, hermes, nanoclaw, astrbot]",
                "--path <PATH>",
                "--format <FORMAT>",
                "--resume",
                "--json",
            ],
        ),
        ("show", vec!["Usage: ctx show", "session", "event"]),
        ("locate", vec!["Usage: ctx locate", "session", "event"]),
        (
            "docs",
            vec![
                "Usage: ctx docs",
                "list",
                "search",
                "show",
                "man",
                "Read embedded ctx documentation",
            ],
        ),
        ("mcp", vec!["Usage: ctx mcp", "serve"]),
        (
            "sql",
            vec![
                "Usage: ctx sql",
                "--format <FORMAT>",
                "--file <FILE>",
                "--max-rows <MAX_ROWS>",
                "Run read-only SQL against the local ctx index",
            ],
        ),
        (
            "search",
            vec![
                "Usage: ctx search",
                "[QUERY]",
                "Natural-language query to search local agent history",
                "--term <TERM>",
                "Add another search query or keyword",
                "--provider <PROVIDER>",
                "--workspace <WORKSPACE>",
                "Filter by stored workspace",
                "--since <SINCE>",
                "Filter to recent history, as RFC3339 or a day window like 30d",
                "--include-subagents",
                "Include subagent sessions",
                "--event-type <EVENT_TYPE>",
                "Filter by event type:",
                "--file <FILE>",
                "indexed touched-file path metadata",
                "--session <SESSION>",
                "--events",
                "--limit <LIMIT>",
                "Maximum results to return, from 1 to 200",
                "--refresh <REFRESH>",
                "Pre-search refresh behavior. auto best-effort refreshes",
                "--include-current-session",
                "Include the active Codex session tree when CODEX_THREAD_ID is set",
                "--json",
                "Print machine-readable JSON",
                "--verbose",
                "Print expanded text details",
            ],
        ),
        ("doctor", vec!["Usage: ctx doctor", "--json", "--progress"]),
    ] {
        let output = ctx(&temp)
            .args([command, "--help"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let help = String::from_utf8(output).unwrap();
        for needle in required {
            assert!(
                help.contains(needle),
                "{command} help missing {needle} in\n{help}"
            );
        }
        for forbidden in ["dashboard", "shim", "publish", "link-pr"] {
            assert!(
                !help.contains(forbidden),
                "{command} help leaked {forbidden} in\n{help}"
            );
        }
    }
}

#[test]
fn sql_reads_existing_store_and_supports_formats_and_input_sources() {
    let temp = tempdir();
    ctx(&temp)
        .args(["setup", "--catalog-only", "--progress", "none"])
        .assert()
        .success();

    let json = json_output(ctx(&temp).args(["sql", "SELECT 1 AS one, 'two' AS two", "--json"]));
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["item_type"], "sql_result");
    assert_eq!(json["read_only"], true);
    assert_eq!(json["share_safe"], false);
    assert_eq!(json["columns"], json!(["one", "two"]));
    assert_eq!(json["rows"], json!([[1, "two"]]));
    assert_eq!(json["returned_rows"], 1);

    let query_file = temp.path().join("query.sql");
    fs::write(&query_file, "SELECT 'a,b' AS value, 2 AS n").unwrap();
    let csv_output = ctx(&temp)
        .arg("sql")
        .arg("--file")
        .arg(&query_file)
        .args(["--format", "csv"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(
        String::from_utf8(csv_output).unwrap(),
        "value,n\n\"a,b\",2\n"
    );

    let raw_output = ctx(&temp)
        .args(["sql", "-", "--format", "raw"])
        .write_stdin("SELECT 'abc' AS value")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(String::from_utf8(raw_output).unwrap(), "abc\n");
}

#[test]
fn sql_is_read_only_and_does_not_initialize_store() {
    let temp = tempdir();
    let stderr = failure_stderr(ctx(&temp).args(["sql", "SELECT 1"]));
    assert!(stderr.contains("ctx store is not initialized"));
    assert!(!temp.path().join("work.sqlite").exists());

    ctx(&temp)
        .args(["setup", "--catalog-only", "--progress", "none"])
        .assert()
        .success();

    let stderr = failure_stderr(ctx(&temp).args(["sql", "CREATE TABLE nope(x INTEGER)"]));
    assert!(stderr.contains("SQL query must be read-only"));
    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'nope'"
        ),
        0
    );

    let stderr = failure_stderr(ctx(&temp).args(["sql", "SELECT 1; SELECT 2"]));
    assert!(stderr.contains("Multiple statements provided"));
}

fn write_bare_store_with_user_version(temp: &TempDir, version: i64) -> PathBuf {
    let db_path = temp.path().join("work.sqlite");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch("CREATE TABLE foreign_marker (id INTEGER PRIMARY KEY, note TEXT);")
        .unwrap();
    conn.execute(
        "INSERT INTO foreign_marker (note) VALUES ('foreign row')",
        [],
    )
    .unwrap();
    conn.execute_batch(&format!("PRAGMA user_version = {version};"))
        .unwrap();
    db_path
}

#[test]
fn read_only_commands_direct_old_schemas_to_a_writable_migration() {
    let temp = tempdir();
    write_bare_store_with_user_version(&temp, 15);

    let stderr = failure_stderr(ctx(&temp).args(["sql", "SELECT 1"]));
    assert!(
        stderr.contains("schema version 15 is older than this ctx binary"),
        "{stderr}"
    );
    assert!(
        stderr.contains("run a writable command such as `ctx status` once to migrate"),
        "{stderr}"
    );
}

#[test]
fn read_only_commands_reject_foreign_schemas_without_impossible_migration_advice() {
    // 16 sits in the unreviewed upstream gap; 1001 is newer than this
    // binary. Neither can be migrated by it, so the guidance must say
    // upgrade/restore rather than suggesting a migration command.
    for version in [16i64, 1001] {
        let temp = tempdir();
        let db_path = write_bare_store_with_user_version(&temp, version);
        let before = fs::read(&db_path).unwrap();

        let stderr = failure_stderr(ctx(&temp).args(["sql", "SELECT 1"]));
        assert!(
            stderr.contains(&format!(
                "schema version {version} is newer than or incompatible with this ctx binary"
            )),
            "{stderr}"
        );
        assert!(stderr.contains("upgrade ctx"), "{stderr}");
        assert!(
            !stderr.contains("once to migrate"),
            "foreign version {version} must not be advertised as migratable: {stderr}"
        );

        // A writable command refuses the foreign database too, and the
        // rejection happens before any persistent PRAGMA: the file bytes
        // (header, schema, journal mode) stay exactly as a foreign binary
        // left them.
        ctx(&temp).args(["status"]).assert().failure();
        assert_eq!(
            fs::read(&db_path).unwrap(),
            before,
            "rejected version {version} store was mutated"
        );
    }
}

#[test]
fn mcp_status_reports_version_guidance_for_foreign_schema() {
    let temp = tempdir();
    write_bare_store_with_user_version(&temp, 1001);
    let responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": { "name": "status", "arguments": {} }
            }),
        ],
    );

    assert_eq!(responses.len(), 2);
    let result = &responses[1]["result"];
    assert_eq!(result["isError"], true);
    let error = result["structuredContent"]["error"].as_str().unwrap();
    assert!(
        error.contains("schema version 1001 is newer than or incompatible with this ctx binary"),
        "{error}"
    );
    assert!(error.contains("upgrade ctx"), "{error}");
    assert!(!error.contains("once to migrate"), "{error}");
}

#[test]
fn docs_commands_expose_embedded_docs_and_man_pages() {
    let temp = tempdir();

    let list = json_output(ctx(&temp).args(["docs", "list", "--json"]));
    assert_eq!(list["schema_version"], 1);
    assert!(list["topics"]
        .as_array()
        .unwrap()
        .iter()
        .any(|topic| topic["id"] == "cli-reference"));
    for topic_id in ["docs", "mcp", "sql", "storage"] {
        assert!(list["topics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|topic| topic["id"] == topic_id));
    }
    assert!(
        !list["topics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|topic| topic["id"] == "upgrade"),
        "removed upgrade docs topic still listed"
    );

    let search = json_output(ctx(&temp).args(["docs", "search", "storage", "--json"]));
    assert_eq!(search["schema_version"], 1);
    assert_eq!(search["query"], "storage");
    assert!(!search["results"].as_array().unwrap().is_empty());

    let sql_search = json_output(ctx(&temp).args(["docs", "search", "sql", "--json"]));
    assert_eq!(sql_search["results"][0]["id"], "sql");

    let mcp_search = json_output(ctx(&temp).args(["docs", "search", "mcp", "--json"]));
    assert_eq!(mcp_search["results"][0]["id"], "mcp");

    let weak_search = json_output(ctx(&temp).args(["docs", "search", "a", "--json"]));
    assert!(weak_search["results"].as_array().unwrap().is_empty());
    assert!(weak_search["suggested_next_commands"]
        .as_array()
        .unwrap()
        .iter()
        .any(|command| command == "ctx docs list"));

    let show = json_output(ctx(&temp).args(["docs", "show", "cli-reference", "--format", "json"]));
    assert_eq!(show["schema_version"], 1);
    assert_eq!(show["id"], "cli-reference");
    assert!(show["body"].as_str().unwrap().contains("ctx search"));

    let mcp = json_output(ctx(&temp).args(["docs", "show", "mcp", "--format", "json"]));
    assert!(mcp["body"].as_str().unwrap().contains("ctx mcp serve"));

    let missing_topic = failure_stderr(ctx(&temp).args(["docs", "show", "cli"]));
    assert!(missing_topic.contains("unknown ctx docs topic: cli"));
    assert!(missing_topic.contains("nearest topics:"));
    assert!(missing_topic.contains("ctx docs list"));
    assert!(missing_topic.contains("ctx docs search cli"));

    let man = ctx(&temp)
        .args(["docs", "man", "--print", "ctx"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let man = String::from_utf8(man).unwrap();
    assert!(man.contains(".TH ctx"));
    assert!(man.contains("Search local agent history"));
}

#[test]
fn provider_session_lookup_requires_explicit_provider_flags_in_help() {
    let temp = tempdir();
    for args in [
        vec!["show", "session", "--help"],
        vec!["locate", "session", "--help"],
        vec!["locate", "event", "--help"],
    ] {
        let output = ctx(&temp)
            .args(args.clone())
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let help = String::from_utf8(output).unwrap();
        for needle in [
            "--provider <PROVIDER>",
            "--provider-session <PROVIDER_SESSION>",
        ] {
            if args.as_slice() == ["locate", "event", "--help"] {
                continue;
            }
            assert!(
                help.contains(needle),
                "{args:?} help missing {needle} in\n{help}"
            );
        }
        if args[0] == "locate" {
            assert!(
                help.contains("[possible values: text, json]"),
                "{args:?} help should restrict locate formats to text/json in\n{help}"
            );
            assert!(
                !help.contains("markdown") && !help.contains("jsonl"),
                "{args:?} help leaked unsupported locate formats in\n{help}"
            );
        }
        if args.as_slice() == ["show", "session", "--help"] {
            for needle in [
                "--mode <MODE>",
                "--out <OUT>",
                "[default: lite]",
                "[possible values: full, lite, log]",
            ] {
                assert!(
                    help.contains(needle),
                    "{args:?} help missing {needle} in\n{help}"
                );
            }
        }
    }
}

#[test]
fn removed_public_commands_are_rejected() {
    let temp = tempdir();
    let root_output = ctx(&temp)
        .arg("--help")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let root_help = String::from_utf8(root_output).unwrap();
    let commands = root_help
        .split("Commands:")
        .nth(1)
        .and_then(|tail| tail.split("Options:").next())
        .unwrap_or(&root_help);
    for removed in ["context", "list", "export", "validate"] {
        assert!(
            !commands.contains(removed),
            "removed {removed} command appeared in root help\n{root_help}"
        );
    }

    for args in [
        vec!["context", "onboarding", "--json"],
        vec!["list", "--json"],
        vec!["export", "session", "00000000-0000-0000-0000-000000000000"],
        vec!["validate", "--json"],
    ] {
        ctx(&temp).args(args.clone()).assert().failure().stderr(
            predicate::str::contains("unrecognized subcommand")
                .and(predicate::str::contains(args[0])),
        );
    }
}

#[test]
fn fresh_home_search_mvp_flow() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");

    ctx(&temp)
        .arg("setup")
        .assert()
        .success()
        .stdout(predicate::str::contains("no local history was indexed"));

    let setup_json = json_output(ctx(&temp).args(["setup", "--json"]));
    assert_eq!(setup_json["schema_version"], 1);
    assert_eq!(setup_json["network_required"], false);
    assert_eq!(setup_json["repo_writes"], false);
    assert_eq!(setup_json["import"]["ran"], true);

    let sources = json_output(ctx(&temp).args(["sources", "--json"]));
    assert_eq!(sources["schema_version"], 1);
    assert!(sources["sources"]
        .as_array()
        .unwrap()
        .iter()
        .any(|source| source["provider"] == "codex"));

    let import = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(import["schema_version"], 1);
    assert!(import["totals"]["imported_sessions"].as_u64().unwrap() > 0);
    assert!(import["totals"]["source_files"].as_u64().unwrap() > 0);
    assert!(import["totals"]["source_bytes"].as_u64().unwrap() > 0);

    let search =
        json_output(ctx(&temp).args(["search", "onboarding", "--provider", "codex", "--json"]));
    assert_eq!(search["schema_version"], 1);
    assert_eq!(search["share_safe"], false);
    assert_omits_keys(
        &search,
        &[
            "record_id",
            "history_record_id",
            "raw_source_path",
            "kind",
            "external_session_id",
        ],
    );
    let first_result = &search["results"][0];
    assert_eq!(first_result["item_type"], "session_result");
    assert_eq!(first_result["result_scope"], "session");
    let ctx_event_id = first_result["ctx_event_id"].as_str().unwrap().to_owned();
    let ctx_session_id = first_result["ctx_session_id"].as_str().unwrap().to_owned();
    assert!(first_result["provider_session_id"].is_string());
    assert!(first_result["source_path"].is_string());
    assert!(first_result["cursor"].is_string());
    assert_session_suggested_next_commands(first_result);
    assert!(first_result["citations"][0]["ctx_event_id"].is_string());
    assert!(first_result["citations"][0]["ctx_session_id"].is_string());

    let term_search = json_output(ctx(&temp).args([
        "search",
        "zzzz-no-match",
        "--term",
        "onboarding",
        "--provider",
        "codex",
        "--json",
    ]));
    assert_eq!(term_search["query"], "zzzz-no-match OR onboarding");
    assert!(!term_search["results"].as_array().unwrap().is_empty());
    assert!(term_search["results"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|result| { result["suggested_next_commands"].as_array().unwrap().iter() })
        .all(|command| !command.as_str().unwrap().starts_with("ctx search ")));

    let event_search = json_output(ctx(&temp).args([
        "search",
        "onboarding",
        "--provider",
        "codex",
        "--events",
        "--json",
    ]));
    assert_event_search_provider_oracle(&event_search, "codex", "onboarding", 1, "message");

    let session_events = json_output(ctx(&temp).args([
        "search",
        "onboarding",
        "--provider",
        "codex",
        "--session",
        &ctx_session_id,
        "--json",
    ]));
    assert_event_search_provider_oracle(&session_events, "codex", "onboarding", 1, "message");
    assert_eq!(session_events["filters"]["session"], ctx_session_id);
    assert!(session_events["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|result| result["ctx_session_id"] == ctx_session_id));

    let session_prefix = &ctx_session_id[..8];
    let prefixed_session_events = json_output(ctx(&temp).args([
        "search",
        "onboarding",
        "--provider",
        "codex",
        "--session",
        session_prefix,
        "--json",
    ]));
    assert_event_search_provider_oracle(
        &prefixed_session_events,
        "codex",
        "onboarding",
        1,
        "message",
    );
    assert_eq!(
        prefixed_session_events["filters"]["session"],
        ctx_session_id
    );

    let human_search = ctx(&temp)
        .args(["search", "onboarding"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let human_search = String::from_utf8(human_search).unwrap();
    assert!(human_search.contains("1. "));
    assert!(human_search.contains("importance"));
    assert!(human_search.contains("session "));
    assert!(human_search.contains("event "));
    assert!(human_search.contains("inspect: ctx show event"));
    assert!(!human_search.contains("ctx_event_id"));
    assert!(!human_search.contains("provider_session_id"));
    assert!(!human_search.contains("next:"));
    assert!(!human_search.contains("work_record"));
    assert!(!human_search.contains("history_record"));

    let verbose_search = ctx(&temp)
        .args(["search", "onboarding", "--verbose"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let verbose_search = String::from_utf8(verbose_search).unwrap();
    assert!(verbose_search.contains("ctx_event_id"));
    assert!(verbose_search.contains("ctx_session_id"));
    assert!(verbose_search.contains("provider_session_id"));
    assert!(verbose_search.contains("session_importance"));
    assert!(verbose_search.contains("next: ctx show session"));
    assert!(verbose_search.contains("next: ctx show event"));
    assert!(verbose_search.contains("next: ctx search onboarding --session"));
    assert!(!human_search.contains("work_record"));
    assert!(!human_search.contains("history_record"));

    let file_search =
        json_output(ctx(&temp).args(["search", "--file", "crates/foo/src/lib.rs", "--json"]));
    assert_eq!(file_search["query"], "");
    assert!(file_search["results"].is_array());

    let show_event = json_output(ctx(&temp).args([
        "show",
        "event",
        &ctx_event_id,
        "--window",
        "2",
        "--format",
        "json",
    ]));
    assert_eq!(show_event["schema_version"], 1);
    assert_eq!(show_event["item_type"], "event_window");
    assert_eq!(show_event["event"]["ctx_event_id"], ctx_event_id);
    assert_eq!(show_event["event"]["ctx_session_id"], ctx_session_id);
    assert_omits_keys(
        &show_event,
        &[
            "record_id",
            "history_record_id",
            "kind",
            "payload",
            "payload_blob_id",
            "dedupe_key",
            "capture_source_id",
        ],
    );
    assert!(show_event["events"]
        .as_array()
        .unwrap()
        .iter()
        .all(|event| event["ctx_event_id"].is_string()
            && event["ctx_session_id"].is_string()
            && event["preview"].is_string()));

    let show_event_prefix = json_output(ctx(&temp).args([
        "show",
        "event",
        &ctx_event_id[..8],
        "--window",
        "1",
        "--format",
        "json",
    ]));
    assert_eq!(show_event_prefix["event"]["ctx_event_id"], ctx_event_id);

    let show_session =
        json_output(ctx(&temp).args(["show", "session", &ctx_session_id, "--format", "json"]));
    assert_eq!(show_session["schema_version"], 1);
    assert_eq!(show_session["item_type"], "session_transcript");
    assert_eq!(show_session["session"]["item_type"], "session");
    assert_eq!(show_session["session"]["item_id"], ctx_session_id);
    assert_eq!(show_session["mode"], "lite");

    let show_session_prefix =
        json_output(ctx(&temp).args(["show", "session", &ctx_session_id[..8], "--format", "json"]));
    assert_eq!(show_session_prefix["session"]["item_id"], ctx_session_id);

    let show_session_full = json_output(ctx(&temp).args([
        "show",
        "session",
        &ctx_session_id,
        "--mode",
        "full",
        "--format",
        "json",
    ]));
    assert_eq!(show_session_full["schema_version"], 1);
    assert_eq!(show_session_full["item_type"], "session_transcript");
    assert_eq!(show_session_full["session"]["item_id"], ctx_session_id);
    assert_eq!(show_session_full["mode"], "full");

    let locate_event = json_output(ctx(&temp).args(["locate", "event", &ctx_event_id, "--json"]));
    assert_eq!(locate_event["schema_version"], 1);
    assert_eq!(locate_event["item_type"], "event_location");
    assert_eq!(locate_event["ctx_event_id"], ctx_event_id);
    assert_eq!(locate_event["ctx_session_id"], ctx_session_id);
    assert_eq!(locate_event["provider"], "codex");
    assert!(locate_event["provider_session_id"].is_string());
    assert!(locate_event["source"]["path"].is_string());
    assert!(locate_event["cursor"].is_string());

    let export_path = temp.path().join("transcript.md");
    ctx(&temp)
        .args([
            "show",
            "session",
            &ctx_session_id,
            "--format",
            "markdown",
            "--out",
            export_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    assert!(
        export_path.exists(),
        "show session --out should write the requested artifact path"
    );
    let exported = fs::read_to_string(&export_path).unwrap();
    assert!(
        exported.contains("- mode: `lite`"),
        "show session --out should default to lite transcript mode"
    );

    let full_export_path = temp.path().join("transcript-full.md");
    ctx(&temp)
        .args([
            "show",
            "session",
            &ctx_session_id,
            "--mode",
            "full",
            "--format",
            "markdown",
            "--out",
            full_export_path.to_str().unwrap(),
        ])
        .assert()
        .success();
    let exported_full = fs::read_to_string(&full_export_path).unwrap();
    assert!(
        exported_full.contains("- mode: `full`"),
        "show session --mode full --out should remain explicit"
    );

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["schema_version"], 1);
    assert!(status["indexed_items"].as_u64().unwrap() > 0);

    let doctor = json_output(ctx(&temp).args(["doctor", "--json"]));
    assert_eq!(doctor["schema_version"], 1);
    assert_eq!(doctor["ok"], true);
    assert_eq!(doctor["progress"], "auto");

    let doctor_progress = ctx(&temp)
        .args(["doctor", "--json", "--progress", "json"])
        .assert()
        .success()
        .get_output()
        .stderr
        .clone();
    let doctor_progress = String::from_utf8(doctor_progress).unwrap();
    assert!(doctor_progress.contains(r#""operation":"doctor""#));
    assert!(doctor_progress.contains(r#""phase":"checking""#));
}

#[test]
fn doctor_reports_missing_store_without_creating_it() {
    let temp = tempdir();

    let doctor = json_output(ctx(&temp).args(["doctor", "--json"]));

    assert_eq!(doctor["schema_version"], 1);
    assert_eq!(doctor["ok"], false);
    assert!(doctor["findings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|finding| {
            finding
                .as_str()
                .unwrap()
                .contains("ctx store is not initialized")
        }));
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "doctor should not create the ctx store"
    );
}

#[test]
fn mcp_status_and_tools_list_are_read_only_without_initialized_store() {
    let temp = tempdir();
    let responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list"
            }),
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "status",
                    "arguments": {}
                }
            }),
        ],
    );

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "ctx");
    assert_eq!(
        responses[0]["result"]["capabilities"]["tools"]["listChanged"],
        false
    );

    let tools = responses[1]["result"]["tools"].as_array().unwrap();
    for expected in [
        "status",
        "sources",
        "search",
        "sql",
        "show_session",
        "show_event",
    ] {
        assert!(
            tools.iter().any(|tool| tool["name"] == expected),
            "missing MCP tool {expected} in {tools:#?}"
        );
    }
    assert!(
        tools.iter().all(|tool| tool["name"] != "research"),
        "MCP research tool should not be exposed in {tools:#?}"
    );
    let search_tool = tools.iter().find(|tool| tool["name"] == "search").unwrap();
    let providers = search_tool["inputSchema"]["properties"]["provider"]["enum"]
        .as_array()
        .unwrap();
    assert!(providers.iter().any(|provider| provider == "copilot-cli"));
    assert!(providers.iter().any(|provider| provider == "copilot_cli"));

    let status = &responses[2]["result"]["structuredContent"];
    assert_eq!(status["schema_version"], 1);
    assert_eq!(status["initialized"], false);
    assert_eq!(status["read_only"], true);
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "MCP status should not initialize the ctx store"
    );
}

#[test]
fn mcp_sql_tool_returns_structured_json_and_rejects_writes() {
    let temp = tempdir();
    ctx(&temp)
        .args(["setup", "--catalog-only", "--progress", "none"])
        .assert()
        .success();

    let responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "sql",
                "method": "tools/call",
                "params": {
                    "name": "sql",
                    "arguments": {
                        "sql": "SELECT COUNT(*) AS sessions FROM ctx_sessions",
                        "max_rows": 5
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "write",
                "method": "tools/call",
                "params": {
                    "name": "sql",
                    "arguments": {
                        "sql": "CREATE TABLE nope(x INTEGER)"
                    }
                }
            }),
        ],
    );

    let sql = &responses[1]["result"]["structuredContent"];
    assert_eq!(sql["item_type"], "sql_result");
    assert_eq!(sql["read_only"], true);
    assert_eq!(sql["share_safe"], false);
    assert_eq!(sql["columns"], json!(["sessions"]));
    assert_eq!(sql["rows"], json!([[0]]));

    let write = &responses[2]["result"];
    assert_eq!(write["isError"], true);
    assert!(write["structuredContent"]["error"]
        .as_str()
        .unwrap()
        .contains("SQL query must be read-only"));
}

#[test]
fn mcp_search_and_show_tools_return_structured_json_without_refresh() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
        "--progress",
        "none",
    ]));
    assert!(imported["totals"]["imported_events"].as_u64().unwrap() > 0);

    let search_responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": "onboarding",
                        "provider": "codex",
                        "limit": 5
                    }
                }
            }),
        ],
    );
    let search = &search_responses[1]["result"]["structuredContent"];
    assert_eq!(search["schema_version"], 1);
    assert_eq!(search["query"], "onboarding");
    assert_eq!(search["freshness"]["mode"], "off");
    assert_eq!(search["freshness"]["status"], "skipped");
    assert_eq!(search["freshness"]["ran"], false);
    assert_eq!(search["freshness"]["reason"], "refresh_off");
    assert_eq!(search["freshness"]["duration_ms"], 0);
    assert!(search["freshness"]["index_age_seconds"].is_number());
    assert_eq!(search["freshness"]["totals"]["unchanged_sources"], 0);
    assert_eq!(search["share_safe"], false);
    assert_eq!(
        search_responses[1]["result"]["content"][0]["text"],
        "ctx returned structured JSON in structuredContent. Treat it as private local history."
    );
    let first_result = &search["results"][0];
    let ctx_session_id = first_result["ctx_session_id"].as_str().unwrap();
    let ctx_event_id = first_result["ctx_event_id"].as_str().unwrap();

    let show_responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "session",
                "method": "tools/call",
                "params": {
                    "name": "show_session",
                    "arguments": {
                        "ctx_session_id": ctx_session_id,
                        "mode": "lite"
                    }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "event",
                "method": "tools/call",
                "params": {
                    "name": "show_event",
                    "arguments": {
                        "ctx_event_id": ctx_event_id,
                        "window": 1
                    }
                }
            }),
        ],
    );

    let session = &show_responses[1]["result"]["structuredContent"];
    assert_eq!(session["item_type"], "session_transcript");
    assert_eq!(session["ctx_session_id"], ctx_session_id);
    assert_eq!(session["mode"], "lite");
    assert!(session["events"].as_array().unwrap().iter().all(|event| {
        event["ctx_session_id"] == ctx_session_id && event["ctx_event_id"].is_string()
    }));

    let event = &show_responses[2]["result"]["structuredContent"];
    assert_eq!(event["item_type"], "event_window");
    assert_eq!(event["ctx_event_id"], ctx_event_id);
    assert_eq!(event["ctx_session_id"], ctx_session_id);
    assert!(!event["events"].as_array().unwrap().is_empty());
}

#[test]
fn mcp_search_requires_query_term_or_file_without_opening_store() {
    let temp = tempdir();
    let responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "provider": "codex",
                        "limit": 5
                    }
                }
            }),
        ],
    );

    let result = &responses[1]["result"];
    assert_eq!(result["isError"], true);
    assert!(result["structuredContent"]["error"]
        .as_str()
        .unwrap()
        .contains("search needs a query or file"));
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "invalid MCP search should fail before opening the ctx store"
    );
}

#[test]
fn mcp_sources_and_search_support_history_source_plugins() {
    let temp = tempdir();
    let plugin = write_history_source_plugin(&temp, "hermes", false, None);
    json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "import",
                "--history-source",
                "hermes/default",
                "--json",
                "--progress",
                "none",
            ]),
    );

    let responses = mcp_roundtrip_with_env(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "sources",
                "method": "tools/call",
                "params": {
                    "name": "sources",
                    "arguments": {}
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": "hermes plugin initial marker",
                        "provider": "custom",
                        "history_source": "hermes/default",
                        "limit": 5
                    }
                }
            }),
        ],
        &[(
            "CTX_HISTORY_PLUGIN_PATH",
            plugin.manifest_dir.to_str().unwrap(),
        )],
    );

    let sources = responses[1]["result"]["structuredContent"]["sources"]
        .as_array()
        .unwrap();
    assert!(sources
        .iter()
        .any(|source| source["history_source"] == "hermes/default"));

    let search = &responses[2]["result"]["structuredContent"];
    assert_eq!(search["filters"]["provider"], "custom");
    assert_eq!(search["filters"]["history_source"], "hermes/default");
    assert_eq!(search["results"][0]["history_source"], "hermes/default");
}

#[test]
fn mcp_search_excludes_active_codex_session_by_default_when_available() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
        "--progress",
        "none",
    ]));

    let excluded = mcp_roundtrip_with_env(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": "onboarding",
                        "provider": "codex",
                        "limit": 5
                    }
                }
            }),
        ],
        &[("CODEX_THREAD_ID", "codex-session-root")],
    );
    let excluded_search = &excluded[1]["result"]["structuredContent"];
    assert_eq!(excluded_search["results"].as_array().unwrap().len(), 0);
    assert_eq!(
        excluded_search["filters"]["exclude_provider_session"]["provider"],
        "codex"
    );

    let included = mcp_roundtrip_with_env(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": "onboarding",
                        "provider": "codex",
                        "limit": 5,
                        "include_current_session": true
                    }
                }
            }),
        ],
        &[("CODEX_THREAD_ID", "codex-session-root")],
    );
    let included_search = &included[1]["result"]["structuredContent"];
    assert_eq!(included_search["results"].as_array().unwrap().len(), 1);
    assert!(included_search["filters"]["exclude_provider_session"].is_null());
}

#[test]
fn mcp_rejects_unknown_tool_arguments() {
    let temp = tempdir();
    let responses = mcp_roundtrip(
        &temp,
        &[
            json!({
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "ctx-test", "version": "0" }
                }
            }),
            json!({
                "jsonrpc": "2.0",
                "id": "search",
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "arguments": {
                        "query": "onboarding",
                        "refresh": "strict"
                    }
                }
            }),
        ],
    );

    let error = &responses[1]["error"];
    assert_eq!(error["code"], -32602);
    assert!(error["data"]["error"]
        .as_str()
        .unwrap()
        .contains("unknown argument refresh"));
}

#[test]
fn codex_cli_resume_is_idempotent_rescan_and_filters_subagents() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");

    let first = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(first["schema_version"], 1);
    assert_eq!(first["resume"], false);
    assert_eq!(first["resume_mode"], "normal_scan");
    assert_eq!(first["totals"]["imported_sessions"], 2);
    assert_eq!(first["totals"]["imported_events"], 4);
    assert_eq!(first["totals"]["imported_edges"], 1);

    let primary_default = json_output(ctx(&temp).args(["search", "subagent", "--json"]));
    assert_eq!(primary_default["filters"]["include_subagents"], false);
    let primary_default_text = serde_json::to_string(&primary_default).unwrap();
    assert!(
        !primary_default_text.contains("codex-session-child"),
        "{primary_default_text}"
    );

    let default_events = json_output(ctx(&temp).args(["search", "subagent", "--events", "--json"]));
    assert_eq!(default_events["filters"]["include_subagents"], false);
    let default_events_text = serde_json::to_string(&default_events).unwrap();
    assert!(
        !default_events_text.contains("codex-session-child"),
        "{default_events_text}"
    );

    let with_subagents =
        json_output(ctx(&temp).args(["search", "subagent", "--include-subagents", "--json"]));
    assert!(!with_subagents["results"].as_array().unwrap().is_empty());
    assert_eq!(with_subagents["filters"]["include_subagents"], true);
    assert!(serde_json::to_string(&with_subagents)
        .unwrap()
        .contains("codex-session-child"));

    let child_session_lookup = json_output(ctx(&temp).args([
        "sql",
        "SELECT ctx_session_id FROM ctx_sessions WHERE provider_session_id = 'codex-session-child'",
        "--format",
        "json",
    ]));
    let child_session_id = child_session_lookup["rows"][0][0].as_str().unwrap();
    let explicit_child_session = json_output(ctx(&temp).args([
        "search",
        "subagent",
        "--session",
        child_session_id,
        "--json",
    ]));
    assert_eq!(
        explicit_child_session["filters"]["session"],
        child_session_id
    );
    assert!(serde_json::to_string(&explicit_child_session)
        .unwrap()
        .contains("codex-session-child"));

    let primary_only =
        json_output(ctx(&temp).args(["search", "subagent", "--primary-only", "--json"]));
    assert_eq!(primary_only["filters"]["include_subagents"], false);
    assert!(primary_only["filters"]["primary_only"].is_null());
    assert!(
        primary_only["results"].as_array().unwrap().len()
            <= with_subagents["results"].as_array().unwrap().len()
    );

    let second = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--resume",
        "--json",
    ]));
    assert_eq!(second["schema_version"], 1);
    assert_eq!(second["resume"], true);
    assert_eq!(second["resume_mode"], "idempotent_rescan");
    assert_eq!(second["totals"]["imported_sessions"], 0);
    assert_eq!(second["totals"]["imported_events"], 0);
    assert_eq!(second["totals"]["imported_edges"], 0);
    assert!(second["totals"]["skipped"].as_u64().unwrap() > 0);
    assert_eq!(second["sources"][0]["imported_sessions"], 0);
    assert_eq!(second["sources"][0]["imported_events"], 0);
}

#[test]
fn search_refreshes_discovered_codex_sessions_before_query() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    let discovered = temp.path().join(".codex").join("sessions");
    copy_dir_all(&fixture, &discovered);

    let search =
        json_output(ctx(&temp).args(["search", "onboarding", "--provider", "codex", "--json"]));
    assert_search_provider_oracle(&search, "codex", "onboarding", 1, "message");
    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "completed");
    assert_eq!(search["freshness"]["source_count"], 1);
    assert_eq!(search["freshness"]["totals"]["imported_sessions"], 2);

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["cataloged_sessions"], 2);
    assert_eq!(status["indexed_catalog_sessions"], 2);
    assert_eq!(status["pending_catalog_sessions"], 0);
}

#[test]
fn search_refresh_off_serves_existing_index_without_importing() {
    let temp = tempdir();
    let indexed_fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &indexed_fixture,
        "--json",
    ]));
    let discovered_fixture = provider_history_fixture("codex-rich-sessions");
    let discovered = temp.path().join(".codex").join("sessions");
    copy_dir_all(&PathBuf::from(discovered_fixture), &discovered);

    let stale = json_output(ctx(&temp).args([
        "search",
        "redacted sample app",
        "--provider",
        "codex",
        "--refresh",
        "off",
        "--json",
    ]));
    assert_eq!(stale["freshness"]["mode"], "off");
    assert_eq!(stale["freshness"]["status"], "skipped");
    assert!(stale["results"].as_array().unwrap().is_empty());

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["cataloged_sessions"], 2);
    assert_eq!(status["indexed_catalog_sessions"], 2);

    let fresh =
        json_output(ctx(&temp).args(["search", "onboarding", "--provider", "codex", "--json"]));
    assert_search_provider_oracle(&fresh, "codex", "onboarding", 1, "message");
}

#[test]
fn search_refresh_auto_runs_enabled_auto_history_source_plugins_incrementally() {
    let temp = tempdir();
    let cursor_log = temp.path().join("cursor-log.txt");
    let plugin = write_history_source_plugin_with_refresh(
        &temp,
        "hermes",
        true,
        Some("auto"),
        Some(&cursor_log),
    );

    let initial = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "hermes plugin initial marker",
                "--provider",
                "custom",
                "--json",
            ]),
    );
    assert_eq!(initial["freshness"]["mode"], "auto");
    assert_eq!(initial["freshness"]["status"], "completed");
    assert_eq!(initial["freshness"]["source_count"], 1);
    assert_eq!(initial["freshness"]["totals"]["imported_sources"], 1);
    assert_eq!(initial["freshness"]["totals"]["imported_sessions"], 1);
    assert_eq!(initial["freshness"]["totals"]["imported_events"], 1);
    assert!(
        !initial["results"].as_array().unwrap().is_empty(),
        "initial plugin refresh was not searchable before query: {initial:#}"
    );
    assert!(plugin.run_marker.exists());

    fs::remove_file(&plugin.run_marker).unwrap();
    let incremental = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "hermes plugin incremental marker",
                "--provider",
                "custom",
                "--json",
            ]),
    );
    assert_eq!(incremental["freshness"]["mode"], "auto");
    assert_eq!(incremental["freshness"]["status"], "completed");
    assert_eq!(incremental["freshness"]["source_count"], 1);
    assert_eq!(incremental["freshness"]["totals"]["imported_sources"], 1);
    assert_eq!(incremental["freshness"]["totals"]["imported_events"], 1);
    assert!(
        !incremental["results"].as_array().unwrap().is_empty(),
        "incremental plugin refresh was not searchable before query: {incremental:#}"
    );
    assert!(plugin.run_marker.exists());

    let unchanged = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "hermes plugin incremental marker",
                "--provider",
                "custom",
                "--json",
            ]),
    );
    assert_eq!(unchanged["freshness"]["totals"]["imported_sessions"], 0);
    assert_eq!(unchanged["freshness"]["totals"]["imported_events"], 0);
    assert_eq!(unchanged["freshness"]["totals"]["unchanged_sources"], 1);

    let cursor_log = fs::read_to_string(cursor_log).unwrap();
    assert!(cursor_log.contains(r#""message_id":7"#), "{cursor_log}");
    assert!(cursor_log.contains("cursor_file="), "{cursor_log}");
}

#[test]
fn search_refresh_history_source_filter_runs_only_matching_auto_plugin() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let dorkos = write_history_source_plugin_at_with_refresh(
        &plugin_root,
        "dorkos",
        true,
        Some("auto"),
        None,
    );
    let hermes = write_history_source_plugin_at_with_refresh(
        &plugin_root,
        "hermes",
        true,
        Some("auto"),
        None,
    );

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args([
                "search",
                "dorkos plugin initial marker",
                "--history-source",
                "dorkos/default",
                "--json",
            ]),
    );

    assert_eq!(search["filters"]["provider"], "custom");
    assert_eq!(search["filters"]["history_source"], "dorkos/default");
    assert_eq!(search["freshness"]["status"], "completed");
    assert_eq!(search["freshness"]["source_count"], 1);
    assert!(dorkos.run_marker.exists());
    assert!(!hermes.run_marker.exists());
    assert!(
        !search["results"].as_array().unwrap().is_empty(),
        "source-filtered refresh did not import matching plugin: {search:#}"
    );
}

#[test]
fn search_refresh_auto_combines_native_sources_and_auto_history_source_plugins() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    copy_dir_all(&fixture, &temp.path().join(".codex").join("sessions"));
    let plugin =
        write_history_source_plugin_with_refresh(&temp, "hermes", true, Some("auto"), None);

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["search", "hermes plugin initial marker", "--json"]),
    );

    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "completed");
    assert_eq!(search["freshness"]["source_count"], 2);
    assert!(
        search["freshness"]["totals"]["imported_sessions"]
            .as_u64()
            .unwrap()
            >= 3
    );
    assert!(
        !search["results"].as_array().unwrap().is_empty(),
        "combined refresh did not make plugin history searchable: {search:#}"
    );
    assert!(plugin.run_marker.exists());
}

#[test]
fn search_refresh_provider_filter_does_not_execute_history_source_plugins() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    copy_dir_all(&fixture, &temp.path().join(".codex").join("sessions"));
    let plugin =
        write_history_source_plugin_with_refresh(&temp, "hermes", true, Some("auto"), None);

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["search", "onboarding", "--provider", "codex", "--json"]),
    );

    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "completed");
    assert_eq!(search["freshness"]["source_count"], 1);
    assert_search_provider_oracle(&search, "codex", "onboarding", 1, "message");
    assert!(!plugin.run_marker.exists());
}

#[test]
fn search_refresh_off_does_not_execute_history_source_plugins() {
    let temp = tempdir();
    json_output(ctx(&temp).args(["setup", "--json"]));
    let plugin =
        write_history_source_plugin_with_refresh(&temp, "hermes", true, Some("auto"), None);

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "hermes plugin initial marker",
                "--provider",
                "custom",
                "--refresh",
                "off",
                "--json",
            ]),
    );

    assert_eq!(search["freshness"]["mode"], "off");
    assert_eq!(search["freshness"]["status"], "skipped");
    assert!(search["results"].as_array().unwrap().is_empty());
    assert!(!plugin.run_marker.exists());
}

#[test]
fn search_refresh_auto_skips_disabled_or_manual_history_source_plugins() {
    let temp = tempdir();
    let plugin_root = temp.path().join("history-plugins");
    let manual = write_history_source_plugin_at_with_refresh(
        &plugin_root,
        "hermes",
        true,
        Some("manual"),
        None,
    );
    let disabled = write_history_source_plugin_at_with_refresh(
        &plugin_root,
        "dorkos",
        false,
        Some("auto"),
        None,
    );

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin_root)
            .args([
                "search",
                "plugin initial marker",
                "--provider",
                "custom",
                "--json",
            ]),
    );

    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "no_sources");
    assert_eq!(search["freshness"]["source_count"], 0);
    assert!(search["results"].as_array().unwrap().is_empty());
    assert!(!manual.run_marker.exists());
    assert!(!disabled.run_marker.exists());
}

#[test]
fn search_refresh_strict_fails_on_history_source_plugin_failure() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import sys
print("plugin exploded", file=sys.stderr)
sys.exit(23)
"#;
    let plugin = write_raw_history_source_plugin_with_options(
        &temp,
        "badplugin",
        script,
        true,
        Some("auto"),
    );

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "anything",
                "--provider",
                "custom",
                "--refresh",
                "strict",
                "--json",
            ]),
    );

    assert!(stderr.contains("search refresh failed"), "{stderr}");
    assert!(
        stderr.contains("history source plugin badplugin/default failed"),
        "{stderr}"
    );
    assert!(stderr.contains("plugin exploded"), "{stderr}");
}

#[test]
fn search_refresh_auto_failure_without_prior_store_fails_instead_of_serving_empty_index() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import sys
print("plugin exploded", file=sys.stderr)
sys.exit(23)
"#;
    let plugin = write_raw_history_source_plugin_with_options(
        &temp,
        "badplugin",
        script,
        true,
        Some("auto"),
    );

    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["search", "anything", "--provider", "custom", "--json"]),
    );

    assert!(
        stderr.contains("search refresh failed and no existing ctx index is available"),
        "{stderr}"
    );
    assert!(
        stderr.contains("history source plugin badplugin/default failed"),
        "{stderr}"
    );
    assert!(stderr.contains("plugin exploded"), "{stderr}");
}

#[test]
fn search_refresh_auto_failure_serves_prior_index() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    let script = r#"#!/usr/bin/env python3
import sys
print("plugin exploded", file=sys.stderr)
sys.exit(23)
"#;
    let plugin = write_raw_history_source_plugin_with_options(
        &temp,
        "badplugin",
        script,
        true,
        Some("auto"),
    );
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));

    let search = json_output(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args(["search", "onboarding", "--json"]),
    );

    assert_eq!(search["freshness"]["status"], "failed");
    assert_eq!(search["freshness"]["ran"], true);
    assert_eq!(search["freshness"]["reason"], "refresh_failed");
    assert!(search["freshness"]["duration_ms"].as_u64().unwrap() > 0);
    assert!(search["freshness"]["error"]
        .as_str()
        .unwrap()
        .contains("history source plugin badplugin/default failed"));
    assert!(!search["results"].as_array().unwrap().is_empty());
}

#[test]
fn search_refresh_auto_delayed_progress_stays_on_stderr_with_json_stdout() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    let script = r#"#!/usr/bin/env python3
import sys, time
time.sleep(0.05)
print("slow plugin exploded", file=sys.stderr)
sys.exit(23)
"#;
    let plugin =
        write_raw_history_source_plugin_with_options(&temp, "slowbad", script, true, Some("auto"));

    let output = ctx(&temp)
        .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
        .env("CTX_TEST_REFRESH_PROGRESS_DELAY_MS", "1")
        .args(["search", "onboarding", "--json"])
        .assert()
        .success()
        .get_output()
        .clone();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let json: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(json["freshness"]["status"], "failed");
    assert!(stderr.contains(r#""type":"ctx_progress""#), "{stderr}");
    assert!(stderr.contains("refresh still running"), "{stderr}");
}

#[test]
fn search_refresh_strict_times_out_when_plugin_helper_keeps_stdout_open() {
    let temp = tempdir();
    let script = r#"#!/usr/bin/env python3
import json
import os
import subprocess

observed = "2026-07-01T12:00:00Z"
source_id = os.environ["CTX_HISTORY_SOURCE_ID"]
provider_key = os.environ["CTX_HISTORY_PROVIDER_KEY"]
source_format = os.environ["CTX_HISTORY_SOURCE_FORMAT"]
cursor_stream = os.environ["CTX_HISTORY_CURSOR_STREAM"]
records = [
    {"record_type": "manifest", "schema_version": "ctx-history-jsonl-v1"},
    {"record_type": "source", "source_id": source_id, "provider_key": provider_key, "source_format": source_format, "observed_at": observed, "cursor": {"after": {"stream": cursor_stream, "cursor": json.dumps({"seq": 1}), "observed_at": observed}}},
    {"record_type": "session", "source_id": source_id, "session_id": "hanging-session", "started_at": observed, "agent_type": "primary", "is_primary": True, "status": "completed"},
    {"record_type": "event", "source_id": source_id, "session_id": "hanging-session", "event_index": 0, "event_type": "message", "role": "assistant", "occurred_at": observed, "payload": {"text": "hanging plugin marker"}, "preview": "hanging plugin marker"},
]
for record in records:
    print(json.dumps(record, separators=(",", ":")), flush=True)
subprocess.Popen(["sh", "-c", "sleep 5"])
"#;
    let plugin = write_raw_history_source_plugin_with_options_and_timeout(
        &temp,
        "hanging",
        script,
        true,
        Some("auto"),
        1,
    );

    let started = Instant::now();
    let stderr = failure_stderr(
        ctx(&temp)
            .env("CTX_HISTORY_PLUGIN_PATH", &plugin.manifest_dir)
            .args([
                "search",
                "hanging plugin marker",
                "--provider",
                "custom",
                "--refresh",
                "strict",
                "--json",
            ]),
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "plugin timeout did not bound pipe draining: {stderr}"
    );
    assert!(
        stderr.contains("history source plugin hanging/default timed out after 1s"),
        "{stderr}"
    );
}

#[test]
fn search_refresh_auto_imports_fresh_work_despite_large_existing_catalog() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    let _ = json_output(ctx(&temp).args(["setup", "--json"]));
    let discovered = temp.path().join(".codex").join("sessions");
    copy_dir_all(&fixture, &discovered);

    let mut conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    let tx = conn.transaction().unwrap();
    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO catalog_sessions (
                    source_path, provider, source_format, source_root,
                    external_session_id, agent_type, file_size_bytes,
                    file_modified_at_ms, cataloged_at_ms, indexed_status,
                    indexed_at_ms, indexed_file_size_bytes,
                    indexed_file_modified_at_ms, metadata_json
                ) VALUES (?1, 'codex', 'codex_session_jsonl_tree', ?2, ?3,
                    'primary', 2, 1782259200000, 1782259200000, 'indexed',
                    1782259200000, 2, 1782259200000, '{}')",
            )
            .unwrap();
        for index in 0..10_000 {
            stmt.execute(params![
                format!("{}/seed-{index:05}.jsonl", discovered.display()),
                discovered.display().to_string(),
                format!("large-catalog-session-{index:05}"),
            ])
            .unwrap();
        }
    }
    tx.commit().unwrap();
    let search =
        json_output(ctx(&temp).args(["search", "onboarding", "--provider", "codex", "--json"]));
    assert_eq!(search["freshness"]["mode"], "auto");
    assert_eq!(search["freshness"]["status"], "completed");
    assert_eq!(search["freshness"]["source_count"], 1);
    assert_eq!(search["freshness"]["totals"]["imported_sessions"], 2);
    assert_search_provider_oracle(&search, "codex", "onboarding", 1, "message");

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["pending_catalog_sessions"], 0);
}

#[test]
fn search_refresh_auto_tail_imports_appended_codex_session_event() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    let discovered = temp.path().join(".codex").join("sessions");
    copy_dir_all(&fixture, &discovered);
    let root_session = discovered.join("2026/06/23/root.jsonl");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&root_session)
        .unwrap();
    for index in 0..250 {
        writeln!(
            file,
            "{}",
            json!({
                "timestamp": "2026-06-23T15:00:00.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": format!("tail-refresh-baseline-{index}")}]
                }
            })
        )
        .unwrap();
    }
    drop(file);

    let first =
        json_output(ctx(&temp).args(["search", "onboarding", "--provider", "codex", "--json"]));
    assert_search_provider_oracle(&first, "codex", "onboarding", 1, "message");

    let appended_needle = "tail-refresh-append-oracle";
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&root_session)
        .unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "timestamp": "2026-06-23T15:00:30.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": appended_needle}]
            }
        })
    )
    .unwrap();

    let started = Instant::now();
    let refreshed =
        json_output(ctx(&temp).args(["search", appended_needle, "--provider", "codex", "--json"]));
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "tail refresh took {elapsed:?}"
    );
    assert_eq!(refreshed["freshness"]["status"], "completed");
    assert_eq!(refreshed["freshness"]["totals"]["imported_events"], 1);
    assert!(
        refreshed["freshness"]["totals"]["skipped"]
            .as_u64()
            .unwrap()
            < 20,
        "tail refresh unexpectedly reprocessed old events: {}",
        refreshed["freshness"]["totals"]
    );
    assert_search_provider_oracle(&refreshed, "codex", appended_needle, 1, "message");

    let second_append_needle = "tail-refresh-second-append-oracle";
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&root_session)
        .unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "timestamp": "2026-06-23T15:00:31.000Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": second_append_needle}]
            }
        })
    )
    .unwrap();

    let second_refreshed = json_output(ctx(&temp).args([
        "search",
        second_append_needle,
        "--provider",
        "codex",
        "--json",
    ]));
    assert_eq!(second_refreshed["freshness"]["status"], "completed");
    assert_eq!(
        second_refreshed["freshness"]["totals"]["imported_events"],
        1
    );
    assert!(
        second_refreshed["freshness"]["totals"]["skipped"]
            .as_u64()
            .unwrap()
            < 20,
        "second tail refresh unexpectedly reprocessed old events: {}",
        second_refreshed["freshness"]["totals"]
    );
    assert_search_provider_oracle(
        &second_refreshed,
        "codex",
        second_append_needle,
        1,
        "message",
    );
}

#[test]
fn search_refresh_auto_imports_discovered_top_provider_sources() {
    for (cli_provider, stored_provider, install_fixture) in [
        (
            "claude",
            "claude",
            install_default_claude_fixture as fn(&TempDir, &str),
        ),
        ("pi", "pi", install_default_pi_fixture),
        ("cursor", "cursor", install_default_cursor_fixture),
        ("openclaw", "openclaw", install_default_openclaw_fixture),
        ("hermes", "hermes", install_default_hermes_fixture),
    ] {
        let temp = tempdir();
        let query = format!("{stored_provider}-default-refresh-oracle");
        install_fixture(&temp, &query);

        let search =
            json_output(ctx(&temp).args(["search", &query, "--provider", cli_provider, "--json"]));
        assert_eq!(search["freshness"]["mode"], "auto");
        assert_eq!(search["freshness"]["status"], "completed");
        assert_eq!(search["freshness"]["source_count"], 1);
        assert!(
            search["freshness"]["totals"]["imported_sessions"]
                .as_u64()
                .unwrap()
                >= 1
        );
        assert_search_provider_oracle(&search, stored_provider, &query, 1, "message");

        let started = Instant::now();
        let refreshed =
            json_output(ctx(&temp).args(["search", &query, "--provider", cli_provider, "--json"]));
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "{cli_provider} no-op refresh took {elapsed:?}"
        );
        assert_eq!(refreshed["freshness"]["mode"], "auto");
        assert_eq!(refreshed["freshness"]["status"], "completed");
        assert_eq!(refreshed["freshness"]["totals"]["imported_sessions"], 0);
        assert_eq!(refreshed["freshness"]["totals"]["imported_events"], 0);
        assert_search_provider_oracle(&refreshed, stored_provider, &query, 1, "message");
    }
}

#[test]
fn search_refresh_strict_json_emits_progress_on_stderr() {
    let temp = tempdir();
    let fixture = PathBuf::from(provider_history_fixture("codex-sessions"));
    copy_dir_all(&fixture, &temp.path().join(".codex").join("sessions"));

    let output = ctx(&temp)
        .args([
            "search",
            "onboarding",
            "--provider",
            "codex",
            "--refresh",
            "strict",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .clone();
    let stdout: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stdout["freshness"]["status"], "completed");
    assert_search_provider_oracle(&stdout, "codex", "onboarding", 1, "message");

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains(r#""type":"ctx_progress""#), "{stderr}");
    assert!(
        stderr.contains(r#""operation":"search-refresh""#),
        "{stderr}"
    );
}

#[test]
fn search_refresh_strict_fails_when_no_supported_refresh_source_exists() {
    let temp = tempdir();
    ctx(&temp)
        .args(["search", "anything", "--refresh", "strict", "--json"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "strict search refresh found no supported",
        ));
}

#[test]
fn search_rejects_unbounded_limit() {
    let temp = tempdir();
    ctx(&temp)
        .args(["search", "anything", "--limit", "201"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid value"));
}

#[test]
fn codex_cli_default_import_uses_catalog_state_for_incremental_catch_up() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-sessions");

    let first = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(first["resume"], false);
    assert_eq!(first["resume_mode"], "normal_scan");
    assert_eq!(first["totals"]["imported_sessions"], 2);
    assert_eq!(first["totals"]["imported_events"], 4);
    assert_eq!(first["totals"]["failed"], 0);

    let status = json_output(ctx(&temp).args(["status", "--json"]));
    assert_eq!(status["cataloged_sessions"], 2);
    assert_eq!(status["indexed_catalog_sessions"], 2);
    assert_eq!(status["pending_catalog_sessions"], 0);
    assert_eq!(status["failed_catalog_sessions"], 0);

    let second = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(second["resume"], false);
    assert_eq!(second["resume_mode"], "normal_scan");
    assert_eq!(second["totals"]["imported_sessions"], 0);
    assert_eq!(second["totals"]["imported_events"], 0);
    assert_eq!(second["totals"]["imported_edges"], 0);
    assert_eq!(second["totals"]["skipped"], 0);
    assert_eq!(second["totals"]["failed"], 0);
}

#[test]
fn codex_cli_provider_oracle_covers_retrieval_and_claimed_fidelity() {
    let temp = tempdir();
    let basic_fixture = provider_history_fixture("codex-sessions");
    let rich_fixture = provider_history_fixture("codex-rich-sessions");

    let basic = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &basic_fixture,
        "--json",
    ]));
    assert_eq!(basic["totals"]["imported_sessions"], 2);
    assert_eq!(basic["totals"]["imported_events"], 4);
    assert_eq!(basic["totals"]["imported_edges"], 1);

    let rich = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &rich_fixture,
        "--json",
    ]));
    assert_eq!(rich["totals"]["imported_sessions"], 1);
    assert_eq!(rich["totals"]["imported_events"], 1);

    let query = "setup flow";
    let search = json_output(ctx(&temp).args(["search", query, "--provider", "codex", "--json"]));
    assert_search_provider_oracle(&search, "codex", query, 1, "message");

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE provider = 'codex' AND fidelity = 'imported'"
        ),
        3
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.fidelity = 'imported'"
        ),
        5
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.event_type = 'message' AND e.role = 'user'"
        ),
        3
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.event_type = 'message' AND e.role = 'assistant'"
        ),
        2
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.event_type = 'tool_call'"
        ),
        0
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.event_type = 'tool_output'"
        ),
        0
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.event_type = 'command_output'"
        ),
        0
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE provider = 'codex' AND metadata_json LIKE '%model_provider%'"
        ),
        3
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'codex' AND e.payload_json LIKE '%token_usage%'"
        ),
        0
    );
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM session_edges"), 1);
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM artifacts"), 0);
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM files_touched"), 1);
}

#[test]
fn pi_cli_import_search_flow() {
    let temp = tempdir();
    let fixture = provider_history_fixture("pi-session.jsonl");

    let imported =
        json_output(ctx(&temp).args(["import", "--provider", "pi", "--path", &fixture, "--json"]));
    assert_eq!(imported["schema_version"], 1);
    assert_eq!(imported["sources"][0]["provider"], "pi");
    assert_eq!(imported["sources"][0]["source_format"], "pi_session_jsonl");
    assert_eq!(imported["totals"]["imported_sessions"], 1);
    assert_eq!(imported["totals"]["imported_events"], 6);

    let search =
        json_output(ctx(&temp).args(["search", "provider metadata", "--provider", "pi", "--json"]));
    assert_search_provider_oracle(&search, "pi", "provider metadata", 1, "message");

    let second = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "pi",
        "--path",
        &fixture,
        "--resume",
        "--json",
    ]));
    assert_eq!(second["resume"], true);
    assert_eq!(second["resume_mode"], "idempotent_rescan");
    assert_eq!(second["totals"]["imported_sessions"], 0);
    assert_eq!(second["totals"]["imported_events"], 0);
    assert_eq!(second["totals"]["skipped"].as_u64().unwrap(), 7);

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE provider = 'pi' AND fidelity = 'imported'"
        ),
        1
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'pi' AND e.fidelity = 'imported'"
        ),
        6
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'pi' AND e.event_type = 'message' AND e.role = 'user'"
        ),
        1
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'pi' AND e.event_type = 'message' AND e.role = 'assistant'"
        ),
        1
    );
    assert_eq!(
        sqlite_count(
            &conn,
            "SELECT COUNT(*) FROM events e JOIN sessions s ON e.session_id = s.id WHERE s.provider = 'pi' AND json_type(e.metadata_json, '$.metadata.model') = 'text'"
        ),
        2
    );
    assert_eq!(sqlite_count(&conn, "SELECT COUNT(*) FROM session_edges"), 0);
}

#[test]
fn native_provider_cli_flow_imports_new_supported_provider_paths() {
    for (cli_provider, stored_provider, expected_format, fixture) in [
        (
            "claude",
            "claude",
            "claude_projects_jsonl_tree",
            write_native_claude_fixture as fn(&TempDir, &str) -> String,
        ),
        (
            "opencode",
            "opencode",
            "opencode_sqlite",
            write_native_opencode_fixture,
        ),
        (
            "gemini",
            "gemini",
            "gemini_cli_chat_recording_jsonl",
            write_native_gemini_fixture,
        ),
        (
            "cursor",
            "cursor",
            "cursor_agent_transcript_jsonl_tree",
            write_native_cursor_fixture,
        ),
        (
            "copilot-cli",
            "copilot_cli",
            "copilot_cli_session_events_jsonl",
            write_native_copilot_fixture,
        ),
        (
            "factory-ai-droid",
            "factory_ai_droid",
            "factory_ai_droid_sessions_jsonl",
            write_native_factory_droid_fixture,
        ),
        (
            "openclaw",
            "openclaw",
            "openclaw_session_jsonl_tree",
            write_native_openclaw_fixture,
        ),
        (
            "hermes",
            "hermes",
            "hermes_state_sqlite",
            write_native_hermes_fixture,
        ),
        (
            "nanoclaw",
            "nanoclaw",
            "nanoclaw_project",
            write_native_nanoclaw_fixture,
        ),
        (
            "astrbot",
            "astrbot",
            "astrbot_data_v4_sqlite",
            write_native_astrbot_fixture,
        ),
    ] {
        let temp = tempdir();
        let query = format!("{stored_provider}-native-cli-oracle");
        let path = fixture(&temp, &query);

        let imported = json_output(ctx(&temp).args([
            "import",
            "--provider",
            cli_provider,
            "--path",
            &path,
            "--json",
        ]));
        assert_eq!(imported["schema_version"], 1);
        assert_eq!(imported["sources"][0]["provider"], stored_provider);
        assert_eq!(imported["sources"][0]["source_format"], expected_format);
        assert_eq!(imported["totals"]["failed"], 0);
        assert!(imported["totals"]["imported_sessions"].as_u64().unwrap() >= 1);
        assert!(imported["totals"]["imported_events"].as_u64().unwrap() >= 1);

        let search =
            json_output(ctx(&temp).args(["search", &query, "--provider", cli_provider, "--json"]));
        assert_search_provider_oracle(&search, stored_provider, &query, 1, "message");
        let result = &search["results"].as_array().unwrap()[0];
        let ctx_event_id = result["ctx_event_id"].as_str().unwrap();
        let ctx_session_id = result["ctx_session_id"].as_str().unwrap();

        let show_event =
            json_output(ctx(&temp).args(["show", "event", ctx_event_id, "--format", "json"]));
        assert_eq!(show_event["event"]["provider"], stored_provider);
        assert!(show_event["event"]["source"]["source_format"].is_string());
        assert!(show_event["event"]["source"]["path"].is_string());
        assert!(show_event["event"]["cursor"].is_string());

        let locate_event =
            json_output(ctx(&temp).args(["locate", "event", ctx_event_id, "--json"]));
        assert_eq!(locate_event["provider"], stored_provider);
        assert_eq!(locate_event["ctx_session_id"], ctx_session_id);
        assert!(locate_event["source"]["source_format"].is_string());
        assert!(locate_event["source"]["path"].is_string());
        assert!(locate_event["cursor"].is_string());

        let status = json_output(ctx(&temp).args(["status", "--json"]));
        assert!(status["indexed_items"].as_u64().unwrap() >= 2);
        assert!(status["indexed_sources"].as_u64().unwrap() >= 1);

        let doctor = json_output(ctx(&temp).args(["doctor", "--json"]));
        assert_eq!(doctor["ok"], true);

        let second = json_output(ctx(&temp).args([
            "import",
            "--provider",
            cli_provider,
            "--path",
            &path,
            "--json",
        ]));
        assert_eq!(second["totals"]["failed"], 0);
        assert_eq!(second["totals"]["imported_events"], 0);
    }
}

#[test]
fn opencode_old_cursor_triggers_rescan_that_imports_message_part_history() {
    let temp = tempdir();
    let path = write_native_opencode_fixture(&temp, "opencode-cursor-migration-oracle");

    // Simulate a store written by the old adapter: the same database was
    // recorded as fully indexed, but the sync cursor still has the old
    // pre-message/part format and no events were imported.
    let first = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "opencode",
        "--path",
        &path,
        "--json",
    ]));
    assert_eq!(first["totals"]["failed"], 0);
    assert!(first["totals"]["imported_events"].as_u64().unwrap() >= 1);

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    let updated = conn
        .execute(
            "UPDATE sync_cursors SET cursor = 'session_message:ses_old:seq:1'
             WHERE stream LIKE 'provider:opencode:%'",
            [],
        )
        .unwrap();
    assert_eq!(updated, 1);
    drop(conn);

    // Old-format cursor forces a full rescan even though the database file is
    // unchanged and the import file manifest says it is already indexed; the
    // rescan is idempotent (everything dedupes to skipped).
    let second = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "opencode",
        "--path",
        &path,
        "--json",
    ]));
    assert_eq!(second["totals"]["failed"], 0);
    assert_eq!(second["totals"]["imported_events"], 0);
    assert!(second["totals"]["skipped"].as_u64().unwrap() >= 1);

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    let cursor: String = conn
        .query_row(
            "SELECT cursor FROM sync_cursors WHERE stream LIKE 'provider:opencode:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        cursor.starts_with("opencode-v2:"),
        "cursor not migrated: {cursor}"
    );
    drop(conn);

    // With a v2 cursor and an unchanged database the manifest short-circuits
    // again: nothing is re-read.
    let third = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "opencode",
        "--path",
        &path,
        "--json",
    ]));
    assert_eq!(third["totals"]["failed"], 0);
    assert_eq!(third["totals"]["imported_events"], 0);
    assert_eq!(third["totals"]["skipped"], 0);
}

#[test]
fn personal_agent_provider_imports_are_idempotent_and_incremental() {
    for (cli_provider, stored_provider, fixture, append_event) in [
        (
            "openclaw",
            "openclaw",
            write_native_openclaw_fixture as fn(&TempDir, &str) -> String,
            append_native_openclaw_event as fn(&str, &str),
        ),
        (
            "hermes",
            "hermes",
            write_native_hermes_fixture,
            append_native_hermes_event,
        ),
        (
            "nanoclaw",
            "nanoclaw",
            write_native_nanoclaw_fixture,
            append_native_nanoclaw_event,
        ),
        (
            "astrbot",
            "astrbot",
            write_native_astrbot_fixture,
            append_native_astrbot_event,
        ),
    ] {
        let temp = tempdir();
        let initial_query = format!("{stored_provider}-incremental-initial-oracle");
        let incremental_query = format!("{stored_provider}-incremental-next-oracle");
        let path = fixture(&temp, &initial_query);

        let first = json_output(ctx(&temp).args([
            "import",
            "--provider",
            cli_provider,
            "--path",
            &path,
            "--json",
        ]));
        assert_eq!(first["totals"]["failed"], 0);
        assert!(first["totals"]["imported_events"].as_u64().unwrap() >= 1);

        let second = json_output(ctx(&temp).args([
            "import",
            "--provider",
            cli_provider,
            "--path",
            &path,
            "--json",
        ]));
        assert_eq!(second["totals"]["failed"], 0);
        assert_eq!(second["totals"]["imported_events"], 0);

        append_event(&path, &incremental_query);
        let third = json_output(ctx(&temp).args([
            "import",
            "--provider",
            cli_provider,
            "--path",
            &path,
            "--json",
        ]));
        assert_eq!(third["totals"]["failed"], 0);
        assert!(third["totals"]["imported_events"].as_u64().unwrap() >= 1);

        let search = json_output(ctx(&temp).args([
            "search",
            &incremental_query,
            "--provider",
            cli_provider,
            "--json",
        ]));
        assert_search_provider_oracle(&search, stored_provider, &incremental_query, 1, "message");
    }
}

fn install_default_claude_fixture(temp: &TempDir, query: &str) {
    let source = PathBuf::from(write_native_claude_fixture(temp, query));
    copy_dir_all(&source, &temp.path().join(".claude").join("projects"));
}

fn install_default_pi_fixture(temp: &TempDir, query: &str) {
    let root = temp.path().join(".pi");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("sessions.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "type": "session",
                "version": 3,
                "id": "pi-default-refresh",
                "timestamp": "2026-06-24T12:00:00.000Z",
                "cwd": "/workspace"
            }),
            json!({
                "type": "message",
                "id": "pi-default-refresh-user",
                "timestamp": "2026-06-24T12:00:01.000Z",
                "message": {"role": "user", "content": query}
            })
        ),
    )
    .unwrap();
}

fn install_default_cursor_fixture(temp: &TempDir, query: &str) {
    let source = PathBuf::from(write_native_cursor_fixture(temp, query));
    copy_dir_all(&source, &temp.path().join(".cursor").join("projects"));
}

fn install_default_openclaw_fixture(temp: &TempDir, query: &str) {
    let source = PathBuf::from(write_native_openclaw_fixture(temp, query));
    copy_dir_all(&source, &temp.path().join(".openclaw"));
}

fn install_default_hermes_fixture(temp: &TempDir, query: &str) {
    let source = PathBuf::from(write_native_hermes_fixture(temp, query));
    let target = temp.path().join(".hermes");
    fs::create_dir_all(&target).unwrap();
    fs::copy(source, target.join("state.db")).unwrap();
}

fn install_default_astrbot_fixture(temp: &TempDir, query: &str) {
    let source = PathBuf::from(write_native_astrbot_fixture(temp, query));
    let target = temp.path().join(".astrbot/data");
    fs::create_dir_all(&target).unwrap();
    fs::copy(source, target.join("data_v4.db")).unwrap();
}

fn write_native_claude_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp.path().join("native-claude/projects/-workspace");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("claude-cli-native.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "sessionId": "claude-cli-native",
                "timestamp": "2026-06-24T12:00:00Z",
                "cwd": "/workspace",
                "version": "test",
                "type": "user",
                "message": {"role": "user", "content": [{"type": "text", "text": query}]},
                "uuid": "claude-cli-native-user"
            }),
            json!({
                "sessionId": "claude-cli-native",
                "timestamp": "2026-06-24T12:00:01Z",
                "cwd": "/workspace",
                "version": "test",
                "type": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "native import ok"}]},
                "uuid": "claude-cli-native-assistant"
            })
        ),
    )
    .unwrap();
    temp.path()
        .join("native-claude/projects")
        .to_str()
        .unwrap()
        .to_owned()
}

fn write_native_opencode_fixture(temp: &TempDir, query: &str) -> String {
    let path = temp.path().join("native-opencode.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "create table session (
            id text primary key,
            project_id text not null,
            parent_id text,
            slug text not null,
            directory text not null,
            title text not null,
            version text not null,
            share_url text,
            summary_additions integer,
            summary_deletions integer,
            summary_files integer,
            summary_diffs text,
            revert text,
            permission text,
            time_created integer not null,
            time_updated integer not null,
            time_compacting integer,
            time_archived integer,
            workspace_id text
        );
        create table message (
            id text primary key,
            session_id text not null,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );
        create table part (
            id text primary key,
            message_id text not null,
            session_id text not null,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );",
    )
    .unwrap();
    conn.execute(
        "insert into session (
            id, project_id, parent_id, slug, directory, title, version, permission,
            time_created, time_updated
        ) values (?1, 'project-1', null, 'native', '/workspace', 'native', '0.8.0',
            'default', 1782259200000, 1782259200000)",
        ["opencode-cli-native"],
    )
    .unwrap();
    conn.execute(
        "insert into message values (?1, ?2, 1782259200000, 1782259200000, ?3)",
        [
            "opencode-cli-native-user",
            "opencode-cli-native",
            r#"{"role":"user","time":{"created":1782259200000}}"#,
        ],
    )
    .unwrap();
    conn.execute(
        "insert into part values (?1, ?2, ?3, 1782259200000, 1782259200000, ?4)",
        [
            "opencode-cli-native-part",
            "opencode-cli-native-user",
            "opencode-cli-native",
            &format!(r#"{{"type":"text","text":"{query}"}}"#),
        ],
    )
    .unwrap();
    path.to_str().unwrap().to_owned()
}

fn write_native_gemini_fixture(temp: &TempDir, query: &str) -> String {
    let chats = temp.path().join("native-gemini/.gemini/tmp/project/chats");
    fs::create_dir_all(&chats).unwrap();
    fs::write(
        chats.join("session-native.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "sessionId": "gemini-cli-native",
                "startTime": "2026-06-24T12:00:00Z",
                "kind": "main",
                "directories": ["/workspace"]
            }),
            json!({
                "id": "gemini-cli-native-user",
                "timestamp": "2026-06-24T12:00:01Z",
                "type": "user",
                "content": query
            })
        ),
    )
    .unwrap();
    temp.path()
        .join("native-gemini/.gemini")
        .to_str()
        .unwrap()
        .to_owned()
}

fn write_native_cursor_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp
        .path()
        .join("native-cursor/projects/sanitized-workspace/agent-transcripts/cursor-cli-native");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("cursor-cli-native.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "timestamp": "2026-06-24T12:00:00Z",
                "role": "user",
                "message": {"role": "user", "content": [{"type": "text", "text": query}]}
            }),
            json!({
                "timestamp": "2026-06-24T12:00:01Z",
                "role": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "native import ok"}]}
            })
        ),
    )
    .unwrap();
    temp.path()
        .join("native-cursor/projects")
        .to_str()
        .unwrap()
        .to_owned()
}

fn write_native_copilot_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp
        .path()
        .join("native-copilot/session-state/copilot-cli-native");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("events.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "id": "copilot-cli-native-start",
                "timestamp": "2026-06-24T12:00:00Z",
                "type": "session.start",
                "data": {
                    "sessionId": "copilot-cli-native",
                    "startTime": "2026-06-24T12:00:00Z",
                    "selectedModel": "gpt-5-mini",
                    "context": {"cwd": "/workspace"}
                }
            }),
            json!({
                "id": "copilot-cli-native-user",
                "timestamp": "2026-06-24T12:00:01Z",
                "type": "user.message",
                "data": {"content": query}
            })
        ),
    )
    .unwrap();
    temp.path()
        .join("native-copilot/session-state")
        .to_str()
        .unwrap()
        .to_owned()
}

fn write_native_factory_droid_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp.path().join("native-droid/sessions/project");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("droid-cli-native.jsonl"),
        format!(
            "{}\n{}\n",
            json!({
                "type": "session_start",
                "sessionId": "droid-cli-native",
                "timestamp": "2026-06-24T12:00:00Z",
                "cwd": "/workspace",
                "model": "factory/droid"
            }),
            json!({
                "type": "message",
                "id": "droid-cli-native-user",
                "timestamp": "2026-06-24T12:00:01Z",
                "role": "user",
                "content": [{"type": "text", "text": query}]
            })
        ),
    )
    .unwrap();
    temp.path()
        .join("native-droid/sessions")
        .to_str()
        .unwrap()
        .to_owned()
}

fn write_native_openclaw_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp.path().join("native-openclaw");
    let sessions = root.join("agents/personal-agent/sessions");
    fs::create_dir_all(&sessions).unwrap();
    fs::write(
        sessions.join("sessions.json"),
        serde_json::to_string(&json!({
            "openclaw-cli-native": {
                "sessionId": "openclaw-cli-native",
                "sessionFile": sessions.join("openclaw-cli-native.jsonl"),
                "sessionStartedAt": "2026-06-24T12:00:00Z",
                "modelProvider": "openai",
                "model": "gpt-5-mini",
                "lastChannel": "telegram"
            }
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        sessions.join("openclaw-cli-native.jsonl"),
        format!(
            "{}\n{}\n{}\n",
            json!({
                "type": "session",
                "version": 1,
                "id": "openclaw-cli-native",
                "timestamp": "2026-06-24T12:00:00Z",
                "cwd": "/workspace"
            }),
            json!({
                "type": "message",
                "id": "openclaw-cli-native-user",
                "timestamp": "2026-06-24T12:00:01Z",
                "message": {"role": "user", "content": query}
            }),
            json!({
                "type": "message",
                "id": "openclaw-cli-native-assistant",
                "parentId": "openclaw-cli-native-user",
                "timestamp": "2026-06-24T12:00:02Z",
                "message": {"role": "assistant", "content": "native import ok"}
            })
        ),
    )
    .unwrap();
    root.to_str().unwrap().to_owned()
}

fn write_native_hermes_fixture(temp: &TempDir, query: &str) -> String {
    let path = temp.path().join("native-hermes-state.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "create table sessions (
            id text primary key,
            source text not null,
            model text,
            model_config text,
            parent_session_id text,
            started_at real not null,
            ended_at real,
            message_count integer default 0,
            tool_call_count integer default 0,
            input_tokens integer default 0,
            output_tokens integer default 0,
            cwd text,
            title text,
            archived integer default 0
        );
        create table messages (
            id integer primary key autoincrement,
            session_id text not null,
            role text not null,
            content text,
            tool_calls text,
            tool_call_id text,
            tool_name text,
            timestamp real not null,
            active integer not null default 1,
            compacted integer not null default 0
        );",
    )
    .unwrap();
    conn.execute(
        "insert into sessions (
            id, source, model, model_config, started_at, message_count, cwd, title
        ) values (?1, 'acp', 'gpt-5-mini', ?2, 1782259200.0, 2, '/workspace', 'native hermes')",
        [
            "hermes-cli-native",
            r#"{"cwd":"/workspace","provider":"openai"}"#,
        ],
    )
    .unwrap();
    conn.execute(
        "insert into messages (session_id, role, content, timestamp) values (?1, 'user', ?2, 1782259201.0)",
        ["hermes-cli-native", query],
    )
    .unwrap();
    conn.execute(
        "insert into messages (session_id, role, content, timestamp) values (?1, 'assistant', 'native import ok', 1782259202.0)",
        ["hermes-cli-native"],
    )
    .unwrap();
    path.to_str().unwrap().to_owned()
}

fn write_native_nanoclaw_fixture(temp: &TempDir, query: &str) -> String {
    let root = temp.path().join("native-nanoclaw");
    let data = root.join("data");
    let session_dir = data.join("v2-sessions/ag-1/session-1");
    fs::create_dir_all(&session_dir).unwrap();
    let central = Connection::open(data.join("v2.db")).unwrap();
    central
        .execute_batch(
            "create table agent_groups (
                id text primary key,
                name text,
                folder text,
                agent_provider text
            );
            create table messaging_groups (
                id text primary key,
                channel_type text,
                platform_id text,
                instance text,
                name text
            );
            create table sessions (
                id text primary key,
                agent_group_id text not null,
                messaging_group_id text,
                thread_id text,
                agent_provider text,
                status text,
                container_status text,
                last_active integer,
                created_at integer
            );",
        )
        .unwrap();
    central
        .execute(
            "insert into agent_groups values ('ag-1', 'Personal', '/workspace', 'codex')",
            [],
        )
        .unwrap();
    central
        .execute(
            "insert into messaging_groups values ('mg-1', 'telegram', 'chat-1', 'default', 'DM')",
            [],
        )
        .unwrap();
    central
        .execute(
            "insert into sessions values (
                'session-1', 'ag-1', 'mg-1', 'thread-1', 'codex', 'active',
                'running', 1782259202000, 1782259200000
            )",
            [],
        )
        .unwrap();
    let inbound = Connection::open(session_dir.join("inbound.db")).unwrap();
    inbound
        .execute_batch(
            "create table messages_in (
                id text primary key,
                seq integer,
                kind text,
                timestamp integer,
                status text,
                trigger text,
                platform_id text,
                channel_type text,
                thread_id text,
                content text,
                source_session_id text,
                on_wake integer
            );",
        )
        .unwrap();
    inbound
        .execute(
            "insert into messages_in values (
                'in-1', 1, 'chat', 1782259201000, 'done', 'message',
                'chat-1', 'telegram', 'thread-1', ?1, null, 0
            )",
            [json!({"text": query}).to_string()],
        )
        .unwrap();
    let outbound = Connection::open(session_dir.join("outbound.db")).unwrap();
    outbound
        .execute_batch(
            "create table messages_out (
                id text primary key,
                seq integer,
                in_reply_to text,
                timestamp integer,
                kind text,
                platform_id text,
                channel_type text,
                thread_id text,
                content text
            );",
        )
        .unwrap();
    outbound
        .execute(
            "insert into messages_out values (
                'out-1', 2, 'in-1', 1782259202000, 'chat',
                'chat-1', 'telegram', 'thread-1', ?1
            )",
            [json!({"text": "native import ok"}).to_string()],
        )
        .unwrap();
    root.to_str().unwrap().to_owned()
}

fn write_native_astrbot_fixture(temp: &TempDir, query: &str) -> String {
    let data = temp.path().join("native-astrbot/data");
    fs::create_dir_all(&data).unwrap();
    let path = data.join("data_v4.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "create table conversations (
            id integer primary key,
            inner_conversation_id text,
            conversation_id text,
            platform_id text,
            user_id text,
            content text not null,
            title text,
            persona_id text,
            token_usage text,
            created_at integer,
            updated_at integer
        );
        create table preferences (
            scope text,
            key text,
            value text
        );
        create table platform_message_history (
            id integer primary key,
            platform_id text,
            user_id text,
            sender_id text,
            sender_name text,
            content text,
            llm_checkpoint_id text,
            created_at integer
        );",
    )
    .unwrap();
    conn.execute(
        "insert into conversations values (
            1, 'umo-1', 'conv-1', 'webchat', 'user-1', ?1, 'native astrbot',
            'default', ?2, 1782259200000, 1782259202000
        )",
        [
            json!([
                {"role": "user", "content": query},
                {"type": "_checkpoint", "id": "checkpoint-1"},
                {"role": "assistant", "content": "native import ok"}
            ])
            .to_string(),
            json!({"prompt": 1, "completion": 1}).to_string(),
        ],
    )
    .unwrap();
    conn.execute(
        "insert into preferences values ('umo', 'sel_conv_id', 'conv-1')",
        [],
    )
    .unwrap();
    conn.execute(
        "insert into platform_message_history values (
            1, 'webchat', 'user-1', 'user-1', 'User', ?1, 'checkpoint-1', 1782259201000
        )",
        [json!({"text": query}).to_string()],
    )
    .unwrap();
    path.to_str().unwrap().to_owned()
}

fn append_native_openclaw_event(path: &str, query: &str) {
    let transcript =
        Path::new(path).join("agents/personal-agent/sessions/openclaw-cli-native.jsonl");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(transcript)
        .unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "type": "message",
            "id": "openclaw-cli-native-incremental",
            "parentId": "openclaw-cli-native-assistant",
            "timestamp": "2026-06-24T12:00:03Z",
            "message": {"role": "user", "content": query}
        })
    )
    .unwrap();
}

fn append_native_hermes_event(path: &str, query: &str) {
    let conn = Connection::open(path).unwrap();
    conn.execute(
        "insert into messages (session_id, role, content, timestamp) values (?1, 'user', ?2, 1782259203.0)",
        ["hermes-cli-native", query],
    )
    .unwrap();
}

fn append_native_nanoclaw_event(path: &str, query: &str) {
    let conn = Connection::open(
        Path::new(path)
            .join("data/v2-sessions/ag-1/session-1")
            .join("inbound.db"),
    )
    .unwrap();
    conn.execute(
        "insert into messages_in values (
            'in-2', 1, 'chat', 1782259203000, 'done', 'message',
            'chat-1', 'telegram', 'thread-1', ?1, null, 0
        )",
        [json!({"text": query}).to_string()],
    )
    .unwrap();
}

fn append_native_astrbot_event(path: &str, query: &str) {
    let conn = Connection::open(path).unwrap();
    let content: String = conn
        .query_row(
            "select content from conversations where id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut content: Value = serde_json::from_str(&content).unwrap();
    content
        .as_array_mut()
        .unwrap()
        .push(json!({"role": "assistant", "content": query}));
    conn.execute(
        "update conversations set content = ?1, updated_at = 1782259203000 where id = 1",
        [content.to_string()],
    )
    .unwrap();
}

#[test]
fn openclaw_import_accepts_explicit_session_jsonl_file() {
    let temp = tempdir();
    let query = "openclaw-explicit-file-oracle";
    let path = temp.path().join("openclaw-single-session.jsonl");
    fs::write(
        &path,
        format!(
            "{}\n{}\n",
            json!({
                "type": "session",
                "id": "openclaw-single-session",
                "timestamp": "2026-06-24T12:00:00Z"
            }),
            json!({
                "type": "message",
                "id": "openclaw-single-user",
                "timestamp": "2026-06-24T12:00:01Z",
                "message": {"role": "user", "content": query}
            })
        ),
    )
    .unwrap();

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "openclaw",
        "--path",
        path.to_str().unwrap(),
        "--json",
    ]));
    assert_eq!(imported["totals"]["failed"], 0);
    assert_eq!(imported["totals"]["imported_sources"], 1);

    let search =
        json_output(ctx(&temp).args(["search", query, "--provider", "openclaw", "--json"]));
    assert_search_provider_oracle(&search, "openclaw", query, 1, "message");
}

#[test]
fn nanoclaw_import_tolerates_partial_auxiliary_tables() {
    let temp = tempdir();
    let query = "nanoclaw-partial-auxiliary-schema-oracle";
    let path = write_native_nanoclaw_fixture(&temp, query);
    let conn = Connection::open(Path::new(&path).join("data/v2.db")).unwrap();
    conn.execute_batch(
        "drop table agent_groups;
         create table agent_groups (id text primary key);
         insert into agent_groups values ('ag-1');
         drop table messaging_groups;
         create table messaging_groups (id text primary key);
         insert into messaging_groups values ('mg-1');",
    )
    .unwrap();

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "nanoclaw",
        "--path",
        &path,
        "--json",
    ]));
    assert_eq!(imported["totals"]["failed"], 0);
    assert_eq!(imported["totals"]["imported_sources"], 1);

    let search =
        json_output(ctx(&temp).args(["search", query, "--provider", "nanoclaw", "--json"]));
    assert_search_provider_oracle(&search, "nanoclaw", query, 1, "message");
}

#[test]
fn personal_agent_sqlite_imports_report_corrupt_databases() {
    for (provider, path) in [
        ("hermes", "corrupt-hermes-state.db"),
        ("astrbot", "corrupt-astrbot-data_v4.db"),
    ] {
        let temp = tempdir();
        let db_path = temp.path().join(path);
        fs::write(&db_path, b"not sqlite").unwrap();
        let output = ctx(&temp)
            .args([
                "import",
                "--provider",
                provider,
                "--path",
                db_path.to_str().unwrap(),
                "--json",
            ])
            .assert()
            .failure()
            .get_output()
            .stderr
            .clone();
        let stderr = String::from_utf8(output).unwrap();
        assert!(stderr.contains("not a database"), "{stderr}");
    }

    let temp = tempdir();
    let root = temp.path().join("corrupt-nanoclaw");
    fs::create_dir_all(root.join("data/v2-sessions")).unwrap();
    fs::write(root.join("data/v2.db"), b"not sqlite").unwrap();
    let output = ctx(&temp)
        .args([
            "import",
            "--provider",
            "nanoclaw",
            "--path",
            root.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .failure()
        .get_output()
        .stderr
        .clone();
    let stderr = String::from_utf8(output).unwrap();
    assert!(stderr.contains("not a database"), "{stderr}");
}

#[test]
fn native_provider_cli_requires_existing_history_or_explicit_path() {
    for (cli_provider, expected_blocker) in [
        ("claude", "no importable claude history found"),
        ("opencode", "no importable opencode history found"),
        ("antigravity", "no importable antigravity history found"),
        ("gemini", "no importable gemini history found"),
        ("cursor", "no importable cursor history found"),
        ("copilot-cli", "no importable copilot_cli history found"),
        (
            "factory-ai-droid",
            "no importable factory_ai_droid history found",
        ),
        ("openclaw", "no importable openclaw history found"),
        ("hermes", "no importable hermes history found"),
        ("nanoclaw", "no importable nanoclaw history found"),
        ("astrbot", "no importable astrbot history found"),
    ] {
        let temp = tempdir();
        let stderr =
            failure_stderr(ctx(&temp).args(["import", "--provider", cli_provider, "--json"]));

        assert!(stderr.contains(expected_blocker), "{stderr}");
        assert!(stderr.contains("use `ctx sources`"), "{stderr}");
        if cli_provider == "nanoclaw" {
            assert!(
                stderr.contains("no default paths are registered for this provider"),
                "{stderr}"
            );
        } else {
            assert!(stderr.contains("checked paths:"), "{stderr}");
            assert!(stderr.contains(temp.path().to_str().unwrap()), "{stderr}");
        }
    }
}

#[test]
fn antigravity_cli_imports_native_transcript_tree() {
    let temp = tempdir();
    let fixture = provider_history_fixture("antigravity/v1/brain");

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "antigravity",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(imported["schema_version"], 1);
    assert_eq!(imported["sources"][0]["provider"], "antigravity");
    assert_eq!(
        imported["sources"][0]["source_format"],
        "antigravity_cli_transcript_jsonl_tree"
    );
    assert_eq!(imported["totals"]["imported_sessions"], 4);
    assert_eq!(imported["totals"]["imported_events"], 11);
    assert_eq!(imported["totals"]["failed"], 1);

    let search = json_output(ctx(&temp).args([
        "search",
        "write_to_file",
        "--provider",
        "antigravity",
        "--json",
    ]));
    assert_search_provider_oracle(&search, "antigravity", "write_to_file", 1, "tool_call");
}

#[test]
fn codex_cli_reports_malformed_partial_import_progress() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-malformed-session.jsonl");

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));
    assert_eq!(imported["schema_version"], 1);
    assert_eq!(imported["totals"]["imported_sessions"], 1);
    assert_eq!(imported["totals"]["imported_events"], 2);
    assert_eq!(imported["totals"]["failed"], 1);
    assert_eq!(imported["sources"][0]["failed"], 1);

    let search = json_output(ctx(&temp).args(["search", "after malformed", "--json"]));
    assert!(!search["results"].as_array().unwrap().is_empty());
}

#[test]
fn pi_cli_reports_malformed_partial_and_schema_failures() {
    let temp = tempdir();
    let fixture = provider_history_fixture("pi-malformed-partial.jsonl");

    let imported =
        json_output(ctx(&temp).args(["import", "--provider", "pi", "--path", &fixture, "--json"]));
    assert_eq!(imported["schema_version"], 1);
    assert_eq!(imported["totals"]["imported_sessions"], 1);
    assert_eq!(imported["totals"]["imported_events"], 2);
    assert_eq!(imported["totals"]["failed"], 2);
    assert_eq!(imported["sources"][0]["failed"], 2);
    assert_eq!(
        imported["sources"][0]["failures"].as_array().unwrap().len(),
        2
    );

    let query = "after malformed line";
    let search = json_output(ctx(&temp).args(["search", query, "--provider", "pi", "--json"]));
    assert_search_provider_oracle(&search, "pi", query, 1, "message");
}

#[test]
fn human_search_reports_no_results() {
    let temp = tempdir();
    let fresh = ctx(&temp)
        .args(["search", "definitely-no-results-here"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let fresh = String::from_utf8(fresh).unwrap();
    assert!(fresh.contains("no results for definitely-no-results-here"));
    assert!(fresh.contains("next: ctx import --all"));

    let fixture = provider_history_fixture("codex-sessions");
    ctx(&temp)
        .args([
            "import",
            "--provider",
            "codex",
            "--path",
            &fixture,
            "--progress",
            "none",
        ])
        .assert()
        .success();
    let indexed = ctx(&temp)
        .args(["search", "definitely-no-results-here"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let indexed = String::from_utf8(indexed).unwrap();
    assert!(indexed.contains("no results for definitely-no-results-here"));
    assert!(indexed.contains("next: try broader terms with ctx search --term \"<term>\""));

    let term_only = ctx(&temp)
        .args(["search", "--term", "term-only-no-results"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let term_only = String::from_utf8(term_only).unwrap();
    assert!(term_only.contains("no results for --term term-only-no-results"));
}

#[test]
fn search_requires_query_term_or_file_before_refreshing() {
    let temp = tempdir();
    let stderr = failure_stderr(ctx(&temp).args(["search", "--provider", "codex"]));
    assert!(
        stderr.contains("search needs a query, --term, or --file"),
        "{stderr}"
    );
    assert!(
        stderr.contains("ctx search \"failed migration\""),
        "{stderr}"
    );
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "invalid search should fail before creating the ctx store"
    );

    let punctuation = failure_stderr(ctx(&temp).args(["search", "!!!"]));
    assert!(
        punctuation.contains("search needs a query, --term, or --file"),
        "{punctuation}"
    );
    let hyphen_only = failure_stderr(ctx(&temp).args(["search", "--", "---"]));
    assert!(
        hyphen_only.contains("search needs a query, --term, or --file"),
        "{hyphen_only}"
    );
    let underscore_term = failure_stderr(ctx(&temp).args(["search", "--term", "___"]));
    assert!(
        underscore_term.contains("search needs a query, --term, or --file"),
        "{underscore_term}"
    );
}

#[test]
fn search_refresh_off_requires_existing_store_without_creating_one() {
    let temp = tempdir();
    let stderr = failure_stderr(ctx(&temp).args(["search", "anything", "--refresh", "off"]));

    assert!(stderr.contains("ctx store is not initialized"), "{stderr}");
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "refresh-off search should not create the ctx store"
    );
}

#[test]
fn file_only_search_returns_touched_file_matches() {
    let temp = tempdir();
    let fixture = provider_history_fixture("codex-rich-sessions");
    json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &fixture,
        "--json",
    ]));

    let search = json_output(ctx(&temp).args(["search", "--file", "src/main.rs", "--json"]));
    assert_eq!(search["query"], "");
    let results = search["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0]["why_matched"]
        .as_array()
        .unwrap()
        .iter()
        .any(|reason| reason == "file_touched"));
    assert!(results[0]["citations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|citation| citation["item_type"] == "file" && citation["label"] == "file touched"));
}

#[test]
fn pi_cli_rejects_directory_import_path() {
    let temp = tempdir();
    let path = temp.path().join("pi-sessions-dir");
    fs::create_dir_all(&path).unwrap();

    ctx(&temp)
        .args([
            "import",
            "--provider",
            "pi",
            "--path",
            path.to_str().unwrap(),
            "--json",
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("no importable pi history files")
                .and(predicate::str::contains(path.to_str().unwrap())),
        );
}

#[test]
fn pi_cli_rejects_wrong_file_import_path() {
    let temp = tempdir();
    let path = temp.path().join("pi-session.txt");
    fs::write(&path, "{}\n").unwrap();

    ctx(&temp)
        .args([
            "import",
            "--provider",
            "pi",
            "--path",
            path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("no importable pi history files")
                .and(predicate::str::contains(path.to_str().unwrap())),
        );
}

#[test]
fn import_rejects_nonexistent_path() {
    let temp = tempdir();
    let path = temp.path().join("missing-codex-history");
    let path = path.to_str().unwrap();

    ctx(&temp)
        .args(["import", "--provider", "codex", "--path", path])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("import path does not exist")
                .and(predicate::str::contains(path)),
        );
}

#[test]
fn import_path_requires_provider_before_opening_store() {
    let temp = tempdir();
    let path = temp.path().join("missing-codex-history");
    let path = path.to_str().unwrap();

    ctx(&temp)
        .args(["import", "--path", path])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "ctx import --path requires --provider",
        ));
    assert!(
        !temp.path().join("work.sqlite").exists(),
        "native path import without provider should fail before opening the store"
    );
}

#[cfg(unix)]
#[test]
fn import_rejects_symlinked_provider_root() {
    use std::os::unix::fs::symlink;

    let temp = tempdir();
    let target = temp.path().join("pi-sessions");
    fs::create_dir_all(&target).unwrap();
    let path = temp.path().join("pi-sessions-link");
    symlink(&target, &path).unwrap();

    ctx(&temp)
        .args([
            "import",
            "--provider",
            "pi",
            "--path",
            path.to_str().unwrap(),
        ])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("symlinked provider transcript roots are rejected")
                .and(predicate::str::contains(path.to_str().unwrap())),
        );
}

#[cfg(unix)]
#[test]
fn import_reports_unreadable_directory_with_path_context() {
    if unsafe { libc::geteuid() } == 0 {
        return;
    }

    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir();
    let path = temp.path().join("unreadable-pi-sessions");
    fs::create_dir_all(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

    let stderr = failure_stderr(ctx(&temp).args([
        "import",
        "--provider",
        "pi",
        "--path",
        path.to_str().unwrap(),
    ]));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();

    assert!(stderr.contains("read import source directory"), "{stderr}");
    assert!(stderr.contains(path.to_str().unwrap()), "{stderr}");
}

#[test]
fn codex_cli_marks_deleted_raw_source_citations_unavailable() {
    let temp = tempdir();
    let source = PathBuf::from(provider_history_fixture("codex-sessions"));
    let copied = temp.path().join("copied-codex-sessions");
    copy_dir_all(&source, &copied);
    let copied_text = copied.to_str().unwrap().to_owned();

    let imported = json_output(ctx(&temp).args([
        "import",
        "--provider",
        "codex",
        "--path",
        &copied_text,
        "--json",
    ]));
    assert_eq!(imported["totals"]["imported_events"], 4);

    fs::remove_dir_all(&copied).unwrap();

    let search = json_output(ctx(&temp).args(["search", "onboarding", "--json"]));
    assert!(search["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|result| result["source_exists"] == false));
    assert!(search["results"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|result| result["citations"].as_array().unwrap().iter())
        .any(|citation| citation["source_exists"] == false));
}

#[test]
fn local_transcript_oracle_preserves_cli_json_and_sqlite() {
    let temp = tempdir();
    let fixture = redaction_fixture("codex-sessions");

    let import = json_output(
        ctx(&temp)
            .env("CTX_CODEX_TOOL_OUTPUT_MODE", "full")
            .env("CTX_CODEX_EVENT_MODE", "rich")
            .env("CTX_CODEX_INCLUDE_NOTICES", "1")
            .args([
                "import",
                "--provider",
                "codex",
                "--path",
                &fixture,
                "--json",
            ]),
    );
    assert_eq!(import["schema_version"], 1);
    assert_eq!(import["totals"]["failed"], 0);
    assert!(import["totals"]["imported_sessions"].as_u64().unwrap() > 0);

    let search = json_output(ctx(&temp).args(["search", "visible marker", "--json"]));
    assert_eq!(search["schema_version"], 1);
    assert_eq!(search["share_safe"], false);
    assert!(!search["results"].as_array().unwrap().is_empty());

    let conn = Connection::open(temp.path().join("work.sqlite")).unwrap();
    let ctx_session_id: String = conn
        .query_row(
            "SELECT id FROM sessions WHERE provider = 'codex' ORDER BY started_at_ms LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    let show = json_output(ctx(&temp).args([
        "show",
        "session",
        &ctx_session_id,
        "--mode",
        "log",
        "--format",
        "json",
    ]));
    assert_eq!(show["schema_version"], 1);
    assert!(show["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["preview"]
            .as_str()
            .unwrap_or("")
            .contains("fake.jwt.token")));

    let cli_json = format!("{import}\n{search}\n{show}");
    assert!(!cli_json.contains("[REDACTED"));
    assert_contains_markers("cli json", &cli_json, local_cli_markers());

    let event_payloads = sqlite_column_text(&conn, "SELECT COALESCE(payload_json, '') FROM events");
    let event_index = sqlite_column_text(
        &conn,
        "SELECT COALESCE(safe_preview_text, '') FROM event_search",
    );
    let record_index = sqlite_column_text(
        &conn,
        "SELECT COALESCE(title, '') || ' ' || COALESCE(summary, '') || ' ' || COALESCE(primary_user_text, '') || ' ' || COALESCE(decision_text, '') || ' ' || COALESCE(context_text, '') || ' ' || COALESCE(tag_text, '') FROM ctx_history_search",
    );
    let sqlite_text = format!("{event_payloads}\n{event_index}\n{record_index}");
    assert!(!sqlite_text.contains("[REDACTED"));
    assert!(event_index.contains("/home/alice/src/acme-secret/project"));
    assert_contains_markers(
        "sqlite indexed output",
        &sqlite_text,
        local_sqlite_markers(),
    );
}
