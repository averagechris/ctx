# JSON Contracts

ctx JSON is for local agents and scripts. It can include prompts, command
output previews, and local paths. Treat it as private until a user reviews and
redacts it.

Command result JSON currently uses `schema_version: 1`. Progress-event JSON is
stderr progress output and does not include `schema_version`.

## Setup

```bash
ctx setup --json
```

Writes local storage and returns:

- `schema_version`;
- `data_root`;
- `database_path`;
- `config_path`;
- `mode`, either `ready` or `catalog_only`;
- `indexed_items`;
- `sources`;
- `catalog`;
- `catalog_sources`;
- `import`;
- `network_required: false`;
- `repo_writes: false`.

`import.ran` is true for the default setup path and false for
`ctx setup --catalog-only`. When it runs, `import.totals` and `import.sources`
use the same shape as `ctx import --json`.

## Status

```bash
ctx status --json
```

Reads local storage state and returns:

- `schema_version`;
- `initialized`;
- `data_root`;
- `database_path`;
- `config_path`;
- `indexed_items`;
- `indexed_sources`;
- `cataloged_sessions`;
- `indexed_catalog_sessions`;
- `pending_catalog_sessions`;
- `failed_catalog_sessions`;
- `stale_catalog_sessions`;
- `storage` with logical file sizes for `main_db_bytes`, `wal_bytes`,
  `shm_bytes`, `objects_bytes`, `spool_bytes`, `total_data_root_bytes`,
  `approx_bytes_per_event`, `available_space_bytes`, `low_space`, `warnings`,
  and `measurement_complete`;
- `diagnostics[]`, bounded path-free measurement diagnostics when sizing was
  partial or unavailable;
- `local_only: true`;
- `read_only: true`;
- `private: true` and `share_safe: false`.

Status JSON is not share-safe: it avoids transcript content but includes local
storage metadata and, for compatibility, existing absolute path fields. Low
space is stable across CLI and MCP: `warning` below 512 MiB available and
`critical` below 128 MiB available. Warnings note that imports and SQLite work
can require temporary free space. Unsupported filesystem free-space probes are
reported as `available_space_bytes: null` and `low_space: "unknown"`.
`measurement_complete: false` means one or more filesystem entries could not be
measured and reported byte totals are partial. v1 JSON may grow additive fields;
unknown fields should be ignored by consumers.

## Sources

```bash
ctx sources --json
```

Returns:

- `schema_version`;
- `sources[]`.

Each source includes:

- `provider`;
- `path`;
- `exists`;
- `source_format`;
- `status`;
- `import_support`;
- `native_import`;
- `importable`;
- `raw_retention`;
- `unsupported_reason`.

`status` is `available`, `empty`, `unknown`, `missing`, or `unsupported`.
`import_support` is `native` or `unsupported`. `native_import` is a boolean
derived from `import_support == "native"`. `importable` is true only when the
source is both available and natively importable. `unknown` means the bounded
provider-specific transcript probe hit its scan budget before proving the
source available or empty. `unsupported_reason` is a string for unsupported,
empty, or unknown rows and otherwise null.

## Import

```bash
ctx import --json
```

Writes the local SQLite index and returns:

- `schema_version`;
- `resume`;
- `resume_mode`;
- `totals`;
- `sources[]`.

`totals` and each source row include file, byte, session, event, edge, skipped,
and failed counts. `totals.zero_yield_anomaly_sources` counts source rows with
one or more zero-yield anomalies. Per-source `health.reason_counts.zero_yield_anomaly`
counts exact known import units where the importer can attribute them; otherwise
ctx reports only the source-level anomaly. Each source row includes
additive `health.classification` and `health.reason_counts` fields plus
`skipped_reasons`. Classifications currently emitted are `success`,
`partial_success`, `unchanged`, `all_skipped`, `empty`,
`unsupported_or_malformed`, `zero_yield_anomaly`, and `failed`, but consumers
must ignore unknown future values. Skip reason precision is limited by existing
adapter ledgers; when only an aggregate skip count is available, ctx reports the
stable `unspecified` reason instead of inferring from messages.
`scanned_files` and `scanned_bytes` are pre-import observed regular-file
inventory measurements from source discovery, not parser-consumed record counts.

`zero_yield_anomaly` means a non-empty source produced zero imported sessions,
events, and edges, with no failures, no known-safe skips, and no explicit empty
or cursor-only result. It is not used for idempotent/all-skipped repeat imports,
genuinely empty discovery, malformed/failed sources, or history-source plugin
validated plugin cursor-only updates (`plugin_cursor_only`). `ctx import` still exits successfully by default and
writes one valid JSON report to stdout; path/content-free anomaly warnings are
written to stderr. `ctx import --strict` prints the same complete report first,
then exits nonzero (runtime exit code 1) if any `zero_yield_anomaly` was found.
`resume_mode` is currently `idempotent_rescan` when `--resume` is passed and
`normal_scan` otherwise.

`ctx doctor --json` includes additive `import_health` diagnostics. In this
no-schema slice, `import_health.ledger_backed_zero_yield_anomalies` covers only
stable anomaly codes persisted by existing manifested `source_import_files` and
`catalog_sessions` ledgers; it is not universal historical coverage for custom,
plugin, or unmanifested imports.

## Progress

```bash
ctx setup --progress json
ctx import --progress json
ctx import --json --progress json
```

`--progress json` writes newline-delimited progress objects to stderr for
`setup` and `import`. It does not change command result stdout. This means
`ctx setup --json --progress json` and `ctx import --json --progress json`
write the command result object to stdout and zero or more progress objects to
stderr.

Each progress object includes:

- `type: "ctx_progress"`;
- `operation`, currently `setup` or `import`;
- `phase`;
- `message`;
- `completed_bytes`;
- `total_bytes`;
- `percent`;
- `elapsed_seconds`;
- `eta_seconds`, nullable when no estimate is available or the operation is
  complete;
- `completed_files`, nullable;
- `total_files`, nullable;
- `imported_events`, nullable;
- `done`.

Progress events are operational status events, not durable result records.
Consumers should key on `type` and `operation`, ignore unknown fields, and read
the final command result from stdout when `--json` is present.

## Show

```bash
ctx show session <ctx-session-id> --format json
ctx show event <ctx-event-id> --format json
```

Writes nothing and returns:

- `schema_version`;
- `item_type`, either `session_transcript` or `event_window`;
- `mode` for session transcripts;
- `format`;
- `session` for session output;
- `event` for event output;
- `source`;
- `events[]`.

`session` includes the ctx-owned `item_id`, `provider`, and
`provider_session_id` when known. `event` and `events[]` rows include
`ctx_event_id`, `ctx_session_id`, `sequence`, `event_type`, `role`,
`occurred_at`, `source`, `cursor`, `text` or `preview`, and
`redaction_state`.

`redaction_state` values describe local payload handling, not whether a row is
safe to publish. In particular, `safe_preview` is legacy contract spelling for a
local searchable preview: the text may be truncated or projected from provider
payloads, but it can still include absolute paths, token-shaped strings, command
output, and other private transcript content. Treat `safe_preview` output as
private unless a user separately reviews and redacts it.

## Locate

```bash
ctx locate session <ctx-session-id> --format json
ctx locate event <ctx-event-id> --format json
```

Writes nothing and returns provenance metadata:

- `schema_version`;
- `item_type`, either `session_location` or `event_location`;
- `ctx_session_id`;
- `ctx_event_id` for event output;
- `provider`;
- `provider_session_id` when known;
- `source`;
- `resume`.

`source` includes `path`, `cursor`, `exists`, `source_id`, and
`source_format` when known. `resume` includes provider cursor or import resume
metadata when available.

## Transcript Artifacts

```bash
ctx show session <ctx-session-id> --mode full --format json --out transcript.json
```

With `--out`, writes the requested transcript artifact to that path and prints
nothing on success. Without `--out`, stdout is the requested transcript
artifact. JSON and JSONL artifact rows use the same ctx-owned ID fields as
`show`.

## Search

```bash
ctx search <query>|--term <term>|--file <path> --json
```

Returns:

- `schema_version`;
- `query`;
- `filters`;
- `freshness`;
- `generated_at`;
- `results[]`;
- `pagination`;
- `truncation`.

Each result can include:

- `ctx_event_id` for event hits;
- `ctx_session_id` when known;
- `provider_session_id`;
- `event_seq`;
- `title`;
- `snippet`;
- `rank`;
- `result_scope`, either `session` for a session-level result or `event` for an
  event-level result;
- `session_importance` for default session results;
- `more_matches_in_session` for default session results;
- `provider`;
- `timestamp`;
- `cwd`;
- `source_path`;
- `source_exists`;
- `cursor`;
- `why_matched`;
- `citations[]`;
- `suggested_next_commands[]`;
- `visibility`.

`why_matched[]` can include text, metadata, or touched-file reasons. A touched
file match is backed by normalized touched-file storage and can appear when
search uses `--file <path>` or when file-path metadata contributes to ranking.
`citations[]` can cite sessions, events, files, or source metadata depending on
which indexed item produced the match.

Search JSON is local/private by default and is not share-safe or redacted for
external publication.

`freshness` describes the pre-search refresh attempt:

- `mode`, one of `auto`, `off`, or `strict`;
- `status`, such as `completed`, `degraded_zero_yield`, `skipped`,
  `no_sources`, `skipped_large_index`, or `failed`;
- `source_count`;
- `ran`, true when refresh/import execution was attempted and false for
  pre-execution skips such as `--refresh off` or no supported sources;
- `duration_ms`, elapsed pre-search refresh time in milliseconds for attempted
  refreshes and skipped source discovery; it is `0` for `--refresh off`;
- `index_age_seconds`, omitted when no persisted indexed/import timestamp is
  known; when present, the age in seconds of the newest persisted indexed/import
  timestamp, not the time since a no-op freshness check;
- `reason`, one of the stable current reasons `refresh_off`, `no_sources`,
  `refreshed`, `zero_yield_anomaly`, or `refresh_failed`;
- `totals`, using the same import total fields as `ctx import --json`;
- `error`, present when refresh failed but results were still served.

This is an additive compatible revision to search `schema_version: 1`; consumers
must continue to ignore unknown fields. `totals.unchanged_sources` is additive and
counts provider sources proven unchanged by metadata/cursor state without
importing new rows.

`suggested_next_commands` can include `ctx show event`, `ctx show session`,
`ctx search "<query>" --session <ctx-session-id>`, `ctx locate event`, and
`ctx locate session` command strings when the required ctx IDs are known.

When ctx can identify the active Codex provider session through
`CODEX_THREAD_ID`, search filters include `exclude_provider_session` and omit
that active session tree by default. Passing `--include-current-session` removes
that filter.

## SQL

```bash
ctx sql "SELECT COUNT(*) AS sessions FROM ctx_sessions" --json
ctx sql --file query.sql --format json
```

Runs one read-only SQL statement against the existing local SQLite index and
returns:

- `schema_version`;
- `item_type: "sql_result"`;
- `read_only: true`;
- `share_safe: false`;
- `columns[]`, ordered selected column names;
- `rows[]`, ordered arrays matching `columns[]`;
- `returned_rows`;
- `truncated.rows`;
- `truncated.values`;
- `limits.max_rows`;
- `limits.max_columns`;
- `limits.max_value_bytes`;
- `limits.max_sql_bytes`;
- `limits.timeout_ms`;
- `elapsed_ms`.

Scalar SQL values are encoded as JSON nulls, numbers, or strings when they fit
the configured value cap. Truncated text values are encoded as objects with
`type: "text"`, `value`, `bytes`, and `truncated: true`. Blob values are
encoded as objects with `type: "blob"`, `bytes`, `preview_hex`, and
`truncated`.

Use stable `ctx_*` views for scripts when possible: `ctx_sessions`,
`ctx_events`, `ctx_files_touched`, and `ctx_sources`. Internal tables remain
queryable for advanced local inspection but are not the preferred compatibility
surface.

## MCP Tool Results

`ctx mcp serve` exposes read-only MCP tools over stdio for status, sources,
search, SQL, showing sessions, and showing events. Tool results include
`structuredContent` JSON using the same private local fields as CLI JSON. MCP
output may include absolute paths, source metadata, snippets, and transcript
text, and the MCP host may log or forward it.

MCP search does not refresh or import provider history. It also excludes the
active Codex session tree by default when `CODEX_THREAD_ID` is set; pass
`include_current_session: true` to opt back in.

The MCP `sql` tool uses the same `sql_result` JSON contract as `ctx sql
--json`, always read-only.

## Docs

```bash
ctx docs list --json
ctx docs search <query> --json
ctx docs show <topic> --format json
```

`ctx docs list --json` returns:

- `schema_version`;
- `topics[]`.

Each topic includes `id`, `title`, `audience`, `summary`, `tags`, and
`source_path`.

`ctx docs search <query> --json` returns:

- `schema_version`;
- `query`;
- `results[]`.

Each result uses the topic fields above and adds `score`.

`ctx docs show <topic> --format json` returns one topic object plus:

- `schema_version`;
- `body`, containing the embedded markdown source.

Docs JSON is generated from embedded static docs and does not read provider
history or SQLite.

## Citation Fields

Citations can include:

- `item_id`;
- `item_type`;
- `ctx_event_id`;
- `ctx_session_id`;
- `label`;
- `time`;
- `provider`;
- `session_id`;
- `event_seq`;
- `source_path`;
- `source_exists`;
- `cursor`.

`source_exists: false` means indexed text is available but the raw source
was not present at the stored path when checked.

## Doctor

```bash
ctx doctor --json
```

Reads local storage and returns findings:

- `schema_version`;
- `ok`;
- `private: true` and `share_safe: false`;
- `findings`.

Without `--storage`, `storage` is `null`. With `ctx doctor --storage --json`,
`storage` is a direct object containing
`files`, `sqlite`, `external_provider_sources`, `thresholds`, and
`temporary_space_note`; it is not a nested status envelope. `storage.sqlite` is
`null` when the store is uninitialized or SQLite metrics cannot be collected.
When present, `fts_derived_bytes_available` distinguishes unavailable `dbstat`
from a measured `fts_derived_bytes: 0`. `external_provider_sources` is
machine-typed and currently reports `bytes: null`, `measured: false`, and
`reason: "external_provider_sources_not_measured_read_only"` because the storage
doctor does not walk provider-history roots.
`storage.sqlite.live_bytes` is total non-freelist allocated SQLite bytes,
including FTS-derived/shadow storage. When `dbstat` is available,
`primary_live_bytes` is the disjoint primary live estimate computed as
`live_bytes - fts_derived_bytes`; otherwise it is `null`.
`storage_optional_diagnostics[]` contains optional path-free notes that do not
make `ok` false; `findings[]` contains integrity, permission, low-space, and
required measurement failures.

## Provider Smoke

Provider smoke tests call normal `ctx` commands with temporary local storage and
static fixtures. Their output is ordinary command JSON covered by the command
schemas above; there is no separate provider artifact schema in the public CLI.

## Compatibility Limits

Compatibility `item_id`, `id`, `session_id`, and `event_id` fields can remain
in some outputs. New integrations should prefer ctx-owned `ctx_session_id` and
`ctx_event_id` where present, and should treat provider-owned IDs as metadata
unless an explicit provider lookup flag is present.
