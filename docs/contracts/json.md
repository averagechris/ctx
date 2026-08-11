# JSON Contracts

ctx JSON is for local agents and scripts. It can include prompts, command
output previews, and local paths. Treat it as private until a user reviews and
redacts it.

Command result JSON currently uses `schema_version: 1`. Progress-event JSON is
stderr progress output and does not include `schema_version`.

## Storage reclaim

```bash
ctx storage reclaim --json
```

On a successful attempt the single stdout object contains `status`, `reason`,
`before`, `after`, and `estimated_temporary_bytes`. `status` is `completed`
when a compact database was installed, or `skipped` when there was nothing to
reclaim. The skipped reason is `nothing_reclaimable`; `after` is null for that
case. A snapshot contains `main_db_bytes`, `wal_bytes`, `shm_bytes`,
`objects_bytes`, `spool_bytes`, `freelist_bytes`, `available_space_bytes`, and
`temporary_bytes`. `reason` is null after reclaim and otherwise a stable reason
when the operation safely declines. On failure, JSON mode writes no result to
stdout and emits `{ "status": "failed", "error": { "code":
"storage_reclaim_failed", ... } }` to stderr, exiting 1; the message is not a
diagnostic contract. The operation is physical compaction only: it does not
logically delete history, object files, or spool data. These sizing fields are
private local metadata and contain no transcript content or paths.

## Archive compaction and suppression

`archive create --cutoff-ms N` returns the normal archive result shape with
`format: "ctx-selective-archive"`; it selects completed sessions ending at or
before inclusive cutoff and their authenticated closure. Without the option,
`format` is `"ctx-archive"`. Creation does not delete rows.

Selective restore JSON includes `format`, `format_version`, `archive_id`,
`source_schema_version`, `path`, `restored`, `entity_count`, `objects` (with
`count` and `total_bytes`), `inserted_count`, `reused_count`, and
`selected_root_count`. `--session-id` may be repeated; duplicate IDs are
harmless, and an empty list means all authenticated roots in a selective
bundle. The success path includes `path`, so treat it as private.

`archive suppression-status --json` returns exactly the bounded counts
`active`, `conflict`, `restored`, and `overridden`; it is read-only and
path-free. `archive override --json` returns `operation_id`,
`affected_associations`, and `effective_state`. Override is an audited ledger
operation, not a change to canonical history. Consumers must ignore additive
future fields and must not treat these objects as share-safe.

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
- `pagination`, for session output: `{ has_more, cursor? }`;
- `total_events`, `omitted_events`, and `fields`, for session output.

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

`ctx show session` is the #195 paged-query slice of #187. Status, sources,
locate, bounded raw SQL, search, and session/event windows now share the
transport-neutral read-only query layer. Show returns additive query-owned v1
typed projections with `fields: "full"|"compact"`. Compact session output
contains only ctx session ID, provider, agent type, status, primary flag, and
start/end times; compact events contain only ctx event ID, sequence, event type,
role, time, text, and text truncation. Compact structurally excludes provider
session IDs, source IDs/metadata/path/existence, cwd, provider/source cursors,
citations, raw payload, and suggested commands. Full is still private local
history, not share-safe. Raw/withheld event payloads project as the literal text
`raw event payload withheld`.

Session pagination selects the transcript mode before applying the page limit:
`log` selects all events; `full` selects user/assistant/system message events;
`lite` selects user messages plus the final assistant message before the next
user message or end of session. JSON reports the exact `selected_total`,
`pagination.{has_more,continuation,offset,page_size,returned_items}`, compatible
`pagination.cursor` and top-level `next`, exact `omitted.{before,after,exact}`
and compatible `omitted_events`. Store reads are bounded keyset reads over
`(seq,id)`.

Limits default to `--limit 200`, `--max-event-bytes 4096`, and
`--max-page-bytes 262144`; caps are `--limit 1000`, per-item bytes 1048576, and
page bytes 16777216. The byte cap is UTF-8 safe: code points are not split and a
three-byte ellipsis is appended only when it fits. `bytes.item_json_bytes` is the
exact sum of the final compact JSON encoding of admitted public item objects,
including full-mode compatibility aliases. The same item object is used in CLI
JSON, MCP `structuredContent`, and nested under each JSONL item record. Fixed metadata
(`session`, pagination, omitted counts, next commands, etc.) is outside that
budget, as are JSONL framing fields and newlines. Records are admitted whole, so no partial JSON item is emitted; if the
next item would exceed the page budget, `page_budget_exhausted: true` and the
page stops after the last admitted item. If even the first item cannot fit, the
query fails with `item_exceeds_page_budget` rather than returning a
non-advancing continuation; this includes a zero page budget on a non-empty
result set.

CLI text/markdown ends with a page summary: returned range, omitted before and
after, exactness, selected total, and either a copyable continuation command or
`no more events`. JSON uses explicit `next`, `next_command`, and `next_argv`, all
`null` on the final page. Canonical continuation argv preserves the mode, fields,
limit, byte caps, format, and continuation.

Session JSONL emits one independent `record_type: "event"` record per line, with
compatible item type, mode, session/provider identity, and final event item, and
exactly one terminal `record_type: "completion"` line. Invalid continuation or
post-format request/lookup/store errors in JSONL mode emit one structured terminal `record_type: "error"`
line and exit nonzero. A real broken stdout pipe exits successfully without
diagnostic noise for paged stdout streaming only. Clap failures before output
format parsing retain Clap's stderr and exit-code behavior.

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

With `--out`, writes the requested transcript page to that path and prints
nothing on success. Without `--out`, stdout is the requested transcript
page. Continuation argv does not retain `--out`. JSON and JSONL rows use the same ctx-owned ID fields as
`show`.

## Search

```bash
ctx search <query>|--term <term>|--file <path> --json
ctx search <query> --format jsonl --refresh off
```

Returns:

- `schema_version`;
- `query`;
- `query_plan`, with `schema_version: 1`, `mode`, `clauses[]` (`original`, normalized `terms[]`), `within_clause_operator`, `between_clause_operator: "OR"`, and `filters_operator: "AND"`;
- `filters`;
- `broadened_search`, optional; when present it is `{ "executed": false, "from_match": "phrase|all", "to_match": "all|any", "command": "ctx search ...", "argv": ["ctx", "search", ...] }` for a no-result CLI query that can be broadened one step; compact JSON omits it when no broader mode is useful;
- `freshness`;
- `generated_at`, the response-generation time for this request (not the time a
  reusable internal candidate pool was created);
- `results[]`;
- `pagination`;
- `truncation`.

Each result can include:

- `ctx_event_id` for event hits;
- `ctx_session_id` when known;
- compatible `event_id` and `session_id` aliases;
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
- `links`;
- `suggested_next_commands[]`;
- `visibility`.

`why_matched[]` can include text, metadata, or touched-file reasons. A touched
file match is backed by normalized touched-file storage and can appear when
search uses `--file <path>` or when file-path metadata contributes to ranking.
`citations[]` can cite sessions, events, files, or source metadata depending on
which indexed item produced the match.

`query_plan` is additive schema v1 and contains only query structure, normalized terms, and operators; it does not include unrelated index content. Existing `query` remains the original compatible spelling. MCP search returns the same `query_plan`; CLI-only `broadened_search.command` is a shell-quoted suggestion rendered from `broadened_search.argv`; it is never executed by ctx.

Search JSON is local/private by default and is not share-safe or redacted for
external publication.

Search pagination is additive in v1. `pagination.cursor` is an opaque
continuation token and is `null` when `has_more` is false. CLI continuation use
requires `--refresh off` so the query executes against a stable read-only
snapshot. Search pages are stable replay/slices of a fixed candidate pool. A
live query service may reuse matching candidate work from a private,
process-local four-entry cache for up to five minutes; misses regenerate with
the internal candidate limit fixed at 200, then slice by offset. The TTL is
enforced lazily on lookup/insertion, and separate CLI processes share nothing.
The source scan itself can truncate earlier; `pool_total` is
the exact returned candidate pool size, while `source_truncation.omitted_results`
is only exact when `omitted_results_exact: true` and otherwise is a lower bound.

Tokens bind the full request shape: query string, ordered/repeated `--term`
vector, query revision, DTO and search-packet schemas, query plan/match mode,
every packet option (including snippet length), every filter, result
mode, page size, field set, and byte policy, plus a conservative local database
fingerprint. They do not contain private content: no query text, snippets,
paths, citations, provider IDs, or provider metadata. Malformed, wrong-kind,
request-mismatched, out-of-range, or stale tokens fail closed; search
continuations also reject embedded show keyset fields. CLI canonical `next_argv`
preserves options and always forces `--refresh off` for the next page. JSON uses
explicit `next`, `next_command`, and `next_argv`, all `null` on the final page.

Search uses the same `--fields`, `--max-snippet-bytes`/aliases
`--snippet-bytes`/`--item-bytes`, `--max-page-bytes`/alias `--page-bytes`, UTF-8
ellipsis accounting, whole-record admission, and exact final item JSON byte
budget as show. Defaults are `--limit 20`, snippets 4096 bytes, page 262144
bytes; caps are `--limit 200`, per-item 1048576 bytes, page 16777216 bytes.
`--format jsonl` emits one independent `record_type: "result"` per line and
exactly one terminal `record_type: "completion"` with returned count, range,
omitted counts, `pool_total`, source truncation, byte summary, `next`, and
copyable `next_argv` when another page exists. JSONL invalid continuation or
post-format request errors emit a single `record_type: "error"` line and exit
nonzero; broken paged-stdout pipes are silent success. Explicit `--out` I/O
errors are propagated.

Search compact projections omit provider-session IDs, history source/provider
key/source ID/source format, source path/existence/cursor, cwd, citations, and
suggested commands. Full remains private and may include local paths, source
metadata, snippets, and citations. The query-owned full projection includes
compatibility aliases and `suggested_next_commands[]`; compact does not.

This pagination service is one part of the broader #187 extraction. Status,
sources, locate, and bounded raw SQL use the same transport-neutral read-only
query layer; CLI and MCP retain only their transport-specific envelopes and
rendering.

Continuation snapshot fingerprints are conservative SHA-256 digests over physical
SQLite state: selected PRAGMAs plus stable samples of the main DB, WAL, and SHM
files, checked before and after page construction. They intentionally exclude
path, query, and provider metadata. Because WAL/checkpoint timing affects the
physical files, a logically equivalent checkpointed store can make an old token
stale. Show tokens necessarily carry an opaque event ordering key (`seq` and ctx
event ID) inside the hex token so keyset paging can resume; this is not path,
query, or provider metadata.

Writable opens migrate known v0-v15 stores through the fork chain — v1000
  (durable FTS rowid maps), v1001 (pagination indexes), v1002 (path-free
  source health), v1003 (path-free refresh coordination), v1004 (bounded
  OpenCode incremental state), then v1005 (selective-archive suppression) —
  and existing v1000–v1004 stores to v1005; reserved versions 16-999 and
  versions above 1005 fail closed
without mutation. Index creation and `user_version = 1001` are one
transaction, and the step touches neither the rowid maps nor the FTS
projections. The v1001 migration adds exactly
`idx_sessions_provider_external_session_started` on
`sessions(provider, external_session_id, started_at_ms DESC, id)` and
`idx_events_session_seq_id` on `events(session_id, seq, id)`. Read-only
search/show/locate/MCP opens require exactly v1005 and never migrate or write.
Restart long-lived processes such as `ctx mcp` after upgrading so they reopen
through the v1002 gate.

`ctx doctor --json` reports both the legacy
`ledger_backed_zero_yield_anomalies` count and the path-free
`source_level_anomalies`/`source_level_class_breakdown`, with coverage
`all_import_paths_since_v1002` and compatibility field `not_persisted_for: []`.
Default doctor remains read-only. `ctx doctor --acknowledge-source-health`
explicitly clears only advisory source-health rows and reports the exact
`acknowledged` count; it does not repair, suppress imports, or modify history,
FTS projections, or rowid maps. Import strict mode also fails after printing a
complete report when health persistence failed. Search refresh instead reports
`degraded_health_persistence` and continues serving search after durable import.

The migration was measured on 2026-07-11 with a temporary synthetic v15 SQLite
database containing 100,000 events (100 sessions × 1,000 events) and 1,000
session resolver rows. Over 500 warm runs on macOS, median event-page lookup fell
from 97.3 µs to 32.4 µs and median provider-session resolution from 22.8 µs to
14.0 µs. The v15 plans used the single-column indexes plus `USE TEMP B-TREE FOR
ORDER BY`; the pagination indexes give covering `idx_events_session_seq_id`
with the `(seq,id)`
range and covering `idx_sessions_provider_external_session_started`, with no
temporary sort. These are synthetic local measurements, not production latency
guarantees.

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
