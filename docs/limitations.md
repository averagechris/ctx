# Limitations

ctx is production-scoped to local history indexing and search retrieval.
These limitations are intentional unless another document says a capability has
shipped.

## Provider Coverage

- Codex local import is supported for documented local JSONL sources.
- Pi local import is supported only when a matching local `sessions.jsonl` file
  exists.
- Antigravity, Claude, OpenCode, OpenClaw, Hermes, Gemini, Cursor, Copilot CLI,
  and Factory AI Droid local import is supported only when their documented
  local history paths exist and match the supported native formats in the
  provider matrix.
- NanoClaw and AstrBot local import are preview/manual-path support. They are
  not included in `ctx import --all` or pre-search refresh, and AstrBot imports
  local LLM context plus available platform history rather than guaranteeing a
  complete raw IM transcript.
- Unknown provider formats should not be parsed optimistically.

## Import Semantics

- Imports are explicit; ctx does not collect provider history in the background.
- Current importers use idempotent rescans.
- `--resume` is reported in output but is not a universal provider cursor
  contract.
- Explicit `--path` imports are not remembered as future defaults.

## Search Semantics

- Search quality depends on what providers expose and what importers index.
- Large outputs may be represented as bounded previews.
- Ranking is deterministic for the same local database and options, but it is
  not a claim of semantic understanding.
- Empty or punctuation-only search is invalid. Broad valid queries can still
  return metadata-driven matches.
- `ctx search --json` and the MCP `search` tool include additive pagination
  metadata. Continuations are opaque, deterministic for the same read-only local
  database snapshot and normalized request, and fail closed when malformed,
  stale, or replayed with different options. CLI continuation use requires
  `--refresh off`; the query layer never imports, refreshes, migrates, writes, or
  contacts providers.
- Search continuations contain request/snapshot hashes and offsets only; they do
  not embed query text, transcript text, paths, citations, provider-session IDs,
  or provider metadata. Show continuations additionally carry an opaque event
  key (`seq` and ctx event ID) so bounded keyset paging can resume.
- Search pages replay a fixed candidate pool with maximum size 200 and slice it
  by offset. `pool_total` is exact for that pool, but provider/source scan
  truncation can make omitted source results a lower bound.

## Retrieval Semantics

- Search output is retrieval material, not generated analysis.
- Token counts are estimates.
- If a raw source moves, ctx may still return indexed text from SQLite.
- JSON is local/private and can include sensitive content.
- `ctx show session` is bounded by `--limit` and returns `pagination`,
  `total_events`, and `omitted_events` in JSON. Human output states omitted
  counts and a copyable `--continue` token when more events remain.
- `ctx show session --format jsonl` emits independently valid event records and
  a final completion record with `has_more`, `next`, `omitted_before`, and
  `omitted_after`. Event
  windows and session pages are resolved with bounded store reads rather than
  loading complete sessions.
- Compact field selection is opt-in (`--fields compact`) and is intended for
  future transport-neutral consumers; omitted source/path/citation fields must
  not leak in compact projections. Compact also excludes provider-session IDs,
  source IDs/metadata/existence, cwd, provider/source cursors, raw payload, and
  suggested commands. Compact is smaller, not share-safe.
- Source/status/locate/raw-SQL DTO construction is shared through the reusable
  read-only query-service layer; transport-specific rendering remains in CLI/MCP.
- `--max-snippet-bytes`/`--max-event-bytes` and `--max-page-bytes` bound item
  projection text and admitted item JSON bytes, not the total response envelope.
  JSON records are admitted whole; ctx does not emit partial JSON items. A
  non-empty result whose first item cannot fit is an error, avoiding a
  non-advancing continuation.
- Continuation snapshots are conservative SHA-256 fingerprints over SQLite
  physical state (main DB plus WAL/SHM samples and PRAGMAs). Checkpoints or WAL
  changes can stale a token even when logical content appears unchanged.
- Writable opens migrate known v0-v15 stores through the fork chain (v1000
  rowid maps, v1001 pagination indexes, v1002 path-free source health, then
  v1003 path-free refresh coordination) and existing v1000–v1002 stores to
  v1003, each step atomic; versions 16-999 and above 1003 fail closed. Read-only commands require exactly v1003 and never
  migrate/write.

## Operations

- Core setup/import/search are local filesystem operations.
- This fork makes no network calls; there is no telemetry and no self-update.
  Update via Nix / SourceHut release tags.
- No provider beyond the support matrix should be described as supported.

## Portable Archives

- `ctx archive create`, `verify`, and `restore` are local-only; they do not
  upload, sync, or contact a remote service.
- Archive v1 is a logical content bundle, not an exact SQLite disaster
  snapshot. It is not a merge or overwrite operation, and restore requires a
  strictly absent target (an empty existing directory is rejected).
- Restore rebuilds the active record/event FTS projections and their store-local
  rowid maps. It clears `artifact_search` and intentionally leaves it empty and
  unused; restore does not repopulate every FTS table. It omits machine-local
  operational tables, configuration, logs, and SQLite WAL/SHM state. See
  [Portable archive workflows](archive.md) and the normative [format
  contract](archive-format-v1.md).
- Archive integrity checks do not provide encryption, signing, or authenticity
  against an attacker able to rewrite the bundle. Compression is not
  encryption; treat bundles as verbatim secret-bearing data.
- Verification is bounded and fail-closed. It detects malformed layout,
  truncation, digest/size mismatches, unsupported format versions, duplicate
  or dangling content, and unsafe filesystem entries, but it cannot prove
  provenance or recover data that was never archived.
- Verification does not modify the published bundle, but it uses a private
  sibling `.ctxar-verify-*/state.sqlite` scratch database and normally removes
  it. A crash can leave that scratch path or unpublished create/restore staging;
  treat all such residue as sensitive and do not open unknown roots.
- The exclusive rename is the publication point. Before it, handled failures
  leave the target absent and clean staging; a parent-directory `fsync` can
  report an error after the complete target exists. Inspect and verify before
  retrying, and never overwrite a target after such an error.
