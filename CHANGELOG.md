# Changelog

## Unreleased

### Added

- Added a persistent automatic-refresh policy with source health, refresh
  leases, path-free state, and bounded incremental OpenCode refreshes. Search
  now attributes refresh phases and coalesces concurrent stateful refresh work.
- Added deterministic, read-only compaction planning and verified selective
  archive bundles, followed by archive-backed deletion, archived-reimport
  suppression, selective restore, and explicit physical SQLite reclaim.
- Documented the archive-first compaction contract, archive format, storage
  operations, and the compaction and reclaim CLI and JSON interfaces.

### Changed

- Adopted the fleet's fail-closed, resumable release workflow, including a
  documented readiness check and a Nix contract check that prevents the release
  CLI and operator instructions from drifting.

### Fixed

- Kept fallback OpenCode refreshes fail-closed when a source scan is partial,
  stabilized crash-lease coverage, synchronized archives before deletion, and
  reported reclaim failures consistently in JSON output.

## v1.1.0 - 2026-08-09

### Added

- `ctx evidence session|search|events` exports deterministic private JSONL or
  Markdown bundles to stdout or to a create-only `0600` file. File publication
  uses descriptor-anchored traversal, verified private staging, and the native
  macOS/Linux atomic no-replace primitive (#280).
- A closed deterministic evidence normalization boundary with fail-closed
  content proof, bounded provenance and citations, exact normalized byte
  accounting, and no raw payload, path, cursor, clock, filesystem, or ambient
  environment exposure (#278).
## v1.0.3 - 2026-08-06

### Added

- Bounded pagination across `ctx search`, `ctx show session`, and the MCP
  `search`/`show_session` tools (#195): opaque `--continue` continuation
  tokens with conservative physical-snapshot staleness detection, `--fields
  full|compact` projections, per-item and per-page byte budgets
  (`--max-snippet-bytes`/`--max-event-bytes` and `--max-page-bytes`), JSONL/
  NDJSON output via `--format jsonl`, and suggested `next` commands/arguments
  on every truncated page. Store reads for transcripts and event windows are
  now hard-bounded keyset pages (never unbounded scans), backed by the new
  fork schema v1001 covering indexes
  `idx_sessions_provider_external_session_started` and
  `idx_events_session_seq_id`. The #195 migration was renumbered from its
  pre-rebase v1000 to v1001 on top of the authoritative v1000 rowid-map
  migration: v1000 stores upgrade in place without touching maps or FTS
  projections, fresh databases receive both fork migrations, read-only opens
  require exactly v1001, and versions 16–999 or >1001 are rejected without
  mutation.
- `ctx search` gains role-aware and tool-noise relevance controls: repeatable
  `--role` / `--exclude-role` filters (`user`, `assistant`, `tool`),
  `--exclude-tool-noise` to drop tool/command events, and a repeatable
  `--exclude-tool <name>` to drop tool/command events whose structured tool or
  command executable matches a name such as `ctx`. The MCP `search` tool
  accepts the same filters as optional `role`, `exclude_role`,
  `exclude_tool_noise`, and `exclude_tool` arguments with permissive defaults.
  Equivalent user/assistant message matches now rank ahead of incidental
  tool/command matches by default across every match mode, reranked within the
  documented bounded candidate pool (`max(limit*8, 50, limit+1)`), and
  `why_matched` explains the role, event type, source field, and any relevance
  penalty (human output shows it with `--verbose`). No-result broadening
  suggestions preserve the new filters in the order they were given. Exactly
  representable role and tool-noise predicates are pushed into the ranked SQL
  page alongside the existing filter pushdown; executable-name exclusion
  stays a Rust-side residual filter under the scan budget.
- `ctx search --match all|any|phrase` (and the MCP `search` tool's `match`
  argument) makes multiword matching explicit: `all` (default) requires every
  normalized word of a clause in one indexed section, `any` broadens and
  rewards more matched tokens, and `phrase` requires adjacent ordered words.
  Queries are always literal ctx tokens — punctuation separates, diacritics
  are not folded, and FTS operator syntax is never interpreted. Search JSON
  gains a structured `query_plan`, and no-result searches print a labeled
  `suggestion (not run)` one-step broadening command (JSON
  `broadened_search`) instead of ctx ever retrying broader semantics itself.
- `ctx search` now reports index freshness in human and JSON output, including
  whether refresh ran, its outcome and duration, index age, and import totals
  (#196). `--refresh off` remains strictly read-only.
- `ctx status` now reports the local storage footprint and available space,
  with stable low-space warnings; `ctx doctor --storage` adds read-only SQLite,
  FTS, and reclaimable-space diagnostics without checkpointing or maintenance
  writes (#199).

### Fixed

- Delegated the Pages OAuth grant through Linux release builds so their nested
  downloads-site refresh can be submitted successfully, and made artifact
  uploads safe to retry after a partial release failure.
- Accepted unambiguous ctx ID prefixes consistently across CLI and MCP lookups,
  including case-insensitive compact and canonical UUID prefixes of at least
  eight hex digits (#198).
- Diagnosed non-empty import sources that unexpectedly produce no sessions,
  events, or edges with path-free `zero_yield_anomaly` health reports and
  warnings; `ctx import --strict` now exits nonzero after printing the complete
  report (#197).

### Changed

- Fresh record and event ingestion now skips unnecessary full FTS scans while
  keeping base rows and search projections atomic (#186).
- First fork schema divergence (v1000): existing-row search index updates are
  now keyed by durable FTS rowid maps instead of full-index scans. The
  migration only adds two empty map tables — no reindex, no backfill; legacy
  rows heal lazily on their first update. Once migrated, the store is no
  longer openable by older ctx binaries; restart long-lived ctx processes
  (for example `ctx mcp`) after upgrading, since the version check happens at
  open time and cannot stop an already-running pre-upgrade process. An
  unsupported external downgrade procedure is documented in `ctx docs show
  storage` (#186).
- Consolidated indexed projections, status, source listing, locate, and raw SQL
  behind a shared read-only query service without changing their CLI/MCP
  behavior or weakening raw-SQL protections (#187).
- Compact search and session projections now build only the fields they return,
  avoiding discarded data and store lookups without changing output (#186).
## v1.0.2 - 2026-08-05

### Changed

- Refreshed compatible Cargo dependencies and the Nix flake inputs, including
  the fleet release tooling and its SourceHut integration.
- Reduced search and index-maintenance work by filtering before hydration,
  avoiding full FTS counts, bounding post-import merges, and rebuilding FTS
  projections atomically.
- Added a streaming large-index profiling harness and expanded the downloads
  site with overview and example pages.
- Standardized the release workflow, reduced the release closure, and enabled
  CI cache warming and static checks.

### Fixed

- Corrected SourceHut release authentication and CI provisioning, including
  the approved release channel, OAuth grants, Cachix secret, and Python path.

## v1.0.1 - 2026-07-03

### Added

- Reproducible `release-artifact` tarballs plus a static downloads page
  published to SourceHut Pages (`build-pages`, `publish-pages`).

### Changed

- README shows the tag-pinned `nix run` invocation for installs.

### Fixed

- OpenCode history import follows the current message/part schema.

## v1.0.0 - 2026-07-03

### Added

- Nix flake with devShell and local CI wrappers (`ci-fmt`, `ci-clippy`,
  `ci-test`, `ci-docs`) plus SourceHut CI running the same gates.
- `jj lint` fast gates for pre-push validation.

### Changed

- Rebaselined fork versioning at 1.0.0 (hard fork of ctxrs/ctx at upstream
  `38241f0c`).
- Removed telemetry/identity, self-upgrade, SDKs, wire contracts, Bazel, and
  Buildkite plumbing; the binary makes no network calls.

### Fixed

- Made Nix CI apps self-contained and fixed newer clippy lints.
- Initialize the store before probing provider filter aliases in tests.
