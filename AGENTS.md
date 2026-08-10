# AGENTS.md

This is a hard fork of [ctxrs/ctx](https://github.com/ctxrs/ctx) maintained at
`git.sr.ht/~averagechris/ctx`. Decision record and phased plan:
[docs/fork-plan.md](docs/fork-plan.md).

## Working in this repo

- Use `jj`, not `git`. Trunk bookmark is `main` → remote `origin`
  (`git@git.sr.ht:~averagechris/ctx`). Remote `upstream` is
  github.com/ctxrs/ctx and is fetch-only, used for selectively porting fixes.
- Build/test via the Nix flake: `nix develop` for a cargo devShell, then
  `cargo fmt --all --check`, `cargo clippy --locked --all-targets -- -D warnings`,
  `cargo test --workspace`. Or run the CI wrappers directly: `nix run .#ci-fmt`,
  `nix run .#ci-clippy`, `nix run .#ci-test`, `nix run .#ci-docs`.
  `nix flake check` builds the package with the full test suite. SourceHut CI
  runs the same wrappers via `.builds/ci.yml`. `jj lint` runs the fast gates
  (`ci-fmt`, `ci-clippy`, `ci-docs`) from `.jj-lint.toml` — run it before
  pushing.
- `crates/ctx-cli/tests/cli.rs` is the behavioral contract — lean on it when
  refactoring; extend it when changing command behavior.

## Issue tracker

- Use todo.sr.ht tracker `~averagechris/projects`; the local `srht` CLI
  automatically scopes this repository with `repo:ctx`.
- Apply exactly one conventional-commit type label (`chore`, `fix`, `feature`,
  `security`, `docs`, `refactor`, or `perf`). Add an optional Fibonacci
  `points:N` label for estimated work; tracking/index tickets need no points.
- Common JSON commands: `srht --json todo list`, `srht --json todo show ID`,
  `srht --json todo new 'Title' -l TYPE -l points:N`,
  `srht --json todo start ID`, and
  `srht --json todo done ID --resolution fixed`. Use
  `-t '~averagechris/projects'` when explicit tracker selection matters.

## Fork guardrails

- **No network calls from the binary, ever.** This fork removed telemetry
  (`analytics.rs`, `identity.rs`), self-upgrade (`upgrade.rs`), and all
  `ctx.rs` endpoints. Do not reintroduce phone-home, update checks, or any
  HTTP client dependency. Releases are SourceHut `vX.Y.Z` tags; Nix owns the
  binary lifecycle.
- **No SDKs / wire contracts.** `sdks/`, `ctx-sdk`, `ctx-protocol`, and
  `contracts/` were deleted. The programmatic surface is CLI `--json` output
  and the MCP server. Recover from upstream history if genuinely needed.
- **The index is a secrets store.** Transcripts are indexed verbatim with no
  redaction; `~/.ctx/work.sqlite` contains anything ever pasted into an agent
  session. Keep the raw-SQL surface strictly read-only (the 5-layer
  enforcement in `ctx-history-store` must not be weakened) and keep data-root
  permissions at `0o700`/`0o600`.
- **FTS projections and their rowid maps are maintained manually** (no SQLite
  triggers). Any new write path in `ctx-history-store` must update the search
  projections or search silently misses data, and must maintain the
  `record_search_rowids`/`event_search_rowids` maps in the same write
  transaction. The maps are performance caches keyed by explicit
  SQLite-assigned FTS rowids — never inferred from base-table rowids, never a
  search-correctness input. Unmapped or stale entries heal lazily via the
  legacy full-scan delete; refresh/rebuild clears and repopulates maps in
  lockstep with the projections.
- **Schema versioning:** upstream chain is v1–v15; this fork diverged at
  v1000 (durable FTS rowid map tables, no rebuild or backfill at migration)
  and continued with v1001 (bounded-pagination covering indexes, reconciled
  from #195; no map or FTS rebuild), v1002 (keyed, path-free advisory
  source-health ledger), v1003 (path-free automatic-refresh state and leases),
  then v1004 (bounded path-free OpenCode incremental state; no backfill or
  search changes), then v1005 (path-free selective-archive and reimport
  suppression ledger; no backfill or search changes). Writable opens migrate
  ≤v15 and v1000–v1004 stores; versions 16–999 and >1005 fail closed without
  mutation, and read-only opens require exactly v1005. Future fork migrations
  continue from 1006. The
  open-time gate is load-bearing for the map invariants; do
  not weaken it (it cannot evict pre-upgrade processes that already hold a
  connection; restart long-lived ctx processes such as `ctx mcp` after
  upgrading). New provider strings require a
  CHECK-constraint rebuild migration — prefer the external history-source
  plugin format (`ctx-history-jsonl-v1`) over in-tree adapters.
- **Tests must not touch the network or the real home directory.** Integration
  tests run against temp homes; keep it that way. The suite must pass on
  macOS, not just Linux.

## Release flow

- The routine release interface is exactly these two commands; Tiny should run
  the readiness check first and proceed only when it succeeds:

    nix run .#release -- --version X.Y.Z --check
    nix run .#release -- --version X.Y.Z [--submit-linux-build]

  The non-mutating check fails fast on a dirty/stale/diverged checkout, missing
  origin or SourceHut authentication, an invalid/downgrade version, and local or
  remote tag conflicts. The release then prepares the versioned tree; validates
  that prepared tree with fmt, clippy, tests, and docs; builds and verifies the
  artifact and checksum; and atomically publishes `main` plus an annotated tag
  with a lease. Artifact uploads and the downloads-site refresh are idempotent;
  Linux submission is also idempotent when requested. If publication succeeded
  but a later step failed, rerun the exact same command: only an exact matching
  version, tag, and main state resumes, while mismatches fail closed. Successful
  publication leaves a new empty `@` above `main`.
- `builds/release-linux-x86_64.yml` builds the Linux `release-artifact` and
  uploads it and requests a downloads-site refresh. It lives in `builds/` (not
  `.builds/`) so it does not auto-run on every push; request it through the
  routine release command's optional argument.
- `prepare-release`, `release-tag`, `build-pages`, and `publish-pages` are
  lower-level recovery tools only. Do not compose them into the
  normal release path; inspect their help and release state before using one
  during manual recovery.

## Upstream review memory

- Forked at upstream `main` commit `38241f0c` ("Require search intent in
  SDKs", 2026-07-02). Future upstream review starts after `38241f0c`.
- Permanently excluded from porting: analytics/identity, upgrade/self-update/
  release signing, SDKs/protocol/contracts, Bazel and Buildkite plumbing,
  hosted installer scripts, docs marketing gates.
- To refresh upstream refs: `jj git fetch --remote upstream` (exposed as
  `main@upstream`).

<!-- Last audited: 2026-07-03 | phases 1-4 landed 2026-07-03 -->
