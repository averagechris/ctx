# AGENTS.md

This is a hard fork of [ctxrs/ctx](https://github.com/ctxrs/ctx) maintained at
`git.sr.ht/~averagechris/ctx`. Decision record and phased plan:
[docs/fork-plan.md](docs/fork-plan.md).

## Working in this repo

- Use `jj`, not `git`. Trunk bookmark is `main` → remote `origin`
  (`git@git.sr.ht:~averagechris/ctx`). Remote `upstream` is
  github.com/ctxrs/ctx and is fetch-only, used for selectively porting fixes.
- Build/test with cargo (via the flake devShell once Phase 4 lands):
  `cargo fmt --all --check`, `cargo clippy --locked --all-targets -- -D warnings`,
  `cargo test --workspace`.
- `crates/ctx-cli/tests/cli.rs` is the behavioral contract — lean on it when
  refactoring; extend it when changing command behavior.

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
- **FTS projections are maintained manually** (no SQLite triggers). Any new
  write path in `ctx-history-store` must update the search projections or
  search silently misses data.
- **Schema versioning:** upstream chain is v1–v15. If this fork ever diverges
  the schema, start at version 1000. New provider strings require a
  CHECK-constraint rebuild migration — prefer the external history-source
  plugin format (`ctx-history-jsonl-v1`) over in-tree adapters.
- **Tests must not touch the network or the real home directory.** Integration
  tests run against temp homes; keep it that way. The suite must pass on
  macOS, not just Linux.

## Upstream review memory

- Forked at upstream `main` commit `38241f0c` ("Require search intent in
  SDKs", 2026-07-02). Future upstream review starts after `38241f0c`.
- Permanently excluded from porting: analytics/identity, upgrade/self-update/
  release signing, SDKs/protocol/contracts, Bazel and Buildkite plumbing,
  hosted installer scripts, docs marketing gates.
- To refresh upstream refs: `jj git fetch --remote upstream` (exposed as
  `main@upstream`).

<!-- Last audited: 2026-07-02 | fork plan committed; phases 1-4 pending -->
