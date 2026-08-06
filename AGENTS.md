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
  v1000 (durable FTS rowid map tables, no rebuild or backfill at migration).
  **v1000 is taken by the landed rowid-map migration and is authoritative;**
  two v1000 schemas cannot coexist. Binaries whose migration chain ends at
  v15 refuse to newly open a maps-v1000 store both read-only and read-write.
  The unpublished, pre-rebase local #195 binary is different: it also stamps
  v1000, so it is not technically rejected and would silently assume its
  different schema. It must never be built or run against a store migrated by
  this change, and #195 must be rebased onto this authoritative v1000 as v1001
  before its next build, run, or ship. Future fork migrations continue from
  1001. The v15 open-time gate is load-bearing for the map invariants; do not
  weaken it (it cannot evict pre-upgrade processes that already hold a
  connection; restart long-lived ctx processes such as `ctx mcp` after
  upgrading). New provider strings require a
  CHECK-constraint rebuild migration — prefer the external history-source
  plugin format (`ctx-history-jsonl-v1`) over in-tree adapters.
- **Tests must not touch the network or the real home directory.** Integration
  tests run against temp homes; keep it that way. The suite must pass on
  macOS, not just Linux.

## Release flow

- Standard interface (no hand-editing manifests, no host jj aliases — the old
  host-level `jj tag-push` is superseded by `nix run .#release-tag`):
  - `nix run .#prepare-release -- --version X.Y.Z` bumps
    `crates/ctx-cli/Cargo.toml` + `Cargo.lock`, converts the CHANGELOG.md
    `## Unreleased` section into a dated `## vX.Y.Z` entry (generating bullets
    from conventional commits if it is empty), and rewrites the artifact
    filenames in `builds/release-linux-x86_64.yml`.
  - `nix run .#release-tag` creates `vX.Y.Z` from the Cargo.toml version via
    `jj tag set` and pushes it to origin.
  - `nix run .#release -- --version X.Y.Z [--publish-pages]
    [--submit-linux-build] [--skip-validate|--skip-tag|--skip-artifact|--skip-pages]`
    orchestrates the whole flow: prepare → validate (`ci-fmt`, `ci-clippy`,
    `ci-test`, `ci-docs`) → tag + move the `main` bookmark → build the local
    `release-artifact` into `dist/downloads/` → `build-pages` (pages publish
    and Linux build submission are opt-in flags).
- `builds/release-linux-x86_64.yml` builds the Linux `release-artifact` and
  publishes the downloads page (https://averagechris.srht.site/ctx/) via
  `build-pages` + `publish-pages`. It lives in `builds/` (not `.builds/`) so
  it does not auto-run on every push; submit it explicitly with
  `nix run .#release -- --submit-linux-build` or
  `srht builds submit --secrets builds/release-linux-x86_64.yml`.
- The darwin artifact is built/published locally by the orchestrator, or by
  hand: `nix build .#release-artifact`, copy the outputs into
  `dist/downloads/`, then `nix run .#build-pages -- --include-existing-downloads`
  and `nix run .#publish-pages`.

## Upstream review memory

- Forked at upstream `main` commit `38241f0c` ("Require search intent in
  SDKs", 2026-07-02). Future upstream review starts after `38241f0c`.
- Permanently excluded from porting: analytics/identity, upgrade/self-update/
  release signing, SDKs/protocol/contracts, Bazel and Buildkite plumbing,
  hosted installer scripts, docs marketing gates.
- To refresh upstream refs: `jj git fetch --remote upstream` (exposed as
  `main@upstream`).

<!-- Last audited: 2026-07-03 | phases 1-4 landed 2026-07-03 -->
