# Fork Plan: ctx → ~averagechris/ctx

Decision record for forking [ctxrs/ctx](https://github.com/ctxrs/ctx) (v0.17.0,
forked at upstream `38241f0c`) as a personal/team agent context layer, packaged
with Nix, hosted on SourceHut, and maintained in the style of
`~averagechris/linear-cli`.

## Why fork

ctx is a disciplined, dependency-lean Rust CLI (~51k lines, 7 crates,
Apache-2.0) that indexes local coding-agent session history into SQLite and
exposes deterministic search, read-only SQL, and an MCP server. An audit
(2026-07-02, full workspace build + test run verified locally) found:

- **Strong**: fully parameterized SQL; 5-layer read-only SQL sandbox; symlink
  rejection in importers; minimal sound `unsafe`; zero `unwrap()` outside
  tests; ~268 tests with a 110-test behavioral CLI suite; honest docs that are
  CI-gated against the code.
- **Unacceptable for this fork**: opt-out telemetry with persistent device IDs
  posting to `cli.ctx.rs` on nearly every command; background self-upgrade
  (`auto = "apply"`) that silently replaces the binary from ctx.rs
  infrastructure; curl|sh install flow whose signature verification lives only
  in an unauditable hosted script.
- **Dead weight for our use**: 6 language SDKs + `ctx-sdk` + `ctx-protocol` +
  `contracts/` (typed subprocess wrappers, unused by the CLI); fake Bazel
  wrapper around cargo; Buildkite CI on private queues; speculative hosted-sync
  schema scaffolding.
- **Known risks we accept**: transcripts are indexed verbatim (no redaction) —
  `work.sqlite` is effectively a secrets store; treat the MCP `sql`/`search`
  tools as able to surface anything ever pasted into an agent session. The
  four monolith files (`capture/lib.rs` 14k, `store/lib.rs` 9k, `main.rs`
  6.5k, `search/lib.rs` 5.4k) make upstream merges expensive; this is a hard
  fork that ports upstream fixes selectively, not a tracking fork.

## Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Name / branding | Keep `ctx`, `~/.ctx` data root, `CTX_` env prefix. Remove all `ctx.rs`/`cli.ctx.rs` endpoints. Repo: `git.sr.ht/~averagechris/ctx`. |
| 2 | Telemetry | **Delete entirely** — `analytics.rs`, `identity.rs`, call sites, config keys, env vars, tests, docs. No first-party analytics in this fork, ever. |
| 3 | Self-upgrade | **Delete entirely** — `upgrade.rs`, `ctx upgrade`, install markers, release-metadata signing plumbing. Nix owns the binary lifecycle; releases are SourceHut `vX.Y.Z` tags. |
| 4 | SDKs | **Drop all** — `sdks/`, `crates/ctx-sdk`, `crates/ctx-protocol`, `contracts/`. CLI `--json` output + MCP is the programmatic surface. Recoverable from upstream history if ever needed. |
| 5 | Build/CI | Delete Bazel (`MODULE.bazel*`, `BUILD.bazel`, `.bazelrc`, `.bazelversion`) and `.buildkite/`. Replace with a Nix flake (`nix run .#ci-fmt` / `.#ci-clippy` / `.#ci-test`, `nix flake check`) and a SourceHut `.builds` manifest, following linear-cli. |
| 6 | Install scripts | Delete `scripts/install.sh` / `install.ps1`. Install via `nix run` / flake / release tarballs. |
| 7 | Providers | **Deferred.** No new providers for now; when needed, prefer the external history-source plugin format (`ctx-history-jsonl-v1`) over in-tree adapters. Do not restructure the provider CHECK constraints yet. |
| 8 | MCP `sql` tool | Keep. Document the secrets-exposure risk; this is a personal/trusted-team tool. |
| 9 | Schema | Leave upstream schema v15 untouched for now. If we ever diverge, start our migration numbering at 1000 to avoid colliding with upstream's chain. |
| 10 | VCS | jj. Remote `upstream` = github.com/ctxrs/ctx (fetch-only for selective porting), remote `origin` = git.sr.ht/~averagechris/ctx. Trunk bookmark: `main`. |
| 11 | Versioning | Rebaselined to `1.0.0` at first fork release. Upstream stayed at 0.x; a disjoint major makes fork releases unambiguous. Releases are SourceHut `vX.Y.Z` tags on `main`. |

## Plan

### Phase 1 — de-network (supply-chain hardening)
- Remove `crates/ctx-cli/src/analytics.rs`, `identity.rs`, `upgrade.rs`,
  `net.rs` and every call site in `main.rs` / `config.rs` / `mcp.rs`.
- Remove `[analytics]` / `[upgrade]` config sections, `CTX_ANALYTICS_*` /
  `CTX_UPGRADE_*` / release-signing env vars, and the `ctx upgrade` and
  `ctx analytics` command surface.
- Drop now-unused deps: `ureq`, `ring`, `base64` (if unused elsewhere), and the
  already-dead `ed25519-dalek` and `url` workspace deps.
- Remove/adjust the related integration tests and docs
  (`docs/upgrade.md`, analytics sections of `docs/storage.md`, etc.).
- Post-condition: the binary makes **zero network calls**. `rg 'ctx\.rs'`
  finds no live endpoints.

### Phase 2 — prune
- Delete `sdks/`, `crates/ctx-sdk`, `crates/ctx-protocol`, `contracts/`,
  Bazel files, `.buildkite/`, install scripts, and the check scripts that
  exist only to serve them (`check-sdks.sh`, `sdk-*.sh`,
  `check-agent-history-contract.py`, `check-buildkite-pipeline.sh`,
  `bazel-test.sh`, marketing-phrase docs gates, etc.).
- Trim workspace `Cargo.toml` members and dependencies accordingly.
- Post-condition: `cargo test --workspace` passes; repo contains only what the
  CLI needs.

### Phase 3 — tests green on macOS
- Fix the 3 macOS-failing CLI tests (they assume `XDG_STATE_HOME` is honored;
  the `directories` crate ignores it on macOS). Analytics tests disappear with
  Phase 1; fix or platform-gate the remainder
  (`provider_json_names_are_accepted_as_cli_filter_aliases`).
- Post-condition: full workspace test suite passes on macOS (darwin is the
  primary dev target).

### Phase 4 — Nix + SourceHut plumbing
- `flake.nix` (nixpkgs unstable + flake-utils, `cargoLock`, package + devShell,
  `ci-fmt` / `ci-clippy` / `ci-test` apps, `fetch-upstream` helper running
  `jj git fetch --remote upstream`), `.envrc` (`use flake`), `.gitattributes`.
- `.builds/` SourceHut manifest running the Nix CI checks.
- Rewrite `README.md` for the fork (install via Nix, no hosted installer);
  update `AGENTS.md` guardrails; prune docs that describe deleted surfaces.

### Later / explicitly deferred
- New providers (via plugin format), provider CHECK-constraint refactor,
  monolith file splits, release artifact publishing + pages, coverage tooling,
  fuzzing the importers, redaction options.

## Upstream review memory

- Forked at `38241f0c` ("Require search intent in SDKs", 2026-07-02,
  upstream `main`). Future upstream reviews start after that commit.
- Permanently exclude from porting: analytics/identity/telemetry, upgrade/
  self-update/release-signing, SDKs/protocol/contracts, Bazel/Buildkite,
  hosted installer scripts, marketing docs gates.
