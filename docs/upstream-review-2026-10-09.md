# Selective upstream review: 2026-10-09

This review records a bounded comparison against upstream `ctxrs/ctx`, respecting this fork's guardrails in `AGENTS.md` and `docs/fork-plan.md`. It is not a blanket merge or a claim that every individual patch in upstream was code-reviewed.

## Watermark and history shape

- Fork review boundary previously recorded: `38241f0c1d167b2f98b358d8d8fa1c37807f66ac` ("Require search intent in SDKs", 2026-07-02).
- Upstream default branch `main` at audit time: `56aaf35233ce7f8aa739c207525a3d67e6ddddbd` (2026-10-05).
- GitHub compare API for `38241f0c1d167b2f98b358d8d8fa1c37807f66ac...main`: `ahead_by=3690`, `behind_by=51`, merge base `647d7889d0a404ca7ebf3ca02a66e174cc2b3165`.
- The old watermark exists, but is not an ancestor of current upstream `main`. Therefore 3,690 is the count of commits unique to the current `main` side in the compare response, not a linear descendant range from the old SHA. There are 51 commits unique to the old review lineage. Do not use the phrase “all commits after the old SHA” as if history were linear.
- The complete metadata inventory for all 3,690 commits in the current-main side is in [`upstream-review-2026-10-09-commits.tsv`](upstream-review-2026-10-09-commits.tsv). It records full SHA, date, subject, and a triage disposition. Dispositions for unselected commits are metadata-based screening outcomes; their individual diffs were not exhaustively inspected. The file is intended to make the reviewed watermark auditable and future candidate selection bounded.

## Selected importer changes

### Codex JSONL line bound

Upstream `b286839ed19bbfe885aff94eb9d842aea1635012` added bounded reads that drain an oversized JSONL row and continue at the next row. The fork's Codex importer previously used unbounded `read_until`, so a malformed or extreme line could allocate without a cap. The fork now applies a 16 MiB per-line bound in its normalization, full-import, and tail-import paths. Oversized event rows are skipped and counted; an oversized tail header remains an explicit error. This reuses the fork's current importer and does not introduce a provider or alter stored schemas.

Behavior evidence: `codex_session_jsonl_skips_oversized_line_and_keeps_following_events` checks that a valid event after an over-limit row still imports. The CLI regression `search_refresh_auto_tail_imports_appended_codex_session_event` passed on an isolated rerun.

### OpenCode SQLite row bound

Upstream `9c8bac08fc89534a19b7e11b3be54e2f7bb57bdc`, `3b6c41508ceae17983889e7c978ef21f2bc9eb56`, and `7b931bb2c1d1916c6bf4f8b8620fc0746f0c62dd` establish the oversized-row handling. Later `ffe973d224468e7a6c17f6f79d67e26ce9d1c3aa` uses a broader SQL projection/hydration design that is not suitable to cherry-pick into this fork. The fork adapts the invariant with a 16 MiB SQL-side bound for OpenCode message, part, and legacy fallback `data` values, avoiding hydration of oversized content into Rust. Oversized rows are counted as skipped so one row does not abort otherwise usable source data. This does not touch provider enum constraints or OpenCode incremental-state schema.

Behavior evidence: `native_opencode_skips_oversized_message_and_part_values` exercises valid rows around oversized rows and all-oversized input. Ensure that genuine SQL NULL values retain the prior importer error behavior; they must not be conflated with oversized rows.

## Explicit exclusions and deferrals

- Telemetry, analytics, identity, update checks, self-upgrade, hosted endpoints, release signing, and install/release automation remain excluded by fork policy.
- SDKs, protocol/wire contracts, `contracts/`, Bazel, Buildkite, hosted installers, and marketing gates remain excluded.
- Provider additions, provider enum/check-constraint changes, and broad importer/provider reorganizations remain deferred. The fork prefers its external history-source plugin format and owns its schema/versioning.
- Upstream semantic indexing, daemon orchestration, Sift/Blame unification, source-backed generations, and attribution redesign are architecture/product work outside the fork's current local CLI and SQLite model. Reconsider only against a concrete fork behavior gap.
- Other commits in the inventory have no selected change in this bounded pass. The inventory records the metadata-screening disposition and distinguishes explicitly excluded areas from deferred or unselected work. Before porting any such change, inspect its full diff against the then-current fork.

## Verification status

The selective changes are implemented in this fork's importer. `nix run .#ci-fmt`, `nix run .#ci-clippy`, `nix run .#ci-test`, `nix run .#ci-docs`, and `nix flake check` passed on 2026-10-09. The Codex oversized-line fixture was then corrected to be valid JSONL (the prior fixture had an extra closing brace), and its focused regression passed again; the correction changes only fixture bytes. `nix flake check` reported that it omitted aarch64-darwin, aarch64-linux, and x86_64-darwin on this host. No upstream merge or cherry-pick is implied.
