# Security Checks

This page defines the checks public docs and validation should keep true for
the local retrieval product.

## Required Invariants

- `ctx setup` reads supported provider history and writes only the configured
  ctx data root and SQLite index.
- `ctx sources` writes nothing in local-only security mode.
- `ctx import` writes only the configured ctx data root and SQLite index.
- `ctx search` may refresh discovered native provider history into the
  configured ctx data root before querying.
- `ctx show` and `ctx locate` write nothing in local-only security mode, except
  `ctx show session --out` writes only the explicit path when one is provided.
- `ctx sql` opens only the existing SQLite index and rejects write statements
  and multiple statements.
- In local-only security mode, setup/import/search do not use network access or
  API keys.
- `ctx docs` reads embedded documentation and writes only an explicit topic
  output path for `ctx docs show --out` or an explicit man-page output
  directory when `ctx docs man --out` is used.
- The binary makes no network calls; there is no telemetry and no self-update.
  Updates come from Nix / SourceHut release tags.
- Provider files are read as sources and not modified.
- Provider transcript imports reject symlinked JSONL files by default.
- JSON output is private by default and must not be described as share-safe.
- Search/show/locate JSON and SQLite search projections preserve local
  transcript text by default, including absolute paths and secret-shaped
  strings. They must be treated as private local data.
- Search/show continuation tokens are not persisted by ctx, contain no transcript
  or provider/path/query metadata, and are validated against the current request
  plus a local SQLite snapshot. Read-only search/show/MCP paths must reject old
  schemas rather than migrate or write.
- The legacy `safe_preview` state and `safe_preview_text` columns mean local
  searchable preview text, not share-safe redaction.
- Unsupported providers remain explicit in the provider support matrix.

## Static Docs Checks

Public docs should avoid claims for capabilities outside the product contract.
Run the repository docs check, which scans public copy for removed or unsupported
product surfaces:

```bash
bash scripts/check-docs.sh
```

Validate the provider matrix JSON:

```bash
jq empty docs/provider-support-matrix.json
```

## Transcript Preservation Checks

The workspace test suite imports synthetic provider histories with fake
secret-shaped values, then checks `search`, `show`, and SQLite search
projections preserve local transcript text and do not claim to be share-safe:

```bash
cargo test --workspace
```

## Mode Placement

Security-sensitive product changes should run the full check set described in
[`docs/testing-taxonomy.md`](testing-taxonomy.md) (fmt, clippy, workspace
tests, and the docs check).

The default retrieval boundary remains local provider-history search. Security
docs and tests should continue to reject claims that setup, import, search, or
doctor need remote accounts, provider-history background collection,
repository mutation, or API keys.

## Manual Review Checklist

- README scope matches `docs/product-contract.md`.
- CLI examples use flags implemented by `crates/ctx-cli`.
- Provider support docs match `docs/provider-support-matrix.json`.
- Testing taxonomy keeps the public command surface focused on local search and
  static smoke coverage.
- JSON docs identify local/private output and compatibility limits.
- Symlink policy stays explicit: provider transcript symlinks are rejected unless
  a future change adds canonical root-contained symlink support with tests.
- Security docs do not promise default local sanitization. Share-safe or
  shared-service redaction requires an explicit future mode.
- Public docs do not make strict no-network claims except when describing
  local-only security mode or this fork's no-telemetry, no-self-update binary.
