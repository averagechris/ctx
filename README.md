# ctx

ctx is a local CLI that indexes your past coding-agent sessions (Claude Code,
Codex, Cursor, OpenCode, and others) into a SQLite database on your machine,
then gives you and your agents fast search, read-only SQL, and an MCP server
over that history.

This is a hard fork of [ctxrs/ctx](https://github.com/ctxrs/ctx) maintained at
[https://github.com/averagechris/ctx](https://github.com/averagechris/ctx).
The fork makes **no network calls**: telemetry, self-update, and all hosted
endpoints were removed. Updates ship as annotated GitHub tags and manually
verified GitHub Release assets, and are managed by Nix. See
[docs/fork-plan.md](docs/fork-plan.md) for the decision record.

## Install

With Nix (flakes):

```bash
# run directly from the latest release tag
nix run 'github:averagechris/ctx/v1.2.0'

# or track main
nix run github:averagechris/ctx

# or build locally
nix build .#ctx
```

Or add `github:averagechris/ctx` as a flake input.

Release downloads and checksums are attached to
[GitHub Releases](https://github.com/averagechris/ctx/releases). The manual
release policy is documented in [docs/release.md](docs/release.md).

From source with cargo:

```bash
cargo build --release
# binary at target/release/ctx
```

## Quick start

```bash
# discover local agent history and index it
ctx setup

# search prior sessions with normal language
ctx search "failed migration"

# inspect the exact index data with read-only SQL
ctx sql "SELECT provider, COUNT(*) AS sessions FROM ctx_sessions GROUP BY provider"

# serve search/SQL to agents over MCP
ctx mcp
```

Search results include session and event IDs; use `ctx show event <id>` or
`ctx show session <id>` to recover the original transcript context.

## Docs

| Page | What it covers |
| --- | --- |
| [Getting started](docs/getting-started.md) | Install ctx, initialize local storage, and index discovered local history. |
| [Search](docs/search.md) | Query syntax, filters, and how ranking works. |
| [SQL](docs/sql.md) | The read-only SQL surface and schema. |
| [MCP](docs/mcp.md) | Serving ctx tools to agents over MCP. |
| [Providers](docs/providers.md) | Which agent histories ctx can discover, import, and search. |
| [Large-index profile](docs/large-index-profile.md) | Deterministic bounded synthetic profile harness and manual large-run commands. |
| [Portable archive workflows](docs/archive.md) | Create, verify, and restore private logical archives into fresh data roots. |
| [Archive format v1](docs/archive-format-v1.md) | Normative container and stream contract for the portable, checksummed logical archive. |
| [Fork plan](docs/fork-plan.md) | Why this fork exists and what was removed. |

## Privacy

The index is local-only, but transcripts are stored **verbatim** with no
redaction: `~/.ctx/work.sqlite` contains anything ever pasted into an agent
session, including secrets. Treat it accordingly, and review copied output
before sharing it outside the machine. See
[docs/privacy-storage.md](docs/privacy-storage.md).

## License

Apache-2.0, same as upstream. See [LICENSE](LICENSE).
