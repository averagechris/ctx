# MCP

`ctx mcp serve` starts a read-only MCP server over newline-delimited stdio
JSON-RPC. It is for agents or MCP hosts that prefer tool discovery over shell
commands. The CLI remains the primary interface.

```bash
ctx mcp serve
```

The server exposes these tools:

- `status`, local ctx index status;
- `sources`, discovered local agent history sources;
- `search`, search the existing index;
- `sql`, run one read-only SQL statement against the existing index;
- `show_session`, return an indexed session transcript by ctx session ID;
- `show_event`, return an indexed event and optional surrounding window by ctx
  event ID.

MCP `search.session`, `show_session.ctx_session_id`, and
`show_event.ctx_event_id` accept the same ctx-owned ID spellings as compatible
CLI commands: full UUIDs, case-insensitive compact prefixes with at least 8 hex
digits, and canonical-hyphenated prefixes (including a trailing canonical
separator such as `abcdef12-`). Hyphens must appear only in canonical UUID
positions.

MCP search and SQL query the existing index only. They do not refresh provider
history, import files, initialize storage, or write provider data.

MCP `search` and `show_session` use the same #195 query-owned pagination as CLI
JSON. They accept `continue`, `fields`, `max_snippet_bytes`/`max_event_bytes`,
and `max_page_bytes`, and return one bounded `structuredContent` response with
the same `pagination`, exact omitted/range fields, byte accounting, `next`,
`next_command`, and `next_argv` semantics. They also return canonical
`next_arguments`, which can be passed directly to the same MCP tool. Relative
search windows such as `30d` are frozen there as absolute RFC3339 timestamps.
MCP search always behaves like `--refresh off`; generated continuation argv
also forces `--refresh off`.
Defaults/caps match the CLI: search limit 20/200, show limit 200/1000,
per-item bytes 4096/1048576, and page bytes 262144/16777216. The page byte
budget is only the exact sum of admitted final item JSON bytes, including full
compatibility aliases and suggested commands; fixed
response metadata is not capped as a whole-output budget.

Continuation tokens are opaque, bind the complete request and a conservative
SQLite snapshot, contain no private snippets/paths/query/provider metadata, and
fail closed when malformed, wrong-kind, mismatched, or stale. `show_session`
tokens do carry an opaque event ordering key inside the hex token for keyset
resumption. Read-only MCP opens reject old schemas and never migrate or write.

Repeated search input is limited at schema and runtime to 32 `terms`, 4096
UTF-8 bytes per query clause, and 65536 aggregate query bytes. Raw duplicate and
blank repeated clauses count toward the request-level clause limit.

MCP `search` accepts `match: "all"|"any"|"phrase"`, matching the CLI. The
positional `query` is one clause. `all` is the default and requires every
normalized token in that clause to appear in one indexed section; order and
adjacency are not required. `any` requires at least one token and rewards more
matched tokens. `phrase` requires normalized tokens to be ordered and adjacent.
Tokenization uses ctx portable literal tokens: letters/numbers are
tokens, diacritics are not folded, and punctuation such as `_`, `-`, `/`, `.`, quotes, `*`, and `:` separates
tokens; operator-looking input such as `OR`, `NOT`, `title:body`, or `star*` is
literal, not raw FTS syntax. Search results include structured `query_plan` with
mode, normalized clauses, OR between clauses, and AND filters. MCP does not emit
CLI shell broadening commands.

MCP search defaults to primary-agent sessions only, matching `ctx search`. Pass
`include_subagents: true` when implementation details, code review notes, test
output, or failure traces from subagent sessions are relevant. When
`CODEX_THREAD_ID` is set, MCP search also excludes the active Codex session tree
by default; pass `include_current_session: true` when the active session tree is
the target.

The MCP `sql` tool uses the same read-only stable views and result limits as
`ctx sql --json`. Prefer stable `ctx_*` views for scripts and agent workflows.
Run `ctx docs show sql` for the view schemas and examples.

Tool results include MCP text content plus `structuredContent` JSON. Treat all
MCP output as private local history: it may include absolute paths, source
metadata, snippets, transcript text, and raw SQL result fields, and the MCP host
may log or forward tool output.

`fields: "compact"` is structurally smaller but still private. It excludes
provider-session IDs, source IDs/metadata/path/existence, cwd, cursors,
citations, raw payload, and suggested commands from the query projections; full
output can include them.
