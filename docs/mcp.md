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

MCP search and SQL query the existing index only. They do not refresh provider
history, import files, initialize storage, or write provider data.

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

MCP `search` also accepts the CLI's role and tool-noise filters as optional
arguments with permissive defaults: `role` and `exclude_role` (arrays of role
names such as `user`, `assistant`, or `tool`), `exclude_tool_noise` (boolean,
default `false`), and `exclude_tool` (array of executable names such as `ctx`,
matching the repeatable `--exclude-tool` CLI flag). Omitting them searches all
roles and keeps tool evidence, exactly like the CLI defaults.

The MCP `sql` tool uses the same read-only stable views and result limits as
`ctx sql --json`. Prefer stable `ctx_*` views for scripts and agent workflows.
Run `ctx docs show sql` for the view schemas and examples.

Tool results include MCP text content plus `structuredContent` JSON. Treat all
MCP output as private local history: it may include absolute paths, source
metadata, snippets, transcript text, and raw SQL result fields, and the MCP host
may log or forward tool output.
