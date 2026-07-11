# Search

`ctx search` finds matching indexed history. Default results are session-diverse:
ctx shows the strongest matching span from each session, then lets you drill
into dense event-level results when needed. By default it first performs a quiet
best-effort refresh of discovered native provider sources and enabled auto
history-source plugins, then queries the local SQLite store.

## Search

Examples:

```bash
ctx search "build failure"
ctx search "sqlite storage" --provider codex
ctx search "retry handling" --workspace checkout --since 60d
ctx search "tool output" --event-type tool_output
ctx search --file crates/foo/src/lib.rs
ctx search "token budget" --refresh off
ctx search "signed metadata" --term checksum --term release
ctx search "exact adjacent words" --match phrase
ctx search "one of these words" --match any
ctx search "token budget" --limit 5
ctx search "token budget" --session <ctx-session-id>
ctx search "review findings" --include-subagents
ctx search "this current task" --include-current-session
```

A result can include:

- `ctx_event_id`, the ctx-owned event ID for event hits;
- `ctx_session_id`, the ctx-owned session ID when known;
- `provider_session_id`, the provider-owned session ID when known;
- title or event label;
- snippet with truncation where needed;
- rank, result scope, and match reasons;
- session importance and more-matches count for default session results;
- provider;
- event sequence;
- timestamp;
- working directory when known;
- source path and cursor when available;
- source availability flag when known;
- citations;
- `suggested_next_commands`, copyable commands for `ctx show`, `ctx locate`,
  and scoped follow-up searches.

Search result IDs are ctx-owned. Commands accept full ctx IDs or unambiguous
ctx ID prefixes of at least eight hex digits. Prefixes are case-insensitive and
may be compact (`abcdef12`, `abcdef123`) or canonical-hyphenated
(`abcdef12-`, `abcdef12-3`); if hyphens are present, they must be in canonical
UUID positions and spell an exact prefix of the canonical UUID. The minimum is
counted by hex digits, so `abcd` is too short while the trailing canonical
hyphen in `abcdef12-` is accepted. Provider-owned IDs are
exposed as metadata so humans can recognize the original provider session, but
they are not positional lookup IDs. Provider-owned lookup must be explicit, for
example `--provider codex --provider-session <provider-session-id>` on commands
that support it.

## Match modes

`ctx search` treats the positional query and each repeated `--term` as separate
query clauses. Clauses are combined with OR: a result may match the positional
query, any `--term`, or several of them. Filters such as provider, source,
workspace, time, event type, file, session, subagent/current-session flags, and
result mode are combined with AND and never broaden a query. Result `--limit` is
global after OR-clause merging.

Inside one clause, `--match` controls how normalized words match one indexed
event/search section:

- `--match all` is the default. Every normalized token in the clause must appear
  in the same indexed section; order and adjacency are not required.
- `--match any` matches when at least one normalized token appears. Ranking
  rewards sections that match more tokens, within existing result limits.
- `--match phrase` requires the normalized tokens to appear ordered and adjacent
  in the same indexed section.

Tokenization uses ctx portable literal tokens: letters and numbers form
tokens, diacritics are not folded, and punctuation such as `_`, `-`, `/`, `.`, quotes, `*`, and `:` separates
tokens. Input is always literal text. Operator-looking words or syntax such as
`OR`, `NOT`, `title:body`, or `star*` are normalized/quoted and are not accepted
as raw SQLite FTS operators. For example, `write_to_file`, `write-to-file`, and
`write to file` normalize to the same three tokens; `--match phrase` requires
`write to file` adjacency after normalization.

ctx never silently retries a broader mode. If a multiword text search has no
results, human output may print a labeled `suggestion (not run)` command that
broadens exactly one step (`phrase -> all`, `all -> any`; no suggestion for
`any`) while preserving active filters, repeated `--term` clauses, result flags,
refresh mode, verbosity, and `--limit`.

## Filters

Search filters narrow both human output and JSON:

- `--provider codex|pi|claude|opencode|openclaw|hermes|nanoclaw|astrbot|antigravity|gemini|cursor|copilot-cli|factory-ai-droid`;
- `--history-source <plugin/source-or-provider_key/source_id>`, for custom
  history imports;
- `--provider-key <key>`, `--source-id <id>`, and
  `--source-format <format>`, for exact custom history source filters;
- `--workspace <name-or-path>`, substring match over stored workspace, cwd,
  source path, or repository-name text;
- `--since <rfc3339-or-days>d`;
- `--event-type <event-type>`, one of `message`, `tool_call`, `tool_output`,
  `command_started`, `command_output`, `command_finished`, `file_touched`,
  `vcs_change`, `artifact`, `summary`, or `notice`;
- `--file <path>`, indexed touched-file path metadata, not the current
  filesystem;
- `--session <ctx-session-id-or-prefix>`;
- `--match all|any|phrase`, controls matching within each positional query or `--term` clause;
- `--term <query-or-keyword>`, repeatable broadening clauses merged with OR-style
  semantics, not required terms;
- `--events`;
- `--include-subagents`;
- `--limit <n>`;
- `--refresh auto|off|strict`;
- `--include-current-session`.

CLI provider filters use the kebab-case names above. JSON output and stable SQL
views use provider IDs in ctx output; multiword provider IDs may be snake_case,
such as `copilot_cli` or `factory_ai_droid`, while compact IDs such as
`openclaw`, `nanoclaw`, and `astrbot` stay compact.

`--since` accepts RFC 3339 timestamps such as `2026-06-01T00:00:00Z` or a day
window such as `30d`.

`--file <path>` filters by normalized `files_touched` metadata when provider
transcripts expose touched paths. Use it without a query to list indexed events
for a file, or combine it with query terms to find sessions that both mention a
topic and touched that path. It searches paths recorded during import; it does
not inspect the current filesystem.

Search requires a non-empty query, at least one non-empty `--term`, or
`--file <path>`. Provider, workspace, time, session, event, source, and result
flags only narrow an actual search; by themselves they do not browse recent
history.

The default searches primary-agent sessions so human intent and decisions stay
prominent. Use `--include-subagents` when you want implementation details, code
review notes, test output, or failure analysis from subagent sessions too.

`--limit` defaults to `20` and is capped at `200`.

Default search returns diverse session-level results. Use
`--session <ctx-session-id>` after a default search has identified a session to
inspect; scoped session search returns dense event hits. Use `--events` without
`--session` when you want dense event hits across sessions.

When ctx is run from Codex and `CODEX_THREAD_ID` is available, search excludes
the active Codex session tree by default so the current prompt and its subagent
work do not dominate history research. Use `--include-current-session` when you
are intentionally looking for material from the active session tree.

`--refresh` defaults to `auto`. `auto` attempts a best-effort pre-search import
of discovered native provider sources and enabled auto history-source plugins,
then serves the existing index if that refresh fails. On large discovered
sources or already-cataloged indexes, `auto` serves current results without a
foreground catch-up scan; use `--refresh strict` or `ctx import --all` when you
need a full catch-up before querying. `off` skips the pre-search refresh and
never runs plugin commands. `strict` fails the search if the refresh cannot run
or import successfully. Preview native sources such as NanoClaw and AstrBot,
plus search-only sources without native import support, are searched from the
existing index until they are explicitly imported through a supported path.

Use `--refresh off` for a strictly read-only search over the existing ctx index.
This avoids provider imports, plugin execution, and updates to the ctx SQLite
store.

## History Reports

Use the agent history-search skill when a topic needs a cited report instead of
a ranked hit list. The skill should run several `ctx search` queries, inspect the
best cited events or sessions with `ctx show`, and write the report itself. ctx
only retrieves indexed local evidence; it does not synthesize conclusions.

## Machine Output

Use default text output for agent reading. Use `ctx search <query> --json` or a
term/file search with `--json` for scripts, `jq`, or exact field extraction.
JSON results include the same result metadata and citations as the human output,
plus top-level `query_plan`, optional no-result `broadened_search`, and `freshness` objects
describing the pre-search refresh mode and outcome. A citation with
`source_exists: false` means ctx can return indexed text, but the raw provider
file was not available at the stored path when the result was built.

Search output is local/private by default and is not redacted for sharing.
Review and redact copied snippets, JSON, or transcripts before sending them
outside the machine.


Relevance note: `--match any` rewards results that match more normalized tokens, but ctx reranks only a bounded candidate pool (at least `max(limit*8, 50)` candidates on unfiltered fast/fallback paths, subject to existing scan caps), not the entire historical corpus.
