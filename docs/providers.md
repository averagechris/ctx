# Providers

ctx imports existing agent history through provider adapters. Each adapter must
make a narrow, testable claim about the source format it reads and the event
fields it indexes.

## Supported Local Imports

The current CLI imports local history for:

- Codex session JSONL trees under `~/.codex/sessions`;
- Codex `~/.codex/history.jsonl`;
- Pi `~/.pi/sessions.jsonl` when that local file exists and matches the
  supported JSONL format;
- Claude Code project JSONL transcripts under `~/.claude/projects`;
- OpenCode SQLite history under `~/.local/share/opencode/opencode.db`;
- OpenClaw session JSONL trees under `OPENCLAW_STATE_DIR`, `~/.openclaw`,
  legacy `~/.clawdbot`, or legacy `~/.moltbot`;
- Hermes Agent SQLite history under `HERMES_HOME/state.db` or
  `~/.hermes/state.db`;
- NanoClaw project history from a project root with `data/v2.db` and
  `data/v2-sessions` when imported explicitly;
- AstrBot local SQLite history from `ASTRBOT_ROOT/data/data_v4.db`,
  `~/.astrbot/data/data_v4.db`, or a project `data/data_v4.db` when imported
  explicitly;
- Antigravity transcript JSONL mirrors under
  `~/.gemini/antigravity-cli/brain/*/.system_generated/logs/transcript_full.jsonl`
  or `transcript.jsonl`;
- Gemini CLI chat JSONL records under `~/.gemini/tmp/**/chats/**/*.jsonl`;
- Cursor CLI agent transcript JSONL files under
  `~/.cursor/projects/**/agent-transcripts/**/*.jsonl`;
- Copilot CLI session event logs named `events.jsonl` under
  `~/.copilot/session-state`;
- Factory AI Droid session JSONL files under `~/.factory/sessions`.

These are built-in provider adapters for native local history. The custom
history format is separate: `ctx import --format ctx-history-jsonl-v1 --path
<file>` reads an explicit JSONL interchange file from any exporter, and
history-source plugins can stream the same format from local adapter commands.
Custom history is stored internally under the bounded provider `custom` while
preserving the exporter's `provider_key`, `source_id`, and `session_id` as
metadata and ID namespace components. File imports are not auto-discovered;
local plugin manifests are listed by `ctx sources`.

Use `ctx sources` for the truth on the current machine:

```bash
ctx sources
ctx sources --json
```

CLI provider flags use names such as `openclaw`, `hermes`, `nanoclaw`,
`astrbot`, `copilot-cli`, and `factory-ai-droid`.
Structured JSON and stable SQL views use provider IDs in ctx output; multiword IDs may be
snake_case, such as `copilot_cli` or `factory_ai_droid`, while compact native
IDs such as `openclaw`, `nanoclaw`, and `astrbot` stay compact.

`ctx sources --json` reports each known provider source with `import_support`
and `importable` fields. A native source is marked available/importable only
when provider-specific transcript files exist. Sources with `import_support:
"preview"` are explicit-import preview paths: use `ctx import --provider
nanoclaw` or `ctx import --provider astrbot` when discovery finds the desired
source, or add `--path` to target a specific source before searching it. They
are intentionally excluded from `ctx import --all` and pre-search refresh until
promoted. Sources with
`status: "unknown"` hit the bounded transcript probe budget before proving
history exists, and sources with `import_support: "unsupported"` are detections
or blockers, not importable native history.

If a provider is selected without a proven native importer, `ctx import`
returns a provider-specific native-history blocker. Do not document a provider
as natively locally importable until the CLI can discover or parse that
provider's real local history and the provider support matrix marks the shipped
path accordingly.

## Provider Smoke

Public provider smoke coverage uses static local-history fixtures. It verifies
supported imports, unsupported-provider blockers, provider filtering, citations,
and deterministic search without executing provider CLIs, reading real user
history, requiring API keys, or making network calls:

```bash
cargo test -p ctx --test cli
```

## OpenCode

OpenCode ships from a fast-moving development branch, so the adapter detects
the schema of `opencode.db` at import time (via `sqlite_master` and
`pragma table_info`) instead of assuming a version. The
`__drizzle_migrations` table is recorded only as a diagnostic hint in source
metadata, never used for dispatch.

Source preference, richest first:

- `session` joined with `message` + `part` — the current schema, where the
  full history lives. Parts map to ctx events: `text` → message, `tool` →
  tool_call (with input/output previews), `reasoning`/`compaction` → summary,
  `patch` → file_touched; `step-start`/`step-finish`/`snapshot` bookkeeping
  parts are skipped and reported in import notes. Later-added session columns
  (`workspace_id`, `path`, `agent`, `model`, token counters) are optional.
- `session_message` / `session_entry` rows are also imported, but rows whose
  external IDs already appeared in the `message`/`part` tables are
  deduplicated, so a nearly empty `session_message` table can never mask a
  populated `message`/`part` store and nothing is double-imported.
- In databases where `part` is absent or empty, message content is read
  inline from `message.data` (very old schemas).

Sessions with no message rows are still imported so the session catalog
matches the provider database, and subagent sessions keep their `parent_id`
hierarchy. If the database contains sessions and message rows but the adapter
produces zero events, the import reports a loud schema-mismatch failure
instead of silently succeeding. Import notes (`ctx import --json` `notes`
field) explain everything that was skipped and why.

### Cursor format and migration

Adapter sync cursors are prefixed `opencode-v2:`
(`opencode-v2:message_part:<session_id>:<row_id>` for message/part events,
`opencode-v2:session_message:<session_id>:seq:<n>` for fallback rows).
Versions before the message/part-aware adapter wrote
`session_message:<session_id>:seq:<n>` and could import almost nothing from a
current-schema database. On upgrade, a stored old-format cursor forces one
full rescan of the database (bypassing the unchanged-file manifest check), so
existing history is picked up automatically; event-level deduplication keeps
the rescan idempotent and previously imported stub sessions are not
duplicated. `ctx import --provider opencode --resume` forces the same full
rescan manually.

### Legacy JSON storage

Very old OpenCode versions stored sessions as JSON files under
`~/.local/share/opencode/storage/` (`message/<session_id>/*.json`,
`part/<message_id>/*.json`) before `opencode.db` existed; newer OpenCode
builds migrate that data into the database themselves. ctx does not ship an
in-tree adapter for the JSON tree; if you have unmigrated JSON-only history,
export it through a history-source plugin using `ctx-history-jsonl-v1`.
Databases and storage trees can coexist — the adapter only reads
`opencode.db` and never touches `storage/`.

## Import Rules

Provider imports should be:

- read-only with respect to provider-owned files;
- explicit through `ctx import`;
- safe to interrupt and re-run, using idempotent rescans or provider cursors
  when available;
- idempotent for unchanged source files;
- clear about which fields were indexed and which were left raw-only;
- conservative when a transcript schema is unknown or malformed.

Custom history imports follow the same read-only and idempotent principles, but
their compatibility contract is the `ctx-history-jsonl-v1` schema rather than a
provider-owned native transcript format.

## Fidelity

An imported session may include messages, tool calls, command events, output
previews, file references, parent/child agent relationships, usage metadata, and
lifecycle events. Not every provider exposes every field.

Search output must identify the provider and cite the source path or cursor
when available so an agent can verify important details.
