Index freshness
===============

`ctx search` treats index freshness as part of the result contract. Human output
prints a `freshness:` line and JSON output includes `freshness.ran`,
`freshness.status`, `freshness.reason`, `freshness.duration_ms`,
`freshness.index_age_seconds`, and import totals including imported, skipped,
unchanged, and failed counts.

`--refresh off` is the strict read-only mode: it does not discover provider
sources, execute history-source plugins, create config/data-root files, open the
store writable, run migrations, import, checkpoint, or write. It only opens an
existing schema-current database read-only and searches it.

Cheap unchanged-source target
-----------------------------

The supported fast path is metadata based: manifested provider roots persist
file path, size, mtime, and indexed timestamp in `source_import_files`. Refresh
re-stats provider files and imports only files whose metadata changed; unchanged
files return `unchanged_sources` without content hashing or parsing. Cursor based
plugin sources remain incremental via provider cursor handoff.

Measured target (synthetic data, macOS, local APFS): unchanged manifested
sources should report zero imported sessions/events and non-zero
`unchanged_sources`; after appending one file, refresh should import only the
changed/appended data. Timing is reported for observability, but tests assert
counts and state rather than wall clock.

Large manifested-source measurement recorded for #196 on macOS/APFS in this
workspace used a temp-only `HOME`/`CTX_DATA_ROOT` and a synthetic Pi
`~/.pi/sessions.jsonl` with 1,000 sessions and 1,000 messages (2,000 JSONL
records). Pi is used because it is a supported manifested native provider source
for search refresh. Results from `target/debug/ctx search --provider pi
--refresh strict --json`:

- Initial refresh/query for `needle-999`: 969.94 ms wall time;
  `freshness.totals` imported 1,000 sessions and 1,000 events,
  `unchanged_sources: 0`, `skipped: 0`, `failed: 0`.
- Unchanged no-op refresh/query for `needle-999`: 213.01 ms wall time;
  imported 0 sessions/events, `unchanged_sources: 1`, `skipped: 0`, `failed: 0`.
- One appended synthetic session/message queried as `needle-new`: 802.30 ms wall
  time; imported 1 session and 1 event, `unchanged_sources: 0`, `skipped: 2000`,
  `failed: 0`. Existing records were deduped/skipped, and only the appended data
  produced new rows.

Freshness-age query measurement used a synthetic SQLite database with 300,000
`events` rows, 1,000 `sessions` rows, and one `history_records` row. Before the
optimization, `latest_indexed_source_at_ms()` included `events.created_at_ms`:
`EXPLAIN QUERY PLAN` showed `SCAN events` and the query took 9.37 ms. After the
optimization, the query uses manifest-backed `source_import_files.indexed_at_ms`
(the existing persisted equivalent of a source-file last-import timestamp) plus
session/history indexed timestamps first; `EXPLAIN QUERY PLAN` scans
`source_import_files`, `history_records`, and `sessions` but no longer scans
`events`, and the same synthetic query took 0.09 ms. A fallback event timestamp
query is used only for stores with no source/session/history timestamps. The
optimized path avoids a per-search scan of the unindexed event table without a
schema migration.

`index_age_seconds` means the age of the newest persisted indexed/import
timestamp known to the store, not the time since a no-op freshness check.
