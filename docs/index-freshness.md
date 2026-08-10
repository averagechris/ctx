Index freshness
===============

`ctx search` treats index freshness as part of the result contract. Human output
prints a `freshness:` line and JSON output includes `freshness.ran`,
`freshness.status`, `freshness.reason`, `freshness.duration_ms`,
`freshness.index_age_seconds`, and import totals including imported, skipped,
unchanged, and failed counts. Refresh diagnostics also include aggregate
`freshness.phases` entries for `native_plugin_discovery`,
`provider_observation_catalog`, `provider_normalization`, `ctx_import_decision`,
and `health_persistence`. Each entry contains only a bounded aggregate
`duration_ms` and `count`; it never contains a path, source identifier,
transcript text, provider error, or per-source timing label. The human
`freshness phases:` line presents the same aggregate values.

Phase timings are measured only at stable refresh seams. A zero duration or
count means that the seam did not run (or is not measured by that refresh
path); it does not claim that unmeasured work took zero time. The phase values
are aggregate observations and may overlap when a phase contains nested work
or provider imports run concurrently. They are diagnostic attribution, not a
wall-clock decomposition of `freshness.duration_ms`. The phase contract is
additive: existing freshness fields and reason values remain unchanged. Phase
`count` values describe measured seam work units (for example, discovery
attempts or normalization calls), not source identities or per-source records;
`source_count` and import totals retain their existing meanings.

`--refresh off` is the strict read-only mode: it does not discover provider
sources, execute history-source plugins, create config/data-root files, open the
store writable, run migrations, import, checkpoint, or write. It only opens an
existing schema-current database read-only and searches it.

The persistent equivalent is:

```toml
[search]
refresh = "off"
```

`refresh` accepts `"auto"`, `"off"`, or `"strict"`. An explicit CLI
`--refresh` value overrides `config.toml`; an omitted CLI value uses the config
value, or `auto` when the file or key is absent. Config syntax and values are
validated before search refresh begins. A configured `off` has the same
read-only/no-discovery behavior as `--refresh off`, including on an uninitialized
root (which fails rather than creating a store).

Both explicit `--refresh off` and configured `refresh = "off"` leave every
phase duration and count at zero. They perform no discovery, plugin execution,
store write, or refresh read; the query opens the existing current store
read-only as before.

`unchanged_sources` counts provider sources proven unchanged by metadata/cursor
state without importing new rows: manifested provider roots persist file path,
size, mtime, and indexed timestamp, and a rescan that finds no pending files
reports the source as unchanged. It reports the observed refresh
outcome; it does not add a new metadata fast path or change provider import
performance.

Freshness age uses existing manifest, history-record, and session timestamps.
Event timestamps are queried only as a compatibility fallback for stores that
have none of those timestamps. This requires no schema migration.

`index_age_seconds` means the age of the newest persisted indexed/import
timestamp known to the store, not the time since a no-op freshness check.

For unattended freshness, prefer scheduling `ctx import --all --strict` rather
than relying on a foreground search refresh. A launchd or systemd job must run
as the same user with the same `HOME`, `CTX_DATA_ROOT`, plugin path and `PATH`,
and provider-specific visibility/environment as interactive ctx commands. The
job otherwise may discover a different set of provider files or fail to find
the configured history-source plugin executable.
