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
state without importing new rows. OpenCode automatic refresh persists only a
store-keyed HMAC identity and 32-byte main/WAL/SHM metadata signatures. An equal
successful signature reports `unchanged` without opening or normalizing the
provider database. It stores no source path or transcript content.

Concurrent OpenCode refreshes coordinate with a crash-released, per-source OS
advisory lock (private opaque filename) plus a 30-minute token-fenced database lease.
Auto mode serves the existing index with `refresh_in_progress`; unchanged
failures use deterministic exponential backoff (one second through five
minutes) and report `retry_backoff`. A changed signature bypasses backoff.
Strict mode bypasses backoff and fails rather than silently serving stale data
when another owner holds the lease. Additive freshness totals expose bounded
`refresh_in_progress_sources` and `retry_backoff_sources` counts. Mixed work
uses status `completed_mixed`, reason `refreshed_partial` when any source was
refreshed (otherwise `mixed`), and authoritative aggregate `reason_counts`;
diagnostics expose no source key, path, signature,
lease token, or provider error.

OpenCode changed-source refresh uses a versioned, bounded, path-free cursor only
when the exact durable `event`/`event_sequence` schema, foreign key, named
indexes, BINARY collations, projector tables/indexes, and OpenCode ascending ID
encoding are present. The cursor stores the exact schema fingerprint, indexed
event/session/message/part maxima and hashed anchors, source observation,
incremental-run count, and last full reconciliation. It stores no corpus counts,
paths, or transcript text.

Changed rows are discovered with the event primary key query `id > high_water
ORDER BY id LIMIT cap+1`; startup validates its query plan is indexed. Known
durable events identify affected session aggregates. At most 4,096 journal rows,
512 sessions, and 20,000 rows per affected projection are held; projected rows
are read through validated session/message/part indexes and each affected
closure is normalized once. JSON totals expose aggregate full/incremental mode,
journal and projection rows scanned, and bounded fallback reason counts.

The deterministic CI harness builds 5,002 valid durable journal index rows
across two projected sessions, establishes a full baseline, atomically appends
one event/message/part, then imports it. The measured logical scan is one journal
row, one session, two messages, and two parts; the 5,001 unrelated journal rows
and unrelated session closure are not read by the delta query. The focused test
completes in roughly 0.1 seconds on the CI development host and verifies the new
text reaches the existing transactional event/FTS import path.

The ignored `opencode_sparse_incremental_append_search_evidence` test is the
manual #291 append/search harness. It creates the exact capable synthetic
OpenCode `session`/`message`/`part`/`event`/`event_sequence` schema and named
BINARY indexes, imports its full baseline, extends that valid database file to
41 GiB with a sparse tail, then performs one atomic projected message/part and
durable journal append before invoking the actual incremental importer. It
asserts incremental mode, one journal row, one affected session, two messages,
two parts, one normalized session rather than a full normalization, and the
new text in both the event FTS projection and store search. The observed
elapsed times over five repeated invocations were 2.73--2.96 ms, with p95
2.97 ms (target <=500 ms). Each invocation uses a fresh temporary database and
performs one append. The fixture uses only a temporary root, verifies that root
is removed, and does not read the real `HOME` or use the network.

Malformed/foreign cursors, incapable/lookalike schemas, index/collation changes,
invalid IDs and periodic reconciliation use the existing full normalizer. Every
32 changed incremental successes or 24 hours forces that reconciliation.
Unknown, malformed, deletion/reset events, journal regression/removal, a changed
signature without a journal append, an overflowing delta, pre-baseline row
updates, and oversized affected closures are consciously unsupported mutations:
auto serves the prior index under backoff and strict refresh fails. A full scan
is not claimed to repair deletes because the importer is append/upsert-only.
Any normalization/import failure or unstable observation leaves the cursor
unchanged; token-fenced completion stores it only after successful import.

The ignored
`opencode_sparse_signature_warm_p95` capture test is the manual metadata-only
40+ GiB sparse-file harness (target warm p95 <=500 ms). It is #290 metadata
signature evidence, not #291 incremental-read evidence,
is not run by CI, and uses a temporary
root and does not read file contents, the real home directory, or the network.

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
