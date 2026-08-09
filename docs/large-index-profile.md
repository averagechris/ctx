# Large-index profile harness

`ctx-history-search` contains a deterministic synthetic profile harness for
exercising canonical per-entity `Store` write APIs and search queries without
reading real transcripts. The corpus is generated and written in bounded batches,
so peak generation memory is controlled by `CTX_LARGE_PROFILE_BATCH_RECORDS` and
`CTX_LARGE_PROFILE_EVENTS_PER_RECORD`, not by the total event target. The harness
does not materialize the accumulated corpus, does not use the real home/data
root, and performs no network access.

## CI smoke

The normal test is small and uses a temporary root:

```sh
cargo test -p ctx-history-search streaming_large_profile_smoke_writes_stable_artifact -- --nocapture
```

It writes a temporary `ctx-large-index-profile-v1.json` artifact and checks
repeat determinism for stable fields, exact row/FTS cardinality formulas,
deterministic sampled base/FTS identity and indexed-content parity, bounded batch
construction, replay no-op behavior, one-record incremental deltas, partial final
batches, batch-size-invariant ordered result IDs, reopen digest stability,
artifact parsing, and temp-root isolation. Timing, RSS, paths, OS/hardware, and
jj identifiers are environmental and are not compared as deterministic fields.

## Manual release profile

The large profile is ignored and must be opted in explicitly from a release build
with an absolute output directory:

```sh
CTX_LARGE_PROFILE_OUTPUT=/tmp/ctx-large-profile \
CTX_LARGE_PROFILE_EVENTS=1250000 \
CTX_LARGE_PROFILE_MEASUREMENT_REPEATS=5 \
cargo test -p ctx-history-search --release streaming_large_profile_manual_release -- --ignored --nocapture
```

The run fails unless the measured post-checkpoint footprint reaches the configured
minimum. The default minimum is `10 * 1024^3` bytes. `CTX_LARGE_PROFILE_EVENTS`
must be calibrated for the machine and schema: raise it until the artifact's
`storage.post_checkpoint.total_present_bytes` reaches the minimum. For local
harness testing only, `CTX_LARGE_PROFILE_MIN_FOOTPRINT_BYTES` can lower the
threshold; the artifact records that override. Expect substantial disk use and
long runtimes. Use local scratch storage and remove it when done:

```sh
rm -rf /tmp/ctx-large-profile
```

`CTX_LARGE_PROFILE_MEASUREMENT_REPEATS` is bounded to 1–20 and defaults to 1
for smoke compatibility. Manual ticket profiles default to exactly 5 and reject
other repeat counts. It repeats only the warm
ordinary search, warm heavily filtered search, and replay no-op against the
already generated corpus. Their artifact objects contain the ordered sample
array, sample count, p50, p95, minimum, and maximum; the existing singular
`*_ms` fields remain as the first sample for artifact consumers.

Each ordinary and filtered sample also records non-overlapping search phases,
their p50/p95 values, path (`fast_event` or `fallback_ranked_fts`), and available
statement/candidate/hydration counters. The production execution phase remains a
combined ranked-FTS/candidate-paging/base-context-hydration phase: this profile
does not change runtime behavior. In addition, the filtered fallback profile has
a test-only `ranked_fts_hydration` attribution that runs the same bounded ranked
FTS ID selection first and then the existing hydration/scoring path. It records
selection and hydration timings plus candidate/result IDs and order-sensitive
digests, and rejects any mismatch against the production ranked-fallback path.
This attribution is explicitly labelled `fallback_ranked_fts` and its reference
is `production_ranked_fts_fallback`; it does not describe the default filtered
search route. The latter is recorded independently in
`filtered_search_phases.path` (on the 10 GiB corpus it is `fast_event`) and in
the ordinary filtered result digest. Manual ticket evidence is qualified only
when the ranked attribution has exactly five warm samples; smoke artifacts may
retain their one-sample compatibility mode.
Assembly includes snippets, citations, clustering, sorting, and projection.
Explicit unattributed overhead makes every sample reconcile to its end-to-end
duration. The artifact also records the reproducible command assembled from the
effective harness configuration.

Debug builds are refused. `CTX_LARGE_PROFILE_OUTPUT` must be explicit, absolute,
must not resolve into `~/.ctx`, and must either not exist or contain the harness
ownership marker `.ctx-large-profile-owned` containing the marker version and
canonical output path. Existing marked output is reused only after marker/path
validation; the harness removes only its exact known files (`synthetic-large-profile.sqlite`,
sidecars, and `ctx-large-index-profile-v1.json`) so unrelated files are never
recursively deleted based on marker presence.

## Artifact

The versioned JSON artifact contains `schema_version`, requested baseline events
and minimum footprint, achieved baseline and incremental counts, deterministic
seed and batch bound, OS/arch and local jj IDs when available without network,
and current artifacts use schema version `2` and mark the ranked-attribution
contract with `ranked_fts_attribution_revision: 1`. The checked-in pre-phase
schema-version-`1` fixture remains readable only in its legacy shape: it must
omit the command, phase evidence, ranked revision, and ranked evidence. Schema
version `2` always requires all of those current fields, so deleting optional
evidence cannot downgrade a newly generated artifact to the legacy contract.
Unsupported schema or attribution revisions are rejected.
actual SQLite version/PRAGMAs, canonical absolute DB/WAL/SHM/artifact paths,
pre/post checkpoint sidecar states with absent sidecars distinguished from
metadata errors, exact base table and FTS table cardinalities, deterministic
first/quartile/middle/last/incremental sampled projection identity/content checks
with event FTS samples constrained by the synthetic harness insertion-order rowid invariant (baseline rowids follow insertion index + 1; incremental rowids follow after the inserted baseline cardinality; not a production storage contract),
initial/replay-no-op/one-record incremental measurements, warm ordinary and heavily filtered search timings,
bounded session event-window retrieval timing/results through public read APIs
(`get_event` + `events_for_session`), explicit WAL checkpoint
timing/results, reopen ordered result IDs/digest, high-water RSS, and a privacy
statement. The corpus uses only synthetic paths under `/workspace/ctx` and
synthetic payload text.

The harness reports cache state honestly: search measurements are warm, followed
by a reopen measurement labelled `reopen_not_cold`. True cold-cache runs require
operator-controlled OS cache-drop/reboot steps; the test does not automate
privileged cache drops. Peak RSS is reported from
`getrusage(RUSAGE_SELF).ru_maxrss`: macOS reports bytes; Linux reports KiB and the
harness converts to bytes. Treat RSS as local evidence rather than a
cross-platform pass/fail threshold.
