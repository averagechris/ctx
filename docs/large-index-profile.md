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
for backward-compatible smoke/profile behavior. It repeats only the warm
ordinary search, warm heavily filtered search, and replay no-op against the
already generated corpus. Their artifact objects contain the ordered sample
array, sample count, p50, p95, minimum, and maximum; the existing singular
`*_ms` fields remain as the first sample for artifact consumers.

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
