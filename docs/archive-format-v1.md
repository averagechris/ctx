# ctx Archive Format v1 (`ctx-archive`, format_version 1)

Status: **accepted and shipped contract**. The v1 writer, read-only verifier,
and fresh-root restore are shipped as `ctx archive create`, `ctx archive verify`,
and `ctx archive restore`. The internal `SessionHistoryArchive` JSON structure
must not be presented as this format.

This contract defines the smallest safe, portable, streaming, checksummed
**logical content archive** of a ctx data root. It is a portable re-import
surface for a fresh data root, not a byte-level SQLite disaster-recovery
snapshot: it is portable across machines and future schema versions, while
merge semantics remain outside v1; a database file copy is neither.

Related reading: [storage.md](storage.md) (data root layout, privacy truth),
[fork-plan.md](fork-plan.md) (schema divergence policy), tickets #186
(large-index profiling), #188 (archive-first compaction), #189 (offline
multi-machine merging).

Selective compaction bundles are a distinct, exact family:
`("ctx-selective-archive", 1)`. `ctx archive create --cutoff-ms N TARGET`
computes a fresh #282 plan from the same read-only v1005 snapshot used by the
writer and archives only that authenticated closure. The cutoff is the sole
selection input. The bundle retains this v1 layout and all fifteen streams
(including empty streams), but carries `scope.kind = "selective"` and the
planner's root decisions, member dispositions, deletion authorization, and
root/closure/membership/deletion/plan digests in `selective`. Full and
selective family/scope mismatches are rejected. `ctx archive restore` accepts
a registered selective bundle and an existing data root; repeatable
`--session-id UUID` selectors restore the authenticated union closure, while
omitting selectors restores every archived root. Restore is explicit and does
not discover archives or run from refresh/import paths.

Selective bundles keep large closure evidence out of `manifest.json`.
`evidence/root-members.jsonl` is a private, checksummed, bounded-line canonical
stream containing root decisions, union members/deletion dispositions, and
per-root mappings. The manifest contains only its count, byte length, SHA-256,
and the small plan/digest header. Verification streams the evidence and
canonical entity files into private scratch state, derives content keys from
the actual v1 column encoding (and object hashes), and rejects evidence that
does not reproduce the authenticated plan or completed-child cutoff rules.
The verifier reruns the directional-closure planner over canonical rows in
that scratch database: evidence root assignment, member disposition, union
ownership/shared status, and deletion authorization are comparison inputs,
never graph truth. Inbound owners that revoke deletion are therefore archived
as non-deletable boundary rows so the decision is independently
reconstructible. Evidence parsing and equality use ordered scratch tables one
row at a time; per-root closure export is recomputed and written one root at a
time, so physically duplicated shared membership does not require an
`O(root_count * closure_size)` in-memory map.

## Design constraints

1. **Archives are secrets.** They contain verbatim agent history — prompts,
   code, commands, paths, credentials. Compression is not encryption and v1
   claims neither; see [Privacy and security posture](#privacy-and-security-posture).
2. **Bounded memory.** Export, verification, and restore must stream. No step
   may materialize all entities or any whole blob in memory.
3. **Fail closed.** Anything unknown, missing, duplicated, truncated, or
   out-of-vocabulary rejects the whole archive. There is no partial accept.
4. **Atomic publication.** An incomplete archive or restored data root is never
   visible at the destination path before its exclusive rename publication
   point. A handled pre-publication failure cleans staging; a parent-directory
   `fsync` can still report an error after a complete target has been published.
5. **No network.** Nothing in this format enables or requires transport,
   upload, discovery, or remote credentials. Transport is external tooling.
6. **Independent versioning.** The archive format version is not the SQLite
   `PRAGMA user_version` (currently 1005) and not the internal
   `SessionHistoryArchive` `schema_version` (1/2). See
   [Format identity](#format-identity-and-versioning).

## Container choice

**v1 is a private directory bundle, not a single-file container.**

Rationale, resolved against the alternatives:

- A directory bundle makes atomic publication a single `rename(2)` of a
  staged sibling directory — the same pattern the store already uses for
  private files — with no custom framing.
- Object blobs are stored verbatim under their content address, so blob
  verification is plain SHA-256 of a file, and export/restore copy blobs with
  bounded buffers and no archive-member seeking.
- tar/zip framing would add member-header parsing, path-name attack surface
  inside the container, and (for zip) central-directory trust problems, while
  still needing every check defined here. Users who want one file for
  transport can `tar` the published bundle externally; users who want
  encryption can use `age`. Both compose without ctx owning them.
- No compression is used **anywhere inside the bundle** in v1. This is
  deliberate, not deferred laziness: it removes the entire
  decompression-bomb/ratio-attack class from the verifier, keeps blob hashes
  checkable byte-for-byte, and keeps restore I/O proportional to declared
  sizes. External `zstd`/`gzip` of the whole bundle remains available. A
  future format_version may add per-stream compression; it must then declare
  exact decompressed sizes and the verifier must enforce them.

### Layout

```text
<name>.ctxar/                    directory bundle, mode 0700
  manifest.json                  archive identity, stream index, counts, digests
  COMPLETE                       completion marker, written last
  streams/                       mode 0700
    01-capture_sources.jsonl
    02-vcs_workspaces.jsonl
    03-history_records.jsonl
    04-artifacts.jsonl
    05-sessions.jsonl
    06-session_edges.jsonl
    07-runs.jsonl
    08-events.jsonl
    09-vcs_changes.jsonl
    10-summaries.jsonl
    11-files_touched.jsonl
    12-tags.jsonl
    13-history_record_tags.jsonl
    14-history_record_links.jsonl
    15-record_edges.jsonl
  objects/                       mode 0700; content-addressed blob bytes
    <2-hex-shard>/               first two hex digits of the blob hash
      <64-hex-sha256>            verbatim blob bytes, mode 0600
```

Every file is mode `0600` and every directory `0700` on Unix, matching the
data-root policy. All fifteen stream files are always present, even when
empty (an empty stream is a zero-byte file with count 0 in the manifest).
No other entries may exist anywhere in the bundle. All entries must be
regular files or directories: no symlinks, no hard links (`st_nlink > 1`),
no FIFOs/devices, anywhere.

Operational note on the hard-link rule: it also fires on *external* extra
links. Backup tools that deduplicate unchanged files via hard links
(`rsync --link-dest`, `cp -al` snapshot farms, some NAS snapshot schemes)
raise `st_nlink` above 1 on bundle files, so a bundle inspected inside
such a backup fails verification **by design** (the bytes are shared with
paths outside the bundle's control). Verify and restore from the original
published bundle or from a plain copy, not from a link-deduplicated
backup view.

The recommended bundle name suffix is `.ctxar`. The staging directory during
creation is `<name>.ctxar.tmp-<archive_id>` in the same parent directory.

## Format identity and versioning

`manifest.json` carries:

| field | value | meaning |
| --- | --- | --- |
| `format` | `"ctx-archive"` | container/format family name |
| `format_version` | `1` | this contract |

Version acceptance is **exact**: a v1 reader accepts `("ctx-archive", 1)` and
rejects everything else, including higher versions. Any change to layout,
stream set, record schemas, encoding rules, or verification semantics bumps
`format_version`. There are no minor versions and no capability flags in v1.

Explicit non-couplings:

- **SQLite schema version.** The manifest records the writer's
  `PRAGMA user_version` as `source_schema_version` (currently 1005) for
  diagnostics only. Readers must not gate on it: restore always materializes
  the current binary's schema via the normal store-creation path.
- **`SessionHistoryArchive` versions 1/2.** That in-memory JSON structure is
  an internal import intermediate. It omits session edges, tags, tag
  assignments, and record edges, and does not carry blob bytes; it is not a
  backup and shares no version number with this format.

## Encoding rules

All stream files are JSON Lines. The rules below are normative for writers;
verifiers must reject any deviation.

1. **UTF-8, no BOM.** Each record is one JSON object on one line terminated
   by exactly one `\n` (0x0A). No `\r`, no blank lines, no leading/trailing
   whitespace, and the final line is `\n`-terminated (a missing final newline
   is truncation).
2. **Compact encoding.** No whitespace between tokens
   (`serde_json` compact form).
3. **Fixed key order.** Top-level keys appear exactly in the order given by
   the stream schema tables below. Duplicate keys within any object are
   invalid.
4. **No nulls, omitted optionals.** Optional fields that are NULL in the
   store are omitted entirely. Writers never emit JSON `null`; verifiers
   reject explicit `null` at the top level.
5. **Unknown fields are fatal.** The top-level key set must be exactly the
   schema's required keys plus any subset of its optional keys. This is the
   same fail-closed posture as the store's schema-version gate: an archive a
   reader does not fully understand is not partially restored.
6. **IDs.** UUIDs are lowercase, hyphenated, 36 characters. They are opaque
   and preserved exactly; restore never rewrites entity IDs.
7. **Timestamps.** All timestamps are integer Unix epoch **milliseconds** in
   fields named `*_at_ms` (plus `author_time_ms`), exactly as stored in
   SQLite. No RFC3339 strings, no floats. Values must fit a signed 64-bit
   integer.
8. **Embedded JSON columns are carried verbatim as strings.** Columns that
   store JSON text (`metadata_json`, `payload_json`, `citations_json`,
   `parent_change_ids_json`, `tags_json`) are carried as JSON **strings**
   containing the exact stored text, byte-for-byte. The writer does not
   parse, canonicalize, or re-encode them, and restore stores the string back
   verbatim. This makes stream bytes deterministic without defining canonical
   JSON for arbitrary nested metadata, and makes cross-machine equality
   checks (needed by #189) plain byte comparisons. Verifiers check only that
   the string is valid UTF-8; they do not parse it.
9. **Integers.** `seq`, `sync_version`, `byte_size`, counts, and sizes are
   non-negative JSON integers that fit in a signed 64-bit integer. Other
   integer fields fit their logical storage type: `process_id` is in
   `0..=u32::MAX`, `exit_code` is in `i32::MIN..=i32::MAX`, and
   `line_count_delta` fits a signed 64-bit integer. No floats anywhere.
10. **Booleans.** `is_primary` is a JSON boolean (the only boolean field in
    v1 streams).
11. **Enums.** Closed vocabularies (below) are normative; out-of-vocabulary
    values are fatal. The vocabularies mirror the SQLite CHECK constraints at
    schema v1005. A future provider addition requires a store CHECK-rebuild
    migration **and** an archive `format_version` review; a v1 reader
    encountering an unknown provider string fails closed, which is correct
    (the reader binary could not have imported that provider either).

Determinism: given the same logical store contents and the same
(`archive_id`, `created_at_ms`) pair, two exports produce byte-identical
bundles. This is a contract requirement, not an aspiration; it is what makes
per-stream SHA-256 digests meaningful across machines.

### Shared vocabularies

- `provider`: `codex`, `claude`, `pi`, `opencode`, `antigravity`, `gemini`,
  `cursor`, `copilot_cli`, `factory_ai_droid`, `openclaw`, `hermes`,
  `nanoclaw`, `astrbot`, `shell`, `git`, `jj`, `gh`, `custom`, `unknown`
  (applies to `sessions.provider` too, even though that column has no CHECK)
- `visibility`: `local_only`, `reportable`, `sync_metadata`, `sync_full`,
  `withheld`
- `fidelity`: `full`, `partial`, `imported`, `inferred`, `summary_only`
- `sync_state`: `local_only`, `pending`, `synced`, `failed`, `withheld`
- `confidence`: `explicit`, `high`, `medium`, `low`, `unknown`
- `redaction_state`: `raw`, `redacted`, `safe_preview`, `withheld`

### Common trailer fields

Streams 02, 03, 04, 06, 07, 09, 10, 11, 14, and 15 end with this ordered
field group, abbreviated below as **SYNC**: `source_id?` (uuid),
`visibility`, `fidelity`, `sync_state`, `sync_version` (int),
`deleted_at_ms?` (int), `metadata_json` (verbatim string).

`capture_sources`, `sessions`, `events`, `tags`, and `history_record_tags`
do **not** use SYNC; their trailers are spelled out field-by-field in their
stream schemas. In particular, `sessions` and `events` have no `source_id`
column at all — their capture-source reference is `capture_source_id` —
and each carries exactly one `fidelity` field in its explicit list.

Soft-deleted rows (`deleted_at_ms` set) are exported verbatim; fidelity
beats tidiness.

`created_at_ms`/`updated_at_ms` pairs are abbreviated **TS**.

## Entity streams

Streams are written, verified, and restored in the numbered order. The order
is a valid topological order of the reference graph; the only forward
references permitted are the intra-stream session self-references noted
below.

Within each stream, records are sorted ascending by the listed sort key,
compared by the key's **native type**:

- **UUID keys** compare lexically on the canonical lowercase hyphenated
  form (equivalent to a bytewise compare of that form, and to SQLite's
  default BINARY collation on the stored TEXT value);
- **integer keys** (`events.seq`) compare numerically — *not* on their
  serialized digits (`10` sorts after `9`);
- **tuple keys** compare field-by-field, left to right, each field by its
  own native type.

The exporter's keyset pagination must produce exactly this order
(`ORDER BY id`, `ORDER BY seq`, `ORDER BY history_record_id, tag_id` over
the canonical columns), and the verifier's order check must implement the
same comparison, so the two can never disagree. Sort keys are unique, so
ordering is total and deterministic.

| # | stream | source table | sort key |
| --- | --- | --- | --- |
| 01 | `capture_sources` | `capture_sources` | `id` |
| 02 | `vcs_workspaces` | `vcs_workspaces` | `id` |
| 03 | `history_records` | `history_records` | `id` |
| 04 | `artifacts` | `artifacts` | `id` |
| 05 | `sessions` | `sessions` | `id` |
| 06 | `session_edges` | `session_edges` | `id` |
| 07 | `runs` | `runs` | `id` |
| 08 | `events` | `events` | `seq` |
| 09 | `vcs_changes` | `vcs_changes` | `id` |
| 10 | `summaries` | `summaries` | `id` |
| 11 | `files_touched` | `files_touched` | `id` |
| 12 | `tags` | `tags` | `id` |
| 13 | `history_record_tags` | `history_record_tags` | (`history_record_id`, `tag_id`) |
| 14 | `history_record_links` | `history_record_links` | `id` |
| 15 | `record_edges` | `record_edges` | `id` |

v1 scope is always the **full canonical content** of one store
(`scope.kind = "full"` in the manifest). Subset selection (per-session,
cutoff-bounded) is #188's planner and requires a format review there.

Field lists below are ordered and normative. `?` marks optional (omitted when
NULL). Types: `uuid`, `str`, `int`, `bool`, `json-str` (verbatim embedded
JSON string), enum names refer to the vocabularies above.

**01 capture_sources** — `id` uuid, `kind` enum(`provider_import`,
`provider_hook`, `direct_cli`, `manual`), `provider`, `machine_id` str,
`process_id?` int, `cwd?` str, `raw_source_path?` str,
`external_session_id?` str, `started_at_ms` int, `ended_at_ms?` int,
`fidelity`, `visibility`, `sync_state`, `sync_version` int,
`metadata_json` json-str.

**02 vcs_workspaces** — `id` uuid, `kind` enum(`git`, `jj`), `root_path` str,
`repo_fingerprint` str, `primary_remote_url_normalized?` str, `host`
enum(`github`, `gitlab`, `bitbucket`, `local`, `unknown`), `owner?` str,
`name?` str, `monorepo_subpath?` str, TS, SYNC. Natural key
(`kind`, `repo_fingerprint`) must be unique.

**03 history_records** — `id` uuid, `title` str, `summary?` str, `status`
enum(`open`, `active`, `completed`, `abandoned`, `archived`),
`primary_vcs_workspace_id?` uuid, `started_at_ms?` int,
`last_activity_at_ms` int, `completed_at_ms?` int, `confidence`, TS, SYNC,
then `body` str, `tags_json` json-str, `kind` str, `workspace?` str. The
legacy text columns `created_at`/`updated_at` are **not** archived; restore
recomputes them as RFC3339 UTC renderings of the millisecond fields (the
store's storage convention for those columns).

**04 artifacts** — `id` uuid, `kind` enum(`transcript`, `stdout`, `stderr`,
`screenshot`, `report`, `diff`, `file_snapshot`, `json`, `markdown`,
`binary`), `blob_hash` str (64 lowercase hex), `byte_size` int,
`media_type?` str, `preview_text?` str, `redaction_state`, TS, SYNC. Natural
key (`blob_hash`, `kind`) must be unique. **`blob_path` is intentionally not
archived**: it is derived, and restore recomputes the canonical
`objects/<shard>/<hash>` path.

**05 sessions** — `id` uuid, `history_record_id?` uuid,
`parent_session_id?` uuid, `root_session_id?` uuid,
`capture_source_id?` uuid, `provider`, `external_session_id?` str,
`external_agent_id?` str, `agent_type` enum(`primary`, `subagent`,
`agent_team_member`, `reviewer`, `implementer`, `unknown`), `role_hint?` str,
`is_primary` bool, `status` enum(`started`, `active`, `idle`, `completed`,
`failed`, `interrupted`, `imported`), `fidelity`,
`transcript_blob_id?` uuid, `started_at_ms` int, `ended_at_ms?` int, TS,
`visibility`, `sync_state`, `sync_version` int, `deleted_at_ms?` int,
`metadata_json` json-str. Explicit trailer — **no SYNC**: sessions have no
`source_id` column, and `fidelity` appears exactly once (above).
`parent_session_id`/`root_session_id` may reference any session in
this stream regardless of position (id-sorted order does not guarantee
parents first); see restore.

**06 session_edges** — `id` uuid, `from_session_id` uuid, `to_session_id`
uuid, `edge_type` enum(`parent_child`, `delegated`, `reviewed`, `spawned`,
`resumed_from`, `imported_related`), `confidence`, TS, SYNC.

**07 runs** — `id` uuid, `history_record_id?` uuid, `session_id?` uuid,
`run_type` enum(`agent_turn`, `command`, `tool_call`, `review`, `import`,
`summary`), `status` enum(`queued`, `running`, `succeeded`, `failed`,
`cancelled`, `partial`), `started_at_ms` int, `ended_at_ms?` int,
`exit_code?` int, `cwd?` str, `command_preview?` str, `input_blob_id?` uuid,
`output_blob_id?` uuid, TS, SYNC.

**08 events** — `id` uuid, `seq` int, `history_record_id?` uuid,
`session_id?` uuid, `run_id?` uuid, `event_type` enum(`message`,
`tool_call`, `tool_output`, `command_started`, `command_output`,
`command_finished`, `file_touched`, `vcs_change`, `artifact`, `summary`,
`notice`), `role?` enum(`user`, `assistant`, `system`, `tool`, `unknown`),
`occurred_at_ms` int, `capture_source_id?` uuid, `payload_json` json-str,
`payload_blob_id?` uuid, `dedupe_key?` str, `visibility`,
`redaction_state`, `fidelity`, `sync_state`, `sync_version` int,
`deleted_at_ms?` int, `metadata_json` json-str. Explicit trailer — **no
SYNC**: events have no `source_id` column (the capture-source reference is
`capture_source_id`), no `created_at_ms`/`updated_at_ms` pair, and exactly
one `fidelity` field, ordered as in the events table (`visibility`, then
`redaction_state`, then `fidelity`). `seq` values must be unique across
the stream and preserved exactly (they are the store-global ordering and
are cited by search results); non-null `dedupe_key` values must be unique.

**09 vcs_changes** — `id` uuid, `vcs_workspace_id` uuid, `kind`
enum(`git_commit`, `git_branch`, `git_worktree`, `jj_change`, `jj_bookmark`,
`patch`, `working_copy`), `change_id` str, `parent_change_ids_json` json-str,
`branch_or_bookmark?` str, `tree_hash?` str, `author_time_ms?` int,
`confidence`, TS, SYNC. Natural key (`vcs_workspace_id`, `kind`,
`change_id`) must be unique.

**10 summaries** — `id` uuid, `history_record_id?` uuid, `session_id?` uuid,
`kind` enum(`imported_provider_summary`, `ctx_generated`, `agent_supplied`,
`human_note`), `model_or_source?` str, `text` str, `citations_json` json-str,
TS, SYNC.

**11 files_touched** — `id` uuid, `history_record_id?` uuid, `run_id?` uuid,
`event_id?` uuid, `vcs_workspace_id?` uuid, `path` str, `change_kind?`
enum(`read`, `created`, `modified`, `deleted`, `renamed`, `unknown`),
`old_path?` str, `line_count_delta?` int, `confidence`, TS, SYNC.

**12 tags** — `id` uuid, `name` str (unique), `kind` enum(`user`, `system`,
`inferred`), TS, `metadata_json` json-str.

**13 history_record_tags** — `history_record_id` uuid, `tag_id` uuid,
`source_id?` uuid, `confidence`, `created_at_ms` int. Composite key must be
unique.

**14 history_record_links** — `id` uuid, `history_record_id` uuid,
`target_type` enum(`session`, `run`, `event`, `vcs_workspace`, `vcs_change`,
`artifact`), `target_id` uuid, `link_type` enum(`produced`, `touched`,
`references`, `likely_related`), `confidence`, TS, SYNC. Natural key
(`history_record_id`, `target_type`, `target_id`, `link_type`) must be
unique. `target_id` is polymorphic; the verifier resolves it against the
stream named by `target_type`.

**15 record_edges** — `id` uuid, `from_record_id` uuid, `to_record_id` uuid,
`edge_type` enum(`continues`, `duplicates`, `blocks`, `related`,
`supersedes`, `split_from`), `confidence`, TS, SYNC.

`raw_source_path`, `cwd`, and `root_path` values are origin-machine metadata.
They are preserved verbatim for citation fidelity and must never be treated
as paths that exist (or should be created/read) on the restoring machine.
Restore never dereferences them.

## Object blobs

- Every blob referenced by an `artifacts` record — and only those — is
  stored at `objects/<first-two-hex>/<sha256-hex>` with its verbatim bytes.
  The layout deliberately mirrors the data root's `objects/` store, so
  restore is a bounded streaming copy.
- The file name **is** the checksum: lowercase hex SHA-256 of the content.
  The shard directory must equal the first two hash characters.
- Multiple artifact records may share one blob (uniqueness is
  (`blob_hash`, `kind`)); the blob appears once.
- Bijection rule: the set of `blob_hash` values in stream 04 equals the set
  of blob files present. A referenced-but-missing blob is fatal; an
  unreferenced blob file is fatal. v1 does not support declared-elided
  blobs; an artifact row without its bytes is not archivable, by design
  ("survives restore without the original provider transcripts").

## Manifest

`manifest.json` is a single compact-encoded JSON object, `\n`-terminated,
fixed key order as listed, maximum 16 MiB:

| key | type | notes |
| --- | --- | --- |
| `format` | str | `"ctx-archive"` |
| `format_version` | int | `1` |
| `archive_id` | uuid | fresh UUIDv7 per export |
| `created_at_ms` | int | export wall-clock time |
| `generator` | obj | `{"name":"ctx","version":"<crate version>"}` |
| `source_schema_version` | int | writer's `PRAGMA user_version`; diagnostic only |
| `origin_device_id?` | str | `local_devices.stable_device_id` when present; groundwork for #189 origin identity |
| `scope` | obj | `{"kind":"full"}` — only value in v1 |
| `streams` | array | one entry per stream, in stream order |
| `objects` | obj | `{"count":N,"total_bytes":N}` |
| `entity_count` | int | sum of stream counts; cross-check |

Each `streams` entry, fixed order:
`{"name":"events","path":"streams/08-events.jsonl","count":N,"bytes":N,"sha256":"<64 hex>"}`.
Paths are fixed by this contract; the verifier rejects any path not exactly
equal to the contractual value (this removes path-traversal surface — no
manifest-supplied path is ever joined into the filesystem).

The manifest intentionally contains **no absolute paths, no hostname, no
username, and no store location**. Origin specifics beyond
`origin_device_id` live in `capture_sources` rows where they already exist.

`origin_device_id` selection rule: the field is emitted only when
`local_devices` contains **exactly one** row, in which case it carries
that row's `stable_device_id`. With zero rows or more than one row the
field is omitted — an ambiguous origin is never guessed. #189 may replace
this with a mandatory origin model in a later `format_version`.

## Completion marker and integrity chain

`COMPLETE` contains exactly one compact JSON line:

```json
{"format":"ctx-archive-complete","format_version":1,"manifest_sha256":"<64 hex>"}
```

where `manifest_sha256` is the SHA-256 of the exact bytes of
`manifest.json`. The integrity chain is an acyclic DAG — no circular digest:

- blob bytes are authenticated by their **file name** and by
  `artifacts.byte_size`;
- stream bytes are authenticated by `streams[].sha256` + `bytes` + `count`
  in the manifest;
- the manifest is authenticated by `COMPLETE`;
- `COMPLETE` itself is authenticated by its fixed grammar plus the manifest
  digest matching. It is written **last**; its presence is the publication
  bit. A bundle without a valid `COMPLETE` is not an archive.

## Export protocol

1. Create staging directory `<name>.ctxar.tmp-<archive_id>` (0700) in the
   destination's parent. Fail if the final path already exists.
2. Open the store read-only with **one read transaction held for the whole
   export**. The snapshot covers **SQLite rows only** — all counts and
   cross-stream references are mutually consistent — but files under the
   data root's `objects/` store are outside transaction control. Blob
   bytes are therefore re-hashed during the copy in step 4; any concurrent
   change to a content-addressed blob surfaces as a hash or size mismatch
   and aborts the export rather than being silently captured.
3. For each stream in order: iterate the source table with keyset pagination
   on the sort key (`ORDER BY` matching the native-type sort order defined
   in [Entity streams](#entity-streams); bounded batch size, e.g. 1000
   rows), encode each record, update a running SHA-256/count/byte tally,
   and append to the stream file. Peak memory is one batch of rows.
4. Copy each referenced blob from the data root `objects/` store in bounded
   chunks (e.g. 64 KiB), verifying SHA-256 and byte size **during the copy**;
   a source blob that is missing, unreadable, a non-regular file, or
   hash/size-mismatched aborts the export.
5. Write `manifest.json` from the tallies; write `COMPLETE`.
6. `fsync` every file, `fsync` directories bottom-up, `rename(2)` the staging
   directory to the final name, `fsync` the parent directory.
7. On any pre-publication failure, delete the staging directory. A crash can
    leave an unpublished `*.tmp-*` directory behind, potentially with a
    complete/usable-looking bundle and `COMPLETE`; it never constitutes the
    published archive because the exclusive rename never happened, and tools
    must refuse `*.tmp-*` paths for verify and restore. The verifier also uses
    a private sibling `.ctxar-verify-*/state.sqlite` scratch directory,
    normally removed on exit; a crash can leave it partial or complete. All
    residue is sensitive. After the exclusive rename, the target exists; a
    parent-directory `fsync` may still fail and report an error. Inspect and
    verify before retrying, and never overwrite that target.

The exporter must self-verify (run the full verification pass below against
the staged bundle) before publication. Export cost is two passes; safety
beats speed here.

## Resource bounds

| limit | value |
| --- | --- |
| max JSONL line length | 32 MiB |
| max `manifest.json` / `COMPLETE` size | 16 MiB / 4 KiB |
| stream file set | exactly the 15 contractual paths |
| blob copy buffer | bounded (implementation-chosen, ≤ a few MiB) |
| verifier memory | fixed-size ID/digest sets; never row payloads |

Verification and restore never buffer more than one line plus fixed-size
state per stream. The uniqueness and referential checks keep in-memory
sets with a **fixed byte cost per entry**, regardless of field lengths:

- UUID references and blob hashes are stored exactly (16 and 32 bytes);
  **referential (dangling-reference) checks must use exact UUID values**.
- Natural keys containing unbounded strings (`tags.name`,
  `events.dedupe_key`, and the vcs_changes / history_record_links tuples)
  may be tracked as fixed-size cryptographic digests (≥ 16 bytes, e.g.
  truncated SHA-256 over an unambiguous length-prefixed encoding of the
  key fields) instead of the full strings. A digest collision then causes
  a **false duplicate rejection** — fail closed, astronomically unlikely —
  and can never cause a false accept. Digests are permitted only for
  uniqueness checks, never for reference resolution.

Budget is therefore 16–64 bytes per entity (tens of MiB for a
million-entity archive) — acceptable and documented, not accidental.
There is no
decompression step in v1, so decompressed-size attacks are excluded by
construction; declared `bytes`/`count`/`byte_size` values must exactly match
observed sizes, which bounds total I/O before deep reads begin.

## Verification

`verify` is read-only with respect to the published bundle and must pass before
any restore; it writes only the private verifier scratch described below.
Ordered phases:

1. **Shape.** Path is a directory not matching `*.tmp-*`; `COMPLETE` exists,
   parses, format/version accepted, manifest digest matches; manifest
   parses, format/version accepted, unknown keys absent; directory contains
   exactly the contractual entries; every entry is a regular file or
   directory — no symlinks, no hard links, no foreign files (including in
   `objects/` shards). Group- or world-**writable** entries are fatal
   (tampering surface); relaxed read permissions are reported as a warning
   but do not fail verification, since transport may not preserve modes and
   the contents are the owner's own secrets either way.
2. **Stream integrity.** Per stream: byte size, SHA-256, and line count
   equal the manifest; every line satisfies the encoding rules and its
   schema (key order/set, types, vocabularies, integer ranges, UTF-8).
3. **Uniqueness.** Primary IDs unique per stream; cross-stream ID collisions
   between entity kinds fatal; natural keys unique: `events.seq`,
   non-null `events.dedupe_key`, `tags.name`, artifacts
   (`blob_hash`,`kind`), vcs_changes (`vcs_workspace_id`,`kind`,`change_id`),
   vcs_workspaces (`kind`,`repo_fingerprint`), history_record_tags pair,
   history_record_links tuple.
4. **References.** Every `*_id` reference resolves to a record in the
   correct stream, including session self-references, polymorphic link
   targets, and every `transcript_blob_id`/`input_blob_id`/
   `output_blob_id`/`payload_blob_id` resolving to an artifact record.
   Sort-order violations are fatal (they break determinism and dedup).
5. **Blobs.** Bijection with stream 04; for each blob: shard/name
   well-formed and consistent, regular file, SHA-256 of contents equals
   name, size equals `byte_size`.

The rejection matrix. Each row is independently fatal and is reported with
the stable machine-readable error name below plus a location (stream/line/
path):

| error name | rejects |
| --- | --- |
| `marker_missing` | no `COMPLETE` file (unpublished/incomplete bundle) |
| `marker_invalid` | `COMPLETE` over 4 KiB, malformed JSON, wrong key grammar, or noncanonical marker |
| `manifest_digest_mismatch` | `manifest.json` bytes do not hash to `manifest_sha256` |
| `manifest_too_large` | `manifest.json` exceeds 16 MiB |
| `format_unsupported` | unknown manifest/marker `format`, or numeric `format_version` other than 1 (including newer or out-of-range values) |
| `unknown_field` | unknown key in the manifest or in any stream record |
| `layout_mismatch` | missing/extra/renamed bundle entry or stream file; manifest `path` differing from the contractual value (traversal attempts never influence I/O because no archive-supplied path is ever joined into the filesystem — the difference itself is the error) |
| `stream_integrity_mismatch` | stream digest, byte size, or line count differs from the manifest |
| `stream_truncated` | short read or missing final newline |
| `line_too_long` | any JSONL line exceeds 32 MiB |
| `record_malformed` | invalid UTF-8/JSON, duplicate keys, explicit `null`, wrong key order, wrong type, malformed UUID/hex, integer out of range |
| `vocabulary_unknown` | enum value outside the normative vocabularies |
| `duplicate_id` | duplicate primary ID within or across streams |
| `natural_key_conflict` | duplicate natural key (`seq`, `dedupe_key`, `tags.name`, artifact/vcs/link/tag tuples); includes digest-collision false rejects |
| `stream_unsorted` | sort-order violation per the native-type comparison |
| `dangling_reference` | any `*_id` or polymorphic target that does not resolve |
| `blob_missing` | artifact `blob_hash` with no blob file |
| `blob_unreferenced` | blob file matching no artifact record |
| `blob_mismatch` | blob content hash ≠ name, size ≠ `byte_size`, or malformed shard/name |
| `special_file` | symlink, hard link (`st_nlink > 1`), or non-regular file anywhere in the bundle |
| `permissions_writable` | group- or world-writable bundle entry |
| `size_cap_exceeded` | declared sizes exceed an operator-supplied cap |

Implementations may append more specific error names in later tickets but
must not repurpose or weaken the ones above.

### `ctx archive verify` CLI contract

`ctx archive verify <bundle>` performs the complete verification pass above and
never writes to the published bundle. It creates a private sibling
`.ctxar-verify-*/state.sqlite` scratch database beside the bundle and normally
removes it; a crash can leave that sensitive scratch residue. It exits `0` only
after all phases succeed. A
verification rejection exits `1`; command-line usage errors retain clap's exit
code `2`. The human failure form includes the stable category and the
explicitly requested bundle path, but never includes transcript fields or
verifier temporary paths.

With `--json`, success writes exactly one JSON object to stdout and nothing to
stderr:

```json
{"format":"ctx-archive","format_version":1,"path":"...","verified":true,"entity_count":0,"objects":{"count":0,"total_bytes":0}}
```

With `--json`, a verification rejection writes exactly one JSON object to
stderr, nothing to stdout, and exits `1`:

```json
{"error":{"code":"stream_truncated","message":"...","path":"..."}}
```

The `error.code` values are the rejection-matrix names in this document. The
message is a stable, non-sensitive summary; consumers must branch on `code`,
not on message text. The store API carries the same exhaustive typed
`ArchiveVerificationCode` taxonomy; diagnostics are retained separately and
are not used to derive the public code. `--max-entities`, `--max-objects`, `--max-object-bytes`,
and `--max-bytes` may lower the fixed v1 ceilings. Values above the fixed
ceilings are rejected as `size_cap_exceeded`; defaults are 10,000,000 entities,
1,000,000 objects, 4 GiB per object, and 16 GiB total stream-plus-object bytes.

## Restore

v1 restore targets a **fresh data root only**. Merging into an existing
store is explicitly out of scope (that is #189's contract; #188 owns
selective rehydration and reimport suppression). This keeps v1's conflict
policy trivial: there is nothing to conflict with.

Protocol:

1. Run full verification; refuse on any failure.
2. Refuse if the target root exists at all, including an empty directory. Create
   a staging root `<target>.tmp-<restore-uuid>` (0700) beside the target.
3. Initialize a normal store in the staging root via the standard creation
   path (current schema version 1005, WAL, 0700/0600, `objects/`
   directory). The archive never dictates schema DDL.
4. In **one write transaction**, insert streams in numbered order using
   dedicated verbatim restore INSERTs — **not** the import/upsert business
   logic, which may coalesce fields, refresh timestamps, or apply
   defaults. Every archived field is stored back byte-for-byte: IDs,
   `seq`, timestamps, enums, sync/visibility columns, previews, and the
   verbatim `_json` strings. Exactly two derived columns may be
   recomputed, and nothing else: `artifacts.blob_path` (recomputed as
   `objects/<shard>/<hash>`) and the legacy `history_records.created_at`/
   `updated_at` text columns (recomputed as RFC3339 UTC renderings of the
   corresponding millisecond fields, the store's storage convention).
   Sessions are inserted in two phases inside the same transaction: first
   with `parent_session_id`/`root_session_id` as NULL, then a deterministic
   UPDATE pass setting both — id-sorted order cannot guarantee parents
   first, and store connections run with foreign keys ON. All other streams
   insert directly in stream order.
5. Copy blobs into the staging root's `objects/` store (bounded chunks,
   re-hashing during copy), before the transaction commits or under the
   store's existing blob-guard ordering — database rows must never commit
   ahead of the bytes they reference.
6. Rebuild derived state inside the same write scope using the store's
   existing projection rebuild (`refresh_search_index` semantics): the active
   record/event FTS projections and the `record_search_rowids`/
   `event_search_rowids` maps are cleared and repopulated in lockstep, per the
   store invariant that maps are maintained manually in the same write
   transaction. `artifact_search` is cleared and intentionally remains empty
   and unused; it is not repopulated by restore. This rebuild is what licenses
   the verbatim INSERTs of step 4 to skip per-row projection maintenance: the
   active projections and maps are recreated from the restored rows before the
   write scope ends, so the store's write-path invariant still holds.
7. Post-restore checks, all mandatory: per-table counts equal manifest
   stream counts; `PRAGMA foreign_key_check` empty; `PRAGMA quick_check`
   ok; projection row counts consistent with the rebuild's own accounting;
   blob file count equals `objects.count`.
8. Publish: `fsync`, exclusive rename staging root to the target path, then
   `fsync` the parent. The exclusive rename is the publication point. Before
   it, handled failure deletes staging and leaves the target absent. A parent
   `fsync` failure can be reported after the complete target exists; operators
   must inspect and verify before retrying and must never overwrite the target.

Restored stores preserve every archived column byte-for-byte — entity IDs,
`events.seq` ordering, citations (summary `citations_json` target IDs
remain valid because IDs are never rewritten), counts — and active record/event
search behavior after the projection rebuild. Consequently, re-exporting a restored store
(with pinned `archive_id`/`created_at_ms`) reproduces the original streams
byte-identically; the round-trip test relies on this.

Post-restore caveat (by design, resolved to #188): the import ledgers are
not archived, so if original provider transcript files are still present on
the restoring machine, the next discovery/refresh will rescan them. Event
`dedupe_key` and artifact (`blob_hash`,`kind`) uniqueness make that rescan
an idempotent upsert, not a duplication; suppression of rescans for
compacted-then-restored content is #188's retention-ledger deliverable.

## Excluded data and rationale

The archive carries canonical content only. Excluded, deliberately:

| excluded | rationale |
| --- | --- |
| FTS5 tables (`ctx_history_search`, `event_search`, `artifact_search`) and all their SQLite shadow tables | Derived projections; active record/event projections are rebuilt at the destination, while `artifact_search` is cleared and intentionally remains empty/unused. Copying them would freeze tokenizer/rowid details into the format. |
| `record_search_rowids`, `event_search_rowids` | Performance caches keyed by store-local FTS rowids; meaningless outside the database file that assigned them. |
| `catalog_sessions`, `source_import_files` | Machine-local discovery/import ledgers keyed by absolute source paths. Restoring them on another machine would assert the presence of files that do not exist there; they regenerate on rescan. Retention/suppression state is #188. |
| `sync_cursors`, `sync_batches`, `sync_outbox` | Upstream hosted-sync scaffolding; device- and team-scoped operational state, not content. |
| `local_devices`, `local_workspaces` | Device identity must not travel into another machine's store (only the informational `origin_device_id` manifest field is carried). |
| `audit_log` | Machine-local operational log of store actions, not agent history; replaying it onto a new store would be false. |
| `source_health`, `source_health_key` | Keyed by a per-store 32-byte secret that must never leave the store; classifications are recomputable. |
| SQLite internals: WAL/SHM files, `PRAGMA user_version`, page images, freelists | This is a logical archive; physical database state is what makes file copies non-portable. |
| `config.toml`, `logs/`, `spool/`, `device.json` | Configuration and runtime residue, not indexed history. |

Rule of thumb encoded above: if the store can rebuild it, or it is keyed to
this machine/database instance, it stays out of the archive.

## Privacy and security posture

- An archive is exactly as sensitive as `work.sqlite`: verbatim prompts,
  code, commands, credentials, transcript text, plus full artifact bytes.
  Treat bundle files as secrets; the 0700/0600 modes are a floor, not a
  sharing mechanism. Unpublished staging and verifier scratch residue is also
  sensitive, even when incomplete or only potentially complete.
- v1 provides **integrity, not confidentiality or authenticity**: SHA-256
  digests detect corruption and truncation, not tampering by an adversary
  who can rewrite the manifest and marker. Signing/encryption are external
  by design (`age`, OS volume encryption); ctx makes no encryption claims
  and must not grow key management.
- No component of create/verify/restore performs network I/O or reads outside
  the store, explicit bundle paths, and the private staging/scratch directories
  it creates. Create and restore write content only to private staging
  directories and publish them by exclusive rename; verify writes only its
  private sibling scratch database (`.ctxar-verify-*/state.sqlite`) and never
  modifies the published bundle. These are local filesystem effects, not
  transport or upload.

## Compatibility and evolution

- v1 readers accept exactly `format_version` 1 and fail closed otherwise —
  the same posture as the store's schema gate (versions 16–999 and above
  the current chain fail closed). No silent best-effort reads of future
  archives.
- Any additive or semantic change — new stream, new field, compression,
  subset scopes for #188, merge/origin extensions for #189 — is a new
  `format_version` with its own review. Removing this contract's checks is
  never a compatible change.
- Restore into newer schema versions is expected to keep working because
  the staging store's schema DDL comes from the current binary's normal
  creation path while row **contents** are the archived values stored
  verbatim (never rewritten by import/upsert logic); a future migration
  that reshapes canonical tables must state how v1 streams map onto it (or
  bump the format and provide a converter).
- Enum vocabulary growth (notably `provider`) is coupled to store CHECK
  migrations; archives written by newer binaries with unknown vocabularies
  are rejected by older readers, matching the store's own downgrade story.

## Test plan (high-value, synthetic, macOS-inclusive)

All tests use synthetic fixtures, temp homes/data roots, and no network,
per the repo testing guardrails; the full suite must pass on macOS.

1. **Round-trip fidelity**: build a synthetic store exercising every stream
   (incl. session parent/child + edges, shared blobs across artifact kinds,
   soft-deleted rows, unicode + embedded-JSON-heavy payloads); export,
   verify, restore to fresh root; assert byte-identical stream re-export,
    ID/seq/count/citation parity, and representative active record/event search
    + show output equality after rebuild (with `artifact_search` still empty).
2. **Determinism**: two exports of the same store with pinned
   `archive_id`/`created_at_ms` are byte-identical.
3. **Atomic publication**: kill/fail injection before rename leaves only an
   unpublished `*.tmp-*` directory, which may already contain `COMPLETE`; the
   published target is absent and verify refuses staging paths. A parent-fsync
   failure is checked separately because the target may already exist.
4. **Adversarial corpus** (table-driven, one fixture per rejection-matrix
   row): truncated stream, flipped bit in stream/blob/manifest, duplicated
   ID, conflicting natural key, dangling reference, missing blob, extra
   blob, symlinked blob, hard-linked stream, non-contractual path in
   manifest, unknown field, explicit null, unsorted stream, unsupported
   version, oversized line, count mismatch.
5. **Restore transactionality**: failure injected before publication at each
    restore phase (mid-transaction, blob copy, projection rebuild, post-checks)
    leaves no staging root and an untouched target path; the parent-fsync case
    separately checks the already-published target.
6. **Permissions**: created bundle and restored root are 0700/0600
   throughout (Unix), including after rename publication.
7. **Bounded memory smoke**: export/verify/restore over a
   large-synthetic-count store (reusing the #186 profile harness) with an
   allocation ceiling or peak-RSS assertion.
8. **Reimport idempotence after restore**: restore beside still-present
   synthetic provider transcripts, run refresh, assert stable counts via
   dedupe keys.

## Implementation history

The writer, verifier, fresh-root restore, and user-facing documentation are
shipped. Later work such as archive-first compaction (#188) or multi-machine
merging (#189) requires its own format review and must not weaken this
verification matrix or imply that v1 itself merges stores.
