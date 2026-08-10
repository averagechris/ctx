# Issue #281: archive-first compaction decision record

Status: **accepted design contract** (documentation only). This record freezes
the decisions that the implementation tickets must use. It does not implement
planning, archive creation, importer suppression, hot deletion, restore,
physical reclaim, or a schema migration.

The portable full-archive container and its fifteen stream schemas remain
normatively specified by [Archive Format v1](archive-format-v1.md). This record
defines the *selective-compaction* extension and its ledger; it links to v1
instead of copying that specification. The current store is SQLite schema
v1002. Archive format versions, selective archive versions, and SQLite schema
versions are three deliberately independent number spaces.

Related implementation tickets are #282 (planner), #283 (selective archive),
#284 (suppression), #285 (archive-backed deletion), #286 (rehydration), and
#287 (optional physical reclaim). #189 is the future offline origin/merge
compatibility issue. #199 is the read-only storage-diagnostics contract.

## 1. Scope and non-goals

Compaction is an explicit, local, archive-first workflow:

1. A read-only planner selects completed sessions and computes a closed,
   content-addressed dependency set.
2. A selective archive writes that exact set using the streaming, checksummed,
   private-bundle mechanics of v1.
3. Only a complete, verified, atomically published archive can be registered
   in the ledger or activate suppression.
4. A later explicit commit may logically remove only the archive-backed,
   exclusively owned hot rows. Shared rows remain hot.
5. A later explicit restore may rehydrate all or a selected closed subset.
6. Optional physical reclaim is a separate operation and is never implied by
   logical compaction or disk pressure.

No step contacts a network, discovers a remote peer, changes a source
repository, encrypts data, or treats a SQLite file copy as an archive. A
selective archive is not a second meaning for `ctx-archive` format v1.

## 2. Names and compatibility

### 2.1 Canonical content

The current canonical streams, in their existing order, are:

1. `capture_sources`
2. `vcs_workspaces`
3. `history_records`
4. `artifacts`
5. `sessions`
6. `session_edges`
7. `runs`
8. `events`
9. `vcs_changes`
10. `summaries`
11. `files_touched`
12. `tags`
13. `history_record_tags`
14. `history_record_links`
15. `record_edges`

These names and the column order/types are taken from
`crates/ctx-history-store/src/{lib,archive,restore}.rs` and
[archive-format-v1.md](archive-format-v1.md). Stable primary IDs are opaque
lowercase UUID strings, except that `events.seq` remains the store-global
integer ordering key and `history_record_tags` has the composite key
`(history_record_id, tag_id)`. In a ledger `entity_key`, that composite is
encoded as `history_record_id || ':' || tag_id`; UUIDs contain no colon, so
this is unambiguous. Object blobs are the SHA-256-addressed files
referenced by `artifacts.blob_hash`; they are not a sixteenth SQL entity
stream.

### 2.2 Selective archive family

The selective family is exact and fail-closed:

```json
{"format":"ctx-selective-archive","format_version":1}
```

It reuses v1's directory layout, JSONL encoding, canonical stream field
definitions, object naming, bounded streaming, integrity chain, permissions,
and verifier rejection posture by reference. It additionally requires
authenticated root dispositions, canonical graph algorithm identity, per-root
closure digests, archive membership, `deletion_set` membership, and
`plan_digest`, `closure_digest`, `membership_digest`, `root_set_digest`, and
`deletion_set_digest`. The exact selective manifest field order belongs to #283,
but these evidence fields and their meanings are fixed here.

Compatibility rules:

* `("ctx-archive", 1)` means a complete/full v1 archive. It is never treated
  as a selective archive and cannot satisfy a selective compaction ledger
  registration.
* `("ctx-selective-archive", 1)` is the only selective family/version this
  contract accepts. It must contain all fifteen stream files, even when a
  stream is empty, and its root/disposition, membership, and deletion-set
  evidence must describe the exact rows present in those streams and the exact
  object set.
* Unknown families, version 0, future versions, an omitted membership or
  closure digest, a full/selective scope mismatch, a stream mismatch, or a
  source path used as membership identity are fatal. There is no best-effort
  import, version downgrade, or partial registration.
* `source_schema_version` is diagnostic metadata. A v1002 writer can produce
  a selective bundle without changing `PRAGMA user_version`; a reader does
  not choose its destination schema from that field. The archive family and
  version are not SQLite schema numbers.

This deliberate family split leaves room for a future selective format change
without changing the established meaning of [Archive Format v1](archive-format-v1.md).

## 3. Retention signal, cutoff, and boundaries

The planner accepts one explicit integer `cutoff_ms`. It never derives a
cutoff from wall clock time, row order, archive creation time, or a source
path. A session is an eligible completed root **only when both** are true:

```text
sessions.status = 'completed'
AND sessions.ended_at_ms IS NOT NULL
AND sessions.ended_at_ms <= cutoff_ms
```

The comparison is inclusive. A session ending at exactly the cutoff belongs in
the candidate set. `started_at_ms`, `history_records.completed_at_ms`, and
provider import status are evidence and reporting fields, not substitutes for
the completed-session signal.

Boundary decisions are deterministic:

* `status = 'completed'` with a NULL `ended_at_ms` is **ambiguous** and is not
  selected. The plan reports it as ambiguous and retains it and its dependants.
* A non-`completed` session with a non-NULL end time is **not completed** and
  is not selected. It is not silently promoted based on the timestamp.
* `started_at_ms > ended_at_ms`, an invalid status/time combination, a
  dangling reference, or an invalid canonical row is a fail-closed planning
  error, not an invitation to guess.
* Equal timestamps do not form a tie. Every eligible session satisfying the
  inclusive predicate is selected; every one above the cutoff is excluded.
  IDs are used only for deterministic output ordering.
* Every child session reached from a selected or owned parent is independently
  evaluated with the same completed + non-NULL `ended_at_ms` + inclusive-cutoff
  predicate before ownership is assigned. Only an independently eligible child
  may be `owned_child` and deletion-authorized. An active, ambiguous, or
  post-cutoff child receives the corresponding retained disposition, remains
  outside deletion membership, and keeps all of its supporting content hot.
* An empty eligible set is a valid, stable empty plan. It has a digest and no
  archive may be registered as a deletion authorization unless its membership
  is also empty.

### Worked example: inclusive retention boundary

With `cutoff_ms = 1_000`, `s1` has `status=completed, ended_at_ms=1_000`,
`s2` has `status=completed, ended_at_ms=1_001`, and `s3` has
`status=completed, ended_at_ms=NULL`. The plan selects `s1`, excludes `s2`,
and reports `s3` as ambiguous. If `s1` has a `session_edges` edge to `s3`,
`s3` is included as a closure dependency but is not counted as a selected
completed root and is not independently deletable.

### Root dispositions and deletion authorization

The planner produces two different authenticated products:

1. **Archive membership**: every selected root and every row needed to make
   the selected roots and their dependencies complete.
2. **Deletion authorization**: a separately authenticated subset containing
   only selected roots and their owned children that are proven exclusive.

Membership is not permission to delete. In particular, an active, post-cutoff,
or ambiguous session reached as a dependency, its complete supporting rows,
and every shared resource are retained even when the planner can prove that no
other currently hot row references them. Such a row never appears in the
deletion-set membership or deletion-set digest. The deletion commit accepts
only the exact authenticated deletion set; it does not recalculate a broader
set from “exclusive” flags.

The plan records every candidate root's disposition as `selected`,
`excluded_active`, `excluded_post_cutoff`, or `excluded_ambiguous`. It also
records each archived member's disposition and whether it appears in the
deletion set. A root/dependency mapping is authenticated by the canonical
graph algorithm in the next section, the sorted root digest, the union
membership digest, and the separate deletion-set digest. Implementations must
not replace this with a table-order walk or a fresh deletion query.

## 4. Directional dependency closure

The planner uses a canonical directed graph and algorithm, not a connected
component. The algorithm is part of the authenticated plan contract:

1. Start with the sorted eligible root sessions.
2. Evaluate every child session reached from a selected or owned parent against
   the completed + non-NULL end + inclusive-cutoff predicate. Add it as an
   **owned child** only when it is independently eligible; otherwise add it
   with `retained_active`, `retained_post_cutoff`, or `retained_ambiguous`, keep
   it out of deletion membership, and protect all supporting content needed to
   keep it complete. Owned-child traversal may recurse through another eligible
   owned child (for example root session → run → event → file touch), but never
   through a shared resource or a retained/dependency owner.
3. Add **referenced dependencies** named by an included row's foreign key or
   polymorphic link. A dependency is included to keep the selected content
   complete, but the algorithm never reverse-fans out from that dependency to
   its other owners or children.
4. Add **shared resources** once when directly referenced. Shared resources
   are leaves for closure purposes: they do not cause their other sessions,
   records, changes, or files to be traversed.
5. Apply the explicit boundary-edge rule below. A crossing edge is archived
   as evidence and its outside endpoint is a retained dependency, but the
   outside endpoint's owned children are not traversed.
6. Repeat steps 2–5 only for newly included owned children and direct
   references. A visited `(entity_kind, entity_key)` set makes the result
   finite and deterministic. Cycles in owned children are valid finite graphs;
   dangling or malformed references fail closed.

This algorithm authenticates each root's closure digest and the sorted union
of root memberships. It is therefore a canonical graph+algorithm contract,
rather than a permissive “walk until connected” interpretation. Each member
also records one of these dispositions:

* `selected_root`: an eligible root selected by the inclusive cutoff;
* `owned_child`: content owned by a selected aggregate; for a child session,
  this role additionally proves independent completed + inclusive-cutoff
  eligibility. It is deletable only if it is in the separate deletion set and
  exclusive;
* `referenced_dependency`: a row needed to resolve an included reference;
* `shared_resource`: a directly referenced source, workspace, artifact, tag,
  object, or other shared value that must remain available to another owner;
* `boundary_edge`: an edge crossing from selected content to retained content;
* `retained_active`, `retained_post_cutoff`, or `retained_ambiguous`: a
  session dependency that must remain complete regardless of exclusivity.

Only `selected_root` and `owned_child` members can appear in the separately
authenticated deletion set. `referenced_dependency`, `shared_resource`,
`boundary_edge`, and every retained-session disposition are never deletable by
this archive, even when an implementation's current reference count is one.
For every retained dependency, the algorithm follows its direct references
needed to make that dependency row complete (for example a dependency
session's parent/root/source/transcript, or a dependency event's payload
object), assigning those rows dependency/shared dispositions as appropriate.
It does not enumerate the dependency's owned children or other owners.

| Stream | Directional rule |
| --- | --- |
| `sessions` | Selected roots are seeds. A session reached from a selected/owned session by a `session_edges.edge_type = 'parent_child'` edge in the `from_session_id → to_session_id` direction must first be independently evaluated against completed + non-NULL end + inclusive cutoff. Only an eligible child is `owned_child`; an active, post-cutoff, or ambiguous child receives its matching retained disposition, stays outside deletion membership, and keeps all supporting content hot. `parent_session_id`/`root_session_id`, `history_record_id`, `capture_source_id`, and `transcript_blob_id` are otherwise referenced dependencies. For a retained/dependency session, follow the direct references needed to keep its row and supporting content complete, but do not traverse its owned children. Do not include sibling sessions merely because they share a `history_record_id`. |
| `session_edges` | Include an edge when it is owned by selected content or crosses its boundary. A `parent_child` edge directed from selected/owned parent to child permits owned-session classification only after the child's independent eligibility check; an ineligible child is retained and the edge cannot authorize its deletion. Any other edge, or a reversed/uncertain parent-child relation, is a `boundary_edge`, retains its outside endpoint as a dependency, and does not traverse that endpoint's children. |
| `history_records` | Include the record directly referenced by a selected/owned session as a referenced dependency unless the plan explicitly declares that record the selected aggregate. Never reverse-expand from a record to every session that names it. |
| `runs` | A run with `session_id` equal to a selected/owned session is an owned child; a run reached only through another dependency is a referenced dependency. Include `input_blob_id` and `output_blob_id` as artifact dependencies. |
| `events` | An event with `session_id` equal to a selected/owned session, or reached through an owned run, is an owned child. An event reached only through a dependency is retained as a dependency. Preserve `seq` exactly. Include `payload_blob_id` and `capture_source_id`. |
| `summaries` | A summary directly owned by a selected/owned session is an owned child; a summary reached through a dependency is retained. Preserve `citations_json` verbatim and resolve its declared citations as dependencies without reverse fan-out. |
| `files_touched` | A row reached through an owned session's run/event is an owned child. A row reached only through a dependency is retained. `path` and `old_path` are content, never filesystem instructions. |
| `history_record_links` | Include links owned by an included selected aggregate or retained record. Resolve each polymorphic target as a referenced dependency; never traverse the target's owners. |
| `record_edges` | Include an edge owned by selected content or crossing its boundary. The outside record is a retained dependency and its other edges/children are not traversed. |
| `vcs_workspaces` | Include a workspace only when directly referenced by an included row. It is a shared resource and never expands to all workspace changes, sessions, or files. |
| `vcs_changes` | Include a change directly referenced by an included row or directly owned by a selected aggregate. Parent-change IDs are preserved content; they do not reverse-expand a workspace or change graph. |
| `artifacts` | Include an artifact only when directly referenced by a selected/owned session, run, event, or link. An artifact is a shared resource when another hot owner references it; it never expands to other owners. |
| `capture_sources` | Include a directly referenced source as a shared resource. A source does not expand to every session or row that uses it. |
| `tags` | Include a tag directly referenced by an included assignment as a shared resource. A tag does not expand to other records. |
| `history_record_tags` | Include assignments owned by an included selected aggregate/record; tag rows are direct shared dependencies only. |

The implementation must publish the canonical graph algorithm identifier and
the sorted per-root closure records in authenticated plan/archive evidence.
The evidence includes selected roots, excluded root dispositions, member
dispositions, observed child status/end-time/cutoff decisions, the union
membership digest, the deletion-set digest, and counts for each. A verifier
rejects an archive whose stream rows cannot reproduce that evidence, or whose
child status/time evidence conflicts with an `owned_child` disposition or
deletion authorization.

### Shared rows, objects, and cross-session edges

The closure includes a shared row once, but membership and deletion authority
remain separate. An object is identified by `blob_hash`, not artifact ID;
multiple artifacts may share it, and an artifact outside the plan may keep it
hot. Shared artifact rows and objects are `shared_resource` members and cannot
appear in the deletion set. Only a later explicit reclaim may unlink an object
after no hot artifact references it.

### Worked example: shared workspace without reverse fan-out

An old completed session `S-old` and a recent active session `S-new` both point
to workspace `W`. A plan selecting `S-old` includes `W` as one shared-resource
dependency, but it does not traverse `W` to find `S-new`, its runs, or its
events. `S-new` is absent from membership unless a direct session edge or
reference reaches it; if such an edge exists, only that edge and `S-new` as a
retained boundary dependency are included. Neither `S-new` nor its activity
can enter the deletion set.

### Worked example: shared dependency

Sessions `A` and `B` reference artifacts `a1` and `a2`, and both artifacts have
the same `blob_hash = h`. A plan for `A` includes only the directly reachable
artifact rows and one object `h`; `a1`, `a2` if directly reached, and `h` are
`shared_resource` when `B` still references them. None appears in the deletion
membership. Deleting `A` removes only authorized selected/owned rows; `B`, its
edge rows, shared artifacts, and object `h` remain hot. A later verified plan
for `B` can make `h` reclaimable, but only #287 may physically unlink it.

## 5. Stable identity and content keys

No ledger identity contains an absolute path, `cwd`, `root_path`, hostname,
username, or a guessed path backfill. Paths remain verbatim canonical content
for citation fidelity, as specified by v1, but are never used to recognize
previously archived content or to read the restoring machine's filesystem.

All ledger keys are lowercase 64-hex SHA-256 strings. They use a domain tag
and unambiguous big-endian length-prefixed fields:

```text
SHA256("ctx-compaction/<key-kind>/v1" || field_count || len(field_1) || field_1 || ...)
```

`content_key` for a canonical member covers the stream name, entity key, and
the exact stored canonical field values in the v1 column order (including
verbatim `_json` strings and NULL/omission markers). For an object member,
`content_key` is its blob SHA-256. Thus stable IDs and exact content can be
compared without storing transcript bodies in the ledger.

Importer suppression has two keys:

* `identity_key` is derived from a provider/source-format stable identity: the
  provider namespace plus an external session/source ID and external agent ID,
  plus the explicit origin device ID when needed to disambiguate devices.
  It never includes a source path. If no stable external identity exists,
  the importer uses a content-derived identity that excludes path and local
  observation metadata; it must not invent one from a path.
* `content_key` is the digest of the provider's canonical source material
  after excluding path, machine-local observation, and volatile catalog fields.
  A provider that cannot produce either a stable identity or a path-independent
  content key is not suppressible and must report that fact rather than guess.

An exact `(identity_key, content_key)` match in active suppression is skipped.
The same identity with different content is a `conflict`, not a silent update
or overwrite; an explicit user override is required. A path move with the
same keys remains suppressed. A new content-derived identity can be imported
normally when there is no stable identity conflict.

### Worked example: reimport suppression

An archive registers a provider source with identity `I` and content `C` after
the source at `/old/provider/session.jsonl` has been compacted. Discovery later
finds the same source at `/new/provider/session.jsonl` and computes `(I,C)`;
normal import skips it, without comparing either path. If the file's canonical
content is `C2`, `(I,C2)` is recorded as a conflict and automatic import stops.
An explicit override may acknowledge the changed content, after which the
normal import can proceed and the old `(I,C)` suppression remains an audit
fact.

### Worked example: two archive guards

Archives `X` and `Y` both support the same `(I,C)`, so one effective fact has
two active archive associations. Restoring `X` changes only `X`'s association
to `restored`; the effective fact remains `active` because `Y` still guards the
pair. An explicit override of `X` has the same result. A recompaction creates
archive `Z` and a third association rather than rewriting `X` or `Y`.

## 6. Ledger schema and keys

The following is the implementable logical schema. Exact SQL formatting may
follow the style in `lib.rs`, but names, columns, checks, keys, and indexes are
not optional. Ledger tables have no foreign keys to canonical content tables:
compaction must remain auditable after hot rows are deleted. They do have
foreign keys to the ledger archive table, with `ON DELETE RESTRICT`; ledger
history is never cascaded away. The schema has eight ledger tables:
`compaction_archives`, `compaction_archive_roots`,
`compaction_archive_members`, `compaction_deletion_members`,
`compaction_suppression_facts`, `compaction_archive_suppressions`,
`compaction_operations`, and `compaction_operation_objects`.

### `compaction_archives`

| column | type/constraint | meaning |
| --- | --- | --- |
| `archive_id` | `TEXT PRIMARY KEY NOT NULL` | Manifest `archive_id`, canonical UUID string; bundle identity. |
| `format_family` | `TEXT NOT NULL CHECK (format_family = 'ctx-selective-archive')` | Exact selective family. |
| `format_version` | `INTEGER NOT NULL CHECK (format_version = 1)` | Selective family version, not SQLite `user_version`. |
| `scope_kind` | `TEXT NOT NULL CHECK (scope_kind = 'selective')` | Prevents a full archive from masquerading as selective. |
| `manifest_sha256` | `TEXT NOT NULL CHECK (length(manifest_sha256) = 64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*')` | Digest of exact manifest bytes. |
| `plan_digest` | `TEXT NOT NULL CHECK (length(plan_digest) = 64 AND plan_digest NOT GLOB '*[^0-9a-f]*')` | Authenticated planner input and output digest. |
| `closure_digest` | `TEXT NOT NULL CHECK (length(closure_digest) = 64 AND closure_digest NOT GLOB '*[^0-9a-f]*')` | Digest of the canonical directional closure union. |
| `membership_digest` | `TEXT NOT NULL CHECK (length(membership_digest) = 64 AND membership_digest NOT GLOB '*[^0-9a-f]*')` | Digest of sorted member/content tuples. |
| `root_set_digest` | `TEXT NOT NULL CHECK (length(root_set_digest) = 64 AND root_set_digest NOT GLOB '*[^0-9a-f]*')` | Digest of sorted root IDs, cutoff decisions, and per-root closure digests. |
| `deletion_set_digest` | `TEXT NOT NULL CHECK (length(deletion_set_digest) = 64 AND deletion_set_digest NOT GLOB '*[^0-9a-f]*')` | Digest of the separately authorized deletion membership. |
| `deletion_member_count` | `INTEGER NOT NULL CHECK (deletion_member_count >= 0)` | Cross-check for deletion-set rows; never inferred from archive membership. |
| `source_schema_version` | `INTEGER NOT NULL` | Writer diagnostic only. |
| `origin_device_id` | `TEXT CHECK (origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256)` | Optional stable origin marker; no local device row is imported. |
| `created_at_ms` | `INTEGER NOT NULL` | Manifest creation time. |
| `published_at_ms` | `INTEGER NOT NULL` | Publication observation time. |
| `verified_at_ms` | `INTEGER NOT NULL` | Successful complete verification time. |
| `updated_at_ms` | `INTEGER NOT NULL` | Last committed ledger transition. |

Constraints and indexes:

* `UNIQUE(format_family, format_version, manifest_sha256)` makes exact
  duplicate publication registration idempotent.
* `CHECK` expressions reject malformed lowercase hex; no path column exists.
* Index `compaction_archives_updated(updated_at_ms)` supports bounded
  diagnostics and recovery scans.

Archive aggregate status is **derived**, not a mutable column. The read-only
`compaction_archive_status` view uses this exact precedence: `conflict` if any
member or association for this
archive, or any effective fact referenced by one of its associations, is in
`conflict`; `restored` after a committed whole-archive restore has every
member `restored/present`; `partially_restored` after any committed selective
restore changes at least one member; `compacted` after a
committed deletion has every row in the deletion membership `compacted/absent`;
`suppression_active` if an effective active fact is associated with the
archive; otherwise `verified`. A restore never re-enables deletion for the
same archive: a new compaction requires a new plan and archive. This defines
the only legal aggregate progression and avoids persisted status cycles.

### `compaction_archive_roots`

This table records selection decisions and authenticates the per-root mapping
while the canonical graph algorithm defines how each closure is computed.

| column | type/constraint | meaning |
| --- | --- | --- |
| `archive_id` | `TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT` | Owning archive. |
| `root_session_id` | `TEXT NOT NULL` | Candidate session ID; no FK because excluded candidates need not be archived. |
| `root_disposition` | `TEXT NOT NULL CHECK (root_disposition IN ('selected','excluded_active','excluded_post_cutoff','excluded_ambiguous'))` | Exact cutoff decision. |
| `cutoff_ms` | `INTEGER NOT NULL` | Explicit planner boundary. |
| `observed_status` | `TEXT NOT NULL CHECK (observed_status IN ('started','active','idle','completed','failed','interrupted','imported'))` | Status used for the decision. |
| `observed_ended_at_ms` | `INTEGER` | Boundary evidence, including NULL for ambiguous completion. |
| `root_closure_digest` | `TEXT CHECK (root_closure_digest IS NULL OR (length(root_closure_digest) = 64 AND root_closure_digest NOT GLOB '*[^0-9a-f]*'))` | Digest of the root's canonical directional closure; required for `selected`. |
| `root_member_count` | `INTEGER NOT NULL CHECK (root_member_count >= 0)` | Per-root membership cross-check. |
| `root_deletion_member_count` | `INTEGER NOT NULL CHECK (root_deletion_member_count >= 0)` | Per-root authorized deletion cross-check. |
| `created_at_ms` | `INTEGER NOT NULL` | Registration time. |

Primary key: `(archive_id, root_session_id)`. Add
`compaction_roots_disposition(root_disposition, root_session_id)` and
`compaction_roots_digest(archive_id, root_closure_digest)`. The verifier
requires a non-NULL `root_closure_digest` and nonzero membership for selected
roots, and requires zero counts for excluded roots.

### `compaction_archive_members`

| column | type/constraint | meaning |
| --- | --- | --- |
| `archive_id` | `TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT` | Owning bundle. |
| `entity_kind` | `TEXT NOT NULL CHECK (entity_kind IN ('capture_sources','vcs_workspaces','history_records','artifacts','sessions','session_edges','runs','events','vcs_changes','summaries','files_touched','tags','history_record_tags','history_record_links','record_edges','object_blob'))` | One of the fifteen canonical stream names or `object_blob`. |
| `entity_key` | `TEXT NOT NULL CHECK (length(entity_key) BETWEEN 1 AND 512)` | UUID, `seq`-independent canonical entity key; for `history_record_tags`, `history_record_id || ':' || tag_id`; for objects, blob hash. |
| `content_key` | `TEXT NOT NULL CHECK (length(content_key) = 64 AND content_key NOT GLOB '*[^0-9a-f]*')` | Exact canonical content digest. |
| `disposition` | `TEXT NOT NULL CHECK (disposition IN ('selected_root','owned_child','referenced_dependency','shared_resource','boundary_edge','retained_active','retained_post_cutoff','retained_ambiguous'))` | Directional closure role and retention decision. |
| `ownership` | `TEXT NOT NULL CHECK (ownership IN ('exclusive','shared_retained'))` | Planner classification, revalidated at commit. |
| `membership_state` | `TEXT NOT NULL CHECK (membership_state IN ('verified','suppressed','compacted','restored','conflict'))` | Per-member state machine. |
| `hot_state` | `TEXT NOT NULL CHECK (hot_state IN ('present','absent','conflict'))` | Whether matching hot content is present; does not describe SQLite free pages. |
| `created_at_ms` | `INTEGER NOT NULL` | Registration time. |
| `updated_at_ms` | `INTEGER NOT NULL` | Last state transition. |

Primary key: `(archive_id, entity_kind, entity_key)`, plus
`UNIQUE(archive_id, entity_kind, entity_key, content_key)` for exact
deletion-membership foreign keys. Add indexes
`compaction_members_entity(entity_kind, entity_key)`,
`compaction_members_content(content_key)`, and
`compaction_members_disposition(archive_id, disposition)` and
`compaction_members_state(archive_id, membership_state, hot_state)`. The
`entity_kind` is part of every comparison, so UUID reuse across kinds cannot
collide. There is no foreign key to an entity table and no inferred SQLite
rowid.

### `compaction_deletion_members`

This is the authenticated deletion authorization, deliberately separate from
archive membership. It can contain only selected roots and owned children;
there is no “exclusive dependency” fallback.

| column | type/constraint | meaning |
| --- | --- | --- |
| `archive_id` | `TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT` | Authorizing archive. |
| `entity_kind` | `TEXT NOT NULL CHECK (entity_kind IN ('capture_sources','vcs_workspaces','history_records','artifacts','sessions','session_edges','runs','events','vcs_changes','summaries','files_touched','tags','history_record_tags','history_record_links','record_edges','object_blob'))` | Must match the archive member. |
| `entity_key` | `TEXT NOT NULL CHECK (length(entity_key) BETWEEN 1 AND 512)` | Must match the archive member. |
| `content_key` | `TEXT NOT NULL CHECK (length(content_key) = 64 AND content_key NOT GLOB '*[^0-9a-f]*')` | Must match the archived content digest. |
| `authorization_reason` | `TEXT NOT NULL CHECK (authorization_reason IN ('selected_root','owned_child'))` | The only legal deletion roles. |
| `created_at_ms` | `INTEGER NOT NULL` | Authorization time. |

Primary key: `(archive_id, entity_kind, entity_key)`. Add a composite foreign
key to `compaction_archive_members(archive_id, entity_kind, entity_key,
content_key)` so authorization cannot name different content. Add
`compaction_deletion_members_entity(entity_kind,
entity_key)` and `compaction_deletion_members_archive(archive_id)`. The
sorted `(entity_kind, entity_key, content_key, authorization_reason)` rows
produce `compaction_archives.deletion_set_digest`; the count must equal
`deletion_member_count`. The registration transaction rejects a row unless
the referenced member's `disposition` equals `authorization_reason` and its
`ownership` is `exclusive`; this cross-table invariant is part of verification
because SQLite `CHECK` constraints cannot inspect another table. For a session
authorized as `owned_child`, verification also requires authenticated observed
status/end-time/cutoff evidence proving independent eligibility; conflicting or
missing evidence rejects registration and cannot be repaired by exclusivity.

### `compaction_suppression_facts`

This table contains the effective, path-independent facts consulted by an
importer. It is not an archive association: one fact can be supported by many
archives.

| column | type/constraint | meaning |
| --- | --- | --- |
| `identity_key` | `TEXT NOT NULL CHECK (length(identity_key) = 64 AND identity_key NOT GLOB '*[^0-9a-f]*')` | Path-independent provider/source identity. |
| `content_key` | `TEXT NOT NULL CHECK (length(content_key) = 64 AND content_key NOT GLOB '*[^0-9a-f]*')` | Path-independent canonical source content. |
| `effective_state` | `TEXT NOT NULL CHECK (effective_state IN ('active','restored','overridden','conflict'))` | Effective fact after precedence recomputation. |
| `origin_device_id` | `TEXT CHECK (origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256)` | Opaque audit marker only. |
| `created_at_ms` | `INTEGER NOT NULL` | First fact observation. |
| `updated_at_ms` | `INTEGER NOT NULL` | Last recomputation. |
| `last_error_code` | `TEXT CHECK (last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128)` | Stable non-sensitive conflict/diagnostic code, not content or path. |

Primary key: `(identity_key, content_key)`. Add
`compaction_suppression_facts_identity(identity_key, effective_state,
content_key)` and `compaction_suppression_facts_state(effective_state,
updated_at_ms)`. A fact with no archive association is allowed only for an
explicitly recorded content conflict; it blocks automatic import until
resolved.

### `compaction_archive_suppressions`

This association table records which archive supports a fact and what happened
to that archive's guard. It is intentionally many-to-one with facts.

| column | type/constraint | meaning |
| --- | --- | --- |
| `archive_id` | `TEXT NOT NULL REFERENCES compaction_archives(archive_id) ON DELETE RESTRICT` | Supporting archive. |
| `identity_key` | `TEXT NOT NULL CHECK (length(identity_key) = 64 AND identity_key NOT GLOB '*[^0-9a-f]*')` | Must match a suppression fact. |
| `content_key` | `TEXT NOT NULL CHECK (length(content_key) = 64 AND content_key NOT GLOB '*[^0-9a-f]*')` | Must match a suppression fact. |
| `association_state` | `TEXT NOT NULL CHECK (association_state IN ('active','restored','overridden','conflict'))` | State for this archive only. |
| `origin_device_id` | `TEXT CHECK (origin_device_id IS NULL OR length(origin_device_id) BETWEEN 1 AND 256)` | Opaque audit marker only. |
| `created_at_ms` | `INTEGER NOT NULL` | Association time. |
| `updated_at_ms` | `INTEGER NOT NULL` | Last association transition. |
| `last_error_code` | `TEXT CHECK (last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128)` | Stable failure class, not content or path. |

Primary key: `(archive_id, identity_key, content_key)`. Add a composite
foreign key to `compaction_suppression_facts(identity_key, content_key)` and
indexes `compaction_archive_suppressions_archive(archive_id,
association_state)` and `compaction_archive_suppressions_identity(identity_key,
association_state)`. Restoring or overriding one archive changes only its
association and then recomputes the effective fact; another archive's active
association therefore continues to guard the identical `(identity_key,
content_key)`.

### `compaction_operations`

This small coordination/audit table makes retry boundaries explicit without
making a filesystem path durable:

| column | type/constraint | meaning |
| --- | --- | --- |
| `operation_id` | `TEXT PRIMARY KEY NOT NULL` | UUID operation identity. |
| `operation_kind` | `TEXT NOT NULL CHECK (operation_kind IN ('archive_register','suppression_activate','delete','restore','override','conflict_resolve','reclaim'))` | Explicit operation. Planning is read-only and is not recorded here. |
| `archive_id` | nullable FK to `compaction_archives` with `ON DELETE RESTRICT` | Related archive. |
| `request_digest` | `TEXT NOT NULL CHECK (length(request_digest) = 64 AND request_digest NOT GLOB '*[^0-9a-f]*')` | Idempotency key for the exact request/plan. |
| `scope_kind` | `TEXT NOT NULL CHECK (scope_kind IN ('none','whole','selective'))` | Restore scope for status derivation; all other operations use `none`. |
| `phase` | `TEXT NOT NULL CHECK (phase IN ('started','staged','published','committed','failed'))` | Last durable boundary. |
| `attempt_count` | `INTEGER NOT NULL CHECK (attempt_count >= 1)` | Bounded retry accounting. |
| `last_error_code` | `TEXT CHECK (last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 128)` | Stable, non-sensitive failure class. |
| `created_at_ms`, `updated_at_ms` | `INTEGER NOT NULL` | Audit times. |

Use the expression unique index
`CREATE UNIQUE INDEX compaction_operations_idempotency ON compaction_operations
(operation_kind, COALESCE(archive_id, ''), request_digest)` (the `COALESCE`
prevents SQLite's NULL-unique exception for archive-independent reclaim
operations) and index `compaction_operations_recovery(phase, updated_at_ms)`.
A failed operation is retryable only with the same request digest; a changed
plan is a new request and must pass all validations again.

### `compaction_operation_objects`

This durable object set is required for restore. It contains no staging or
final filesystem path.

| column | type/constraint | meaning |
| --- | --- | --- |
| `operation_id` | `TEXT NOT NULL REFERENCES compaction_operations(operation_id) ON DELETE RESTRICT` | Restore operation. |
| `blob_hash` | `TEXT NOT NULL CHECK (length(blob_hash) = 64 AND blob_hash NOT GLOB '*[^0-9a-f]*')` | Final content-addressed object name. |
| `content_key` | `TEXT NOT NULL CHECK (length(content_key) = 64 AND content_key NOT GLOB '*[^0-9a-f]*' AND content_key = blob_hash)` | Expected object content digest. |
| `byte_size` | `INTEGER NOT NULL CHECK (byte_size >= 0)` | Expected byte count. |
| `object_state` | `TEXT NOT NULL CHECK (object_state IN ('staged','published','verified'))` | Durable object protocol state. |
| `updated_at_ms` | `INTEGER NOT NULL` | State transition time. |

Primary key: `(operation_id, blob_hash)`. Add
`compaction_operation_objects_state(operation_id, object_state)` and
`compaction_operation_objects_hash(blob_hash, object_state)`. Before the
SQLite restore transaction begins, every row must be `verified`; no database
row may commit while a required object is missing or only `staged`.

## 7. Deterministic state transitions

The following transitions are the only automatic transitions. Each named
operation is atomic and idempotent **within its own boundary**; registration
and suppression are intentionally separate operations, not one contradictory
transaction.

| From | Event | To and effects |
| --- | --- | --- |
| no archive row | verified selective bundle + exact plan/root/membership/deletion evidence | One `archive_register` transaction inserts the archive, roots, members, deletion members, and committed operation. It never changes suppression facts, hot data, FTS, or maps. |
| `verified` archive | explicit #284 suppression activation | A separate `suppression_activate` transaction inserts one active association per applicable source fact and recomputes effective facts. If it fails, the archive remains `verified` and no partial guard is visible. |
| effective fact absent | first association for `(identity_key, content_key)` | Insert the effective fact and archive association together; the fact becomes `active`. |
| effective fact present | another archive supports the same pair | Insert another `active` association and recompute the one effective fact as `active`; duplicate archive/request is a no-op. |
| `suppression_active` archive | #285 validates exact deletion-set digest and current content | A separate delete transaction removes only rows named by `compaction_deletion_members`, maintains FTS/maps, then marks those members `compacted/absent`. #285 rejects the whole commit if current or authenticated child status/time evidence conflicts with owned-child deletion authorization. Dependencies, retained children and their supporting content, and shared resources remain `present`; no exclusive fallback is permitted. |
| any non-conflict archive | explicit whole/selective restore succeeds | A restore operation first completes the object protocol below, then atomically changes only requested member rows and that archive's associations to `restored`; effective facts are recomputed from all associations. |
| `active` association | exact successful explicit restore | That association becomes `restored`; another archive's active association is untouched, so the effective fact remains `active` when one exists. |
| `active` association | explicit override with reason | That association becomes `overridden`; effective state remains `active` if any other archive association is active. |
| any identity | same identity, different content without explicit resolution | Create/update a conflict fact and stop automatic import; no canonical row or association is overwritten. |
| `conflict` fact/association | explicit conflict resolution | Resolve only the named content/identity with an audited `conflict_resolve` operation; recompute precedence and leave unrelated archive associations unchanged. |
| any state | new compaction of the same source | Register a new archive association; never mutate or replace the prior archive's association. This is the recompaction rule. |
| any state | duplicate request with same digest and already committed effects | Return the recorded result; do not repeat side effects. |

The ledger never infers a transition from a file name, a source path, a base
table rowid, an FTS rowid, or a timestamp race. A conflict is safe metadata
only: diagnostics must not contain transcript bodies or paths.

### Effective suppression precedence

For one identity, importer decisions are deterministic and independent of
archive insertion order. Inspect all facts for the identity in lexical
`content_key` order, then apply this precedence:

1. Any `conflict` fact or unresolved conflict association means **conflict**
   for the observed identity/content and blocks automatic import.
2. For an exact `(identity_key, content_key)`, any `active` association means
   **suppress**, even if other associations for that pair are `restored` or
   `overridden`.
3. With no active or conflict association for the exact pair, any
   `restored`/`overridden` association means **allow-with-audit**. Existing
   identical canonical rows still make an explicit import idempotent.
4. No fact means **allow**.

When a new content key is observed for an identity that has an active fact for
another content key, create a conflict rather than selecting a winner. A
restore, override, or recompaction changes only the named association; the
effective fact is recomputed in the same transaction from the complete
association set. This is the required per-identity precedence and prevents
restoring one archive from disabling another active guard.

## 8. Archive creation, registration, and crash recovery

The filesystem protocol follows v1: private sibling staging directory, bounded
stream/object copying, complete verification before publication, `COMPLETE`
written last, directory/file `fsync`, and exclusive rename. Selective creation
does not modify the hot store, ledger, FTS tables, maps, WAL, or `user_version`.

Registration and suppression are separate atomic/idempotent operations:

* `archive_register` commits the verified archive, root dispositions, archive
  membership, deletion membership, and its operation row. It does not create
  suppression associations.
* `suppression_activate` runs only against a committed verified archive. It
  commits all applicable archive associations and effective-fact
  recomputations together. A failure leaves no association from that request;
  the archive remains verified and can be retried with the same request digest.
* Delete and restore each have their own SQLite transaction after their
  filesystem/validation preconditions. No caller is allowed to treat a
  published archive as suppressed until the second operation commits.

There are five meaningful crash boundaries:

1. **Before publication:** remove staging on handled failure. A process crash
   may leave sensitive unpublished staging; it is not a usable archive and
   cannot suppress or delete anything.
2. **After rename, before ledger commit:** the bundle is published but has no
   trusted ledger registration. On retry, verify the exact family/version and
   all digests, then register idempotently. Never infer registration from a
   directory name and never activate suppression from an unregistered bundle.
3. **During archive registration:** archive row, roots, members, deletion
   members, and operation phase commit together. SQLite rollback leaves the
   prior state; retry with the same digest is safe. Suppression is still off.
4. **During suppression activation:** facts, archive associations, effective
   fact recomputation, and operation phase commit together. SQLite rollback
   leaves the archive verified and the prior effective guards unchanged.
5. **After either ledger operation commits:** the archive/guard state is
   authoritative even if a caller
   loses its response. A repeat returns the committed result. A parent fsync
   error after publication is reported without overwriting the existing target.

An archive with the same family/version/manifest digest is the same publication
for registration purposes. A different manifest, even with the same plan, is
not silently substituted. Archive paths are operator inputs and are not stored
in the ledger.

## 9. Rehydration (whole and selective)

Restore is explicit; source discovery never calls it implicitly.

* **Whole restore** selects every member in one archive.
* **Selective restore** accepts a set of member session IDs. The implementation
  takes the archive's authenticated closure for those sessions, including all
  required history records, edges, runs, events, summaries, files, tags,
  links, artifacts, capture sources, and objects. It never recomputes a smaller
  closure from table order. A requested session not present in the archive is
  rejected unless the exact dependency is already hot and verifiable.
* Both modes validate family/version, completion marker, all stream/object
  checksums, membership/closure evidence, natural keys, references, and
  conflicts before mutating hot data.
* Restore first verifies the complete archive and stages every required object
  into a private sibling directory with bounded buffers. It hashes and sizes
  each staged file before inserting a durable `restore` operation and its
  `compaction_operation_objects` set. The durable set is the complete object
  authorization for that restore request; it contains no path.
* It then publishes each object at its final hash-addressed location
  idempotently. An existing file is reusable only after hash and size
  verification; a mismatch is a conflict. New files are written privately,
  fsynced, atomically renamed into the hash-addressed object location, and the
  containing object directory is fsynced. The operation-object rows become
  `published` and then `verified` only after a fresh final-path hash/size check.
* Only after every required operation-object row is `verified` may the SQLite
  restore transaction begin. That transaction performs all base-row
  insert/no-op checks, member states, archive-association transitions, and
  FTS/map maintenance atomically. A committed database row therefore never
  references a missing blob. A non-failed restore operation pins its verified
  object set against #287 reclaim until the restore transaction commits or the
  operation is explicitly marked failed.
* A crash before the durable object set leaves only expected staging residue.
  A crash after durable set creation, during publication, after final object
  publication, or after object verification is resumed by rechecking hashes
  and advancing the same operation rows. A crash after final publication but
  before SQLite commit may leave an unreferenced final object; it is safe and
  reusable after verification, never treated as a restore success by itself,
  and is not deleted opportunistically. A database rollback leaves the old hot
  set and ledger states.
* IDs, `events.seq`, native stream ordering, citations, metadata, provenance,
  and object bytes are preserved. `artifacts.blob_path` is recomputed from the
  hash as in v1; no other canonical value is rewritten. Citation targets are
  not remapped.
* Same ID plus byte-identical canonical content is an idempotent no-op. Same
  ID with different content is a safe conflict and does not overwrite. A
  different ID that violates a natural key (for example `events.seq`, tag
  name, or an artifact `(blob_hash, kind)`) also fails closed. Shared hot
  dependencies are reused, never duplicated. There is no partial restore.
* An archived `deleted_at_ms` value is content and is preserved; restore does
  not apply an incoming tombstone as a replicated delete. An explicit local
  policy may request a separate reactivation, but it is not part of archive
  compatibility.

### Worked example: selective restore

Archive `X` contains sessions `A` and `B`, their shared artifact `a`, and the
session edge `A -> B`. After `A` is compacted, an operator explicitly requests
restore of `A`. The authenticated closure requires `B`, `a`, the edge, and
their referenced objects, so the restore either installs that complete set or
does nothing. If `B` is already hot with identical content, it is a no-op and
is not duplicated. IDs, `events.seq`, and summary citation IDs remain exactly
as they were in `X`. A conflicting row for `B` aborts the entire restore before
any row or suppression state changes.

## 10. #189 origin/device/bundle compatibility

Every selective manifest has a globally unique `bundle_id` equal to its
`archive_id`. It may carry an opaque `origin_device_id` when the source has
exactly one known stable device identity, using the same no-guess rule as v1:
zero or multiple candidates means omission. No `local_devices` or
`local_workspaces` row is copied into a hot store.

This is forward-compatible audit input, not a #189 transport or merge
implementation:

* Current code records opaque `bundle_id`/`archive_id`, optional
  `origin_device_id`, stable IDs, canonical content digests, and bounded
  conflict inputs. It does not define future event identity, event ordering,
  per-member provenance, or merge semantics; #189 owns those decisions.
* Same stable identity and same canonical content is locally idempotent across
  bundles. Same identity with different bytes is a conflict containing only
  bounded IDs/digests/origin markers; neither side overwrites the other.
* Natural-key conflicts are also conflicts even if UUIDs differ. There is no
  last-writer-wins rule and no path-based tie-break. Which event wins, how
  event order is reconciled, and how provenance is attributed remain #189
  decisions.
* Archived `deleted_at_ms` is not a replicated delete command. Compaction
  deletes only local hot rows after its own archive-backed transaction; no
  delete tombstones are broadcast or applied to another origin. Future #189
  must define replicated deletion separately.
* Missing or ambiguous origin metadata never gets guessed from hostname, path,
  or current local device. It remains absent and the future merge path must
  fail closed when origin is required.

### Worked example: conflicting origin

Bundle `B1` from origin `D1` contains entity UUID `u` with canonical digest
`C1`. A future local import sees bundle `B2` from `D2` containing the same `u`
with digest `C2`. The ledger keeps both bundle identities and records a
conflict on `(u, C1, C2, D1, D2)`; it does not overwrite `u`, delete either
version, or choose based on path/time. An operator or the future #189 merge
contract must resolve it explicitly.

## 11. Logical deletion versus physical reclaim

Logical compaction is the archive-backed transactional removal of selected
canonical hot rows and their derived search rows. It is not a tombstone in
`deleted_at_ms`, not a replicated delete, and not a promise about file size.
Shared rows and objects remain. Content-addressed object files that become
unreferenced may be marked reclaimable by the ledger, but logical compaction
does not unlink them.

SQLite pages freed by logical deletion remain in the database/freelist and WAL
until an explicit #287 reclaim operation. #287 is optional, user-requested,
must measure temporary space and concurrency, and must validate any replacement
before publication. It may not change membership, suppression, restore
semantics, IDs, ordering, citations, or archive bytes. Status, doctor, search,
import, setup, and planning never checkpoint, vacuum, rewrite, unlink, or
auto-delete on low disk space.

## 12. Read-only diagnostics and privacy (#199)

The planner may use the bounded, read-only storage diagnostics defined by #199
to report logical bytes, live/primary bytes, FTS-derived bytes, freelist/
reclaimable bytes, available space, expected archive bytes, and temporary-space
requirements. It must not checkpoint, vacuum, optimize, migrate, write a
ledger row, refresh FTS, or alter WAL/SHM while producing those values.

Diagnostics are conservative when `dbstat`, free-space, or filesystem
measurements are unavailable: report an unknown/bounded value or refuse a
destructive next step; never report zero as a guess. Share-oriented output
contains only bounded counts, byte totals, opaque IDs/digests, state, and safe
error codes. It omits transcript bodies, embedded JSON, credentials, source
paths, and filesystem locations. Raw SQL remains strictly read-only with the
existing row/column/value/SQL-byte/timeout caps.

Archives, staging, verifier scratch, databases, object files, and ledger data
are secrets. Use `0700` directories and `0600` files on Unix; these modes are a
floor, not a sharing mechanism. Tests use private temporary data roots and
homes, never the real home, and no network. No implementation may introduce a
network client, upload, peer discovery, or remote credential path.

## 13. FTS and rowid-map invariants

The manually maintained projections in the current store remain load-bearing:

* `ctx_history_search` and `event_search` are the only active searchable
  projections for records/events; `artifact_search` remains unused where the
  current restore contract says so. FTS shadow tables are never archived or
  copied.
* `record_search_rowids(record_id, search_rowid)` and
  `event_search_rowids(event_id, search_rowid)` are explicit caches of the
  SQLite-assigned FTS rowids. They are never derived from base-table IDs,
  insertion counts, or stream position, and search correctness never depends
  on them.
* Every insert/update/delete touching a base row and its projection occurs in
  one write transaction. For a delete, verify the mapped row's FTS ID first;
  use its stored SQLite rowid for a point delete only when it matches, otherwise
  use the legacy full-scan delete and heal the map. Remove the map entry in the
  same transaction.
* A restore may use verbatim base inserts followed by a full rebuild, but the
  rebuild must clear and repopulate each projection and its map in lockstep in
  one transaction. A rebuild failure rolls back to the prior complete state.
* Planning, archive creation/verification, ledger registration, suppression,
  and schema migration do not rewrite FTS or either map. #285 deletion and
  #286 restore must test stale, missing, duplicate, and unmapped entries; they
  may degrade to the documented full-scan healing path but may never guess a
  rowid.

## 14. Schema version and migration ordering

Do **not** unconditionally reserve v1003. #290 may land another fork
migration. At implementation time, inspect the reviewed fork chain and assign
the compaction ledger migration the next available fork version: **v1003 if no
earlier migration than compaction has landed; otherwise the next sequential
version** (for example v1004). This decision record intentionally contains no
hard-coded reservation.

The chosen migration must:

1. begin `BEGIN IMMEDIATE` after the existing fail-closed version probe;
2. create the eight ledger tables and all required indexes with **unconditional
   DDL** from the recognized predecessor, in dependency order
   (`compaction_archives`, roots, members, deletion members, suppression facts,
   archive associations, operations, operation objects, then indexes). Do not
   use `IF NOT EXISTS`: a pre-existing table, index, or incompatible object is
   a collision and must fail the migration rather than being silently accepted;
3. create no archive rows and perform no guessed path backfill. Existing
   `catalog_sessions.source_path`, `source_import_files.source_path`,
   `raw_source_path`, `cwd`, and `root_path` values must not be transformed
   into suppression keys;
4. leave all base rows, canonical indexes, FTS projections, FTS shadow tables,
   `record_search_rowids`, and `event_search_rowids` untouched; there is no FTS
   rebuild, map rebuild, or backfill during migration;
5. set `PRAGMA user_version` to the chosen version last and commit the DDL and
   version atomically; and
6. be safe to retry after a crash. A pre-commit crash rolls back all DDL and
   the version, so the next open repeats the unconditional migration from the
   same recognized predecessor. A post-commit retry is not a migration retry:
   the version gate advances to the next sequential step. Migration failure
   leaves the prior version usable or fails closed without claiming success.

Writable opens must apply reviewed migrations sequentially: v1–v15, then each
landed fork version from v1000 through the current version, including #290's
version if it precedes compaction. A version in the unreviewed gap `(15,
1000)`, a skipped/unrecognized fork version, or a version above the current
binary is rejected without mutation. Read-only opens require **exactly the
current chosen version**; they never silently migrate. A v1002 store therefore
requires one explicit writable open through all prior reviewed migrations,
including #290 if it landed first, before read-only status/doctor/sql/MCP use.
A process that already held a connection before upgrade is not evicted by the
gate; long-lived processes must be restarted.

Migration tests use a parameter `compaction_schema_version` resolved from the
actual reviewed chain at implementation time. They cover v1002 when compaction
is the next migration, or the immediate recognized predecessor when #290 (or
another migration) has already consumed the next number; they never assert a
hard-coded v1003. Tests also create each ledger object name at the predecessor
version to prove that unconditional DDL fails, rolls back, and leaves
`user_version` unchanged. A clean predecessor, crash rollback, successful
sequential open, exact-version read-only gate, and unsupported-version
no-mutation case are all required.

## 15. Acceptance criteria frozen for the child tickets

These are implementation gates, not future design questions.

### #282 — deterministic read-only planner

* Given an explicit cutoff, report selected root IDs, inclusive-boundary and
  ambiguous decisions, directional dispositions by all fifteen stream kinds
  and objects, exclusive/shared classifications, logical bytes, expected
  selective-archive bytes, reclaimable estimates, and bounded temporary-space
  requirements in stable ID/key order.
* Emit authenticated selected-root/dependency disposition, per-root closure
  digests, union membership, deletion-set membership, and a deletion-set
  digest. Prove that active/post-cutoff/ambiguous session dependencies and
  everything needed to keep them complete never enter deletion authorization,
  even when exclusive by current reference count.
* Prove completed-session, equal-cutoff, incomplete/ambiguous,
  parent/child, shared dependency, shared-workspace no-fan-out,
  cross-boundary-edge, duplicate-reference, cycle, dangling reference, and
  empty-selection behavior. Include explicit fixtures for an old completed
  parent pointing separately to an active child, a completed post-cutoff child,
  and an ambiguous completed child with NULL end time; each child has a retained
  disposition, no deletion membership, and complete protected supporting
  content. Repeated plans have identical digests and output.
* Read-only tests prove unchanged `user_version`, ledger/base rows, FTS/maps,
  WAL/SHM, and no import/checkpoint/vacuum/migration side effect. Unsupported
  schema versions fail closed.

### #283 — verified selective archive

* Create exactly `ctx-selective-archive` version 1 from one #282 plan, with
  all fifteen streams, exact root/dependency dispositions, closure and
  deletion-set evidence, stable IDs/order/citations, shared objects once,
  per-stream/object checksums, and complete v1-style private atomic
  publication. Archive membership must not be interpreted as deletion
  authorization.
* Reject unsupported family/version, incomplete/truncated/corrupt archives,
  duplicate/conflicting IDs, missing/unreferenced/mismatched objects,
  symlinks/special files/path traversal, and unreasonable size declarations.
  Verify before registration; hot data, FTS/maps, and `user_version` remain
  unchanged. Repeat/crash recovery is idempotent.

### #284 — durable path-independent suppression

* Apply the exact identity/content keys and state transitions above at every
  relevant normal import/setup/search-refresh path. Moved paths with the same
  keys are skipped; changed content conflicts; unrelated content remains
  importable; explicit override and restore are distinguishable and audited.
* Keep effective suppression facts separate from per-archive associations.
  Test identical `(identity,content)` supported by two archives, restoring or
  overriding one while the other remains active, deterministic per-identity
  precedence, recompaction, conflict resolution, duplicate discovery, process
  restart, and interruption before/after each separate registration and
  suppression transaction. No path backfill and no FTS/map mutation is
  allowed.

### #285 — archive-backed logical deletion

* Revalidate archive identity, root/closure digest, deletion-set digest,
  current content, references, and directional dispositions at commit. One
  transaction removes only rows explicitly named by the deletion membership
  and their FTS rows/maps; an injected failure leaves the old state. Reject the
  commit when archived or current status/end-time/cutoff evidence conflicts
  with deletion authorization for any child session.
* After success, deleted content is absent from normal search/views/show/
  locate, active/post-cutoff/ambiguous dependencies remain complete, shared
  resources remain, ledger membership and effective suppression facts agree,
  and retry is a no-op. Fixtures must include a dependency that is exclusive
  by reference count but forbidden by disposition, plus an old completed parent
  pointing separately to active, completed post-cutoff, and ambiguous children;
  all three children and all supporting content remain hot and outside deletion
  membership. No low-space auto-delete, checkpoint, vacuum, object unlink, or
  physical reclaim is performed.

### #286 — explicit whole/selective rehydration

* Validate the exact selective archive and requested authenticated closure
  before mutation. Restore all-or-nothing with bounded object staging, stable
  IDs/seq/order/citations/metadata/provenance, same-ID idempotence, and
  fail-closed same-ID/natural-key conflicts. Shared hot dependencies are not
  duplicated.
* Require the verify/stage → durable operation/object-set → publish and fsync
  hash-addressed objects → SQLite commit protocol. Inject crashes before and
  after each boundary, including after final object publication but before DB
  commit; prove orphan final objects are hash-verified, reusable, and never
  evidence of committed missing content.
* Only successfully restored members transition that archive's associations
  to `restored`; effective facts remain active when another archive supports
  the same pair. Test fresh-root and post-#285 restore, crash/retry, moved
  sources, origin markers, private permissions, #199 diagnostics, no real
  home/network, and FTS/map parity using assigned rowids—not inferred rowids.

### #287 — optional physical reclaim

* Be an explicit, separately reported operation with before/after logical,
  live, FTS-derived, freelist/reclaimable, WAL/SHM/object/spool and temporary
  space measurements. Skip/refuse conservatively for low space, unsafe
  permissions, unsupported schema, active/concurrent use, or invalid output.
* Reopen and validate any replacement before publication; preserve the current
  base schema plus the dynamically chosen compaction ledger version, all
  archive/member/fact/association state, hot content, search behavior, and
  FTS/map parity. Failure leaves the original usable.
  Never auto-trigger from status/doctor/search/import/setup or disk pressure.

## 16. Decision summary

The contract is archive-first, exact, path-independent, and fail-closed. The
retention boundary is an inclusive `ended_at_ms` cutoff over the explicit
`completed` + non-NULL-end signal. Directional closure includes all fifteen
canonical streams, shared dependencies, boundary edges, and referenced
objects, while archive membership remains separate from the authenticated
deletion set. Selective archives have their own exact family/version. Archive
registration and suppression activation are separate atomic operations;
effective suppression facts are separate from per-archive associations;
restore is explicit and atomic only after verified, durable, fsynced objects;
conflicts never overwrite; deletes are local and not replicated; reclaim is
optional. Read-only diagnostics remain read-only, privacy modes remain
`0700`/`0600`, raw SQL remains read-only, and every FTS/map change obeys the
same-transaction SQLite-assigned-rowid invariant. The ledger migration takes
the next available fork version at implementation time, uses collision-failing
unconditional DDL, and performs no FTS/map rewrite.
