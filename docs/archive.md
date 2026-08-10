# Portable Archive Workflows

`ctx archive` plans selective compaction and creates, checks, and restores a portable logical archive of the
current local data root. The archive is a private directory bundle, not a copy
of SQLite. It is useful for moving indexed history to a fresh data root or for
keeping a logical recovery copy; it is not a byte-for-byte SQLite disaster
snapshot.

Inspect a stable read-only compaction plan with an explicit inclusive cutoff:

```bash
CTX_DATA_ROOT="$source_root" ctx archive plan --cutoff-ms 1700000000000 --json
```

Planning opens only an existing current-schema store and performs no discovery,
import, migration, checkpoint, vacuum, or write. Its private output omits paths
and history content. Archive membership and the smaller authenticated deletion
set are distinct; this planner creates no selective archive and deletes or
physically reclaims nothing.

The normative container and stream contract is [Archive Format
v1](archive-format-v1.md). This page is the user workflow; it intentionally
does not duplicate that specification.

## Safety and privacy

Archives contain verbatim indexed history and referenced artifact bytes. Treat a
bundle like `work.sqlite`: it may contain prompts, code, commands, paths, and
credentials. v1 bundles are uncompressed, and compression is not encryption.
The commands provide integrity checks, not confidentiality, signing, or encryption. If
you need those properties, use a reviewed local tool such as `age` outside ctx.

Create, verify, and restore are local-only operations. They do not upload,
discover, or contact a remote service. On Unix, bundle and restored-root
directories are private (`0700`) and files are private (`0600`); these modes
are a floor, not a safe-sharing mechanism.

Use a fresh, local, trusted temporary parent on macOS or Linux. The examples
below use `mktemp -d`, and the archive implementation also handles the
OS-managed sticky `/tmp` roots on those systems. Do not place a bundle or
restore staging path in an attacker-controlled directory. A plain copy is
preferable to a hard-link-deduplicated backup view because v1 verification
rejects hard links and special files.

## Create and verify

The archive destination must not exist. This is true even for an empty
directory; create never merges into or replaces an existing path.

```bash
work="$(mktemp -d "${TMPDIR:-/tmp}/ctx-archive.XXXXXX")"
source_root="$work/source-root"
bundle="$work/history.ctxar"

# Creates source_root and an empty, schema-current store for this example.
CTX_DATA_ROOT="$source_root" ctx setup --catalog-only

CTX_DATA_ROOT="$source_root" ctx archive create "$bundle"
CTX_DATA_ROOT="$source_root" ctx archive verify "$bundle"
```

`create` streams the 15 canonical entity streams and the referenced object
bytes, writes the manifest and SHA-256 checksums, verifies the staged result,
and publishes it atomically. It writes `COMPLETE` last. Its staging sibling is
named like `history.ctxar.tmp-<id>`. A normal pre-publication failure removes
staging, but an abrupt process or machine crash can leave that directory
unpublished and potentially complete, including `COMPLETE`; verify and restore
refuse staging names even when their contents look complete or usable. Treat
the residue as sensitive and never upload or share it.

The streams are, in order: `capture_sources`, `vcs_workspaces`,
`history_records`, `artifacts`, `sessions`, `session_edges`, `runs`, `events`,
`vcs_changes`, `summaries`, `files_touched`, `tags`, `history_record_tags`,
`history_record_links`, and `record_edges`. The manifest records each stream's
count, byte size, and SHA-256 digest; `COMPLETE` records the manifest digest and
is the final publication marker. Referenced artifact objects are embedded once
under their content-addressed SHA-256 names.

`verify` is read-only with respect to the published bundle and must succeed
before restore. It checks the exact v1 layout, completion marker, manifest/stream sizes and digests, JSONL shape and
ordering, IDs and references, artifact/object bijection, permissions, and
bounded resource declarations. It uses a private sibling scratch database at
`.ctxar-verify-<id>/state.sqlite` beside the bundle, normally removing it when
verification ends; the bundle contents are not modified. Verification proves
structural integrity and consistency; SHA-256 is not an authenticity guarantee
against someone who can rewrite both the bundle and its manifest.

Both commands print a concise human result by default. With `--json`, success
is one JSON object on stdout. For `verify`, a verifier rejection is one typed
`{"error":{"code":...}}` object on stderr, no stdout, and exit status `1`;
the stable `error.code` is the machine contract. Usage errors use the normal
CLI diagnostic and exit status `2`; other filesystem/store failures are normal
runtime errors rather than verifier envelopes. Verification limits can be
lowered with `--max-entities`, `--max-objects`, `--max-object-bytes`, and
`--max-bytes`.

## Restore into a fresh root

Restore is strict about its destination: the target path must be absent, not
merely empty. It does not merge, overwrite, or encrypt a store.

```bash
restored_root="$work/restored-root"  # do not mkdir this path first

CTX_DATA_ROOT="$source_root" ctx archive restore "$bundle" "$restored_root"
CTX_DATA_ROOT="$restored_root" ctx status
CTX_DATA_ROOT="$restored_root" ctx search "a term from the archive" --refresh off
```

Restore verifies the complete bundle first, installs it in a private sibling
staging root named `<target>.tmp-<id>`, and publishes the new data root only
after the transaction and post-restore checks pass. An abrupt crash can leave
that staging directory unpublished and potentially complete; a verifier crash
can likewise leave `.ctxar-verify-<id>/state.sqlite`, which may contain a
complete or partial scratch database. Every such residue is sensitive. Do not
open an unknown root or point ctx at a staging/scratch path. For an expected
residue, first confirm no create, verify, or restore is running, then inspect
only its parent-level owner/mode/timestamp metadata if needed and remove the
whole expected directory; preserve unexpected paths for operator review.
The bundle and target must not contain one another.

The exclusive rename into the requested target is the publication point. Before
that rename, a handled failure leaves the target absent and cleans staging.
After the rename, the target exists; a parent-directory `fsync` can still fail
and cause an error report even though the complete target was published. If a
command reports an error near publication, do not retry to overwrite the target:
inspect the target and verify the archive (and, for restore, inspect the new
root) before taking further action.

The 15 streams preserve canonical content, stable IDs and ordering, including
capture sources, VCS workspaces, history records, artifacts, sessions and
edges, runs, events, VCS changes, summaries, touched files, tags and tag
assignments, history-record links, and record edges. Artifact rows reference
content-addressed object files, and those referenced bytes are embedded in the
bundle. Restore rebuilds the active record and event FTS projections and the
`record_search_rowids`/`event_search_rowids` maps for the destination. The
`artifact_search` projection is cleared and intentionally remains empty and
unused; it is not repopulated by restore. Machine-local discovery/import ledgers,
sync/runtime/device state, configuration, logs, WAL/SHM files, and other
operational tables are intentionally excluded; see the format specification.

With `--json`, successful restore emits one JSON object on stdout containing
the archive identity, restored path, entity/object counts, and source schema
diagnostic. A verifier rejection has the same typed stderr envelope and exit
status `1` as `verify`. Destination and restore failures, including an existing
target, remain ordinary errors even when `--json` is supplied; they are not
reported as successful or as verifier rejection envelopes.

The archive `format_version` is independent of SQLite `user_version`. Restore
uses the current binary's normal store-creation and migration path, so a
writer's source schema diagnostic does not select the destination schema.
The v1 reader accepts exactly `format_version: 1` and rejects unsupported or
future archive versions rather than partially restoring them.
After a binary or schema upgrade, restart long-lived processes such as
`ctx mcp` before using the upgraded store or a restored root.

This is a portable logical re-import surface, not a merge facility, an
overwrite facility, an encrypted backup, or an exact SQLite snapshot.
