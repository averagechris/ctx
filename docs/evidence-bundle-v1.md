# SourceHut #200: bounded evidence bundle v1

Status: design decision only. This document does not add an exporter, renderer,
CLI flag, MCP tool, or file writer. It is the implementation contract for the
smallest safe first slice of #200.

## Decision

Choose the **selector-specific pager** decomposition:

1. `session_page`: one ctx session and one transcript mode (`lite`, `full`, or
   `log`), using the existing #195 session page and `(seq, ctx_event_id)`
   keyset cursor;
2. `search_page`: one #195 search request, using the existing bounded candidate
   pool and search continuation; or
3. `event_ids`: a finite, explicitly supplied set of ctx event IDs, using a
   small selector-specific cursor over the canonical sorted ID set.

An invocation has **exactly one selector domain**. It produces one page in that
domain, not a union of sessions, search hits, and explicitly selected events.
The common bundle envelope, record projections, byte policy, error vocabulary,
and output safety rules are shared. The continuation token kind remains
domain-specific.

This is the smaller safe v1 than a union cursor. #195 already binds the full
session and search requests, snapshot, query revisions, filters, and byte
policy, and already emits replayable `next_argv`/`next_arguments`. A union
cursor would have to define cross-domain ordering, count meaning, selector
mutation rules, and three different resume keys before it could export one
more useful record. Reuse the #195 query execution, request hashing, and
opaque-token machinery for the first two domains; use the same token envelope
with `kind: "event_ids"` for the third. Reuse does **not** mean exposing #195
full DTOs: every result crosses the evidence-safe projection boundary below
before rendering. Do not mint a token that can change domain during
continuation.

The bundle is a **private local artifact**, even when its fields are called
“safe”. `private: true` and `share_safe: false` are mandatory. “Safe Markdown”
means structurally safe and deterministic to render, not redacted or suitable
for publication.

## Selector contract

The canonical request contains one and only one of the following top-level
selectors. The implementation must reject requests that contain more than one,
or none, before reading any payload.

| Domain | Required selector | Reused query behavior | v1 page limit |
| --- | --- | --- | ---: |
| `session_page` | canonical `ctx_session_id` | `show session`; mode is applied before the limit | 1–1000 |
| `search_page` | a non-empty query, term, or touched-file selector | `search --refresh off`; session/event result mode is retained | 1–200 |
| `event_ids` | 1–256 complete canonical ctx event IDs | direct live-event lookup; no search ranking | 1–200 |

ID prefixes are useful at an interactive lookup boundary, but are not accepted
inside an evidence request. The exporter must resolve a prefix first and put
the resulting full lower-case UUID in the replayable arguments. Event IDs must
be unique; duplicate IDs are an input error rather than an implicit change of
the requested evidence set. The finite list is a set for selection and is
ordered canonically for output, so caller argument order cannot change the
artifact.

The common options are `limit`, `fields` (`full` or `compact`), per-item byte
cap, page byte cap, output format, and optional output path. `--out` (or its
equivalent) is never included in continuation arguments. Defaults and caps
reuse #195: per-item bytes 4096/1048576, page bytes 262144/16777216, with
whole-record admission and UTF-8-safe truncation. V1 also has
`artifact_bytes`, default 1048576 and maximum 16777216; it bounds the complete
serialized artifact, not only item projections.

### Supported filters

`search_page` supports the current #195 filters without changing their
meaning: provider, custom history-source identity, workspace, `since`, event
type, include/exclude role, tool-noise and tool-name exclusion, file path,
session, primary-only compatibility scope, session/event result mode,
subagent inclusion, and current-session inclusion. Search input remains literal
ctx token input, not raw FTS syntax.

Evidence search is independent of ambient `CODEX_THREAD_ID`. Its canonical
effective request always sets `include_current_session: true`, meaning the
active session tree is included when the environment identifies one and no
implicit exclusion is applied when it does not. This is the safer deterministic
rule for a private, explicitly requested artifact: the same arguments do not
silently change because the exporter was launched by a different agent
process. The exporter does not read `CODEX_THREAD_ID` to add an exclusion.
`since` is evaluated against the indexed event/session timestamp as it is in
#195. A relative value is frozen to an absolute RFC3339 UTC timestamp before
the first page is executed.

The custom-source rules are also inherited: `history_source` is either
`plugin/source` or `provider_key/source_id`; exact `provider_key`, `source_id`,
and `source_format` filters imply the custom provider; a custom-source filter
cannot be combined with a different provider. The canonical request preserves
the ordered repeated term vector, including duplicates, rather than only its
normalized form.

`session_page` has no independent provider, workspace, source, time, role, or
event-type filters in v1. Its session ID and transcript mode are the complete
selection. If a filtered transcript is needed, use `search_page` with event
results and an applicable filter.

`event_ids` has no independent filters in v1. Provider, source, workspace,
time, role, and event-type restrictions cannot be combined with it. This is
intentional: applying a second predicate after an explicit set would make a
missing result indistinguishable from a rejected or deleted target. Select the
events with `search_page` instead, or provide the already-filtered IDs.

The following combinations are errors, not silently ignored options:

- any two selector domains in one request;
- search-only options on `session_page` or `event_ids`;
- `include_deleted` (there is no deleted-data mode in v1);
- a session selector combined with a search `session` filter;
- an empty search intent, an empty ID list, duplicate IDs, malformed IDs, or
  more than 256 explicit IDs;
- a continuation whose domain, request, byte policy, or snapshot does not
  match the current request.

## Live rows and nested capture sources

V1 is **live-only**. Sessions, events, search candidates, and finite ID targets
are selected with `deleted_at IS NULL`. There is no opt-in deleted export.

- A deleted or missing explicit event target is an error (`deleted_target` or
  `missing_target`), not an omitted row and not a withheld-content row.
- A deleted session is not a valid `session_page` target.
- Deleted rows are not counted in any exact count or lower-bound calculation.
- The implementation must apply the same live predicate to every base-table
  query; it must not rely on a current stable view in one path and an
  unfiltered table query in another. In particular, the existing #195 session
  event predicate needs the live-event condition added before it is reused.
  The predicate is required in session counts, session page reads, `lite`
  mode's correlated “next message” lookahead, continuation cursor-position
  validation, explicit-ID lookup/count/order, search candidate generation and
  hydration, and citation target checks. A deleted event must not affect a
  lookahead, count, retained pool, or cursor position merely because it is
  present in the base table.

Capture-source provenance follows the owning row, with the same selection
parity. An event uses its event-level `capture_source_id`; a session uses its
session-level source. If an event source differs from its session source, both
may be represented (`capture_source` and `session_capture_source`) rather than
silently falling back. A missing source never makes a live event or session
disappear, but its nested source object is marked `availability: "missing"`
and contains no fields read from that unavailable row. If a future source
ledger supplies an explicit deletion marker, use `availability: "deleted"`;
the current `capture_sources` table has no `deleted_at` column, so an absent
source row must not be described as deleted merely because its raw file is
gone. Source visibility `withheld` is handled below.
The same source object shape and visibility policy is used for session records,
event records, and search-result provenance. Compact fields omit source
metadata exactly as #195 does; full fields may include it, subject to the
withheld rules below.

This preserves provenance parity without turning a source cleanup into a
transcript deletion and without leaking stale source paths. A source lookup is
bounded by the page's distinct source IDs, never by an unbounded source scan.

## Ordering, counts, and continuation

Every page reports `returned`, `has_more`, `omitted_before`,
`omitted_after`, `omitted_exact`, and a byte summary. Counts describe the
selected domain, not the number of bytes that happened to fit on this page.
Whole records are admitted; a page-byte stop is reported separately and never
causes a non-advancing cursor.

### `session_page`

Selection is exactly the requested transcript mode before paging:

- `log`: all live events;
- `full`: live message events with user, assistant, or system role;
- `lite`: live user messages and the final live assistant message before the
  next user message or end of session.

Canonical order is `(seq ASC, ctx_event_id ASC)`, reusing the #195 keyset
query. `selected_total` is exact over live events selected by the mode. The
page reports exact `omitted_before` and `omitted_after`; the existing
`pagination.offset`, `page_size`, `returned_items`, and `continuation` remain
available for compatibility. A continuation carries the last admitted
`(seq, ctx_event_id)` and its offset, and is rejected if the cursor is not a
selected live event or the offset is inconsistent.

The evidence continuation additionally binds `domain: "session_page"`, output
format, evidence schema revision, and artifact byte cap; a token from ordinary
#195 `show session` is not accepted as an evidence token.

The next arguments contain `domain: "session_page"`, the canonical full
session UUID, mode, limit, fields, per-item/page/**artifact** byte caps,
format, and `continue`. They preserve no output path and are sufficient to
select the same exporter domain without ambient CLI or MCP defaults.

### `search_page`

Canonical result order is the existing query-service result order: ranked
candidate order with the existing deterministic tie-breakers. V1 must not
re-rank results while rendering the bundle. `pool_total` is the exact size of
the fixed candidate pool used by every page, and page-local omitted counts are
exact within that pool.

The candidate pool remains capped at 200. Search-core source/scan truncation
is reported in `search_truncation` with its reason, omitted count, and
`omitted_results_exact`. A scan-budget or other sentinel lower bound is never
presented as an exact corpus count. Consequently:

- no truncation: the pool count is exact for the bounded query result;
- candidate-cap or source truncation: the bundle declares its overall match
  count a lower bound, even when the page itself is an exact slice of the
  retained pool;
- a page-byte stop is independent of candidate/source truncation and has its
  own `page_budget_exhausted` flag.

The existing #195 search token binds the query, ordered/repeated terms, query
plan and match mode, all filters, result mode, page size, fields, byte policy,
query/DTO revisions, and conservative SQLite snapshot. The evidence binding
adds effective `include_current_session: true`, `refresh: "off"`, output
domain/format, artifact byte policy, and evidence schema revision. It remains
opaque and private-content-free. `--refresh off` is mandatory for the first
and every continued page; the exporter never imports or refreshes while
exporting.

Replayable next arguments preserve `domain: "search_page"`, every search
option including repeated terms and filter arrays, the effective
`include_current_session: true`, canonicalize relative `since` to RFC3339,
include the opaque `continue`, and force refresh off. They preserve the
requested output `format` (`jsonl` or `markdown`), artifact byte cap, and
never an output path.

Search counts use distinct names so a retained pool is not confused with the
historical corpus. `retained_pool_total` is always exact: it is the number of
ranked candidates retained for paging (at most 200). `corpus_count` is either
`{ "kind": "exact", "value": N }` when search completed without a source or
candidate cap, or `{ "kind": "lower_bound", "value": N }` when the retained
pool is only a known prefix of the corpus. The lower-bound value is never
described as a total. `page.omitted_before` and `page.omitted_after` remain
exact relative to the retained pool in both cases. Session and explicit-ID
domains have an exact `selected_total`; they do not emit a misleading corpus
count.

### `event_ids`

After live lookup, sort the requested events by `(occurred_at ASC,
ctx_event_id ASC)`. `occurred_at` is always present; the UUID tie-break makes
the order total and portable across sessions. Each record also carries its
session ID and session sequence when available. `selected_total` is the exact
number of live requested events, which equals the unique input count after
successful validation. `omitted_before` and `omitted_after` are exact.

The event-ID continuation uses the existing opaque token envelope and a
distinct `kind: "event_ids"`. Its request binding includes a digest of the
canonical ID list, the ordered live-result count, page/byte policy, schema and
query revisions, artifact byte policy, output domain/format, and snapshot. It
carries only the offset (and optionally the
last `(occurred_at, id)` key); it does not embed event content or the ID list.
The next arguments repeat the canonical full ID list, fields, limits, per-item/
page/**artifact** byte caps, **domain**, **format**, and `continue`, with no
output path. A stale snapshot or changed ID list fails closed rather than
silently exporting a different set.

No cursor can be exchanged between these three domains. The absence of a
union cursor is part of the v1 contract.

## Evidence-safe projection boundary

The query layer and renderer are separated by a new
`EvidenceProjectionV1::normalize` boundary. Query code may use #195 typed
projections and internal search packets to select and page data, but the
renderer must never receive `SessionFullV1`, `EventFullV1`,
`SearchResultFullV1`, arbitrary `serde_json::Value`, raw payload JSON, or
source metadata directly. The sanitizer constructs new evidence-owned types,
validates every field and byte cap, and either returns a complete normalized
record or a fail-closed error. A renderer has no database handle and no access
to the unsanitized input.

The normalized page also carries a closed `EvidenceRequestBindingV1`, the
actual #277 request hash, and the actual opaque snapshot fingerprint captured
while selecting the page. The binding contains the canonical selector,
effective filters and repeated terms, fields, byte policy, format, refresh, and
all work bounds; it contains no raw payload, source object, path, cursor, or
open metadata. These values are part of the manifest and bundle identity even
for a first page that has no continuation token.

The evidence types have no `flatten`, open metadata map, cursor field, raw
payload field, filesystem probe result, or provider-specific extension point in
v1. Additive future fields require a new evidence schema version or an
explicitly bounded extension map with its own cap.

### Exact normalized item fields

All records also carry the common envelope fields from the JSONL contract:
`schema_version`, `record_type`, `private: true`, and `share_safe: false`.
The normalized item shapes are:

- **Session**: `record_id` (equal to canonical `ctx_session_id`),
  `ctx_session_id`, `provider`, `agent_type`, `status`, `is_primary`,
  `started_at`, `ended_at`, `fidelity`, and optional `provenance`.
- **Event**: `record_id` (equal to canonical `ctx_event_id`),
  `ctx_event_id`, nullable `ctx_session_id`, `sequence`, `event_type`,
  nullable `role`, `occurred_at`, `content`, `fidelity`, and optional
  `provenance`, optional `session_provenance`, `citations`, and
  `citation_omissions`. `ctx_session_id` is present even in `compact` mode;
  compact event records must not lose the session join needed to group an
  evidence page.
- **Search result**: `record_id` (equal to the stable `item_id`), `item_id`,
  `result_scope` (`session` or `event`), nullable `ctx_session_id`, nullable
  `ctx_event_id`, nullable `event_seq`, bounded `title`, bounded `content`
  containing the indexed snippet only when accompanied by
  `SearchSnippetWithProofV1`, `rank`, nullable `timestamp`, bounded
  `why_matched` strings, optional `provenance`, optional `citations`, and
  `citation_omissions`. It never carries `links`, suggested shell commands,
  raw query-plan metadata, or arbitrary search-core fields.

`fields: "compact"` retains only stable IDs, event/session identity needed for
joining, provider/agent or event classification, timestamps, bounded content,
content truncation/state, rank where applicable, and bounded match reasons. It
omits provenance, provider-owned IDs, paths, cwd, source format, cursors,
citations, and raw/opaque metadata. `fields: "full"` adds only the explicit
`ProvenanceV1` below and eligible citations; it is still private. There is no
direct #195 full/compact DTO passthrough.

### Content and suppression

`ContentV1` is exactly:

```json
{
  "content_state": "available|truncated|withheld|metadata_only",
  "text": "bounded UTF-8 text or null",
  "truncation": {
    "original_bytes": 0,
    "returned_bytes": 0,
    "truncated": false
  },
  "suppression_reason": "raw_payload|withheld_visibility|unavailable|missing_proof|not_applicable"
}
```

`suppression_reason` is omitted when content is available and is a closed enum
when present. `text` is null for `withheld` and `metadata_only`; the renderer
uses a fixed non-content marker such as `[content withheld]` and never emits a
payload-derived substitute. Raw payloads, payload blobs, provider cursors,
and arbitrary nested payload values are never accepted by the sanitizer.
Indexed search snippets are accepted only through an explicit bounded
`snippet` input from the query service; they are not permission to hydrate a
raw payload. A row with raw/withheld redaction, withheld visibility, or an
unavailable payload becomes `content_state: "withheld"`.

Content has the existing per-item cap (default 4096, maximum 1048576 bytes),
is truncated only at a UTF-8 boundary, and reports original and returned byte
counts. Metadata strings are never silently truncated: they are rejected when
over their field cap.

Search results have an additional input type, `SearchSnippetWithProofV1`; a
bare `String` or a #195 result snippet is not sufficient. The query layer must
attach this bounded proof before the sanitizer can use the text:

```json
{
  "content_origin": "indexed_preview",
  "redaction_state": "safe_preview|redacted|raw|withheld",
  "visibility": "local_only|reportable|sync_metadata|sync_full|withheld",
  "source_availability": "live|missing|deleted|withheld|not_applicable|unknown"
}
```

The proof is query-owned, closed, and not copied from arbitrary search-core
metadata. `content_origin` must be `indexed_preview`, `redaction_state` must
be `safe_preview` or `redacted`, `visibility` must not be `withheld`, and
`source_availability` must be a known value other than `unknown`. Only then
may the bounded snippet become `ContentV1` text. A missing proof field,
unknown value, raw/withheld redaction, withheld visibility, or unknown source
availability makes the result `content_state: "withheld"` with null text and
sets `suppression_reason: "missing_proof"`; the sanitizer
never accepts the indexed snippet merely because it is non-empty. A known
`source_availability: "missing"` does not erase an already-proven indexed
preview, but it suppresses source provenance fields as defined below.

### Explicit provenance types

`ProvenanceV1` is a closed type, not a copy of `SourceFullV1`:

```json
{
  "capture_source_id": "uuid",
  "availability": "live|missing|deleted|withheld",
  "provider": "...",
  "kind": "...",
  "provider_session_id": "...",
  "cwd": "...",
  "stored_path": "...",
  "source_format": "...",
  "started_at": "RFC3339 UTC",
  "ended_at": "RFC3339 UTC or null"
}
```

The implemented v1 boundary omits `provider_session_id`, `cwd`, `stored_path`,
and `source_format` even in full mode (D1); these fields are reserved rather
than exposing path or provider-owned identity. It emits no volatile
`exists`/`filesystem_exists` value. Availability
means database/source-record state only: a missing source row is `missing`, an
explicit source deletion marker is `deleted`, and withheld source visibility
is `withheld`. The current capture-source schema has no deletion column, so it
cannot manufacture `deleted` from a missing raw file. `source_format` is the
bounded value extracted from the allowlisted metadata pointers already used by
the query layer; all other source metadata is suppressed. `cursor`, machine
ID, process ID, raw source payload, and arbitrary sync metadata are never
emitted in any field set.
Missing and withheld provenance serializes only `capture_source_id` and
`availability`; provider, kind, timestamps, and all metadata are omitted.

An event's `provenance` is its event source. `session_provenance` is included
only when its session source is distinct. A session's `provenance` is its
session source. Search results use the source associated with the selected
result item, with the same event-versus-session rule. This makes nested source
behavior explicit and consistent across all domains.

### Citation eligibility and omissions

`EvidenceCitationTypeV1` is a new closed enum owned by the evidence contract;
it is independent of the current `ContextCitationType` and must not be a type
alias, integer cast, or string passthrough:

```text
history_record | session | run | event | vcs_change | artifact | summary |
file | source
```

The mapping from the current core enum is exact and fail-closed:

| Current `ContextCitationType` | `EvidenceCitationTypeV1` |
| --- | --- |
| `HistoryRecord` | `history_record` |
| `Session` | `session` |
| `Run` | `run` |
| `Event` | `event` |
| `VcsChange` | `vcs_change` |
| `Artifact` | `artifact` |
| `Summary` | `summary` |
| `File` | `file` |

`source` has no current `ContextCitationType` variant. It is emitted only from
an explicit bounded capture-source citation candidate whose target kind is
`source`; it is never inferred from a path, source ID, or metadata label. An
unknown future core variant, an absent mapping, a mismatched target type, or a
source candidate without explicit source kind is omitted with
`unsupported_type`/`missing_proof`, never copied as a string.

`CitationV1` is exactly the bounded tuple `citation_type` (the new enum),
`target_id`, matching `target_item_type`, `label`, `time`, optional
`ctx_session_id`, optional `ctx_event_id`, optional `event_seq`, and optional
normalized `provenance`. It has no quote, snippet, cursor, URL, raw path, or
arbitrary metadata field. The sanitizer emits a citation only when its target
is live, its enum mapping and target ID are known, its stored timestamp is
known, and its provenance passes the same source visibility rules. For a
withheld target, the citation is omitted even if its ID is known.

`citation_omissions` is a bounded array of `{ "reason": ENUM, "count": N }`
with reasons `missing_target`, `deleted_target`, `ambiguous_target`,
`withheld_target`, `unsupported_type`, `missing_proof`, `over_limit`, or
`invalid_provenance`.
Reasons contain no target text. Counts are exact within the page's bounded
citation input. Exceeding the citation count or string cap fails closed rather
than silently dropping an unknown tail; known ineligible citations are
omitted and accounted for.
Eligible citations are deduplicated and sorted by `(citation_type, target_id,
time)`; labels are evidence-owned fixed strings rather than source labels.

### Timestamp and byte determinism

SQLite stores ctx timestamps as integer milliseconds (`*_at_ms`). The
normalized projection serializes every stored timestamp as UTC RFC3339 with
exactly three fractional digits (`YYYY-MM-DDTHH:MM:SS.sssZ`), including
`.000`, and never invents sub-millisecond precision. `generated_at` is not part
of the v1 artifact: it is volatile operation metadata, not evidence. Likewise
filesystem existence is not part of provenance. With those fields excluded,
the same canonical request and snapshot produce byte-identical JSONL/Markdown
apart from an explicitly changed database snapshot. Operational timing may be
reported on stderr, never in the artifact.

## Common record contract

### JSONL

The format identifier is `ctx-evidence-bundle-jsonl-v1`. Output is UTF-8,
without a BOM, one compact JSON object per LF-terminated line, with no blank
lines. JSON serialization uses the repository serializer and its normal JSON
escaping; object member order is fixed by the contract below. Consumers must
ignore additive fields but must reject an unknown `schema_version`.

Every record, including errors, contains:

```json
{
  "schema_version": "ctx-evidence-bundle-jsonl-v1",
  "record_type": "manifest|session|event|result|completion|error",
  "private": true,
  "share_safe": false
}
```

The first successful record is `manifest`:

```json
{
  "schema_version": "ctx-evidence-bundle-jsonl-v1",
  "record_type": "manifest",
  "private": true,
  "share_safe": false,
  "bundle_id": "sha256:<64 lowercase hex>",
  "request_binding": { "kind": "evidence_selector", "selector": { "domain": "...", "...": "structural policy and opaque digests" },
  "request_hash": "<opaque #277 request hash>",
  "selector": { "domain": "session_page|search_page|event_ids", "...": "canonical request" },
  "ordering": "session_seq_id_asc|search_ranked_v1|event_occurred_at_id_asc",
  "snapshot": { "schema_version": 1002, "query_revision": 1, "fingerprint": "<opaque #277 snapshot fingerprint>" },
  "counts": {
    "selected_total": 0,
    "retained_pool_total": null,
    "corpus_count": null,
    "returned": 0,
    "omitted_before": 0,
    "omitted_after": 0,
    "omitted_exact": true
  },
  "search_truncation": null,
  "continuation": { "has_more": false, "next": null, "next_arguments": null },
  "bytes": { "item_json_bytes": 0, "page_budget_exhausted": false },
  "format": "jsonl"
}
```

`request_binding` is a closed normalized value, not a query DTO. It includes
the selector domain; modes, roles, event/tool policies, counts and presence;
fields; per-item/page/artifact byte policy; limit; output format; refresh mode;
and the effective current-session rule. Sensitive selector groups are bound by
domain-separated deterministic SHA-256 digests. Raw query/term text, stable
selector IDs, provider/history/source/provider-session identity, tool-name
values, timestamps, workspace/repository/file paths, cursors, payloads, source
objects, and arbitrary metadata are never serialized in the binding.
`request_hash` and `snapshot.fingerprint` are the actual opaque values captured
by the selector/query layer; they are not renderer-generated stand-ins.
`bundle_id` binds the canonical original request through that trusted request
hash plus the structural/digested binding, snapshot fingerprint, normalized
records, and continuation data. It is stable when the same page is replayed,
distinguishes changes to sensitive request groups, and does not expose their
values.
There is intentionally no generation-time field: source/session/event times
are the only timestamps in the artifact, and are normalized to stored
millisecond precision.

For `session_page` and `event_ids`, `selected_total` is populated and the
search-only `retained_pool_total`/`corpus_count` fields are null. For
`search_page`, `selected_total` is null, `retained_pool_total` is populated,
and `corpus_count` uses the exact/lower-bound form defined above.

`session_page` writes one `session` record followed by event records. The
session envelope is additional: `selected_total`, `returned`, and omitted
accounting count events only, so `omitted_before + returned + omitted_after ==
selected_total`. The session record uses the evidence-safe normalized session
projection.
`search_page` writes one `result` record per admitted normalized search
projection. `event_ids` writes one normalized event record per admitted event.
The projections retain stable ID names (`ctx_session_id`, `ctx_event_id`,
`item_id`, `event_seq`, `sequence`) where useful, but are not #195 DTOs.

An event record contains the exact normalized event fields above. A result
record contains the exact normalized result fields above. Search records retain
the current result rank and explicit content state; they do not claim to be
complete when `search_truncation.truncated` is true.

The final successful record is exactly one `completion` record. It repeats the
counts, truncation, byte summary, and continuation, so a JSONL consumer can
stream item records and only commit the page after completion. A post-format
failure emits exactly one `error` record and exits nonzero; it does not emit a
false completion. A broken stdout pipe is the existing paged-output silent
success case. An explicit unsafe or failed `--out` write is an error.

### Safe Markdown

The format identifier is `ctx-evidence-bundle-markdown-v1`. The first line is
the exact machine marker:

```text
<!-- ctx-evidence-bundle: {"schema_version":"ctx-evidence-bundle-markdown-v1","private":true,"share_safe":false} -->
```

The document then contains the same manifest, item records, and completion in
the same order as JSONL. Each record begins with a fixed HTML comment carrying
`record_type`, `schema_version`, `private:true`, `share_safe:false`, and its
stable `record_id` when it has one. The renderer is a projection, not a second
data model: every displayed ID, timestamp, provenance field, truncation state,
and citation must be obtainable from the corresponding JSONL record.

Metadata is rendered as fixed-label Markdown paragraphs. User-controlled title,
snippet, and event text are rendered inside a fenced block, never interpolated
into a heading, link destination, raw HTML, or an unquoted attribute. A
record's stable ID may be shown in a heading only after it has been validated
as a canonical UUID. Markdown is still private local history and is not
share-safe.

Every non-fenced scalar is passed through the same
`escape_markdown_scalar(value)` routine; there are no bare interpolated values.
This includes title, label, provider, agent type, status, role, event type,
fidelity, availability, capture-source ID, provider-session ID, cwd, stored
path, source format, timestamps, citation type/ID/label, rank, counts, error
codes, selector/filter values, bundle ID, and replay metadata. The routine is:

1. require the already-enforced UTF-8/field byte bound and reject any remaining
   NUL or disallowed control character;
2. normalize CRLF, CR, and LF to the two visible characters `\n`, and tab to
   `\t`, so a scalar cannot create a new Markdown block;
3. emit a bounded Markdown code span whose backtick delimiter is one character
   longer than the longest consecutive backtick run in the normalized value
   (minimum one), with no link destination or HTML context; and
4. escape a delimiter-equivalent run if a future Markdown parser permits
   adjacent longer runs, failing with `markdown_scalar_limit` rather than
   emitting a bare value.

The fixed labels and structural punctuation are renderer constants. Fenced
content uses the separate lengthened-fence routine above; it is never passed
through a scalar interpolation path. This makes provenance values receive the
same escaping as user text even though they are private metadata.

## Provenance, timestamps, citations, and withholding

All ctx IDs are lower-case canonical UUID strings. Provider-owned IDs remain
metadata and never replace ctx IDs. Timestamps are RFC3339 UTC strings and
retain stored millisecond precision. Use `occurred_at` for event time, session
`started_at`/`ended_at` for session time, source `started_at`/`ended_at` for
capture provenance. There is no `generated_at` field: operation timing is not
evidence and is reported only on stderr when needed. The exact closed
`ProvenanceV1`, `ContentV1`, and `CitationV1` fields are defined above; no
other #195 source or payload fields are eligible. Fields unavailable in the
live source are omitted or represented by the closed availability/state enum,
never guessed. Full output remains private.

The following are hard no-leak rules:

- `redaction_state` `raw` or `withheld`, `visibility` `withheld`, and an
  unavailable raw/blob payload all produce `content_state: "withheld"` with
  null text; Markdown may render only the fixed marker `[content withheld]`.
  No raw JSON, blob bytes, fallback preview, cursor, or error detail is
  substituted;
- a withheld payload is never made available by choosing Markdown, full
  fields, search results, or a citation;
- a citation is emitted only when its target is a live, eligible ctx item (or a
  live source/file metadata target supported by the current citation type) and
  its stable target ID, type, timestamp, and bounded provenance are known;
  citation quotes and raw payloads are not part of v1;
- a missing, deleted, ambiguous, or withheld citation target is omitted and
  counted in `citation_omissions` with a non-sensitive reason. The exporter
  never invents a citation from a source path or text snippet;
- a source marked withheld contributes no path, cwd, cursor, provider-owned
  ID, or metadata values; only its stable ctx source ID and
  `availability: "withheld"` may remain.

This is fail-closed: inability to prove that content or a citation is
eligible removes the content, rather than broadening access or guessing.

## Deterministic text and long content

- JSONL uses standard JSON string escaping, including escaping control
  characters; it never emits invalid UTF-8 or a partially written JSON line.
- Object member order is fixed by the normalized Rust structs. Event/result
  records use the domain ordering above; canonical request arrays preserve
  their defined input order (including repeated terms), citation arrays sort by
  `(citation_type, target_id, time)`, and citation-omission reasons use a fixed
  enum order; `why_matched` uses the query service's documented order, with a
  lexical fallback for any future unordered reason source. No hash-map
  iteration order may reach the artifact.
- Markdown uses LF line endings and UTF-8 without a BOM. Normalize CRLF and
  bare CR in content to LF for rendering; retain the original text byte count
  in the truncation object.
- Text truncation follows #195: truncate only at a UTF-8 code-point boundary;
  append the three-byte Unicode ellipsis only when it fits; report original
  and returned UTF-8 byte counts and `truncated`.
- A page admits whole item projections only. The page byte count is the exact
  sum of the compact JSON encoding of admitted item objects, excluding fixed
  envelope/framing bytes. If the first item cannot fit, fail with
  `item_exceeds_page_budget`; never return a cursor that repeats forever.
- Markdown fences use backticks and have length `max(3, longest consecutive
  backtick run in the content + 1)`. This is computed after truncation, so a
  fence cannot be closed by content. Empty content still gets a three-backtick
  fence. No content is parsed as Markdown.
- Titles and labels used outside fences escape backslash, `*`, `_`, `` ` ``,
  `[`, `]`, `<`, `>`, `#`, and leading list/quote markers. URLs and file paths
  are displayed as text, not links. This avoids both formatting ambiguity and
  accidental link fetching by a viewer.

## Artifact-wide bounds

The page byte budget is not sufficient: a large manifest, replay request,
provenance object, citation array, or Markdown comment could otherwise make an
unbounded artifact around a bounded page. V1 applies these limits before any
bytes are emitted:

| Value | Default | Maximum | Applies to |
| --- | ---: | ---: | --- |
| `artifact_bytes` | 1 MiB | 16 MiB | Complete UTF-8 JSONL/Markdown bytes, including LF/newlines, comments, manifest, items, completion, and next arguments |
| `record_json_bytes` | — | 2 MiB | Any normalized JSON record including its envelope |
| `manifest_bytes` | — | 128 KiB | Manifest after serialization |
| `completion_bytes` | — | 128 KiB | Completion after serialization |
| `next_arguments_bytes` | — | 128 KiB | Canonical replay arguments, including domain, format, filters, and token |
| `error_bytes` | — | 8 KiB | Structured JSONL error or stderr diagnostic |
| metadata/filter string | — | 4096 UTF-8 bytes | Every provider/source/workspace/path/role/tool/filter value and every scalar metadata field |
| label/reason string | — | 512 UTF-8 bytes | Titles, labels, `why_matched`, omission reasons, and Markdown labels |
| filter/terms array | — | 32 entries | Roles, excluded roles/tools, repeated terms, match reasons, and other repeated filter values; existing 32-clause and 65536 aggregate query limits also apply |
| citations per item | — | 32 | Normalized eligible citations |
| citation-omission reasons per item | — | 16 | `{reason,count}` entries |
| provenance objects per item | — | 2 | Event source plus distinct session source |
| explicit event IDs | — | 256 | Complete canonical UUIDs in the selector |
| records per page | — | 1001 | One session plus 1000 events, or the existing 200-result/200-event caps |

The existing content cap remains 4096/1048576 bytes per content field, and the
page item projection sum remains 262144/16777216 bytes. The artifact cap is
independent of that item sum and includes the envelope overhead. The effective
artifact cap must be at least the serialized empty manifest plus completion;
otherwise the request fails with `artifact_budget_too_small`.

Every string and array is checked after canonicalization and before rendering.
Metadata, filter, ID, provenance, citation, and replay-argument values are
never silently truncated. Oversize input or normalized data fails closed with
`string_limit`, `array_limit`, `provenance_limit`, `citation_limit`,
`next_arguments_limit`, `manifest_limit`, `record_limit`, or
`artifact_limit` as appropriate. Content is the only user-derived field that
may be truncated, and it carries its explicit `ContentV1.truncation` object.

The canonical request itself is serialized and measured against
`next_arguments_bytes`; query clauses additionally retain #195's 4096-byte
per-clause and 65536-byte aggregate limits. The effective search request
always includes `include_current_session: true`, `refresh: "off"`,
`domain: "search_page"`, and the requested `format`, so those fields are
included in both the cap and the token binding. A continuation token's size is
not a substitute for bounding the arguments it replays.

The implementation must normalize records and render the complete bounded
artifact into a bounded staging buffer (or perform an equivalent complete
size preflight) before writing stdout or an output file. It must account for
every byte, including JSONL framing/newlines, Markdown comments/fences,
manifest/completion records, and next arguments. It must not emit a prefix and
then discover `artifact_limit`. Staging is capped at 16 MiB plus a fixed
constant for the write syscall; an over-limit artifact fails before output and
before creating an output target.

The renderer validates normalized-page invariants before either format is
rendered: `pagination.returned_items` equals the selected item-record count;
for `session_page`, the one session envelope is additional and excluded,
`has_more` is equivalent to continuation presence, pagination and replay
tokens agree, normalized record JSON bytes recompute to the carried accounting,
domain count fields are mutually exclusive and consistent, selector/page/work
limits match the closed request binding, and session/search/event record kinds
match the selector domain. Completion counts and byte summaries are derived
from the validated records rather than contradictory carried values. Markdown
uses a counting pass followed by a cap-enforced writing pass; neither pass can
allocate beyond the artifact cap apart from bounded scalar formatting
temporaries.

## Atomic private output

`--out PATH` writes one complete JSONL or Markdown file at an absent target.
It is not append or merge. **V1 only creates an absent target. Existing targets are
always refused.** Atomic replacement of an existing target is explicitly
deferred to the parent #200 scope; v1 does not claim to solve replacement
races with a check-then-rename sequence.

The threat model assumes another local process with a different UID, including
an untrusted one, may race path components, create a target, replace a
symlink, or add a hard link between any two ordinary metadata checks. The
writer therefore treats `lstat`/`stat` checks as diagnostics only, never as
authorization for a later rename. The owner-only parent is the trust boundary:
a compromised process already running as the caller and able to access that
`0700` directory is outside v1's guarantee, because portable path-based
rename APIs cannot make the temporary source name immune to that same-UID
attacker. V1 must not claim protection against that actor. It must:

1. walk the parent directory descriptor-relatively without following symlinks;
   create a missing final parent only with mode `0700`, and require an existing
   parent to be owned by the caller, a directory, and free of group/other
   permission bits. A symlink or unsafe component fails with
   `unsafe_output_path`;
2. reject an existing output immediately as `output_exists`, regardless of
   whether it is a regular file, symlink, directory, or hard link. It must not
   open, truncate, chmod, unlink, or replace that target;
3. create a uniquely named sibling through the parent directory descriptor
   with `O_CREAT|O_EXCL|O_NOFOLLOW` where available and mode `0600`, write the
   already bounded artifact, flush and `fsync` it, and verify regular-file,
   caller-ownership, and `st_nlink == 1` invariants. A failed verification
   unlinks only this private temporary file through the parent descriptor;
4. publish with an OS conditional **no-replace** primitive, never ordinary
   rename: Linux `renameat2(RENAME_NOREPLACE)`, macOS
   `renameatx_np(RENAME_EXCL)`, or a platform-equivalent operation that
   atomically succeeds only when the destination is absent. If the primitive
   is unavailable, unsupported, or returns an ambiguous result, fail closed
   with `atomic_create_unavailable`/`atomic_create_failed` and leave the
   target untouched;
5. `fsync` the parent directory after successful publication. A competitor
   that creates the target before the conditional operation causes
   `output_exists`; a competitor cannot cause the writer to overwrite a target,
   symlink, or hard link. The temporary file is cleaned on all failures.

The conditional no-replace operation is the security boundary. A preflight
`lstat` that says “absent” does not authorize publication, and a post-check
cannot repair an unsafe ordinary rename. V1 has no overwrite or replacement
semantics at all. The deferred parent-scope replacement design must separately
specify same-file identity, symlink/hard-link races, and platform support
before it can be added.

Without `--out`, the bounded private-marked artifact goes to stdout and normal
pipe behavior applies. With `--out`, success writes no data to stdout. A
secure-output error (`output_exists`, `unsafe_output_path`,
`atomic_create_unavailable`, `atomic_create_failed`, or `output_io`) is emitted
once on stderr with a bounded machine-readable code and human context, exits
nonzero, and never writes a JSONL error record into stdout or a partial target.
For stdout JSONL, post-format query/render errors still emit one bounded
terminal JSONL `error` record; Markdown/stdout errors are stderr-only. This
keeps `--out`'s data channel unambiguous and ensures a failed private write
cannot be mistaken for a valid bundle.

## Bounded work and errors

The exporter inherits the #195 bounds and adds no unbounded staging:

- search candidate generation is capped at 200 and uses the existing bounded
  scan/truncation signals;
- session reads fetch at most `limit + 1` selected events, with a maximum page
  limit of 1000;
- explicit IDs are capped at 256, are resolved in one bounded query, and use a
  bounded source cache; no per-ID full transcript fetch is allowed;
- item text, final item JSON, page JSON, citation arrays, provenance fields,
  and next arguments are each bounded by the existing request caps and the
  explicit finite-ID cap;
- JSONL retains one-record-per-line framing, but query/render work must finish
  and validate counts/continuation before the bounded artifact is published;
  no raw blob is loaded merely to discover its size;
- complete artifact staging is capped at 16 MiB and includes every envelope,
  string, array, provenance, citation, filter, newline, and replay argument;
  no output prefix is emitted before that bound is proven;
- no network, provider refresh, import, repository write, or raw SQL is part
  of export.

Stable machine-readable error codes are required for at least:
`invalid_selector`, `unsupported_combination`, `invalid_id_set`,
`too_many_event_ids`, `missing_target`, `deleted_target`,
`invalid_continuation`, `continuation_request_mismatch`,
`continuation_kind_mismatch`, `stale_continuation`, `snapshot_changed`,
`item_exceeds_page_budget`, `artifact_budget_too_small`, `string_limit`,
`array_limit`, `provenance_limit`, `citation_limit`, `next_arguments_limit`,
`manifest_limit`, `record_limit`, `artifact_limit`, `unsafe_output_path`,
`output_exists`, `atomic_create_unavailable`, `atomic_create_failed`,
`markdown_scalar_limit`, `output_io`, and `serialization`. Error details must
not include raw payload,
source content, or withheld values. JSONL emits one terminal error record after
output intent has been parsed; secure-file errors are stderr-only and never
replace an existing output. Markdown is staged and published only after
success.

## Tests and acceptance gates

The implementation is accepted only when these contract tests pass with temp
homes and deterministic fixtures, without network access or the real home:

1. selector exclusivity, unsupported filter combinations, full-ID validation,
   duplicate/size limits, and canonical next arguments for all three domains;
2. session mode selection, `(seq,id)` order, exact live counts, deleted-event
   exclusion in count/read/lookahead/cursor paths, page-byte stops,
   stale/mismatched/wrong-kind continuations, and replay in a fresh process;
3. search filter preservation, explicit `include_current_session: true`,
   relative-time freezing, candidate cap, retained-pool versus corpus
   lower-bound counts, stable rank order, and no refresh;
4. finite-ID chronological/tie ordering, exact counts, missing/deleted target
   failures, ID-list digest binding, and continuation replay;
5. the sanitizer boundary: compact event session IDs, exact safe fields,
   missing/deleted/withheld sources, no cursor/raw/filesystem-exists fields,
   stored-millisecond timestamps, citations, citation omissions, and
   raw/withheld payload non-leakage;
6. JSONL parseability one line at a time, exactly one manifest and completion,
   one terminal error on failure, and Markdown marker/order/fence/escaping
   behavior for quotes, backticks, CRLF, invalid-looking URLs, control
   characters, Unicode boundaries, and long content;
7. output permissions, owner-only parent creation, descriptor-relative
   symlink/hard-link checks, existing-target refusal, conditional no-replace
   creation on macOS/Linux (including unavailable-primitive failure), failed
   publication cleanup, target-creation races, and `--out` stderr behavior;
8. every artifact-wide string/array/provenance/citation/filter/replay cap,
   complete byte accounting before stdout, over-limit fail-closed errors, and
   the existing `ctx-cli` behavioral contract, with docs and formatting checks.

## Implementation sequence

1. Add a transport-neutral evidence selector/query core. Reuse
   `QueryService::session_events`, `QueryService::search`, byte policy,
   snapshot fingerprint, and token encoding, but add the live predicate to
   every count/read/lookahead/cursor path, finite-ID lookup/cursor, and the
   explicit `include_current_session: true` effective search request. Do not
   add a union cursor.
2. Add `EvidenceProjectionV1::normalize` and its closed provenance/content/
   citation types. Enforce the field and artifact-wide caps here, before any
   renderer, and prove that no #195 full DTO, raw payload, cursor, arbitrary
   metadata, or filesystem existence value crosses the boundary.
3. Add JSONL and Markdown renderers over only normalized records. Make
   manifest/completion authoritative, preflight the complete artifact byte
   bound, preserve the stdout JSONL error/broken-pipe behavior, and generate
   canonical next arguments containing domain and format.
4. Add the secure absent-target writer and thin CLI adapter only after the
   query, sanitizer, and renderer tests are green. MCP wiring is deferred
   until a separate scope decision; it is not a hidden requirement of this
   v1 child. The CLI adapter parses and delegates rather than inventing a
   second query or projection model.

## Non-goals

V1 does not export deleted history, merge selector domains, refresh/import
sources, retrieve raw payload blobs, redact or classify secrets, synthesize
claims, resolve external URLs, preserve provider argument order as evidence
order, add a persistent export database, add cross-process cursor state, raise
the #195 candidate cap, change the existing search ranking contract, or
overwrite/replace an existing output path. Atomic replacement is deferred to
the parent scope and requires a separate no-replace-safe design. It also does
not claim that a truncated search bundle represents the complete historical
corpus, and it does not add MCP export wiring in v1.

## Proposed child tickets

These are proposed local IDs only; no tracker records were created or changed.
They are intentionally limited to four implementation slices:

| ID | Type / points | Title | Dependency | Acceptance summary |
| --- | --- | --- | --- | --- |
| `200.1` | `feature`, `points:5` | Add bounded evidence selector/query core | none | Exactly-one-domain normalization; live-only session/search/ID selection across count/read/lookahead/cursor paths; finite-ID cursor; explicit current-session inclusion; retained-pool/corpus count semantics; canonical domain/format-preserving replay arguments. |
| `200.2` | `security`, `points:5` | Add evidence-safe normalized projections and sanitizer | `200.1` | Closed session/event/result/provenance/content/citation types; compact event session ID; cursor/raw/filesystem-exists suppression; stored-ms timestamps; all field and artifact-wide caps; fail-closed omission/error tests. |
| `200.3` | `feature`, `points:5` | Render bounded evidence-bundle JSONL and Markdown v1 | `200.2` | Versioned manifest/items/completion/error records; complete artifact preflight; private marker; deterministic UTF-8, escaping, fences, timestamps, counts, and long-content tests. |
| `200.4` | `security`, `points:5` | Add atomic absent-target output and thin CLI adapter | `200.3` | 0700/0600 owner-only output, descriptor-relative checks, conditional no-replace primitive or fail-closed error, existing-target refusal, stderr `--out` errors, and temp-home macOS/Linux coverage; MCP deferred. |

Completion of these children would implement #200. This document itself is
only the bounded scope decision and does not claim that implementation or
ticket closure has happened.
