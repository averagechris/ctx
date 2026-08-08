# SourceHut #267 continuation replay evidence

This is an evidence-only spike. It does not change production query behavior,
continuation tokens, MCP state, or cross-process lifecycle. The ignored tests
are deliberately opt-in because the deterministic fixture writes 200,000
records.

## Same-process QueryService fixture

Command:

```text
cargo test -p ctx-history-query --release -- --ignored --nocapture --test-threads=1
```

The fixture has 200,000 deterministic `HistoryRecord` rows. Every row matches
`needle`, has the same timestamp/title/body, and uses
`Uuid::from_u128(index + 1)`. Page size is 100. The test creates one read-only
`Store` handle and one `QueryService` borrowing that handle, warms one page-1 /
page-2 pair, then records five more pairs. The measured interval includes both
queries; page-1 and page-2 intervals are also printed.

Exact ordered IDs for every sample:

- page 1: `Uuid::from_u128(1..=100)`, canonical IDs
  `00000000-0000-0000-0000-000000000001` through
  `00000000-0000-0000-0000-000000000064`;
- page 2: `Uuid::from_u128(101..=200)`, canonical IDs
  `00000000-0000-0000-0000-000000000065` through
  `00000000-0000-0000-0000-0000000000c8`.

Each page reported `pool_total=200`, page 1 offset `0`, and page 2 offset
`100`. The store-local ranked record-search counter recorded one fixed
candidate-generation/search statement for each page. Thus the per-sample
increment was 2; the cumulative counts below are relative to the post-warmup
baseline:

| sample | page 1 (ms) | page 2 (ms) | page 1 + page 2 (ms) | ranked-search statements since warmup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 176.012 | 173.674 | 349.687 | 2 |
| 2 | 171.382 | 169.587 | 340.970 | 4 |
| 3 | 171.682 | 169.888 | 341.570 | 6 |
| 4 | 170.308 | 170.318 | 340.627 | 8 |
| 5 | 171.090 | 169.265 | 340.355 | 10 |

This proves that the continuation returns the correct next ordered IDs and
that page 2 reruns the fixed candidate generation/search work in a live,
reusable `QueryService` path. The `QueryService` itself is stateless today,
but it can technically be retained with its borrowed `Store` in one process;
the fixture uses exactly that lifetime.

## MCP lifetime

The ignored CLI integration test
`issue_267_mcp_process_serves_both_pages_before_exit` drives initialize, page 1,
and page 2 through one child `ctx mcp serve` process before closing stdin. It
passes with page offsets 0 and 2 and disjoint ordered IDs, so the MCP stdio
loop remains alive across page requests.

That process lifetime does **not** currently preserve query state: each
`tools/call` enters `tool_search`, opens a new read-only `Store`, and constructs
a new `QueryService`. The process survives; the `Store`/`QueryService` state
does not. Reuse is therefore technically possible in the MCP process, but no
MCP-local reusable query object exists today. No state or cache was added by
this spike.

## CLI lifecycle

One representative observation used four separate deterministic JSONL source
files and two separate `target/debug/ctx search` subprocess invocations. The
second invocation received the opaque `next` value emitted by the first:

```text
page 1: 13.988 ms, IDs a9eb3185-a41a-799a-b09f-5988d514c009 and 204de501-cb8b-74a8-b713-fa98adcc43db
page 2: 14.000 ms, IDs 7f981639-7746-79fb-b8c6-befebfc5800c and 91a99543-cdb4-753f-a13d-de537dfd2e10
```

The timings include process startup and JSON output. CLI page 2 is a separate
process, so no process-local `Store` or `QueryService` state can persist across
it. The opaque continuation is sufficient to replay the request, but this
spike does not build cross-process state.

## Decision

The replay gate is **met** on the real same-process reusable path: every page 2
sample is above 25 ms, and page 2 is approximately 98.7%–100.0% of page 1,
well above the required 20% threshold. The MCP process itself is long-lived,
but its current per-call store/service construction is not a reusable path.

Recommendation for a separate small implementation ticket:

> `perf: reuse continuation candidate work within a live QueryService/MCP request lifetime` — **points:3**. Limit the follow-up to process-local reuse on the measured same-process path, retain existing CLI separate-invocation behavior, and define/validate invalidation and correctness there. Do not expand it into a persistent cache, cross-process state, or a continuation-token redesign.

This ticket intentionally does not implement that recommendation.
