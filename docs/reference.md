# Reference

For a walkthrough and examples, start with the [README](../README.md).
This page covers the details that matter when interpreting a replay or using
its JSON output.

## Command-line details

Put flags before file names. Start/end accept RFC3339 (including fractions), a
UTC date, Unix seconds, `now`, or `now-30d`. Relative times share one clock
snapshot. Durations support Go syntax plus `d` and `w`. `-start` and `-url` are
required; `-end` defaults to `now`. Repeated headers preserve multiple values;
avoid embedding credentials in shell history. Cross-origin redirects are rejected
to prevent forwarding authentication headers to another host.

The step defaults to each group's `interval`, otherwise 1m. Evaluations are
Unix-epoch aligned, starting at the first grid point >= start and ending at the
last <= end. Steps must be whole milliseconds. Group labels, rule labels,
`query_offset`, and series limits are honored. Each file is strictly validated
with `rulefmt.ParseFile`, including recording rules and unselected alerts.
Unknown `-rule` names fail rather than silently producing empty output.

Requests run **serially**, at most `-qps` per second (default 3), with a two-minute
per-request timeout and Ctrl-C cancellation. Each expression is fetched in
non-overlapping chunks of at most 11,000 points per series. Coverage requests
use the same limiter. Expression vectors are held in memory **one rule at a
time**. High-cardinality rules/long ranges can still use substantial server and
client memory; rate limiting is not a query-cost limit.

## Output and incidents

Text includes a rule summary, each firing's start/end/duration/pending time and
labels, and per-selector coverage. Counts and total duration sum **alert
instances**: two overlapping label sets count twice. JSON includes `rules`,
`warnings`, and optional `incidents`; timestamps are RFC3339, durations ending
in `_seconds` are numbers, and `step_ns` is integer nanoseconds. Rule identity
includes file and group, so duplicate alert names remain distinguishable.

```yaml
# incidents.yaml
- name: arc-throttle
  start: 2026-09-15T22:07:00Z
  end: 2026-09-16T00:30:00Z
```

An incident is caught when a firing overlaps `[start - lookback, end]`.
The default lookback is 24h. Lead time is `incident start - first matching
fire`: positive is early warning, negative is late. Firings overlapping no
incident window are listed as false positives; this classification only has
meaning if the incident list is complete. One firing may catch multiple
incidents. Closed firing intervals are half-open `[start, end)`.

Alerts still firing at the last evaluation have JSON `end: null`; their duration
is a **lower bound**, measured only through `observed_until`. Pending-only
instances are not counted as firings. Pending duration is reported for each
instance that actually fires.

## Data coverage and recording rules

Vector selectors are extracted with Prometheus's parser. Lightweight
`min(timestamp(selector))` range queries find the first sample visible on the
evaluation grid, stopping at the first nonempty chunk. Missing selectors report
unavailable; late selectors report "no data observed before X". The aggregate
coverage start is the latest first-observed sample among required selectors.

**Coverage is range-limited and sampled, not the exact metric creation time or
proof of uninterrupted coverage.** Short-lived series between evaluations may
be missed, different label sets may start later, and Prometheus's lookback may
make samples before the requested start visible. Range selectors, offsets,
subqueries, and OR/absent expressions make this a diagnostic rather than a
formal expression-availability guarantee. No unbounded all-history scan or
high-cardinality `/series` enumeration is performed.

References to recording metrics receive the same coverage checks, so late or
missing recording history is warned about. `-inline-recording` substitutes
parenthesized recording expressions recursively, with an explicit **best-effort**
warning. Only bare metric references are supported. Decorated selectors
(matchers, ranges, offsets or `@`), cycles, and ambiguous duplicate recording
names fail rather than silently changing meaning. Inlining does **not** reproduce
recording labels or recording evaluation schedules. Coverage of both original
recording metrics and substituted source metrics is retained.

## Accuracy and limits

- Prometheus **3.15.0** (`prometheus/prometheus v0.315.0`) provides parsing and
  `rules.AlertingRule.Eval`. The server performs PromQL evaluation; matching the
  server version/features to the embedded version is recommended.
- Live rules run at per-group offsets; edges can differ by up to one evaluation
  interval. Scrape/evaluation ordering can add boundary ambiguity.
- Historical gaps reset `for` when the expression actually becomes absent, just
  as live evaluation would. A missed scrape alone does **not** necessarily reset
  it: lookback and range functions may bridge the gap. Stored data cannot always
  reconstruct past failed evaluations or missing staleness markers.
- Each replay starts with empty alert state; `ALERTS_FOR_STATE` restoration after
  restarts is not simulated. Allow a warm-up period before incidents of interest.
- Annotation templates are skipped. Label templates are engine-rendered, but
  template `query` calls are rejected. External labels and external URL are
  empty; supply equivalent rule labels if necessary.
- Native histogram *result vectors* are rejected explicitly, not silently
  dropped. Expressions reducing histograms to floats can work normally.
- `@ start()`/`@ end()` are rejected: their query-range chunk semantics differ
  from live instant evaluations. Explicit fixed `@` timestamps are supported.
- Incomplete retention/downsampling, server settings and recording-rule changes
  can limit historical fidelity. This tool does not replay recording rules as
  a historical dependency engine or infer missing incidents.

## Dependencies

The embedded rule engine is pinned to Prometheus 3.15.0. Its dependencies make
the module tree relatively large. In addition to Prometheus, its API client,
and YAML, the tool imports `prometheus/common` for types used by the public APIs.
