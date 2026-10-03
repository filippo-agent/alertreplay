# Real-Prometheus integration tests

Ordinary `go test ./...` skips these process-based tests. To enable them:

```sh
ALERTREPLAY_INTEGRATION=1 go test ./integration -v -count=1
```

Use Prometheus **3.15.0** binaries in `.tools/prometheus` and `.tools/promtool`, or
put them on `PATH`. Override their locations independently:

```sh
ALERTREPLAY_INTEGRATION=1 \
ALERTREPLAY_PROMETHEUS=/path/to/prometheus \
ALERTREPLAY_PROMTOOL=/path/to/promtool \
go test ./integration -v -count=1
```

Missing binaries skip tests with instructions. An explicitly supplied invalid
binary path fails instead of silently skipping. Tests bind loopback ephemeral
ports, write all TSDB/config/log files to temporary directories, and kill their
Prometheus processes at cleanup. The live cross-check typically takes 10–15s.

`testdata/generate.py` regenerates the checked-in OpenMetrics fixture. It uses
Unix **seconds**, not milliseconds. Gauge plateaus are 3m, 7m, and 20m; a 6m
missing-scrape region resets pending state with `--query.lookback-delta=30s`.
Historical samples are on the exact minute grid. Tests assert firing starts,
resolutions, pending durations, total firing duration, rule/series labels,
coverage starts (including a late metric), and an unresolved final interval.
The counter rises at 1/s normally and 10/s over the high window.

The CLI test builds the actual executable and checks JSON output, repeated
`-rule` filtering, date/Unix timestamp parsing, and incident catches/lead times.

The live test serves a synthetic gauge with 1s scrapes, loads a 1s rule with
`for: 2s` and `keep_firing_for: 2s`, then replays both that rule and its recorded
`ALERTS{alertstate="firing"}` series. Transition differences may be up to 2s
because live groups have a sub-second scheduling offset and scrape/evaluation
ordering can shift an edge by a step.

`testdata/example-output.txt` contains actual CLI output captured against the
backfilled fixture. Regenerate it without any persistent service:

```sh
python3 integration/generate-example.py
```

The script builds the current CLI, backfills a temporary TSDB, starts Prometheus
on a loopback ephemeral port, captures output, and stops Prometheus even if
capture fails. It accepts the same binary-path environment overrides as the
tests.

To reproduce CLI output manually (keep Prometheus running in another terminal):

```sh
mkdir -p /tmp/alertreplay-example-tsdb
.tools/promtool tsdb create-blocks-from openmetrics \
  testdata/synthetic.om /tmp/alertreplay-example-tsdb
printf 'global:\n  scrape_interval: 1m\nscrape_configs: []\n' > /tmp/alertreplay-example.yml
.tools/prometheus --config.file=/tmp/alertreplay-example.yml \
  --storage.tsdb.path=/tmp/alertreplay-example-tsdb \
  --storage.tsdb.retention.time=100y --query.lookback-delta=30s \
  --web.listen-address=127.0.0.1:19090
```

```sh
go run . -url http://127.0.0.1:19090 \
  -start 2026-09-01T00:00:00Z -end 2026-09-01T01:00:00Z \
  -qps 1000 testdata/synthetic.rules.yml
```
