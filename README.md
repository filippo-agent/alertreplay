# alertreplay

Would this alert have caught last week's outage? Would a longer `for:` have
filtered out the noise—or delayed the page until it was too late?

alertreplay runs a Prometheus rules file against historical data and shows when
each alert would have fired. Edit a threshold or duration, run it again, and
compare the results before deploying the change.

It uses Prometheus's own alert rule engine, including `for:` and
`keep_firing_for`. You only need a rules file and access to a Prometheus server
that still has the data. The rules don't need to be deployed, and alertreplay
doesn't change anything on the server.

## Try it

Build with Go 1.27.1 or newer:

```sh
go build -o alertreplay .
```

Then replay your rules over the last 30 days:

```sh
./alertreplay -url http://localhost:9090 -start now-30d rules.yml
```

The report lists each alert's firing periods, how long it was pending before it
fired, and the labels identifying the affected instances. For example, this
shortened output comes from the synthetic data included in the repository:

```text
GaugeForFiveMinutes  fired 2 times  total 17m0s
  START                 END                   DURATION  PENDING
  2026-09-01T00:15:00Z  2026-09-01T00:17:00Z  2m0s      5m0s
  2026-09-01T00:30:00Z  2026-09-01T00:45:00Z  15m0s     5m0s
```

Here, the metric went above the threshold three times: for 3, 7, and 20 minutes.
With `for: 5m`, the first excursion never fired. The other two fired after five
minutes and stayed firing for another two and fifteen minutes, respectively.
Adding `keep_firing_for: 3m` extends their end times to 00:20 and 00:48.

## Tune one alert at a time

Use `-rule` to focus on an alert and `-end` to stop at a particular date:

```sh
./alertreplay -url http://localhost:9090 \
  -start 2026-09-01 -end 2026-09-08 \
  -rule HighLatency rules.yml
```

Now change its threshold or `for:` in the file and run the same command again.
You can repeat `-rule` to select several alerts, or pass several rules files.

Dates are interpreted as UTC. You can also use a full timestamp, such as
`2026-09-01T14:30:00Z`, or Unix seconds. Without `-end`, the replay ends at the
current time.

By default, alerts are evaluated at their rule group's interval, or once a
minute if none is specified. Use `-step 30s` to try a different interval.

## Compare against known incidents

If you have an incident timeline, put it in a YAML file:

```yaml
# incidents.yaml
- name: database-overload
  start: 2026-09-15T22:07:00Z
  end: 2026-09-16T00:30:00Z
```

Then pass it alongside your rules:

```sh
./alertreplay -url http://localhost:9090 \
  -start 2026-09-01 -incidents incidents.yaml rules.yml
```

The report shows which alerts caught each incident and how much warning they
provided. A positive lead time means the alert fired before the incident began;
a negative one means it fired after.

By default, an alert counts as a catch if it was firing at any point during the
incident or in the 24 hours before it. Use, for example, `-lookback 1h` to narrow
that window. Firings outside every incident window are listed as false
positives—so that label is only as reliable as your incident list.

## Before trusting the results

**Silence isn't always good news.** A metric might not have existed yet, or its
older data might have expired. The report checks when each metric first appears
within the replay range and warns about missing or late data. This is a sampled
check, not proof that the history is complete.

**The replay starts with no active alerts.** Start it before the incident you
care about so alerts have time to become pending and fire. Prometheus's alert
state restoration after a restart isn't simulated. Live evaluation schedules
can also shift a firing or resolution by about one evaluation interval.

**Scrape gaps don't necessarily reset `for:`.** They reset it when the alert
expression stops returning the affected series. Prometheus's lookback and
functions such as `rate()` may bridge a gap.

An alert still firing at the end of the replay is marked `OPEN`; the tool
can't tell you when it will resolve. Counts are per alert instance, so two
hosts firing at the same time count as two firings.

### What about recording rules?

Normally, alertreplay uses the recording-rule results already stored in
Prometheus. If you only deployed a recording rule recently, its older history
will be missing even if the underlying metrics were collected.

For simple references, `-inline-recording` can substitute the recording rule's
expression from the same input files. Treat this as an approximation: it doesn't
reproduce the recording rule's labels or evaluation schedule, and it rejects
references it can't safely substitute. See the [reference](docs/reference.md)
for the supported cases and other limitations.

## Other useful options

- **JSON output:** add `-json` to save results for comparison or further analysis.
- **Authentication:** add `-header 'Authorization: Bearer TOKEN'`. Repeat the
  flag if your server needs several headers.
- **Request rate:** `-qps` defaults to three queries per second. Queries run one
  at a time, but a long replay or a rule covering many series can still be
  expensive. Start with a short range.

Run `./alertreplay -help` for all options. Put flags before the rules file names.
The [reference](docs/reference.md) covers output fields, evaluation details, and
unsupported features.

## Development

Run the unit tests with `go test ./...`.

To test against a real Prometheus server, download the pinned test binaries and
run the integration suite:

```sh
./scripts/fetch-prometheus.sh
ALERTREPLAY_INTEGRATION=1 go test ./integration -v -count=1
```

The suite checks known firing periods in synthetic data and compares a replay
with alerts produced by a running Prometheus. See the [integration test guide](integration/README.md)
for details, or the [full example report](testdata/example-output.txt) to see
what the output looks like.
