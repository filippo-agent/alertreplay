// Package integration exercises replay against a real Prometheus TSDB and rule engine.
// Opt in with ALERTREPLAY_INTEGRATION=1; ordinary go test ./... stays fast.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"alertreplay/internal/replay"
)

func rootDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

func binary(t *testing.T, env, name string) string {
	t.Helper()
	if os.Getenv("ALERTREPLAY_INTEGRATION") != "1" {
		t.Skip("set ALERTREPLAY_INTEGRATION=1 to run real-Prometheus integration tests")
	}
	if path := os.Getenv(env); path != "" {
		resolved, err := exec.LookPath(path)
		if err != nil {
			t.Fatalf("%s=%q: %v", env, path, err)
		}
		return resolved
	}
	path := filepath.Join(rootDir(), ".tools", name)
	if info, err := os.Stat(path); err == nil && info.Mode()&0111 != 0 {
		return path
	}
	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	t.Skipf("%s unavailable: install Prometheus 3.15.0 in .tools/ or set %s to the binary path; see integration/README.md", name, env)
	return ""
}

func startPrometheus(t *testing.T, data, config, lookback string) string {
	t.Helper()
	prometheus := binary(t, "ALERTREPLAY_PROMETHEUS", "prometheus")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	configPath := filepath.Join(t.TempDir(), "prometheus.yml")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "prometheus.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(prometheus,
		"--config.file="+configPath, "--storage.tsdb.path="+data,
		"--storage.tsdb.retention.time=100y", "--web.listen-address="+address,
		"--query.lookback-delta="+lookback)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = log.Close()
		if t.Failed() {
			contents, _ := os.ReadFile(logPath)
			t.Logf("Prometheus log:\n%s", contents)
		}
	})
	url := "http://" + address
	waitUntil(t, 15*time.Second, "Prometheus readiness", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", url+"/-/ready", nil)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode == 200
	})
	return url
}

func waitUntil(t *testing.T, timeout time.Duration, description string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, description)
}

func instantValue(url, expression string) (float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", url+"/api/v1/query", nil)
	query := req.URL.Query()
	query.Set("query", expression)
	req.URL.RawQuery = query.Encode()
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer response.Body.Close()
	var result struct {
		Data struct {
			Result []struct {
				Value []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.NewDecoder(response.Body).Decode(&result) != nil || len(result.Data.Result) == 0 || len(result.Data.Result[0].Value) != 2 {
		return 0, false
	}
	var value string
	if json.Unmarshal(result.Data.Result[0].Value[1], &value) != nil {
		return 0, false
	}
	var number float64
	if _, err := fmt.Sscan(value, &number); err != nil {
		return 0, false
	}
	return number, true
}

func historicalPrometheus(t *testing.T) string {
	t.Helper()
	promtool := binary(t, "ALERTREPLAY_PROMTOOL", "promtool")
	// Resolve both binaries before doing work so missing prerequisites skip cleanly.
	binary(t, "ALERTREPLAY_PROMETHEUS", "prometheus")
	data := t.TempDir()
	cmd := exec.Command(promtool, "tsdb", "create-blocks-from", "openmetrics", filepath.Join(rootDir(), "testdata", "synthetic.om"), data)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("backfill: %v\n%s", err, out)
	}
	return startPrometheus(t, data, "global:\n  scrape_interval: 1m\nscrape_configs: []\n", "30s")
}

func TestHistoricalReplay(t *testing.T) {
	url := historicalPrometheus(t)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := replay.Run(ctx, replay.Config{URL: url, Start: start, End: start.Add(time.Hour), QPS: 1000}, []string{filepath.Join(rootDir(), "testdata", "synthetic.rules.yml")})
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string][][2]int{
		"GaugeForFiveMinutes": {{15, 17}, {30, 45}},
		"GaugeKeepFiring":     {{15, 20}, {30, 48}},
		"GapResetsPending":    {{16, 20}},
		"CounterRate":         {{12, 20}},
		"LateCoverage":        {{17, 25}},
		"StillFiring":         {{55, -1}},
	}
	pending := map[string]float64{"GaugeForFiveMinutes": 300, "GaugeKeepFiring": 300, "GapResetsPending": 300, "CounterRate": 120, "LateCoverage": 300, "StillFiring": 300}
	if len(report.Rules) != len(expected) {
		t.Fatalf("got %d rules, want %d", len(report.Rules), len(expected))
	}
	for _, rule := range report.Rules {
		t.Run(rule.Name, func(t *testing.T) {
			intervals, found := expected[rule.Name]
			if !found {
				t.Fatalf("unexpected rule %q", rule.Name)
			}
			if rule.Step != time.Minute {
				t.Errorf("step=%s, want 1m", rule.Step)
			}
			if len(rule.Intervals) != len(intervals) {
				t.Fatalf("got intervals %+v, want %v", rule.Intervals, intervals)
			}
			total := 0.0
			for i, interval := range rule.Intervals {
				wantStart := start.Add(time.Duration(intervals[i][0]) * time.Minute)
				if !interval.Start.Equal(wantStart) {
					t.Errorf("interval %d start=%s, want %s", i, interval.Start, wantStart)
				}
				endMinute := intervals[i][1]
				if endMinute < 0 {
					if interval.End != nil {
						t.Errorf("open interval end=%s, want nil", interval.End)
					}
					if !interval.ObservedUntil.Equal(start.Add(time.Hour)) {
						t.Errorf("observed_until=%s, want %s", interval.ObservedUntil, start.Add(time.Hour))
					}
					endMinute = 60
				} else {
					wantEnd := start.Add(time.Duration(endMinute) * time.Minute)
					if interval.End == nil || !interval.End.Equal(wantEnd) {
						t.Errorf("interval %d end=%v, want %s", i, interval.End, wantEnd)
					}
				}
				duration := float64(endMinute-intervals[i][0]) * 60
				total += duration
				if interval.DurationSeconds != duration {
					t.Errorf("duration=%v, want %v", interval.DurationSeconds, duration)
				}
				if interval.PendingSeconds != pending[rule.Name] {
					t.Errorf("pending=%v, want %v", interval.PendingSeconds, pending[rule.Name])
				}
				if interval.Labels["instance"] != "fixture" {
					t.Errorf("labels=%v, want instance=fixture", interval.Labels)
				}
				if rule.Name == "GaugeForFiveMinutes" && interval.Labels["severity"] != "warning" {
					t.Errorf("rule label missing: %v", interval.Labels)
				}
			}
			if rule.TotalFiringSeconds != total {
				t.Errorf("total=%v, want %v", rule.TotalFiringSeconds, total)
			}
			selector := "synthetic_load"
			switch rule.Name {
			case "GapResetsPending":
				selector = "synthetic_gap"
			case "CounterRate":
				selector = "synthetic_requests_total"
			case "LateCoverage":
				selector = "synthetic_late"
			case "StillFiring":
				selector = "synthetic_tail"
			}
			coverageFound := false
			for _, coverage := range rule.Coverage {
				if strings.Contains(coverage.Selector, selector) {
					coverageFound = true
					first := start
					if rule.Name == "LateCoverage" {
						first = first.Add(12 * time.Minute)
					}
					if coverage.FirstSample == nil || !coverage.FirstSample.Equal(first) {
						t.Errorf("coverage first=%v, want %s", coverage.FirstSample, first)
					}
				}
			}
			if !coverageFound {
				t.Errorf("missing %s coverage: %+v", selector, rule.Coverage)
			}
		})
	}
}

func TestLiveAlertsCrossCheck(t *testing.T) {
	binary(t, "ALERTREPLAY_PROMETHEUS", "prometheus")
	var high atomic.Bool
	scrape := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		value := 0
		if high.Load() {
			value = 1
		}
		fmt.Fprintf(w, "# TYPE synthetic_live gauge\nsynthetic_live %d\n", value)
	}))
	defer scrape.Close()
	ruleFile := filepath.Join(rootDir(), "testdata", "live.rules.yml")
	config := fmt.Sprintf("global:\n  scrape_interval: 1s\n  evaluation_interval: 1s\nrule_files:\n  - %q\nscrape_configs:\n  - job_name: fixture\n    static_configs:\n      - targets: [%q]\n", ruleFile, strings.TrimPrefix(scrape.URL, "http://"))
	url := startPrometheus(t, t.TempDir(), config, "2s")
	waitUntil(t, 10*time.Second, "first successful synthetic scrape", func() bool { value, ok := instantValue(url, "synthetic_live"); return ok && value == 0 })
	start := time.Now().UTC().Truncate(time.Second)
	high.Store(true)
	waitUntil(t, 10*time.Second, "live alert firing", func() bool {
		_, ok := instantValue(url, `ALERTS{alertname="LiveCrossCheck",alertstate="firing"}`)
		return ok
	})
	// Keep the plateau wide enough for multiple evaluations after the transition.
	time.Sleep(3 * time.Second)
	high.Store(false)
	waitUntil(t, 10*time.Second, "low sample scraped", func() bool { value, ok := instantValue(url, "synthetic_live"); return ok && value == 0 })
	waitUntil(t, 10*time.Second, "live alert resolving after keep_firing_for", func() bool {
		_, ok := instantValue(url, `ALERTS{alertname="LiveCrossCheck",alertstate="firing"}`)
		return !ok
	})
	time.Sleep(2 * time.Second)
	end := time.Now().UTC().Truncate(time.Second)
	// Independently backtest Prometheus's recorded firing-state series with no hold.
	// This obtains the live transitions through exactly the same aligned query grid.
	recordedFile := filepath.Join(t.TempDir(), "recorded.rules.yml")
	recorded := `groups:
  - name: recorded
    interval: 1s
    rules:
      - alert: RecordedLiveAlerts
        expr: ALERTS{alertname="LiveCrossCheck",alertstate="firing"}
`
	if err := os.WriteFile(recordedFile, []byte(recorded), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := replay.Run(ctx, replay.Config{URL: url, Start: start, End: end, QPS: 1000}, []string{ruleFile, recordedFile})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]replay.RuleResult{}
	for _, rule := range report.Rules {
		byName[rule.Name] = rule
	}
	predicted, actual := byName["LiveCrossCheck"], byName["RecordedLiveAlerts"]
	if len(predicted.Intervals) != 1 || len(actual.Intervals) != 1 {
		t.Fatalf("want one predicted and live interval; predicted=%+v actual=%+v", predicted.Intervals, actual.Intervals)
	}
	p, a := predicted.Intervals[0], actual.Intervals[0]
	if p.End == nil || a.End == nil {
		t.Fatalf("expected resolved intervals: predicted=%+v actual=%+v", p, a)
	}
	// A live group has an arbitrary sub-second offset; one extra step tolerates
	// scrape/evaluation ordering at a boundary. This is not a for-duration tolerance.
	const tolerance = 2 * time.Second
	for _, edge := range []struct {
		name              string
		predicted, actual time.Time
	}{{"start", p.Start, a.Start}, {"end", *p.End, *a.End}} {
		delta := edge.predicted.Sub(edge.actual)
		if delta < 0 {
			delta = -delta
		}
		if delta > tolerance {
			t.Errorf("%s difference=%s exceeds %s: replay=%s live=%s", edge.name, delta, tolerance, edge.predicted, edge.actual)
		}
	}
	t.Logf("replay [%s,%s); live ALERTS [%s,%s), allowed group-offset tolerance %s", p.Start, p.End, a.Start, a.End, tolerance)
}

func TestCLIEndToEnd(t *testing.T) {
	url := historicalPrometheus(t)
	executable := filepath.Join(t.TempDir(), "alertreplay")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", executable, ".")
	build.Dir = rootDir()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	args := []string{"-url", url, "-start", "2026-09-01", "-end", "1788224400", "-qps", "1000", "-rule", "GaugeForFiveMinutes", "-rule", "CounterRate", "-incidents", filepath.Join(rootDir(), "testdata", "incidents.yml"), "-lookback", "0s", "-json", filepath.Join(rootDir(), "testdata", "synthetic.rules.yml")}
	command := exec.CommandContext(ctx, executable, args...)
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI: %v\n%s", err, out)
	}
	var result struct {
		replay.Report
		Incidents []replay.RuleIncidents `json:"incidents"`
	}
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("decode CLI JSON: %v\n%s", err, out)
	}
	if len(result.Rules) != 2 {
		t.Fatalf("repeatable -rule got %d rules, want2: %s", len(result.Rules), out)
	}
	for _, rule := range result.Rules {
		want := 1020.0
		if rule.Name == "CounterRate" {
			want = 480
		} else if rule.Name != "GaugeForFiveMinutes" {
			t.Fatalf("unexpected rule %q", rule.Name)
		}
		if rule.TotalFiringSeconds != want {
			t.Errorf("%s total=%v, want%v", rule.Name, rule.TotalFiringSeconds, want)
		}
	}
	if len(result.Incidents) != 2 {
		t.Fatalf("got %d incident rows, want2", len(result.Incidents))
	}
	for _, row := range result.Incidents {
		if len(row.Incidents) != 2 {
			t.Fatalf("%s got%d incidents, want2", row.Name, len(row.Incidents))
		}
		first, second := row.Incidents[0], row.Incidents[1]
		if !first.Caught || first.LeadSeconds == nil {
			t.Errorf("%s missed first incident: %+v", row.Name, first)
		}
		if row.Name == "GaugeForFiveMinutes" {
			if first.LeadSeconds != nil && *first.LeadSeconds != 0 {
				t.Errorf("first lead=%v, want0", *first.LeadSeconds)
			}
			if !second.Caught || second.LeadSeconds == nil || *second.LeadSeconds != 300 {
				t.Errorf("second match=%+v, want5m lead", second)
			}
		} else {
			if first.LeadSeconds != nil && *first.LeadSeconds != 180 {
				t.Errorf("counter lead=%v, want180", *first.LeadSeconds)
			}
			if second.Caught {
				t.Errorf("counter should not catch second incident: %+v", second)
			}
		}
		if len(row.FalsePositives) != 0 {
			t.Errorf("unexpected false positives: %+v", row.FalsePositives)
		}
	}
}
