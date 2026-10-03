package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"alertreplay/internal/replay"
)

func TestParseCLI(t *testing.T) {
	now := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	args := []string{"-url", "https://prom.example/prefix", "-start", "now-30d", "-step", "2m", "-header", "Authorization: Bearer abc:def", "-header", "X-Value: first", "-header", "X-Value: second", "-rule", "One", "-rule", "Two", "-lookback", "2d", "-inline-recording", "-json", "-incidents", "incidents.yml", "one.yml", "two.yml"}
	opts, err := parseCLI(args, now, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.config.Start.Equal(now.Add(-30*24*time.Hour)) || !opts.config.End.Equal(now) {
		t.Errorf("times: %+v", opts.config)
	}
	if opts.config.Step != 2*time.Minute || opts.config.QPS != 3 || !opts.config.InlineRecording || opts.lookback != 48*time.Hour || !opts.json || opts.incidentsPath != "incidents.yml" {
		t.Errorf("options: %+v", opts)
	}
	if got := opts.config.Headers.Get("Authorization"); got != "Bearer abc:def" {
		t.Errorf("authorization=%q", got)
	}
	if got := opts.config.Headers.Values("X-Value"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("headers=%v", got)
	}
	if len(opts.config.RuleNames) != 2 || len(opts.files) != 2 {
		t.Errorf("names/files: %+v", opts)
	}
	opts, err = parseCLI([]string{"-url", "http://prom", "-start", "2026-09-01", "rules.yml"}, now, io.Discard)
	if err != nil || opts.config.Step != 0 || opts.lookback != 24*time.Hour {
		t.Errorf("defaults: %+v, %v", opts, err)
	}
}

func TestCLIValidation(t *testing.T) {
	base := []string{"-url", "http://prom", "-start", "2026-09-01", "-end", "2026-10-01"}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"missing-url", []string{"-start", "2026-09-01", "rules.yml"}},
		{"missing-start", []string{"-url", "http://prom", "rules.yml"}},
		{"missing-file", base},
		{"bad-url", appendCopy(base, "-url", "ftp://prom", "rules.yml")},
		{"missing-host", appendCopy(base, "-url", "http:///", "rules.yml")},
		{"query-url", appendCopy(base, "-url", "http://prom?x=1", "rules.yml")},
		{"bad-start", appendCopy(base, "-start", "invalid", "rules.yml")},
		{"bad-end", appendCopy(base, "-end", "invalid", "rules.yml")},
		{"reversed", appendCopy(base, "-end", "2026-08-01", "rules.yml")},
		{"equal", appendCopy(base, "-end", "2026-09-01", "rules.yml")},
		{"bad-step", appendCopy(base, "-step", "x", "rules.yml")},
		{"zero-step", appendCopy(base, "-step", "0", "rules.yml")},
		{"negative-step", appendCopy(base, "-step", "-1m", "rules.yml")},
		{"fractional-ms-step", appendCopy(base, "-step", "1.5ms", "rules.yml")},
		{"zero-qps", appendCopy(base, "-qps", "0", "rules.yml")},
		{"nan-qps", appendCopy(base, "-qps", "NaN", "rules.yml")},
		{"infinite-qps", appendCopy(base, "-qps", "+Inf", "rules.yml")},
		{"negative-lookback", appendCopy(base, "-lookback", "-1d", "rules.yml")},
		{"bad-lookback", appendCopy(base, "-lookback", "x", "rules.yml")},
		{"bad-header", appendCopy(base, "-header", "MissingColon", "rules.yml")},
		{"empty-header", appendCopy(base, "-header", ": value", "rules.yml")},
		{"spaced-header", appendCopy(base, "-header", "Bad Name: value", "rules.yml")},
		{"newline-header", appendCopy(base, "-header", "Good: value\n", "rules.yml")},
		{"nul-header", appendCopy(base, "-header", "Good: value\x00", "rules.yml")},
		{"empty-rule", appendCopy(base, "-rule", " ", "rules.yml")},
		{"unknown-flag", appendCopy(base, "-not-real", "rules.yml")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if opts, err := parseCLI(tc.args, time.Now(), io.Discard); err == nil {
				t.Fatalf("accepted %+v", opts)
			}
		})
	}
}
func appendCopy(base []string, extra ...string) []string {
	return append(append([]string{}, base...), extra...)
}

func TestRunCLIJSON(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer test" {
			t.Errorf("missing auth header")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"instance":"test"},"values":[[0,"1"],[60,"1"],[120,"1"]]}]}}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	rules := filepath.Join(dir, "rules.yml")
	incidents := filepath.Join(dir, "incidents.yml")
	if err := os.WriteFile(rules, []byte("groups:\n- name: test\n  rules:\n  - alert: Always\n    expr: vector(1)\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(incidents, []byte("- name: outage\n  start: 1970-01-01T00:01:00Z\n  end: 1970-01-01T00:02:00Z\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := runCLI(context.Background(), []string{"-url", server.URL, "-start", "0", "-end", "120", "-qps", "1000000", "-header", "Authorization: Bearer test", "-json", "-incidents", incidents, rules}, &stdout, &stderr, time.Now())
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	var decoded struct {
		replay.Report
		Incidents []replay.RuleIncidents `json:"incidents"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON %q: %v", stdout.String(), err)
	}
	if requests != 1 || len(decoded.Rules) != 1 || len(decoded.Rules[0].Intervals) != 1 {
		t.Fatalf("requests=%d output=%s", requests, stdout.String())
	}
	if len(decoded.Incidents) != 1 || !decoded.Incidents[0].Incidents[0].Caught || *decoded.Incidents[0].Incidents[0].LeadSeconds != 60 {
		t.Errorf("matches=%+v", decoded.Incidents)
	}
	if strings.Contains(stdout.String(), "Report") || !strings.Contains(stdout.String(), `"total_firing_seconds"`) {
		t.Errorf("JSON not flattened/snake-case: %s", stdout.String())
	}
}

func TestRunCLIErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		code int
	}{
		{"help", []string{"-help"}, 0},
		{"usage", nil, 2},
		{"load-incidents", []string{"-url", "http://127.0.0.1:1", "-start", "0", "-end", "60", "-incidents", filepath.Join(t.TempDir(), "missing"), "rules.yml"}, 1},
		{"load-rules", []string{"-url", "http://127.0.0.1:1", "-start", "0", "-end", "60", filepath.Join(t.TempDir(), "missing")}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := runCLI(context.Background(), tc.args, &stdout, &stderr, time.Now()); code != tc.code {
				t.Errorf("code=%d; want %d", code, tc.code)
			}
			if stdout.Len() != 0 || stderr.Len() == 0 {
				t.Errorf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestPrintReport(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	first := start.Add(time.Minute)
	end := start.Add(2 * time.Minute)
	report := &replay.Report{Start: start, End: end, Warnings: []string{"best-effort recording-rule inlining"}, Rules: []replay.RuleResult{{Name: "Alert", Group: "group", File: "rules.yml", Expression: "metric > 0", Step: time.Minute, TotalFiringSeconds: 60,
		Coverage:  []replay.Coverage{{Selector: "old", FirstSample: &start}, {Selector: "new", FirstSample: &first}, {Selector: "missing"}},
		Intervals: []replay.Interval{{Start: first, ObservedUntil: end, PendingSeconds: 300, DurationSeconds: 60, Labels: map[string]string{"z": "last", "a": "first", "alertname": "Alert"}}},
	}}}
	matches := replay.MatchIncidents(report, []replay.Incident{{Name: "outage", Start: end, End: end}}, 24*time.Hour)
	var stdout bytes.Buffer
	if err := printReport(&stdout, report, matches, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Alert", "fired 1 times", "total 1m0s", "step 1m0s", "coverage start unavailable", "WARNING:", "file: rules.yml; group: group", "expression: metric > 0", "data present at range start", "no data observed before", "range-limited sample-grid estimate", "unavailable / no data found", "OPEN (observed through", "5m0s", `{a="first", z="last"}`, "outage", "positive lead = early warning", "0 false-positive firings"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("missing %q in:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "alertname=") {
		t.Errorf("alertname repeated: %s", stdout.String())
	}
	if err := printReport(errorWriter{}, report, matches, 0); err == nil {
		t.Error("output error was ignored")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("broken output") }

func TestCoverageStart(t *testing.T) {
	a, b := time.Unix(0, 0), time.Unix(60, 0)
	for _, tc := range []struct {
		coverage []replay.Coverage
		want     string
	}{
		{nil, "n/a"}, {[]replay.Coverage{{}}, "unavailable"},
		{[]replay.Coverage{{FirstSample: &b}, {FirstSample: &a}}, formatTime(b) + " (grid estimate)"},
		{[]replay.Coverage{{FirstSample: &b}, {}}, "unavailable"},
	} {
		if got := coverageStart(tc.coverage); got != tc.want {
			t.Errorf("got %s; want %s", got, tc.want)
		}
	}
}
