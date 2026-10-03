package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
)

var replayTestStart = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

type replayAPIRequest struct {
	Query  string
	Range  v1.Range
	Header http.Header
	Path   string
}

type replayAPIResponse struct {
	Matrix     model.Matrix
	Warnings   []string
	ResultType string
	Result     any
	Error      string
	HTTPStatus int
}

// Exercise the real client, including HTTP form encoding and result decoding.
func replayTestAPI(t *testing.T, respond func(replayAPIRequest) replayAPIResponse) (string, func() []replayAPIRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []replayAPIRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse query form: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		parseTime := func(key string) time.Time {
			seconds, err := strconv.ParseFloat(r.Form.Get(key), 64)
			if err != nil {
				t.Errorf("parse %s: %v", key, err)
			}
			return time.UnixMilli(int64(math.Round(seconds * 1000))).UTC()
		}
		seconds, err := strconv.ParseFloat(r.Form.Get("step"), 64)
		if err != nil {
			t.Errorf("parse step: %v", err)
		}
		request := replayAPIRequest{
			Query:  r.Form.Get("query"),
			Range:  v1.Range{Start: parseTime("start"), End: parseTime("end"), Step: time.Duration(seconds * float64(time.Second))},
			Header: r.Header.Clone(), Path: r.URL.Path,
		}
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		response := respond(request)
		for _, stream := range response.Matrix {
			if stream.Metric == nil {
				stream.Metric = model.Metric{}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if response.HTTPStatus != 0 {
			w.WriteHeader(response.HTTPStatus)
		}
		body := map[string]any{"status": "success", "warnings": response.Warnings}
		if response.Error != "" {
			body["status"], body["errorType"], body["error"] = "error", "execution", response.Error
		} else {
			kind := response.ResultType
			if kind == "" {
				kind = "matrix"
			}
			result := response.Result
			if result == nil {
				result = response.Matrix
				if response.Matrix == nil {
					result = model.Matrix{}
				}
			}
			body["data"] = map[string]any{"resultType": kind, "result": result}
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Errorf("encode API response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, func() []replayAPIRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]replayAPIRequest(nil), requests...)
	}
}

func replayTestFetcher(t *testing.T, address string) *fetcher {
	t.Helper()
	client, err := api.NewClient(api.Config{Address: address})
	if err != nil {
		t.Fatal(err)
	}
	return &fetcher{api: v1.NewAPI(client), report: &Report{Warnings: []string{}}}
}

func replayTestRules(t *testing.T, text string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "rules.yml")
	if err := os.WriteFile(file, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func replayTestConfig(address string) Config {
	return Config{URL: address, Start: replayTestStart, End: replayTestStart.Add(12 * time.Minute), Step: time.Minute, QPS: 1e9}
}

func replayTestPair(ts time.Time, value float64) model.SamplePair {
	return model.SamplePair{Timestamp: model.Time(ts.UnixMilli()), Value: model.SampleValue(value)}
}

func replayTestGrid(r v1.Range, metric model.Metric, value float64) model.Matrix {
	stream := &model.SampleStream{Metric: metric}
	for ts := r.Start; !ts.After(r.End); ts = ts.Add(r.Step) {
		stream.Values = append(stream.Values, replayTestPair(ts, value))
	}
	return model.Matrix{stream}
}

func TestChunks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		points     int
		step, tail time.Duration
		want       []int
	}{
		{"one", 1, time.Minute, 0, []int{1}},
		{"limit", 11000, time.Second, 0, []int{11000}},
		{"limit_plus_one", 11001, time.Second, 0, []int{11000, 1}},
		{"two_full", 22000, time.Millisecond, 0, []int{11000, 11000}},
		{"two_plus_one", 22001, time.Millisecond, 0, []int{11000, 11000, 1}},
		{"ragged_end", 11002, time.Second, 500 * time.Millisecond, []int{11000, 2}},
		// Previously 10999*step overflowed and moved the loop backwards.
		{"large_step_no_overflow", 4, 14 * 24 * time.Hour, 0, []int{4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			end := replayTestStart.Add(time.Duration(tc.points-1)*tc.step + tc.tail)
			chunks := Chunks(replayTestStart, end, tc.step)
			if len(chunks) != len(tc.want) {
				t.Fatalf("got %d chunks, want %d", len(chunks), len(tc.want))
			}
			seen := 0
			for i, chunk := range chunks {
				if chunk.Step != tc.step || chunk.Start.After(chunk.End) || chunk.End.After(end) {
					t.Fatalf("bad chunk: %+v", chunk)
				}
				wantStart := replayTestStart.Add(time.Duration(seen) * tc.step)
				if !chunk.Start.Equal(wantStart) {
					t.Fatalf("chunk %d starts at %s, want %s", i, chunk.Start, wantStart)
				}
				count := int(chunk.End.Sub(chunk.Start)/tc.step) + 1
				if count != tc.want[i] || count > 11000 {
					t.Fatalf("chunk %d has %d evaluations, want %d", i, count, tc.want[i])
				}
				seen += count
			}
			if seen != tc.points || !chunks[len(chunks)-1].End.Equal(end) {
				t.Fatalf("incomplete range: %d evaluations, last end %s", seen, chunks[len(chunks)-1].End)
			}
		})
	}
	for _, step := range []time.Duration{0, -time.Second} {
		if got := Chunks(replayTestStart, replayTestStart.Add(time.Hour), step); got != nil {
			t.Errorf("invalid step %s: %v", step, got)
		}
	}
	if got := Chunks(replayTestStart, replayTestStart.Add(-time.Second), time.Second); got != nil {
		t.Errorf("reversed range: %v", got)
	}
}

func TestRunHTTPForKeepFiringAndLabels(t *testing.T) {
	const offset = 30 * time.Second
	address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		if strings.HasPrefix(r.Query, "min(timestamp(") {
			return replayAPIResponse{Matrix: model.Matrix{&model.SampleStream{Values: []model.SamplePair{replayTestPair(r.Range.Start, float64(r.Range.Start.Unix()))}}}, Warnings: []string{"coverage backend warning"}}
		}
		stream := &model.SampleStream{Metric: model.Metric{"__name__": "up", "instance": "a", "severity": "metric"}}
		// First pending spell resets at 2m. A new spell fires at 5m.
		// Return at 7m resets keep_firing_for; missing 8m/9m resolves at 10m.
		for _, minute := range []int{0, 1, 3, 4, 5, 7, 11, 12} {
			stream.Values = append(stream.Values, replayTestPair(replayTestStart.Add(time.Duration(minute)*time.Minute-offset), 2))
		}
		return replayAPIResponse{Matrix: model.Matrix{stream}, Warnings: []string{"expression backend warning"}}
	})
	file := replayTestRules(t, `groups:
- name: production
  interval: 1m
  query_offset: 30s
  labels:
    team: infra
    severity: group
  rules:
  - alert: Unavailable
    expr: up > 0
    for: 2m
    keep_firing_for: 2m
    labels:
      severity: critical
      identity: '{{ $labels.instance }}-{{ printf "%.0f" $value }}'
`)
	config := replayTestConfig(address)
	config.Headers = http.Header{"Authorization": {"Bearer secret"}, "X-Tenant": {"first", "second"}}
	report, err := Run(context.Background(), config, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rules) != 1 {
		t.Fatalf("rules: %+v", report.Rules)
	}
	rr := report.Rules[0]
	if rr.Name != "Unavailable" || rr.Group != "production" || rr.File != file || rr.Expression != "up > 0" || rr.Step != time.Minute {
		t.Fatalf("metadata: %+v", rr)
	}
	if len(rr.Coverage) != 1 || rr.Coverage[0].Selector != "up" || rr.Coverage[0].FirstSample == nil || !rr.Coverage[0].FirstSample.Equal(config.Start.Add(-offset)) {
		t.Fatalf("coverage: %+v", rr.Coverage)
	}
	if len(rr.Intervals) != 1 {
		t.Fatalf("intervals: %+v", rr.Intervals)
	}
	iv := rr.Intervals[0]
	if !iv.Start.Equal(config.Start.Add(5*time.Minute)) || iv.End == nil || !iv.End.Equal(config.Start.Add(10*time.Minute)) || !iv.ObservedUntil.Equal(*iv.End) || iv.PendingSeconds != 120 || iv.DurationSeconds != 300 || rr.TotalFiringSeconds != 300 {
		t.Fatalf("interval: %+v, total %v", iv, rr.TotalFiringSeconds)
	}
	wantLabels := map[string]string{"instance": "a", "team": "infra", "severity": "critical", "identity": "a-2"}
	if !reflect.DeepEqual(iv.Labels, wantLabels) {
		t.Errorf("labels: %v, want %v", iv.Labels, wantLabels)
	}
	if !reflect.DeepEqual(report.Warnings, []string{"coverage backend warning", "expression backend warning"}) {
		t.Errorf("warnings: %v", report.Warnings)
	}
	got := requests()
	if len(got) != 2 {
		t.Fatalf("requests: %+v", got)
	}
	for _, r := range got {
		if r.Path != "/api/v1/query_range" || r.Header.Get("Authorization") != "Bearer secret" || !reflect.DeepEqual(r.Header.Values("X-Tenant"), []string{"first", "second"}) {
			t.Errorf("HTTP request: %+v", r)
		}
		if !r.Range.Start.Equal(config.Start.Add(-offset)) || !r.Range.End.Equal(config.End.Add(-offset)) || r.Range.Step != time.Minute {
			t.Errorf("query offset not applied: %+v", r.Range)
		}
	}
}

func TestRunOpenIntervalsAndSorting(t *testing.T) {
	address, _ := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		if strings.HasPrefix(r.Query, "min(timestamp(") {
			return replayAPIResponse{}
		}
		var matrix model.Matrix
		for _, instance := range []string{"z", "a"} {
			stream := &model.SampleStream{Metric: model.Metric{"__name__": "up", "instance": model.LabelValue(instance)}}
			for _, minute := range []int{0, 2, 3} {
				stream.Values = append(stream.Values, replayTestPair(replayTestStart.Add(time.Duration(minute)*time.Minute), 0))
			}
			matrix = append(matrix, stream)
		}
		return replayAPIResponse{Matrix: matrix}
	})
	file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: up == 0\n")
	config := replayTestConfig(address)
	config.End = config.Start.Add(3 * time.Minute)
	report, err := Run(context.Background(), config, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	rr := report.Rules[0]
	if len(rr.Intervals) != 4 {
		t.Fatalf("intervals: %+v", rr.Intervals)
	}
	for i, iv := range rr.Intervals {
		wantInstance := []string{"a", "z", "a", "z"}[i]
		wantStart := config.Start
		if i >= 2 {
			wantStart = config.Start.Add(2 * time.Minute)
		}
		if !iv.Start.Equal(wantStart) || iv.Labels["instance"] != wantInstance || iv.PendingSeconds != 0 || iv.DurationSeconds != 60 {
			t.Errorf("interval %d: %+v", i, iv)
		}
		if i < 2 && (iv.End == nil || !iv.End.Equal(config.Start.Add(time.Minute))) {
			t.Errorf("closed interval: %+v", iv)
		}
		if i >= 2 && (iv.End != nil || !iv.ObservedUntil.Equal(config.End)) {
			t.Errorf("open interval: %+v", iv)
		}
	}
	if rr.TotalFiringSeconds != 240 {
		t.Errorf("total: %v", rr.TotalFiringSeconds)
	}
}

func TestRunAcrossHTTPChunks(t *testing.T) {
	address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		return replayAPIResponse{Matrix: replayTestGrid(r.Range, model.Metric{}, 1)}
	})
	file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: vector(1)\n    for: 11s\n")
	config := replayTestConfig(address)
	config.Step, config.End = time.Millisecond, config.Start.Add(11001*time.Millisecond)
	report, err := Run(context.Background(), config, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	got := requests()
	if len(got) != 2 || !got[0].Range.End.Equal(config.Start.Add(10999*time.Millisecond)) || !got[1].Range.Start.Equal(config.Start.Add(11000*time.Millisecond)) || !got[1].Range.End.Equal(config.End) {
		t.Fatalf("chunks: %+v", got)
	}
	ivs := report.Rules[0].Intervals
	if len(ivs) != 1 || !ivs[0].Start.Equal(config.Start.Add(11*time.Second)) || ivs[0].PendingSeconds != 11 || ivs[0].End != nil || ivs[0].DurationSeconds != 0.001 {
		t.Fatalf("for state must survive chunk boundary: %+v", ivs)
	}
}

func TestRunUnixAlignedGrid(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end time.Time
		step       time.Duration
	}{
		{"seven_minutes", time.Unix(0, 0).UTC(), time.Unix(840, 0).UTC(), 7 * time.Minute},
		{"before_epoch", time.Unix(-841, 0).UTC(), time.Unix(-1, 0).UTC(), 7 * time.Minute},
		{"fractional_millisecond_bounds", time.Unix(0, 1500000).UTC(), time.Unix(0, 8500000).UTC(), 3 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
				return replayAPIResponse{Matrix: replayTestGrid(r.Range, model.Metric{}, 1)}
			})
			file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: vector(1)\n")
			config := replayTestConfig(address)
			config.Start, config.End, config.Step = tc.start, tc.end, tc.step
			report, err := Run(context.Background(), config, []string{file})
			if err != nil {
				t.Fatal(err)
			}
			wantStart := AlignCeil(tc.start, tc.step)
			wantEnd := AlignCeil(tc.end, tc.step)
			if wantEnd.After(tc.end) {
				wantEnd = wantEnd.Add(-tc.step)
			}
			got := requests()
			if len(got) != 1 || !got[0].Range.Start.Equal(wantStart) || !got[0].Range.End.Equal(wantEnd) {
				t.Fatalf("grid: %+v, want %s to %s", got, wantStart, wantEnd)
			}
			iv := report.Rules[0].Intervals[0]
			if !iv.Start.Equal(wantStart) || !iv.ObservedUntil.Equal(wantEnd) {
				t.Errorf("interval: %+v", iv)
			}
			if !report.Start.Equal(tc.start) || !report.End.Equal(tc.end) {
				t.Errorf("requested bounds changed: %+v", report)
			}
		})
	}
}

func TestCoverageDeduplicationOrderingAndFiniteSamples(t *testing.T) {
	address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		if r.Query == "min(timestamp(absent))" {
			return replayAPIResponse{}
		}
		return replayAPIResponse{Matrix: model.Matrix{
			&model.SampleStream{Values: []model.SamplePair{replayTestPair(r.Range.Start, math.NaN()), replayTestPair(r.Range.Start.Add(time.Second), math.Inf(1)), replayTestPair(r.Range.Start.Add(2*time.Second), math.Inf(-1))}},
			&model.SampleStream{Values: []model.SamplePair{replayTestPair(r.Range.Start.Add(3*time.Second), float64(replayTestStart.Unix())+3), replayTestPair(r.Range.Start.Add(4*time.Second), float64(replayTestStart.Unix())+1.25)}},
		}}
	})
	expr, err := parser.NewParser(parser.Options{}).ParseExpr("z + z or absent")
	if err != nil {
		t.Fatal(err)
	}
	f := replayTestFetcher(t, address)
	coverage, err := f.coverage(context.Background(), expr, replayTestStart, replayTestStart.Add(11001*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(coverage) != 2 || coverage[0].Selector != "absent" || coverage[0].FirstSample != nil || coverage[1].Selector != "z" || coverage[1].FirstSample == nil || !coverage[1].FirstSample.Equal(replayTestStart.Add(1250*time.Millisecond)) {
		t.Fatalf("coverage: %+v", coverage)
	}
	got := requests()
	if len(got) != 3 || got[0].Query != "min(timestamp(absent))" || got[1].Query != "min(timestamp(absent))" || got[2].Query != "min(timestamp(z))" {
		t.Fatalf("coverage chunk scan/deduplication: %+v", got)
	}
}

func TestRunCoverageWarnings(t *testing.T) {
	address, _ := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		switch r.Query {
		case "min(timestamp(late))":
			return replayAPIResponse{Matrix: model.Matrix{&model.SampleStream{Values: []model.SamplePair{replayTestPair(r.Range.Start.Add(2*time.Minute), float64(replayTestStart.Add(2*time.Minute).Unix()))}}}}
		case "min(timestamp(missing))":
			return replayAPIResponse{}
		case "min(timestamp(up))":
			return replayAPIResponse{Matrix: model.Matrix{&model.SampleStream{Values: []model.SamplePair{replayTestPair(r.Range.Start, float64(replayTestStart.Add(-time.Minute).Unix()))}}}}
		default:
			return replayAPIResponse{}
		}
	})
	file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: up or missing or late\n")
	report, err := Run(context.Background(), replayTestConfig(address), []string{file})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"A: late: no data observed before 2024-01-01T00:02:00Z (evaluation-grid estimate)", "A: missing has no observed data in requested range"}
	if !reflect.DeepEqual(report.Warnings, want) {
		t.Errorf("warnings: %v, want %v", report.Warnings, want)
	}
	if len(report.Rules[0].Intervals) != 0 {
		t.Errorf("unexpected firings: %+v", report.Rules[0].Intervals)
	}
}

func TestFetchErrorsAndNativeHistograms(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response replayAPIResponse
		want     string
	}{
		{"execution", replayAPIResponse{Error: "bad query", HTTPStatus: http.StatusUnprocessableEntity, Warnings: []string{"partial data"}}, "bad query"},
		{"wrong_type", replayAPIResponse{ResultType: "vector", Result: []any{}}, "expected matrix"},
		{"native_histogram", replayAPIResponse{Matrix: model.Matrix{&model.SampleStream{Metric: model.Metric{"instance": "a"}, Histograms: []model.SampleHistogramPair{{Timestamp: model.Time(replayTestStart.UnixMilli()), Histogram: &model.SampleHistogram{Count: 1, Sum: 2}}}}}}, "native histogram alert results are unsupported"},
		{"mixed_float_histogram", replayAPIResponse{Matrix: model.Matrix{&model.SampleStream{Values: []model.SamplePair{replayTestPair(replayTestStart, 1)}, Histograms: []model.SampleHistogramPair{{Timestamp: model.Time(replayTestStart.Add(time.Second).UnixMilli()), Histogram: &model.SampleHistogram{Count: 1, Sum: 2}}}}}}, "native histogram alert results are unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, _ := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return tc.response })
			f := replayTestFetcher(t, address)
			_, err := f.vectors(context.Background(), "up", replayTestStart, replayTestStart.Add(time.Minute), time.Minute)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got error %v, want %q", err, tc.want)
			}
			if !reflect.DeepEqual(f.report.Warnings, tc.response.Warnings) && len(tc.response.Warnings) != 0 {
				t.Errorf("warnings lost: %v", f.report.Warnings)
			}
		})
	}
}

func TestFetchRateLimitCancellation(t *testing.T) {
	address, requests := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return replayAPIResponse{} })
	f := replayTestFetcher(t, address)
	f.next = time.Now().Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := f.query(ctx, "up", v1.Range{Start: replayTestStart, End: replayTestStart.Add(time.Second), Step: time.Second})
	if err != context.Canceled || len(requests()) != 0 {
		t.Fatalf("cancellation: %v, requests %v", err, requests())
	}
}

func TestValidateExprBoundaries(t *testing.T) {
	for _, tc := range []struct {
		expr   string
		reject bool
	}{
		{"up @ start()", true}, {"up @ end()", true},
		{"rate(up[5m] @ start())", true},
		{"max_over_time((up > 0)[5m:1m] @ end())", true},
		{"sum(up) + on() group_left() max(up @ end())", true},
		{"up @ 1704067200", false}, {"rate(up[5m] @ 1704067200)", false},
		{"max_over_time((up > 0)[5m:1m] @ 1704067200)", false},
		{"up offset 5m", false}, {"up offset -5m", false}, {"vector(time())", false},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			expr, err := parser.NewParser(parser.Options{}).ParseExpr(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			err = validateExpr(expr)
			if (err != nil) != tc.reject {
				t.Fatalf("validateExpr: %v, reject=%v", err, tc.reject)
			}
		})
	}
}

func TestRunRejectsUnsupportedBoundariesAndQueryTemplates(t *testing.T) {
	for _, tc := range []struct{ name, expr, groupLabels, labels, want string }{
		{"vector_boundary", "up @ start()", "", "", "@ start()/end()"},
		{"subquery_boundary", "max_over_time(up[5m:1m] @ end())", "", "", "@ start()/end()"},
		{"label_query", "up", "", "      extra: '{{ query \"other\" | first | value }}'\n", "label template query calls"},
		{"group_label_query", "up", "  labels:\n    extra: '{{ query \"other\" | first | value }}'\n", "", "group label template query calls"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, requests := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return replayAPIResponse{} })
			text := "groups:\n- name: g\n" + tc.groupLabels + "  rules:\n  - alert: A\n    expr: " + tc.expr + "\n"
			if tc.labels != "" {
				text += "    labels:\n" + tc.labels
			}
			_, err := Run(context.Background(), replayTestConfig(address), []string{replayTestRules(t, text)})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error: %v, want %q", err, tc.want)
			}
			if len(requests()) != 0 {
				t.Fatalf("invalid rule sent HTTP queries: %+v", requests())
			}
		})
	}
}

func TestInline(t *testing.T) {
	records := map[string]rulefmt.Rule{
		"recorded": {Record: "recorded", Expr: "sum by (job) (raw)"},
		"nested":   {Record: "nested", Expr: "recorded / 2"},
		"cycle_a":  {Record: "cycle_a", Expr: "cycle_b"},
		"cycle_b":  {Record: "cycle_b", Expr: "cycle_a"},
	}
	for _, tc := range []struct {
		name, input, want, wantErr string
		changed                    bool
	}{
		{"plain", " recorded > 1 ", "(sum by (job) (raw)) > 1", "", true},
		{"multiple_positions", "recorded + recorded", "(sum by (job) (raw)) + (sum by (job) (raw))", "", true},
		{"recursive", "nested > 1", "((sum by (job) (raw)) / 2) > 1", "", true},
		{"subquery", "max_over_time(recorded[5m:1m])", "max_over_time((sum by (job) (raw))[5m:1m])", "", true},
		{"do_not_edit_strings_or_matchers", `label_replace(raw{job="recorded"}, "x", "recorded", "job", ".*")`, `label_replace(raw{job="recorded"}, "x", "recorded", "job", ".*")`, "", false},
		{"unicode_before_edit", `raw{job="café"} + recorded`, `raw{job="café"} + (sum by (job) (raw))`, "", true},
		{"matcher", `recorded{job="a"}`, "", "decorated recording selector", false},
		{"offset", "recorded offset 1m", "", "decorated recording selector", false},
		{"negative_offset", "recorded offset -1m", "", "decorated recording selector", false},
		{"fixed_timestamp", "recorded @ 1704067200", "", "decorated recording selector", false},
		{"boundary_timestamp", "recorded @ start()", "", "decorated recording selector", false},
		{"range", "rate(recorded[5m])", "", "cannot safely inline range selector", false},
		{"cycle", "cycle_a", "", "recording rule cycle", false},
		{"syntax_error", "recorded +", "", "parse error", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := parser.NewParser(parser.Options{})
			got, changed, err := inline(tc.input, records, p, map[string]bool{})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error: %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want || changed != tc.changed {
				t.Fatalf("inline: %q, %v, %v; want %q, %v", got, changed, err, tc.want, tc.changed)
			}
			if _, err := p.ParseExpr(got); err != nil {
				t.Fatalf("rewritten expression invalid: %v", err)
			}
		})
	}
}

func TestRunRecordingInliningAndLabelsWarning(t *testing.T) {
	address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
		if strings.HasPrefix(r.Query, "min(timestamp(") {
			return replayAPIResponse{}
		}
		return replayAPIResponse{Matrix: replayTestGrid(r.Range, model.Metric{"job": "api"}, 1)}
	})
	file := replayTestRules(t, `groups:
- name: g
  rules:
  - record: recorded
    expr: sum by (job) (raw)
    labels:
      recording_label: intentionally_not_reproduced
  - alert: A
    expr: recorded > 0
`)
	config := replayTestConfig(address)
	config.InlineRecording = true
	report, err := Run(context.Background(), config, []string{file})
	if err != nil {
		t.Fatal(err)
	}
	rr := report.Rules[0]
	if rr.Expression != "(sum by (job) (raw)) > 0" || len(rr.Coverage) != 2 || rr.Coverage[0].Selector != "recorded" || rr.Coverage[1].Selector != "raw" {
		t.Fatalf("rewritten metadata: %+v", rr)
	}
	if len(rr.Intervals) != 1 || !reflect.DeepEqual(rr.Intervals[0].Labels, map[string]string{"job": "api"}) {
		t.Fatalf("recording labels unexpectedly reproduced: %+v", rr.Intervals)
	}
	if !strings.Contains(strings.Join(report.Warnings, "\n"), "recorded labels and recording evaluation schedules are not reproduced") {
		t.Fatalf("missing inlining caveat: %v", report.Warnings)
	}
	got := requests()
	if len(got) != 3 || got[0].Query != "min(timestamp(recorded))" || got[1].Query != "min(timestamp(raw))" || got[2].Query != "(sum by (job) (raw)) > 0" {
		t.Fatalf("queries: %+v", got)
	}
}

func TestRunRejectsBoundaryIntroducedByInlining(t *testing.T) {
	address, requests := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return replayAPIResponse{} })
	file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - record: recorded\n    expr: up @ end()\n  - alert: A\n    expr: recorded > 0\n")
	config := replayTestConfig(address)
	config.InlineRecording = true
	_, err := Run(context.Background(), config, []string{file})
	if err == nil || !strings.Contains(err.Error(), "@ start()/end()") {
		t.Fatalf("error: %v", err)
	}
	got := requests()
	if len(got) != 1 || got[0].Query != "min(timestamp(recorded))" {
		t.Fatalf("rewritten unsupported expression was queried: %+v", got)
	}
}

func TestRunDuplicateRecordingRule(t *testing.T) {
	address, requests := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return replayAPIResponse{} })
	file := replayTestRules(t, "groups:\n- name: first\n  rules:\n  - record: recorded\n    expr: up\n- name: second\n  rules:\n  - record: recorded\n    expr: down\n  - alert: A\n    expr: recorded > 0\n")
	config := replayTestConfig(address)
	config.InlineRecording = true
	_, err := Run(context.Background(), config, []string{file})
	if err == nil || !strings.Contains(err.Error(), `ambiguous recording rule "recorded"`) || len(requests()) != 0 {
		t.Fatalf("duplicate recording: %v, requests: %+v", err, requests())
	}
	config.InlineRecording = false
	if _, err := Run(context.Background(), config, []string{file}); err != nil {
		t.Fatalf("duplicates without inlining are harmless: %v", err)
	}
}

func TestRunValidation(t *testing.T) {
	file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: vector(1)\n")
	for _, tc := range []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"equal_bounds", func(c *Config) { c.End = c.Start }, "end must be after start"},
		{"reversed_bounds", func(c *Config) { c.End = c.Start.Add(-time.Second) }, "end must be after start"},
		{"zero_qps", func(c *Config) { c.QPS = 0 }, "qps must be finite and positive"},
		{"negative_qps", func(c *Config) { c.QPS = -1 }, "qps must be finite and positive"},
		{"nan_qps", func(c *Config) { c.QPS = math.NaN() }, "qps must be finite and positive"},
		{"infinite_qps", func(c *Config) { c.QPS = math.Inf(1) }, "qps must be finite and positive"},
		{"relative_url", func(c *Config) { c.URL = "/prometheus" }, "url must be an absolute http(s) URL"},
		{"bad_scheme", func(c *Config) { c.URL = "ftp://example.com" }, "url must be an absolute http(s) URL"},
		{"negative_step", func(c *Config) { c.Step = -time.Second }, "step must be at least 1ms"},
		{"sub_millisecond", func(c *Config) { c.Step = time.Microsecond }, "step must be at least 1ms"},
		{"fractional_millisecond", func(c *Config) { c.Step = 1500 * time.Microsecond }, "whole number of milliseconds"},
		{"no_grid_points", func(c *Config) { c.Start = c.Start.Add(time.Second); c.End = c.Start.Add(time.Second) }, "range contains no aligned evaluation timestamps"},
		{"missing_selected_alert", func(c *Config) { c.RuleNames = []string{"Missing"} }, `requested alert "Missing" not found`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, requests := replayTestAPI(t, func(replayAPIRequest) replayAPIResponse { return replayAPIResponse{} })
			config := replayTestConfig(address)
			tc.edit(&config)
			_, err := Run(context.Background(), config, []string{file})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error: %v, want %q", err, tc.want)
			}
			if len(requests()) != 0 {
				t.Fatalf("invalid config queried server: %+v", requests())
			}
		})
	}
}

func TestRunRuleSelectionAndIntervals(t *testing.T) {
	for _, tc := range []struct {
		interval string
		override time.Duration
		want     time.Duration
	}{
		{"", 0, time.Minute}, {"7m", 0, 7 * time.Minute}, {"7m", 2 * time.Minute, 2 * time.Minute},
	} {
		t.Run(fmt.Sprintf("interval=%s_override=%s", tc.interval, tc.override), func(t *testing.T) {
			address, requests := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
				return replayAPIResponse{Matrix: replayTestGrid(r.Range, model.Metric{}, 1)}
			})
			text := "groups:\n- name: g\n"
			if tc.interval != "" {
				text += "  interval: " + tc.interval + "\n"
			}
			text += "  rules:\n  - alert: Skip\n    expr: up @ start()\n  - alert: Selected\n    expr: vector(1)\n"
			config := replayTestConfig(address)
			config.Step, config.RuleNames = tc.override, []string{"Selected"}
			report, err := Run(context.Background(), config, []string{replayTestRules(t, text)})
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Rules) != 1 || report.Rules[0].Name != "Selected" || report.Rules[0].Step != tc.want {
				t.Fatalf("selection/step: %+v", report.Rules)
			}
			if got := requests(); len(got) != 1 || got[0].Range.Step != tc.want {
				t.Fatalf("query steps: %+v", got)
			}
		})
	}
}

func TestRunEngineErrors(t *testing.T) {
	for _, tc := range []struct {
		name, groupExtra, expr, labels, want string
		matrix                               model.Matrix
	}{
		{"scalar_expression", "", "1", "", "must return an instant vector", nil},
		{"matrix_expression", "", "up[5m]", "", "must return an instant vector", nil},
		{"no_alerts", "", "", "", "no alerting rules selected", nil},
		{"limit", "  limit: 1\n", "up", "", "exceeded limit of 1", model.Matrix{
			&model.SampleStream{Metric: model.Metric{"instance": "a"}, Values: []model.SamplePair{replayTestPair(replayTestStart, 1)}},
			&model.SampleStream{Metric: model.Metric{"instance": "b"}, Values: []model.SamplePair{replayTestPair(replayTestStart, 1)}},
		}},
		{"duplicate_alert_labels", "", "up", "    labels:\n      instance: collapsed\n", "same labelset", model.Matrix{
			&model.SampleStream{Metric: model.Metric{"instance": "a"}, Values: []model.SamplePair{replayTestPair(replayTestStart, 1)}},
			&model.SampleStream{Metric: model.Metric{"instance": "b"}, Values: []model.SamplePair{replayTestPair(replayTestStart, 1)}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, _ := replayTestAPI(t, func(r replayAPIRequest) replayAPIResponse {
				if strings.HasPrefix(r.Query, "min(timestamp(") {
					return replayAPIResponse{}
				}
				return replayAPIResponse{Matrix: tc.matrix}
			})
			text := "groups:\n- name: g\n" + tc.groupExtra + "  rules:\n"
			if tc.expr != "" {
				text += "  - alert: A\n    expr: " + tc.expr + "\n" + tc.labels
			} else {
				text += "  - record: recorded\n    expr: up\n"
			}
			_, err := Run(context.Background(), replayTestConfig(address), []string{replayTestRules(t, text)})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error: %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRunRedirectAuthenticationSafety(t *testing.T) {
	for _, crossOrigin := range []bool{false, true} {
		t.Run(fmt.Sprintf("cross_origin=%t", crossOrigin), func(t *testing.T) {
			seen := make(chan string, 10)
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- "other-origin"
				fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
			}))
			t.Cleanup(target.Close)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.URL.Path
				if r.Header.Get("Authorization") != "Bearer private" || r.Header.Get("X-API-Key") != "private-key" {
					t.Errorf("configured credentials missing on same-origin request")
				}
				if r.URL.Path == "/api/v1/query_range" {
					destination := "/redirected"
					if crossOrigin {
						destination = target.URL + "/redirected"
					}
					http.Redirect(w, r, destination, http.StatusTemporaryRedirect)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
			}))
			t.Cleanup(source.Close)
			config := replayTestConfig(source.URL)
			config.Headers = http.Header{"Authorization": {"Bearer private"}, "X-API-Key": {"private-key"}}
			file := replayTestRules(t, "groups:\n- name: g\n  rules:\n  - alert: A\n    expr: vector(1)\n")
			_, err := Run(context.Background(), config, []string{file})
			if crossOrigin {
				if err == nil || !strings.Contains(err.Error(), "cross-origin redirect") {
					t.Fatalf("unsafe redirect result: %v", err)
				}
				if len(seen) != 1 {
					t.Fatalf("cross-origin target was contacted: %d requests", len(seen))
				}
			} else {
				if err != nil {
					t.Fatalf("same-origin redirect rejected: %v", err)
				}
				if len(seen) != 2 {
					t.Fatalf("same-origin redirect not followed: %d requests", len(seen))
				}
			}
		})
	}
}

func TestHasTemplateQuery(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       bool
	}{
		{"static_query_word", "query", false},
		{"label_named_query", `{{ $labels.query }}`, false},
		{"field_named_query", `{{ .Labels.query }}`, false},
		{"literal_query_word", `{{ printf "query" }}`, false},
		{"word_between_actions", `{{ $labels.instance }} query {{ $value }}`, false},
		{"comment", `{{/* query "up" */}}ok`, false},
		{"prometheus_variables", `{{ $labels.job }} {{ $externalLabels.cluster }} {{ $externalURL }} {{ $value }}`, false},
		{"benign_control_flow", `{{ if $labels.query }}{{ $labels.query }}{{ else }}query{{ end }}`, false},
		{"different_function", `{{ queryish "up" }}`, false},
		{"invalid_template_left_to_prometheus", `{{ query "up"`, false},
		{"direct", `{{ query "up" }}`, true},
		{"pipeline", `{{ query "up" | first | value }}`, true},
		{"nested_pipeline", `{{ printf "%.0f" (query "up" | first | value) }}`, true},
		{"nested_commands", `{{ value (first (query "up")) }}`, true},
		{"assignment", `{{ $samples := query "up" }}{{ $samples | first | value }}`, true},
		{"if_condition", `{{ if query "up" }}yes{{ end }}`, true},
		{"if_body", `{{ if $labels.job }}{{ query "up" }}{{ end }}`, true},
		{"if_else", `{{ if $labels.job }}yes{{ else }}{{ query "up" }}{{ end }}`, true},
		{"range_condition", `{{ range query "up" }}{{ .Value }}{{ end }}`, true},
		{"range_body", `{{ range $labels }}{{ query "up" }}{{ end }}`, true},
		{"range_else", `{{ range $labels }}yes{{ else }}{{ query "up" }}{{ end }}`, true},
		{"with_condition", `{{ with query "up" }}{{ . }}{{ end }}`, true},
		{"with_body", `{{ with $labels }}{{ query "up" }}{{ end }}`, true},
		{"with_else", `{{ with $labels }}yes{{ else }}{{ query "up" }}{{ end }}`, true},
		{"defined_template", `{{ define "nested" }}{{ query "up" }}{{ end }}{{ template "nested" . }}`, true},
		{"template_argument", `{{ define "nested" }}{{ . }}{{ end }}{{ template "nested" (query "up") }}`, true},
		{"nested_field_chain", `{{ (first (query "up")).Value }}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasTemplateQuery(tc.text); got != tc.want {
				t.Fatalf("hasTemplateQuery(%q) = %t, want %t", tc.text, got, tc.want)
			}
		})
	}
}
