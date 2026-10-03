// Package replay evaluates historical vectors with Prometheus's own alert state machine.
package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"text/template/parse"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

type Config struct {
	URL             string
	Start, End      time.Time
	Step            time.Duration
	Headers         http.Header
	RuleNames       []string
	QPS             float64
	InlineRecording bool
}
type Report struct {
	Start    time.Time    `json:"start"`
	End      time.Time    `json:"end"`
	Rules    []RuleResult `json:"rules"`
	Warnings []string     `json:"warnings"`
}
type RuleResult struct {
	Name               string        `json:"name"`
	File               string        `json:"file"`
	Group              string        `json:"group"`
	Expression         string        `json:"expression"`
	Step               time.Duration `json:"step_ns"`
	Coverage           []Coverage    `json:"coverage"`
	Intervals          []Interval    `json:"intervals"`
	TotalFiringSeconds float64       `json:"total_firing_seconds"`
}
type Coverage struct {
	Selector    string     `json:"selector"`
	FirstSample *time.Time `json:"first_sample"`
}
type Interval struct {
	Start           time.Time         `json:"start"`
	End             *time.Time        `json:"end"`
	ObservedUntil   time.Time         `json:"observed_until"`
	PendingSeconds  float64           `json:"pending_seconds"`
	DurationSeconds float64           `json:"duration_seconds"`
	Labels          map[string]string `json:"labels"`
}
type headerTransport struct {
	headers http.Header
	base    http.RoundTripper
}

func (t headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	for k, vs := range t.headers {
		r.Header.Del(k)
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	return t.base.RoundTrip(r)
}

type fetcher struct {
	api    v1.API
	delay  time.Duration
	next   time.Time
	report *Report
}

func (f *fetcher) query(ctx context.Context, expr string, r v1.Range) (model.Matrix, error) {
	if wait := time.Until(f.next); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	f.next = time.Now().Add(f.delay)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	value, warnings, err := f.api.QueryRange(ctx, expr, r)
	for _, w := range warnings {
		f.report.Warnings = append(f.report.Warnings, string(w))
	}
	if err != nil {
		return nil, fmt.Errorf("query %q: %w", expr, err)
	}
	matrix, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("query %q returned %T, expected matrix", expr, value)
	}
	return matrix, nil
}

// Chunks returns disjoint, inclusive ranges with at most 11,000 evaluations each.
func Chunks(start, end time.Time, step time.Duration) []v1.Range {
	if step <= 0 || start.After(end) {
		return nil
	}
	var out []v1.Range
	for t := start; !t.After(end); {
		e := end
		if step <= end.Sub(t)/10999 {
			e = t.Add(10999 * step)
		}
		out = append(out, v1.Range{Start: t, End: e, Step: step})
		t = e.Add(step)
	}
	return out
}
func (f *fetcher) coverage(ctx context.Context, expr parser.Expr, start, end time.Time, step time.Duration) ([]Coverage, error) {
	selectors := map[string]bool{}
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if s, ok := n.(*parser.VectorSelector); ok {
			selectors[s.String()] = true
		}
		return nil
	})
	keys := make([]string, 0, len(selectors))
	for k := range selectors {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Coverage, 0, len(keys))
	for _, selector := range keys {
		c := Coverage{Selector: selector}
		for _, chunk := range Chunks(start, end, step) {
			m, err := f.query(ctx, "min(timestamp("+selector+"))", chunk)
			if err != nil {
				return nil, err
			}
			for _, s := range m {
				for _, p := range s.Values {
					v := float64(p.Value)
					if math.IsNaN(v) || math.IsInf(v, 0) {
						continue
					}
					t := time.UnixMilli(int64(v * 1000)).UTC()
					if c.FirstSample == nil || t.Before(*c.FirstSample) {
						c.FirstSample = &t
					}
				}
			}
			if c.FirstSample != nil {
				break
			}
		}
		out = append(out, c)
	}
	return out, nil
}
func (f *fetcher) vectors(ctx context.Context, expr string, start, end time.Time, step time.Duration) (map[int64]promql.Vector, error) {
	out := map[int64]promql.Vector{}
	for _, chunk := range Chunks(start, end, step) {
		m, err := f.query(ctx, expr, chunk)
		if err != nil {
			return nil, err
		}
		for _, s := range m {
			if len(s.Histograms) > 0 {
				return nil, fmt.Errorf("native histogram alert results are unsupported; reduce them to floats")
			}
			ls := map[string]string{}
			for k, v := range s.Metric {
				ls[string(k)] = string(v)
			}
			l := labels.FromMap(ls)
			for _, p := range s.Values {
				t := int64(p.Timestamp)
				out[t] = append(out[t], promql.Sample{T: t, F: float64(p.Value), Metric: l})
			}
		}
	}
	return out, nil
}
func Run(ctx context.Context, c Config, files []string) (*Report, error) {
	if !c.End.After(c.Start) {
		return nil, errors.New("end must be after start")
	}
	if c.QPS <= 0 || math.IsNaN(c.QPS) || math.IsInf(c.QPS, 0) {
		return nil, errors.New("qps must be finite and positive")
	}
	if c.Step < 0 || (c.Step > 0 && c.Step < time.Millisecond) {
		return nil, errors.New("step must be at least 1ms")
	}
	u, err := url.Parse(c.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("url must be an absolute http(s) URL")
	}
	report := &Report{Start: c.Start, End: c.End, Rules: []RuleResult{}, Warnings: []string{}}
	httpClient := &http.Client{Transport: headerTransport{c.Headers, http.DefaultTransport}, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != u.Scheme || req.URL.Host != u.Host {
			return errors.New("refusing cross-origin redirect (protecting authentication headers)")
		}
		if len(via) >= 10 {
			return errors.New("too many redirects")
		}
		return nil
	}}
	client, err := api.NewClient(api.Config{Address: c.URL, Client: httpClient})
	if err != nil {
		return nil, err
	}
	f := &fetcher{api: v1.NewAPI(client), delay: time.Duration(float64(time.Second) / c.QPS), report: report}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := parser.NewParser(parser.Options{})
	type input struct {
		file  string
		group rulefmt.RuleGroup
	}
	var groups []input
	recordings := map[string]rulefmt.Rule{}
	for _, file := range files {
		rg, errs := rulefmt.ParseFile(file, false, model.UTF8Validation, p, logger)
		if len(errs) > 0 {
			return nil, errors.Join(errs...)
		}
		for _, g := range rg.Groups {
			groups = append(groups, input{file, g})
			for _, r := range g.Rules {
				if r.Record != "" {
					if _, ok := recordings[r.Record]; ok && c.InlineRecording {
						return nil, fmt.Errorf("ambiguous recording rule %q", r.Record)
					}
					recordings[r.Record] = r
				}
			}
		}
	}
	wanted := map[string]bool{}
	for _, n := range c.RuleNames {
		wanted[n] = false
	}
	for _, in := range groups {
		g := in.group
		selected := false
		for _, def := range g.Rules {
			if def.Alert == "" {
				continue
			}
			if len(wanted) == 0 {
				selected = true
				break
			}
			if _, ok := wanted[def.Alert]; ok {
				selected = true
				break
			}
		}
		if !selected {
			continue
		}
		step := c.Step
		if step == 0 {
			step = time.Duration(g.Interval)
		}
		if step == 0 {
			step = time.Minute
		}
		if step < time.Millisecond || step%time.Millisecond != 0 {
			return nil, errors.New("evaluation interval must be a whole number of milliseconds")
		}
		start := AlignCeil(c.Start, step)
		end := AlignCeil(c.End, step)
		if end.After(c.End) {
			end = end.Add(-step)
		}
		if start.After(end) {
			return nil, errors.New("range contains no aligned evaluation timestamps")
		}
		for _, def := range g.Rules {
			if def.Alert == "" {
				continue
			}
			if len(wanted) > 0 {
				if _, ok := wanted[def.Alert]; !ok {
					continue
				}
				wanted[def.Alert] = true
			}
			for _, value := range def.Labels {
				if hasTemplateQuery(value) {
					return nil, fmt.Errorf("%s: label template query calls are unsupported", def.Alert)
				}
			}
			for _, value := range g.Labels {
				if hasTemplateQuery(value) {
					return nil, fmt.Errorf("%s: group label template query calls are unsupported", def.Alert)
				}
			}
			expr, err := p.ParseExpr(def.Expr)
			if err != nil {
				return nil, err
			}
			if expr.Type() != parser.ValueTypeVector {
				return nil, fmt.Errorf("alert %s must return an instant vector", def.Alert)
			}
			if err := validateExpr(expr); err != nil {
				return nil, fmt.Errorf("%s: %w", def.Alert, err)
			}
			rr := RuleResult{Name: def.Alert, File: in.file, Group: g.Name, Expression: def.Expr, Step: step, Intervals: []Interval{}}
			offset := time.Duration(0)
			if g.QueryOffset != nil {
				offset = time.Duration(*g.QueryOffset)
			}
			rr.Coverage, err = f.coverage(ctx, expr, start.Add(-offset), end.Add(-offset), step)
			if err != nil {
				return nil, fmt.Errorf("%s coverage: %w", def.Alert, err)
			}
			for _, cov := range rr.Coverage {
				if cov.FirstSample == nil {
					report.Warnings = append(report.Warnings, fmt.Sprintf("%s: %s has no observed data in requested range", def.Alert, cov.Selector))
				} else if cov.FirstSample.After(c.Start) {
					report.Warnings = append(report.Warnings, fmt.Sprintf("%s: %s: no data observed before %s (evaluation-grid estimate)", def.Alert, cov.Selector, cov.FirstSample.Format(time.RFC3339)))
				}
			}
			if c.InlineRecording {
				rewritten, changed, e := inline(def.Expr, recordings, p, map[string]bool{})
				if e != nil {
					return nil, e
				}
				if changed {
					report.Warnings = append(report.Warnings, def.Alert+": best-effort recording-rule inlining; recorded labels and recording evaluation schedules are not reproduced")
					expr, err = p.ParseExpr(rewritten)
					if err != nil {
						return nil, err
					}
					if err := validateExpr(expr); err != nil {
						return nil, err
					}
					rr.Expression = rewritten
					extra, e := f.coverage(ctx, expr, start.Add(-offset), end.Add(-offset), step)
					if e != nil {
						return nil, e
					}
					rr.Coverage = append(rr.Coverage, extra...)
				}
			}
			vectors, err := f.vectors(ctx, expr.String(), start.Add(-offset), end.Add(-offset), step)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", def.Alert, err)
			}
			merged := map[string]string{}
			for k, v := range g.Labels {
				merged[k] = v
			}
			for k, v := range def.Labels {
				merged[k] = v
			}
			// Annotation templates are intentionally omitted. Label templates remain engine-owned.
			rule := rules.NewAlertingRule(def.Alert, expr, time.Duration(def.For), time.Duration(def.KeepFiringFor), labels.FromMap(merged), labels.EmptyLabels(), labels.EmptyLabels(), "", true, logger)
			query := func(_ context.Context, q string, t time.Time) (promql.Vector, error) {
				if q != expr.String() {
					return nil, errors.New("template query calls are unsupported")
				}
				return vectors[t.UnixMilli()], nil
			}
			open := map[string]int{}
			for ts := start; !ts.After(end); ts = ts.Add(step) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if _, err := rule.Eval(ctx, offset, ts, query, nil, g.Limit); err != nil {
					return nil, fmt.Errorf("%s at %s: %w", def.Alert, ts, err)
				}
				active := map[string]bool{}
				for _, a := range rule.ActiveAlerts() {
					if a.State != rules.StateFiring {
						continue
					}
					key := a.Labels.String()
					active[key] = true
					if _, exists := open[key]; !exists {
						ls := a.Labels.Map()
						delete(ls, "alertname")
						rr.Intervals = append(rr.Intervals, Interval{Start: a.FiredAt, ObservedUntil: ts, PendingSeconds: a.FiredAt.Sub(a.ActiveAt).Seconds(), Labels: ls})
						open[key] = len(rr.Intervals) - 1
					}
				}
				for key, i := range open {
					iv := &rr.Intervals[i]
					iv.ObservedUntil = ts
					if !active[key] {
						t := ts
						iv.End = &t
						delete(open, key)
					}
				}
			}
			for i := range rr.Intervals {
				iv := &rr.Intervals[i]
				iv.DurationSeconds = iv.ObservedUntil.Sub(iv.Start).Seconds()
				rr.TotalFiringSeconds += iv.DurationSeconds
			}
			sort.Slice(rr.Intervals, func(i, j int) bool {
				a, b := rr.Intervals[i], rr.Intervals[j]
				if a.Start.Equal(b.Start) {
					return labels.FromMap(a.Labels).String() < labels.FromMap(b.Labels).String()
				}
				return a.Start.Before(b.Start)
			})
			report.Rules = append(report.Rules, rr)
		}
	}
	for n, found := range wanted {
		if !found {
			return nil, fmt.Errorf("requested alert %q not found", n)
		}
	}
	if len(report.Rules) == 0 {
		return nil, errors.New("no alerting rules selected")
	}
	return report, nil
}

// inline replaces only bare vector selectors using parser source positions. More
// complex selectors are rejected, never silently stripped of matchers or offsets.
func inline(text string, records map[string]rulefmt.Rule, p parser.Parser, stack map[string]bool) (string, bool, error) {
	expr, err := p.ParseExpr(text)
	if err != nil {
		return "", false, err
	}
	type replacement struct {
		start, end int
		text       string
	}
	var edits []replacement
	parser.Inspect(expr, func(n parser.Node, path []parser.Node) error {
		if err != nil {
			return err
		}
		s, ok := n.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		def, ok := records[s.Name]
		if !ok {
			return nil
		}
		if stack[s.Name] {
			err = fmt.Errorf("recording rule cycle at %s", s.Name)
			return err
		}
		if len(s.LabelMatchers) != 1 || s.OriginalOffset != 0 || s.Timestamp != nil || s.StartOrEnd != 0 {
			err = fmt.Errorf("cannot safely inline decorated recording selector %s", s.String())
			return err
		}
		for _, parent := range path {
			if _, ok := parent.(*parser.MatrixSelector); ok {
				err = fmt.Errorf("cannot safely inline range selector for %s; use an explicit subquery", s.Name)
				return err
			}
		}
		stack[s.Name] = true
		body, _, e := inline(def.Expr, records, p, stack)
		delete(stack, s.Name)
		if e != nil {
			err = e
			return e
		}
		pos := s.PositionRange()
		edits = append(edits, replacement{int(pos.Start), int(pos.End), "(" + body + ")"})
		return nil
	})
	if err != nil {
		return "", false, err
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		text = text[:e.start] + e.text + text[e.end:]
	}
	return strings.TrimSpace(text), len(edits) > 0, nil
}

// Inspect template syntax, not text: a label named "query" or a quoted
// string containing it is harmless, while nested/pipelined calls are not.
func hasTemplateQuery(text string) bool {
	tree := parse.New("label")
	tree.Mode = parse.SkipFuncCheck
	trees := map[string]*parse.Tree{}
	_, err := tree.Parse("{{$labels := .Labels}}{{$value := .Value}}{{$externalLabels := .ExternalLabels}}{{$externalURL := .ExternalURL}}"+text, "", "", trees)
	if err != nil {
		return false
	} // Prometheus handles invalid templates itself.
	var visit func(parse.Node) bool
	visit = func(n parse.Node) bool {
		switch n := n.(type) {
		case *parse.IdentifierNode:
			return n.Ident == "query"
		case *parse.ListNode:
			if n != nil {
				for _, child := range n.Nodes {
					if visit(child) {
						return true
					}
				}
			}
		case *parse.ChainNode:
			return visit(n.Node)
		case *parse.ActionNode:
			return visit(n.Pipe)
		case *parse.PipeNode:
			if n != nil {
				for _, child := range n.Cmds {
					if visit(child) {
						return true
					}
				}
			}
		case *parse.CommandNode:
			for _, child := range n.Args {
				if visit(child) {
					return true
				}
			}
		case *parse.IfNode:
			return visit(n.Pipe) || visit(n.List) || visit(n.ElseList)
		case *parse.RangeNode:
			return visit(n.Pipe) || visit(n.List) || visit(n.ElseList)
		case *parse.WithNode:
			return visit(n.Pipe) || visit(n.List) || visit(n.ElseList)
		case *parse.TemplateNode:
			return visit(n.Pipe)
		}
		return false
	}
	for _, t := range trees {
		if visit(t.Root) {
			return true
		}
	}
	return false
}

// start()/end() mean the current instant in live rule evaluations, but
// query_range would bind them to chunk boundaries. Refuse misleading replay.
func validateExpr(expr parser.Expr) error {
	var err error
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		var boundary parser.ItemType
		switch s := n.(type) {
		case *parser.VectorSelector:
			boundary = s.StartOrEnd
		case *parser.SubqueryExpr:
			boundary = s.StartOrEnd
		}
		if boundary != 0 {
			err = errors.New("@ start()/end() is unsupported by batched replay; use a fixed @ timestamp instead")
		}
		return nil
	})
	return err
}
