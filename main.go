// alertreplay evaluates Prometheus alert rules against historical server data.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"alertreplay/internal/replay"
)

type repeatedFlag []string

func (r *repeatedFlag) String() string     { return strings.Join(*r, ", ") }
func (r *repeatedFlag) Set(s string) error { *r = append(*r, s); return nil }

type cliOptions struct {
	config        replay.Config
	files         []string
	incidentsPath string
	json          bool
	lookback      time.Duration
}

func parseCLI(args []string, now time.Time, stderr io.Writer) (cliOptions, error) {
	var opts cliOptions
	var start, end, step, lookback string
	var headers, names repeatedFlag
	flags := flag.NewFlagSet("alertreplay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.config.URL, "url", "", "Prometheus base URL (required)")
	flags.StringVar(&start, "start", "", "range start: RFC3339, date, Unix seconds, now-30d (required)")
	flags.StringVar(&end, "end", "now", "range end: RFC3339, date, Unix seconds, now")
	flags.StringVar(&step, "step", "", "override evaluation interval (default: group interval or 1m)")
	flags.Var(&names, "rule", "alert name to replay (repeatable)")
	flags.Var(&headers, "header", "HTTP header as Key: Value (repeatable)")
	flags.Float64Var(&opts.config.QPS, "qps", 3, "maximum HTTP queries per second")
	flags.BoolVar(&opts.config.InlineRecording, "inline-recording", false, "best-effort inline substitution of recording rules")
	flags.StringVar(&opts.incidentsPath, "incidents", "", "known incidents YAML file")
	flags.BoolVar(&opts.json, "json", false, "emit structured JSON")
	flags.StringVar(&lookback, "lookback", "24h", "incident early-warning window (supports d/w)")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: alertreplay -url http://prometheus:9090 -start 2026-07-01 [options] rules.yml [more.yml ...]")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return opts, err
	}
	opts.files = flags.Args()
	if opts.config.URL == "" {
		return opts, errors.New("-url is required")
	}
	u, err := url.Parse(opts.config.URL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return opts, errors.New("-url must be an http(s) base URL without query or fragment")
	}
	if start == "" {
		return opts, errors.New("-start is required")
	}
	if len(opts.files) == 0 {
		return opts, errors.New("at least one rules file is required")
	}
	if opts.config.Start, err = replay.ParseTime(start, now); err != nil {
		return opts, fmt.Errorf("-start: %w", err)
	}
	if opts.config.End, err = replay.ParseTime(end, now); err != nil {
		return opts, fmt.Errorf("-end: %w", err)
	}
	if !opts.config.End.After(opts.config.Start) {
		return opts, errors.New("-end must be after -start")
	}
	if step != "" {
		if opts.config.Step, err = replay.ParseDuration(step); err != nil {
			return opts, fmt.Errorf("-step: %w", err)
		}
		if opts.config.Step <= 0 {
			return opts, errors.New("-step must be positive")
		}
		if opts.config.Step < time.Millisecond || opts.config.Step%time.Millisecond != 0 {
			return opts, errors.New("-step must be a whole number of milliseconds")
		}
	}
	if opts.lookback, err = replay.ParseDuration(lookback); err != nil {
		return opts, fmt.Errorf("-lookback: %w", err)
	}
	if opts.lookback < 0 {
		return opts, errors.New("-lookback must not be negative")
	}
	if opts.config.QPS <= 0 || math.IsNaN(opts.config.QPS) || math.IsInf(opts.config.QPS, 0) {
		return opts, errors.New("-qps must be finite and positive")
	}
	opts.config.Headers = make(http.Header)
	for _, header := range headers {
		if strings.ContainsAny(header, "\r\n") {
			return opts, fmt.Errorf("invalid -header %q: line breaks are forbidden", header)
		}
		name, value, found := strings.Cut(header, ":")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !found || !validHeaderName(name) || !validHeaderValue(value) {
			return opts, fmt.Errorf("invalid -header %q: expected Key: Value", header)
		}
		opts.config.Headers.Add(name, value)
	}
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return opts, errors.New("-rule must not be empty")
		}
		opts.config.RuleNames = append(opts.config.RuleNames, name)
	}
	return opts, nil
}

func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c) {
			continue
		}
		return false
	}
	return true
}

func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == 127 || s[i] < 32 && s[i] != '\t' {
			return false
		}
	}
	return true
}

type jsonReport struct {
	*replay.Report
	Incidents []replay.RuleIncidents `json:"incidents,omitempty"`
}

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer, now time.Time) int {
	opts, err := parseCLI(args, now, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(stderr, "alertreplay:", err)
		return 2
	}
	var incidents []replay.Incident
	if opts.incidentsPath != "" {
		incidents, err = replay.LoadIncidents(opts.incidentsPath)
		if err != nil {
			fmt.Fprintln(stderr, "alertreplay:", err)
			return 1
		}
	}
	report, err := replay.Run(ctx, opts.config, opts.files)
	if err != nil {
		fmt.Fprintln(stderr, "alertreplay:", err)
		return 1
	}
	var matches []replay.RuleIncidents
	if opts.incidentsPath != "" {
		matches = replay.MatchIncidents(report, incidents, opts.lookback)
	}
	if opts.json {
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(jsonReport{Report: report, Incidents: matches})
	} else {
		err = printReport(stdout, report, matches, opts.lookback)
	}
	if err != nil {
		fmt.Fprintln(stderr, "alertreplay: write output:", err)
		return 1
	}
	return 0
}

func printReport(out io.Writer, report *replay.Report, matches []replay.RuleIncidents, lookback time.Duration) error {
	var buffer bytes.Buffer
	w := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Replay %s → %s\n", formatTime(report.Start), formatTime(report.End))
	for _, warning := range report.Warnings {
		fmt.Fprintf(w, "WARNING: %s\n", warning)
	}
	if len(report.Rules) == 0 {
		fmt.Fprintln(w, "No alerting rules matched.")
	}
	for _, rule := range report.Rules {
		fmt.Fprintf(w, "\n%s\tfired %d times\ttotal %s\tstep %s\tcoverage start %s\n", rule.Name, len(rule.Intervals), formatDuration(rule.TotalFiringSeconds), rule.Step, coverageStart(rule.Coverage))
		fmt.Fprintf(w, "  file: %s; group: %s\n  expression: %s\n", rule.File, rule.Group, rule.Expression)
		if len(rule.Coverage) == 0 {
			fmt.Fprintln(w, "  coverage: no vector selectors")
		}
		for _, coverage := range rule.Coverage {
			if coverage.FirstSample == nil {
				fmt.Fprintf(w, "  coverage %s: unavailable / no data found\n", coverage.Selector)
			} else if !coverage.FirstSample.After(report.Start) {
				fmt.Fprintf(w, "  coverage %s: data present at range start (first sampled timestamp %s)\n", coverage.Selector, formatTime(*coverage.FirstSample))
			} else {
				fmt.Fprintf(w, "  coverage %s: no data observed before %s (range-limited sample-grid estimate)\n", coverage.Selector, formatTime(*coverage.FirstSample))
			}
		}
		if len(rule.Intervals) > 0 {
			fmt.Fprintln(w, "  START\tEND\tDURATION\tPENDING\tLABELS")
		}
		for _, interval := range rule.Intervals {
			end := "OPEN (observed through " + formatTime(interval.ObservedUntil) + ")"
			if interval.End != nil {
				end = formatTime(*interval.End)
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\n", formatTime(interval.Start), end, formatDuration(interval.DurationSeconds), formatDuration(interval.PendingSeconds), formatLabels(interval.Labels))
		}
	}
	if matches != nil {
		fmt.Fprintf(w, "\nIncidents (lookback %s; positive lead = early warning, negative = late)\n", lookback)
		fmt.Fprintln(w, "RULE [GROUP / FILE]\tINCIDENT\tCAUGHT\tLEAD TIME\tFIRST FIRE\tMATCHING FIRINGS")
		for _, row := range matches {
			for _, match := range row.Incidents {
				lead, first := "—", "—"
				if match.LeadSeconds != nil {
					lead = formatDuration(*match.LeadSeconds)
				}
				if match.FirstFire != nil {
					first = formatTime(*match.FirstFire)
				}
				fmt.Fprintf(w, "%s [%s / %s]\t%s\t%t\t%s\t%s\t%d\n", row.Name, row.Group, row.File, match.Name, match.Caught, lead, first, match.FiringCount)
			}
			fmt.Fprintf(w, "  %s [%s / %s]: %d false-positive firings (no incident window overlap)\n", row.Name, row.Group, row.File, len(row.FalsePositives))
			for _, interval := range row.FalsePositives {
				fmt.Fprintf(w, "    %s\t%s\n", formatTime(interval.Start), formatLabels(interval.Labels))
			}
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err := buffer.WriteTo(out)
	return err
}

func coverageStart(coverage []replay.Coverage) string {
	if len(coverage) == 0 {
		return "n/a"
	}
	var latest *time.Time
	for _, selector := range coverage {
		if selector.FirstSample == nil {
			return "unavailable"
		}
		if latest == nil || selector.FirstSample.After(*latest) {
			latest = selector.FirstSample
		}
	}
	return formatTime(*latest) + " (grid estimate)"
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func formatDuration(seconds float64) string {
	return (time.Duration(seconds * float64(time.Second))).String()
}
func formatLabels(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if key != "alertname" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%q", key, labels[key]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(runCLI(ctx, os.Args[1:], os.Stdout, os.Stderr, time.Now()))
}
