package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMatchIncidents(t *testing.T) {
	at := func(minute int) time.Time { return time.Unix(0, 0).Add(time.Duration(minute) * time.Minute) }
	closed := func(start, end int) Interval {
		endTime := at(end)
		return Interval{Start: at(start), End: &endTime, ObservedUntil: at(end)}
	}
	report := &Report{Rules: []RuleResult{{Name: "Early", File: "rules.yml", Group: "group", Intervals: []Interval{
		closed(-20, -10), // resolved exactly at lookback start: not a catch
		closed(-15, -5),  // first overlapping fire
		closed(-3, 5),
		closed(20, 21),                         // starts exactly at closed incident end: catch
		closed(21, 22),                         // false positive
		{Start: at(9), ObservedUntil: at(10)},  // open bounded before next incident
		{Start: at(30), ObservedUntil: at(30)}, // known firing point despite zero duration
	}}}}
	incidents := []Incident{{Name: "outage", Start: at(0), End: at(20)}, {Name: "later", Start: at(30), End: at(40)}, {Name: "missed", Start: at(60), End: at(70)}}
	rows := MatchIncidents(report, incidents, 10*time.Minute)
	if len(rows) != 1 || rows[0].Name != "Early" || rows[0].File != "rules.yml" || rows[0].Group != "group" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	first := rows[0].Incidents[0]
	if !first.Caught || first.FiringCount != 4 || !first.FirstFire.Equal(at(-15)) || *first.LeadSeconds != 900 {
		t.Errorf("first match: %+v", first)
	}
	later := rows[0].Incidents[1]
	if !later.Caught || later.FiringCount != 3 || !later.FirstFire.Equal(at(20)) || *later.LeadSeconds != 600 {
		t.Errorf("later match: %+v", later)
	}
	missed := rows[0].Incidents[2]
	if missed.Caught || missed.FirstFire != nil || missed.LeadSeconds != nil || missed.FiringCount != 0 {
		t.Errorf("missed: %+v", missed)
	}
	if len(rows[0].FalsePositives) != 1 || !rows[0].FalsePositives[0].Start.Equal(at(-20)) {
		t.Errorf("false positives: %+v", rows[0].FalsePositives)
	}
	if got := MatchIncidents(nil, incidents, 0); len(got) != 0 {
		t.Errorf("nil report: %+v", got)
	}
}

func TestIncidentBoundariesAndLateLead(t *testing.T) {
	at := func(seconds int64) time.Time { return time.Unix(seconds, 0) }
	for _, tc := range []struct {
		name     string
		interval Interval
		caught   bool
		lead     float64
	}{
		{"resolved-at-start", Interval{Start: at(1), End: ptrTime(at(10))}, false, 0},
		{"starts-at-end", Interval{Start: at(20), End: ptrTime(at(21))}, true, -10},
		{"open-point-at-start", Interval{Start: at(10), ObservedUntil: at(10)}, true, 0},
		{"open-end-at-start", Interval{Start: at(5), ObservedUntil: at(10)}, true, 5},
		{"open-end-before-start", Interval{Start: at(5), ObservedUntil: at(9)}, false, 0},
		{"empty-closed", Interval{Start: at(10), End: ptrTime(at(10))}, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := MatchIncidents(&Report{Rules: []RuleResult{{Intervals: []Interval{tc.interval}}}}, []Incident{{Name: "x", Start: at(10), End: at(20)}}, 0)
			match := rows[0].Incidents[0]
			if match.Caught != tc.caught {
				t.Fatalf("caught=%v; want %v", match.Caught, tc.caught)
			}
			if tc.caught && *match.LeadSeconds != tc.lead {
				t.Errorf("lead=%v; want %v", *match.LeadSeconds, tc.lead)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func TestLoadIncidents(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		valid      bool
	}{
		{"valid", "- name: outage\n  start: 2026-09-15T22:07:00Z\n  end: 2026-09-16T00:30:00Z\n", true},
		{"date", "- name: outage\n  start: 2026-09-15\n  end: 2026-09-16\n", true},
		{"empty-list", "[]", true},
		{"null", "null", false},
		{"unknown", "- name: outage\n  start: 2026-09-15\n  end: 2026-09-16\n  typo: 1\n", false},
		{"no-name", "- start: 2026-09-15\n  end: 2026-09-16\n", false},
		{"duplicate", "- name: outage\n  start: 2026-09-15\n  end: 2026-09-16\n- name: outage\n  start: 2026-09-15\n  end: 2026-09-16\n", false},
		{"reversed", "- name: outage\n  start: 2026-09-16\n  end: 2026-09-15\n", false},
		{"missing-start", "- name: outage\n  end: 2026-09-15\n", false},
		{"relative", "- name: outage\n  start: now-1d\n  end: now\n", false},
		{"extra-doc", "[]\n---\n[]", false},
		{"mapping", "incidents: []", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "incidents.yaml")
			if err := os.WriteFile(file, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := LoadIncidents(file)
			if (err == nil) != tc.valid {
				t.Errorf("incidents=%+v err=%v; valid=%v", got, err, tc.valid)
			}
		})
	}
	if _, err := LoadIncidents(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("accepted missing file")
	}
}
