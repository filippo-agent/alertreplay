package replay

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Incident is a known incident's closed time window.
type Incident struct {
	Name  string    `json:"name" yaml:"name"`
	Start time.Time `json:"start" yaml:"start"`
	End   time.Time `json:"end" yaml:"end"`
}

type IncidentMatch struct {
	Incident
	Caught      bool       `json:"caught"`
	FirstFire   *time.Time `json:"first_fire,omitempty"`
	LeadSeconds *float64   `json:"lead_seconds,omitempty"`
	FiringCount int        `json:"firing_count"`
}

type RuleIncidents struct {
	Name           string          `json:"name"`
	File           string          `json:"file"`
	Group          string          `json:"group"`
	Incidents      []IncidentMatch `json:"incidents"`
	FalsePositives []Interval      `json:"false_positives"`
}

// LoadIncidents loads the documented top-level YAML sequence, rejecting unknown
// fields, duplicate names, reversed windows, and multiple YAML documents.
func LoadIncidents(path string) ([]Incident, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read incidents: %w", err)
	}
	defer f.Close()
	var entries []struct {
		Name  string `yaml:"name"`
		Start string `yaml:"start"`
		End   string `yaml:"end"`
	}
	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)
	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("parse incidents: %w", err)
	}
	if entries == nil {
		return nil, fmt.Errorf("incidents file must contain a top-level YAML sequence (use [] for no incidents)")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("incidents file must contain one YAML document")
		}
		return nil, fmt.Errorf("parse incidents: %w", err)
	}
	incidents := make([]Incident, 0, len(entries))
	seen := make(map[string]bool)
	for i, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" || seen[name] {
			return nil, fmt.Errorf("incident %d must have a nonempty, unique name", i+1)
		}
		seen[name] = true
		// Historical incident windows should not change when the file is read.
		if strings.HasPrefix(strings.TrimSpace(entry.Start), "now") || strings.HasPrefix(strings.TrimSpace(entry.End), "now") {
			return nil, fmt.Errorf("incident %q must use absolute timestamps", name)
		}
		start, err := ParseTime(entry.Start, time.Time{})
		if err != nil {
			return nil, fmt.Errorf("incident %q start: %w", name, err)
		}
		end, err := ParseTime(entry.End, time.Time{})
		if err != nil {
			return nil, fmt.Errorf("incident %q end: %w", name, err)
		}
		if end.Before(start) {
			return nil, fmt.Errorf("incident %q ends before it starts", name)
		}
		incidents = append(incidents, Incident{Name: name, Start: start, End: end})
	}
	return incidents, nil
}

// MatchIncidents matches half-open firing intervals to each closed incident
// window [start-lookback,end]. Open firings include their final known firing
// point, ObservedUntil, but never the unobserved future. LeadSeconds is positive for early
// warning and negative if the first matching firing starts after the incident.
func MatchIncidents(report *Report, incidents []Incident, lookback time.Duration) []RuleIncidents {
	result := make([]RuleIncidents, 0)
	if report == nil {
		return result
	}
	for _, rule := range report.Rules {
		row := RuleIncidents{Name: rule.Name, File: rule.File, Group: rule.Group, Incidents: make([]IncidentMatch, 0, len(incidents)), FalsePositives: make([]Interval, 0)}
		matched := make([]bool, len(rule.Intervals))
		for _, incident := range incidents {
			match := IncidentMatch{Incident: incident}
			windowStart := incident.Start.Add(-lookback)
			for i, interval := range rule.Intervals {
				end := interval.ObservedUntil
				if interval.End != nil {
					end = *interval.End
				}
				overlaps := end.After(interval.Start) && end.After(windowStart)
				if interval.End == nil {
					overlaps = !end.Before(interval.Start) && !end.Before(windowStart)
				}
				if !overlaps || interval.Start.After(incident.End) {
					continue
				}
				matched[i] = true
				match.Caught = true
				match.FiringCount++
				if match.FirstFire == nil || interval.Start.Before(*match.FirstFire) {
					first := interval.Start
					lead := incident.Start.Sub(first).Seconds()
					match.FirstFire, match.LeadSeconds = &first, &lead
				}
			}
			row.Incidents = append(row.Incidents, match)
		}
		for i, interval := range rule.Intervals {
			if !matched[i] {
				row.FalsePositives = append(row.FalsePositives, interval)
			}
		}
		result = append(result, row)
	}
	return result
}
