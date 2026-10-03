package replay

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 34, 56, 123, time.UTC)
	for _, tc := range []struct {
		input string
		want  time.Time
	}{
		{"now", now}, {"now-30d", now.Add(-30 * 24 * time.Hour)}, {"now+1.5h", now.Add(90 * time.Minute)},
		{"2026-07-01", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)},
		{"2026-07-01T01:02:03.123456789+02:00", time.Date(2026, 6, 30, 23, 2, 3, 123456789, time.UTC)},
		{"0", time.Unix(0, 0)}, {"+1.125", time.Unix(1, 125000000)}, {"-0.5", time.Unix(-1, 500000000)},
		{"-1.25", time.Unix(-2, 750000000)}, {"  now  ", now},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseTime(tc.input, now)
			if err != nil || !got.Equal(tc.want) {
				t.Fatalf("got %s, %v; want %s", got, err, tc.want)
			}
		})
	}
	for _, input := range []string{"", "yesterday", "2026-02-30", "now-", "now--1h", "now+-1h", "now-1x", "1.2345678901", "NaN", "9223372036854775808"} {
		if got, err := ParseTime(input, now); err == nil {
			t.Errorf("accepted %q: %s", input, got)
		}
	}
}

func TestParseDuration(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{
		{"0", 0}, {"30d", 30 * 24 * time.Hour}, {"1w2d3h4m5s", 9*24*time.Hour + 3*time.Hour + 4*time.Minute + 5*time.Second},
		{"1.5d", 36 * time.Hour}, {"-1d2h", -26 * time.Hour}, {"+.5w", 84 * time.Hour},
		{"1d1ms1us1µs1μs1ns", 24*time.Hour + time.Millisecond + 3*time.Microsecond + time.Nanosecond},
		{"0.00000000000001d", 0}, {"1d.5h", 24*time.Hour + 30*time.Minute},
	} {
		got, err := ParseDuration(tc.input)
		if err != nil || got != tc.want {
			t.Errorf("%q: got %s, %v; want %s", tc.input, got, err, tc.want)
		}
	}
	for _, input := range []string{"", "1", "1dgarbage", "1d-1h", "1d 2h", "999999999999999999999d", "--1d", "NaNd"} {
		if d, err := ParseDuration(input); err == nil {
			t.Errorf("accepted %q: %s", input, d)
		}
	}
}

func TestAlignCeil(t *testing.T) {
	for _, tc := range []struct {
		at   time.Time
		step time.Duration
		want time.Time
	}{
		{time.Unix(0, 0), time.Minute, time.Unix(0, 0)},
		{time.Unix(60, 0), time.Minute, time.Unix(60, 0)},
		{time.Unix(60, 1), time.Minute, time.Unix(120, 0)},
		{time.Unix(-61, 0), time.Minute, time.Unix(-60, 0)},
		{time.Unix(-60, 1), time.Minute, time.Unix(0, 0)},
		{time.Unix(-1, 999999999), 100 * time.Millisecond, time.Unix(0, 0)},
		{time.Unix(0, 100000001), 100 * time.Millisecond, time.Unix(0, 200000000)},
		{time.Date(2500, 1, 1, 0, 0, 1, 0, time.UTC), time.Minute, time.Date(2500, 1, 1, 0, 1, 0, 0, time.UTC)},
		{time.Date(1000, 1, 1, 0, 0, 1, 0, time.UTC), time.Minute, time.Date(1000, 1, 1, 0, 1, 0, 0, time.UTC)},
		{time.Unix(1, 2), 0, time.Unix(1, 2)},
		{time.Unix(1, 2), -time.Minute, time.Unix(1, 2)},
	} {
		if got := AlignCeil(tc.at, tc.step); !got.Equal(tc.want) {
			t.Errorf("AlignCeil(%s,%s) = %s; want %s", tc.at, tc.step, got, tc.want)
		}
	}
}
