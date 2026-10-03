package replay

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var durationPart = regexp.MustCompile(`^(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h|d|w)`)
var unixTime = regexp.MustCompile(`^[+-]?\d+(?:\.\d{1,9})?$`)

// ParseDuration accepts Go durations plus days (24h) and weeks (7d).
func ParseDuration(s string) (time.Duration, error) {
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	original := s
	sign := int64(1)
	if strings.HasPrefix(s, "-") {
		sign, s = -1, s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	if s == "" {
		return 0, fmt.Errorf("invalid duration %q", original)
	}
	units := map[string]int64{"ns": 1, "us": 1000, "µs": 1000, "μs": 1000, "ms": 1000000, "s": int64(time.Second), "m": int64(time.Minute), "h": int64(time.Hour), "d": int64(24 * time.Hour), "w": int64(7 * 24 * time.Hour)}
	total := new(big.Rat)
	for s != "" {
		part := durationPart.FindStringSubmatch(s)
		if part == nil {
			return 0, fmt.Errorf("invalid duration %q", original)
		}
		value, ok := new(big.Rat).SetString(part[1])
		if !ok {
			return 0, fmt.Errorf("invalid duration %q", original)
		}
		value.Mul(value, new(big.Rat).SetInt64(units[part[2]]))
		total.Add(total, value)
		s = s[len(part[0]):]
	}
	total.Mul(total, new(big.Rat).SetInt64(sign))
	nanos := new(big.Int).Quo(total.Num(), total.Denom())
	if !nanos.IsInt64() {
		return 0, fmt.Errorf("duration %q overflows time.Duration", original)
	}
	return time.Duration(nanos.Int64()), nil
}

// ParseTime parses RFC3339, YYYY-MM-DD (UTC), Unix seconds (optionally
// fractional), now, and now-/now+ duration offsets. All relative values use the
// caller's same reference instant, avoiding drift between start and end.
func ParseTime(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "now" {
		return now.UTC(), nil
	}
	if strings.HasPrefix(s, "now-") || strings.HasPrefix(s, "now+") {
		duration := s[4:]
		if duration == "" || duration[0] == '-' || duration[0] == '+' {
			return time.Time{}, fmt.Errorf("invalid relative time %q", s)
		}
		d, err := ParseDuration(duration)
		if err != nil || d < 0 {
			return time.Time{}, fmt.Errorf("invalid relative time %q", s)
		}
		if s[3] == '-' {
			d = -d
		}
		return now.Add(d).UTC(), nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	if unixTime.MatchString(s) {
		negative := strings.HasPrefix(s, "-")
		parts := strings.SplitN(strings.TrimLeft(s, "+-"), ".", 2)
		secondsText := parts[0]
		if negative {
			secondsText = "-" + secondsText
		}
		seconds, err := strconv.ParseInt(secondsText, 10, 64)
		if err == nil {
			var nanos int64
			if len(parts) == 2 {
				nanos, _ = strconv.ParseInt(parts[1]+strings.Repeat("0", 9-len(parts[1])), 10, 64)
				if negative {
					nanos = -nanos
				}
			}
			return time.Unix(seconds, nanos).UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time %q: use RFC3339, YYYY-MM-DD, Unix seconds, now, or now-30d", s)
}

// AlignCeil returns the first Unix-epoch-aligned instant at or after t. A
// nonpositive step leaves t unchanged. Integer arithmetic supports dates outside
// time.Time.UnixNano's limited representable range, and dates before the epoch.
func AlignCeil(t time.Time, step time.Duration) time.Time {
	if step <= 0 {
		return t
	}
	billion := big.NewInt(int64(time.Second))
	nanos := new(big.Int).Mul(big.NewInt(t.Unix()), billion)
	nanos.Add(nanos, big.NewInt(int64(t.Nanosecond())))
	width := big.NewInt(int64(step))
	remainder := new(big.Int).Mod(nanos, width)
	if remainder.Sign() != 0 {
		nanos.Add(nanos, new(big.Int).Sub(width, remainder))
	}
	seconds, fraction := new(big.Int), new(big.Int)
	seconds.DivMod(nanos, billion, fraction)
	return time.Unix(seconds.Int64(), fraction.Int64()).In(t.Location())
}
