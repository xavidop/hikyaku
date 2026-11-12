// Package cron parses five-field cron expressions and finds the next time
// one matches. It is what a schedule source runs on, and what validation
// checks its cron key with.
//
// The fields are minute, hour, day of month, month and day of week. Each
// takes a star, a number, a range (1-5), a list (1,15) and a step (*/10,
// 8-18/2). Months and days of the week take names too (jan, mon), and 7 is
// Sunday like 0. When both the day of month and the day of week are
// restricted, a day that matches either one matches, as in Vixie cron.
// @hourly, @daily (@midnight), @weekly, @monthly and @yearly (@annually)
// stand for the usual expressions.
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	// Zone names must load in a container that ships no zone files.
	_ "time/tzdata"
)

// Schedule is a parsed expression.
type Schedule struct {
	minute, hour, dom, month, dow uint64 // bit i set: value i matches
	domStar, dowStar, hourStar    bool
}

var macros = map[string]string{
	"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *",
	"@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *",
}

var (
	monthNames = map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
	dayNames   = map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
)

// Parse reads a five-field expression or a macro.
func Parse(expr string) (*Schedule, error) {
	expr = strings.TrimSpace(expr)
	if m, ok := macros[strings.ToLower(expr)]; ok {
		expr = m
	} else if strings.HasPrefix(expr, "@") {
		return nil, fmt.Errorf("unknown macro %q (@hourly, @daily, @weekly, @monthly and @yearly exist)", expr)
	}
	f := strings.Fields(expr)
	if len(f) != 5 {
		return nil, fmt.Errorf("a cron expression has 5 fields (minute hour day-of-month month day-of-week), not %d", len(f))
	}
	s := &Schedule{hourStar: startsWithStar(f[1]), domStar: startsWithStar(f[2]), dowStar: startsWithStar(f[4])}
	var err error
	if s.minute, err = field("minute", f[0], 0, 59, nil); err != nil {
		return nil, err
	}
	if s.hour, err = field("hour", f[1], 0, 23, nil); err != nil {
		return nil, err
	}
	if s.dom, err = field("day of month", f[2], 1, 31, nil); err != nil {
		return nil, err
	}
	if s.month, err = field("month", f[3], 1, 12, monthNames); err != nil {
		return nil, err
	}
	if s.dow, err = field("day of week", f[4], 0, 7, dayNames); err != nil {
		return nil, err
	}
	if s.dow&(1<<7) != 0 { // 7 is Sunday too
		s.dow = s.dow&^(1<<7) | 1
	}
	if s.Next(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).IsZero() {
		return nil, errors.New("the expression never matches (a day that no month has)")
	}
	return s, nil
}

func startsWithStar(f string) bool { return strings.HasPrefix(f, "*") }

// field parses one field into a bit set.
func field(name, f string, lo, hi int, names map[string]int) (uint64, error) {
	var set uint64
	for _, part := range strings.Split(f, ",") {
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("%s: step %q is not a number above 0", name, stepText)
			}
			step = n
		}
		from, to := lo, hi
		switch {
		case rng == "*":
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err error
			if from, err = value(name, a, lo, hi, names); err != nil {
				return 0, err
			}
			if to, err = value(name, b, lo, hi, names); err != nil {
				return 0, err
			}
			if from > to {
				return 0, fmt.Errorf("%s: range %q goes backwards", name, rng)
			}
		default:
			v, err := value(name, rng, lo, hi, names)
			if err != nil {
				return 0, err
			}
			from, to = v, v
			if hasStep { // 5/15 means 5-59/15
				to = hi
			}
		}
		for v := from; v <= to; v += step {
			set |= 1 << uint(v)
		}
	}
	return set, nil
}

func value(name, s string, lo, hi int, names map[string]int) (int, error) {
	if v, ok := names[strings.ToLower(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a value", name, s)
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("%s: %d is outside %d-%d", name, v, lo, hi)
	}
	return v, nil
}

func (s *Schedule) dayMatches(t time.Time) bool {
	dom := s.dom&(1<<uint(t.Day())) != 0
	dow := s.dow&(1<<uint(t.Weekday())) != 0
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dow
	case s.dowStar:
		return dom
	}
	return dom || dow
}

// Next is the first matching minute after t, in t's time zone. It is the
// zero time when nothing matches within five years.
//
// Around a clock change: a time that the clock skips (02:30 on the night
// the clocks go forward) does not run that day, and a time that comes twice
// runs once (an expression with a star for the hour runs through both).
func (s *Schedule) Next(t time.Time) time.Time {
	loc := t.Location()
	t = t.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(5, 0, 0)
	for t.Before(limit) {
		switch {
		case s.month&(1<<uint(t.Month())) == 0:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
		case !s.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
		case s.hour&(1<<uint(t.Hour())) == 0:
			// Add, not Date: an hour the clock skips must not loop.
			t = t.Truncate(time.Hour).Add(time.Hour)
		case s.minute&(1<<uint(t.Minute())) == 0:
			t = t.Add(time.Minute)
		case !s.hourStar && repeated(t):
			t = t.Add(time.Minute)
		default:
			return t
		}
	}
	return time.Time{}
}

// repeated reports whether t is the second time its wall clock time comes
// around: the hour after the clocks went back.
func repeated(t time.Time) bool {
	before := t.Add(-time.Hour)
	_, off := t.Zone()
	_, offBefore := before.Zone()
	return offBefore > off && before.Hour() == t.Hour()
}
