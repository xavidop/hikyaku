package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func at(t *testing.T, s string, loc *time.Location) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
	require.NoError(t, err)
	return v
}

func TestNext(t *testing.T) {
	for _, c := range []struct{ expr, from, want string }{
		{"* * * * *", "2026-11-12 10:00", "2026-11-12 10:01"},
		{"*/15 * * * *", "2026-11-12 10:07", "2026-11-12 10:15"},
		{"*/15 * * * *", "2026-11-12 10:45", "2026-11-12 11:00"},
		{"0 8 * * *", "2026-11-12 08:00", "2026-11-13 08:00"},
		{"0 8 * * 1-5", "2026-11-13 09:00", "2026-11-16 08:00"}, // Friday → Monday
		{"0 8 * * mon-fri", "2026-11-13 09:00", "2026-11-16 08:00"},
		{"30 9 1 * *", "2026-11-12 00:00", "2026-12-01 09:30"},
		{"0 0 1 1 *", "2026-11-12 00:00", "2027-01-01 00:00"},
		{"0 0 29 2 *", "2026-11-12 00:00", "2028-02-29 00:00"},
		{"0 12 * jan,jul *", "2026-11-12 00:00", "2027-01-01 12:00"},
		{"0 0 * * 7", "2026-11-12 00:00", "2026-11-15 00:00"}, // 7 is Sunday
		{"0 0 * * sun", "2026-11-12 00:00", "2026-11-15 00:00"},
		{"0 0 13 * fri", "2026-11-12 01:00", "2026-11-13 00:00"}, // either day matches
		{"0 0 13 * fri", "2026-11-13 01:00", "2026-11-20 00:00"},
		{"5/20 * * * *", "2026-11-12 10:06", "2026-11-12 10:25"},
		{"0 8-18/5 * * *", "2026-11-12 09:00", "2026-11-12 13:00"},
		{"@hourly", "2026-11-12 10:30", "2026-11-12 11:00"},
		{"@daily", "2026-11-12 10:30", "2026-11-13 00:00"},
		{"@weekly", "2026-11-12 10:30", "2026-11-15 00:00"},
		{"@monthly", "2026-11-12 10:30", "2026-12-01 00:00"},
		{"@yearly", "2026-11-12 10:30", "2027-01-01 00:00"},
	} {
		s, err := Parse(c.expr)
		require.NoError(t, err, c.expr)
		assert.Equal(t, at(t, c.want, time.UTC), s.Next(at(t, c.from, time.UTC)), "%s after %s", c.expr, c.from)
	}
}

func TestNextKeepsTheTimeZone(t *testing.T) {
	madrid, err := time.LoadLocation("Europe/Madrid")
	require.NoError(t, err)
	s, err := Parse("0 8 * * *")
	require.NoError(t, err)
	// 08:00 in Madrid is 07:00 UTC in winter and 06:00 UTC in summer.
	assert.Equal(t, "2026-11-13T07:00:00Z", s.Next(at(t, "2026-11-12 09:00", madrid)).UTC().Format(time.RFC3339))
	assert.Equal(t, "2026-07-02T06:00:00Z", s.Next(at(t, "2026-07-01 09:00", madrid)).UTC().Format(time.RFC3339))
}

func TestNextAroundAClockChange(t *testing.T) {
	madrid, err := time.LoadLocation("Europe/Madrid")
	require.NoError(t, err)
	s, err := Parse("30 2 * * *")
	require.NoError(t, err)
	// 2026-03-29: the clocks go from 02:00 to 03:00, so 02:30 does not exist.
	next := s.Next(at(t, "2026-03-28 12:00", madrid))
	assert.Equal(t, "2026-03-30 02:30", next.Format("2006-01-02 15:04"))
	// 2026-10-25: 02:30 comes twice, and runs once.
	first := s.Next(at(t, "2026-10-24 12:00", madrid))
	assert.Equal(t, "2026-10-25 02:30", first.Format("2006-01-02 15:04"))
	assert.Equal(t, "2026-10-26 02:30", s.Next(first).Format("2006-01-02 15:04"))

	twice, err := Parse("0,30 2 * * *")
	require.NoError(t, err)
	half := twice.Next(first.Add(-time.Minute))
	assert.Equal(t, first, half)
	assert.Equal(t, "2026-10-26 02:00", twice.Next(half).Format("2006-01-02 15:04"))

	every, err := Parse("*/30 * * * *")
	require.NoError(t, err)
	n := 0
	for tm := at(t, "2026-03-29 00:00", madrid); tm.Day() == 29; tm = every.Next(tm) {
		n++
	}
	assert.Equal(t, 46, n, "a 23 hour day has 46 half hours")
	n = 0
	for tm := at(t, "2026-10-25 00:00", madrid); tm.Day() == 25; tm = every.Next(tm) {
		n++
	}
	assert.Equal(t, 50, n, "a 25 hour day has 50 half hours")
}

func TestParseErrors(t *testing.T) {
	for expr, want := range map[string]string{
		"":             "5 fields",
		"* * * *":      "5 fields",
		"* * * * * *":  "5 fields",
		"60 * * * *":   "minute: 60 is outside 0-59",
		"* 24 * * *":   "hour: 24 is outside 0-23",
		"* * 0 * *":    "day of month: 0 is outside 1-31",
		"* * * 13 *":   "month: 13 is outside 1-12",
		"* * * * 8":    "day of week: 8 is outside 0-7",
		"*/0 * * * *":  `step "0" is not a number above 0`,
		"a * * * *":    `minute: "a" is not a value`,
		"10-5 * * * *": "goes backwards",
		"0 0 30 2 *":   "never matches",
		"0 0 31 4 *":   "never matches",
		"@reboot":      "unknown macro",
	} {
		_, err := Parse(expr)
		assert.ErrorContains(t, err, want, expr)
	}
}
