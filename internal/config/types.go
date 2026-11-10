package config

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/invopop/jsonschema"
	"go.yaml.in/yaml/v3"
)

// Duration is a time.Duration written as a Go duration ("500ms", "1h30m")
// with days allowed ("7d", "1d12h").
type Duration time.Duration

// Std returns the duration as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// String formats the duration as time.Duration does.
func (d Duration) String() string { return time.Duration(d).String() }

var daysRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)d(.*)$`)

// ParseDuration parses a Go duration with an optional leading day count.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	var total time.Duration
	if m := daysRe.FindStringSubmatch(s); m != nil {
		days, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		total = time.Duration(days * float64(24*time.Hour))
		s = m[2]
		if s == "" {
			return total, nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (use 500ms, 30s, 5m, 1h, 7d)", s)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid duration %q: negative", s)
	}
	return total + d, nil
}

// UnmarshalYAML reads a duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return typeErr(n, "a duration is a string such as 5s")
	}
	v, err := ParseDuration(n.Value)
	if err != nil {
		return typeErr(n, err.Error())
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML writes the duration as a string.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// UnmarshalJSON reads a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("a duration is a string such as 5s")
	}
	v, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON writes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// JSONSchema describes a duration.
func (Duration) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Pattern:     `^(\d+(\.\d+)?d)?(\d+(\.\d+)?(ns|us|µs|ms|s|m|h))*$`,
		Description: "A duration: 500ms, 30s, 5m, 1h, 7d.",
	}
}

// Size is a byte count written as "8KiB", "10MB" or a plain number.
type Size int64

var sizeRe = regexp.MustCompile(`^(?i)\s*(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|kib|mib|gib|k|m|g)?\s*$`)

// ParseSize parses a byte count.
func ParseSize(s string) (int64, error) {
	m := sizeRe.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("invalid size %q (use 512, 8KiB, 10MB)", s)
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	mult := map[string]float64{
		"": 1, "b": 1,
		"kb": 1e3, "mb": 1e6, "gb": 1e9,
		"kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30,
		"k": 1 << 10, "m": 1 << 20, "g": 1 << 30,
	}[strings.ToLower(m[2])]
	v := n * mult
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}
	return int64(v), nil
}

// UnmarshalYAML reads a size.
func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return typeErr(n, "a size is a number or a string such as 8KiB")
	}
	v, err := ParseSize(n.Value)
	if err != nil {
		return typeErr(n, err.Error())
	}
	*s = Size(v)
	return nil
}

// MarshalYAML writes the size as a number of bytes.
func (s Size) MarshalYAML() (any, error) { return int64(s), nil }

// JSONSchema describes a size.
func (Size) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		OneOf: []*jsonschema.Schema{
			{Type: "integer", Minimum: json.Number("0")},
			{Type: "string", Pattern: `^(?i)\d+(\.\d+)?\s*(b|kb|mb|gb|kib|mib|gib|k|m|g)?$`},
		},
		Description: "A byte count: 512, 8KiB, 10MB.",
	}
}

// Percent is a share in [0, 1], written as "50%" or 0.5.
type Percent float64

// ParsePercent parses "50%" or a fraction.
func ParsePercent(s string) (float64, error) {
	s = strings.TrimSpace(s)
	pct := strings.HasSuffix(s, "%")
	v, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid share %q (use 50%% or 0.5)", s)
	}
	if pct {
		v /= 100
	}
	if v < 0 || v > 1 {
		return 0, fmt.Errorf("invalid share %q: must be within 0%%..100%%", s)
	}
	return v, nil
}

// UnmarshalYAML reads a share.
func (p *Percent) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return typeErr(n, "a share is 50% or 0.5")
	}
	v, err := ParsePercent(n.Value)
	if err != nil {
		return typeErr(n, err.Error())
	}
	*p = Percent(v)
	return nil
}

// MarshalYAML writes the share as a percentage.
func (p Percent) MarshalYAML() (any, error) {
	return strconv.FormatFloat(float64(p)*100, 'f', -1, 64) + "%", nil
}

// JSONSchema describes a share.
func (Percent) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		OneOf: []*jsonschema.Schema{
			{Type: "number", Minimum: json.Number("0"), Maximum: json.Number("1")},
			{Type: "string", Pattern: `^\d+(\.\d+)?%$`},
		},
		Description: "A share: 50% or 0.5.",
	}
}

// typeErr reports a bad value the way yaml reports type errors, so decoding
// goes on and every problem of a file is reported at once.
func typeErr(n *yaml.Node, msg string) error {
	return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: %s", n.Line, msg)}}
}

// TagList is a list of error tags (5xx, 429, connect, ws:close_1011). A bare
// number such as 429 is a tag too.
type TagList []string

// JSONSchema describes a tag list.
func (TagList) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "array",
		Items:       &jsonschema.Schema{OneOf: []*jsonschema.Schema{{Type: "string"}, {Type: "integer"}}},
		Description: "Error tags: classes (connect, timeout), ranges (5xx, 4xx), exact codes (503, or a bare number) and protocol codes (ws:close_1011).",
	}
}

// Bool returns *b, or def when b is nil.
func Bool(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}

// Ptr returns a pointer to v.
func Ptr[T any](v T) *T { return &v }
