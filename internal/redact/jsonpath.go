package redact

import (
	"fmt"
	"strconv"
	"strings"
)

// Redacted replaces the values that JSONPath redaction removes.
const Redacted = "[redacted]"

// segment is one step of a JSONPath.
type segment struct {
	field     string // a member name; "" with index < 0 and !wild means none
	index     int    // an array index, or -1
	wild      bool   // * : every member or element
	recursive bool   // ..: this step matches at any depth
}

// Path is a compiled JSONPath of the subset hikyaku supports: $.a.b,
// $.a[0], $.a[*].b, $['odd key'], $..email (any depth).
type Path struct {
	raw  string
	segs []segment
}

func (p Path) String() string { return p.raw }

// CompilePath parses a JSONPath.
func CompilePath(raw string) (Path, error) {
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "$") {
		return Path{}, fmt.Errorf("jsonpath %q must start with $", raw)
	}
	s = s[1:]
	var segs []segment
	for len(s) > 0 {
		recursive := false
		switch {
		case strings.HasPrefix(s, ".."):
			recursive = true
			s = s[2:]
		case s[0] == '.':
			s = s[1:]
		case s[0] == '[':
		default:
			return Path{}, fmt.Errorf("jsonpath %q: unexpected %q", raw, s)
		}
		if len(s) == 0 {
			return Path{}, fmt.Errorf("jsonpath %q ends with a dot", raw)
		}
		if s[0] == '[' {
			end := strings.IndexByte(s, ']')
			if end < 0 {
				return Path{}, fmt.Errorf("jsonpath %q: unclosed [", raw)
			}
			inner := strings.TrimSpace(s[1:end])
			s = s[end+1:]
			switch {
			case inner == "*":
				segs = append(segs, segment{index: -1, wild: true, recursive: recursive})
			case len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0]:
				segs = append(segs, segment{field: inner[1 : len(inner)-1], index: -1, recursive: recursive})
			default:
				n, err := strconv.Atoi(inner)
				if err != nil || n < 0 {
					return Path{}, fmt.Errorf("jsonpath %q: [%s] is not an index, * or a quoted name", raw, inner)
				}
				segs = append(segs, segment{index: n, recursive: recursive})
			}
			continue
		}
		end := strings.IndexAny(s, ".[")
		if end < 0 {
			end = len(s)
		}
		name := s[:end]
		s = s[end:]
		if name == "*" {
			segs = append(segs, segment{index: -1, wild: true, recursive: recursive})
		} else {
			segs = append(segs, segment{field: name, index: -1, recursive: recursive})
		}
	}
	if len(segs) == 0 {
		return Path{}, fmt.Errorf("jsonpath %q selects the whole document; name a field", raw)
	}
	return Path{raw: raw, segs: segs}, nil
}

// CompilePaths parses every path, reporting the first bad one.
func CompilePaths(raw []string) ([]Path, error) {
	out := make([]Path, 0, len(raw))
	for _, r := range raw {
		p, err := CompilePath(r)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// JSON replaces, in place, every value the paths select in a parsed JSON
// document (maps and slices from encoding/json) with Redacted, and returns
// how many values it replaced.
func JSON(doc any, paths []Path) int {
	n := 0
	for _, p := range paths {
		n += apply(doc, p.segs)
	}
	return n
}

func apply(node any, segs []segment) int {
	if len(segs) == 0 {
		return 0
	}
	seg, rest := segs[0], segs[1:]
	n := 0
	if seg.recursive {
		// The step may match here or at any depth below.
		n += apply(node, append([]segment{{field: seg.field, index: seg.index, wild: seg.wild}}, rest...))
		switch v := node.(type) {
		case map[string]any:
			for _, child := range v {
				n += apply(child, segs)
			}
		case []any:
			for _, child := range v {
				n += apply(child, segs)
			}
		}
		return n
	}
	set := func(get func() any, put func(any)) {
		if len(rest) == 0 {
			put(Redacted)
			n++
			return
		}
		n += apply(get(), rest)
	}
	switch v := node.(type) {
	case map[string]any:
		if seg.wild {
			for k := range v {
				set(func() any { return v[k] }, func(x any) { v[k] = x })
			}
		} else if seg.field != "" {
			if _, ok := v[seg.field]; ok {
				set(func() any { return v[seg.field] }, func(x any) { v[seg.field] = x })
			}
		}
	case []any:
		switch {
		case seg.wild:
			for i := range v {
				set(func() any { return v[i] }, func(x any) { v[i] = x })
			}
		case seg.index >= 0 && seg.index < len(v):
			i := seg.index
			set(func() any { return v[i] }, func(x any) { v[i] = x })
		}
	}
	return n
}
