package transform

import (
	"fmt"

	"github.com/itchyny/gojq"
)

// JQ is a compiled jq program.
type JQ struct {
	src  string
	code *gojq.Code
}

// CompileJQ compiles a jq program.
func CompileJQ(src string) (*JQ, error) {
	q, err := gojq.Parse(src)
	if err != nil {
		return nil, fmt.Errorf("jq %q: %w", src, err)
	}
	code, err := gojq.Compile(q)
	if err != nil {
		return nil, fmt.Errorf("jq %q: %w", src, err)
	}
	return &JQ{src: src, code: code}, nil
}

// String returns the program's source.
func (q *JQ) String() string { return q.src }

// Run returns every output of the program for one input.
func (q *JQ) Run(input any) ([]any, error) {
	var out []any
	iter := q.code.Run(normalize(input))
	for {
		v, ok := iter.Next()
		if !ok {
			return out, nil
		}
		if err, isErr := v.(error); isErr {
			if h, halt := err.(*gojq.HaltError); halt && h.Value() == nil {
				return out, nil
			}
			return nil, fmt.Errorf("jq %q: %w", q.src, err)
		}
		out = append(out, v)
	}
}

// normalize converts a value into the types gojq accepts (maps of string to
// any, slices of any, float64, string, bool, nil).
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]string:
		m := make(map[string]any, len(x))
		for k, s := range x {
			m[k] = s
		}
		return m
	case []string:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = e
		}
		return s
	case int64:
		return int(x)
	}
	return v
}
