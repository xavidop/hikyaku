package config

import (
	"fmt"
	"strings"
)

// Error is one problem in a spec: where (file, line, path) and what.
type Error struct {
	File string
	Line int
	// Path locates the problem in the spec, such as
	// route "smart-triage": router.candidates[0].name.
	Path string
	Msg  string
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.File != "" {
		b.WriteString(e.File)
		if e.Line > 0 {
			fmt.Fprintf(&b, ":%d", e.Line)
		}
		b.WriteString(": ")
	} else if e.Line > 0 {
		fmt.Fprintf(&b, "line %d: ", e.Line)
	}
	if e.Path != "" {
		b.WriteString(e.Path)
		b.WriteString(": ")
	}
	b.WriteString(e.Msg)
	return b.String()
}

// Errors is every problem found; validation reports them all at once.
type Errors []*Error

func (es Errors) Error() string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "\n")
}

// Add records a problem at path.
func (es *Errors) Add(path, format string, args ...any) {
	*es = append(*es, &Error{Path: path, Msg: fmt.Sprintf(format, args...)})
}

// Err returns es as an error, or nil when there are none.
func (es Errors) Err() error {
	if len(es) == 0 {
		return nil
	}
	return es
}

// Warning is a lint finding: valid, but probably not what you want.
type Warning struct {
	Path string
	Msg  string
}

func (w Warning) String() string {
	if w.Path == "" {
		return w.Msg
	}
	return w.Path + ": " + w.Msg
}
