package cli

import (
	"io"
	"os"
)

// Style colours terminal output; the zero Style prints plain text.
type Style struct{ on bool }

// NewStyle returns a style that colours when on.
func NewStyle(on bool) Style { return Style{on: on} }

// Enabled reports whether the style colours.
func (s Style) Enabled() bool { return s.on }

func (s Style) wrap(code, text string) string {
	if !s.on {
		return text
	}
	return "\x1b[" + code + "m" + text + "\x1b[0m"
}

// Bold is for headings.
func (s Style) Bold(t string) string { return s.wrap("1", t) }

// Info is for names: commands, flags, destinations.
func (s Style) Info(t string) string { return s.wrap("36", t) }

// OK is for passed checks.
func (s Style) OK(t string) string { return s.wrap("32", t) }

// Fail is for errors and failed checks.
func (s Style) Fail(t string) string { return s.wrap("31", t) }

// Warn is for warnings.
func (s Style) Warn(t string) string { return s.wrap("33", t) }

// Dim is for details.
func (s Style) Dim(t string) string { return s.wrap("2", t) }

// ColorEnabled decides --color: always, never, or auto (a terminal, unless
// NO_COLOR is set).
func ColorEnabled(mode string, w io.Writer, lookupEnv func(string) (string, bool)) bool {
	switch mode {
	case "always":
		return true
	case "never":
		return false
	}
	if lookupEnv != nil {
		if _, ok := lookupEnv("NO_COLOR"); ok {
			return false
		}
		if v, ok := lookupEnv("TERM"); ok && v == "dumb" {
			return false
		}
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
