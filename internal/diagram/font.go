package diagram

import (
	"sync"

	"golang.org/x/image/font"
	"golang.org/x/image/font/gofont/goregular"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/font/sfnt"
)

// The text of a PNG is drawn with Go Regular, which the binary carries: it
// has Latin, Greek and Cyrillic letters, and the arrows. An SVG names no
// font file, so its viewer picks a sans-serif one. Nodes are sized with Go
// Regular for both, since it is a little wider than the usual choices.
var goFont = sync.OnceValues(func() (*sfnt.Font, error) { return opentype.Parse(goregular.TTF) })

// svgFonts is the font-family of an SVG.
const svgFonts = "Helvetica, Arial, sans-serif"

// A meter measures text. It is not safe for concurrent use.
type meter struct {
	font  *sfnt.Font
	scale float64
	faces map[float64]font.Face
}

func newMeter() (*meter, error) {
	f, err := goFont()
	if err != nil {
		return nil, err
	}
	return &meter{font: f, scale: 1, faces: map[float64]font.Face{}}, nil
}

// face is the font at size, times the meter's scale.
func (m *meter) face(size float64) font.Face {
	if f, ok := m.faces[size]; ok {
		return f
	}
	// NewFace fails for a bad option only, and these are fixed.
	f, _ := opentype.NewFace(m.font, &opentype.FaceOptions{Size: size * m.scale, DPI: 72, Hinting: font.HintingNone})
	m.faces[size] = f
	return f
}

// width is how wide s is at size.
func (m *meter) width(s string, size float64) float64 {
	return float64(font.MeasureString(m.face(size), s)) / 64
}
