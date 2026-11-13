package diagram

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"image/color"
	"io"
	"strconv"
	"strings"
)

// SVG writes the diagram as an SVG document.
func (d Diagram) SVG(w io.Writer) error {
	s, err := d.build()
	if err != nil {
		return err
	}
	b := &bytes.Buffer{}
	fmt.Fprintf(b, `<svg xmlns="http://www.w3.org/2000/svg" width="%s" height="%s" viewBox="0 0 %s %s" role="img" font-family="%s" text-anchor="middle">`+"\n",
		num(s.w), num(s.h), num(s.w), num(s.h), svgFonts)
	if s.title != "" {
		fmt.Fprintf(b, "  <title>%s</title>\n", escape(s.title))
	}
	for _, it := range s.items {
		if it.text != "" {
			fmt.Fprintf(b, `  <text x="%s" y="%s" font-size="%s" fill="%s">%s</text>`+"\n",
				num(it.x), num(it.y), num(it.size), hex(it.fill), escape(it.text))
			continue
		}
		fmt.Fprintf(b, `  <path d="%s" fill="%s"`, it.path.svg(), hex(it.fill))
		if it.stroke != none {
			fmt.Fprintf(b, ` stroke="%s" stroke-width="%s"`, hex(it.stroke), num(it.width))
		}
		if it.dashed {
			fmt.Fprintf(b, ` stroke-dasharray="%s %s"`, num(dashOn), num(dashOff))
		}
		b.WriteString("/>\n")
	}
	b.WriteString("</svg>\n")
	_, err = w.Write(b.Bytes())
	return err
}

// The dashes of a dashed edge.
const dashOn, dashOff = 5.0, 4.0

func (p path) svg() string {
	var b strings.Builder
	for i, s := range p {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteByte(s.op)
		n := 2
		switch s.op {
		case 'C':
			n = 6
		case 'Z':
			n = 0
		}
		for j := range n {
			if j > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(num(s.pts[j]))
		}
	}
	return b.String()
}

// num prints a coordinate with at most two decimals.
func num(f float64) string {
	s := strconv.FormatFloat(f, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "-0" {
		return "0"
	}
	return s
}

func hex(c color.RGBA) string {
	if c == none {
		return "none"
	}
	return fmt.Sprintf("#%02x%02x%02x", c.R, c.G, c.B)
}

func escape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
