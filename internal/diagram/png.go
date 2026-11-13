package diagram

import (
	"image"
	"image/color"
	"image/png"
	"io"
	"math"
	"slices"

	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"
	"golang.org/x/image/vector"
)

const (
	// pngScale is how many pixels an SVG unit takes: two, so that the image
	// stays sharp on a dense screen.
	pngScale = 2.0
	// maxPixels bounds the image. A diagram that would be larger is drawn
	// at a smaller scale.
	maxPixels = 32e6
)

// PNG writes the diagram as a PNG image.
func (d Diagram) PNG(w io.Writer) error {
	s, err := d.build()
	if err != nil {
		return err
	}
	m, err := newMeter()
	if err != nil {
		return err
	}
	m.scale = scaleFor(s.w, s.h)
	c := &canvas{
		img:   image.NewRGBA(image.Rect(0, 0, int(math.Ceil(s.w*m.scale)), int(math.Ceil(s.h*m.scale)))),
		scale: m.scale,
	}
	for _, it := range s.items {
		if it.text != "" {
			face := m.face(it.size)
			at := fixed.Point26_6{
				X: fixed.Int26_6(it.x*c.scale*64) - font.MeasureString(face, it.text)/2,
				Y: fixed.Int26_6(it.y * c.scale * 64),
			}
			(&font.Drawer{Dst: c.img, Src: image.NewUniform(it.fill), Face: face, Dot: at}).DrawString(it.text)
			continue
		}
		lines := it.path.flatten(c.scale)
		if it.fill != none {
			shapes := make([][]pt, len(lines))
			for i, l := range lines {
				shapes[i] = l.pts
			}
			c.fill(shapes, it.fill)
		}
		if it.stroke != none {
			var shapes [][]pt
			for _, l := range lines {
				pieces := [][]pt{l.pts}
				switch {
				case it.dashed:
					pieces = dash(l.pts, dashOn*c.scale, dashOff*c.scale)
				case l.closed:
					// Around once more, so that the first corner is joined too.
					pieces = [][]pt{append(slices.Clone(l.pts), l.pts[0], l.pts[1])}
				}
				for _, piece := range pieces {
					shapes = append(shapes, outline(piece, it.width*c.scale/2)...)
				}
			}
			c.fill(shapes, it.stroke)
		}
	}
	return png.Encode(w, c.img)
}

// scaleFor is the scale of the PNG of a scene w wide and h high.
func scaleFor(w, h float64) float64 {
	if w*h*pngScale*pngScale > maxPixels {
		return math.Sqrt(maxPixels / (w * h))
	}
	return pngScale
}

type pt struct{ x, y float64 }

// A polyline is a path in pixels, with its curves cut into lines.
type polyline struct {
	pts    []pt
	closed bool
}

func (p path) flatten(scale float64) []polyline {
	var out []polyline
	var cur polyline
	end := func() {
		if len(cur.pts) > 1 {
			out = append(out, cur)
		}
		cur = polyline{}
	}
	at := func(s seg, i int) pt { return pt{s.pts[i] * scale, s.pts[i+1] * scale} }
	for _, s := range p {
		switch s.op {
		case 'M':
			end()
			cur.pts = append(cur.pts, at(s, 0))
		case 'L':
			cur.pts = append(cur.pts, at(s, 0))
		case 'C':
			a, b, c, d := cur.pts[len(cur.pts)-1], at(s, 0), at(s, 2), at(s, 4)
			// One line for every three pixels of the curve, more or less.
			n := int(math.Min(math.Max((dist(a, b)+dist(b, c)+dist(c, d))/3, 4), 96))
			for i := 1; i <= n; i++ {
				t := float64(i) / float64(n)
				u := 1 - t
				cur.pts = append(cur.pts, pt{
					u*u*u*a.x + 3*u*u*t*b.x + 3*u*t*t*c.x + t*t*t*d.x,
					u*u*u*a.y + 3*u*u*t*b.y + 3*u*t*t*c.y + t*t*t*d.y,
				})
			}
		case 'Z':
			cur.closed = true
			end()
		}
	}
	end()
	return out
}

func dist(a, b pt) float64 { return math.Hypot(b.x-a.x, b.y-a.y) }

// dash cuts a line into its drawn pieces: on pixels drawn, off pixels not.
func dash(pts []pt, on, off float64) [][]pt {
	var out [][]pt
	cur := []pt{pts[0]}
	left, drawing := on, true
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		rest := dist(a, b)
		for rest > left {
			t := left / rest
			a = pt{a.x + (b.x-a.x)*t, a.y + (b.y-a.y)*t}
			rest -= left
			if drawing {
				out = append(out, append(cur, a))
				cur = nil
			} else {
				cur = []pt{a}
			}
			drawing = !drawing
			left = off
			if drawing {
				left = on
			}
		}
		left -= rest
		if drawing {
			cur = append(cur, b)
		}
	}
	if drawing && len(cur) > 1 {
		out = append(out, cur)
	}
	return out
}

// outline is a line of some width as shapes to fill: a rectangle for every
// piece, and a small disc where two pieces meet. They all turn the same
// way, so the ones that overlap fill as one.
func outline(pts []pt, half float64) [][]pt {
	var out [][]pt
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		l := dist(a, b)
		if l < 1e-9 {
			continue
		}
		nx, ny := -(b.y-a.y)/l*half, (b.x-a.x)/l*half
		out = append(out, []pt{{a.x + nx, a.y + ny}, {b.x + nx, b.y + ny}, {b.x - nx, b.y - ny}, {a.x - nx, a.y - ny}})
		if i == len(pts)-1 {
			break
		}
		const sides = 10
		disc := make([]pt, sides)
		for k := range disc {
			sin, cos := math.Sincos(2 * math.Pi * float64(k) / sides)
			disc[k] = pt{b.x + half*cos, b.y - half*sin}
		}
		out = append(out, disc)
	}
	return out
}

type canvas struct {
	img   *image.RGBA
	scale float64
	ras   vector.Rasterizer
}

// fill paints the shapes in one color. The rasterizer only covers the
// pixels around them, not the whole image.
func (c *canvas) fill(shapes [][]pt, col color.RGBA) {
	lo, hi := pt{math.Inf(1), math.Inf(1)}, pt{math.Inf(-1), math.Inf(-1)}
	for _, s := range shapes {
		for _, p := range s {
			lo = pt{math.Min(lo.x, p.x), math.Min(lo.y, p.y)}
			hi = pt{math.Max(hi.x, p.x), math.Max(hi.y, p.y)}
		}
	}
	if lo.x > hi.x {
		return
	}
	r := image.Rect(int(math.Floor(lo.x))-1, int(math.Floor(lo.y))-1, int(math.Ceil(hi.x))+1, int(math.Ceil(hi.y))+1).
		Intersect(c.img.Bounds())
	if r.Empty() {
		return
	}
	c.ras.Reset(r.Dx(), r.Dy())
	ox, oy := float64(r.Min.X), float64(r.Min.Y)
	for _, s := range shapes {
		c.ras.MoveTo(float32(s[0].x-ox), float32(s[0].y-oy))
		for _, p := range s[1:] {
			c.ras.LineTo(float32(p.x-ox), float32(p.y-oy))
		}
		c.ras.ClosePath()
	}
	c.ras.Draw(c.img, r, image.NewUniform(col), image.Point{})
}
