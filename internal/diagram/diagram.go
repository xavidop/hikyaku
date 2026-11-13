// Package diagram draws a small directed graph as a picture: an SVG or a
// PNG, left to right, with no outside tool. hikyaku graph uses it.
//
// The layout is the usual layered one. Every node gets a column, an edge
// that crosses a column gets a bend there, the columns are ordered to cross
// few edges, and the nodes are moved next to their neighbors. Both formats
// are written from the same scene, so they show the same picture.
package diagram

import (
	"image/color"
	"math"
)

// Shape is the outline of a node.
type Shape int

// The shapes follow hikyaku graph's Mermaid output.
const (
	Box      Shape = iota // a destination
	Stadium               // a source
	Hexagon               // a route
	Cylinder              // the dead-letter queue
)

// Node is one box of the diagram.
type Node struct {
	ID    string
	Shape Shape
	// Rank is the leftmost column the node may stand in. An edge moves its
	// target further right.
	Rank int
	// Title is the first line and Detail, when set, a smaller second one.
	Title, Detail string
}

// Edge is an arrow between two nodes. An edge that names a node the diagram
// does not have, or that joins a node to itself, is not drawn.
type Edge struct {
	From, To string
	Label    string
	Dashed   bool
}

// Diagram is what is drawn. Title names the picture for a screen reader.
type Diagram struct {
	Title string
	Nodes []Node
	Edges []Edge
}

// Sizes, in SVG user units. A PNG multiplies them by its scale.
const (
	margin = 20.0

	titleSize, titleLine   = 14.0, 18.0
	detailSize, detailLine = 12.0, 16.0
	labelSize, labelLine   = 11.0, 16.0

	padX, padY = 18.0, 9.0
	cornerR    = 5.0
	rimY       = 6.0 // the half height of a cylinder's rim

	nodeGap = 22.0 // between two nodes of a column
	bendGap = 14.0 // around an edge that passes through a column
	minGap  = 72.0 // between two columns
	maxGap  = 200.0

	edgeWidth = 1.5
	arrowLen  = 9.0
	arrowHalf = 4.5
	portStep  = 10.0 // between two edges on one side of a node
	labelPadX = 5.0
)

var (
	paper = color.RGBA{0xff, 0xff, 0xff, 0xff}
	ink   = color.RGBA{0x1f, 0x24, 0x30, 0xff}
	muted = color.RGBA{0x5b, 0x64, 0x72, 0xff}
	none  = color.RGBA{}

	// fills and strokes, by shape.
	fills = map[Shape]color.RGBA{
		Stadium:  {0xe8, 0xf1, 0xfd, 0xff},
		Hexagon:  {0xff, 0xf4, 0xd6, 0xff},
		Box:      {0xea, 0xf6, 0xec, 0xff},
		Cylinder: {0xfd, 0xeb, 0xec, 0xff},
	}
	strokes = map[Shape]color.RGBA{
		Stadium:  {0x3b, 0x76, 0xd6, 0xff},
		Hexagon:  {0xb5, 0x80, 0x00, 0xff},
		Box:      {0x3c, 0x8d, 0x4f, 0xff},
		Cylinder: {0xc8, 0x43, 0x4b, 0xff},
	}
)

// A scene is the picture as a list of filled or stroked paths and of texts,
// in drawing order. The SVG and PNG writers only know scenes.
type scene struct {
	title string
	w, h  float64
	items []item
}

type item struct {
	path   path // or a text, when empty
	fill   color.RGBA
	stroke color.RGBA
	width  float64
	dashed bool

	text string // centered on x, with its baseline on y
	x, y float64
	size float64
}

// A path is made of moves, lines, cubic curves and closes.
type path []seg

type seg struct {
	op  byte // M, L, C or Z
	pts [6]float64
}

func (p *path) move(x, y float64) { *p = append(*p, seg{op: 'M', pts: [6]float64{x, y}}) }
func (p *path) line(x, y float64) { *p = append(*p, seg{op: 'L', pts: [6]float64{x, y}}) }
func (p *path) close()            { *p = append(*p, seg{op: 'Z'}) }
func (p *path) curve(x1, y1, x2, y2, x, y float64) {
	*p = append(*p, seg{op: 'C', pts: [6]float64{x1, y1, x2, y2, x, y}})
}

// kappa places the control points of a cubic curve that draws a quarter of
// a circle.
const kappa = 0.5522847498

// roundRect is a rectangle centered on (x, y) with corners of radius r.
func roundRect(x, y, w, h, r float64) path {
	l, t, rt, b := x-w/2, y-h/2, x+w/2, y+h/2
	k := r * (1 - kappa)
	var p path
	p.move(l+r, t)
	p.line(rt-r, t)
	p.curve(rt-k, t, rt, t+k, rt, t+r)
	p.line(rt, b-r)
	p.curve(rt, b-k, rt-k, b, rt-r, b)
	p.line(l+r, b)
	p.curve(l+k, b, l, b-k, l, b-r)
	p.line(l, t+r)
	p.curve(l, t+k, l+k, t, l+r, t)
	p.close()
	return p
}

// hexInset is how far a hexagon's top and bottom corners stand in from its
// side points.
func hexInset(h float64) float64 { return h / 3 }

func hexagon(x, y, w, h float64) path {
	in := hexInset(h)
	var p path
	p.move(x-w/2, y)
	p.line(x-w/2+in, y-h/2)
	p.line(x+w/2-in, y-h/2)
	p.line(x+w/2, y)
	p.line(x+w/2-in, y+h/2)
	p.line(x-w/2+in, y+h/2)
	p.close()
	return p
}

// halfEllipse continues p from (x-rx, y) to (x+rx, y), over the top when
// dir is -1 and under the bottom when it is 1. Negative rx goes right to
// left.
func halfEllipse(p *path, x, y, rx, ry, dir float64) {
	p.curve(x-rx, y+dir*kappa*ry, x-kappa*rx, y+dir*ry, x, y+dir*ry)
	p.curve(x+kappa*rx, y+dir*ry, x+rx, y+dir*kappa*ry, x+rx, y)
}

// cylinder is the body of a cylinder and the front of its rim.
func cylinder(x, y, w, h float64) (body, rim path) {
	l, r, top, bottom := x-w/2, x+w/2, y-h/2+rimY, y+h/2-rimY
	body.move(l, top)
	halfEllipse(&body, x, top, w/2, rimY, -1)
	body.line(r, bottom)
	halfEllipse(&body, x, bottom, -w/2, rimY, 1)
	body.close()
	rim.move(l, top)
	halfEllipse(&rim, x, top, w/2, rimY, 1)
	return body, rim
}

// size is a node's width and height, from its text.
func (m *meter) size(n *Node) (w, h float64) {
	w = m.width(n.Title, titleSize)
	h = titleLine
	if n.Detail != "" {
		w = math.Max(w, m.width(n.Detail, detailSize))
		h += detailLine
	}
	w, h = w+2*padX, h+2*padY
	switch n.Shape {
	case Hexagon:
		w += hexInset(h)
	case Stadium:
		w += h / 4
	case Cylinder:
		h += 2 * rimY
	}
	return w, h
}

// sideX is where the outline of v is, dy under its middle, on its right or
// left side.
func sideX(v *vertex, right bool, dy float64) float64 {
	in := 0.0
	switch v.node.Shape {
	case Hexagon:
		in = hexInset(v.h) * math.Abs(dy) / (v.h / 2)
	case Stadium:
		r := v.h / 2
		in = r - math.Sqrt(math.Max(r*r-dy*dy, 0))
	}
	if right {
		return v.x + v.w/2 - in
	}
	return v.x - v.w/2 + in
}

// build lays the diagram out and draws it: the paper, the edges, their
// labels, then the nodes.
func (d Diagram) build() (*scene, error) {
	m, err := newMeter()
	if err != nil {
		return nil, err
	}
	p := layout(d, m)
	s := &scene{title: d.Title, w: p.w, h: p.h}
	var bg path
	bg.move(0, 0)
	bg.line(p.w, 0)
	bg.line(p.w, p.h)
	bg.line(0, p.h)
	bg.close()
	s.items = append(s.items, item{path: bg, fill: paper})

	var labels []item
	for _, r := range p.routes {
		line, head, at := r.draw(p)
		s.items = append(s.items,
			item{path: line, stroke: muted, width: edgeWidth, dashed: r.edge.Dashed},
			item{path: head, fill: muted})
		if r.edge.Label == "" {
			continue
		}
		w := m.width(r.edge.Label, labelSize) + 2*labelPadX
		labels = append(labels,
			item{path: roundRect(at[0], at[1], w, labelLine, 3), fill: paper},
			item{text: r.edge.Label, x: at[0], y: at[1] + labelSize*0.36, size: labelSize, fill: muted})
	}
	s.items = append(s.items, labels...)

	for _, col := range p.cols {
		for _, v := range col {
			if v.node != nil {
				s.items = append(s.items, drawNode(v)...)
			}
		}
	}
	return s, nil
}

func drawNode(v *vertex) []item {
	n := v.node
	fill, stroke := fills[n.Shape], strokes[n.Shape]
	var out []item
	mid := v.y // the middle of the text
	switch n.Shape {
	case Stadium:
		out = append(out, item{path: roundRect(v.x, v.y, v.w, v.h, v.h/2), fill: fill, stroke: stroke, width: edgeWidth})
	case Hexagon:
		out = append(out, item{path: hexagon(v.x, v.y, v.w, v.h), fill: fill, stroke: stroke, width: edgeWidth})
	case Cylinder:
		body, rim := cylinder(v.x, v.y, v.w, v.h)
		out = append(out,
			item{path: body, fill: fill, stroke: stroke, width: edgeWidth},
			item{path: rim, stroke: stroke, width: edgeWidth})
		mid += rimY
	default:
		out = append(out, item{path: roundRect(v.x, v.y, v.w, v.h, cornerR), fill: fill, stroke: stroke, width: edgeWidth})
	}
	if n.Detail == "" {
		return append(out, item{text: n.Title, x: v.x, y: mid + titleSize*0.36, size: titleSize, fill: ink})
	}
	top := mid - (titleLine+detailLine)/2
	return append(out,
		item{text: n.Title, x: v.x, y: top + titleLine/2 + titleSize*0.36, size: titleSize, fill: ink},
		item{text: n.Detail, x: v.x, y: top + titleLine + detailLine/2 + detailSize*0.36, size: detailSize, fill: muted})
}

// draw is the edge's line, its arrowhead and the middle of its label.
func (r *route) draw(p *plan) (line, head path, label [2]float64) {
	// The points the line goes through, column by column: out of the first
	// node, straight to the edge of its column, across the gap in a curve,
	// straight through every column it only passes, and into the last node.
	type pt struct{ x, y float64 }
	first, last := r.chain[0], r.chain[len(r.chain)-1]
	pts := []pt{{sideX(first, true, r.out), first.y + r.out}, {p.right[first.layer], first.y + r.out}}
	for _, v := range r.chain[1 : len(r.chain)-1] {
		pts = append(pts, pt{p.left[v.layer], v.y}, pt{p.right[v.layer], v.y})
	}
	pts = append(pts, pt{p.left[last.layer], last.y + r.in}, pt{sideX(last, false, r.in), last.y + r.in})

	// The label sits in the middle of the gap the edge crosses halfway.
	g := 1 + 2*((len(r.chain)-1)/2)
	label = [2]float64{(pts[g].x + pts[g+1].x) / 2, (pts[g].y + pts[g+1].y) / 2}

	if r.reversed {
		for i, j := 0, len(pts)-1; i < j; i, j = i+1, j-1 {
			pts[i], pts[j] = pts[j], pts[i]
		}
	}
	// The line stops where the arrowhead starts.
	tip := pts[len(pts)-1]
	dir := 1.0
	if r.reversed {
		dir = -1
	}
	pts[len(pts)-1].x -= dir * (arrowLen - 1)

	line.move(pts[0].x, pts[0].y)
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		if a.y == b.y {
			line.line(b.x, b.y)
			continue
		}
		mx := (a.x + b.x) / 2
		line.curve(mx, a.y, mx, b.y, b.x, b.y)
	}
	head.move(tip.x, tip.y)
	head.line(tip.x-dir*arrowLen, tip.y-arrowHalf)
	head.line(tip.x-dir*arrowLen, tip.y+arrowHalf)
	head.close()
	return line, head, label
}
