package diagram

import (
	"bytes"
	"encoding/xml"
	"image/color"
	"image/png"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/image/font/sfnt"
)

// relay is a small spec as hikyaku graph draws it: two sources, two routes,
// destinations with a fallback, and the dead-letter queue.
func relay() Diagram {
	return Diagram{
		Title: "shop",
		Nodes: []Node{
			{ID: "s0", Shape: Stadium, Rank: 0, Title: "in", Detail: "stdin"},
			{ID: "s1", Shape: Stadium, Rank: 0, Title: "other", Detail: "stdin"},
			{ID: "r2", Shape: Hexagon, Rank: 1, Title: "triage", Detail: "ai → static"},
			{ID: "r3", Shape: Hexagon, Rank: 1, Title: "copy", Detail: "static"},
			{ID: "d4", Shape: Box, Rank: 2, Title: "billing", Detail: "fault"},
			{ID: "d5", Shape: Box, Rank: 2, Title: "support", Detail: `bugs "and" <questions>`},
			{ID: "d6", Shape: Box, Rank: 2, Title: "spool"},
			{ID: "q7", Shape: Cylinder, Rank: 3, Title: "dead_letter", Detail: "file"},
		},
		Edges: []Edge{
			{From: "s0", To: "r2"},
			{From: "s1", To: "r2"},
			{From: "s0", To: "r3"},
			{From: "r2", To: "d4"},
			{From: "r2", To: "d5", Label: "static fallback"},
			{From: "r3", To: "d5"},
			{From: "d4", To: "d6", Label: "fallback", Dashed: true},
			{From: "d5", To: "q7", Dashed: true},
			{From: "d6", To: "q7", Dashed: true},
		},
	}
}

func plan0(t *testing.T, d Diagram) (*plan, map[string]*vertex) {
	t.Helper()
	m, err := newMeter()
	require.NoError(t, err)
	p := layout(d, m)
	byID := map[string]*vertex{}
	for _, col := range p.cols {
		for _, v := range col {
			if v.node != nil {
				byID[v.node.ID] = v
			}
		}
	}
	return p, byID
}

func TestLayoutColumns(t *testing.T) {
	p, v := plan0(t, relay())
	require.Len(t, v, 8)
	assert.Equal(t, 0, v["s0"].layer)
	assert.Equal(t, 1, v["r2"].layer)
	assert.Equal(t, 2, v["d4"].layer)
	assert.Equal(t, 3, v["d6"].layer, "a fallback stands after the destination it backs")
	assert.Equal(t, 4, v["q7"].layer, "the queue stands after everything that points at it")

	// support → dead_letter crosses the spool column: it bends there.
	var long *route
	for _, r := range p.routes {
		if r.edge.From == "d5" && r.edge.To == "q7" {
			long = r
		}
	}
	require.NotNil(t, long)
	require.Len(t, long.chain, 3)
	assert.Nil(t, long.chain[1].node)
	assert.Equal(t, 3, long.chain[1].layer)
}

// No two vertexes of a column touch, every node is inside the picture, and
// the columns go left to right with a gap.
func TestLayoutKeepsNodesApart(t *testing.T) {
	big := Diagram{}
	for i := range 40 {
		id := string(rune('a'+i%26)) + string(rune('0'+i/26))
		big.Nodes = append(big.Nodes, Node{ID: id, Rank: i % 4, Title: strings.Repeat("n", 3+i%9), Detail: "detail"})
		for j := range i {
			if (i*7+j*3)%11 == 0 {
				big.Edges = append(big.Edges, Edge{From: big.Nodes[j].ID, To: id, Label: "label"})
			}
		}
	}
	for name, d := range map[string]Diagram{"relay": relay(), "big": big, "empty": {}, "one": {Nodes: []Node{{ID: "a", Title: "a"}}}} {
		t.Run(name, func(t *testing.T) {
			p, _ := plan0(t, d)
			for l, col := range p.cols {
				for i, v := range col {
					assert.GreaterOrEqual(t, v.x-v.w/2, margin-0.01)
					assert.LessOrEqual(t, v.x+v.w/2, p.w-margin+0.01)
					assert.GreaterOrEqual(t, v.y-v.h/2, margin-0.01)
					assert.LessOrEqual(t, v.y+v.h/2, p.h-margin+0.01)
					if i > 0 {
						above := col[i-1]
						assert.GreaterOrEqual(t, v.y-v.h/2-(above.y+above.h/2), bendGap-0.01, "column %d, place %d", l, i)
					}
				}
				if l > 0 {
					assert.GreaterOrEqual(t, p.left[l]-p.right[l-1], minGap-0.01)
				}
			}
			for _, r := range p.routes {
				for j := 1; j < len(r.chain); j++ {
					assert.Equal(t, r.chain[j-1].layer+1, r.chain[j].layer, "an edge goes one column at a time")
				}
			}
		})
	}
}

func TestLayoutOrdersColumnsToCrossLess(t *testing.T) {
	// a → y and b → x cross as written; ordering the second column undoes it.
	d := Diagram{
		Nodes: []Node{{ID: "a", Title: "a"}, {ID: "b", Title: "b"}, {ID: "x", Rank: 1, Title: "x"}, {ID: "y", Rank: 1, Title: "y"}},
		Edges: []Edge{{From: "a", To: "y"}, {From: "b", To: "x"}},
	}
	p, v := plan0(t, d)
	assert.Zero(t, p.crossings())
	assert.Less(t, v["y"].y, v["x"].y)
	assert.InDelta(t, v["a"].y, v["y"].y, 0.01, "a node stands next to its only neighbor")
}

func TestLayoutGapFitsTheLabel(t *testing.T) {
	label := strings.Repeat("a long label ", 6)
	d := Diagram{
		Nodes: []Node{{ID: "a", Title: "a"}, {ID: "b", Rank: 1, Title: "b"}},
		Edges: []Edge{{From: "a", To: "b", Label: label}},
	}
	p, _ := plan0(t, d)
	m, _ := newMeter()
	assert.Greater(t, p.left[1]-p.right[0], m.width(label, labelSize))
}

// An edge that closes a cycle is drawn backwards: its arrowhead points left,
// into the right side of its target. Edges to nowhere and to oneself are
// left out.
func TestLayoutCycle(t *testing.T) {
	d := Diagram{
		Nodes: []Node{{ID: "spool", Title: "spool"}, {ID: "dlq", Shape: Cylinder, Rank: 1, Title: "dead_letter"}},
		Edges: []Edge{
			{From: "spool", To: "dlq"},
			{From: "dlq", To: "spool", Label: "fallback"},
			{From: "spool", To: "spool"},
			{From: "spool", To: "ghost"},
		},
	}
	p, v := plan0(t, d)
	require.Len(t, p.routes, 2)
	assert.Equal(t, 0, v["spool"].layer)
	assert.Equal(t, 1, v["dlq"].layer)
	fwd, back := p.routes[0], p.routes[1]
	assert.False(t, fwd.reversed)
	assert.True(t, back.reversed)
	assert.NotEqual(t, fwd.out, back.out, "the two edges leave spool at different heights")

	_, head, _ := back.draw(p)
	tip := head[0].pts
	assert.InDelta(t, v["spool"].x+v["spool"].w/2, tip[0], 0.01)
	assert.Greater(t, head[1].pts[0], tip[0], "the arrowhead opens to the right")

	_, head, _ = fwd.draw(p)
	assert.InDelta(t, v["dlq"].x-v["dlq"].w/2, head[0].pts[0], 0.01)
	assert.Less(t, head[1].pts[0], head[0].pts[0])
}

// The queue's fallback is a destination that itself ends in the queue. The
// edge that goes against the ranks is the one drawn backwards, so the
// destination stays in the destinations' column.
func TestLayoutKeepsRanksInACycle(t *testing.T) {
	d := Diagram{
		Nodes: []Node{
			{ID: "route", Shape: Hexagon, Rank: 1, Title: "all"},
			{ID: "billing", Rank: 2, Title: "billing"},
			{ID: "last", Rank: 2, Title: "last-resort"},
			{ID: "dlq", Shape: Cylinder, Rank: 3, Title: "dead_letter"},
		},
		Edges: []Edge{
			{From: "route", To: "billing"},
			{From: "route", To: "last"},
			{From: "billing", To: "dlq"},
			{From: "last", To: "dlq"},
			{From: "dlq", To: "last", Label: "fallback"},
		},
	}
	p, v := plan0(t, d)
	assert.Equal(t, v["billing"].layer, v["last"].layer)
	assert.Equal(t, v["last"].layer+1, v["dlq"].layer)
	for _, r := range p.routes {
		assert.Equal(t, r.edge.From == "dlq", r.reversed, "%s → %s", r.edge.From, r.edge.To)
	}
}

func TestFontHasTheArrow(t *testing.T) {
	// A route's router chain is written "ai → static".
	f, err := goFont()
	require.NoError(t, err)
	var b sfnt.Buffer
	i, err := f.GlyphIndex(&b, '→')
	require.NoError(t, err)
	assert.NotZero(t, i)
}

func TestSVG(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, relay().SVG(&b))
	out := b.String()

	// It is well-formed XML, with every text escaped.
	dec := xml.NewDecoder(strings.NewReader(out))
	texts, paths := 0, 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if el, ok := tok.(xml.StartElement); ok {
			switch el.Name.Local {
			case "text":
				texts++
			case "path":
				paths++
			}
		}
	}
	assert.Equal(t, 8+7+2, texts, "eight titles, seven details and two labels")
	assert.Greater(t, paths, 8+9)

	assert.True(t, strings.HasPrefix(out, `<svg xmlns="http://www.w3.org/2000/svg" width="`))
	assert.Contains(t, out, "<title>shop</title>")
	assert.Contains(t, out, ">ai → static</text>")
	assert.Contains(t, out, "bugs &#34;and&#34; &lt;questions&gt;</text>")
	assert.Contains(t, out, ">static fallback</text>")
	assert.Equal(t, 3, strings.Count(out, "stroke-dasharray"), "the fallback and the two dead-letter edges")
	assert.NotContains(t, out, "NaN")
	assert.NotContains(t, out, "Inf")

	var again bytes.Buffer
	require.NoError(t, relay().SVG(&again))
	assert.Equal(t, out, again.String(), "the same diagram is the same file")
}

func TestPNG(t *testing.T) {
	d := relay()
	var b bytes.Buffer
	require.NoError(t, d.PNG(&b))
	img, err := png.Decode(&b)
	require.NoError(t, err)

	p, v := plan0(t, d)
	assert.Equal(t, int(math.Ceil(p.w*pngScale)), img.Bounds().Dx())
	assert.Equal(t, int(math.Ceil(p.h*pngScale)), img.Bounds().Dy())

	at := func(x, y float64) color.RGBA {
		r, g, b, a := img.At(int(x*pngScale), int(y*pngScale)).RGBA()
		return color.RGBA{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8), uint8(a >> 8)}
	}
	assert.Equal(t, paper, at(2, 2), "the corner is paper")
	// Just inside a node's left side there is its fill, and on the side its
	// stroke.
	for id, shape := range map[string]Shape{"d6": Box, "q7": Cylinder, "s0": Stadium} {
		n := v[id]
		assert.Equal(t, fills[shape], at(n.x-n.w/2+6, n.y), id)
		assert.Equal(t, strokes[shape], at(n.x-n.w/2, n.y), id)
	}
	// The title is drawn in ink: some pixel of the node's middle is dark.
	n := v["d6"]
	dark := false
	for x := n.x - n.w/2 + padX; x < n.x+n.w/2-padX; x += 0.5 {
		for y := n.y - titleLine/2; y < n.y+titleLine/2; y += 0.5 {
			if c := at(x, y); c.R < 0x60 && c.G < 0x60 {
				dark = true
			}
		}
	}
	assert.True(t, dark, "spool's title is drawn")
}

func TestPNGScale(t *testing.T) {
	assert.InDelta(t, pngScale, scaleFor(1200, 400), 0)
	// A huge diagram is drawn smaller, to stay under the pixel limit.
	s := scaleFor(4000, 50000)
	assert.Less(t, s, pngScale)
	assert.InDelta(t, maxPixels, 4000*s*50000*s, 1)
}

func TestDash(t *testing.T) {
	line := []pt{{0, 0}, {10, 0}, {10, 13}}
	pieces := dash(line, 5, 4)
	// 23 pixels: 5 on, 4 off, 5 on (around the corner), 4 off, 5 on.
	require.Len(t, pieces, 3)
	drawn := 0.0
	for _, piece := range pieces {
		for i := 1; i < len(piece); i++ {
			drawn += dist(piece[i-1], piece[i])
		}
	}
	assert.InDelta(t, 15, drawn, 1e-9)
	assert.Equal(t, []pt{{9, 0}, {10, 0}, {10, 4}}, pieces[1], "a dash turns the corner")
}

func TestOutlineTurnsOneWay(t *testing.T) {
	shapes := outline([]pt{{0, 0}, {10, 0}, {10, 0}, {10, 10}, {0, 3}}, 1)
	require.Len(t, shapes, 3+2, "three pieces, two joints, and nothing for the piece of no length")
	for _, s := range shapes {
		area := 0.0
		for i, a := range s {
			b := s[(i+1)%len(s)]
			area += a.x*b.y - b.x*a.y
		}
		assert.Negative(t, area, "overlapping shapes fill as one only when they turn the same way")
	}
}
