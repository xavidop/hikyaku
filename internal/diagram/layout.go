package diagram

import (
	"math"
	"slices"
)

// A vertex is a node in its column, or the bend of an edge that passes
// through a column (node is nil).
type vertex struct {
	node       *Node
	layer, pos int
	w, h       float64
	x, y       float64 // the middle
	prev, next []*vertex
}

// A route is an edge as laid out: the vertexes it goes through, from the
// earlier column to the later one.
type route struct {
	edge  *Edge
	chain []*vertex
	// reversed is true for an edge that points back, from a later column
	// to an earlier one: it closes a cycle.
	reversed bool
	// out and in are where the line leaves the first vertex and enters the
	// last one, under their middles.
	out, in float64
}

// labelHop is the gap the label is drawn in: the one after chain[labelHop].
func (r *route) labelHop() int { return (len(r.chain) - 1) / 2 }

type plan struct {
	cols        [][]*vertex
	left, right []float64 // of every column, with room for an arrowhead
	routes      []*route
	w, h        float64
}

// lead is the straight part of an edge next to a node, where the arrowhead
// is.
const lead = arrowLen + 3

func layout(d Diagram, m *meter) *plan {
	index := map[string]int{}
	for i, n := range d.Nodes {
		if _, ok := index[n.ID]; !ok {
			index[n.ID] = i
		}
	}
	type link struct {
		from, to int
		edge     *Edge
	}
	var links []link
	for i := range d.Edges {
		e := &d.Edges[i]
		from, ok1 := index[e.From]
		to, ok2 := index[e.To]
		if ok1 && ok2 && from != to {
			links = append(links, link{from, to, e})
		}
	}

	// An edge that points back is laid out the other way round, and drawn
	// with its arrowhead on the earlier column: first every edge that goes
	// against the ranks, then every edge that still closes a cycle.
	n := len(d.Nodes)
	back := make([]bool, len(links))
	for i, l := range links {
		if d.Nodes[l.from].Rank > d.Nodes[l.to].Rank {
			links[i].from, links[i].to = l.to, l.from
			back[i] = true
		}
	}
	out := make([][]int, n)
	for i, l := range links {
		out[l.from] = append(out[l.from], i)
	}
	cycle := make([]bool, len(links))
	state := make([]uint8, n) // 1 on the path, 2 done
	var visit func(u int)
	visit = func(u int) {
		state[u] = 1
		for _, i := range out[u] {
			switch v := links[i].to; state[v] {
			case 0:
				visit(v)
			case 1:
				cycle[i] = true
			}
		}
		state[u] = 2
	}
	for u := range n {
		if state[u] == 0 {
			visit(u)
		}
	}
	for i := range links {
		if cycle[i] {
			links[i].from, links[i].to = links[i].to, links[i].from
			back[i] = !back[i]
		}
	}

	// A node's column is its rank, or one past the last column an edge
	// comes from.
	layer := make([]int, n)
	indeg := make([]int, n)
	out = make([][]int, n)
	for i, l := range links {
		out[l.from] = append(out[l.from], i)
		indeg[l.to]++
	}
	var queue []int
	for u := range n {
		layer[u] = max(d.Nodes[u].Rank, 0)
		if indeg[u] == 0 {
			queue = append(queue, u)
		}
	}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for _, i := range out[u] {
			v := links[i].to
			layer[v] = max(layer[v], layer[u]+1)
			if indeg[v]--; indeg[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	// Columns nothing stands in are closed up.
	used := slices.Clone(layer)
	slices.Sort(used)
	used = slices.Compact(used)
	p := &plan{cols: make([][]*vertex, len(used))}
	verts := make([]*vertex, n)
	for u := range n {
		l, _ := slices.BinarySearch(used, layer[u])
		v := &vertex{node: &d.Nodes[u], layer: l}
		v.w, v.h = m.size(v.node)
		verts[u] = v
		p.add(v)
	}
	for i, l := range links {
		from, to := verts[l.from], verts[l.to]
		r := &route{edge: l.edge, reversed: back[i], chain: []*vertex{from}}
		for c := from.layer + 1; c < to.layer; c++ {
			bend := &vertex{layer: c}
			p.add(bend)
			r.chain = append(r.chain, bend)
		}
		r.chain = append(r.chain, to)
		for j := 1; j < len(r.chain); j++ {
			a, b := r.chain[j-1], r.chain[j]
			a.next = append(a.next, b)
			b.prev = append(b.prev, a)
		}
		p.routes = append(p.routes, r)
	}

	p.order()
	p.placeY()
	p.placeX(m)
	p.ports()
	return p
}

func (p *plan) add(v *vertex) {
	v.pos = len(p.cols[v.layer])
	p.cols[v.layer] = append(p.cols[v.layer], v)
}

// order sorts every column by where its neighbors stand, back and forth,
// and keeps the order that crosses the fewest edges.
func (p *plan) order() {
	keep := func() [][]*vertex {
		out := make([][]*vertex, len(p.cols))
		for i, col := range p.cols {
			out[i] = slices.Clone(col)
		}
		return out
	}
	best, fewest := keep(), p.crossings()
	for round := 0; round < 12 && fewest > 0; round++ {
		for l := 1; l < len(p.cols); l++ {
			sortColumn(p.cols[l], func(v *vertex) []*vertex { return v.prev })
		}
		for l := len(p.cols) - 2; l >= 0; l-- {
			sortColumn(p.cols[l], func(v *vertex) []*vertex { return v.next })
		}
		if x := p.crossings(); x < fewest {
			best, fewest = keep(), x
		}
	}
	p.cols = best
	for _, col := range p.cols {
		for i, v := range col {
			v.pos = i
		}
	}
}

func sortColumn(col []*vertex, side func(*vertex) []*vertex) {
	key := make(map[*vertex]float64, len(col))
	for _, v := range col {
		k := float64(v.pos)
		if ns := side(v); len(ns) > 0 {
			k = 0
			for _, n := range ns {
				k += float64(n.pos)
			}
			k /= float64(len(ns))
		}
		key[v] = k
	}
	slices.SortStableFunc(col, func(a, b *vertex) int {
		switch {
		case key[a] < key[b]:
			return -1
		case key[a] > key[b]:
			return 1
		}
		return 0
	})
	for i, v := range col {
		v.pos = i
	}
}

// crossings counts the pairs of edges that cross between two columns.
func (p *plan) crossings() int {
	total := 0
	for _, col := range p.cols {
		var ends [][2]int
		for _, v := range col {
			for _, n := range v.next {
				ends = append(ends, [2]int{v.pos, n.pos})
			}
		}
		for i, a := range ends {
			for _, b := range ends[i+1:] {
				if (a[0]-b[0])*(a[1]-b[1]) < 0 {
					total++
				}
			}
		}
	}
	return total
}

func gapBetween(a, b *vertex) float64 {
	g := nodeGap
	if a.node == nil || b.node == nil {
		g = bendGap
	}
	return a.h/2 + b.h/2 + g
}

// placeY stacks every column, then moves each vertex toward the middle of
// its neighbors, as far as the vertexes above and under it leave room.
func (p *plan) placeY() {
	for _, col := range p.cols {
		y := 0.0
		for i, v := range col {
			if i > 0 {
				y += gapBetween(col[i-1], v)
			}
			v.y = y
		}
		for _, v := range col {
			v.y -= y / 2
		}
	}
	prev := func(v *vertex) []*vertex { return v.prev }
	next := func(v *vertex) []*vertex { return v.next }
	both := func(v *vertex) []*vertex { return append(slices.Clone(v.prev), v.next...) }
	for range 6 {
		for l := 1; l < len(p.cols); l++ {
			pull(p.cols[l], prev)
		}
		for l := len(p.cols) - 2; l >= 0; l-- {
			pull(p.cols[l], next)
		}
	}
	for range 4 {
		for _, col := range p.cols {
			pull(col, both)
		}
	}
	top, bottom := math.Inf(1), math.Inf(-1)
	for _, col := range p.cols {
		for _, v := range col {
			top, bottom = math.Min(top, v.y-v.h/2), math.Max(bottom, v.y+v.h/2)
		}
	}
	if math.IsInf(top, 1) {
		top, bottom = 0, 0
	}
	for _, col := range p.cols {
		for _, v := range col {
			v.y += margin - top
		}
	}
	p.h = bottom - top + 2*margin
}

// pull moves the vertexes of a column as close to the middle of their
// neighbors as they can all be, in order and without touching: the least
// squares answer, by pooling the neighbors that would swap.
func pull(col []*vertex, side func(*vertex) []*vertex) {
	type block struct {
		sum float64
		n   int
	}
	var blocks []block
	offset := make([]float64, len(col))
	for i, v := range col {
		if i > 0 {
			offset[i] = offset[i-1] + gapBetween(col[i-1], v)
		}
		want := v.y
		if ns := side(v); len(ns) > 0 {
			want = 0
			for _, n := range ns {
				want += n.y
			}
			want /= float64(len(ns))
		}
		blocks = append(blocks, block{want - offset[i], 1})
		for k := len(blocks) - 1; k > 0 && blocks[k-1].sum/float64(blocks[k-1].n) > blocks[k].sum/float64(blocks[k].n); k-- {
			blocks[k-1] = block{blocks[k-1].sum + blocks[k].sum, blocks[k-1].n + blocks[k].n}
			blocks = blocks[:k]
		}
	}
	i := 0
	for _, b := range blocks {
		for range b.n {
			col[i].y = b.sum/float64(b.n) + offset[i]
			i++
		}
	}
}

// placeX gives every column its place. A gap is wide enough for the labels
// drawn in it, and wider when its edges climb far.
func (p *plan) placeX(m *meter) {
	gaps := make([]float64, len(p.cols))
	for i := range gaps {
		gaps[i] = minGap
	}
	for _, r := range p.routes {
		for j := 1; j < len(r.chain); j++ {
			a, b := r.chain[j-1], r.chain[j]
			gaps[a.layer] = math.Max(gaps[a.layer], math.Min(math.Abs(a.y-b.y)*0.3, maxGap))
		}
		if r.edge.Label != "" {
			g := r.chain[r.labelHop()].layer
			gaps[g] = math.Max(gaps[g], m.width(r.edge.Label, labelSize)+2*labelPadX+24)
		}
	}
	p.left = make([]float64, len(p.cols))
	p.right = make([]float64, len(p.cols))
	x := margin - lead
	for l, col := range p.cols {
		w := 0.0
		for _, v := range col {
			w = math.Max(w, v.w)
		}
		p.left[l], p.right[l] = x, x+w+2*lead
		for _, v := range col {
			v.x = x + lead + w/2
		}
		x = p.right[l] + gaps[l]
	}
	p.w = 2 * margin
	if n := len(p.cols); n > 0 {
		p.w = p.right[n-1] - lead + margin
	}
}

// ports spreads the edges that share a side of a node, in the order of
// where they come from, so that two of them never lie on each other.
func (p *plan) ports() {
	type end struct {
		at    *float64
		other float64
	}
	left, right := map[*vertex][]end{}, map[*vertex][]end{}
	for _, r := range p.routes {
		first, last := r.chain[0], r.chain[len(r.chain)-1]
		right[first] = append(right[first], end{&r.out, r.chain[1].y})
		left[last] = append(left[last], end{&r.in, r.chain[len(r.chain)-2].y})
	}
	for _, side := range []map[*vertex][]end{left, right} {
		for v, ends := range side {
			if len(ends) < 2 {
				continue
			}
			slices.SortStableFunc(ends, func(a, b end) int {
				switch {
				case a.other < b.other:
					return -1
				case a.other > b.other:
					return 1
				}
				return 0
			})
			step := math.Min(portStep, v.h*0.6/float64(len(ends)-1))
			for i, e := range ends {
				*e.at = (float64(i) - float64(len(ends)-1)/2) * step
			}
		}
	}
}
