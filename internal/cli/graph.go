package cli

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/xavidop/hikyaku/internal/config"
)

// Graph is a spec as a picture: its sources, routes and destinations, and
// the edges between them (hikyaku graph).
type Graph struct {
	Name  string      `json:"name"`
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// GraphNode is a source, a route, a destination or the dead-letter queue.
type GraphNode struct {
	ID   string `json:"id"`
	Kind string `json:"kind"` // source | route | destination | dead_letter
	Name string `json:"name"`
	// Detail is the second line of the node: a type, a router chain.
	Detail string `json:"detail,omitempty"`
}

// GraphEdge joins two nodes.
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	// Kind is route (a source into a route), to (a route's destination),
	// fallback, shadow, invalid (a validate step's on_invalid) or
	// dead_letter.
	Kind  string `json:"kind"`
	Label string `json:"label,omitempty"`
}

const (
	nodeSource = "source"
	nodeRoute  = "route"
	nodeDest   = "destination"
	nodeDLQ    = "dead_letter"

	edgeRoute    = "route"
	edgeTo       = "to"
	edgeFallback = "fallback"
	edgeShadow   = "shadow"
	edgeInvalid  = "invalid"
	edgeDLQ      = "dead_letter"
)

// routeTargets are the destinations a route may pick, with why: its own to,
// each router of its chain (rules, the default, candidates, the low
// confidence answer), in spec order and once each.
type routeTarget struct {
	dest, label string
}

func routeTargets(r config.Route) []routeTarget {
	var out []routeTarget
	seen := map[string]bool{}
	add := func(label string, dests ...string) {
		for _, d := range dests {
			if !seen[d] {
				seen[d] = true
				out = append(out, routeTarget{dest: d, label: label})
			}
		}
	}
	add("", r.To...)
	var walk func(rs *config.RouterSpec, fallback bool)
	walk = func(rs *config.RouterSpec, fallback bool) {
		if rs == nil {
			return
		}
		// The first router's picks need no label; a later router of the
		// chain is named, since it only decides when the ones before fail.
		label := ""
		if fallback {
			label = rs.Type + " fallback"
		}
		add(label, rs.To...)
		for _, rule := range rs.Rules {
			add(label, rule.To...)
		}
		for _, c := range rs.Candidates {
			add(label, c.Name)
		}
		add(join(label, "default"), rs.Default...)
		add(join(label, "low confidence"), rs.OnLowConfidence...)
		for i := range rs.Fallback {
			walk(&rs.Fallback[i], true)
		}
	}
	walk(r.Router, false)
	return out
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + ", " + b
}

// routerChain names a route's routers in order: "ai → cel → static".
func routerChain(r config.Route) string {
	if r.Router == nil {
		return "static"
	}
	var types []string
	var walk func(rs *config.RouterSpec)
	walk = func(rs *config.RouterSpec) {
		t := rs.Type
		if t == "" {
			t = "static"
		}
		types = append(types, t)
		for i := range rs.Fallback {
			walk(&rs.Fallback[i])
		}
	}
	walk(r.Router)
	return strings.Join(types, " → ")
}

func destDetail(d config.Destination) string {
	if len(d.Members) == 0 {
		return d.Type
	}
	types := map[string]bool{}
	for _, m := range d.Members {
		types[m.Type] = true
	}
	return strings.TrimSpace(fmt.Sprintf("%s group of %d %s", d.Strategy, len(d.Members), strings.Join(sortedKeys(types), ", ")))
}

// buildGraph draws spec. withDLQ adds the dead-letter queue, with an edge
// from every destination a failure is dead-lettered for.
func buildGraph(spec *config.Spec, withDLQ bool) Graph {
	g := Graph{Name: spec.Name}
	ids := map[string]string{}
	node := func(kind, name, detail string) string {
		key := kind + "/" + name
		if id, ok := ids[key]; ok {
			return id
		}
		id := fmt.Sprintf("%c%d", kind[0], len(ids))
		ids[key] = id
		g.Nodes = append(g.Nodes, GraphNode{ID: id, Kind: kind, Name: name, Detail: detail})
		return id
	}
	edge := func(from, to, kind, label string) {
		e := GraphEdge{From: from, To: to, Kind: kind, Label: label}
		if !slices.Contains(g.Edges, e) {
			g.Edges = append(g.Edges, e)
		}
	}
	for _, s := range spec.Sources {
		node(nodeSource, s.Name, s.Type)
	}
	for _, r := range spec.Routes {
		node(nodeRoute, r.Name, routerChain(r))
	}
	dest := func(name string) string {
		if d, ok := spec.DestinationByName(name); ok {
			return node(nodeDest, name, destDetail(*d))
		}
		return node(nodeDest, name, "")
	}
	for _, d := range spec.Destinations {
		dest(d.Name)
	}

	for _, r := range spec.Routes {
		rid := node(nodeRoute, r.Name, "")
		for _, s := range r.From {
			edge(node(nodeSource, s, ""), rid, edgeRoute, "")
		}
		for _, t := range routeTargets(r) {
			d, _ := spec.DestinationByName(t.dest)
			if d != nil && d.Shadow {
				label := "shadow"
				if rate := d.SampleRate(); rate < 1 {
					label = fmt.Sprintf("shadow %s%%", trimFloat(rate*100))
				}
				edge(rid, dest(t.dest), edgeShadow, label)
				continue
			}
			edge(rid, dest(t.dest), edgeTo, t.label)
		}
		for _, t := range r.Transforms {
			if t.Validate != nil && t.Validate.OnInvalid != nil {
				for _, to := range t.Validate.OnInvalid.To {
					edge(rid, dest(to), edgeInvalid, "invalid")
				}
			}
		}
	}
	for _, d := range spec.Destinations {
		for _, f := range d.Fallback {
			edge(dest(d.Name), dest(f), edgeFallback, "fallback")
		}
	}
	if withDLQ {
		kind := "stderr"
		if spec.DeadLetter != nil && spec.DeadLetter.Type != "" {
			kind = spec.DeadLetter.Type
		}
		dlq := node(nodeDLQ, "dead_letter", kind)
		for _, d := range spec.Destinations {
			// A chain's failure is dead-lettered where the chain ends, for
			// a required destination only.
			if d.IsRequired() && len(d.Fallback) == 0 {
				edge(dest(d.Name), dlq, edgeDLQ, "")
			}
		}
		if spec.DeadLetter != nil {
			for _, f := range spec.DeadLetter.Fallback {
				// A fallback is a destination, or a built-in last resort
				// (stderr, stdout).
				var to string
				if _, ok := spec.DestinationByName(f); ok {
					to = dest(f)
				} else {
					to = node(nodeDLQ, f, "")
				}
				edge(dlq, to, edgeFallback, "fallback")
			}
		}
	}
	return g
}

func trimFloat(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", f), "0"), ".")
}

// mermaidText makes s safe inside a quoted Mermaid label.
func mermaidText(s string) string {
	r := strings.NewReplacer(`"`, "#quot;", "<", "#lt;", ">", "#gt;", "\n", " ")
	return r.Replace(s)
}

func (g Graph) label(n GraphNode, br string, esc func(string) string) string {
	if n.Detail == "" {
		return esc(n.Name)
	}
	return esc(n.Name) + br + esc(n.Detail)
}

// Mermaid renders the graph as a Mermaid flowchart, left to right: sources
// are rounded, routes are hexagons, destinations are boxes and the
// dead-letter queue is a cylinder.
func (g Graph) Mermaid(w io.Writer) {
	fprintln(w, "flowchart LR")
	for _, n := range g.Nodes {
		l := `"` + g.label(n, "<br/>", mermaidText) + `"`
		switch n.Kind {
		case nodeSource:
			fprintf(w, "  %s([%s])\n", n.ID, l)
		case nodeRoute:
			fprintf(w, "  %s{{%s}}\n", n.ID, l)
		case nodeDLQ:
			fprintf(w, "  %s[(%s)]\n", n.ID, l)
		default:
			fprintf(w, "  %s[%s]\n", n.ID, l)
		}
	}
	for _, e := range g.Edges {
		arrow := "-->"
		if e.Kind == edgeFallback || e.Kind == edgeShadow || e.Kind == edgeDLQ || e.Kind == edgeInvalid {
			arrow = "-.->"
		}
		if e.Label != "" {
			fprintf(w, "  %s %s|\"%s\"| %s\n", e.From, arrow, mermaidText(e.Label), e.To)
			continue
		}
		fprintf(w, "  %s %s %s\n", e.From, arrow, e.To)
	}
}

func dotText(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", " ").Replace(s)
}

// Dot renders the graph in Graphviz DOT.
func (g Graph) Dot(w io.Writer) {
	fprintf(w, "digraph \"%s\" {\n  rankdir=LR;\n  node [fontname=\"Helvetica\"];\n", dotText(g.Name))
	shapes := map[string]string{nodeSource: "oval", nodeRoute: "hexagon", nodeDest: "box", nodeDLQ: "cylinder"}
	for _, n := range g.Nodes {
		fprintf(w, "  %s [shape=%s, label=\"%s\"];\n", n.ID, shapes[n.Kind], g.label(n, `\n`, dotText))
	}
	for _, e := range g.Edges {
		var attrs []string
		if e.Label != "" {
			attrs = append(attrs, fmt.Sprintf("label=\"%s\"", dotText(e.Label)))
		}
		if e.Kind == edgeFallback || e.Kind == edgeShadow || e.Kind == edgeDLQ || e.Kind == edgeInvalid {
			attrs = append(attrs, "style=dashed")
		}
		if len(attrs) == 0 {
			fprintf(w, "  %s -> %s;\n", e.From, e.To)
			continue
		}
		fprintf(w, "  %s -> %s [%s];\n", e.From, e.To, strings.Join(attrs, ", "))
	}
	fprintln(w, "}")
}

func (a *app) newGraphCmd() *cobra.Command {
	var format string
	var withDLQ bool
	cmd := &cobra.Command{
		Use:   "graph",
		Short: "Draw the spec as a diagram: sources, routes, destinations and fallbacks",
		Long: "Draw the spec as a diagram. Every source points at the routes that read it, every\n" +
			"route at the destinations it may pick, and every destination at its fallbacks.\n" +
			"A shadow destination and a fallback are dashed lines.\n\n" +
			"The default output is a Mermaid flowchart, which GitHub renders inside a\n" +
			"```mermaid block. --format dot is for Graphviz, and --format json is the same\n" +
			"graph as data. Nothing is connected to and no secret is read.",
		Example: "  hikyaku graph > relay.mmd\n" +
			"  hikyaku graph --format dot | dot -Tsvg > relay.svg\n" +
			"  hikyaku graph --dead-letter",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !slices.Contains([]string{"mermaid", "dot", "json"}, format) {
				return usageError(fmt.Errorf("--format %q: want mermaid, dot or json", format))
			}
			l, err := a.load(cmd.Context(), false)
			if err != nil {
				return err
			}
			g := buildGraph(l.Spec, withDLQ)
			w := cmd.OutOrStdout()
			switch format {
			case "dot":
				g.Dot(w)
			case "json":
				return printJSON(w, g)
			default:
				g.Mermaid(w)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&format, "format", "mermaid", "output: mermaid, dot or json")
	cmd.Flags().BoolVar(&withDLQ, "dead-letter", false, "draw the dead-letter queue too")
	return cmd
}
