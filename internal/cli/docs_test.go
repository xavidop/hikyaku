package cli_test

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/xavidop/hikyaku/internal/cli"
)

const docsDir = "../../site/src/pages/docs"

func newRoot() *cobra.Command {
	return cli.NewRoot(cli.BuildInfo{}, cli.Streams{}, cli.Deps{})
}

// userCommands maps "dlq list" style paths to every command a user runs,
// cobra's own help and completion left out.
func userCommands(root *cobra.Command) map[string]*cobra.Command {
	out := map[string]*cobra.Command{}
	var walk func(c *cobra.Command, prefix string)
	walk = func(c *cobra.Command, prefix string) {
		for _, sub := range c.Commands() {
			if sub.Name() == "help" || sub.Name() == "completion" || sub.Hidden {
				continue
			}
			path := strings.TrimSpace(prefix + " " + sub.Name())
			out[path] = sub
			walk(sub, path)
		}
	}
	walk(root, "")
	return out
}

func globalFlags(root *cobra.Command) []string {
	var out []string
	root.PersistentFlags().VisitAll(func(f *pflag.Flag) { out = append(out, "--"+f.Name) })
	return out
}

func localFlags(c *cobra.Command) []string {
	var names []string
	c.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
		if f.Name != "help" {
			names = append(names, "--"+f.Name)
		}
	})
	return names
}

// docSections splits the CLI reference into the text before the first
// "## hikyaku <path>" heading and one body per such heading.
func docSections(doc string) (intro string, sections map[string]string) {
	heading := regexp.MustCompile(`(?m)^## hikyaku ([a-z][a-z -]*?)\s*$`)
	idx := heading.FindAllStringSubmatchIndex(doc, -1)
	sections = map[string]string{}
	intro = doc
	if len(idx) > 0 {
		intro = doc[:idx[0][0]]
	}
	anyHeading := regexp.MustCompile(`(?m)^## `)
	for _, m := range idx {
		end := len(doc)
		if next := anyHeading.FindStringIndex(doc[m[1]:]); next != nil {
			end = m[1] + next[0]
		}
		sections[doc[m[2]:m[3]]] = doc[m[1]:end]
	}
	return intro, sections
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestDocsCoverCommandsAndFlags fails when the CLI reference and the cobra
// command tree disagree: a command or flag the code has that the page
// lacks, or a "## hikyaku <path>" heading or flags-table row the code does
// not have. A subcommand's flag may be documented in its parent's section
// (history's --admin and --token).
func TestDocsCoverCommandsAndFlags(t *testing.T) {
	doc := read(t, filepath.Join(docsDir, "cli.md"))
	intro, sections := docSections(doc)
	root := newRoot()
	for _, f := range globalFlags(root) {
		if !strings.Contains(intro, f) {
			t.Errorf("cli.md intro does not mention the global flag %s", f)
		}
	}
	known := userCommands(root)
	for path, c := range known {
		body, ok := sections[path]
		if !ok {
			t.Errorf("cli.md has no \"## hikyaku %s\" heading", path)
			continue
		}
		if !strings.Contains(body, "hikyaku "+path) {
			t.Errorf("cli.md section %q does not mention \"hikyaku %s\"", path, path)
		}
		parent := ""
		if i := strings.LastIndex(path, " "); i > 0 {
			parent = sections[path[:i]]
		}
		for _, f := range localFlags(c) {
			if !strings.Contains(body, f) && !strings.Contains(parent, f) {
				t.Errorf("cli.md section %q does not mention %s", path, f)
			}
		}
	}
	for path := range sections {
		if _, ok := known[path]; !ok {
			t.Errorf("cli.md documents \"hikyaku %s\", which does not exist", path)
		}
	}
	row := regexp.MustCompile("(?m)^\\| `(?:-\\w, )?(--[a-z-]+)")
	for path, body := range sections {
		c := known[path]
		if c == nil {
			continue
		}
		have := map[string]bool{}
		for _, f := range globalFlags(root) {
			have[f] = true
		}
		c.Flags().VisitAll(func(f *pflag.Flag) { have["--"+f.Name] = true })
		for _, sub := range c.Commands() {
			sub.Flags().VisitAll(func(f *pflag.Flag) { have["--"+f.Name] = true })
		}
		for _, m := range row.FindAllStringSubmatch(body, -1) {
			if !have[m[1]] {
				t.Errorf("cli.md section %q lists %s, which \"hikyaku %s\" does not have", path, m[1], path)
			}
		}
	}
	for _, code := range []string{"`0`", "`1`", "`2`", "`3`"} {
		if !strings.Contains(doc, code) {
			t.Errorf("cli.md does not document exit code %s", code)
		}
	}
}

// TestDocsMentionOnlyRealCommands checks every "hikyaku <command>" written
// in the docs pages, the README and CONTRIBUTING.
func TestDocsMentionOnlyRealCommands(t *testing.T) {
	root := newRoot()
	known := userCommands(root)
	known["completion"], known["help"] = nil, nil
	files, _ := filepath.Glob(filepath.Join(docsDir, "*.md"))
	files = append(files, "../../README.md", "../../CONTRIBUTING.md")
	fence := regexp.MustCompile("(?s)```[a-z]*\\n(.*?)```")
	inline := regexp.MustCompile("`([^`\\n]+)`")
	// A command starts a line of code (after "$ "), an inline span, or a
	// segment after && ; or | (so "helm install hikyaku oci://..." is not
	// one).
	mention := regexp.MustCompile(`(?m)(?:^\s*(?:\$ )?|&& |; |\| )hikyaku ([a-z][a-z-]+)(?: ([a-z][a-z-]+))?`)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var code strings.Builder
		text := string(b)
		for _, m := range fence.FindAllStringSubmatch(text, -1) {
			code.WriteString(m[1] + "\n")
		}
		for _, m := range inline.FindAllStringSubmatch(fence.ReplaceAllString(text, ""), -1) {
			code.WriteString(m[1] + "\n")
		}
		for _, m := range mention.FindAllStringSubmatch(code.String(), -1) {
			if _, ok := known[m[1]]; !ok {
				t.Errorf("%s mentions \"hikyaku %s\", which is not a command", filepath.Base(f), m[1])
				continue
			}
			c := known[m[1]]
			if c == nil || !c.HasSubCommands() || m[2] == "" {
				continue
			}
			if _, ok := known[m[1]+" "+m[2]]; !ok && !strings.HasPrefix(m[2], "-") {
				t.Errorf("%s mentions \"hikyaku %s %s\", which is not a command", filepath.Base(f), m[1], m[2])
			}
		}
	}
}

// TestSiteNavMatchesPages keeps the sidebar of DocsLayout.astro and the
// pages under site/src/pages/docs in step.
func TestSiteNavMatchesPages(t *testing.T) {
	layout := read(t, "../../site/src/layouts/DocsLayout.astro")
	slugRe := regexp.MustCompile(`slug: "([a-z0-9/-]*)"`)
	slugs := map[string]bool{}
	for _, m := range slugRe.FindAllStringSubmatch(layout, -1) {
		slugs[m[1]] = true
		page := filepath.Join(docsDir, m[1])
		if m[1] == "" {
			page = filepath.Join(docsDir, "index")
		}
		_, errFile := os.Stat(page + ".md")
		_, errIndex := os.Stat(filepath.Join(page, "index.md"))
		if errFile != nil && errIndex != nil {
			t.Errorf("DocsLayout.astro links slug %q, which has no page", m[1])
		}
	}
	err := filepath.WalkDir(docsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		rel, _ := filepath.Rel(docsDir, path)
		slug := strings.TrimSuffix(strings.TrimSuffix(filepath.ToSlash(rel), ".md"), "/index")
		if slug == "index" {
			slug = ""
		}
		if !slugs[slug] {
			t.Errorf("docs page %s is not in the sidebar of DocsLayout.astro", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestContributingLayoutMatchesTheModules keeps CONTRIBUTING's repository
// layout in step with go.work: every module is named.
func TestContributingLayoutMatchesTheModules(t *testing.T) {
	work := read(t, "../../go.work")
	contrib := read(t, "../../CONTRIBUTING.md")
	i := strings.Index(contrib, "## Repository layout")
	if i < 0 {
		t.Fatal("CONTRIBUTING.md has no \"## Repository layout\" section")
	}
	section := contrib[i:]
	if j := strings.Index(section[3:], "\n## "); j > 0 {
		section = section[:j+3]
	}
	use := regexp.MustCompile(`(?m)^\s+\./?([a-z0-9/_-]*)\s*$`)
	for _, m := range use.FindAllStringSubmatch(work, -1) {
		if m[1] == "" {
			continue
		}
		if !strings.Contains(section, "`"+m[1]+"`") && !strings.Contains(section, "`"+m[1]+"/`") {
			t.Errorf("CONTRIBUTING.md's repository layout does not name the module %s", m[1])
		}
	}
}

// TestReadmeShape checks the README's header, install and CLI sections.
func TestReadmeShape(t *testing.T) {
	readme := read(t, "../../README.md")
	for _, want := range []string{
		"assets/logo-wordmark-light.svg", "assets/logo-wordmark-dark.svg",
		"brew install xavidop/tap/hikyaku", "ghcr.io/xavidop/hikyaku",
		"oci://ghcr.io/xavidop/charts/hikyaku",
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("README.md does not mention %s", want)
		}
	}
	for path, c := range userCommands(newRoot()) {
		if strings.Contains(path, " ") || c == nil {
			continue
		}
		if !strings.Contains(readme, "hikyaku "+path) {
			t.Errorf("README.md does not list \"hikyaku %s\"", path)
		}
	}
	if slices.Contains(strings.Fields(readme), "TODO") {
		t.Error("README.md has a TODO")
	}
}
