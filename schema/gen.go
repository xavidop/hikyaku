// Command gen writes schema/hikyaku.schema.json and the docs site's spec
// reference page from the Go structs of internal/config.
//
//	go run ./schema/gen.go [schema-dir] [reference-page]
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/hikyaku/internal/config"
)

func main() {
	dir := "schema"
	page := filepath.Join("site", "src", "pages", "docs", "spec-reference.md")
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	if len(os.Args) > 2 {
		page = os.Args[2]
	}
	b, err := config.SchemaJSON(config.Schema())
	check(err)
	check(os.MkdirAll(dir, 0o755))
	check(os.WriteFile(filepath.Join(dir, "hikyaku.schema.json"), b, 0o644))
	check(os.MkdirAll(filepath.Dir(page), 0o755))
	check(os.WriteFile(page, config.ReferenceMarkdown(), 0o644))
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}
