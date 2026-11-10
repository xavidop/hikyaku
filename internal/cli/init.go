package cli

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

//go:embed templates
var templates embed.FS

// Templates lists the init templates.
func Templates() []string {
	entries, _ := fs.ReadDir(templates, "templates")
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	slices.Sort(out)
	return out
}

func (a *app) newInitCmd() *cobra.Command {
	var tmpl, dir string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Scaffold a spec from a template",
		Long: "Scaffold a spec: " + strings.Join(Templates(), ", ") + ".\n" +
			"ai-triage also writes msg.json and cases.yaml for route --dry-run and ai test.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !slices.Contains(Templates(), tmpl) {
				return usageError(fmt.Errorf("unknown template %q (one of %s)", tmpl, strings.Join(Templates(), ", ")))
			}
			root := "templates/" + tmpl
			entries, err := fs.ReadDir(templates, root)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if p := filepath.Join(dir, e.Name()); !force {
					if _, err := os.Stat(p); err == nil {
						return usageError(fmt.Errorf("%s exists (use --force to overwrite)", p))
					}
				}
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
			st := a.style(cmd.OutOrStdout())
			for _, e := range entries {
				b, err := fs.ReadFile(templates, root+"/"+e.Name())
				if err != nil {
					return err
				}
				p := filepath.Join(dir, e.Name())
				if err := os.WriteFile(p, b, 0o644); err != nil {
					return err
				}
				fprintf(cmd.OutOrStdout(), "%s wrote %s\n", st.OK("✓"), p)
			}
			spec := filepath.Join(dir, "hikyaku.yaml")
			fprintf(cmd.OutOrStdout(), "\nNext: hikyaku validate -f %s, then hikyaku run -f %s\n", spec, spec)
			return nil
		},
	}
	cmd.Flags().StringVar(&tmpl, "template", "webhook-fanout", "template: "+strings.Join(Templates(), ", "))
	cmd.Flags().StringVar(&dir, "dir", ".", "directory to write into")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}
