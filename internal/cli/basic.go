package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/spf13/cobra"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/secrets"
)

func (a *app) newValidateCmd() *cobra.Command {
	var checkSecrets bool
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the spec, CEL, templates, references and fallback cycles",
		Long: "Validate the spec: unknown keys, dangling references, fallback cycles, AI candidates\n" +
			"that are not destinations, CEL, jq and template errors. --check-secrets also resolves\n" +
			"every declared secret.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			l, err := a.load(cmd.Context(), checkSecrets)
			if err != nil {
				return err
			}
			st := a.style(cmd.OutOrStdout())
			s := l.Spec
			fprintf(cmd.OutOrStdout(), "%s %s: valid (%d sources, %d destinations, %d routes)\n",
				st.OK("✓"), strings.Join(l.Files, ", "), len(s.Sources), len(s.Destinations), len(s.Routes))
			if checkSecrets && len(l.SecretRefs) > 0 {
				fprintf(cmd.OutOrStdout(), "%s %d secrets resolved\n", st.OK("✓"), len(l.SecretRefs))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&checkSecrets, "check-secrets", false, "resolve every declared secret too")
	return cmd
}

func (a *app) newLintCmd() *cobra.Command {
	var strict bool
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Warnings: unpinned decision models, missing fallbacks, open admin endpoints",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			l, err := a.load(cmd.Context(), false)
			if err != nil {
				return err
			}
			ws := config.Lint(l.Spec)
			st := a.style(cmd.OutOrStdout())
			for _, w := range ws {
				fprintf(cmd.OutOrStdout(), "%s %s\n", st.Warn("!"), w.String())
			}
			if len(ws) == 0 {
				fprintf(cmd.OutOrStdout(), "%s no warnings\n", st.OK("✓"))
				return nil
			}
			fprintf(cmd.OutOrStdout(), "%d warning(s)\n", len(ws))
			if strict {
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&strict, "strict", false, "exit 1 when there are warnings")
	return cmd
}

func (a *app) newSchemaCmd() *cobra.Command {
	var out string
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Print the JSON Schema of hikyaku.yaml",
		Long: "Print the JSON Schema of hikyaku.yaml, with the options of the plugin types this build adds,\n" +
			"and of the out-of-process plugins the spec (-f) lists, when there is one.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			set, x, err := a.startPlugins(cmd.Context(), config.LoadOptions{
				LookupEnv: secrets.EnvLookup(a.envFile, a.deps.LookupEnv), Extensions: extensions(),
			})
			if err != nil {
				return err
			}
			if set != nil {
				defer func() { _ = set.Close() }()
			}
			b, err := config.SchemaJSON(config.SchemaWith(x))
			if err != nil {
				return err
			}
			if out != "" {
				return os.WriteFile(out, b, 0o644)
			}
			_, err = cmd.OutOrStdout().Write(b)
			return err
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "write to this file instead of stdout")
	return cmd
}

func newVersionCmd(a *app) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version, commit and Genkit version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			genkit := "not linked"
			if a.deps.GenkitVersion != nil {
				genkit = a.deps.GenkitVersion()
			}
			v := map[string]string{
				"version": a.info.Version, "commit": orDash(a.info.Commit), "date": orDash(a.info.Date),
				"genkit": genkit, "go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH,
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(v)
			}
			fprintf(cmd.OutOrStdout(), "hikyaku %s (commit %s, built %s)\ngenkit %s\n%s %s\n", v["version"], v["commit"], v["date"], v["genkit"], v["go"], v["platform"])
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	return cmd
}

func orDash(s string) string {
	if s == "" || s == "none" || s == "unknown" {
		return "-"
	}
	return s
}

func readAll(r io.Reader) ([]byte, error) { return io.ReadAll(io.LimitReader(r, 64<<20)) }

func trimNL(b []byte) []byte { return bytes.TrimRight(b, "\r\n") }

func jsonValid(b []byte) bool { return len(b) > 0 && json.Valid(b) }

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// fprintf and fprintln write command output; a closed stdout is not an
// error worth a failed command.
func fprintf(w io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }

func fprintln(w io.Writer, args ...any) { _, _ = fmt.Fprintln(w, args...) }
