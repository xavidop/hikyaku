package cli

import (
	"io"
	"regexp"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

// The help and usage screens are cobra's defaults with the headings, the
// command names and the flag names passed through the app's Style, so they
// are coloured on a terminal and plain everywhere else. The template funcs
// take the command to find its app, so several roots in one process (tests)
// each keep their own --color.

// styleOf is the style of the app that owns cmd, for output to w.
func styleOf(cmd *cobra.Command, w io.Writer) Style {
	if v, ok := apps.Load(cmd.Root()); ok {
		return v.(*app).style(w)
	}
	return Style{}
}

var helpFuncsOnce sync.Once

// flagName matches the "-x, --flag" or "--flag" at the start of a flag usage line.
var flagName = regexp.MustCompile(`^(\s+)(-[^\s,]+(?:, --[^\s]+)?|--[^\s]+)`)

func registerHelpFuncs() {
	helpFuncsOnce.Do(func() {
		cobra.AddTemplateFunc("heading", func(c *cobra.Command, s string) string {
			return styleOf(c, c.OutOrStdout()).Bold(s)
		})
		cobra.AddTemplateFunc("cmdname", func(c *cobra.Command, s string) string {
			return styleOf(c, c.OutOrStdout()).Info(s)
		})
		cobra.AddTemplateFunc("tagline", func(c *cobra.Command, s string) string {
			first, rest, found := strings.Cut(s, "\n")
			first = styleOf(c, c.OutOrStdout()).Bold(first)
			if !found {
				return first
			}
			return first + "\n" + rest
		})
		cobra.AddTemplateFunc("flags", func(c *cobra.Command, s string) string {
			st := styleOf(c, c.OutOrStdout())
			if !st.Enabled() {
				return s
			}
			lines := strings.Split(s, "\n")
			for i, line := range lines {
				lines[i] = flagName.ReplaceAllStringFunc(line, func(m string) string {
					sub := flagName.FindStringSubmatch(m)
					return sub[1] + st.Info(sub[2])
				})
			}
			return strings.Join(lines, "\n")
		})
	})
}

// usageTemplate is cobra's default usage template with the styling funcs.
const usageTemplate = `{{heading . "Usage:"}}{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

{{heading . "Aliases:"}}
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

{{heading . "Examples:"}}
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}{{$cmds := .Commands}}{{if eq (len .Groups) 0}}

{{heading . "Available Commands:"}}{{range $cmds}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{cmdname . (rpad .Name .NamePadding)}} {{.Short}}{{end}}{{end}}{{else}}{{range $group := .Groups}}

{{heading $ .Title}}{{range $cmds}}{{if (and (eq .GroupID $group.ID) (or .IsAvailableCommand (eq .Name "help")))}}
  {{cmdname . (rpad .Name .NamePadding)}} {{.Short}}{{end}}{{end}}{{end}}{{if not .AllChildCommandsHaveGroup}}

{{heading . "Additional Commands:"}}{{range $cmds}}{{if (and (eq .GroupID "") (or .IsAvailableCommand (eq .Name "help")))}}
  {{cmdname . (rpad .Name .NamePadding)}} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

{{heading . "Flags:"}}
{{flags . (.LocalFlags.FlagUsages | trimTrailingWhitespaces)}}{{end}}{{if .HasAvailableInheritedFlags}}

{{heading . "Global Flags:"}}
{{flags . (.InheritedFlags.FlagUsages | trimTrailingWhitespaces)}}{{end}}{{if .HasHelpSubCommands}}

{{heading . "Additional help topics:"}}{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{cmdname . (rpad .CommandPath .CommandPathPadding)}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

// helpTemplate is cobra's default help template with a bold first line.
const helpTemplate = `{{with (or .Long .Short)}}{{tagline $ (. | trimTrailingWhitespaces)}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}`
