package llm

import "runtime/debug"

// GenkitVersion is the version of Genkit Go linked into the binary.
func GenkitVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, d := range bi.Deps {
		if d.Path == "github.com/firebase/genkit/go" {
			if d.Replace != nil {
				return d.Replace.Version
			}
			return d.Version
		}
	}
	return "unknown"
}
