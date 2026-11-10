package llm

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
)

// Genkit's reflection server (what the Genkit Developer UI talks to)
// belongs to a Genkit instance. A reload that changes genkit: or the model
// providers builds a new factory, so a second instance starts next to the
// running one, with a reflection server of its own:
//
//   - by default it takes the next free port from 3100, writes its own
//     runtime file, and the Dev UI moves to it (the most recent runtime);
//     the old one stops, and removes its file, with the old engine;
//   - with GENKIT_REFLECTION_V2_SERVER (genkit start
//     --experimental-reflection-v2) it registers with the CLI under the
//     same runtime ID, which replaces the old registration;
//   - with GENKIT_REFLECTION_PORT pinned it would need the port the running
//     one holds, so New refuses it (secondReflectionServer).

// reflecting counts this process's factories whose Genkit instance runs a
// reflection server, until the context given to New ends.
var reflecting atomic.Int32

// reflectionOn reports whether genkit.Init starts a reflection server:
// under GENKIT_ENV=dev, or with GENKIT_REFLECTION_ENABLED=true.
func reflectionOn() bool {
	switch os.Getenv("GENKIT_REFLECTION_ENABLED") {
	case "true":
		return true
	case "":
		return os.Getenv("GENKIT_ENV") == "dev"
	}
	return false
}

// pinnedReflectionPort is the port GENKIT_REFLECTION_PORT holds a
// reflection server to: "" when Genkit picks one (unset, or 0), connects
// out instead (v2), or starts none.
func pinnedReflectionPort() string {
	if !reflectionOn() || os.Getenv("GENKIT_REFLECTION_V2_SERVER") != "" {
		return ""
	}
	p := os.Getenv("GENKIT_REFLECTION_PORT")
	if n, err := strconv.Atoi(p); err == nil && n == 0 {
		return ""
	}
	return p
}

// secondReflectionServer refuses a Genkit instance whose reflection server
// cannot start: its port is pinned, and another instance of the process
// holds it.
func secondReflectionServer() error {
	p := pinnedReflectionPort()
	if p == "" || reflecting.Load() == 0 {
		return nil
	}
	return restartError{fmt.Errorf("GENKIT_REFLECTION_PORT=%s pins the Genkit reflection server (the Dev UI) to one port, "+
		"and the running Genkit instance holds it: restart hikyaku to load the new Genkit plugins or model providers, "+
		"or unset GENKIT_REFLECTION_PORT so each instance takes a free port", p)}
}

// countReflectionServer counts a new instance's reflection server until
// ctx ends.
func countReflectionServer(ctx context.Context) {
	if !reflectionOn() {
		return
	}
	reflecting.Add(1)
	context.AfterFunc(ctx, func() { reflecting.Add(-1) })
}

// restartError is a change only a new process can load: a reload reports
// it as needs_restart.
type restartError struct{ error }

// NeedsRestart marks the error for the CLI's reload.
func (restartError) NeedsRestart() bool { return true }

func (e restartError) Unwrap() error { return e.error }
