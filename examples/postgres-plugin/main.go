// Command hikyaku-postgres is an out-of-process hikyaku plugin that adds two
// PostgreSQL types:
//
//   - the postgres destination inserts one row per message into a table;
//   - the postgres source is a transactional outbox: it polls a table for
//     rows nobody delivered yet, emits each as a message, and marks the row
//     delivered (or deletes it) once hikyaku is done with it.
//
// hikyaku starts it from a spec and talks gRPC to it. The database comes from
// DATABASE_URL in the plugin's env:
//
//	plugins:
//	  - name: postgres
//	    command: ./bin/hikyaku-postgres
//	    env: { DATABASE_URL: "{{secrets.DATABASE_URL}}" }
//	sources:
//	  - { name: outbox, type: postgres, options: { table: outbox } }
//	destinations:
//	  - { name: events, type: postgres, options: { table: events } }
//
// The destination makes one attempt per delivery and says how it failed:
// hikyaku owns the retries, the circuit breaker, fallbacks and the
// dead-letter queue.
package main

import (
	"flag"
	"os"

	"github.com/xavidop/hikyaku/grpcplugin"
)

func main() {
	maxConns := flag.Int("max-conns", 4, "Connections each source or destination may open.")
	flag.Parse()
	db := &database{url: os.Getenv("DATABASE_URL"), maxConns: int32(*maxConns)}
	grpcplugin.Serve(grpcplugin.Plugins{
		Sinks: map[string]grpcplugin.SinkDef{
			"postgres": {
				Factory:     db.newSink,
				Options:     SinkOptions{},
				Description: "Inserts one row per message into a PostgreSQL table.",
			},
		},
		Sources: map[string]grpcplugin.SourceDef{
			"postgres": {
				Factory:     db.newSource,
				Options:     SourceOptions{},
				Description: "A transactional outbox: polls a table and emits each undelivered row as a message.",
			},
		},
	})
}
