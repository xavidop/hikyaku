// Command hikyaku-plugin-echo is an example out-of-process hikyaku plugin.
// It serves two types:
//
//   - echo, a destination that appends each message body to a file (one
//     line per message) and replies with the body;
//   - ticker, a source that emits {"n":i} every interval and keeps each
//     message until hikyaku settles it, emitting again the ones that failed.
//
// Build it, list it under plugins: in a spec, and use its types like any
// other:
//
//	plugins:
//	  - { name: echo, command: ./hikyaku-plugin-echo }
//	sources:
//	  - { name: ticks, type: ticker, options: { interval: 1s } }
//	destinations:
//	  - { name: out, type: echo, options: { file: out/echo.ndjson } }
//	routes:
//	  - { name: r, from: [ticks], to: [out] }
package main

import (
	"github.com/xavidop/hikyaku/grpcplugin"
)

func main() {
	grpcplugin.Serve(grpcplugin.Plugins{
		Sinks: map[string]grpcplugin.SinkDef{
			"echo": {
				Factory:     newEcho,
				Options:     EchoOptions{},
				Description: "Appends each message body to a file and replies with it.",
			},
		},
		Sources: map[string]grpcplugin.SourceDef{
			"ticker": {
				Factory:     newTicker,
				Options:     TickerOptions{},
				Description: `Emits {"n":i} every interval; a message that fails is emitted again.`,
			},
		},
	})
}
