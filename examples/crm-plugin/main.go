// Command hikyaku-crm is an out-of-process hikyaku plugin that adds the crm
// destination type: each message is a lead, posted to a CRM's HTTP API.
// hikyaku starts it from a spec and talks gRPC to it:
//
//	plugins:
//	  - name: crm
//	    command: ./bin/hikyaku-crm
//	    args: ["--region", "eu"]
//	    env: { CRM_TOKEN: "{{secrets.CRM_TOKEN}}" }
//	destinations:
//	  - { name: leads, type: crm, options: { pipeline: inbound } }
//
// It makes one attempt per delivery and says how it failed: hikyaku owns the
// retries, the circuit breaker, fallbacks and the dead-letter queue.
package main

import (
	"flag"
	"os"

	"github.com/xavidop/hikyaku/grpcplugin"
)

func main() {
	region := flag.String("region", "eu", "The CRM's region: the default API host, and the X-CRM-Region header.")
	flag.Parse()
	grpcplugin.Serve(grpcplugin.Plugins{
		Sinks: map[string]grpcplugin.SinkDef{
			"crm": {
				Factory:     newCRM(*region, os.Getenv("CRM_TOKEN")),
				Options:     CRMOptions{},
				Description: "Posts each message as a lead to a CRM pipeline.",
			},
		},
	})
}
