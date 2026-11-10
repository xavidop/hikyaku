// Command hikyaku-chat is hikyaku with two more types, the chat destination
// and the inbox source: every built-in protocol, the AI router and the CLI,
// plus plugins of its own, registered before the CLI runs.
package main

import (
	"os"

	"github.com/xavidop/hikyaku/cmd/hikyaku/app"
	"github.com/xavidop/hikyaku/plugin"
)

// register adds the chat destination type and the inbox source type.
func register() {
	plugin.RegisterSink("chat", newChat,
		plugin.WithOptions(ChatOptions{}),
		plugin.WithDescription("Posts each message as a text to a chat webhook."))
	plugin.RegisterSource("inbox", newInbox,
		plugin.WithOptions(InboxOptions{}),
		plugin.WithDescription("Sends each file dropped in a directory as a message, and removes it once the relay is done with it."))
}

func main() {
	register()
	os.Exit(app.Main(os.Args[1:], app.WithBuild("custom", "", "")))
}
