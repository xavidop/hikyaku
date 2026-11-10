# Records site/public/demo.gif. Run `make demo` from the repository root: it
# builds ./bin/hikyaku with the fake model provider (-tags llmfake), so the AI
# example routes with no API key and no network.
Output site/public/demo.gif

Require ./bin/hikyaku

Set Shell "bash"
Set FontSize 18
Set Width 1400
Set Height 720
Set Framerate 15
Set Padding 12
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Env PS1 "$ "
# A stand-in for the webhook secret the examples' sources reference.
Env WEBHOOK_SECRET "demo-webhook-secret"

Hide
Type "export PATH=$PWD/bin:$PATH && clear"
Enter
Show

Type "hikyaku validate -f examples/webhook-fanout/hikyaku.yaml"
Enter
Sleep 2.5s

Type "hikyaku route --dry-run -f examples/ai-triage/hikyaku.llmfake.yaml --source webhooks --data @examples/ai-triage/msg.json"
Enter
Sleep 6s
