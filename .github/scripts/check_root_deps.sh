#!/usr/bin/env bash
# Fails if the module in <moddir> (default: .) depends, directly or
# transitively, on a heavy SDK that belongs in a leaf module: Genkit and the
# model SDKs (llm/), LiveKit and pion (targets/livekit, targets/sip), chromedp
# (targets/browser), CGO audio (audio/), or a cloud SDK (the mamori providers
# are linked by cmd/hikyaku only).
set -euo pipefail

moddir="${1:-.}"
denylist='^(github\.com/firebase/genkit|google\.golang\.org/genai|github\.com/openai/openai-go|github\.com/anthropics/anthropic-sdk-go|github\.com/redis/go-redis|github\.com/twmb/franz-go|github\.com/nats-io/|github\.com/eclipse/paho|github\.com/rabbitmq/amqp091-go|github\.com/aws/aws-sdk-go|cloud\.google\.com/go|github\.com/Azure/azure-sdk-for-go|k8s\.io/|github\.com/hashicorp/vault|github\.com/testcontainers/|github\.com/xavidop/mamori/providers/)'

# The modules of the packages the root module actually compiles (its tests
# included). `go list -m all` would also list what dependencies merely
# mention in their go.mod (grpc names cloud.google.com/go/auth for its xds
# code, which nothing here builds).
mods="$(cd "$moddir" && GOWORK=off go list -deps -test -f '{{with .Module}}{{.Path}}{{end}}' ./... | sort -u)"
offenders="$(printf '%s\n' "$mods" | awk '{print $1}' | grep -E "$denylist" || true)"

if [ -n "$offenders" ]; then
	echo "check-root-deps: $moddir must not depend on Genkit, model SDKs, Redis, broker or cloud SDKs; found:" >&2
	printf '  %s\n' $offenders >&2
	exit 1
fi
echo "check-root-deps: $moddir is free of heavy SDKs"
