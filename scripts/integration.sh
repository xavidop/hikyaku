#!/usr/bin/env bash
# Runs the examples end to end with the fake model provider (make
# integration): builds bin/hikyaku with -tags llmfake, validates every
# example spec, then runs each offline spec and drives it like a user would.
# Needs curl and openssl; the wss-to-http step needs node (22+, for its
# built-in WebSocket client) and is skipped without it.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
bin="$root/bin/hikyaku"
work="$(mktemp -d)"
pids=()

cleanup() {
	for p in "${pids[@]:-}"; do
		[ -n "$p" ] && kill "$p" 2>/dev/null || true
	done
	rm -rf "$work"
}
trap cleanup EXIT

fail() {
	echo "integration: FAIL: $*" >&2
	for f in "$work"/*.err; do
		[ -f "$f" ] && { echo "--- $f" >&2; tail -20 "$f" >&2; }
	done
	exit 1
}

need() { command -v "$1" >/dev/null || fail "$1 is not installed"; }
need curl
need openssl

wait_ready() {
	for _ in $(seq 1 100); do
		curl -sf "localhost:9090/healthz" >/dev/null 2>&1 && return 0
		sleep 0.1
	done
	fail "the relay did not come up"
}

# start <dir> <spec> [binary]: runs a relay from dir in the background.
start() {
	local dir="$1" spec="$2" b="${3:-$bin}" name
	name="$(basename "$dir")"
	(cd "$dir" && exec "$b" run -f "$spec" --color never >"$work/$name.out" 2>"$work/$name.err") &
	pids+=("$!")
	wait_ready
}

stop() {
	local last=$((${#pids[@]} - 1))
	kill -TERM "${pids[$last]}" 2>/dev/null || true
	wait "${pids[$last]}" 2>/dev/null || true
	unset "pids[$last]"
	pids=("${pids[@]:-}")
	sleep 0.2
}

contains() { grep -q -- "$2" "$1" 2>/dev/null || fail "$1 does not contain $2"; }

echo "==> build bin/hikyaku (-tags llmfake)"
(cd cmd/hikyaku && GOWORK=off go build -tags llmfake -o "$bin" .)

echo "==> validate every example spec"
for f in examples/*/hikyaku*.yaml; do
	case "$f" in *custom-destination* | *crm-plugin* | *postgres-plugin*) continue ;; esac # their types need their binaries; postgres-plugin needs a database
	case "$f" in *.test.yaml) continue ;; esac # tests of an example, not a spec
	"$bin" validate -f "$f" --color never
done

echo "==> run the tests every example ships"
for f in examples/*/hikyaku.test.yaml; do
	"$bin" test -f "$(dirname "$f")/hikyaku.yaml" "$f" --color never
done

echo "==> webhook-fanout: signed webhook, retries, fallback, best-effort"
ex="$work/webhook-fanout"
cp -r examples/webhook-fanout "$ex"
export WEBHOOK_SECRET=s3cret
start "$ex" hikyaku.llmfake.yaml
body='{"type":"invoice.paid","id":"evt_1"}'
sig="$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" -hex | sed 's/^.* //')"
code="$(curl -s -o "$work/resp" -w '%{http_code}' localhost:8080/ingest -H 'Content-Type: application/json' \
	-H "X-Signature-256: sha256=$sig" -H 'Idempotency-Key: evt_1' -d "$body")"
[ "$code" = 200 ] || fail "the signed webhook answered $code: $(cat "$work/resp")"
code="$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/ingest -d "$body")"
[ "$code" = 401 ] || fail "an unsigned webhook answered $code, want 401"
contains "$ex/out/archive.ndjson" '"id":"evt_1"'
contains "$ex/out/spool.ndjson" 'evt_1'
curl -s localhost:9090/metrics >"$work/metrics"
contains "$work/metrics" 'hikyaku_fallback_total{from="flaky",reason="exhausted",to="spool"} 1'
contains "$work/metrics" 'hikyaku_delivery_attempts_total{destination="billing",endpoint="billing",outcome="ok"} 1'
contains "$work/metrics" 'hikyaku_delivery_dropped_total{destination="analytics"} 1'
stop

echo "==> ai-triage: model fallback, history, sticky follow-up"
ex="$work/ai-triage"
cp -r examples/ai-triage "$ex"
start "$ex" hikyaku.llmfake.yaml
curl -sf localhost:8080/events -H 'Content-Type: application/json' -d @"$ex/msg.json" >/dev/null || fail "the first event was refused"
contains "$ex/out/billing.ndjson" 'charged twice'
sleep 0.3 # the call record is appended after delivery, asynchronously
curl -sf localhost:8080/events -H 'Content-Type: application/json' \
	-d '{"customer_id":"cus_42","text":"Still no refund, following up again on the same invoice."}' >/dev/null
[ "$(grep -c '' "$ex/out/billing.ndjson")" = 2 ] || fail "the follow-up did not stick to billing"
(cd "$ex" && "$bin" history show -f hikyaku.llmfake.yaml --route triage --key cus_42 --color never) >"$work/history"
contains "$work/history" 'routed to billing'
stop
(cd "$ex" && "$bin" ai test -f hikyaku.llmfake.yaml --route triage --cases cases.yaml --min-accuracy 0.6 --color never) >/dev/null ||
	fail "ai test fell below 60% accuracy"

if command -v node >/dev/null && node -e 'process.exit(typeof WebSocket === "function" ? 0 : 1)'; then
	echo "==> wss-to-http: WebSocket in, filtered and reshaped, HTTP out"
	ex="$work/wss-to-http"
	cp -r examples/wss-to-http "$ex"
	start "$ex" hikyaku.llmfake.yaml
	node -e 'const w=new WebSocket("ws://localhost:8081/events");w.onopen=()=>{w.send(JSON.stringify({id:"e1",type:"order.created",ts:1,payload:{total:42}}));w.send(JSON.stringify({type:"heartbeat"}));setTimeout(()=>{w.close();process.exit(0)},300)};w.onerror=()=>process.exit(1)'
	sleep 0.5
	contains "$ex/out/api.ndjson" '"type":"order.created"'
	! grep -q heartbeat "$ex/out/api.ndjson" || fail "the heartbeat was not filtered out"
	[ "$(grep -c '' "$ex/out/archive.ndjson")" = 2 ] || fail "the archive should hold both events"
	stop
else
	echo "==> wss-to-http: skipped (node 22+ not found)"
fi

echo "==> custom-destination: a plugin type in a custom build"
ex="$work/custom-destination"
cp -r examples/custom-destination "$ex"
(cd examples/custom-destination && GOWORK=off go build -o "$work/hikyaku-chat" .)
start "$ex" hikyaku.llmfake.yaml "$work/hikyaku-chat"
curl -sf localhost:8080/alerts -d '{"severity":"high","summary":"disk full on db-1"}' >/dev/null || fail "the alert was refused"
contains "$ex/out/chat.ndjson" '"text":"high: disk full on db-1"'
stop

echo "==> crm-plugin: an out-of-process plugin"
ex="$work/crm-plugin"
cp -r examples/crm-plugin "$ex"
(cd examples/crm-plugin && GOWORK=off go build -o "$ex/bin/hikyaku-crm" .)
CRM_TOKEN=dev-token start "$ex" hikyaku.llmfake.yaml
curl -sf localhost:8080/leads -d '{"email":"ada@example.com","name":"Ada"}' >/dev/null || fail "the lead was refused"
contains "$ex/out/crm.ndjson" '"email":"ada@example.com"'
curl -s localhost:8080/leads -d '{"name":"Bob"}' >/dev/null
contains "$ex/out/dlq.ndjson" "it has no email"
stop

echo "integration: ok"
