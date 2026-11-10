#!/usr/bin/env bash
# The resilience suite through the CLI (make chaos-e2e): hikyaku chaos
# pushes messages through fault-injected destinations of the webhook-fanout
# example. Whatever the faults, no message may be lost: each one reaches its
# destination, a fallback, or the DLQ, from which dlq replay recovers it. The
# last step kills a relay with a disk buffer (kill -9) and checks that the
# next start delivers every message the first one accepted.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
work="$(mktemp -d)"
# Background relays die with the script, even when a step fails.
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$work"' EXIT
bin="$work/hikyaku"

fail() {
	echo "chaos-e2e: FAIL: $*" >&2
	exit 1
}

# field <json file> <key>: one number from a chaos summary.
field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }

echo "==> build hikyaku"
(cd cmd/hikyaku && GOWORK=off go build -o "$bin" .)
cp -r examples/webhook-fanout "$work/ex"
cd "$work/ex"
export WEBHOOK_SECRET=s3cret
spec=hikyaku.llmfake.yaml

echo "==> half of every attempt fails: retries absorb it"
"$bin" chaos -f "$spec" --destination archive --fail 50% --seed 7 --data '{"n":1}' --count 100 --json >"$work/half.json"
[ "$(field "$work/half.json" lost)" = 0 ] || fail "messages were lost at 50% faults"
total=$(($(field "$work/half.json" delivered_primary) + $(field "$work/half.json" delivered_fallback) + $(field "$work/half.json" dead_lettered)))
[ "$total" = 100 ] || fail "only $total of 100 messages are accounted for"

echo "==> billing is down: its fallback takes every message"
"$bin" chaos -f "$spec" --destination billing --fail 100% --data '{"n":2}' --count 20 --json >"$work/down.json"
python3 - "$work/down.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
assert s["lost"] == 0, s
assert s["served_by"].get("billing-dr") == 20, s
# billing's circuit breaker opens once min_requests (20) attempts failed:
# 20 attempts on billing in all, then billing-dr alone for every message.
assert s["attempts"] == 20 + 20, s
PY

echo "==> a destination without a fallback is down: the DLQ holds everything"
rm -f out/dlq.ndjson
"$bin" chaos -f "$spec" --destination archive --fail 100% --data '{"n":3}' --count 5 --json >"$work/dlq.json"
[ "$(field "$work/dlq.json" dead_lettered)" = 5 ] || fail "want 5 dead letters: $(cat "$work/dlq.json")"
[ "$(field "$work/dlq.json" lost)" = 0 ] || fail "messages were lost"
"$bin" dlq list -f "$spec" --color never | grep -q '5 dead letter(s)' || fail "dlq list does not show the 5 dead letters"

echo "==> the destination is back: dlq replay delivers every dead letter"
"$bin" dlq replay -f "$spec" --color never >"$work/replay"
[ "$(grep -c 'delivered' "$work/replay")" = 5 ] || fail "replay: $(cat "$work/replay")"

echo "==> timeouts: a destination slower than its timeout fails over"
"$bin" chaos -f "$spec" --destination billing --error timeout --fail 100% --data '{"n":4}' --count 3 --json >"$work/timeout.json"
python3 - "$work/timeout.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
assert s["lost"] == 0 and s["served_by"].get("billing-dr") == 3, s
PY

echo "==> kill -9 with a disk buffer: every accepted message survives"
k9="$work/kill9"
mkdir -p "$k9"
cat >"$k9/down.yaml" <<YAML
version: 1
name: kill9
buffer: { type: disk, path: $k9/buffer.db }
sources: [{ name: in, type: http, listen: "127.0.0.1:18480", ack: accepted }]
destinations: [{ name: out, type: fault, fault: { fail: 100% }, retry: { attempts: 1 } }]
dead_letter: { type: fault, fault: { fail: 100% } }
routes: [{ name: r, from: [in], to: [out] }]
server: { admin: { listen: "127.0.0.1:18490" } }
YAML
sed -e 's|type: fault, fault: { fail: 100% }, retry: { attempts: 1 }|type: file, path: '"$k9"'/delivered.ndjson, format: body|' \
	-e 's|dead_letter: { type: fault, fault: { fail: 100% } }|dead_letter: { type: stderr }|' "$k9/down.yaml" >"$k9/up.yaml"
# started relay: fail when another process already answers on the port, so a
# leftover relay can never pass for this one.
started() {
	for _ in $(seq 1 100); do
		kill -0 "$1" 2>/dev/null || fail "the relay exited: $(tail -3 "$2")"
		curl -sf 127.0.0.1:18490/healthz >/dev/null 2>&1 && return 0
		sleep 0.1
	done
	fail "the relay did not come up: $(tail -3 "$2")"
}
if curl -s -o /dev/null 127.0.0.1:18490/healthz 2>/dev/null || curl -s -o /dev/null 127.0.0.1:18480/ 2>/dev/null; then
	fail "ports 18480/18490 are taken by another process"
fi
"$bin" run -f "$k9/down.yaml" --color never >"$k9/down.log" 2>&1 &
pid=$!
started "$pid" "$k9/down.log"
for i in $(seq 1 20); do
	code="$(curl -s -o /dev/null -w '%{http_code}' 127.0.0.1:18480/ -d "{\"n\":$i}")"
	[ "$code" = 202 ] || fail "message $i was answered $code, want 202 (accepted once on disk)"
done
kill -9 "$pid"
wait "$pid" 2>/dev/null || true
"$bin" run -f "$k9/up.yaml" --color never >"$k9/up.log" 2>&1 &
pid=$!
started "$pid" "$k9/up.log"
for _ in $(seq 1 100); do
	[ "$(grep -c '' "$k9/delivered.ndjson" 2>/dev/null || echo 0)" -ge 20 ] && break
	sleep 0.1
done
kill -TERM "$pid"
wait "$pid" 2>/dev/null || true
got="$(sort -u "$k9/delivered.ndjson" | grep -c '')"
[ "$got" = 20 ] || fail "after kill -9 the next start delivered $got of 20 accepted messages: $(tail -5 "$k9/up.log")"

echo "chaos-e2e: ok"
