#!/usr/bin/env bash
# Runs govulncheck on every module and fails when the code reaches a
# vulnerability that .github/govulncheck-ignore does not list. govulncheck has
# no ignore list of its own; that file names the IDs whose risk is handled in
# hikyaku's code, each with the reason, so an upstream report without a fixed
# release does not block CI while the mitigation is in place. Vulnerabilities
# in modules the code does not call never fail the scan (govulncheck's own
# rule); ignored IDs are reported, not hidden.
#
# Usage: govulncheck.sh [module dir...]   (default: every module in the repo)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

govulncheck="${GOVULNCHECK:-govulncheck}"
if ! command -v "$govulncheck" >/dev/null; then
  echo "govulncheck is not installed: go install golang.org/x/vuln/cmd/govulncheck@latest" >&2
  exit 1
fi
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

# No file, or no IDs in it, means nothing is ignored.
ignored="$({ grep -oE '^GO-[0-9]{4}-[0-9]+' .github/govulncheck-ignore 2>/dev/null || true; } | jq -R . | jq -sc .)"

dirs=("$@")
if [ ${#dirs[@]} -eq 0 ]; then
  while IFS= read -r d; do dirs+=("$d"); done < <(find . -name go.mod \
    -not -path './site/*' -not -path './.claude/*' -not -path './node_modules/*' -not -path '*/testdata/*' \
    -exec dirname {} \; | sort)
fi

# reachable prints the IDs of the symbol-level findings (the code calls a
# vulnerable function) in govulncheck's JSON stream, keeping only the ignored
# ones or only the others.
reachable() { # <json> <ignored-ones: true|false>
  jq -r --argjson ignored "$ignored" --argjson keep "$2" \
    'select(.finding) | .finding | select(.trace[0].function != null)
     | select((.osv as $id | $ignored | index($id) != null) == $keep) | .osv' <<<"$1" | sort -u
}

failed=""
for dir in "${dirs[@]}"; do
  echo "==> govulncheck $dir"
  out="$(cd "$dir" && "$govulncheck" -format json ./...)"
  skipped="$(reachable "$out" true)"
  if [ -n "$skipped" ]; then
    echo "ignored (mitigated in code, see .github/govulncheck-ignore): $(tr '\n' ' ' <<<"$skipped")"
  fi
  found="$(reachable "$out" false)"
  if [ -n "$found" ]; then
    echo "reachable: $(tr '\n' ' ' <<<"$found")"
    (cd "$dir" && "$govulncheck" ./...) || true
    failed="$failed $dir"
  else
    echo "OK $dir"
  fi
done

if [ -n "$failed" ]; then
  echo "Modules with reachable vulnerabilities:$failed" >&2
  exit 1
fi
echo "No reachable vulnerabilities in any module (beyond the ignored IDs)."
