#!/usr/bin/env bash
# Fails when the user-facing docs disagree with the code or with each other:
#   1. CHANGELOG.md opens with the changelogTitle that semantic-release keeps;
#   2. the README has its fixed shape, its Install section matches
#      .goreleaser.yaml, its command block matches the cobra tree, its targets
#      table matches the config's target types and its links resolve;
#   3. CONTRIBUTING's repository layout names every module of go.work;
#   4. the CLI reference on the docs site names every command and flag the
#      cobra tree has (and no command that does not exist), the site's sidebar
#      matches its pages, and the agent skill names every command;
#   5. the same guarded words as check_readme_claims.sh are applied to the
#      skill and the docs pages.
# Checks 2 to 4 are Go tests, so they read the real command tree.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# 1. semantic-release strips changelogTitle from the top of CHANGELOG.md and
#    puts it back above each new release, so the two must match exactly.
python3 - <<'PY'
import json, sys
plugins = json.load(open(".releaserc.json"))["plugins"]
title = next(p[1]["changelogTitle"] for p in plugins if isinstance(p, list) and p[0] == "@semantic-release/changelog")
if not open("CHANGELOG.md", encoding="utf-8").read().startswith(title + "\n"):
    sys.exit("check-docs-claims: CHANGELOG.md does not start with changelogTitle from .releaserc.json")
PY
echo "check-docs-claims: changelog title ok"

# 2 to 4. README, CONTRIBUTING, the CLI reference, the sidebar and the skill
#         against the code.
export GOWORK=off
go test ./internal/cli -run 'TestReadme|TestContributing|TestDocs|TestSiteNav|TestSkill' -count=1
echo "check-docs-claims: README, CONTRIBUTING, docs and skill ok"

# 5. The skill and the docs pages make no unshipped claims either.
for f in $(find site/src/pages/docs -name '*.md' 2>/dev/null | sort); do
	[ -f "$f" ] && bash .github/scripts/check_readme_claims.sh "$f" >/dev/null
done
echo "check-docs-claims: skill and docs pages ok"
