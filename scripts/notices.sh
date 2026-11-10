#!/usr/bin/env bash
# Writes the third-party license files from the modules the hikyaku binary
# links (make notices):
#   THIRD_PARTY_LICENSES.csv  module, license URL, license name
#   THIRD_PARTY_NOTICES.md    every license text, once per distinct text with
#                             the modules it covers, then the NOTICE files the
#                             Apache-2.0 license asks a distribution to carry.
# Both come from one go-licenses report of cmd/hikyaku (the full build; the
# lite build links a subset). Needs go-licenses and git.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v go-licenses >/dev/null || {
	echo "go-licenses is not installed: go install github.com/google/go-licenses@latest" >&2
	exit 1
}

work=$(mktemp -d)
cleanup() {
	rm -f "$work/report.tpl" "$work/rows.tsv" "$work/keyed.tsv"
	rmdir "$work"
}
trap cleanup EXIT

# One tab-separated row per module: name, version, license, URL, license file.
printf '{{ range . }}{{ .Name }}\t{{ .Version }}\t{{ .LicenseName }}\t{{ .LicenseURL }}\t{{ .LicensePath }}\n{{ end }}' >"$work/report.tpl"
(cd cmd/hikyaku && GOWORK=off go-licenses report . --ignore github.com/xavidop/hikyaku --template "$work/report.tpl" 2>/dev/null) |
	LC_ALL=C sort >"$work/rows.tsv"
[ -s "$work/rows.tsv" ] || { echo "notices: go-licenses reported no modules" >&2; exit 1; }

awk -F'\t' '{ printf "%s,%s,%s\n", $1, $4, $3 }' "$work/rows.tsv" >THIRD_PARTY_LICENSES.csv

# The same rows, keyed by the license text's hash, so identical texts group.
while IFS=$'\t' read -r name version license url path; do
	hash=$(git hash-object "$path")
	printf '%s\t%s\t%s\t%s\t%s\n' "$license" "$hash" "$name" "$version" "$path"
done <"$work/rows.tsv" | LC_ALL=C sort -t$'\t' -k1,1 -k2,2 -k3,3 >"$work/keyed.tsv"

module() { # name version
	if [ -n "$2" ]; then printf '%s %s' "$1" "$2"; else printf '%s' "$1"; fi
}

{
	echo "# Third-party notices"
	echo
	echo "The hikyaku binaries link the Go modules below. Each license text appears once,"
	echo "with the modules it covers, followed by the NOTICE files of the modules that"
	echo "ship one. \`make notices\` regenerates this file and THIRD_PARTY_LICENSES.csv."
	echo
	echo "## Licenses"
	prev=""
	members=""
	text=""
	flush() {
		[ -n "$prev" ] || return 0
		echo
		echo "### ${prev%%$'\t'*}"
		echo
		echo "Covers: $members."
		echo
		echo '```text'
		sed -e 's/[[:space:]]*$//' "$text"
		echo '```'
	}
	while IFS=$'\t' read -r license hash name version path; do
		key="$license"$'\t'"$hash"
		if [ "$key" != "$prev" ]; then
			flush
			prev="$key"
			members="$(module "$name" "$version")"
			text="$path"
		else
			members="$members, $(module "$name" "$version")"
		fi
	done <"$work/keyed.tsv"
	flush

	echo
	echo "## NOTICE files"
	while IFS=$'\t' read -r name version license url path; do
		dir=$(dirname "$path")
		for n in NOTICE NOTICE.txt NOTICE.md; do
			if [ -f "$dir/$n" ]; then
				echo
				echo "### $(module "$name" "$version")"
				echo
				echo '```text'
				sed -e 's/[[:space:]]*$//' "$dir/$n"
				echo '```'
				break
			fi
		done
	done <"$work/rows.tsv"
} >THIRD_PARTY_NOTICES.md

echo "THIRD_PARTY_LICENSES.csv: $(wc -l <THIRD_PARTY_LICENSES.csv | tr -d ' ') modules"
echo "THIRD_PARTY_NOTICES.md: $(grep -c '^### ' THIRD_PARTY_NOTICES.md | tr -d ' ') sections, $(wc -c <THIRD_PARTY_NOTICES.md | tr -d ' ') bytes"
