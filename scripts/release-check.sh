#!/usr/bin/env bash
# The Go module release end to end (make release-check). internal/tools/release
# cuts a release; this checks that the go command can use it from outside the
# repository:
#   1. the go command, building each module from its tag (GOPROXY=direct, git
#      reading this repository), gets the hashes the release serves: what
#      proxy.golang.org builds from the tags is what the go.sum files hold;
#   2. from a scratch module, go install builds the CLI, full and lite, and
#      its version and validate work;
#   3. a custom build like examples/custom-destination builds there after
#      go get github.com/xavidop/hikyaku/cmd/hikyaku@<version>: its validate
#      accepts its chat destination, which the stock CLI's rejects.
#
# Usage:
#   scripts/release-check.sh
#       Cuts a throwaway release of HEAD (RELEASE_CHECK_VERSION, default
#       v1.99.99-test) in a scratch clone and checks it. Nothing is tagged in
#       this repository.
#   scripts/release-check.sh --version vX.Y.Z --proxy DIR
#       Checks the release the tool cut in this repository, whose tags are
#       not pushed yet, with its file proxy in DIR.
#   scripts/release-check.sh --version vX.Y.Z --public
#       Checks a pushed release through proxy.golang.org and the checksum
#       database alone. It retries while the proxy fetches the new tags.
#
# Third-party modules come from the local module cache, then
# https://proxy.golang.org: no other network.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
module=github.com/xavidop/hikyaku
version="${RELEASE_CHECK_VERSION:-v1.99.99-test}"
proxy=""
public=false
while [ $# -gt 0 ]; do
	case "$1" in
	--version) version="$2"; shift 2 ;;
	--proxy) proxy="$(cd "$2" && pwd)"; shift 2 ;;
	--public) public=true; shift ;;
	*) echo "usage: $0 [--version vX.Y.Z (--proxy DIR | --public)]" >&2; exit 2 ;;
	esac
done

fail() {
	echo "release-check: FAIL: $*" >&2
	exit 1
}

# retry runs a command, and with --public runs it again while proxy.golang.org
# has not fetched the new tags yet.
retry() {
	local n=1
	until "$@"; do
		if [ "$public" != true ] || [ "$n" -ge 8 ]; then
			return 1
		fi
		echo "    proxy.golang.org may not have the tags yet: again in 30s" >&2
		sleep 30
		n=$((n + 1))
	done
}

work="$(mktemp -d)"
trap 'chmod -R u+w "$work" 2>/dev/null || true; rm -rf "$work"' EXIT
cache="$(go env GOMODCACHE)/cache/download"

repo="$root"
if [ -z "$proxy" ] && [ "$public" = false ]; then
	echo "==> cut $version of HEAD in a scratch clone"
	repo="$work/repo"
	git clone --quiet --shared --no-checkout "$root" "$repo"
	proxy="$work/proxy"
	(cd "$root/internal/tools" && GOWORK=off go run ./release \
		-repo "$repo" -version "$version" -commit "$(git -C "$root" rev-parse HEAD)" -proxy "$proxy") >/dev/null
fi
base="$(git -C "$repo" rev-parse --verify --quiet "refs/tags/$version^{commit}")" ||
	fail "$repo has no tag $version"

# Every go command from here on sees only what this script sets: no go env
# file, no workspace, a module cache of its own (so no version comes from an
# earlier run), and -trimpath, so the build cache serves repeated runs.
export GOENV=off GOWORK=off GOTOOLCHAIN=local GOPRIVATE='' GONOPROXY='' GOINSECURE=''
export GOFLAGS="-mod=mod -modcacherw -trimpath"
export GOMODCACHE="$work/gomodcache"
if [ "$public" = true ]; then
	export GOPROXY=https://proxy.golang.org GONOSUMDB='' GOSUMDB=sum.golang.org
else
	third=https://proxy.golang.org
	[ -d "$cache" ] && third="file://$cache,$third"
	export GOPROXY="file://$proxy,$third" GONOSUMDB="$module"
fi

# The modules of the release, from its tags.
args=()
while read -r tag; do
	if [ "$tag" = "$version" ]; then
		args+=("$module@$version")
	else
		args+=("$module/${tag%/"$version"}@$version")
	fi
done < <(git -C "$repo" tag --list "$version" "*/$version")
[ "${#args[@]}" -gt 1 ] || fail "$repo has no module tags */$version"

# download <name> <env...>: go mod download -json of every module, into
# $work/<name>, keeping the fields that must agree (path, version and
# hashes; a direct download has an Origin after them, hence the commas).
download() {
	local name="$1"
	shift
	env "$@" go mod download -json "${args[@]}" >"$work/$name.json" 2>"$work/$name.err" || {
		cat "$work/$name.err" >&2
		grep '"Error"' "$work/$name.json" >&2 || true
		return 1
	}
	grep -E '"(Path|Version|Sum|GoModSum)":' "$work/$name.json" | sed 's/,$//' >"$work/$name"
}

echo "==> the go command builds the same ${#args[@]} modules from the tags (GOPROXY=direct)"
printf '[url "%s"]\n\tinsteadOf = https://%s\n' "$repo" "$module" >"$work/gitconfig"
download direct GOPROXY=direct GIT_CONFIG_GLOBAL="$work/gitconfig" GIT_CONFIG_NOSYSTEM=1 \
	GONOSUMDB="$module" GOMODCACHE="$work/cache-direct" || fail "go mod download from the tags"
retry download served GOMODCACHE="$work/cache-served" || fail "go mod download from $GOPROXY"
diff -u "$work/direct" "$work/served" || fail "the release does not serve the zips the go command builds from its tags"

mkdir "$work/scratch" "$work/spec" "$work/custom"
cd "$work/scratch"
go mod init example.com/release-check >/dev/null 2>&1

echo "==> go install $module/cmd/hikyaku@$version"
GOBIN="$work/full" retry go install "$module/cmd/hikyaku@$version" || fail "go install"
"$work/full/hikyaku" version | tee "$work/version"
grep -q "^hikyaku $version " "$work/version" || fail "hikyaku version does not report $version"
(cd "$work/spec" && "$work/full/hikyaku" init >/dev/null && "$work/full/hikyaku" validate -f hikyaku.yaml) ||
	fail "the installed CLI does not validate the spec hikyaku init writes"

echo "==> go install -tags lite $module/cmd/hikyaku@$version"
GOBIN="$work/lite" go install -tags lite "$module/cmd/hikyaku@$version" || fail "go install -tags lite"
go version -m "$work/lite/hikyaku" | grep -q -- '-tags=lite' || fail "the lite install is not a -tags lite build"
"$work/lite/hikyaku" version | grep -q "^hikyaku $version " || fail "the lite CLI does not report $version"
(cd "$work/spec" && "$work/lite/hikyaku" validate -f hikyaku.yaml) || fail "the lite CLI does not validate the spec"

echo "==> a custom build, after go get $module/cmd/hikyaku@$version"
cd "$work/custom"
for f in main.go chat.go hikyaku.yaml; do
	git -C "$repo" show "$base:examples/custom-destination/$f" >"$f"
done
go mod init example.com/hikyaku-chat >/dev/null 2>&1
go get "$module/cmd/hikyaku@$version"
go mod tidy
go build -o hikyaku-chat .
export CHAT_WEBHOOK_URL=https://chat.example.invalid/hook
./hikyaku-chat validate -f hikyaku.yaml || fail "the custom build does not validate a spec with its chat destination"
if "$work/full/hikyaku" validate -f hikyaku.yaml >/dev/null 2>&1; then
	fail "the stock CLI validates the chat spec too, so validate proves nothing"
fi

echo "release-check: $version installs from outside the repository"
