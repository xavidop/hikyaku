#!/usr/bin/env bash
# Builds the container image locally (make docker), tagged
# ghcr.io/xavidop/hikyaku:dev unless an image name is given.
#
# The build context in dist/docker has the layout GoReleaser's dockers_v2 gives
# the release build, so the one Dockerfile serves both: the linux binary at
# linux/<arch>/hikyaku, plus LICENSE and, when they exist, the THIRD_PARTY_*
# notices. <arch> is the Docker daemon's architecture, so the image runs
# natively; set ARCH=amd64 or ARCH=arm64 to build the other one.
#
#   scripts/docker-build.sh [image]
set -euo pipefail
cd "$(dirname "$0")/.."

image="${1:-ghcr.io/xavidop/hikyaku:dev}"
command -v docker >/dev/null || { echo "docker is not installed" >&2; exit 1; }
arch="${ARCH:-$(docker version --format '{{.Server.Arch}}' 2>/dev/null || go env GOARCH)}"

ctx=dist/docker
rm -rf "$ctx"
mkdir -p "$ctx/linux/$arch"

commit="$(git rev-parse --short HEAD 2>/dev/null || echo none)"
date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "==> build hikyaku (linux/$arch)"
(cd cmd/hikyaku && GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build \
	-ldflags "-s -w -X main.version=dev -X main.commit=$commit -X main.date=$date" \
	-o "../../$ctx/linux/$arch/hikyaku" .)

cp Dockerfile LICENSE "$ctx/"
for f in THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.csv; do
	if [ -f "$f" ]; then cp "$f" "$ctx/"; fi
done

# buildx sets TARGETPLATFORM, which the Dockerfile's COPY needs, and --load puts
# the image in the local image store whichever builder is selected.
echo "==> docker buildx build $image"
docker buildx build --load --platform "linux/$arch" -t "$image" "$ctx"
echo "built $image (linux/$arch); try: docker run --rm $image version"
