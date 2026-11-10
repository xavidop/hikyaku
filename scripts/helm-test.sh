#!/usr/bin/env bash
# Installs the chart on a local kind cluster with chart-testing (make
# helm-test), the way the Helm workflow's install job does:
#   1. creates the kind cluster hikyaku-ct, or reuses it;
#   2. builds the linux hikyaku binary and the image ghcr.io/xavidop/hikyaku:ci
#      (scripts/docker-build.sh, for the Docker daemon's architecture);
#   3. loads the image into the cluster;
#   4. runs ct lint and ct install from the chart-testing image, once per
#      charts/hikyaku/ci/*-values.yaml, with --set image.tag=ci
#      --set image.pullPolicy=Never; ct installs into throwaway namespaces,
#      runs helm test, and deletes them.
# The cluster is kept for the next run.
#
#   scripts/helm-test.sh            run the tests
#   scripts/helm-test.sh --delete   delete the cluster
#
# Needs docker (running), kind and go. helm, ct and kubectl come from the
# chart-testing image. HIKYAKU_CT_CLUSTER, CT_IMAGE and KIND_NODE_IMAGE
# override the cluster name, the chart-testing image and kind's node image.
#
# The ct container joins kind's Docker network and reaches the API server by
# the control plane's container name (kind's --internal kubeconfig). Unlike
# --network host and 127.0.0.1, that does not depend on how the Docker runtime
# (Docker Desktop, colima, podman) maps the host network into its VM.
set -euo pipefail
cd "$(dirname "$0")/.."

cluster="${HIKYAKU_CT_CLUSTER:-hikyaku-ct}"
image="ghcr.io/xavidop/hikyaku:ci"
ct_image="${CT_IMAGE:-quay.io/helmpack/chart-testing:v3.15.0}"
state=.hikyaku/helm-test

usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "$0"; }
die() { echo "helm-test: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not installed: $2"; }

delete=false
case "${1:-}" in
	"") ;;
	--delete) delete=true ;;
	-h | --help) usage; exit 0 ;;
	*) usage >&2; die "unknown argument: $1" ;;
esac
[ $# -le 1 ] || die "expected at most one argument"

need docker "https://docs.docker.com/get-docker/"
need kind "https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
docker info >/dev/null 2>&1 || die "the Docker daemon is not running"

if $delete; then
	if kind get clusters 2>/dev/null | grep -qx "$cluster"; then
		echo "==> delete kind cluster $cluster"
		kind delete cluster --name "$cluster"
	else
		echo "helm-test: no kind cluster named $cluster"
	fi
	exit 0
fi

need go "https://go.dev/dl/"
[ -d cmd/hikyaku ] || die "cmd/hikyaku is missing: there is no CLI to build yet"
[ -f scripts/docker-build.sh ] || die "scripts/docker-build.sh is missing"

echo "==> kind cluster $cluster"
if kind get clusters 2>/dev/null | grep -qx "$cluster"; then
	echo "reusing it (scripts/helm-test.sh --delete removes it)"
else
	kind create cluster --name "$cluster" --wait 120s ${KIND_NODE_IMAGE:+--image "$KIND_NODE_IMAGE"}
fi

echo "==> build $image"
bash scripts/docker-build.sh "$image"

echo "==> kind load docker-image $image"
kind load docker-image "$image" --name "$cluster"

mkdir -p "$state"
kubeconfig="$state/kubeconfig"
trap 'rm -f "$kubeconfig"' EXIT
kind get kubeconfig --internal --name "$cluster" >"$kubeconfig"

echo "==> ct lint and ct install ($ct_image)"
# The repository is mounted at /workdir; git there belongs to another user,
# hence safe.directory. --charts lints and installs this chart without diffing
# against a remote branch (and without ct's version bump check).
docker run --rm \
	--network kind \
	--volume "$PWD:/workdir" \
	--workdir /workdir \
	--env KUBECONFIG="/workdir/$kubeconfig" \
	"$ct_image" \
	sh -c 'git config --global --add safe.directory /workdir &&
		ct lint --config .github/ct.yaml --charts charts/hikyaku &&
		ct install --config .github/ct.yaml --charts charts/hikyaku \
			--helm-extra-set-args "--set image.tag=ci --set image.pullPolicy=Never"'

echo "helm-test: ok (cluster $cluster kept; scripts/helm-test.sh --delete removes it)"
