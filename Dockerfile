# ghcr.io/xavidop/hikyaku: the hikyaku relay on distroless, for linux/amd64 and
# linux/arm64.
#
# GoReleaser (dockers_v2) builds it from a context that holds each platform's
# binary at <os>/<arch>/hikyaku plus the extra files; scripts/docker-build.sh
# (make docker) lays out the same context for one platform. The base has no
# shell or package manager and this file only copies, so a cross-platform build
# needs no emulation. Dependabot keeps the base image's digest current.
FROM gcr.io/distroless/static:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/hikyaku /usr/local/bin/hikyaku
# LICENSE always, and the THIRD_PARTY_* notices when the context has them.
COPY LICENSE THIRD_PARTY_* /usr/share/doc/hikyaku/
# 8080: the sources' listeners (http, ws, sse); 9090: the admin API.
EXPOSE 8080 9090
# distroless's nonroot user, by number so Kubernetes' runAsNonRoot can verify it.
USER 65532:65532
ENTRYPOINT ["hikyaku"]
CMD ["run", "-f", "/etc/hikyaku/hikyaku.yaml"]
