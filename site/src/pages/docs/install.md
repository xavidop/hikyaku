---
layout: ../../layouts/DocsLayout.astro
title: Install
---

# Install

This page shows how to install hikyaku and check that it works. hikyaku is a single binary.
There is a way to install it for each platform:

- macOS: Homebrew.
- Windows: Scoop.
- Linux: a `.deb`, `.rpm` or `.apk` package.
- Any of the three: a release archive.
- With Go: `go install`, or build it from source.
- As a service: the container image, or the Helm chart on Kubernetes.

After the install, run `hikyaku version`. Once you have a spec, run `hikyaku doctor` too.

## Quick start

```sh
brew install xavidop/tap/hikyaku
hikyaku version
hikyaku init --template webhook-fanout && hikyaku validate
```

## Platforms

| Platform | Package manager | Package | Release archive | From source |
|---|---|---|---|---|
| macOS (amd64, arm64) | Homebrew | no | yes | yes |
| Linux (amd64, arm64) | no | `.deb`, `.rpm`, `.apk` | yes | yes |
| Windows (amd64, arm64) | Scoop | no | yes (zip) | yes |

The container image works on linux/amd64 and linux/arm64. The Helm chart runs it on Kubernetes.

## Two builds

Each release ships the same CLI in two builds. Both are attached to the release as archives:

- **`hikyaku`** is the full build. It has every protocol module. Its archive is
  `hikyaku_<version>_<os>_<arch>`. Homebrew, Scoop, the Linux packages and the container image
  all install this build.
- **`hikyaku-lite`** is the small build, made with `-tags lite`. It leaves out the protocols
  whose SDKs are large. Its archive is `hikyaku-lite_<version>_<os>_<arch>`. It has no grpc,
  kafka, nats, jetstream, mqtt, amqp, redis, sqs, sns, eventbridge, s3, pubsub, gcs, servicebus
  or eventhubs. It has no out-of-process plugins (`plugins:`) and no cloud secret providers: it
  resolves `env:`, `file://` and `dotenv://` secret refs only. It keeps http, ws, sse, tcp, udp,
  stdin, stdout, stderr and file, the fault sink and the LLM proxy (`openai` sources and `model`
  destinations).

Both builds install a binary named `hikyaku`. When a spec uses a type the lite build does not
have, `hikyaku doctor` reports it as "only in the full build".

## Homebrew (macOS)

```sh
brew install xavidop/tap/hikyaku
hikyaku version
```

This is a Homebrew cask, so it works on **macOS only**. On Linux, use a package or a release
archive.

## Scoop (Windows)

```powershell
scoop bucket add xavidop https://github.com/xavidop/scoop-bucket
scoop install hikyaku
hikyaku version
```

Scoop installs the Windows zip from the release. `scoop update hikyaku` updates it.

## Linux packages

Each release attaches a `.deb`, an `.rpm` and an `.apk`, for amd64 and arm64. The files are
named `hikyaku_<version>_linux_<arch>.<format>`. There is no package repository. Download the
file for your distribution from the
[GitHub releases page](https://github.com/xavidop/hikyaku/releases) and install it. This
installs the latest release on amd64:

```sh
v=$(curl -fsSL https://api.github.com/repos/xavidop/hikyaku/releases/latest | sed -n 's/.*"tag_name": *"v\([^"]*\)".*/\1/p')
curl -fsSLO "https://github.com/xavidop/hikyaku/releases/download/v$v/hikyaku_${v}_linux_amd64.deb"
sudo dpkg -i "hikyaku_${v}_linux_amd64.deb"      # Debian, Ubuntu
# sudo rpm -i "hikyaku_${v}_linux_amd64.rpm"     # Fedora, RHEL, openSUSE
# sudo apk add --allow-untrusted "hikyaku_${v}_linux_amd64.apk"   # Alpine
hikyaku version
```

The packages put `hikyaku` in `/usr/bin` and install the bash, zsh and fish completions. The
packages are not signed, which is why Alpine needs `--allow-untrusted`. To verify a package,
check the file against `checksums.txt` from the same release.

## Release archives (Linux, macOS, Windows)

1. Download the archive for your platform from the
   [GitHub releases page](https://github.com/xavidop/hikyaku/releases).
2. Check it against `checksums.txt`.
3. Unpack it.
4. Put the `hikyaku` binary on your `PATH`.

```sh
sha256sum --check --ignore-missing checksums.txt   # macOS: shasum -a 256 --check --ignore-missing checksums.txt
tar xzf hikyaku_*_linux_amd64.tar.gz hikyaku
install -m 0755 hikyaku ~/.local/bin/
hikyaku version
```

Archives are `.tar.gz` on Linux and macOS, and `.zip` on Windows. Each holds, at its top level:

- the binary;
- the license and the README;
- the third-party licenses and notices (`THIRD_PARTY_LICENSES.csv`, `THIRD_PARTY_NOTICES.md`);
- the shell completions, under `completions/`.

Each release also comes with:

- an SBOM (a list of the software inside the build) per archive;
- SLSA provenance for the archives and packages (`hikyaku.intoto.jsonl`): a signed record of
  how they were built;
- a keyless [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) signature of
  `checksums.txt` (`checksums.txt.sig`, with its certificate `checksums.txt.pem`) and of the
  container image. The Release workflow signs them with its own identity.

To verify the signatures:

```sh
cosign verify-blob checksums.txt \
  --certificate checksums.txt.pem --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/xavidop/hikyaku/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
cosign verify ghcr.io/xavidop/hikyaku:latest \
  --certificate-identity-regexp 'https://github.com/xavidop/hikyaku/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The binaries are not code-signed. On macOS, Gatekeeper may block a binary you downloaded
yourself. Clear the quarantine attribute to run it: `xattr -d com.apple.quarantine hikyaku`.

## With go install

You need Go 1.27 or newer:

```sh
go install github.com/xavidop/hikyaku/cmd/hikyaku@latest
hikyaku version
```

This installs the full build. Add `-tags lite` for the lite build. Use `@vX.Y.Z` instead of
`@latest` to install a given release. `go install` works for releases made with the module
release tooling ([Contributing](/docs/contributing/#releases)). Each such release tags every Go
module of the repository. In those tags, each module requires the others at the same version,
with no `replace` directive. `hikyaku version` reports the version that `go install` built.

## From source

You need Go 1.27 or newer and `make`:

```sh
git clone https://github.com/xavidop/hikyaku.git
cd hikyaku
make build
cmd/hikyaku/hikyaku version
```

`make build` builds every module, then the full CLI. It writes the binary to
`cmd/hikyaku/hikyaku`; copy it onto your `PATH`. For the lite build, run
`go build -tags lite -o hikyaku .` in `cmd/hikyaku`. The examples' `*.llmfake.yaml` specs need a
different build, one with the fake model provider (`-tags llmfake`). `make build-llmfake` writes
that build to `bin/hikyaku`. See the
[Quick start](/docs/quickstart/#route-with-ai-on-a-fake-model).

## Docker

```sh
docker run --rm ghcr.io/xavidop/hikyaku:latest version
```

The image is `ghcr.io/xavidop/hikyaku`, tagged with each version and with `latest`. It is built
on `gcr.io/distroless/static:nonroot` and runs as a non-root user. Its entrypoint is
`ENTRYPOINT ["hikyaku"]`, and its default command is
`CMD ["run","-f","/etc/hikyaku/hikyaku.yaml"]`, so mount your spec at that path. Port 8080 is
for sources, and 9090 is for the admin API. [Deploy](/docs/deploy/#docker) has a full
`docker run`.

## Helm

```sh
helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku
```

The chart is published in two places: as an OCI artifact (above), and in a chart repository
served with these docs:

```sh
helm repo add hikyaku https://xavidop.github.io/hikyaku
helm install hikyaku hikyaku/hikyaku
```

[Deploy](/docs/deploy/#helm) walks through its values: the spec, secrets, Redis, persistence,
probes, the ServiceMonitor and the Gateway API route.

## Shell completion

`hikyaku completion bash|zsh|fish|powershell` prints a completion script for that shell. The
Linux packages install the bash, zsh and fish scripts for you. On other installs, load the
script yourself:

```sh
source <(hikyaku completion bash)                                  # bash, this shell
hikyaku completion zsh > "${fpath[1]}/_hikyaku"                    # zsh
hikyaku completion fish > ~/.config/fish/completions/hikyaku.fish  # fish
```

## Check the install

```sh
hikyaku version     # the version, the commit and the Genkit version
hikyaku doctor      # with a spec: every destination, model, secret and plugin
```

`hikyaku version --json` prints the same as JSON. `hikyaku doctor` needs a spec to check.
[`hikyaku init`](/docs/cli/#hikyaku-init) creates a starter one for you.
