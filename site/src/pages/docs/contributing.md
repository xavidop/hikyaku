---
layout: ../../layouts/DocsLayout.astro
title: Contributing
---

# Contributing

This page is the short guide to contributing to hikyaku: how to build and test, the rules every
change follows, how the docs site and the releases work, and how to add a protocol. The full guide
is [CONTRIBUTING.md](https://github.com/xavidop/hikyaku/blob/main/CONTRIBUTING.md) in the
repository. Contributions are welcome, new protocols especially.

## Build and test

You need Go 1.27 or newer.

```sh
git clone https://github.com/xavidop/hikyaku
cd hikyaku
make build           # every module; the binary is cmd/hikyaku/hikyaku
make all             # what CI runs: fmt, vet, check-deps, check-claims, schema-check, test, lint, the llmfake suites
make build-llmfake   # bin/hikyaku with the fake model provider, for the *.llmfake.yaml examples
```

The repository is a multi-module monorepo: several Go modules in one repository, tied together by
`go.work`. The root module is light. It holds the envelope, the plugin interfaces, the store
interfaces, the engine, the routers, the transforms, config and the CLI. Each heavy dependency has
a module of its own: `llm/` (Genkit), `stores/redis/`, one per protocol under `protocols/`, and
`cmd/hikyaku/`. `make check-deps` keeps Genkit, model SDKs, Redis and broker or cloud SDKs out of
the root module.

## Rules of the house

- [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, ...),
  small, and each one passes `make all`. Releases and the changelog are generated from them.
- Every package has tests. The fake-model suites build with `-tags llmfake` (`make test-llmfake`);
  `llmfake` is a model provider that answers without a network or an API key. The container suites
  build with `-tags integration`.
- Every model call goes through the `llm` module, and every secret through mamori. A secret is
  never logged.
- Errors from sinks carry a class and tags, so retry policies can act on them.
- The spec reference and the JSON Schema are generated. Change the config structs and their
  descriptions, then run `make schema`.
- The README and these docs change with the code. `make check-claims` fails when they name a
  command or flag the CLI does not have, or a protocol that has not shipped, unless the page says
  it is planned.

## The docs site

The site is Astro, under `site/`:

```sh
make site-dev          # the dev server, on Node 22 through nvm
make site-linkcheck    # build the site and check every internal link
```

## Releases

A push to `main` releases what its Conventional Commits call for. semantic-release picks the
version, updates `CHANGELOG.md` and tags the root module `vX.Y.Z`. GoReleaser builds the binaries,
the packages and the image.

On `main`, every module requires its sibling modules at `v0.0.0` through `replace` directives.
`go install` refuses such a module, and a module outside a clone cannot use it. So the release
workflow then runs `internal/tools/release`:

1. It writes a release commit on top of `vX.Y.Z`, on no branch. There, every module requires its
   siblings at `vX.Y.Z`, with no `replace`.
2. `go mod tidy` completes each `go.sum`, module by module in dependency order, against a local
   `file://` GOPROXY of the sibling modules. Their zips are built with `golang.org/x/mod/zip` from
   the git tree, the way the go command builds them from a tag, so proxy.golang.org gets the same
   hashes.
3. It tags every other module `<dir>/vX.Y.Z` on the release commit. The root module keeps
   `vX.Y.Z`: the release commit changes none of its files.

The workflow installs the release from that proxy, pushes the tags, and installs it again through
proxy.golang.org. The release commit takes its author and date from the commit of `vX.Y.Z`, so a
re-run makes the same commit and the same tags.

`make release-check` runs the same steps on `HEAD`, with a throwaway version in a scratch clone.
It checks that the go command builds the same zips from the tags. Then it installs from outside
the repository: `go install`, full and lite, and a custom build after `go get`. CI runs it on every
pull request. To tag the modules of a version semantic-release already tagged, by hand:

```sh
go -C internal/tools run ./release -version vX.Y.Z   # prints the tags; pushes nothing
git push origin <the tags it printed>
```

## Adding a protocol

A protocol is a Go module under `protocols/<name>`. It registers a source factory, a sink factory
or both. `cmd/hikyaku/app/wiring.go` calls that registration, or `cmd/hikyaku/app/wiring_full.go`
(`//go:build !lite`) when the protocol's SDK is heavy, so hikyaku-lite leaves it out. Its options
live in `internal/config`, so `make schema` reflects them, and its `config.go` converts and
validates them. A failed delivery returns a `*plugin.Error` with a class and tags, so `retry_on`
and `never_on` can match it.

Its docs change with it: the protocol tables on [Protocols](/docs/protocols/) and in the README.
If the docs named the protocol before it shipped, take its name off the guarded words of
`.github/scripts/check_readme_claims.sh`; those words keep the docs from claiming a protocol before
it ships. Open an issue first; the checklist is in CONTRIBUTING.md.
