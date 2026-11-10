<!-- Thanks for contributing to hikyaku! Please fill this out so reviewers have context. -->

## Summary

<!-- What does this change do, and why is it needed? Link any related issue. -->

Closes #

## Type of change

- [ ] feat (non-breaking)
- [ ] fix (non-breaking)
- [ ] Breaking change (spec keys, CLI, exit codes or the library API)
- [ ] New or changed protocol
- [ ] docs
- [ ] ci / release / tooling

## Checklist

- [ ] `make all` passes
- [ ] New or changed behaviour is covered by tests (every package has tests)
- [ ] README and `site/src/pages/docs/` are updated (the claim checks fail otherwise)
- [ ] `make schema` was run if a spec struct in `internal/config` changed
- [ ] No secret, API key or token appears in code, fixtures, logs or artifacts
- [ ] Every model call goes through the `llm` module (Genkit), every secret through mamori
- [ ] The AI router still picks only from destinations declared in the spec

## New or changed protocol

- [ ] It lives in its own module under `protocols/<name>`, with a `go.work` entry and a line in dependabot's `directories`
- [ ] It is registered in `cmd/hikyaku/wiring.go` (and left out of `-tags lite` if its SDK is heavy)
- [ ] Its errors are `*plugin.Error` with a `Class` and `Tags`, so retry policies can match them
- [ ] Unit tests use a fake of the network; container tests sit behind `//go:build integration`
- [ ] Its docs page, the protocol matrix and the sidebar are updated

## Notes for reviewers

<!-- Anything else worth calling out. -->
