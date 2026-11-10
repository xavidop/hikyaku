# hikyaku - multi-module monorepo Makefile.
# Each module is built/tested independently with the workspace disabled, matching CI.

# All Go modules in the repo, discovered dynamically: every directory with a
# go.mod, excluding the docs site, node_modules, and testdata. New modules are
# picked up automatically, so this never needs updating.
MODULES := $(shell find . -name go.mod \
	-not -path './site/*' \
	-not -path './.claude/*' \
	-not -path './node_modules/*' \
	-not -path '*/testdata/*' \
	-exec dirname {} \; | sort)

GO ?= go
export GOWORK = off

.PHONY: all build build-llmfake test race vet lint lint-integration fmt tidy work-sync check-deps vuln check-claims schema schema-check notices test-llmfake vet-llmfake lint-llmfake integration genkit-ui site-dev site-build site-linkcheck demo helm-lint helm-test chaos-e2e docker clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

all: fmt vet check-deps check-claims schema-check test lint test-llmfake ## What CI runs: fmt, vet, check-deps, check-claims, schema-check, test, lint and the llmfake suites

build: ## go build every module (the binary is cmd/hikyaku/hikyaku)
	@for m in $(MODULES); do echo "==> build $$m"; (cd $$m && $(GO) build ./...) || exit 1; done
	@(cd cmd/hikyaku && $(GO) build -o hikyaku .)

build-llmfake: ## Build bin/hikyaku with the fake model provider (-tags llmfake), for the *.llmfake.yaml examples
	@mkdir -p bin
	@(cd cmd/hikyaku && $(GO) build -tags llmfake -o ../../bin/hikyaku .)

test: ## go test every module
	@for m in $(MODULES); do echo "==> test $$m"; (cd $$m && $(GO) test ./...) || exit 1; done

race: ## go test -race every module
	@for m in $(MODULES); do echo "==> test -race $$m"; (cd $$m && $(GO) test -race ./...) || exit 1; done

vet: ## go vet every module (default and integration build tags)
	@for m in $(MODULES); do echo "==> vet $$m"; (cd $$m && $(GO) vet ./... && $(GO) vet -tags integration ./...) || exit 1; done

lint: ## golangci-lint every module (requires golangci-lint)
	@for m in $(MODULES); do echo "==> lint $$m"; (cd $$m && golangci-lint run) || exit 1; done

lint-integration: ## golangci-lint -tags integration for every module (the container-backed suites)
	@for m in $(MODULES); do echo "==> lint -tags integration $$m"; (cd $$m && golangci-lint run --build-tags integration) || exit 1; done

fmt: ## Check the tree is gofmt-clean (fails and lists files instead of rewriting them)
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './site/*' -not -path './.claude/*' -not -path '*/node_modules/*')); \
	if [ -n "$$out" ]; then \
		echo "not gofmt-clean:"; echo "$$out"; exit 1; \
	fi

tidy: ## go mod tidy every module
	@for m in $(MODULES); do echo "==> tidy $$m"; (cd $$m && $(GO) mod tidy) || exit 1; done

work-sync: ## Regenerate the go.work workspace (then re-tidy modules)
	@GOWORK= $(GO) work sync
	@$(MAKE) tidy

check-deps: ## Fail if the root module pulls a heavy SDK, or a module other than llm imports Genkit or a model provider SDK
	@bash .github/scripts/check_root_deps.sh .
	@bash .github/scripts/check_ai_deps.sh

vuln: ## govulncheck every module, skipping the IDs in .github/govulncheck-ignore (what CI runs)
	@bash .github/scripts/govulncheck.sh

check-claims: ## Fail if the README or docs claim a command, flag or protocol the CLI does not have
	@bash .github/scripts/check_readme_claims.sh
	@bash .github/scripts/check_docs_claims.sh

SPEC_PAGE := site/src/pages/docs/spec-reference.md

schema: ## Regenerate schema/hikyaku.schema.json and the spec reference page from the Go structs
	@$(GO) run ./schema/gen.go schema $(SPEC_PAGE)

schema-check: ## Fail if the committed schema or the spec reference page differ from what `make schema` generates
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	$(GO) run ./schema/gen.go "$$tmp/schema" "$$tmp/spec-reference.md" || exit 1; \
	stale=0; \
	diff -u schema/hikyaku.schema.json "$$tmp/schema/hikyaku.schema.json" || stale=1; \
	diff -u "$(SPEC_PAGE)" "$$tmp/spec-reference.md" || stale=1; \
	if [ "$$stale" -ne 0 ]; then echo "schema is stale: run make schema and commit the result"; exit 1; fi; \
	echo "schema-check: schema/ and $(SPEC_PAGE) are up to date"

# The end-to-end tests with the fake model provider (no network, no keys) build
# only with -tags llmfake, so a default build never contains the fake provider.
# Entries are "module dir:packages"; modules join the list as they gain llmfake tests.
LLMFAKE := llm:./... cmd/hikyaku:./...

test-llmfake: ## go test -tags llmfake for the fake-provider suites
	@for p in $(LLMFAKE); do d=$${p%%:*}; pkgs=$${p#*:}; echo "==> test -tags llmfake $$d $$pkgs"; \
		(cd $$d && $(GO) test -timeout 30m -tags llmfake $$pkgs) || exit 1; done

vet-llmfake: ## go vet -tags llmfake for the fake-provider suites
	@for p in $(LLMFAKE); do d=$${p%%:*}; pkgs=$${p#*:}; echo "==> vet -tags llmfake $$d $$pkgs"; \
		(cd $$d && $(GO) vet -tags llmfake $$pkgs) || exit 1; done

lint-llmfake: ## golangci-lint -tags llmfake for the fake-provider suites
	@for p in $(LLMFAKE); do d=$${p%%:*}; pkgs=$${p#*:}; echo "==> lint -tags llmfake $$d $$pkgs"; \
		(cd $$d && golangci-lint run --build-tags llmfake $$pkgs) || exit 1; done

integration: ## Run the examples end to end with the fake model provider (scripts/integration.sh)
	@bash scripts/integration.sh

chaos-e2e: ## Run the resilience suite end to end: fault sinks, hikyaku chaos, fallbacks and the DLQ (scripts/chaos-e2e.sh)
	@bash scripts/chaos-e2e.sh

.PHONY: release-check
release-check: ## Cut a throwaway Go module release of HEAD in a scratch clone and go install it from outside (scripts/release-check.sh)
	@bash scripts/release-check.sh

bench: ## Run the benchmarks: the relay end to end (memory, disk, ordering, batching), transforms, envelopes and the disk buffer
	$(GO) test -run '^$$' -bench . -benchmem ./engine/ ./transform/ ./envelope/ ./internal/wal/

notices: ## Regenerate THIRD_PARTY_LICENSES.csv and THIRD_PARTY_NOTICES.md (go-licenses on the CLI module)
	@bash scripts/notices.sh

genkit-ui: ## Run the ai-triage example under `genkit start` to inspect routing flows in the Genkit Developer UI
	@command -v genkit >/dev/null || { echo "genkit CLI is not installed: npm i -g genkit-cli"; exit 1; }
	@mkdir -p bin
	@(cd cmd/hikyaku && $(GO) build -tags llmfake -o ../../bin/hikyaku .)
	@genkit start -- ./bin/hikyaku dev -f examples/ai-triage/hikyaku.llmfake.yaml

helm-lint: ## helm lint the chart (default values and every ci/*-values.yaml)
	@command -v helm >/dev/null || { echo "helm is not installed"; exit 1; }
	@helm lint charts/hikyaku
	@for f in charts/hikyaku/ci/*-values.yaml; do echo "==> helm lint -f $$f"; helm lint charts/hikyaku -f $$f || exit 1; done

helm-test: ## Install the chart on a kind cluster with chart-testing (scripts/helm-test.sh: kind, docker, ct in a container)
	@bash scripts/helm-test.sh

docker: ## Build the container image locally (ghcr.io/xavidop/hikyaku:dev) from a linux build of the CLI
	@bash scripts/docker-build.sh

site-dev: ## Run the docs site dev server (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run dev'

site-build: ## Build the docs site (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run build'

site-linkcheck: ## Build the docs site and check for broken internal links (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run build && npm run linkcheck'

demo: ## Record site/public/demo.gif with vhs (needs vhs, ttyd and ffmpeg)
	@command -v vhs >/dev/null || { echo "vhs is not installed: brew install vhs (or see https://github.com/charmbracelet/vhs#installation)"; exit 1; }
	@mkdir -p bin site/public
	@(cd cmd/hikyaku && $(GO) build -tags llmfake -o ../../bin/hikyaku .)
	@vhs demo.tape

clean: ## Remove build artifacts
	@rm -rf dist bin completions cmd/hikyaku/hikyaku site/dist site/.astro .hikyaku out
