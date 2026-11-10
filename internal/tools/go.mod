// The repository's own tooling: not part of a release, and never imported.
module github.com/xavidop/hikyaku/internal/tools

go 1.27

require (
	github.com/stretchr/testify v1.12.1
	golang.org/x/mod v0.42.0
)

require go.yaml.in/yaml/v3 v3.0.6 // indirect
