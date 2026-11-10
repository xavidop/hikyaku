package main

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildVersion(t *testing.T) {
	installed := &debug.BuildInfo{Main: debug.Module{Path: "github.com/xavidop/hikyaku/cmd/hikyaku", Version: "v1.2.3"}}
	checkout := &debug.BuildInfo{
		Main:     debug.Module{Path: "github.com/xavidop/hikyaku/cmd/hikyaku", Version: "v0.0.0-20261110094500-b4df117d445f+dirty"},
		Settings: []debug.BuildSetting{{Key: "vcs", Value: "git"}, {Key: "vcs.revision", Value: "b4df117d445f"}},
	}
	noVCS := &debug.BuildInfo{Main: debug.Module{Path: "github.com/xavidop/hikyaku/cmd/hikyaku", Version: "(devel)"}}

	for _, tc := range []struct {
		name, v string
		bi      *debug.BuildInfo
		ok      bool
		want    string
	}{
		{"the release build sets it", "1.4.0", installed, true, "1.4.0"},
		{"go install reports the module version", "dev", installed, true, "v1.2.3"},
		{"a checkout stays dev", "dev", checkout, true, "dev"},
		{"a checkout built with -buildvcs=false stays dev", "dev", noVCS, true, "dev"},
		{"no build information", "dev", nil, false, "dev"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, buildVersion(tc.v, tc.bi, tc.ok))
		})
	}
}
