//go:build unit

package buildmeta

import (
	"runtime/debug"
	"testing"
)

// TestScope_OwnTestBinary pins Scope() as a faithful cached call-through to
// scopeFrom over this process's real build info, and pins the resolved name to
// this module's own path. Inside lib-streaming's own test binary the library is
// the main module, so the version is whatever Go stamps for an untagged build.
func TestScope_OwnTestBinary(t *testing.T) {
	t.Parallel()

	wantName, wantVersion := scopeFrom(debug.ReadBuildInfo())

	name, version := Scope()
	if name != wantName || version != wantVersion {
		t.Errorf("Scope() = (%q, %q); want (%q, %q)", name, version, wantName, wantVersion)
	}

	if name != modulePath {
		t.Errorf("scope name = %q; want this module's own path %q", name, modulePath)
	}
}

// TestScopeFrom covers how the scope is read out of a consumer binary's build
// info: this module's own major wins even when a sibling major is linked
// alongside it, a replace directive overrides the version, and anything
// unreadable degrades to (devel).
func TestScopeFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		info        *debug.BuildInfo
		ok          bool
		wantName    string
		wantVersion string
	}{
		{
			name: "linked dependency",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/LerianStudio/midaz", Version: "v4.0.3"},
				Deps: []*debug.Module{
					{Path: "github.com/LerianStudio/lib-commons/v7", Version: "v7.4.0"},
					{Path: "github.com/LerianStudio/lib-streaming/v4", Version: "v4.2.1"},
				},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "v4.2.1",
		},
		{
			// A consumer on v4 whose transitive Lerian libs still pull v3 links
			// both, and Go lists them sorted by path. This copy of the code is
			// the v4 one, so it must report v4 — reporting v3's version would
			// tell an operator a release that did not emit the span.
			name: "two majors linked; this module's own major wins",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/LerianStudio/midaz", Version: "v4.0.3"},
				Deps: []*debug.Module{
					{Path: "github.com/LerianStudio/lib-streaming/v3", Version: "v3.9.9"},
					{Path: "github.com/LerianStudio/lib-streaming/v4", Version: "v4.2.1"},
				},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "v4.2.1",
		},
		{
			name: "replaced dependency reports the replacement version",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/LerianStudio/midaz", Version: "v4.0.3"},
				Deps: []*debug.Module{
					{
						Path:    "github.com/LerianStudio/lib-streaming/v4",
						Version: "v4.2.1",
						Replace: &debug.Module{Path: "../lib-streaming", Version: "(devel)"},
					},
				},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "(devel)",
		},
		{
			name: "dependency without a version",
			info: &debug.BuildInfo{
				Deps: []*debug.Module{{Path: "github.com/LerianStudio/lib-streaming/v4"}},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "(devel)",
		},
		{
			// A foreign major's identity is never borrowed: a binary linking
			// only v9 cannot be running this v4 code, so the version is unknown.
			name: "another major alone is not claimed as this one",
			info: &debug.BuildInfo{
				Deps: []*debug.Module{{Path: "github.com/LerianStudio/lib-streaming/v9", Version: "v9.0.0"}},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "(devel)",
		},
		{
			name: "lib-streaming is the main module",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/LerianStudio/lib-streaming/v4", Version: "v4.2.1"},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "v4.2.1",
		},
		{
			name:        "no build info",
			ok:          false,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "(devel)",
		},
		{
			name: "lib-streaming absent from the build info",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "github.com/LerianStudio/midaz", Version: "v4.0.3"},
				Deps: []*debug.Module{{Path: "github.com/LerianStudio/lib-commons/v7", Version: "v7.4.0"}},
			},
			ok:          true,
			wantName:    "github.com/LerianStudio/lib-streaming/v4",
			wantVersion: "(devel)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, version := scopeFrom(tt.info, tt.ok)
			if name != tt.wantName || version != tt.wantVersion {
				t.Errorf("scopeFrom() = (%q, %q); want (%q, %q)", name, version, tt.wantName, tt.wantVersion)
			}
		})
	}
}
