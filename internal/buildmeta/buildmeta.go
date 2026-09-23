// Package buildmeta resolves the OpenTelemetry instrumentation scope of this
// library: the Go module path of lib-streaming and the version of it linked
// into the running binary.
//
// Libraries have no /version endpoint and never write service.version — the
// module path plus the module version on every span they emit is how an
// operator learns which lib-streaming produced the telemetry they are reading.
package buildmeta

import (
	"runtime/debug"
	"sync"
)

const (
	// modulePath is this module's own path, matched exactly. A consumer binary
	// mid-migration links two lib-streaming majors at once — Go lists both in
	// Deps, sorted lexically — so a prefix match would make this copy of the
	// code sign its spans with a sibling major's path and version. It must
	// claim its own major and no other. The next major bump edits this line,
	// which it already had to for the no-build-info fallback below.
	modulePath = "github.com/LerianStudio/lib-streaming/v4"

	// develVersion is what Go itself reports for a module built outside a
	// tagged release; reusing the spelling keeps the scope readable.
	develVersion = "(devel)"
)

// scope caches the resolved pair: build info is immutable for the life of the
// process and walking Deps on every span creation would be pure waste.
var scope = sync.OnceValues(func() (string, string) {
	return scopeFrom(debug.ReadBuildInfo())
})

// Scope returns the instrumentation scope name and version for lib-streaming.
// Safe for concurrent use; the underlying build info is read once.
func Scope() (name, version string) {
	return scope()
}

// scopeFrom is the pure half of Scope, so the resolution rules can be tested
// against build info this process does not have.
func scopeFrom(info *debug.BuildInfo, ok bool) (string, string) {
	if !ok {
		return modulePath, develVersion
	}

	for _, dep := range info.Deps {
		if dep.Path != modulePath {
			continue
		}

		version := dep.Version
		if dep.Replace != nil && dep.Replace.Version != "" {
			version = dep.Replace.Version
		}

		if version == "" {
			version = develVersion
		}

		return modulePath, version
	}

	// lib-streaming is the main module: its own test and benchmark binaries.
	if info.Main.Path == modulePath && info.Main.Version != "" {
		return modulePath, info.Main.Version
	}

	return modulePath, develVersion
}
