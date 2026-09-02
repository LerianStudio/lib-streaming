// Package obs declares the observability contracts lib-streaming accepts on
// its public API.
//
// Every method here is spelled with stdlib types only. lib-streaming MUST NOT
// import github.com/LerianStudio/lib-observability from this package: Go
// matches types nominally, so an interface that names a type defined by a
// versioned module stays bound to that module's major forever. Naming
// obs.Logger on one exported option was enough to make lib-observability's
// major part of lib-streaming's contract, which is how a Fiber upgrade inside
// lib-observability/middleware ended up blocking midaz.
//
// Because these contracts mention nothing but stdlib types, they are satisfied
// by a logger from any lib-observability major, and equally by one declared in
// a package that has never heard of lib-observability. Since
// lib-observability v4 no adapter is needed in either direction: its loggers
// satisfy Logger and its obs.MetricsRecorder satisfies MetricsRecorder as
// they are. See MIGRATION-v4.md in this repository.
package obs

import "context"

// Log severity levels.
//
// The scale is lib-observability's log.Level, unchanged: LOWER IS MORE SEVERE.
// A logger admitting LevelInfo emits Error, Warn and Info and drops Debug.
// This is inverted from log/slog, so do not reach for slog's constants here.
const (
	// LevelError reports failures. Most severe.
	LevelError = 0
	// LevelWarn reports recoverable anomalies.
	LevelWarn = 1
	// LevelInfo reports normal operational events.
	LevelInfo = 2
	// LevelDebug reports diagnostic detail. Least severe.
	LevelDebug = 3
)

// Logger is the logging contract lib-streaming requires.
//
// Structured attributes travel through kv as alternating key/value pairs.
// lib-streaming only ever passes that form, so an implementation is never
// handed a value type it cannot name. Implementations must be safe for
// concurrent use and must not panic on malformed kv.
//
// The interface deliberately has no With or WithGroup: a method returning the
// interface it is declared on cannot be satisfied from outside the declaring
// package, which is precisely the trap that forced consumers to import
// lib-observability in the first place.
type Logger interface {
	// Log emits msg at the given level with the kv attributes attached.
	Log(ctx context.Context, level int, msg string, kv ...any)
	// Enabled reports whether events at level would be emitted.
	Enabled(level int) bool
	// Sync flushes any buffered log entries.
	Sync(ctx context.Context) error
}

// MetricsRecorder is the metrics contract lib-streaming requires.
//
// The builder chain a metrics SDK usually exposes is flattened into one call
// per emission so that no builder type from a versioned module appears in the
// contract. Instrument creation and caching belong to the implementation.
type MetricsRecorder interface {
	// AddCounter adds delta to the named counter.
	AddCounter(ctx context.Context, name, description, unit string, attrs map[string]string, delta int64) error
	// SetGauge sets the named gauge to value.
	SetGauge(ctx context.Context, name, description, unit string, attrs map[string]string, value int64) error
	// RecordHistogram records value in the named histogram.
	//
	// Record durations in MILLISECONDS. value is float64 for call-site
	// convenience, but lib-observability backs this with an Int64Histogram
	// and rounds, so a duration expressed in seconds (0.004) records as 0.
	RecordHistogram(ctx context.Context, name, description, unit string, attrs map[string]string, value float64, buckets []float64) error
}
