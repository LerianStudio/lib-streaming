// Package streaming is the public entry point for lib-streaming.
//
// This file is the re-export layer. All implementation lives in
// internal/streaming (package streaming). Type aliases and function
// wrappers here make the internal package's API available at the root
// import path without leaking the internal path into consumer godoc or
// IDE tooltips.
//
// Consumers import "github.com/LerianStudio/lib-streaming" and see
// streaming.Emitter, streaming.Event, streaming.New, etc. — the aliases
// are fully transparent.
//
// See doc.go for the package-level documentation.
package streaming

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/opentelemetry/metrics"
	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.opentelemetry.io/otel/trace"

	s "github.com/LerianStudio/lib-streaming/internal/streaming"
)

// ---------------------------------------------------------------------------
// Type aliases — transparent to callers; no wrapper struct needed.
// ---------------------------------------------------------------------------

type (
	// Config is the full runtime configuration for a Producer.
	Config = s.Config

	// Event is the CloudEvents-aligned envelope produced by a service method.
	Event = s.Event

	// Emitter publishes domain events. The three-method surface is the full
	// contract every production service sees.
	Emitter = s.Emitter

	// Producer is the franz-go-backed Emitter implementation.
	Producer = s.Producer

	// NoopEmitter is the fail-safe Emitter implementation returned when
	// Config.Enabled is false or the broker list is empty.
	NoopEmitter = s.NoopEmitter

	// MockEmitter is a concurrency-safe, zero-dependency test double for Emitter.
	MockEmitter = s.MockEmitter

	// EmitterOption configures a Producer at construction time.
	EmitterOption = s.EmitterOption

	// HealthError is the structured error returned from Emitter.Healthy when
	// the producer cannot service new emits.
	HealthError = s.HealthError

	// HealthState classifies a Producer's readiness in three discrete buckets.
	HealthState = s.HealthState

	// ErrorClass classifies the root cause of a publish failure into one of
	// eight operational buckets.
	ErrorClass = s.ErrorClass

	// EmitError is the structured error type returned from Emit on publish
	// failure.
	EmitError = s.EmitError
)

// ---------------------------------------------------------------------------
// Sentinel error vars — re-exported so errors.Is works across import paths.
// Both sides hold the same pointer; pointer identity is preserved.
// ---------------------------------------------------------------------------

var (
	// ErrMissingTenantID is returned when Event.TenantID is empty and
	// Event.SystemEvent is false.
	ErrMissingTenantID = s.ErrMissingTenantID

	// ErrSystemEventsNotAllowed is returned when an Event arrives with
	// SystemEvent=true but the Producer was constructed without
	// WithAllowSystemEvents.
	ErrSystemEventsNotAllowed = s.ErrSystemEventsNotAllowed

	// ErrMissingSource is returned when Event.Source is empty.
	ErrMissingSource = s.ErrMissingSource

	// ErrMissingResourceType is returned when Event.ResourceType is empty.
	ErrMissingResourceType = s.ErrMissingResourceType

	// ErrMissingEventType is returned when Event.EventType is empty.
	ErrMissingEventType = s.ErrMissingEventType

	// ErrInvalidTenantID is returned when Event.TenantID contains control
	// characters or exceeds 256 bytes.
	ErrInvalidTenantID = s.ErrInvalidTenantID

	// ErrInvalidResourceType is returned when Event.ResourceType contains
	// control characters or exceeds 128 bytes.
	ErrInvalidResourceType = s.ErrInvalidResourceType

	// ErrInvalidEventType is returned when Event.EventType contains control
	// characters or exceeds 128 bytes.
	ErrInvalidEventType = s.ErrInvalidEventType

	// ErrInvalidSource is returned when Event.Source contains control
	// characters or exceeds 2048 bytes.
	ErrInvalidSource = s.ErrInvalidSource

	// ErrInvalidSubject is returned when Event.Subject contains control
	// characters or exceeds 1024 bytes.
	ErrInvalidSubject = s.ErrInvalidSubject

	// ErrInvalidEventID is returned when Event.EventID exceeds 256 bytes or
	// contains control characters.
	ErrInvalidEventID = s.ErrInvalidEventID

	// ErrInvalidSchemaVersion is returned when Event.SchemaVersion exceeds
	// 64 bytes or contains control characters.
	ErrInvalidSchemaVersion = s.ErrInvalidSchemaVersion

	// ErrInvalidDataContentType is returned when Event.DataContentType exceeds
	// 256 bytes or contains control characters.
	ErrInvalidDataContentType = s.ErrInvalidDataContentType

	// ErrInvalidDataSchema is returned when Event.DataSchema exceeds 2048 bytes
	// or contains control characters.
	ErrInvalidDataSchema = s.ErrInvalidDataSchema

	// ErrEmitterClosed is returned from Emit after Close has been called.
	ErrEmitterClosed = s.ErrEmitterClosed

	// ErrEventDisabled is returned when Config.EventToggles has disabled the
	// resource.event combination at runtime.
	ErrEventDisabled = s.ErrEventDisabled

	// ErrPayloadTooLarge is returned when Event.Payload exceeds the 1 MiB limit.
	ErrPayloadTooLarge = s.ErrPayloadTooLarge

	// ErrNotJSON is returned when Event.Payload fails json.Valid.
	ErrNotJSON = s.ErrNotJSON

	// ErrMissingBrokers is returned by LoadConfig when STREAMING_ENABLED=true
	// but STREAMING_BROKERS is empty.
	ErrMissingBrokers = s.ErrMissingBrokers

	// ErrInvalidCompression is returned when the compression codec is not one
	// of snappy, lz4, zstd, gzip, none.
	ErrInvalidCompression = s.ErrInvalidCompression

	// ErrInvalidAcks is returned when the required-acks value is not one of
	// all, leader, none.
	ErrInvalidAcks = s.ErrInvalidAcks

	// ErrNilProducer is returned when a method is invoked on a nil *Producer.
	ErrNilProducer = s.ErrNilProducer

	// ErrCircuitOpen is returned from Emit when the circuit breaker is open
	// and no outbox repository has been wired.
	ErrCircuitOpen = s.ErrCircuitOpen

	// ErrOutboxNotConfigured is returned from publishToOutbox when the Producer
	// has no OutboxRepository wired.
	ErrOutboxNotConfigured = s.ErrOutboxNotConfigured

	// ErrNilOutboxRegistry is returned from RegisterOutboxHandler when the
	// supplied *outbox.HandlerRegistry is nil.
	ErrNilOutboxRegistry = s.ErrNilOutboxRegistry
)

// ---------------------------------------------------------------------------
// HealthState constants
// ---------------------------------------------------------------------------

const (
	// Healthy means the Producer is fully operational.
	Healthy HealthState = s.Healthy

	// Degraded means the broker is unreachable but outbox fallback is viable.
	Degraded HealthState = s.Degraded

	// Down means both broker and outbox are unreachable.
	Down HealthState = s.Down
)

// ---------------------------------------------------------------------------
// ErrorClass constants
// ---------------------------------------------------------------------------

const (
	// ClassSerialization covers payload-shape faults.
	ClassSerialization ErrorClass = s.ClassSerialization

	// ClassValidation covers caller-supplied invalid field combinations.
	ClassValidation ErrorClass = s.ClassValidation

	// ClassAuth covers authorization / authentication failures.
	ClassAuth ErrorClass = s.ClassAuth

	// ClassTopicNotFound covers missing topics that the broker did not auto-create.
	ClassTopicNotFound ErrorClass = s.ClassTopicNotFound

	// ClassBrokerUnavailable covers transient broker unreachability.
	ClassBrokerUnavailable ErrorClass = s.ClassBrokerUnavailable

	// ClassNetworkTimeout covers network-level timeouts.
	ClassNetworkTimeout ErrorClass = s.ClassNetworkTimeout

	// ClassContextCanceled covers caller-side cancellation.
	ClassContextCanceled ErrorClass = s.ClassContextCanceled

	// ClassBrokerOverloaded covers quota / throttling responses from the broker.
	ClassBrokerOverloaded ErrorClass = s.ClassBrokerOverloaded
)

// ---------------------------------------------------------------------------
// Package-level functions — thin wrappers so godoc surfaces at the right path.
// ---------------------------------------------------------------------------

// New constructs an Emitter.
//
// When Config.Enabled is false or the broker list is empty, New returns a
// NoopEmitter without constructing a franz-go client. Callers who need a real
// *Producer should use NewProducer.
func New(ctx context.Context, cfg Config, opts ...EmitterOption) (Emitter, error) {
	return s.New(ctx, cfg, opts...)
}

// NewProducer is the unconditional Producer constructor. It never substitutes
// a NoopEmitter. For normal service bootstrap, prefer New.
func NewProducer(ctx context.Context, cfg Config, opts ...EmitterOption) (*Producer, error) {
	return s.NewProducer(ctx, cfg, opts...)
}

// NewNoopEmitter returns an Emitter whose methods are unconditional no-ops.
func NewNoopEmitter() Emitter {
	return s.NewNoopEmitter()
}

// NewMockEmitter returns a fresh MockEmitter with an empty event buffer.
func NewMockEmitter() *MockEmitter {
	return s.NewMockEmitter()
}

// NewHealthError constructs a HealthError with the given state and cause.
func NewHealthError(state HealthState, cause error) *HealthError {
	return s.NewHealthError(state, cause)
}

// LoadConfig reads every STREAMING_* environment variable, applies defaults,
// and validates the result. When Enabled=false, validation is skipped.
func LoadConfig() (Config, error) {
	return s.LoadConfig()
}

// IsCallerError reports whether err represents a caller-correctable fault as
// opposed to a transient infrastructure fault.
func IsCallerError(err error) bool {
	return s.IsCallerError(err)
}

// ---------------------------------------------------------------------------
// Option functions — thin wrappers preserving godoc on each option.
// ---------------------------------------------------------------------------

// WithLogger sets the structured logger used across the package.
func WithLogger(l log.Logger) EmitterOption {
	return s.WithLogger(l)
}

// WithMetricsFactory wires the OTEL metrics factory used to register the
// streaming instruments.
func WithMetricsFactory(f *metrics.MetricsFactory) EmitterOption {
	return s.WithMetricsFactory(f)
}

// WithTracer overrides the OTEL tracer used for the streaming.emit span.
func WithTracer(t trace.Tracer) EmitterOption {
	return s.WithTracer(t)
}

// WithCircuitBreakerManager lets the caller share a process-level
// circuitbreaker.Manager with the Producer.
func WithCircuitBreakerManager(m circuitbreaker.Manager) EmitterOption {
	return s.WithCircuitBreakerManager(m)
}

// WithPartitionKey overrides the default Event.PartitionKey() behavior at
// publish time.
func WithPartitionKey(fn func(Event) string) EmitterOption {
	return s.WithPartitionKey(fn)
}

// WithCloseTimeout caps how long Close waits for buffered-record flush and
// outbox drain.
func WithCloseTimeout(d time.Duration) EmitterOption {
	return s.WithCloseTimeout(d)
}

// WithOutboxRepository wires an OutboxRepository so the Producer can fall
// back to durable storage when the circuit breaker is open.
func WithOutboxRepository(repo outbox.OutboxRepository) EmitterOption {
	return s.WithOutboxRepository(repo)
}

// WithTLSConfig sets a TLS configuration for broker connections.
func WithTLSConfig(cfg *tls.Config) EmitterOption {
	return s.WithTLSConfig(cfg)
}

// WithSASL sets the SASL mechanism for broker authentication.
func WithSASL(mech sasl.Mechanism) EmitterOption {
	return s.WithSASL(mech)
}

// WithAllowSystemEvents opts a Producer into accepting Event.SystemEvent=true
// emissions.
func WithAllowSystemEvents() EmitterOption {
	return s.WithAllowSystemEvents()
}

// ---------------------------------------------------------------------------
// Mock assertion helpers — exported so external test code can use them.
// ---------------------------------------------------------------------------

// AssertEventEmitted fails t when no captured event has the given resource
// and event type pair.
func AssertEventEmitted(t testing.TB, m *MockEmitter, resourceType, eventType string) {
	s.AssertEventEmitted(t, m, resourceType, eventType)
}

// AssertEventCount fails t when the count of captured events matching the
// resource + event type pair does not equal n.
func AssertEventCount(t testing.TB, m *MockEmitter, resourceType, eventType string, n int) {
	s.AssertEventCount(t, m, resourceType, eventType, n)
}

// AssertTenantID fails t when no captured event carries the given tenant ID.
func AssertTenantID(t testing.TB, m *MockEmitter, tenantID string) {
	s.AssertTenantID(t, m, tenantID)
}

// AssertNoEvents fails t when any event was captured.
func AssertNoEvents(t testing.TB, m *MockEmitter) {
	s.AssertNoEvents(t, m)
}

// WaitForEvent blocks until the matcher returns true on a newly-observed
// event, or timeout elapses. Calls t.Fatalf on timeout.
func WaitForEvent(t testing.TB, ctx context.Context, m *MockEmitter, matcher func(Event) bool, timeout time.Duration) Event {
	return s.WaitForEvent(t, ctx, m, matcher, timeout)
}
