package streaming

import (
	"github.com/LerianStudio/lib-streaming/internal/config"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/manifest"
)

type (
	// Config is the full runtime configuration for a Producer.
	Config = config.Config
	// Event is the CloudEvents-aligned envelope resolved from an EmitRequest.
	Event = contract.Event
	// EmitRequest is the catalog-keyed runtime request passed to Emitter.Emit.
	EmitRequest = contract.EmitRequest
	// EventDefinition is the static contract for one supported event.
	EventDefinition = contract.EventDefinition
	// Catalog is an immutable registry of event definitions.
	Catalog = contract.Catalog
	// DeliveryPolicy is the resolved direct/outbox/DLQ delivery policy.
	DeliveryPolicy = contract.DeliveryPolicy
	// DeliveryPolicyOverride carries optional config or per-call policy changes.
	DeliveryPolicyOverride = contract.DeliveryPolicyOverride
	// DirectMode controls whether a resolved emit attempts direct broker publish.
	DirectMode = contract.DirectMode
	// OutboxMode controls when the producer writes to the app-owned outbox.
	OutboxMode = contract.OutboxMode
	// DLQMode controls whether routable publish failures are copied to DLQ.
	DLQMode = contract.DLQMode
	// OutboxEnvelope is the persisted streaming outbox payload shape.
	OutboxEnvelope = contract.OutboxEnvelope
	// HealthState classifies producer readiness.
	HealthState = contract.HealthState
	// HealthError carries readiness state and the underlying health failure.
	HealthError = contract.HealthError
	// PublisherDescriptor carries app-owned manifest metadata.
	PublisherDescriptor = manifest.PublisherDescriptor
	// ManifestDocument is the JSON-serializable producer catalog document.
	ManifestDocument = manifest.ManifestDocument
	// ManifestEvent is one catalog entry rendered in a manifest document.
	ManifestEvent = manifest.ManifestEvent
)

const (
	DirectModeDirect                = contract.DirectModeDirect
	DirectModeSkip                  = contract.DirectModeSkip
	OutboxModeNever                 = contract.OutboxModeNever
	OutboxModeFallbackOnCircuitOpen = contract.OutboxModeFallbackOnCircuitOpen
	OutboxModeAlways                = contract.OutboxModeAlways
	DLQModeNever                    = contract.DLQModeNever
	DLQModeOnRoutableFailure        = contract.DLQModeOnRoutableFailure
	StreamingOutboxEventType        = contract.StreamingOutboxEventType
	Healthy                         = contract.Healthy
	Degraded                        = contract.Degraded
	Down                            = contract.Down
	ManifestVersion                 = manifest.ManifestVersion
)

// NewEmitRequest validates and defensively copies an EmitRequest.
func NewEmitRequest(request EmitRequest) (EmitRequest, error) {
	return contract.NewEmitRequest(request)
}

// NewEventDefinition validates and normalizes an event definition.
func NewEventDefinition(definition EventDefinition) (EventDefinition, error) {
	return contract.NewEventDefinition(definition)
}

// NewCatalog builds an immutable catalog from event definitions.
func NewCatalog(definitions ...EventDefinition) (Catalog, error) {
	return contract.NewCatalog(definitions...)
}

// DefaultDeliveryPolicy returns the package default delivery policy.
func DefaultDeliveryPolicy() DeliveryPolicy {
	return contract.DefaultDeliveryPolicy()
}

// ResolveDeliveryPolicy applies definition, config, and call-level policy precedence.
func ResolveDeliveryPolicy(definition EventDefinition, configOverride, callOverride DeliveryPolicyOverride) (DeliveryPolicy, error) {
	return contract.ResolveDeliveryPolicy(definition, configOverride, callOverride)
}

// LoadConfig reads STREAMING_* environment variables and validates the result.
func LoadConfig() (Config, []string, error) {
	return config.LoadConfig()
}

// NewHealthError constructs a readiness error with state and cause.
func NewHealthError(state HealthState, cause error) *HealthError {
	return contract.NewHealthError(state, cause)
}
