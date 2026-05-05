package producer

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v2/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v2/internal/config"
	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/emitter"
	"github.com/LerianStudio/lib-streaming/v2/internal/manifest"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

type (
	Emitter                = contract.Emitter
	Config                 = config.Config
	Event                  = contract.Event
	EmitRequest            = contract.EmitRequest
	EventDefinition        = contract.EventDefinition
	Catalog                = contract.Catalog
	DeliveryPolicy         = contract.DeliveryPolicy
	DeliveryPolicyOverride = contract.DeliveryPolicyOverride
	OutboxEnvelope         = contract.OutboxEnvelope
	PublisherDescriptor    = manifest.PublisherDescriptor
	HealthState            = contract.HealthState
	HealthError            = contract.HealthError
	ErrorClass             = contract.ErrorClass
	EmitError              = contract.EmitError
	Destination            = contract.Destination
	// headerFieldCheck is the producer-package alias for the canonical
	// contract.HeaderFieldCheck. Validating header-safe fields with the
	// shared shape prevents drift between contract-side and producer-side
	// check tables — there used to be a duplicate type declaration; now
	// there is exactly one definition (in the contract package).
	headerFieldCheck = contract.HeaderFieldCheck
)

const (
	ClassSerialization       = contract.ClassSerialization
	ClassValidation          = contract.ClassValidation
	ClassAuth                = contract.ClassAuth
	ClassTopicNotFound       = contract.ClassTopicNotFound
	ClassBrokerUnavailable   = contract.ClassBrokerUnavailable
	ClassNetworkTimeout      = contract.ClassNetworkTimeout
	ClassContextCanceled     = contract.ClassContextCanceled
	ClassBrokerOverloaded    = contract.ClassBrokerOverloaded
	TransportKafkaLike       = contract.TransportKafkaLike
	Healthy                  = contract.Healthy
	Degraded                 = contract.Degraded
	Down                     = contract.Down
	StreamingOutboxEventType = contract.StreamingOutboxEventType
	outboxEnvelopeVersion    = contract.OutboxEnvelopeVersion
	defaultBatchMaxBytes     = 1_048_576
	maxPayloadBytes          = contract.MaxPayloadBytes
	maxTenantIDBytes         = contract.MaxTenantIDBytes
	maxResourceTypeBytes     = contract.MaxResourceTypeBytes
	maxEventTypeBytes        = contract.MaxEventTypeBytes
	maxSourceBytes           = contract.MaxSourceBytes
	maxSubjectBytes          = contract.MaxSubjectBytes
	maxEventIDBytes          = contract.MaxEventIDBytes
	maxSchemaVersionBytes    = contract.MaxSchemaVersionBytes
	maxDataContentTypeBytes  = contract.MaxDataContentTypeBytes
	maxDataSchemaBytes       = contract.MaxDataSchemaBytes
)

// Producer-package sentinel re-exports. Only error sentinels actually
// referenced by code in this package are re-exported here. Sentinels that
// are produced exclusively by the contract package (e.g. catalog construction
// errors) are reachable through errors.Is wrapping and do not need a
// producer-package alias.
var (
	ErrMissingTenantID            = contract.ErrMissingTenantID
	ErrSystemEventsNotAllowed     = contract.ErrSystemEventsNotAllowed
	ErrMissingSource              = contract.ErrMissingSource
	ErrMissingResourceType        = contract.ErrMissingResourceType
	ErrMissingEventType           = contract.ErrMissingEventType
	ErrInvalidTenantID            = contract.ErrInvalidTenantID
	ErrInvalidResourceType        = contract.ErrInvalidResourceType
	ErrInvalidEventType           = contract.ErrInvalidEventType
	ErrInvalidSource              = contract.ErrInvalidSource
	ErrInvalidSubject             = contract.ErrInvalidSubject
	ErrInvalidEventID             = contract.ErrInvalidEventID
	ErrInvalidSchemaVersion       = contract.ErrInvalidSchemaVersion
	ErrInvalidDataContentType     = contract.ErrInvalidDataContentType
	ErrInvalidDataSchema          = contract.ErrInvalidDataSchema
	ErrInvalidEventDefinition     = contract.ErrInvalidEventDefinition
	ErrUnknownEventDefinition     = contract.ErrUnknownEventDefinition
	ErrInvalidPublisherDescriptor = contract.ErrInvalidPublisherDescriptor
	ErrInvalidDestination         = contract.ErrInvalidDestination
	ErrEmitterClosed              = contract.ErrEmitterClosed
	ErrEventDisabled              = contract.ErrEventDisabled
	ErrPayloadTooLarge            = contract.ErrPayloadTooLarge
	ErrNotJSON                    = contract.ErrNotJSON
	ErrMissingBrokers             = contract.ErrMissingBrokers
	ErrInvalidCompression         = contract.ErrInvalidCompression
	ErrInvalidAcks                = contract.ErrInvalidAcks
	ErrInvalidTLSConfig           = contract.ErrInvalidTLSConfig
	ErrPlaintextSASLNotAllowed    = contract.ErrPlaintextSASLNotAllowed
	ErrNilProducer                = contract.ErrNilProducer
	ErrCircuitOpen                = contract.ErrCircuitOpen
	ErrOutboxNotConfigured        = contract.ErrOutboxNotConfigured
	ErrOutboxTxUnsupported        = contract.ErrOutboxTxUnsupported
	ErrNilOutboxRegistry          = contract.ErrNilOutboxRegistry
)

func IsCallerError(err error) bool { return contract.IsCallerError(err) }

func newEmitRequest(request EmitRequest, copyPayload bool) (EmitRequest, error) {
	if copyPayload {
		return contract.NewEmitRequest(request)
	}

	return contract.NewEmitRequestNoCopy(request)
}

func cloneDeliveryPolicyOverrides(src map[string]DeliveryPolicyOverride) map[string]DeliveryPolicyOverride {
	return contract.CloneDeliveryPolicyOverrides(src)
}

func DefaultDeliveryPolicy() DeliveryPolicy { return contract.DefaultDeliveryPolicy() }

func ResolveDeliveryPolicy(definition EventDefinition, configOverride, callOverride DeliveryPolicyOverride) (DeliveryPolicy, error) {
	return contract.ResolveDeliveryPolicy(definition, configOverride, callOverride)
}

func sanitizeBrokerURL(s string) string { return contract.SanitizeBrokerURL(s) }

func hasControlChar(s string) bool { return contract.HasControlChar(s) }

func NewNoopEmitter() Emitter { return emitter.NewNoopEmitter() }

func NewPublisherDescriptor(descriptor PublisherDescriptor) (PublisherDescriptor, error) {
	return manifest.NewPublisherDescriptor(descriptor)
}

func NewHealthError(state HealthState, cause error) *HealthError {
	return contract.NewHealthError(state, cause)
}

// buildCloudEventsHeaders is the cold-path kgo-typed wrapper retained for
// internal callers that still want kgo.RecordHeader (e.g. roundtrip
// property/benchmark tests that call ParseCloudEventsHeaders against the
// kgo shape). The Emit hot path uses buildCloudEventsTransportHeaders →
// cloudevents.BuildTransportHeaders directly so we no longer pay the
// kgo→transport conversion per Emit.
//
//nolint:unused // referenced from _test.go files (property_test, benchmark_test).
func buildCloudEventsHeaders(event Event) []kgo.RecordHeader {
	return cloudevents.BuildHeaders(event)
}

// buildCloudEventsTransportHeaders is the canonical hot-path header builder
// for every Emit. Calls into cloudevents.BuildTransportHeaders directly,
// allocating a single []transport.Header slice with no intermediate kgo
// type and no per-byte deep copy. The Kafka adapter (toKgoHeaders) reuses
// the byte values verbatim — the message lifetime is fully owned by
// Publish, which fires its produce-callback synchronously inside the
// select gate, so re-aliasing the byte slice is safe.
func buildCloudEventsTransportHeaders(event Event) []transport.Header {
	return cloudevents.BuildTransportHeaders(event)
}
