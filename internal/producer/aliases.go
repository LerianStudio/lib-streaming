package producer

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/internal/config"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/emitter"
	"github.com/LerianStudio/lib-streaming/internal/manifest"
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
	ErrDuplicateEventDefinition   = contract.ErrDuplicateEventDefinition
	ErrUnknownEventDefinition     = contract.ErrUnknownEventDefinition
	ErrInvalidDeliveryPolicy      = contract.ErrInvalidDeliveryPolicy
	ErrInvalidOutboxEnvelope      = contract.ErrInvalidOutboxEnvelope
	ErrInvalidPublisherDescriptor = contract.ErrInvalidPublisherDescriptor
	ErrEmitterClosed              = contract.ErrEmitterClosed
	ErrEventDisabled              = contract.ErrEventDisabled
	ErrPayloadTooLarge            = contract.ErrPayloadTooLarge
	ErrNotJSON                    = contract.ErrNotJSON
	ErrMissingBrokers             = contract.ErrMissingBrokers
	ErrInvalidCompression         = contract.ErrInvalidCompression
	ErrInvalidAcks                = contract.ErrInvalidAcks
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

func buildCloudEventsHeaders(event Event) []kgo.RecordHeader {
	return cloudevents.BuildHeaders(event)
}
