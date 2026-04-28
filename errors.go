package streaming

import (
	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/internal/contract"
)

type (
	// ErrorClass classifies publish failures into operational buckets.
	ErrorClass = contract.ErrorClass
	// EmitError carries structured publish-failure context.
	EmitError = contract.EmitError
)

const (
	ClassSerialization     = contract.ClassSerialization
	ClassValidation        = contract.ClassValidation
	ClassAuth              = contract.ClassAuth
	ClassTopicNotFound     = contract.ClassTopicNotFound
	ClassBrokerUnavailable = contract.ClassBrokerUnavailable
	ClassNetworkTimeout    = contract.ClassNetworkTimeout
	ClassContextCanceled   = contract.ClassContextCanceled
	ClassBrokerOverloaded  = contract.ClassBrokerOverloaded
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
	ErrInvalidOutboxEnvelope      = contract.ErrInvalidOutboxEnvelope
	ErrDuplicateEventDefinition   = contract.ErrDuplicateEventDefinition
	ErrUnknownEventDefinition     = contract.ErrUnknownEventDefinition
	ErrInvalidDeliveryPolicy      = contract.ErrInvalidDeliveryPolicy
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
	ErrMissingRequiredHeader      = cloudevents.ErrMissingRequiredHeader
	ErrUnsupportedSpecVersion     = cloudevents.ErrUnsupportedSpecVersion
)

// IsCallerError reports whether err is caller-correctable rather than infrastructure-caused.
func IsCallerError(err error) bool {
	return contract.IsCallerError(err)
}
