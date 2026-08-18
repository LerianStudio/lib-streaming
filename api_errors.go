package streaming

import (
	"github.com/LerianStudio/lib-streaming/v3/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v3/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
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
	ErrSystemEventsNotAllowed             = contract.ErrSystemEventsNotAllowed
	ErrMissingSource                      = contract.ErrMissingSource
	ErrMissingResourceType                = contract.ErrMissingResourceType
	ErrMissingEventType                   = contract.ErrMissingEventType
	ErrInvalidTenantID                    = contract.ErrInvalidTenantID
	ErrInvalidResourceType                = contract.ErrInvalidResourceType
	ErrInvalidEventType                   = contract.ErrInvalidEventType
	ErrInvalidSource                      = contract.ErrInvalidSource
	ErrInvalidSubject                     = contract.ErrInvalidSubject
	ErrInvalidEventID                     = contract.ErrInvalidEventID
	ErrInvalidSchemaVersion               = contract.ErrInvalidSchemaVersion
	ErrInvalidDataContentType             = contract.ErrInvalidDataContentType
	ErrInvalidDataSchema                  = contract.ErrInvalidDataSchema
	ErrInvalidEventDefinition             = contract.ErrInvalidEventDefinition
	ErrInvalidOutboxEnvelope              = contract.ErrInvalidOutboxEnvelope
	ErrInvalidTraceCarrier                = contract.ErrInvalidTraceCarrier
	ErrDuplicateEventDefinition           = contract.ErrDuplicateEventDefinition
	ErrUnknownEventDefinition             = contract.ErrUnknownEventDefinition
	ErrInvalidDeliveryPolicy              = contract.ErrInvalidDeliveryPolicy
	ErrInvalidPublisherDescriptor         = contract.ErrInvalidPublisherDescriptor
	ErrInvalidRouteDefinition             = contract.ErrInvalidRouteDefinition
	ErrInvalidDestination                 = contract.ErrInvalidDestination
	ErrDuplicateRouteDefinition           = contract.ErrDuplicateRouteDefinition
	ErrNoRoutesConfigured                 = contract.ErrNoRoutesConfigured
	ErrNoRequiredRoute                    = contract.ErrNoRequiredRoute
	ErrMissingTarget                      = contract.ErrMissingTarget
	ErrMultiTransportRuntimeNotConfigured = contract.ErrMultiTransportRuntimeNotConfigured
	ErrEmitterClosed                      = contract.ErrEmitterClosed
	ErrEventDisabled                      = contract.ErrEventDisabled
	ErrPayloadTooLarge                    = contract.ErrPayloadTooLarge
	ErrNotJSON                            = contract.ErrNotJSON
	ErrInvalidCompression                 = contract.ErrInvalidCompression
	ErrInvalidAcks                        = contract.ErrInvalidAcks
	ErrInvalidTLSConfig                   = contract.ErrInvalidTLSConfig
	ErrPlaintextSASLNotAllowed            = contract.ErrPlaintextSASLNotAllowed
	ErrInvalidSASLMechanism               = contract.ErrInvalidSASLMechanism
	ErrInvalidSchemaRegistryConfig        = contract.ErrInvalidSchemaRegistryConfig
	ErrNilProducer                        = contract.ErrNilProducer
	ErrCircuitOpen                        = contract.ErrCircuitOpen
	ErrOutboxNotConfigured                = contract.ErrOutboxNotConfigured
	ErrOutboxTxUnsupported                = contract.ErrOutboxTxUnsupported
	ErrNilOutboxRegistry                  = contract.ErrNilOutboxRegistry
	ErrMissingRequiredHeader              = cloudevents.ErrMissingRequiredHeader
	ErrUnsupportedSpecVersion             = cloudevents.ErrUnsupportedSpecVersion
)

// Producer and consumer config sentinels, disambiguated by side.
//
// The producer and the consumer each define their OWN "missing brokers" and
// "invalid config field" values — different error variables that happen to
// describe the same class of mistake. A single bare streaming.ErrMissingBrokers
// therefore could not be right for both, and the one the root used to export
// (the producer's) silently returned false for every errors.Is against a
// consumer Build failure. Both sides are now named for the side they belong to,
// so there is no bare name left to guess wrong about.
var (
	// ErrProducerMissingBrokers is returned by producer config validation
	// (LoadConfig, Builder.Build) when the broker list is empty.
	ErrProducerMissingBrokers = contract.ErrMissingBrokers
	// ErrProducerInvalidConfigField is returned by producer config validation
	// for an out-of-range numeric or duration field.
	ErrProducerInvalidConfigField = contract.ErrInvalidConfigField

	// ErrConsumerMissingBrokers is returned by ConsumerBuilder.Build when the
	// consumer is enabled with an empty broker list.
	ErrConsumerMissingBrokers = consumer.ErrMissingBrokers
	// ErrConsumerMissingGroup is returned by ConsumerBuilder.Build when the
	// consumer group id is empty.
	ErrConsumerMissingGroup = consumer.ErrMissingGroup
	// ErrConsumerMissingTopics is returned by ConsumerBuilder.Build when
	// neither Apps(...) nor Topics(...) resolves to a subscription.
	ErrConsumerMissingTopics = consumer.ErrMissingTopics
	// ErrConsumerInvalidConfigField is returned by ConsumerBuilder.Build for an
	// out-of-range numeric/duration field or a malformed entry in Apps.
	ErrConsumerInvalidConfigField = consumer.ErrInvalidConfigField
	// ErrNilHandler is returned by ConsumerBuilder.Build when neither
	// Handler(...) nor any On(...) handler was wired.
	ErrNilHandler = consumer.ErrNilHandler
	// ErrHandlerAndDispatchBothSet is returned by ConsumerBuilder.Build when a
	// consumer wires both Handler (whole-stream) and On (per-event dispatch).
	ErrHandlerAndDispatchBothSet = consumer.ErrHandlerAndDispatchBothSet
	// ErrHandlerAndExpectSourcesBothSet is returned by ConsumerBuilder.Build
	// when a whole-stream Handler is combined with ExpectSources, which only
	// the dispatcher enforces.
	ErrHandlerAndExpectSourcesBothSet = consumer.ErrHandlerAndExpectSourcesBothSet
	// ErrHandlerAndUnmatchedPolicyBothSet is returned by ConsumerBuilder.Build
	// when a whole-stream Handler is combined with UnmatchedPolicy, which only
	// the dispatcher enforces.
	ErrHandlerAndUnmatchedPolicyBothSet = consumer.ErrHandlerAndUnmatchedPolicyBothSet
	// ErrAmbiguousSourceVerification is returned by ConsumerBuilder.Build when
	// Apps(...) and Topics(...) are both set without an explicit
	// ExpectSources(...).
	ErrAmbiguousSourceVerification = consumer.ErrAmbiguousSourceVerification
	// ErrExpectSourcesMissingApp is returned by ConsumerBuilder.Build when an
	// explicit ExpectSources(...) list omits an app named in Apps(...).
	ErrExpectSourcesMissingApp = consumer.ErrExpectSourcesMissingApp
	// ErrInvalidExpectSource is returned by ConsumerBuilder.Build when an
	// ExpectSources(...) entry is not a legal ce-source.
	ErrInvalidExpectSource = consumer.ErrInvalidExpectSource
)

// IsCallerError reports whether err is caller-correctable rather than infrastructure-caused.
//
// For *MultiEmitError, returns true only when EVERY Required-route failure
// is itself caller-correctable. A single infrastructure-class failure (broker
// unavailable, circuit open, network timeout) anywhere in the Required set
// flips the answer to false — there is at least one fault the caller cannot
// fix on its own, so the aggregate is treated as infrastructure.
func IsCallerError(err error) bool {
	if err == nil {
		return false
	}

	if isCaller, matched := contract.IsMultiEmitErrorCallerError(err); matched {
		return isCaller
	}

	return contract.IsCallerError(err)
}
