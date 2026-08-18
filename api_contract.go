package streaming

import (
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/LerianStudio/lib-streaming/v3/internal/config"
	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/kafkasec"
	"github.com/LerianStudio/lib-streaming/v3/internal/manifest"
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
	// EventClass says whether a definition is a business fact or a
	// service-to-service command, selecting the queue it rides.
	EventClass = contract.EventClass
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
	// TransportKind identifies the outbound transport family used by a route.
	TransportKind = contract.TransportKind
	// RouteRequirement declares whether a route is required or best-effort.
	RouteRequirement = contract.RouteRequirement
	// Destination identifies a concrete transport destination.
	Destination = contract.Destination
	// RouteDefinition maps one event definition to one transport destination.
	RouteDefinition = contract.RouteDefinition
	// RouteTable is an immutable registry of route definitions.
	RouteTable = contract.RouteTable
	// OutboxEnvelope is the persisted streaming outbox payload shape.
	OutboxEnvelope = contract.OutboxEnvelope
	// TraceCarrier is the bounded W3C trace context persisted with an outbox envelope.
	TraceCarrier = contract.TraceCarrier
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
	// ManifestRoute is one route entry rendered in a manifest document. It
	// is populated only when BuildManifest is called with a non-empty
	// RouteTable.
	ManifestRoute = manifest.ManifestRoute
)

const (
	DirectModeDirect                = contract.DirectModeDirect
	DirectModeSkip                  = contract.DirectModeSkip
	OutboxModeNever                 = contract.OutboxModeNever
	OutboxModeFallbackOnCircuitOpen = contract.OutboxModeFallbackOnCircuitOpen
	OutboxModeAlways                = contract.OutboxModeAlways
	DLQModeNever                    = contract.DLQModeNever
	DLQModeOnRoutableFailure        = contract.DLQModeOnRoutableFailure
	TransportKafkaLike              = contract.TransportKafkaLike
	TransportSQS                    = contract.TransportSQS
	TransportRabbitMQ               = contract.TransportRabbitMQ
	TransportEventBridge            = contract.TransportEventBridge
	TransportCustom                 = contract.TransportCustom
	RouteRequired                   = contract.RouteRequired
	RouteOptional                   = contract.RouteOptional
	// ClassFact is the default event class: a business fact, published on
	// the app topic, ignored by consumers that registered no handler.
	ClassFact = contract.ClassFact
	// ClassCommand marks a service-to-service command: published on the
	// app's ".commands" topic, QUARANTINED by a consumer that registered no
	// handler for it.
	ClassCommand              = contract.ClassCommand
	StreamingOutboxEventType  = contract.StreamingOutboxEventType
	TraceParentHeader         = contract.TraceParentHeader
	TraceStateHeader          = contract.TraceStateHeader
	MaxTraceCarrierEntries    = contract.MaxTraceCarrierEntries
	MaxTraceCarrierValueBytes = contract.MaxTraceCarrierValueBytes
	Healthy                   = contract.Healthy
	Degraded                  = contract.Degraded
	Down                      = contract.Down
	ManifestVersion           = manifest.ManifestVersion
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

// NewRouteDefinition validates and normalizes a route definition.
func NewRouteDefinition(route RouteDefinition) (RouteDefinition, error) {
	return contract.NewRouteDefinition(route)
}

// NewRouteTable builds an immutable route table from route definitions.
func NewRouteTable(routes ...RouteDefinition) (RouteTable, error) {
	return contract.NewRouteTable(routes...)
}

// Topic-name constants. Exposed so operators and provisioning code derive the
// same names the runtime does, rather than re-implementing the concatenation.
const (
	// TopicPrefix is the fixed namespace on every lib-streaming topic.
	TopicPrefix = contract.TopicPrefix
	// DLQTopicSuffix is appended to a topic to derive its dead-letter topic.
	DLQTopicSuffix = contract.DLQTopicSuffix
	// CommandsTopicSuffix is appended to an app topic to derive its
	// service-to-service commands queue. There is no ".commands.dlq".
	CommandsTopicSuffix = contract.CommandsTopicSuffix
	// MaxKafkaTopicNameBytes is Kafka's protocol-level topic-name limit.
	MaxKafkaTopicNameBytes = contract.MaxKafkaTopicNameBytes
)

// AppTopic returns the FACT topic a producing application publishes to:
// "lerian.streaming." + source.
//
// Every business FACT that application emits — every resource type, every
// event type, every schema version — rides this one topic. Its
// service-to-service COMMANDS ride AppCommandsTopic instead. Use it when
// provisioning topics, writing Kafka ACLs, or naming an explicit Kafka
// destination.
//
// It VALIDATES source and returns an error for a malformed one, because every
// caller of this function is deriving a name something else will act on:
// provisioning creates the topic, an ACL grants it, a route publishes to it.
// Returning "lerian.streaming." for an empty source — as the unvalidated
// version did — hands that garbage straight through to a real broker.
//
// The internal derivation stays validation-free on the Emit hot path, where
// the source was already proven legal at Build.
func AppTopic(source string) (string, error) {
	if err := contract.ValidateSource(source); err != nil {
		return "", err
	}

	return contract.AppTopic(source), nil
}

// AppDLQTopic returns the dead-letter topic for an application's stream.
// Like AppTopic, it validates source and returns an error for a malformed one.
func AppDLQTopic(source string) (string, error) {
	if err := contract.ValidateSource(source); err != nil {
		return "", err
	}

	return contract.AppDLQTopic(source), nil
}

// AppCommandsTopic returns the queue carrying an application's
// service-to-service COMMANDS: "lerian.streaming." + source + ".commands".
//
// It is the THIRD and last name a command-emitting application writes (its
// topic, its commands topic, its dlq); an application that emits only facts
// writes two. Consumers READ the commands topics of the applications that
// command them.
//
// There is no ".commands.dlq": a consumer quarantines into its own ".dlq"
// and a producer route-DLQs a failed command publish into its own ".dlq".
//
// Like AppTopic, it validates source and returns an error for a malformed one.
func AppCommandsTopic(source string) (string, error) {
	if err := contract.ValidateSource(source); err != nil {
		return "", err
	}

	return contract.AppCommandsTopic(source), nil
}

// ValidateSource reports whether source is a legal ce-source: a single
// dot-free lowercase segment matching ^[a-z0-9][a-z0-9_-]*$, short enough that
// the derived topic plus ".commands" — the longest derived name — fits Kafka's
// 249-byte limit.
//
// LoadConfig, Builder.Build, NewPublisherDescriptor, and producer preflight all
// apply it; it is exported so a service can validate its own configuration
// before constructing anything. Returns ErrMissingSource for empty and an
// ErrInvalidSource-wrapped error for malformed.
func ValidateSource(source string) error {
	return contract.ValidateSource(source)
}

// KafkaTopic returns a Kafka-like destination for topic. For the default
// destination of a lib-streaming producer, pass AppTopic(source).
func KafkaTopic(topic string) Destination {
	return Destination{Kind: TransportKafkaLike, Name: topic}
}

// SQSQueueURL returns an SQS destination for queueURL.
func SQSQueueURL(queueURL string) Destination {
	return Destination{Kind: TransportSQS, Address: queueURL}
}

// RabbitMQRoute returns a RabbitMQ destination for exchange and routingKey.
func RabbitMQRoute(exchange, routingKey string) Destination {
	return Destination{Kind: TransportRabbitMQ, Name: exchange, Address: routingKey}
}

// EventBridgeBus returns an EventBridge destination for busName.
func EventBridgeBus(busName string) Destination {
	return Destination{Kind: TransportEventBridge, Name: busName}
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

// NewSchemaRegistryClient builds a Schema Registry client from cfg's
// SchemaRegistry* fields for the billing serialize path. It is the public entry
// point producers (billing-api etc.) use to obtain the *sr.Client that
// billing.NewSerializer requires — the internal kafkasec builder is not
// importable from outside this module, so this re-export is the single
// reachable, hardened construction path.
//
// It delegates verbatim to the authoritative builder, inheriting its fail-closed
// guards: an empty URL and a partial (XOR) username/password credential both
// return an error wrapping ErrInvalidSchemaRegistryConfig. Constructing the
// client performs no network I/O. The returned error never includes the
// registry password.
func NewSchemaRegistryClient(cfg Config) (*sr.Client, error) {
	return kafkasec.BuildSchemaRegistryClient(cfg.SchemaRegistryURL, cfg.SchemaRegistryUsername, cfg.SchemaRegistryPassword)
}
