package manifest

import "github.com/LerianStudio/lib-streaming/v3/internal/contract"

// ManifestVersion is the wire-version of the JSON document returned by
// BuildManifest / NewStreamingHandler. Follows semver:
//   - Minor bumps (1.x.0) are additive — new fields, no removals or type
//     changes. Existing consumers parse the new manifest unchanged.
//   - Major bumps (X.0.0) remove or change a field. Coordinate with all
//     downstream contract-diffing tools before bumping.
//
// 2.0.0 is the one-topic-per-app cut: the per-event "topic" field is GONE
// (a definition has no topic of its own) and the document carries the
// application's single "topic" / "dlqTopic" pair instead. The publisher's
// "sourceBase" field was renamed "source" to match the strict single-segment
// ce-source it now holds.
const ManifestVersion = "2.0.0"

// ManifestDocument is the JSON-serializable description of a producer's event
// catalog and default delivery policies.
type ManifestDocument struct {
	Version   string              `json:"version"`
	Publisher PublisherDescriptor `json:"publisher"`
	// Topic is the ONE topic this application publishes every event to,
	// derived from the publisher's source. Under the v3 one-topic-per-app
	// contract this is a document-level fact, not a per-event one.
	Topic string `json:"topic"`
	// DLQTopic is the dead-letter topic derived from Topic.
	DLQTopic string          `json:"dlqTopic"`
	Events   []ManifestEvent `json:"events"`
	// Routes is the active route table for this producer. Omitted (nil/empty)
	// when no routes are wired so producers without a multi-target topology
	// emit a clean catalog-only document.
	Routes []ManifestRoute `json:"routes,omitempty"`
}

// ManifestEvent is one catalog entry rendered for export and introspection.
type ManifestEvent struct {
	Key          string `json:"key"`
	ResourceType string `json:"resourceType"`
	EventType    string `json:"eventType"`
	// EventKey is "<resourceType>.<eventType>" — the dispatch key a consumer
	// registers a handler under. It replaced the per-event "topic" field:
	// there is no per-definition topic in v3, only this selector inside the
	// application's single stream.
	//
	// EventKey is NOT unique across a manifest. A catalog may deliberately
	// hold two definitions that share (ResourceType, EventType) and differ
	// only in schemaVersion major — that is how a producer ships v1 and v2 of
	// the same fact through a migration window without minting a second event
	// name. Their catalog "key" values differ; their "eventKey" values do not.
	//
	// Consumers of this manifest MUST therefore treat eventKey as a
	// many-to-one selector and branch on schemaVersion. A handler registered
	// for such a key receives BOTH majors, and a v2 payload parsed as v1 is
	// silent data corruption, not a decode error.
	EventKey        string `json:"eventKey"`
	SchemaVersion   string `json:"schemaVersion"`
	DataContentType string `json:"dataContentType"`
	DataSchema      string `json:"dataSchema,omitempty"`
	SystemEvent     bool   `json:"systemEvent"`
	Description     string `json:"description,omitempty"`
	// DefaultPolicy is the EventDefinition default policy as registered in
	// the catalog. Runtime per-event overrides from Config.PolicyOverrides
	// are NOT reflected here.
	DefaultPolicy DeliveryPolicy `json:"defaultPolicy"`
}

// ManifestRoute is one route entry rendered for ops/contract introspection.
//
// Routes are deterministically ordered by the underlying RouteTable
// (definition key first, then route key) so the JSON document is
// byte-stable across builds.
type ManifestRoute struct {
	Key string `json:"key"`
	// DefinitionKey is OMITTED for a catch-all route (the shape the default
	// app-topic path uses), because a catch-all serves every definition and
	// naming none of them is the accurate answer. Emitting `"definitionKey":
	// ""` instead read as "scoped to the definition whose key is the empty
	// string", which is not a thing.
	DefinitionKey string        `json:"definitionKey,omitempty"`
	Target        string        `json:"target"`
	Transport     TransportKind `json:"transport"`
	Destination   string        `json:"destination"`
	Required      bool          `json:"required"`
	DLQConfigured bool          `json:"dlqConfigured"`
}

// BuildManifest renders a catalog, descriptor, and route table into an
// exportable document. Performs no file, network, auth, or route side
// effects. The Routes field is populated when the supplied route table has
// at least one entry; pass an empty RouteTable to omit the field.
//
// Routes are deterministically ordered (definition key, then route key) so
// the JSON document is byte-stable across builds.
func BuildManifest(descriptor PublisherDescriptor, catalog Catalog, routes RouteTable) (ManifestDocument, error) {
	descriptor, err := NewPublisherDescriptor(descriptor)
	if err != nil {
		return ManifestDocument{}, err
	}

	// catalog.Definitions() already returns validated EventDefinition values
	// (NewCatalog ran each through NewEventDefinition at construction).
	// Re-validating here was a redundant allocation on every manifest build.
	definitions := catalog.Definitions()

	events := make([]ManifestEvent, 0, len(definitions))
	for _, definition := range definitions {
		events = append(events, ManifestEvent{
			Key:             definition.Key,
			ResourceType:    definition.ResourceType,
			EventType:       definition.EventType,
			EventKey:        definition.EventKey(),
			SchemaVersion:   definition.SchemaVersion,
			DataContentType: definition.DataContentType,
			DataSchema:      definition.DataSchema,
			SystemEvent:     definition.SystemEvent,
			Description:     definition.Description,
			DefaultPolicy:   definition.DefaultPolicy.Normalize(),
		})
	}

	return ManifestDocument{
		Version:   ManifestVersion,
		Publisher: descriptor,
		Topic:     contract.AppTopic(descriptor.Source),
		DLQTopic:  contract.AppDLQTopic(descriptor.Source),
		Events:    events,
		Routes:    renderRoutes(routes),
	}, nil
}

func renderRoutes(routes RouteTable) []ManifestRoute {
	defs := routes.Definitions()
	if len(defs) == 0 {
		return nil
	}

	out := make([]ManifestRoute, 0, len(defs))
	for _, route := range defs {
		out = append(out, ManifestRoute{
			Key:           route.Key,
			DefinitionKey: route.DefinitionKey,
			Target:        route.Target,
			Transport:     route.Destination.Kind,
			Destination:   destinationDisplay(route.Destination),
			Required:      route.Requirement == "" || route.Requirement == contract.RouteRequired,
			DLQConfigured: route.DLQ != nil,
		})
	}

	return out
}

// destinationDisplay returns a stable, single-string view of the
// destination suitable for ops dashboards. This is the WIRE-PINNED
// manifest renderer — its output is part of the
// streaming.ManifestVersion contract surfaced by BuildManifest and
// consumed by ops dashboards and contract introspection clients.
// Changing any branch's format is a manifest version bump and requires
// a CHANGELOG.md migration note.
//
// Format is transport-specific:
//   - kafka → topic name
//   - sqs → queue URL
//   - rabbitmq → exchange "/" routing key
//   - eventbridge → bus name
//   - custom → "name|address" (one or both sides may be empty)
//
// A separate, non-wire renderer for log lines, trace attributes, and
// *RouteError messages lives at describeDestination in
// internal/producer/emit_multi.go. The two renderers intentionally
// differ for RabbitMQ ("name/address" here vs "name:address" in logs)
// and Custom ("name|address" here vs "name address" in logs). Do NOT
// unify them — the log-side renderer has no wire commitment and may
// evolve freely, while this renderer cannot change without a manifest
// version bump.
//
// Every Address-rendering branch is wrapped in contract.SanitizeBrokerURL
// for defense-in-depth: today's Destination.Validate already rejects
// userinfo and credential-shaped query keys at construction time, but a
// regression in Validate or a future custom transport could leak a
// credential into a manifest document served on /streaming. Sanitizing
// at render time is idempotent on clean inputs and adds one regex pass
// per route — the manifest handler pre-marshals once at construction,
// so this cost is paid at NewStreamingHandler, not per-request.
//
// Name fields (kafka topic, eventbridge bus name, rabbitmq exchange
// name) are NOT URL-shaped and skip sanitization. Destination.Validate
// already rejects credential-like names at construction.
func destinationDisplay(d contract.Destination) string {
	switch d.Kind {
	case contract.TransportRabbitMQ:
		if d.Address == "" {
			return d.Name
		}

		return d.Name + "/" + contract.SanitizeBrokerURL(d.Address)
	case contract.TransportSQS:
		return contract.SanitizeBrokerURL(d.Address)
	case contract.TransportKafkaLike, contract.TransportEventBridge:
		return d.Name
	case contract.TransportCustom:
		if d.Name != "" && d.Address != "" {
			return d.Name + "|" + contract.SanitizeBrokerURL(d.Address)
		}

		if d.Name != "" {
			return d.Name
		}

		return contract.SanitizeBrokerURL(d.Address)
	}

	return ""
}
