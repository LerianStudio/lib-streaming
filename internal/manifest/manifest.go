package manifest

import "github.com/LerianStudio/lib-streaming/v2/internal/contract"

// ManifestVersion is the wire-version of the JSON document returned by
// BuildManifest / NewStreamingHandler. Follows semver:
//   - Minor bumps (1.x.0) are additive — new fields, no removals or type
//     changes. Existing consumers parse the new manifest unchanged.
//   - Major bumps (X.0.0) remove or change a field. Coordinate with all
//     downstream contract-diffing tools before bumping.
const ManifestVersion = "1.0.0"

// ManifestDocument is the JSON-serializable description of a producer's event
// catalog and default delivery policies.
type ManifestDocument struct {
	Version   string              `json:"version"`
	Publisher PublisherDescriptor `json:"publisher"`
	Events    []ManifestEvent     `json:"events"`
	// Routes is the active route table for this producer. Omitted (nil/empty)
	// when no routes are wired so producers without a multi-target topology
	// emit a clean catalog-only document.
	Routes []ManifestRoute `json:"routes,omitempty"`
}

// ManifestEvent is one catalog entry rendered for export and introspection.
type ManifestEvent struct {
	Key             string `json:"key"`
	ResourceType    string `json:"resourceType"`
	EventType       string `json:"eventType"`
	Topic           string `json:"topic"`
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
	Key           string        `json:"key"`
	DefinitionKey string        `json:"definitionKey"`
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
			Topic:           definition.Topic(),
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
