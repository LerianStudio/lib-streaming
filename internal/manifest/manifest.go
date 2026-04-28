package manifest

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

// BuildManifest renders a catalog and publisher descriptor into an exportable
// document. It performs no file, network, auth, or route side effects.
func BuildManifest(descriptor PublisherDescriptor, catalog Catalog) (ManifestDocument, error) {
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
	}, nil
}
