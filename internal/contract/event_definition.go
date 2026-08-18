package contract

import (
	"context"
	"fmt"
)

// EventDefinition is the static contract for one event a producer supports.
// Catalog, manifest generation, introspection, and policy resolution all start
// from this type.
type EventDefinition struct {
	Key             string
	ResourceType    string
	EventType       string
	SchemaVersion   string
	DataContentType string
	DataSchema      string
	SystemEvent     bool
	Description     string
	DefaultPolicy   DeliveryPolicy
}

// NewEventDefinition validates and normalizes an EventDefinition.
//
// Asserter trident fires under operation="event_definition.new" with
// structured field violation={"missing_key"|"missing_resource_type"|
// "missing_event_type"} on each required-field rejection so dashboards
// distinguish the failure modes without parsing wrapped sentinels.
func NewEventDefinition(definition EventDefinition) (EventDefinition, error) {
	if definition.Key == "" {
		a := newContractAsserter("event_definition.new")
		_ = a.That(context.Background(), false, "event definition Key is required",
			"violation", "missing_key",
		)

		return EventDefinition{}, fmt.Errorf("%w: key required", ErrInvalidEventDefinition)
	}

	if definition.ResourceType == "" {
		a := newContractAsserter("event_definition.new")
		_ = a.That(context.Background(), false, "event definition ResourceType is required",
			"violation", "missing_resource_type",
			"key", definition.Key,
		)

		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, ErrMissingResourceType)
	}

	if definition.EventType == "" {
		a := newContractAsserter("event_definition.new")
		_ = a.That(context.Background(), false, "event definition EventType is required",
			"violation", "missing_event_type",
			"key", definition.Key,
			"resource_type", definition.ResourceType,
		)

		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, ErrMissingEventType)
	}

	if definition.SchemaVersion == "" {
		definition.SchemaVersion = defaultSchemaVersion
	}

	if definition.DataContentType == "" {
		definition.DataContentType = defaultDataContentType
	}

	// SchemaVersion semver gate. Runs at construction time so unparseable
	// semver fails fast at NewEventDefinition / NewCatalog. The version no
	// longer influences any topic (it left the topic entirely in v3), but
	// ce-schemaversion is now the ONLY version carrier on the wire, so a
	// garbage value would be undetectable downstream — the catalog stays
	// the single source of truth for SchemaVersion shape.
	//
	// Asserter trident fires under operation="event_definition.schema_version"
	// with violation="schema_parse_failed" so dashboards distinguish this
	// from the missing-required-field branches above.
	if _, ok := parseMajorVersionStrict(definition.SchemaVersion); !ok {
		a := newContractAsserter("event_definition.schema_version")
		_ = a.That(context.Background(), false, "event definition SchemaVersion must parse as semver",
			"violation", "schema_parse_failed",
			"key", definition.Key,
			"resource_type", definition.ResourceType,
			"event_type", definition.EventType,
			"schema_version", definition.SchemaVersion,
		)

		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, ErrInvalidSchemaVersion)
	}

	definition.DefaultPolicy = definition.DefaultPolicy.Normalize()

	if err := validateEventDefinitionHeaderFields(definition); err != nil {
		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, err)
	}

	if err := definition.DefaultPolicy.Validate(); err != nil {
		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, err)
	}

	return definition, nil
}

// EventKey is the "<resourceType>.<eventType>" dispatch key a consumer
// registers a handler under. It is the routing unit that replaced the
// per-definition topic: under one-topic-per-app, a consumer receives the
// producer's whole stream on one subscription and selects by this key,
// which it reads from the ce-resourcetype / ce-eventtype headers.
//
// There is deliberately no EventDefinition.Topic in v3. A definition has no
// topic of its own — the producing APPLICATION has exactly one (AppTopic),
// and it is a property of the producer's ce-source, not of any catalog entry.
func (d EventDefinition) EventKey() string {
	return d.ResourceType + "." + d.EventType
}

func validateEventDefinitionHeaderFields(definition EventDefinition) error {
	checks := [...]HeaderFieldCheck{
		{Value: definition.Key, MaxBytes: MaxEventIDBytes, Sentinel: ErrInvalidEventDefinition},
		{Value: definition.ResourceType, MaxBytes: MaxResourceTypeBytes, Sentinel: ErrInvalidResourceType},
		{Value: definition.EventType, MaxBytes: MaxEventTypeBytes, Sentinel: ErrInvalidEventType},
		{Value: definition.SchemaVersion, MaxBytes: MaxSchemaVersionBytes, Sentinel: ErrInvalidSchemaVersion},
		{Value: definition.DataContentType, MaxBytes: MaxDataContentTypeBytes, Sentinel: ErrInvalidDataContentType},
		{Value: definition.DataSchema, MaxBytes: MaxDataSchemaBytes, Sentinel: ErrInvalidDataSchema},
	}

	for _, c := range checks {
		if len(c.Value) > c.MaxBytes || HasControlChar(c.Value) {
			return c.Sentinel
		}
	}

	return nil
}
