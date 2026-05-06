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
	// semver fails fast at NewEventDefinition / NewCatalog rather than
	// silently producing the base topic at runtime ("two-point-oh" lands
	// on lerian.streaming.<r>.<e> instead of the .v2 topic the consumer
	// subscribed to). Topic() is now a zero-allocation hot-path helper
	// that does NOT re-validate; the catalog is the single source of
	// truth for SchemaVersion shape.
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

// Topic returns the Kafka topic derived from the definition.
func (d EventDefinition) Topic() string {
	return (&Event{
		ResourceType:  d.ResourceType,
		EventType:     d.EventType,
		SchemaVersion: d.SchemaVersion,
	}).Topic()
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
