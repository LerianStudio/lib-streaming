package contract

import "fmt"

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
func NewEventDefinition(definition EventDefinition) (EventDefinition, error) {
	if definition.Key == "" {
		return EventDefinition{}, fmt.Errorf("%w: key required", ErrInvalidEventDefinition)
	}

	if definition.ResourceType == "" {
		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, ErrMissingResourceType)
	}

	if definition.EventType == "" {
		return EventDefinition{}, fmt.Errorf("%w: %w", ErrInvalidEventDefinition, ErrMissingEventType)
	}

	if definition.SchemaVersion == "" {
		definition.SchemaVersion = defaultSchemaVersion
	}

	if definition.DataContentType == "" {
		definition.DataContentType = defaultDataContentType
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
