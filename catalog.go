package streaming

import (
	"fmt"
	"sort"
)

// Catalog is an immutable, deterministically ordered registry of event
// definitions.
type Catalog struct {
	definitions []EventDefinition
	byKey       map[string]EventDefinition
}

// NewCatalog validates definitions, rejects duplicates, and stores them in
// deterministic key order.
func NewCatalog(definitions ...EventDefinition) (Catalog, error) {
	ordered := make([]EventDefinition, 0, len(definitions))
	byKey := make(map[string]EventDefinition, len(definitions))
	byContract := make(map[string]string, len(definitions))

	for _, raw := range definitions {
		definition, err := NewEventDefinition(raw)
		if err != nil {
			return Catalog{}, err
		}

		if _, exists := byKey[definition.Key]; exists {
			return Catalog{}, fmt.Errorf("%w: key %q", ErrDuplicateEventDefinition, definition.Key)
		}

		contractKey := definition.ResourceType + "." + definition.EventType + "." + definition.SchemaVersion
		if existingKey, exists := byContract[contractKey]; exists {
			return Catalog{}, fmt.Errorf(
				"%w: %q and %q resolve to %q",
				ErrDuplicateEventDefinition,
				existingKey,
				definition.Key,
				contractKey,
			)
		}

		ordered = append(ordered, definition)
		byKey[definition.Key] = definition
		byContract[contractKey] = definition.Key
	}

	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Key < ordered[j].Key
	})

	return Catalog{
		definitions: ordered,
		byKey:       byKey,
	}, nil
}

// Len returns the number of definitions in the catalog.
func (c Catalog) Len() int {
	return len(c.definitions)
}

// Lookup returns the definition registered under key.
func (c Catalog) Lookup(key string) (EventDefinition, bool) {
	definition, ok := c.byKey[key]
	return definition, ok
}

// MustLookup returns the definition registered under key or an
// ErrUnknownEventDefinition-wrapped error.
func (c Catalog) MustLookup(key string) (EventDefinition, error) {
	definition, ok := c.Lookup(key)
	if !ok {
		return EventDefinition{}, fmt.Errorf("%w: %q", ErrUnknownEventDefinition, key)
	}

	return definition, nil
}

// Definitions returns a copy of the catalog definitions in deterministic key
// order.
func (c Catalog) Definitions() []EventDefinition {
	if len(c.definitions) == 0 {
		return []EventDefinition{}
	}

	definitions := make([]EventDefinition, len(c.definitions))
	copy(definitions, c.definitions)

	return definitions
}
