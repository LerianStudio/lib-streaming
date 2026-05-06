package contract

import (
	"context"
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
//
// Asserter trident fires under operation="catalog.new" with structured
// field violation={"duplicate_key"|"duplicate_contract"} on rejection so
// operators can distinguish the two failure modes from
// assertion_failed_total without parsing wrapped sentinel strings.
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
			a := newContractAsserter("catalog.new")
			_ = a.That(context.Background(), false, "catalog must not contain duplicate Key entries",
				"violation", "duplicate_key",
				"key", definition.Key,
			)

			return Catalog{}, fmt.Errorf("%w: key %q", ErrDuplicateEventDefinition, definition.Key)
		}

		contractKey := definition.ResourceType + "." + definition.EventType + "." + definition.SchemaVersion
		if existingKey, exists := byContract[contractKey]; exists {
			a := newContractAsserter("catalog.new")
			_ = a.That(context.Background(), false, "catalog must not contain duplicate (ResourceType, EventType, SchemaVersion) tuples",
				"violation", "duplicate_contract",
				"existing_key", existingKey,
				"duplicate_key", definition.Key,
				"contract_tuple", contractKey,
			)

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

// Require returns the definition registered under key or an
// ErrUnknownEventDefinition-wrapped error. Unlike the Go stdlib's Must* family
// (regexp.MustCompile, template.Must), Require does NOT panic — it returns an
// error. The name Require makes the intent explicit: "require this key, error
// if missing" without the misleading Must convention.
func (c Catalog) Require(key string) (EventDefinition, error) {
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
