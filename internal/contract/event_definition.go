package contract

import (
	"context"
	"fmt"
	"strings"
)

// EventClass says whether a definition is a business FACT or a
// service-to-service COMMAND. It selects the queue the event rides and,
// through that, the consumer's verdict on an unmatched key.
//
// It is NOT on the wire. No ce-* header carries it, and the record shape is
// byte-identical either way: the QUEUE is the class. Putting it in a header
// would have made the classification a runtime string every consumer has to
// trust and branch on; putting it in the topic name makes it an ACL-visible,
// subscription-time fact.
type EventClass string

const (
	// ClassFact is a business fact: something that HAPPENED, published for
	// whoever cares. It rides AppTopic and an unmatched key on it is
	// ignored — a consumer subscribed to a producer's fact stream receives
	// every fact that producer emits and legitimately handles a handful.
	//
	// It is the DEFAULT: an EventDefinition with an empty Class normalizes
	// to ClassFact at construction, so a catalog written before commands
	// existed keeps its exact meaning.
	ClassFact EventClass = "fact"

	// ClassCommand is a service-to-service command: work one named service
	// is asking another to do. It rides AppCommandsTopic, and an unmatched
	// key there is QUARANTINED, never skipped.
	//
	// That asymmetry is the whole feature. On the consignado rail, lender's
	// commands travel to br-consignado-gw mixed with lender's facts; a new
	// command key published before the gateway deploys its handler would be
	// ignored-and-committed forever under fact semantics — money-path loss
	// with green dashboards.
	ClassCommand EventClass = "command"
)

// Valid reports whether c is one of the two defined classes. The empty
// string is NOT valid here — NewEventDefinition normalizes it to ClassFact
// before this runs, so an empty value reaching Valid means it bypassed
// construction.
func (c EventClass) Valid() bool {
	return c == ClassFact || c == ClassCommand
}

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
	// Class selects the queue this definition publishes to: ClassFact (the
	// zero-value default) rides the app topic, ClassCommand rides the app's
	// ".commands" topic. See EventClass.
	Class EventClass
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

	// EventKey ambiguity gate: "." is the composition separator in
	// "<resourceType>.<eventType>", so a dot INSIDE either component lets two
	// distinct valid pairs collide on one dispatch key — ("payment.refund",
	// "created") and ("payment", "refund.created") both compose
	// "payment.refund.created". The catalog would then reject a legitimate
	// definition as a duplicate, or a consumer could never register distinct
	// handlers for the two. ResourceTypes are snake_case in this fleet;
	// neither component has a legitimate dotted shape.
	if strings.Contains(definition.ResourceType, ".") {
		a := newContractAsserter("event_definition.new")
		_ = a.That(context.Background(), false, "event definition ResourceType must not contain '.'",
			"violation", "dotted_resource_type",
			"key", definition.Key,
			"resource_type", definition.ResourceType,
		)

		return EventDefinition{}, fmt.Errorf("%w: %w: ResourceType %q must not contain '.'",
			ErrInvalidEventDefinition, ErrInvalidResourceType, definition.ResourceType)
	}

	if strings.Contains(definition.EventType, ".") {
		a := newContractAsserter("event_definition.new")
		_ = a.That(context.Background(), false, "event definition EventType must not contain '.'",
			"violation", "dotted_event_type",
			"key", definition.Key,
			"resource_type", definition.ResourceType,
			"event_type", definition.EventType,
		)

		return EventDefinition{}, fmt.Errorf("%w: %w: EventType %q must not contain '.'",
			ErrInvalidEventDefinition, ErrInvalidEventType, definition.EventType)
	}

	if definition.SchemaVersion == "" {
		definition.SchemaVersion = defaultSchemaVersion
	}

	if definition.DataContentType == "" {
		definition.DataContentType = defaultDataContentType
	}

	// Class normalization + gate. Empty means fact, which keeps every
	// catalog written before commands existed meaning exactly what it did.
	// Anything else is a typo that would otherwise route to the fact topic
	// silently — the failure mode a command class exists to prevent.
	//
	// Asserter trident fires under operation="event_definition.class" with
	// violation="invalid_class" so dashboards distinguish it from the
	// missing-required-field branches above.
	if definition.Class == "" {
		definition.Class = ClassFact
	}

	if !definition.Class.Valid() {
		a := newContractAsserter("event_definition.class")
		_ = a.That(context.Background(), false, "event definition Class must be fact or command",
			"violation", "invalid_class",
			"key", definition.Key,
			"resource_type", definition.ResourceType,
			"event_type", definition.EventType,
			"class", string(definition.Class),
		)

		return EventDefinition{}, fmt.Errorf("%w: class %q must be %q or %q",
			ErrInvalidEventDefinition, definition.Class, ClassFact, ClassCommand)
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

// EventKey composes the "<resourceType>.<eventType>" dispatch key from its two
// parts. It is the SINGLE owner of that formula: the producer's catalog spells
// the key with it, the manifest advertises the result, and the consumer's
// dispatcher recomposes it from the ce-resourcetype / ce-eventtype headers to
// look up a handler. Two independent concatenations of the same shape is one
// separator change away from a consumer that silently matches nothing.
//
// The composition is unambiguous because NewEventDefinition rejects a "."
// inside either component: without that gate, ("payment.refund", "created")
// and ("payment", "refund.created") would compose the same key.
func EventKey(resourceType, eventType string) string {
	return resourceType + "." + eventType
}

// EventKey is the dispatch key a consumer registers a handler under. It is the
// routing unit that replaced the per-definition topic: under one-topic-per-app,
// a consumer receives the producer's whole stream on one subscription and
// selects by this key, which it reads from the ce-resourcetype /
// ce-eventtype headers.
//
// There is deliberately no EventDefinition.Topic in v3. A definition has no
// topic of its own — the producing APPLICATION has exactly one (AppTopic),
// and it is a property of the producer's ce-source, not of any catalog entry.
func (d EventDefinition) EventKey() string {
	return EventKey(d.ResourceType, d.EventType)
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
