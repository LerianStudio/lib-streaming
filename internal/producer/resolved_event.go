package producer

// resolvedEvent is the internal output of resolving an EmitRequest against the
// producer catalog and policy overrides.
//
// Carries only the fields the Emit hot path actually reads. DefinitionKey
// (a string) is kept because emit.go threads it through span attributes
// and outbox envelopes; the full EmitRequest and EventDefinition are
// intentionally NOT stored — copying them per Emit was dead work.
type resolvedEvent struct {
	DefinitionKey string
	Event         Event
	Topic         string
	Policy        DeliveryPolicy
}

func (p *Producer) resolveEventAllowDisabled(request EmitRequest) (resolvedEvent, error) {
	return p.resolveEventWithPolicy(request, false)
}

func (p *Producer) resolveEventWithPolicy(request EmitRequest, rejectDisabled bool) (resolvedEvent, error) {
	if p == nil {
		return resolvedEvent{}, ErrNilProducer
	}

	request, err := newEmitRequest(request, false)
	if err != nil {
		return resolvedEvent{}, err
	}

	definition, err := p.catalog.Require(request.DefinitionKey)
	if err != nil {
		return resolvedEvent{}, err
	}

	policy, err := ResolveDeliveryPolicy(
		definition,
		p.policyOverrideFor(request.DefinitionKey),
		request.PolicyOverride,
	)
	if err != nil {
		return resolvedEvent{}, err
	}

	if rejectDisabled && !policy.Enabled {
		return resolvedEvent{}, ErrEventDisabled
	}

	event := Event{
		TenantID:        request.TenantID,
		ResourceType:    definition.ResourceType,
		EventType:       definition.EventType,
		EventID:         request.EventID,
		SchemaVersion:   definition.SchemaVersion,
		Timestamp:       request.Timestamp,
		Source:          p.cloudEventsSource,
		Subject:         request.Subject,
		DataContentType: definition.DataContentType,
		DataSchema:      definition.DataSchema,
		SystemEvent:     definition.SystemEvent,
		Payload:         request.Payload,
	}
	// ApplyDefaults fills Timestamp from time.Now().UTC() when zero, along
	// with EventID / SchemaVersion / DataContentType. No pre-fill needed.
	(&event).ApplyDefaults()

	// TenantID is intentionally NOT required here. An empty TenantID denotes a
	// single-tenant deployment and is a first-class, always-valid scope for
	// business events: single-tenant and multi-tenant run on physically
	// segregated infrastructure (dedicated vs shared DB), so a multi-tenant
	// service that lost its tenant fails at the database-routing layer long
	// before it could emit — a streaming-level tenant guard would be redundant
	// and would only block legitimate single-tenant emits.

	if event.Source == "" {
		return resolvedEvent{}, ErrMissingSource
	}

	// Event.Topic() returns "" only on a nil receiver; here we operate on
	// a value-type Event that already passed tenant/source validation, so
	// the empty-topic case is unreachable. (Previously a defensive guard
	// lived here — Wave 2 confirmed it was dead code.)
	topic := event.Topic()

	return resolvedEvent{
		DefinitionKey: request.DefinitionKey,
		Event:         event,
		Topic:         topic,
		Policy:        policy,
	}, nil
}

func (p *Producer) policyOverrideFor(key string) DeliveryPolicyOverride {
	if p == nil || p.policyOverrides == nil {
		return DeliveryPolicyOverride{}
	}

	return p.policyOverrides[key]
}
