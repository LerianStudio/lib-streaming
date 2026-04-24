package streaming

import (
	"fmt"
	"time"
)

// resolvedEvent is the internal output of resolving an EmitRequest against the
// producer catalog and policy overrides.
type resolvedEvent struct {
	Definition EventDefinition
	Request    EmitRequest
	Event      Event
	Topic      string
	Policy     DeliveryPolicy
}

func (p *Producer) resolveEvent(request EmitRequest) (resolvedEvent, error) {
	if p == nil {
		return resolvedEvent{}, ErrNilProducer
	}

	request, err := newEmitRequest(request, false)
	if err != nil {
		return resolvedEvent{}, err
	}

	definition, err := p.catalog.MustLookup(request.DefinitionKey)
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

	if !policy.Enabled {
		return resolvedEvent{}, ErrEventDisabled
	}

	event := Event{
		TenantID:        request.TenantID,
		ResourceType:    definition.ResourceType,
		EventType:       definition.EventType,
		EventID:         request.EventID,
		SchemaVersion:   definition.SchemaVersion,
		Timestamp:       request.Timestamp,
		Source:          p.cfg.CloudEventsSource,
		Subject:         request.Subject,
		DataContentType: definition.DataContentType,
		DataSchema:      definition.DataSchema,
		SystemEvent:     definition.SystemEvent,
		Payload:         request.Payload,
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	(&event).ApplyDefaults()

	if !event.SystemEvent && event.TenantID == "" {
		return resolvedEvent{}, ErrMissingTenantID
	}

	if event.Source == "" {
		return resolvedEvent{}, ErrMissingSource
	}

	topic := event.Topic()
	if topic == "" {
		return resolvedEvent{}, fmt.Errorf("%w: empty topic", ErrInvalidEventDefinition)
	}

	return resolvedEvent{
		Definition: definition,
		Request:    request,
		Event:      event,
		Topic:      topic,
		Policy:     policy,
	}, nil
}

func (p *Producer) policyOverrideFor(key string) DeliveryPolicyOverride {
	if p == nil || p.policyOverrides == nil {
		return DeliveryPolicyOverride{}
	}

	return p.policyOverrides[key]
}
