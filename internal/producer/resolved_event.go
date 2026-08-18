package producer

import "encoding/json"

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

	// JSON-payload validity is content-type-aware and therefore runs AFTER the
	// catalog lookup, once the definition's DataContentType is known. A JSON
	// content type (or the empty default) must pass json.Valid to keep
	// malformed bytes out of consumers and prevent DLQ re-poisoning; a non-JSON
	// content type (e.g. application/xml for an ISO-8859-1 SFN message) ships
	// its payload as an opaque blob and skips the scan. The size cap already
	// fired in newEmitRequest, content-type-agnostic.
	if isJSONContentType(definition.DataContentType) && !json.Valid(request.Payload) {
		return resolvedEvent{}, ErrNotJSON
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
	// A system event is platform-level, not tenant-scoped: the contract says
	// it omits ce-tenantid entirely. The header builder emits ce-tenantid
	// whenever TenantID is non-empty, so a caller passing a tenant on a system
	// definition would have shipped one — and a consumer filtering on
	// ce-tenantid would have routed a platform event into one tenant's
	// processing. Drop it here, at the single place the wire event is built.
	if event.SystemEvent {
		event.TenantID = ""
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

	// Validate-before-derive is satisfied by CONSTRUCTION, not per Emit:
	// event.Source is p.cloudEventsSource, which NewProducerMulti validated
	// with ValidateSource and which is immutable thereafter. Re-running the
	// regex here would pay a per-Emit cost to re-prove a construction-time
	// fact about a constant.
	//
	// The outbox REPLAY path is different — its Event.Source comes from
	// persisted bytes, not from this Producer — and is validated on every
	// replay by preFlightWithPayload, which is the gate that path goes
	// through.
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
