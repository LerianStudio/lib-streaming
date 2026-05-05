package producer

import (
	"encoding/json"
)

// preFlightWithPayload runs all caller-side validation on an Event before
// it reaches the transport. Defaults must already be applied by the
// caller — preFlight is pure validation, never mutation.
//
// Order of checks is tuned so the cheapest / most-common caller mistake
// surfaces first:
//
//  1. SystemEvent capability gate: reject before any other work if the
//     Producer did not opt into system events.
//  2. Topic-forming fields: resource and event type must be populated.
//  3. Tenant: non-system events require a tenant.
//  4. Source: required CloudEvents ce-source.
//  5. Header sanitization: TenantID / ResourceType / EventType / Source /
//     Subject must be header-safe (no control chars, bounded length).
//  6. Payload size: reject before JSON scan.
//  7. Payload JSON validity: prevents DLQ poisoning downstream.
//
// Returns one of the caller sentinel errors (ErrSystemEventsNotAllowed,
// ErrMissingResourceType, ErrMissingEventType, ErrMissingTenantID,
// ErrMissingSource, ErrInvalid*, ErrPayloadTooLarge, ErrNotJSON).
func (p *Producer) preFlightWithPayload(event Event, validatePayload bool) error {
	// SystemEvent capability gate — runs FIRST so an opt-in violation is
	// rejected before any other validation can mask it. A service that
	// sets SystemEvent=true without WithAllowSystemEvents would otherwise
	// hijack the "system:*" partition space, so the rejection must be
	// unambiguous and fire even if other fields are also malformed.
	if event.SystemEvent && !p.allowSystemEvents {
		return ErrSystemEventsNotAllowed
	}

	// Topic-forming fields MUST be populated. An empty ResourceType or
	// EventType would produce a degenerate Kafka topic ("lerian.streaming..")
	// and a malformed ce-type header ("studio.lerian.."). No downstream
	// consumer routes those, so the event would be silently lost at the
	// worst possible layer.
	if event.ResourceType == "" {
		return ErrMissingResourceType
	}

	if event.EventType == "" {
		return ErrMissingEventType
	}

	// Tenant discipline. System events (`ce-systemevent: true`) opt out of
	// the requirement — they're ops-level fan-out that carries no per-tenant
	// payload.
	if !event.SystemEvent && event.TenantID == "" {
		return ErrMissingTenantID
	}

	// ce-source is a required CloudEvents attribute. Empty source is a
	// caller config bug (usually: forgot to set Config.CloudEventsSource).
	if event.Source == "" {
		return ErrMissingSource
	}

	// Header sanitization. Every field that travels as a CloudEvents
	// context attribute (header) must be free of control characters and
	// within a bounded length. Bypassing these checks would let malicious
	// or buggy callers corrupt downstream log streams (CRLF injection
	// into structured logs) or break OTEL label pipelines.
	if err := p.validateHeaderSafeFields(event); err != nil {
		return err
	}

	// Pre-flight size cap. 1 MiB is the Redpanda default; we check BEFORE
	// JSON validity so the dominant caller mistake (huge payload) short-
	// circuits the slightly more expensive json.Valid scan.
	if validatePayload {
		if len(event.Payload) > maxPayloadBytes {
			return ErrPayloadTooLarge
		}

		// Payload must parse as JSON. This is the line of defense that keeps
		// malformed bytes out of consumers and prevents DLQ replay from
		// repeatedly re-poisoning the same topic. An empty payload is
		// permitted ONLY when it's valid JSON (e.g. `null`, `{}`); a genuinely
		// empty byte slice fails json.Valid and surfaces ErrNotJSON.
		if !json.Valid(event.Payload) {
			return ErrNotJSON
		}
	}

	return nil
}

// validateHeaderSafeFields checks every CloudEvents attribute that travels
// as a Kafka header for control characters and length overruns. Returns
// the first offending sentinel. Empty values are NOT checked here —
// required-vs-optional semantics live in preFlight (tenant, source).
//
// Uses the canonical contract.HeaderFieldCheck shape (re-exported via
// aliases.go as headerFieldCheck) so the producer-side check table cannot
// drift from the contract-side equivalent.
func (*Producer) validateHeaderSafeFields(event Event) error {
	checks := [...]headerFieldCheck{
		{Value: event.TenantID, MaxBytes: maxTenantIDBytes, Sentinel: ErrInvalidTenantID},
		{Value: event.ResourceType, MaxBytes: maxResourceTypeBytes, Sentinel: ErrInvalidResourceType},
		{Value: event.EventType, MaxBytes: maxEventTypeBytes, Sentinel: ErrInvalidEventType},
		{Value: event.Source, MaxBytes: maxSourceBytes, Sentinel: ErrInvalidSource},
		{Value: event.Subject, MaxBytes: maxSubjectBytes, Sentinel: ErrInvalidSubject},
		{Value: event.EventID, MaxBytes: maxEventIDBytes, Sentinel: ErrInvalidEventID},
		{Value: event.SchemaVersion, MaxBytes: maxSchemaVersionBytes, Sentinel: ErrInvalidSchemaVersion},
		{Value: event.DataContentType, MaxBytes: maxDataContentTypeBytes, Sentinel: ErrInvalidDataContentType},
		{Value: event.DataSchema, MaxBytes: maxDataSchemaBytes, Sentinel: ErrInvalidDataSchema},
	}

	for _, c := range checks {
		if len(c.Value) > c.MaxBytes || hasControlChar(c.Value) {
			return c.Sentinel
		}
	}

	return nil
}
