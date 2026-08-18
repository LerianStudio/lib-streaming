package producer

import (
	"context"
	"encoding/json"
	"mime"
	"strings"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
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
//  3. Source: required CloudEvents ce-source, and must not fold to an empty
//     topic-segment under sanitization.
//  4. Header sanitization: TenantID / ResourceType / EventType / Source /
//     Subject must be header-safe (no control chars, bounded length).
//  5. Derived topic length: reject names Kafka cannot accept.
//  6. Payload size: reject before JSON scan.
//  7. Payload JSON validity: prevents DLQ poisoning downstream.
//
// TenantID is NOT required: an empty TenantID is a valid single-tenant scope
// for business events.
//
// Returns one of the caller sentinel errors (ErrSystemEventsNotAllowed,
// ErrMissingResourceType, ErrMissingEventType, ErrMissingSource,
// ErrInvalid*, ErrPayloadTooLarge, ErrNotJSON).
//
// ctx is used by the asserter trident on payload-cap rejection so a caller
// bug surfaces on dashboards under operation="preflight.payload_size".
// Public Producer.Emit signatures pass their own ctx; outbox replay
// passes the dispatcher's ctx; benchmark/property tests pass
// context.Background() at construction.
func (p *Producer) preFlightWithPayload(ctx context.Context, event Event, validatePayload bool) error {
	// SystemEvent capability gate — runs FIRST so an opt-in violation is
	// rejected before any other validation can mask it. A service that
	// sets SystemEvent=true without WithAllowSystemEvents would otherwise
	// hijack the "system:*" partition space, so the rejection must be
	// unambiguous and fire even if other fields are also malformed.
	if event.SystemEvent && !p.allowSystemEvents {
		return ErrSystemEventsNotAllowed
	}

	// ResourceType and EventType MUST be populated. They no longer form the
	// topic (one topic per app), but they ARE the ce-type suffix and the
	// ce-resourcetype / ce-eventtype headers a consumer dispatches on. An
	// empty one yields a malformed ce-type ("studio.lerian.<src>..") and an
	// event no consumer's dispatch table can match — silently lost at the
	// worst possible layer.
	if event.ResourceType == "" {
		return ErrMissingResourceType
	}

	if event.EventType == "" {
		return ErrMissingEventType
	}

	// TenantID is intentionally NOT required. An empty TenantID denotes a
	// single-tenant deployment and is a first-class, always-valid scope for
	// business events. Single-tenant and multi-tenant run on physically
	// segregated infrastructure (dedicated vs shared DB), so a multi-tenant
	// service that lost its tenant fails at the database-routing layer long
	// before emitting — a streaming-level tenant guard would be redundant and
	// would only block legitimate single-tenant emits.

	// ce-source is a required CloudEvents attribute AND the sole input to
	// this application's topic, so it is validated strictly: a single
	// dot-free lowercase segment, short enough that the derived topic and
	// its ".dlq" fit Kafka's 249-byte name limit.
	//
	// v3 REJECTS a malformed source rather than rewriting it (v2 folded it
	// through a lossy sanitizer, which could silently merge two services
	// onto one topic namespace and ACL scope). ErrMissingSource for empty,
	// ErrInvalidSource for malformed — both caller-correctable, both
	// synchronous, so the config bug never reaches a broker.
	if err := contract.ValidateSource(event.Source); err != nil {
		return err
	}

	// Header sanitization. Every field that travels as a CloudEvents
	// context attribute (header) must be free of control characters and
	// within a bounded length. Bypassing these checks would let malicious
	// or buggy callers corrupt downstream log streams (CRLF injection
	// into structured logs) or break OTEL label pipelines.
	if err := p.validateHeaderSafeFields(event); err != nil {
		return err
	}

	// No separate derived-topic length check: the topic is
	// "lerian.streaming." + Source and nothing else, and ValidateSource
	// above already bounds Source so that the topic AND its ".dlq" fit
	// Kafka's limit. In v2 the name also absorbed the resource type, event
	// type, and a version suffix, so the components could each pass their
	// own header caps while the concatenation overflowed — that whole
	// failure mode is gone with the topic collapse.

	// Pre-flight size cap. 1 MiB is the Redpanda default; we check BEFORE
	// JSON validity so the dominant caller mistake (huge payload) short-
	// circuits the slightly more expensive json.Valid scan.
	if validatePayload {
		if len(event.Payload) > maxPayloadBytes {
			// Caller bug — payload exceeds the single-target 1 MiB cap.
			// Pair the trident with the multi-target site at
			// emit_multi.payload_size so dashboards see a symmetric signal
			// across single vs multi callers.
			a := p.newAsserter("preflight.payload_size")
			_ = a.That(ctx, false, "payload size must not exceed single-target cap",
				"payload_bytes", len(event.Payload),
				"max_bytes", maxPayloadBytes,
				"resource_type", event.ResourceType,
				"event_type", event.EventType,
			)

			return ErrPayloadTooLarge
		}

		// Payload must parse as JSON — but only for JSON content types. This
		// is the line of defense that keeps malformed bytes out of consumers
		// and prevents DLQ replay from repeatedly re-poisoning the same topic.
		// An empty payload is permitted ONLY when it's valid JSON (e.g. `null`,
		// `{}`); a genuinely empty byte slice fails json.Valid and surfaces
		// ErrNotJSON. A non-JSON content type (e.g. application/xml) ships its
		// payload verbatim as the record value and skips the scan; the size
		// cap above still protects Kafka's max.message.bytes.
		if isJSONContentType(event.DataContentType) && !json.Valid(event.Payload) {
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

// isJSONContentType reports whether a CloudEvents DataContentType denotes a
// JSON payload that must pass json.Valid. The recognition is media-type aware:
// parameters are stripped (application/json; charset=utf-8) and the RFC 6839
// structured "+json" suffix is honored (application/cloudevents+json,
// application/hal+json). An empty value means the CloudEvents default
// (application/json). A non-JSON media type (e.g. application/xml) is opaque:
// the payload ships verbatim and the json.Valid scan is skipped.
//
// A parse error with no recoverable media type fails CLOSED: an unrecognizable
// content type re-enters the json.Valid gate rather than silently skipping it.
// If mime.ParseMediaType recovers the base media type but rejects malformed
// parameters, classify from that base type so an opaque payload is not
// incorrectly forced through json.Valid.
func isJSONContentType(ct string) bool {
	if ct == "" {
		return true
	}

	mt, _, err := mime.ParseMediaType(ct)
	if err != nil && mt == "" {
		return true
	}

	return mt == "application/json" || strings.HasSuffix(mt, "+json")
}
