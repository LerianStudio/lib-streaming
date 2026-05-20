package contract

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

const (
	// StreamingOutboxEventType is the stable outbox event type used for all
	// streaming relay rows. The concrete destination lives in OutboxEnvelope.
	StreamingOutboxEventType = "lerian.streaming.publish"

	// OutboxEnvelopeVersion is the wire-version of the persisted
	// OutboxEnvelope. Validation uses strict equality (==
	// OutboxEnvelopeVersion) so unknown versions are rejected as
	// malformed.
	OutboxEnvelopeVersion = 1
)

// OutboxEnvelope is the persisted streaming outbox payload. One row is
// written per route whose policy permits outbox capture for the failure
// mode at hand (typically a circuit-open fallback). Replay handlers decode
// envelopes and dispatch directly to the named target adapter without
// going through Emit or the circuit breaker, preventing replay re-enqueue
// loops under sustained outage.
//
// Topic is intentionally NOT a field: the route's Destination already
// identifies where the message belongs, and a consumer that needs the
// derived Kafka topic for backwards-compatibility tooling can compute it
// from Event.Topic() — the runtime preserves Event identity per row.
type OutboxEnvelope struct {
	Version       int              `json:"version"`
	RouteKey      string           `json:"route_key"`
	DefinitionKey string           `json:"definition_key"`
	Target        string           `json:"target"`
	Transport     TransportKind    `json:"transport"`
	Destination   Destination      `json:"destination"`
	AggregateID   uuid.UUID        `json:"aggregate_id"`
	Requirement   RouteRequirement `json:"requirement"`
	Policy        DeliveryPolicy   `json:"policy"`
	Event         Event            `json:"event"`
}

// Validate enforces full OutboxEnvelope structural integrity. Use this
// on the replay-handler decode path where the persisted bytes are
// effectively untrusted (operator-edited rows, cross-process drift,
// schema-evolution skew). Combines cheap structural checks
// (ValidateShape) with the heavier Destination.Validate, which performs
// SSRF and DNS-pinning checks on URL-shaped destinations.
//
// Hot-path producers persisting an envelope they JUST built from a
// validated RouteDefinition + EmitRequest should call ValidateShape
// instead — the destination was already validated at NewRouteDefinition
// time and is immutable in RouteTable, so re-running SSRF on every
// outbox write would amount to a per-Emit DNS lookup with no security
// benefit.
func (e OutboxEnvelope) Validate() error {
	if err := e.ValidateShape(); err != nil {
		return err
	}

	return e.validateDestination()
}

// ValidateShape enforces OutboxEnvelope structural integrity WITHOUT
// re-running Destination URL/SSRF validation.
//
// Intended for trusted in-memory persist paths: the Emit hot path
// constructs OutboxEnvelope from a RouteDefinition that already passed
// NewRouteDefinition (which calls Destination.Validate with full
// SSRF/DNS checks) and is immutable in the RouteTable. Replay handlers
// that decode envelopes from persistent storage MUST use the full
// Validate so a row tampered with at rest cannot bypass SSRF.
//
// Checks performed:
//   - Strict version equality (rejects unknown versions).
//   - RouteKey: non-empty + canonical (lower-case dot-delimited).
//   - DefinitionKey: non-empty.
//   - Target: non-empty.
//   - Transport: known value.
//   - Destination.Kind matches Transport (cheap structural check, no URL parse).
//   - Requirement: known value after normalization.
//   - AggregateID: non-zero UUID.
//   - Policy: valid (mode bounds + cross-field rules).
//   - Event: structural shape — non-empty Topic-forming fields and tenant
//     discipline mirroring producer preFlight.
//
// Skipped vs Validate:
//   - Destination.Validate (URL parse, SSRF, DNS resolution).
func (e OutboxEnvelope) ValidateShape() error {
	if e.Version != OutboxEnvelopeVersion {
		// Schema-evolution canary. Version mismatch during a rolling
		// deploy is the load-bearing operator-actionable signal here.
		// Fire the trident with violation="version_mismatch" so dashboards
		// distinguish this from kind/transport mismatches; replace the
		// bare fmt.Errorf with the canonical ErrInvalidOutboxEnvelope
		// sentinel so callers can errors.Is consistently with every
		// other envelope failure.
		a := newContractAsserter("outbox_envelope.validate_shape")
		_ = a.That(context.Background(), false, "outbox envelope version must match library version",
			"violation", "version_mismatch",
			"got_version", e.Version,
			"want_version", OutboxEnvelopeVersion,
		)

		return fmt.Errorf("%w: unsupported outbox envelope version %d", ErrInvalidOutboxEnvelope, e.Version)
	}

	if e.RouteKey == "" {
		return fmt.Errorf("%w: route key required", ErrInvalidOutboxEnvelope)
	}

	if !isCanonicalRouteKey(e.RouteKey) {
		return fmt.Errorf("%w: route key must be lower-case dot-delimited", ErrInvalidOutboxEnvelope)
	}

	if e.DefinitionKey == "" {
		return fmt.Errorf("%w: definition key required", ErrInvalidOutboxEnvelope)
	}

	if e.Target == "" {
		return fmt.Errorf("%w: %w", ErrInvalidOutboxEnvelope, ErrMissingTarget)
	}

	if !isValidTransportKind(e.Transport) {
		return fmt.Errorf("%w: transport=%q", ErrInvalidOutboxEnvelope, e.Transport)
	}

	if e.Destination.Kind != e.Transport {
		// Last gate before outbox replay dispatches to a target adapter.
		// State-corruption guard for tampered/drifted persisted rows
		// — without the trident, dashboards cannot distinguish kind/
		// transport mismatch from version mismatch (both wrap
		// ErrInvalidOutboxEnvelope).
		a := newContractAsserter("outbox_envelope.validate_shape")
		_ = a.That(context.Background(), false, "outbox envelope destination kind must match transport",
			"violation", "kind_transport_mismatch",
			"route_key", e.RouteKey,
			"target", e.Target,
			"transport", string(e.Transport),
			"destination_kind", string(e.Destination.Kind),
		)

		return fmt.Errorf("%w: destination kind %q does not match envelope transport %q", ErrInvalidOutboxEnvelope, e.Destination.Kind, e.Transport)
	}

	requirement := normalizeRouteRequirement(e.Requirement)
	if !isValidRouteRequirement(requirement) {
		return fmt.Errorf("%w: requirement=%q", ErrInvalidOutboxEnvelope, e.Requirement)
	}

	if e.AggregateID == uuid.Nil {
		return fmt.Errorf("%w: aggregate_id required", ErrInvalidOutboxEnvelope)
	}

	if err := e.Policy.Validate(); err != nil {
		return err
	}

	if err := validateOutboxEventShape(e.Event); err != nil {
		return fmt.Errorf("%w: event: %w", ErrInvalidOutboxEnvelope, err)
	}

	return nil
}

// validateDestination runs the heavier Destination.Validate path that
// performs URL parsing, scheme/host checks, and SSRF + DNS resolution
// for SQS queue URLs. Extracted so ValidateShape can opt out for trusted
// persist paths while Validate keeps the full untrusted-decode contract.
func (e OutboxEnvelope) validateDestination() error {
	if err := e.Destination.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidOutboxEnvelope, err)
	}

	return nil
}

// validateOutboxEventShape enforces the cheap structural Event invariants
// that producer preFlight enforces at Emit time. Re-checking them at
// envelope-validation time means a structurally-empty Event that lacks
// topic-forming fields or tenant discipline is rejected at persist time
// instead of failing at replay-preflight time, where the row would
// already be in the outbox and the operator-facing error would point at
// the relay rather than the originating Emit.
//
// The check intentionally MIRRORS Producer.preFlightWithPayload's first
// gates (resource/event/tenant/source) but stays in the contract package
// so it can be invoked without a Producer instance — replay handlers and
// integration tests both need it.
//
// Payload JSON validity is NOT re-checked here: it lives on the Emit hot
// path (where the caller's bytes are first seen) and again on the replay
// preflight (where Producer.preFlightWithPayload runs after decode).
func validateOutboxEventShape(event Event) error {
	if event.ResourceType == "" {
		return ErrMissingResourceType
	}

	if event.EventType == "" {
		return ErrMissingEventType
	}

	// Tenant discipline mirrors preFlight: SystemEvent opts out.
	if !event.SystemEvent && event.TenantID == "" {
		return ErrMissingTenantID
	}

	if event.Source == "" {
		return ErrMissingSource
	}

	return nil
}
