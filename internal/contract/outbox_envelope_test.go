//go:build unit

package contract

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

// validOutboxEnvelope returns a fully-populated, valid envelope used
// as the base for table-driven mutate-and-fail tests.
func validOutboxEnvelope(t *testing.T) OutboxEnvelope {
	t.Helper()

	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatalf("uuid.NewRandom() error = %v", err)
	}

	return OutboxEnvelope{
		Version:       OutboxEnvelopeVersion,
		RouteKey:      "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Transport:     TransportKafkaLike,
		Destination:   Destination{Kind: TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		AggregateID:   id,
		Requirement:   RouteRequired,
		Policy:        DefaultDeliveryPolicy(),
		Event: Event{
			TenantID:        "tenant-1",
			ResourceType:    "transaction",
			EventType:       "created",
			Source:          "//svc/test",
			SchemaVersion:   "1.0.0",
			DataContentType: "application/json",
			EventID:         "01939c11-1d49-7abc-bd3f-1fa8cafe1234",
			Payload:         json.RawMessage(`{}`),
		},
	}
}

func TestOutboxEnvelope_Validate_HappyPath(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v; want nil", err)
	}
}

func TestOutboxEnvelope_Validate_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*OutboxEnvelope)
		wantSub error // expected sentinel; nil means just non-nil error.
	}{
		{
			name:    "version zero",
			mutate:  func(e *OutboxEnvelope) { e.Version = 0 },
			wantSub: ErrInvalidOutboxEnvelope, // T7 wraps ErrInvalidOutboxEnvelope; previously a bare fmt.Errorf.
		},
		{
			name:    "version two (future)",
			mutate:  func(e *OutboxEnvelope) { e.Version = 2 },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "version three (future)",
			mutate:  func(e *OutboxEnvelope) { e.Version = 3 },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "empty route key",
			mutate:  func(e *OutboxEnvelope) { e.RouteKey = "" },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "non-canonical route key",
			mutate:  func(e *OutboxEnvelope) { e.RouteKey = "Not-Canonical" },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "route key with control char",
			mutate:  func(e *OutboxEnvelope) { e.RouteKey = "transaction\n.created" },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "empty definition key",
			mutate:  func(e *OutboxEnvelope) { e.DefinitionKey = "" },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "empty target",
			mutate:  func(e *OutboxEnvelope) { e.Target = "" },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name:    "invalid transport kind",
			mutate:  func(e *OutboxEnvelope) { e.Transport = TransportKind("ftp") },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name: "transport mismatches destination kind",
			mutate: func(e *OutboxEnvelope) {
				// Envelope claims SQS but destination is Kafka.
				e.Transport = TransportSQS
			},
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name: "destination validation failure propagates",
			mutate: func(e *OutboxEnvelope) {
				// Kafka requires Name; emptying it triggers ErrInvalidDestination.
				e.Destination = Destination{Kind: TransportKafkaLike}
			},
			wantSub: ErrInvalidDestination,
		},
		{
			name:    "zero aggregate id",
			mutate:  func(e *OutboxEnvelope) { e.AggregateID = uuid.Nil },
			wantSub: ErrInvalidOutboxEnvelope,
		},
		{
			name: "invalid policy mode",
			mutate: func(e *OutboxEnvelope) {
				e.Policy.Direct = DirectMode("async")
			},
			wantSub: ErrInvalidDeliveryPolicy,
		},
		{
			name: "invalid policy cross-field rule",
			mutate: func(e *OutboxEnvelope) {
				// Direct=skip requires Outbox=always
				e.Policy.Direct = DirectModeSkip
				e.Policy.Outbox = OutboxModeNever
			},
			wantSub: ErrInvalidDeliveryPolicy,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			envelope := validOutboxEnvelope(t)
			tt.mutate(&envelope)

			err := envelope.Validate()
			if err == nil {
				t.Fatalf("Validate() error = nil; want non-nil for %q", tt.name)
			}

			if tt.wantSub != nil && !errors.Is(err, tt.wantSub) {
				t.Errorf("Validate() error = %v; want errors.Is(..., %v)", err, tt.wantSub)
			}
		})
	}
}

// TestOutboxEnvelope_Validate_NormalizesEmptyRequirement pins the
// requirement-normalization shim: an empty RouteRequirement defaults to
// RouteRequired (mirroring NewRouteDefinition), so persisted envelopes
// that predate the requirement field still validate.
func TestOutboxEnvelope_Validate_NormalizesEmptyRequirement(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	envelope.Requirement = ""

	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v; want nil for empty requirement (defaults to required)", err)
	}
}

// TestOutboxEnvelope_Validate_RejectsInvalidRequirement pins that an
// unknown requirement value (after normalization) is rejected.
func TestOutboxEnvelope_Validate_RejectsInvalidRequirement(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	envelope.Requirement = "sometimes"

	err := envelope.Validate()
	if !errors.Is(err, ErrInvalidOutboxEnvelope) {
		t.Fatalf("Validate() error = %v; want ErrInvalidOutboxEnvelope", err)
	}
}

// TestOutboxEnvelope_ValidateRejectsEmptyEventShape pins the v2 gap
// closed by validateOutboxEventShape: a structurally-empty Event that
// lacks topic-forming fields or tenant discipline must be rejected at
// envelope-validation time, not at replay-preflight time.
func TestOutboxEnvelope_ValidateRejectsEmptyEventShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Event)
		want   error
	}{
		{
			name:   "empty resource type",
			mutate: func(e *Event) { e.ResourceType = "" },
			want:   ErrMissingResourceType,
		},
		{
			name:   "empty event type",
			mutate: func(e *Event) { e.EventType = "" },
			want:   ErrMissingEventType,
		},
		{
			name: "empty tenant on non-system event",
			mutate: func(e *Event) {
				e.TenantID = ""
				e.SystemEvent = false
			},
			want: ErrMissingTenantID,
		},
		{
			name:   "empty source",
			mutate: func(e *Event) { e.Source = "" },
			want:   ErrMissingSource,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			envelope := validOutboxEnvelope(t)
			tt.mutate(&envelope.Event)

			err := envelope.Validate()
			if !errors.Is(err, ErrInvalidOutboxEnvelope) {
				t.Fatalf("Validate() error = %v; want errors.Is(..., ErrInvalidOutboxEnvelope)", err)
			}

			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}

// TestOutboxEnvelope_ValidateAllowsSystemEventWithoutTenant pins
// the system-event opt-out: a SystemEvent with no TenantID must NOT be
// rejected by validateOutboxEventShape (mirrors producer preFlight).
func TestOutboxEnvelope_ValidateAllowsSystemEventWithoutTenant(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	envelope.Event.TenantID = ""
	envelope.Event.SystemEvent = true

	if err := envelope.Validate(); err != nil {
		t.Fatalf("Validate() error = %v; want nil for system event without tenant", err)
	}
}

// TestOutboxEnvelope_ValidateShape_SkipsDestinationSSRF pins that
// ValidateShape does NOT re-run Destination.Validate (which performs
// SSRF / DNS resolution). Use case: the multi-target Emit hot path
// persists an envelope built from a RouteDefinition that already passed
// NewRouteDefinition; re-validating the destination on every persist
// would amount to a per-Emit DNS lookup with no security benefit.
//
// The test forges a destination that would FAIL the full Validate()
// (SQS without https scheme) but whose other shape checks pass, and
// asserts ValidateShape returns nil while Validate returns an error.
func TestOutboxEnvelope_ValidateShape_SkipsDestinationSSRF(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	// Switch to SQS with a destination that would fail Validate's
	// queue-URL/SSRF check (file:// scheme is blocked) but is otherwise
	// shape-correct.
	envelope.Transport = TransportSQS
	envelope.Destination = Destination{
		Kind:    TransportSQS,
		Address: "file:///etc/passwd",
	}

	if err := envelope.ValidateShape(); err != nil {
		t.Fatalf("ValidateShape() error = %v; want nil (SSRF skipped on shape path)", err)
	}

	if err := envelope.Validate(); err == nil {
		t.Fatalf("Validate() error = nil; want non-nil (SSRF must reject file:// scheme)")
	}
}

// TestOutboxEnvelope_ValidateShape_StillEnforcesEventShape pins
// that ValidateShape, even though it skips destination SSRF, STILL
// runs validateOutboxEventShape. Otherwise a hot-path persist could
// silently store a structurally-empty Event that would fail at replay
// preflight.
func TestOutboxEnvelope_ValidateShape_StillEnforcesEventShape(t *testing.T) {
	t.Parallel()

	envelope := validOutboxEnvelope(t)
	envelope.Event.ResourceType = ""

	err := envelope.ValidateShape()
	if !errors.Is(err, ErrMissingResourceType) {
		t.Fatalf("ValidateShape() error = %v; want errors.Is(..., ErrMissingResourceType)", err)
	}
}

// TestOutboxEnvelope_JSONRoundTrip verifies the JSON marshal/unmarshal
// preserves every field, including pointer-shaped Policy.Enabled, so a
// persisted envelope drained later validates and dispatches identically.
func TestOutboxEnvelope_JSONRoundTrip(t *testing.T) {
	t.Parallel()

	enabled := true
	original := validOutboxEnvelope(t)
	original.Policy.Enabled = enabled // bool, not pointer — keep validation simple
	original.Destination.Attributes = map[string]string{"format": "json"}

	body, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	var decoded OutboxEnvelope
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Version != original.Version {
		t.Errorf("Version = %d; want %d", decoded.Version, original.Version)
	}
	if decoded.RouteKey != original.RouteKey {
		t.Errorf("RouteKey = %q; want %q", decoded.RouteKey, original.RouteKey)
	}
	if decoded.DefinitionKey != original.DefinitionKey {
		t.Errorf("DefinitionKey = %q; want %q", decoded.DefinitionKey, original.DefinitionKey)
	}
	if decoded.Target != original.Target {
		t.Errorf("Target = %q; want %q", decoded.Target, original.Target)
	}
	if decoded.Transport != original.Transport {
		t.Errorf("Transport = %q; want %q", decoded.Transport, original.Transport)
	}
	if decoded.Destination.Kind != original.Destination.Kind {
		t.Errorf("Destination.Kind = %q; want %q", decoded.Destination.Kind, original.Destination.Kind)
	}
	if decoded.Destination.Name != original.Destination.Name {
		t.Errorf("Destination.Name = %q; want %q", decoded.Destination.Name, original.Destination.Name)
	}
	if decoded.Destination.Attributes["format"] != "json" {
		t.Errorf("Destination.Attributes[format] = %q; want json", decoded.Destination.Attributes["format"])
	}
	if decoded.AggregateID != original.AggregateID {
		t.Errorf("AggregateID = %v; want %v", decoded.AggregateID, original.AggregateID)
	}
	if decoded.Requirement != original.Requirement {
		t.Errorf("Requirement = %q; want %q", decoded.Requirement, original.Requirement)
	}
	if decoded.Policy.Enabled != original.Policy.Enabled {
		t.Errorf("Policy.Enabled = %v; want %v", decoded.Policy.Enabled, original.Policy.Enabled)
	}
	if decoded.Event.TenantID != original.Event.TenantID {
		t.Errorf("Event.TenantID = %q; want %q", decoded.Event.TenantID, original.Event.TenantID)
	}
	if decoded.Event.ResourceType != original.Event.ResourceType {
		t.Errorf("Event.ResourceType = %q; want %q", decoded.Event.ResourceType, original.Event.ResourceType)
	}

	// Decoded envelope must still validate.
	if err := decoded.Validate(); err != nil {
		t.Errorf("decoded.Validate() error = %v; want nil after round-trip", err)
	}
}
