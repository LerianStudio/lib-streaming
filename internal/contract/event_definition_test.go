//go:build unit

package contract

import (
	"errors"
	"testing"
)

func TestEventDefinition_New_NormalizesDefaults(t *testing.T) {
	t.Parallel()

	definition, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}

	if definition.SchemaVersion != defaultSchemaVersion {
		t.Errorf("SchemaVersion = %q; want %q", definition.SchemaVersion, defaultSchemaVersion)
	}
	if definition.DataContentType != defaultDataContentType {
		t.Errorf("DataContentType = %q; want %q", definition.DataContentType, defaultDataContentType)
	}
	if definition.DefaultPolicy != DefaultDeliveryPolicy() {
		t.Errorf("DefaultPolicy = %#v; want %#v", definition.DefaultPolicy, DefaultDeliveryPolicy())
	}
	if got := definition.EventKey(); got != "transaction.created" {
		t.Errorf("EventKey() = %q; want %q", got, "transaction.created")
	}
}

func TestEventDefinition_New_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition EventDefinition
		want       error
	}{
		{
			name: "missing key",
			definition: EventDefinition{
				ResourceType: "transaction",
				EventType:    "created",
			},
			want: ErrInvalidEventDefinition,
		},
		{
			name: "missing resource",
			definition: EventDefinition{
				Key:       "transaction.created",
				EventType: "created",
			},
			want: ErrMissingResourceType,
		},
		{
			name: "missing event",
			definition: EventDefinition{
				Key:          "transaction.created",
				ResourceType: "transaction",
			},
			want: ErrMissingEventType,
		},
		{
			name: "invalid policy",
			definition: EventDefinition{
				Key:           "transaction.created",
				ResourceType:  "transaction",
				EventType:     "created",
				DefaultPolicy: DeliveryPolicy{Enabled: true, Direct: DirectMode("async")},
			},
			want: ErrInvalidDeliveryPolicy,
		},
		{
			name: "control char in resource",
			definition: EventDefinition{
				Key:          "transaction.created",
				ResourceType: "transaction\n",
				EventType:    "created",
			},
			want: ErrInvalidResourceType,
		},
		{
			// "." is the EventKey separator: ("payment.refund", "created")
			// and ("payment", "refund.created") would otherwise compose the
			// same dispatch key.
			name: "dotted resource",
			definition: EventDefinition{
				Key:          "payment.refund.created",
				ResourceType: "payment.refund",
				EventType:    "created",
			},
			want: ErrInvalidResourceType,
		},
		{
			name: "dotted event",
			definition: EventDefinition{
				Key:          "payment.refund.created",
				ResourceType: "payment",
				EventType:    "refund.created",
			},
			want: ErrInvalidEventType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewEventDefinition(tt.definition)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewEventDefinition() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
			if !errors.Is(err, ErrInvalidEventDefinition) {
				t.Fatalf("NewEventDefinition() error = %v; want ErrInvalidEventDefinition", err)
			}
		})
	}
}

// TestEventDefinition_EventKey pins the dispatch key that replaced the
// per-definition topic. A definition has NO topic of its own in v3 — the
// producing application has exactly one — so what a definition contributes to
// routing is the "<resourceType>.<eventType>" selector a consumer registers a
// handler under. SchemaVersion cannot influence it.
func TestEventDefinition_EventKey(t *testing.T) {
	t.Parallel()

	for _, schemaVersion := range []string{"", "1.0.0", "2.0.0", "3.5.7", "0.9.0"} {
		t.Run("schema_version="+schemaVersion, func(t *testing.T) {
			t.Parallel()

			definition, err := NewEventDefinition(EventDefinition{
				Key:           "payment.authorized",
				ResourceType:  "payment",
				EventType:     "authorized",
				SchemaVersion: schemaVersion,
			})
			if err != nil {
				t.Fatalf("NewEventDefinition() error = %v", err)
			}

			if got, want := definition.EventKey(), "payment.authorized"; got != want {
				t.Errorf("EventKey() = %q; want %q", got, want)
			}
		})
	}
}

// TestEventDefinition_EventKey_SnakeCase pins that a snake_case resource type
// survives verbatim. v2 route keys forbade underscores, which forced every
// consuming repo to carry '_'->'-' translation machinery; the dispatch key
// carries the resource type as the catalog spells it.
func TestEventDefinition_EventKey_SnakeCase(t *testing.T) {
	t.Parallel()

	definition, err := NewEventDefinition(EventDefinition{
		Key:          "loan_contract.disbursed",
		ResourceType: "loan_contract",
		EventType:    "disbursed",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}

	if got, want := definition.EventKey(), "loan_contract.disbursed"; got != want {
		t.Errorf("EventKey() = %q; want %q", got, want)
	}
}

// TestEventDefinition_New_RejectsMalformedSchemaVersion pins the
// construction-time semver gate at operation="event_definition.schema_version".
// A non-empty unparseable SchemaVersion fails NewEventDefinition with
// ErrInvalidEventDefinition wrapping ErrInvalidSchemaVersion AND fires the
// asserter trident with violation="schema_parse_failed". This catches the
// silent-routing-drift failure mode at bootstrap rather than at runtime
// (where Topic() now silently returns base form, by design).
//
// We do NOT call t.Parallel() because this test swaps the package-default
// asserter logger via setContractAsserterLogger; the swap is a global
// pointer flip and concurrent tests would observe whichever logger is
// current. Mirror event_topic_assert_test.go's discipline.
func TestEventDefinition_New_RejectsMalformedSchemaVersion(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	_, err := NewEventDefinition(EventDefinition{
		Key:           "payment.authorized",
		ResourceType:  "payment",
		EventType:     "authorized",
		SchemaVersion: "two-point-oh",
	})

	if !errors.Is(err, ErrInvalidEventDefinition) {
		t.Fatalf("NewEventDefinition() error = %v; want errors.Is(ErrInvalidEventDefinition)", err)
	}
	if !errors.Is(err, ErrInvalidSchemaVersion) {
		t.Fatalf("NewEventDefinition() error = %v; want errors.Is(ErrInvalidSchemaVersion)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on malformed SchemaVersion at NewEventDefinition")
	}
}
