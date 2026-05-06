//go:build unit

package contract

import (
	"errors"
	"testing"
)

// TestNewCatalog_DuplicateKey_StillReturnsSentinel pins T6 site
// catalog.go: duplicate Key entries continue to return
// ErrDuplicateEventDefinition AND fire the asserter trident.
//
// We do NOT call t.Parallel() because this test swaps the package-default
// asserter logger via setContractAsserterLogger; the swap is a global
// pointer flip and concurrent tests would observe whichever logger is
// current. Mirror event_topic_assert_test.go's discipline.
func TestNewCatalog_DuplicateKey_StillReturnsSentinel(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	defA, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition(A) error = %v", err)
	}

	// Second definition with the SAME Key but a different EventType so the
	// contract-tuple branch isn't the one that fires.
	defB, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "updated",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition(B) error = %v", err)
	}

	_, err = NewCatalog(defA, defB)
	if !errors.Is(err, ErrDuplicateEventDefinition) {
		t.Fatalf("NewCatalog() error = %v; want errors.Is(ErrDuplicateEventDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on duplicate catalog Key")
	}
}

// TestNewCatalog_DuplicateContractTuple_StillReturnsSentinel pins T6 site
// catalog.go second branch: distinct Keys but identical
// (ResourceType,EventType,SchemaVersion) tuple resolves to a duplicate
// contract.
func TestNewCatalog_DuplicateContractTuple_StillReturnsSentinel(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	defA, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created.a",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition(A) error = %v", err)
	}
	defB, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created.b", // distinct Key
		ResourceType: "transaction",           // same contract tuple
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition(B) error = %v", err)
	}

	_, err = NewCatalog(defA, defB)
	if !errors.Is(err, ErrDuplicateEventDefinition) {
		t.Fatalf("NewCatalog() error = %v; want errors.Is(ErrDuplicateEventDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on duplicate catalog contract tuple")
	}
}

// TestNewRouteTable_DuplicateKey_StillReturnsSentinel pins T6 site
// route.go: duplicate route.Key entries continue to return
// ErrDuplicateRouteDefinition AND fire the asserter trident.
func TestNewRouteTable_DuplicateKey_StillReturnsSentinel(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	dup := RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   Destination{Kind: TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		Requirement:   RouteRequired,
	}

	_, err := NewRouteTable(dup, dup)
	if !errors.Is(err, ErrDuplicateRouteDefinition) {
		t.Fatalf("NewRouteTable() error = %v; want errors.Is(ErrDuplicateRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on duplicate route Key")
	}
}

// TestNewEventDefinition_MissingFields_StillReturnsSentinel pins T6 site
// event_definition.go: each missing-required-field path continues to
// return the documented wrapped sentinel AND fires the trident.
//
// Subtests do NOT call t.Parallel() — every case observes the same swapped
// global logger pointer; running them serially keeps capture observation
// unambiguous per case.
func TestNewEventDefinition_MissingFields_StillReturnsSentinel(t *testing.T) {
	cases := []struct {
		name  string
		def   EventDefinition
		isErr error
	}{
		{
			name:  "missing key",
			def:   EventDefinition{ResourceType: "transaction", EventType: "created"},
			isErr: ErrInvalidEventDefinition,
		},
		{
			name:  "missing resource type",
			def:   EventDefinition{Key: "k", EventType: "created"},
			isErr: ErrMissingResourceType,
		},
		{
			name:  "missing event type",
			def:   EventDefinition{Key: "k", ResourceType: "transaction"},
			isErr: ErrMissingEventType,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cap := newCaptureContractLogger()
			prev := setContractAsserterLogger(cap)
			t.Cleanup(func() { setContractAsserterLogger(prev) })

			_, err := NewEventDefinition(tc.def)
			if !errors.Is(err, tc.isErr) {
				t.Fatalf("NewEventDefinition() error = %v; want errors.Is(%v)", err, tc.isErr)
			}

			if !cap.containsMessage("ASSERTION FAILED") {
				t.Fatalf("expected asserter trident to fire on %s", tc.name)
			}
		})
	}
}
