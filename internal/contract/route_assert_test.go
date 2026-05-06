//go:build unit

package contract

import (
	"errors"
	"testing"
)

// TestNewRouteDefinition_DLQKindMismatch_FiresAssertion pins T5 site
// route.go: a RouteDefinition whose DLQ.Kind does not match
// Destination.Kind MUST trigger the asserter trident under
// operation="route.dlq_kind_match" AND still return
// ErrInvalidRouteDefinition.
//
// Construction-time invariant rationale: publishRouteDLQ uses the
// originating route's adapter, so a cross-kind DLQ silently no-ops every
// DLQ delivery. Without the trident, an operator would never see the
// silent-failure mode on dashboards.
//
// We do NOT call t.Parallel() because this test swaps the package-default
// asserter logger via setContractAsserterLogger; the swap is a global
// pointer flip and concurrent tests would observe whichever logger is
// current. Mirror event_topic_assert_test.go's discipline.
func TestNewRouteDefinition_DLQKindMismatch_StillReturnsSentinel(t *testing.T) {
	cap := newCaptureContractLogger()
	prev := setContractAsserterLogger(cap)
	t.Cleanup(func() { setContractAsserterLogger(prev) })

	kafkaDLQ := Destination{Kind: TransportKafkaLike, Name: "tx.dlq"}

	_, err := NewRouteDefinition(RouteDefinition{
		Key:           "tx.created.sqs.primary",
		DefinitionKey: "tx.created",
		Target:        "primary",
		// Destination is SQS but DLQ is Kafka — cross-kind mismatch.
		Destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
		DLQ:         &kafkaDLQ,
		Requirement: RouteRequired,
	})

	if !errors.Is(err, ErrInvalidRouteDefinition) {
		t.Fatalf("NewRouteDefinition() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on DLQ kind mismatch")
	}
}
