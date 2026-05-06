//go:build unit

package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestNewProducerMulti_AdapterKindMismatch_FiresAssertion pins T2: when a
// TargetSpec's Adapter.Kind() does not match its Kind, NewProducerMulti
// must fire the asserter trident under
// operation="producer_multi.adapter_kind_match" AND still return
// ErrInvalidRouteDefinition. Public contract preserved; trident makes the
// construction-time invariant violation observable on dashboards alongside
// the runtime mirror at emit_multi.go:303.
//
// The construction-time mirror is the load-bearing one — bootstrap failure
// catches the bug at startup. The runtime mirror is unreachable under
// normal flow but kept as belt-and-suspenders against state corruption.
func TestNewProducerMulti_AdapterKindMismatch_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()

	// Spec.Kind says TransportKafkaLike but the adapter reports
	// TransportSQS. validateRoutesAgainstTargets passes (route.Destination
	// matches Spec.Kind=TransportKafkaLike) but the adapter-vs-spec check
	// inside the targets loop must fire the trident on the kind mismatch.
	// This isolates the construction-time invariant we are protecting.
	mismatchedAdapter := fake.NewAdapter(contract.TransportSQS)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	_, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc://multi-test"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: mismatchedAdapter}},
		routes,
		catalog,
		WithLogger(cap),
		WithCatalog(catalog),
	)

	if !errors.Is(err, contract.ErrInvalidRouteDefinition) {
		t.Fatalf("NewProducerMulti() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on adapter kind mismatch")
	}
}
