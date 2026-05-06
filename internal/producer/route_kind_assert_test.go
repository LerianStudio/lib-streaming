//go:build unit

package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestValidateRoutesAgainstTargets_KindMismatch_FiresAssertion pins T5
// site producer_multi.go:222: when validateRoutesAgainstTargets detects
// a route whose Destination.Kind does not match the registered target's
// Kind, the asserter trident MUST fire under
// operation="producer_multi.validate_routes" and the constructor MUST
// still return ErrInvalidRouteDefinition. Closes the construction-vs-
// runtime asymmetry — the runtime mirror at emit_multi.go:303 is
// unreachable under normal flow but DOES fire; the construction-time
// predecessor was silent until this asserter landed.
func TestValidateRoutesAgainstTargets_KindMismatch_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	ctx := context.Background()

	// Target reports Kafka kind; route declares an SQS destination —
	// validateRoutesAgainstTargets must reject the cross-product.
	primary := fake.NewAdapter(TransportKafkaLike)

	soleDef, err := contract.NewEventDefinition(contract.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}
	catalog, err := contract.NewCatalog(soleDef)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	// Mismatched route: destination is SQS but target is Kafka.
	routes, err := contract.NewRouteTable(
		contract.RouteDefinition{
			Key:           "transaction.created.sqs.primary",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination:   contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
			Requirement:   contract.RouteRequired,
		},
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	_, err = NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://route-kind-assert"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(cap),
		WithCatalog(catalog),
	)

	if !errors.Is(err, contract.ErrInvalidRouteDefinition) {
		t.Fatalf("NewProducerMulti() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on validateRoutesAgainstTargets kind mismatch")
	}
}
