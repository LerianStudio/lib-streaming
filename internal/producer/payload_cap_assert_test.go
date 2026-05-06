//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestEmitMulti_PayloadCap_FiresAssertion pins T3 site (a): the per-route
// payload cap rejection in emit_multi.go must fire the asserter trident
// under operation="emit_multi.payload_size" AND still return
// *EmitError{Class: ClassValidation, Cause: ErrPayloadTooLarge}.
// Dashboards distinguish "blew SQS 256 KiB" from "blew Kafka 1 MiB"
// without parsing error strings. Op-label mirrors preflight.payload_size
// for symmetry across single- vs multi-target sites.
func TestEmitMulti_PayloadCap_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()
	ctx := context.Background()

	// 500 KiB — fits Kafka's 1 MiB but exceeds SQS's 256 KiB cap.
	payload := makeJSONPayload(500 * 1024)

	primary := fake.NewAdapter(contract.TransportSQS)

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

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://payload-cap-assert"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: contract.TransportSQS, Adapter: primary}},
		routes,
		catalog,
		WithLogger(cap),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	req := eventToRequest(sampleEvent())
	req.Payload = payload

	emitErr := p.Emit(ctx, req)
	if !errors.Is(emitErr, ErrPayloadTooLarge) {
		t.Fatalf("Emit() error = %v; want errors.Is(ErrPayloadTooLarge)", emitErr)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on multi-target payload cap rejection")
	}
}

// TestPreFlight_PayloadCap_FiresAssertion pins T3 site (b): the
// single-target preflight payload cap rejection in preflight.go must fire
// the asserter trident under operation="preflight.payload_size" AND still
// return ErrPayloadTooLarge.
func TestPreFlight_PayloadCap_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureLogger()

	// Producer fixture with the capture logger — preFlightWithPayload uses
	// p.newAsserter which reads p.logger.
	p := &Producer{logger: cap}

	event := Event{
		TenantID:     "tenant-1",
		ResourceType: "transaction",
		EventType:    "created",
		Source:       "svc://test",
		// Payload exceeds the 1 MiB single-target cap.
		Payload: []byte(strings.Repeat("a", maxPayloadBytes+1)),
	}

	err := p.preFlightWithPayload(context.Background(), event, true)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("preFlightWithPayload err = %v; want ErrPayloadTooLarge", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on single-target preflight cap rejection")
	}
}
