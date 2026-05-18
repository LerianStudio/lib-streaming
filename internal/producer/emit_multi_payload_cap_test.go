//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/log"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestEmitMulti_PerTransportPayloadCap pins the C4 fix: a payload that
// fits the global 1 MiB ceiling but exceeds a required SQS route's 256
// KiB cap MUST be rejected synchronously with ErrPayloadTooLarge before
// any I/O. The previous behavior (cap not enforced at preflight) would
// let the event land in the outbox where it would perpetually fail
// replay against the smaller-cap transport.
func TestEmitMulti_PerTransportPayloadCap_RejectsForRequiredSQSRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// 500 KiB — fits in Kafka's 1 MiB cap, exceeds SQS's 256 KiB cap.
	payloadSize := 500 * 1024
	payload := makeJSONPayload(payloadSize)

	primary := fake.NewAdapter(contract.TransportSQS)

	// Build a fresh single-definition catalog so mustMultiRouteTable's
	// auto-attached Kafka defaults do not collide with this SQS-only
	// target. The Producer rejects mismatched destination kinds at
	// construction.
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
		MultiProducerConfig{Source: "svc://payload-cap-test"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: contract.TransportSQS, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	req := eventToRequest(sampleEvent())
	req.Payload = payload

	emitErr := p.Emit(ctx, req)
	if emitErr == nil {
		t.Fatal("Emit() error = nil; want ErrPayloadTooLarge for 500KB payload on required SQS route")
	}

	if !errors.Is(emitErr, ErrPayloadTooLarge) {
		t.Errorf("Emit() error = %v; want errors.Is(ErrPayloadTooLarge)", emitErr)
	}

	// No I/O — the adapter must not have received any message.
	if got := len(primary.Messages()); got != 0 {
		t.Errorf("adapter received %d messages; want 0 (preflight should reject before I/O)", got)
	}
}

// TestEmitMulti_PerTransportPayloadCap_AllowsKafkaSizedForKafkaRoute pins
// the negative case: a payload at SQS's smaller cap still fits a Kafka
// route, so emit must succeed. Confirms the preflight cap is route-
// aware, not blanket.
func TestEmitMulti_PerTransportPayloadCap_AllowsKafkaSizedForKafkaRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := sampleCatalog(t)

	// 500 KiB on a Kafka-only route — fits Kafka's 1 MiB cap.
	payloadSize := 500 * 1024
	payload := makeJSONPayload(payloadSize)

	primary := fake.NewAdapter(TransportKafkaLike)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://payload-cap-kafka"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	req := eventToRequest(sampleEvent())
	req.Payload = payload

	if emitErr := p.Emit(ctx, req); emitErr != nil {
		t.Errorf("Emit() error = %v; want nil for 500KB payload on Kafka route", emitErr)
	}

	if got := len(primary.Messages()); got != 1 {
		t.Errorf("adapter received %d messages; want 1", got)
	}
}

// TestEmitMulti_PerTransportPayloadCap_OptionalRouteDoesNotRejectSync
// pins that an OPTIONAL route's smaller cap does NOT trigger a
// synchronous all-or-error rejection. Optional routes are best-effort;
// their failures surface as per-route span events without failing Emit.
//
// Because the optional SQS route would still attempt publish at the cap
// threshold inside the adapter, we use a fake SQS that simply accepts
// — the cap check is the only thing being measured here.
func TestEmitMulti_PerTransportPayloadCap_OptionalRouteDoesNotRejectSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	catalog := sampleCatalog(t)

	// 500 KiB — exceeds SQS cap. Optional route should NOT block emit.
	payloadSize := 500 * 1024
	payload := makeJSONPayload(payloadSize)

	primary := fake.NewAdapter(TransportKafkaLike)
	shadow := fake.NewAdapter(contract.TransportSQS)

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		contract.RouteDefinition{
			Key:           "transaction.created.sqs.shadow",
			DefinitionKey: "transaction.created",
			Target:        "shadow",
			Destination:   contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
			Requirement:   contract.RouteOptional,
		},
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://payload-cap-optional"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "shadow", Kind: contract.TransportSQS, Adapter: shadow},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	req := eventToRequest(sampleEvent())
	req.Payload = payload

	// Emit must NOT fail synchronously. The required Kafka route accepts
	// the payload; the optional SQS route's larger-than-cap payload is a
	// per-route concern, not an all-or-error rejection.
	if emitErr := p.Emit(ctx, req); emitErr != nil {
		t.Errorf("Emit() error = %v; want nil (optional route exceeding its cap must not block emit)", emitErr)
	}
}

// makeJSONPayload returns a JSON-valid payload of approximately
// approximateSize bytes. Uses a single key with a long string value so
// json.Valid passes. The exact size is approximateSize plus the wrapping
// JSON characters.
func makeJSONPayload(approximateSize int) json.RawMessage {
	// Reserve room for: {"data":"..."} = 11 chars overhead.
	const overhead = 11
	if approximateSize <= overhead {
		approximateSize = overhead + 1
	}
	value := strings.Repeat("a", approximateSize-overhead)

	return json.RawMessage(`{"data":"` + value + `"}`)
}
