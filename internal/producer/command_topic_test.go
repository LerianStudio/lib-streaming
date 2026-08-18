//go:build unit

package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-observability/v2/log"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport/fake"
)

// commandTestSource is the producing application for every case in this file.
const commandTestSource = "lender"

const (
	commandTestAppTopic      = "lerian.streaming.lender"
	commandTestCommandsTopic = "lerian.streaming.lender.commands"
	commandTestDLQTopic      = "lerian.streaming.lender.dlq"
)

// commandMixedCatalog is the shape the consignado rail actually has: a pile of
// facts plus a handful of commands, one catalog, one producer.
func commandMixedCatalog(t *testing.T) Catalog {
	t.Helper()

	catalog, err := NewCatalog(
		EventDefinition{
			Key:          "loan.disbursed",
			ResourceType: "loan_contract",
			EventType:    "disbursed",
		},
		EventDefinition{
			Key:          "margin.reserve",
			ResourceType: "margin",
			EventType:    "reserve",
			Class:        contract.ClassCommand,
		},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

// commandProducer wires a producer over ONE catch-all Kafka route to the app
// topic — exactly what the convenience constructor synthesizes — so the class
// split is the only thing that can move a destination.
func commandProducer(t *testing.T, adapter transport.TransportAdapter) *Producer {
	t.Helper()

	catalog := commandMixedCatalog(t)

	routes, err := contract.NewRouteTable(appTopicRoutes(commandTestSource)...)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: commandTestSource},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	return p
}

func commandEmit(t *testing.T, p *Producer, definitionKey string) error {
	t.Helper()

	return p.Emit(context.Background(), EmitRequest{
		DefinitionKey: definitionKey,
		TenantID:      "tenant-abc",
		Payload:       []byte(`{"amount":"1200.00"}`),
	})
}

// TestEmit_CommandRidesTheCommandsTopic is the load-bearing case: one catalog,
// one catch-all route, two destinations. The fact rides the app topic and the
// command rides ".commands", with nothing in the route table saying so —
// the definition's class is the whole input.
func TestEmit_CommandRidesTheCommandsTopic(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(TransportKafkaLike)
	p := commandProducer(t, adapter)

	if err := commandEmit(t, p, "loan.disbursed"); err != nil {
		t.Fatalf("Emit(fact) error = %v", err)
	}

	if err := commandEmit(t, p, "margin.reserve"); err != nil {
		t.Fatalf("Emit(command) error = %v", err)
	}

	msgs := adapter.Messages()
	if len(msgs) != 2 {
		t.Fatalf("published = %d messages; want 2", len(msgs))
	}

	if got := msgs[0].Destination.Name; got != commandTestAppTopic {
		t.Errorf("fact destination = %q; want %q", got, commandTestAppTopic)
	}

	if got := msgs[1].Destination.Name; got != commandTestCommandsTopic {
		t.Errorf("command destination = %q; want %q", got, commandTestCommandsTopic)
	}
}

// TestEmit_CommandDLQIsTheAppDLQNotACommandsDLQ pins the deliberate absence of
// a fourth topic name. A failed command publish route-DLQs into the PRODUCER's
// own ".dlq" — the one that already exists and is already granted — never a
// derived ".commands.dlq" that no ACL covers and no operator provisioned.
//
// The forensic source-topic header still names the COMMANDS topic, because
// that is where the record was actually headed and the DLQ name no longer
// implies it.
func TestEmit_CommandDLQIsTheAppDLQNotACommandsDLQ(t *testing.T) {
	t.Parallel()

	// Fails only the command publish, so the DLQ copy on the SAME adapter
	// still lands and can be inspected.
	adapter := &topicFailingAdapter{failFor: commandTestCommandsTopic}
	p := commandProducer(t, adapter)

	err := commandEmit(t, p, "margin.reserve")
	if err == nil {
		t.Fatal("Emit(command) error = nil; want the required-route failure")
	}

	var dlq *transport.TransportMessage

	for _, msg := range adapter.published {
		if msg.Destination.Name == commandTestCommandsTopic {
			continue
		}

		m := msg
		dlq = &m
	}

	if dlq == nil {
		t.Fatal("no DLQ copy was published for the failed command")
	}

	if dlq.Destination.Name != commandTestDLQTopic {
		t.Errorf("command DLQ destination = %q; want %q (there is no %q)",
			dlq.Destination.Name, commandTestDLQTopic, commandTestCommandsTopic+".dlq")
	}

	headers := map[string]string{}
	for _, h := range dlq.Headers {
		headers[h.Key] = string(h.Value)
	}

	if got := headers[dlqheader.SourceTopic]; got != commandTestCommandsTopic {
		t.Errorf("%s = %q; want %q — the DLQ name no longer says where the record was headed",
			dlqheader.SourceTopic, got, commandTestCommandsTopic)
	}
}

// TestEmit_ExplicitKafkaDestinationIsNotRewrittenForCommands pins the
// escape hatch: only an AppTopic-derived destination moves. A route the caller
// pointed somewhere on purpose — a mirror, a migration window — stays exactly
// where it was pointed, class or no class.
func TestEmit_ExplicitKafkaDestinationIsNotRewrittenForCommands(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(TransportKafkaLike)
	catalog := commandMixedCatalog(t)

	routes, err := contract.NewRouteTable(contract.RouteDefinition{
		Key:         "primary.mirror",
		Target:      "primary",
		Destination: contract.Destination{Kind: TransportKafkaLike, Name: "legacy.mirror.stream"},
		Requirement: contract.RouteRequired,
	})
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: commandTestSource},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	if err := commandEmit(t, p, "margin.reserve"); err != nil {
		t.Fatalf("Emit(command) error = %v", err)
	}

	msgs := adapter.Messages()
	if len(msgs) != 1 {
		t.Fatalf("published = %d messages; want 1", len(msgs))
	}

	if got := msgs[0].Destination.Name; got != "legacy.mirror.stream" {
		t.Errorf("explicit destination = %q; want legacy.mirror.stream (untouched by the class rewrite)", got)
	}
}

// TestEmit_CommandOutboxEnvelopeCarriesTheCommandsTopic pins that a durable
// fallback replays onto the SAME queue a direct emit would have used. An
// envelope naming the fact topic would have the relay quietly reclassify the
// command as a fact days later, on a stream whose consumer ignores it.
func TestEmit_CommandOutboxEnvelopeCarriesTheCommandsTopic(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(TransportKafkaLike)
	catalog := commandMixedCatalog(t)

	routes, err := contract.NewRouteTable(appTopicRoutes(commandTestSource)...)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	writer := &recordingOutboxWriter{}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: commandTestSource},
		map[string]DeliveryPolicyOverride{
			"margin.reserve": {Outbox: contract.OutboxModeAlways, Direct: contract.DirectModeSkip},
		},
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithOutboxWriter(writer),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	if err := commandEmit(t, p, "margin.reserve"); err != nil {
		t.Fatalf("Emit(command) error = %v", err)
	}

	if len(writer.envelopes) != 1 {
		t.Fatalf("outbox writes = %d; want 1", len(writer.envelopes))
	}

	if got := writer.envelopes[0].Destination.Name; got != commandTestCommandsTopic {
		t.Errorf("outbox envelope destination = %q; want %q", got, commandTestCommandsTopic)
	}
}

// topicFailingAdapter fails publishes to ONE destination name and accepts
// everything else, so a test can watch the DLQ copy that follows a failed
// publish through the same adapter.
type topicFailingAdapter struct {
	failFor   string
	published []transport.TransportMessage
}

func (a *topicFailingAdapter) Kind() contract.TransportKind { return TransportKafkaLike }

func (a *topicFailingAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	a.published = append(a.published, message)

	if message.Destination.Name == a.failFor {
		return errors.New("broker unavailable")
	}

	return nil
}

func (a *topicFailingAdapter) Healthy(context.Context) error { return nil }
func (a *topicFailingAdapter) Flush(context.Context) error   { return nil }
func (a *topicFailingAdapter) Close(context.Context) error   { return nil }

func (a *topicFailingAdapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	return contract.ClassBrokerUnavailable
}

// TestEmitBatch_CommandEnvelopeCarriesTheCommandsTopic closes the same hole on
// the BATCH path. A batched command persists an envelope the relay republishes
// from days later; one naming the fact topic would quietly reclassify the
// command as a fact, on a stream whose consumer ignores unmatched keys — the
// exact silent loss the commands queue exists to prevent, arriving through the
// one path nobody watches.
func TestEmitBatch_CommandEnvelopeCarriesTheCommandsTopic(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(TransportKafkaLike)
	catalog := commandMixedCatalog(t)

	routes, err := contract.NewRouteTable(appTopicRoutes(commandTestSource)...)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: commandTestSource},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	envelopes, err := p.buildBatchEnvelopes(context.Background(), []EmitRequest{
		{DefinitionKey: "loan.disbursed", TenantID: "tenant-abc", Payload: []byte(`{}`)},
		{DefinitionKey: "margin.reserve", TenantID: "tenant-abc", Payload: []byte(`{}`)},
	})
	if err != nil {
		t.Fatalf("buildBatchEnvelopes() error = %v", err)
	}

	if len(envelopes) != 2 {
		t.Fatalf("envelopes = %d; want 2", len(envelopes))
	}

	if got := envelopes[0].Destination.Name; got != commandTestAppTopic {
		t.Errorf("fact envelope destination = %q; want %q", got, commandTestAppTopic)
	}

	if got := envelopes[1].Destination.Name; got != commandTestCommandsTopic {
		t.Errorf("command envelope destination = %q; want %q", got, commandTestCommandsTopic)
	}
}

// recordingOutboxWriter captures every envelope handed to the outbox seam.
type recordingOutboxWriter struct {
	envelopes []OutboxEnvelope
}

func (w *recordingOutboxWriter) Write(_ context.Context, envelope OutboxEnvelope) error {
	w.envelopes = append(w.envelopes, envelope)
	return nil
}
