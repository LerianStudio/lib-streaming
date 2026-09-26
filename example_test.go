//go:build unit

package streaming_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/streamingtest"
)

// Example_basicUsage shows the minimal Emit path exercised against a
// MockEmitter. This is the 10-line bootstrap pattern from the package
// godoc, compressed for an executable example.
func Example_basicUsage() {
	mock := streamingtest.NewMockEmitter()

	if err := mock.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "t-abc",
		Subject:       "tx-123",
		Payload:       []byte(`{"amount":100}`),
	}); err != nil {
		return
	}

	fmt.Println(len(mock.Requests()))
	// Output: 1
}

// exceptionDesk is the DiscardHandler from the README's "Reading a DLQ"
// section. It returns nil for everything: a reader's terminal error is
// classified like any other handler error, and a desk that cannot use an entry
// records it on its own surface rather than in the queue it is emptying.
type exceptionDesk struct{}

func (exceptionDesk) HandleDiscard(_ context.Context, r streaming.DiscardRecord) error {
	// r.Event.TenantID — the tenant that owned the poison record
	// r.CauseKind      — why it died, one of the DLQCause* values
	// r.SourceTopic / r.SourcePartition / r.SourceOffset — the route back to it
	// r.PayloadOmitted — whether r.Payload is genuinely absent or the real bytes
	_ = r

	return nil
}

// Example_readingADLQ is the README's DLQ-reader snippet, compiled.
//
// It exists so the documented wiring cannot rot: an example that stops
// compiling fails the build, whereas a fenced block in a Markdown file can
// drift for a year.
//
// It needs no running cluster because franz-go dials lazily — Build constructs
// the clients and returns without contacting a broker, and the example never
// polls. The point under test is the WIRING, in particular the Source(...) line:
// a DLQ reader may not carry the ce-source of the application whose quarantine
// topic it drains, or Build refuses it.
func Example_readingADLQ() {
	ctx := context.Background()

	dlqTopic, err := streaming.AppDLQTopic("lender") // the queue it drains
	if err != nil {
		return
	}

	c, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender-dlq-desk"). // NOT "lender": see ConsumerBuilder.DiscardHandler
		Topics(dlqTopic).
		DiscardHandler(exceptionDesk{}).
		Build(ctx)
	if err != nil {
		return
	}

	defer func() { _ = c.Close() }()

	fmt.Println(dlqTopic)
	// Output: lerian.streaming.lender.dlq
}

// onLoanDisbursed is the handler the signing example registers. A handler on a
// verifying consumer only ever sees records whose signature verified.
func onLoanDisbursed(context.Context, streaming.Event, []byte) error { return nil }

// Example_envelopeSigning is the README's "Signing and verifying envelopes"
// wiring, compiled. Like Example_readingADLQ it needs no running cluster: every
// Build below either returns before contacting a broker or is refused first.
//
// It pins the three things a service gets wrong first: the consumer requires
// signatures with a ring that holds each accepted producer's key, Build refuses
// a ring that leaves an accepted producer uncovered, and a key signs only for
// the ce-source it is bound to.
func Example_envelopeSigning() {
	ctx := context.Background()

	// In production the secret comes from the service's secret store, never
	// from source code. At least MinSigningSecretBytes long.
	lenderKey := streaming.SigningKey{
		ID:     "lender-2026-09",
		Source: "lender",
		Secret: bytes.Repeat([]byte{0x6b}, streaming.MinSigningSecretBytes),
	}

	ring, err := streaming.NewKeyring(lenderKey)
	if err != nil {
		return
	}

	// Consumer: a lender record that is unsigned, signed by a key id the ring
	// lacks, or does not verify goes to this consumer's DLQ; onLoanDisbursed
	// never sees it.
	c, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("loan-projector").
		Source("loan-projector").
		Apps("lender").
		OnFrom("lender", "loan.disbursed", onLoanDisbursed).
		RequireSignatures(ring).
		Build(ctx)
	if err != nil {
		return
	}

	defer func() { _ = c.Close() }()

	// Build proves coverage: matcher is accepted but has no key in the ring.
	_, err = streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("loan-projector").
		Source("loan-projector").
		Apps("lender", "matcher").
		OnFrom("lender", "loan.disbursed", onLoanDisbursed).
		OnFrom("matcher", "loan.disbursed", onLoanDisbursed).
		RequireSignatures(ring).
		Build(ctx)
	fmt.Println(errors.Is(err, streaming.ErrConsumerSignatureKeyMissingForSource))

	// Producer: lender's key cannot sign for matcher. Build refuses it before
	// any transport adapter is built, so no broker is contacted.
	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "loan.disbursed",
		ResourceType: "loan",
		EventType:    "disbursed",
	})
	if err != nil {
		return
	}

	_, err = streaming.NewBuilder().
		Source("matcher").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:         "primary.kafka",
			Target:      "primary",
			Destination: streaming.KafkaTopic("lerian.streaming.matcher"),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:    "primary",
			Kind:    streaming.TransportKafkaLike,
			Brokers: []string{"localhost:9092"},
		}).
		SignEnvelopes(ring, "lender-2026-09").
		Build(ctx)
	fmt.Println(errors.Is(err, streaming.ErrInvalidSigningKey))
	// Output:
	// true
	// true
}
