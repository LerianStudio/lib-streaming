//go:build unit

package streaming_test

import (
	"context"
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
// drift for a year. Build is called with an empty broker list so the example
// terminates without a cluster — the point under test is the WIRING, and the
// two refusals it would otherwise trip (a DLQ reader may not carry the
// ce-source of the application it drains, and may not combine DiscardHandler
// with Handler or On) are what the Source(...) line below avoids.
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
