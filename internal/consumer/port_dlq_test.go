//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport/fake"
)

// TestPublishDLQ_PreservesKey proves the DLQ copy carries the original record Key
// verbatim (finding #5): losing it breaks verbatim republish and changes DLQ
// partitioning for replay.
func TestPublishDLQ_PreservesKey(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := &transportDLQPublisher{adapter: adapter, suffix: ".dlq", groupID: "g"}

	source := &kgo.Record{
		Topic:     "loan.created",
		Partition: 3,
		Offset:    42,
		Key:       []byte("tenant-abc|loan-1"),
		Value:     []byte(`{"ok":true}`),
		Headers:   []kgo.RecordHeader{{Key: "ce-id", Value: []byte("evt-1")}},
	}

	if err := pub.PublishDLQ(context.Background(), source, errTerminalQuarantine, 0); err != nil {
		t.Fatalf("PublishDLQ: %v", err)
	}

	msgs := adapter.Messages()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages; want 1", len(msgs))
	}

	if got := msgs[0].Key; got != string(source.Key) {
		t.Errorf("DLQ message Key = %q; want %q (verbatim from rec.Key)", got, source.Key)
	}

	if got := msgs[0].Destination.Name; got != "loan.created.dlq" {
		t.Errorf("DLQ destination = %q; want loan.created.dlq", got)
	}
}

// TestTransportDLQPublisher_CloseDelegates proves Close flushes/closes the
// underlying produce-side adapter (finding #4): without it the second franz-go
// client Build creates for DLQ publishing leaks.
func TestTransportDLQPublisher_CloseDelegates(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(contract.TransportKafkaLike)
	pub := &transportDLQPublisher{adapter: adapter, suffix: ".dlq", groupID: "g"}

	if err := pub.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !adapter.Closed() {
		t.Error("DLQ adapter not closed; transportDLQPublisher.Close must delegate to adapter.Close")
	}
}

// TestConsumerRuntimeClose_ClosesDLQ proves the runtime closes the DLQ publisher
// on Close (finding #4), so the DLQ client is not leaked alongside the consume
// client.
func TestConsumerRuntimeClose_ClosesDLQ(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient()
	dlq := &fakeDLQ{}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if dlq.closeCount() != 1 {
		t.Errorf("DLQ Close called %d times; want 1", dlq.closeCount())
	}

	// Idempotent: a second Close must not re-close the DLQ.
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if dlq.closeCount() != 1 {
		t.Errorf("DLQ Close called %d times after second runtime Close; want 1 (idempotent)", dlq.closeCount())
	}
}

// TestConsumerRuntimeClose_ReturnsDLQError proves a DLQ publisher close failure
// is surfaced from Close() (wrapped, not swallowed). A failed DLQ close can mean
// buffered quarantine writes were lost, so the shutdown path must let callers see
// it rather than silently returning nil.
func TestConsumerRuntimeClose_ReturnsDLQError(t *testing.T) {
	t.Parallel()

	client := newFakeGroupClient()
	dlq := &fakeDLQ{closeErr: contract.ErrNilProducer}
	r := newTestRuntime(t, client, &fakeHandler{}, dlq)

	err := r.Close()
	if err == nil {
		t.Fatal("Close returned nil; want the DLQ close failure surfaced, not swallowed")
	}

	if !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("Close error = %v; want it to wrap the DLQ close error (%v)", err, contract.ErrNilProducer)
	}
}
