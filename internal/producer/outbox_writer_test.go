//go:build unit

package producer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestOutboxAdapterWritesStableEnvelopeRows(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.Write(context.Background(), envelope); err != nil {
		t.Fatalf("Write err = %v", err)
	}

	row := repo.firstCreated()
	if row == nil {
		t.Fatal("repo.Create was not called")
	}
	if row.EventType != StreamingOutboxEventType {
		t.Fatalf("row.EventType = %q; want %q", row.EventType, StreamingOutboxEventType)
	}

	var got OutboxEnvelope
	if err := json.Unmarshal(row.Payload, &got); err != nil {
		t.Fatalf("json.Unmarshal row.Payload err = %v", err)
	}
	if got.Topic != event.Topic() {
		t.Fatalf("envelope.Topic = %q; want %q", got.Topic, event.Topic())
	}
}

func TestOutboxAdapterWriteWithTxUsesRepositoryTransaction(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.WriteWithTx(context.Background(), &sql.Tx{}, envelope); err != nil {
		t.Fatalf("WriteWithTx err = %v", err)
	}

	if got := repo.createdWithTxCount(); got != 1 {
		t.Fatalf("createdWithTxCount = %d; want 1", got)
	}
	if got := repo.createdCount(); got != 0 {
		t.Fatalf("createdCount = %d; want 0", got)
	}
}

func TestOutboxAdapterNilRepositoryReturnsConfiguredSentinel(t *testing.T) {
	var nilWriter *libCommonsOutboxWriter
	if err := nilWriter.Write(context.Background(), OutboxEnvelope{}); !errors.Is(err, ErrOutboxNotConfigured) {
		t.Fatalf("Write err = %v; want ErrOutboxNotConfigured", err)
	}
}

func TestOutboxAdapterRejectsOversizedSerializedEnvelope(t *testing.T) {
	repo := &fakeOutboxRepo{}
	writer := &libCommonsOutboxWriter{repo: repo}

	event := sampleEvent()
	event.Payload = json.RawMessage(`"` + strings.Repeat("x", maxPayloadBytes-1) + `"`)
	event.ApplyDefaults()
	envelope := testOutboxEnvelope(event, event.Topic(), "transaction.created", DefaultDeliveryPolicy(), uuid.New())

	if err := writer.Write(context.Background(), envelope); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Write err = %v; want ErrPayloadTooLarge", err)
	}
	if got := repo.createdCount(); got != 0 {
		t.Fatalf("createdCount = %d; want 0", got)
	}
}
