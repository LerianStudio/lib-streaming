//go:build unit

package fake

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

func TestAdapter_PublishRecordsDeepCopy(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter(contract.TransportKafkaLike)
	message := transport.TransportMessage{
		Destination: contract.Destination{
			Kind:       contract.TransportKafkaLike,
			Name:       "topic-a",
			Attributes: map[string]string{"format": "json"},
		},
		TenantID:   "tenant-1",
		Key:        "tenant-1",
		Payload:    []byte(`{"ok":true}`),
		Headers:    []transport.Header{{Key: "ce-id", Value: []byte("event-1")}},
		Attributes: map[string]string{"definition_key": "transaction.created"},
	}

	if err := adapter.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	message.Payload[1] = 'X'
	message.Headers[0].Value[0] = 'X'
	message.Attributes["definition_key"] = "mutated"
	message.Destination.Attributes["format"] = "mutated"

	messages := adapter.Messages()
	if len(messages) != 1 {
		t.Fatalf("Messages() len = %d; want 1", len(messages))
	}

	if string(messages[0].Payload) != `{"ok":true}` {
		t.Fatalf("Payload was not copied: %q", string(messages[0].Payload))
	}
	if string(messages[0].Headers[0].Value) != "event-1" {
		t.Fatalf("Header value was not copied: %q", string(messages[0].Headers[0].Value))
	}
	if messages[0].Attributes["definition_key"] != "transaction.created" {
		t.Fatalf("Attributes were not copied: %#v", messages[0].Attributes)
	}
	if messages[0].TenantID != "tenant-1" {
		t.Fatalf("TenantID = %q; want tenant-1", messages[0].TenantID)
	}
	if messages[0].Destination.Attributes["format"] != "json" {
		t.Fatalf("Destination attributes were not copied: %#v", messages[0].Destination.Attributes)
	}

	messages[0].Payload[1] = 'Y'
	fresh := adapter.Messages()
	if string(fresh[0].Payload) != `{"ok":true}` {
		t.Fatalf("Messages() exposed internal storage: %q", string(fresh[0].Payload))
	}
}

func TestAdapter_ConfiguredErrors(t *testing.T) {
	t.Parallel()

	publishErr := errors.New("publish failed")
	healthErr := errors.New("health failed")
	flushErr := errors.New("flush failed")
	closeErr := errors.New("close failed")

	adapter := NewAdapter(contract.TransportSQS)
	adapter.SetPublishError(publishErr)
	adapter.SetHealthError(healthErr)
	adapter.SetFlushError(flushErr)
	adapter.SetCloseError(closeErr)

	if got := adapter.Kind(); got != contract.TransportSQS {
		t.Fatalf("Kind() = %q; want %q", got, contract.TransportSQS)
	}
	if err := adapter.Publish(context.Background(), transport.TransportMessage{}); !errors.Is(err, publishErr) {
		t.Fatalf("Publish() error = %v; want publishErr", err)
	}
	if err := adapter.Healthy(context.Background()); !errors.Is(err, healthErr) {
		t.Fatalf("Healthy() error = %v; want healthErr", err)
	}
	if err := adapter.Flush(context.Background()); !errors.Is(err, flushErr) {
		t.Fatalf("Flush() error = %v; want flushErr", err)
	}
	if err := adapter.Close(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v; want closeErr", err)
	}
	if !adapter.Closed() {
		t.Fatal("Closed() = false; want true")
	}
	if got := len(adapter.Messages()); got != 0 {
		t.Fatalf("Messages() len = %d; want 0 when publish fails", got)
	}
}

func TestAdapter_ConcurrentOperationsAreSafe(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter(contract.TransportKafkaLike)
	configuredPublishErr := errors.New("configured publish failed")
	configuredHealthErr := errors.New("configured health failed")
	configuredFlushErr := errors.New("configured flush failed")
	configuredCloseErr := errors.New("configured close failed")

	var wg sync.WaitGroup
	errCh := make(chan error, 160)

	for i := 0; i < 40; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := adapter.Publish(context.Background(), transport.TransportMessage{
				Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic-a"},
				TenantID:    "tenant-1",
				Key:         "tenant-1",
				Payload:     []byte(`{"ok":true}`),
				Headers:     []transport.Header{{Key: "ce-id", Value: []byte("event")}},
				Attributes:  map[string]string{"index": string(rune('a' + i%26))},
			})
			if err != nil && !errors.Is(err, configuredPublishErr) && !errors.Is(err, contract.ErrEmitterClosed) {
				errCh <- err
			}
		}()
	}

	for i := 0; i < 40; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			if i%2 == 0 {
				adapter.SetPublishError(configuredPublishErr)
				adapter.SetHealthError(configuredHealthErr)
				adapter.SetFlushError(configuredFlushErr)
				adapter.SetCloseError(configuredCloseErr)
				return
			}

			adapter.SetPublishError(nil)
			adapter.SetHealthError(nil)
			adapter.SetFlushError(nil)
			adapter.SetCloseError(nil)
		}()
	}

	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_ = adapter.Messages()
			if err := adapter.Healthy(context.Background()); err != nil && !errors.Is(err, configuredHealthErr) {
				errCh <- err
			}
			if err := adapter.Flush(context.Background()); err != nil && !errors.Is(err, configuredFlushErr) {
				errCh <- err
			}
		}()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := adapter.Close(context.Background()); err != nil && !errors.Is(err, configuredCloseErr) {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent adapter operation error = %v", err)
		}
	}

	if !adapter.Closed() {
		t.Fatal("Closed() = false; want true after concurrent Close")
	}
	_ = adapter.Messages()
}

func TestAdapter_PublishAfterCloseReturnsEmitterClosed(t *testing.T) {
	t.Parallel()

	adapter := NewAdapter(contract.TransportKafkaLike)
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	err := adapter.Publish(context.Background(), transport.TransportMessage{})
	if !errors.Is(err, contract.ErrEmitterClosed) {
		t.Fatalf("Publish() error = %v; want ErrEmitterClosed", err)
	}
}

func TestAdapter_ContextCancellationAndClassify(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	adapter := NewAdapter(contract.TransportKafkaLike)
	if err := adapter.Publish(ctx, transport.TransportMessage{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(canceled ctx) error = %v; want context.Canceled", err)
	}
	if class := adapter.Classify(context.Canceled); class != contract.ClassContextCanceled {
		t.Fatalf("Classify(context.Canceled) = %q; want %q", class, contract.ClassContextCanceled)
	}
	if class := adapter.Classify(errors.New("broker down")); class != contract.ClassBrokerUnavailable {
		t.Fatalf("Classify(broker down) = %q; want %q", class, contract.ClassBrokerUnavailable)
	}
}
