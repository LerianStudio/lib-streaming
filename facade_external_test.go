//go:build unit

package streaming_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFacade_NewEmitRequestDeepCopiesMutableFields(t *testing.T) {
	t.Parallel()

	enabled := true
	payload := json.RawMessage(`{"ok":true}`)

	request, err := streaming.NewEmitRequest(streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       payload,
		PolicyOverride: streaming.DeliveryPolicyOverride{
			Enabled: &enabled,
		},
	})
	if err != nil {
		t.Fatalf("NewEmitRequest() error = %v", err)
	}

	payload[2] = 'X'
	enabled = false

	if string(request.Payload) != `{"ok":true}` {
		t.Fatalf("Payload was not defensively copied: %q", string(request.Payload))
	}
	if request.PolicyOverride.Enabled == nil || !*request.PolicyOverride.Enabled {
		t.Fatalf("PolicyOverride.Enabled was not defensively copied: %v", request.PolicyOverride.Enabled)
	}
}

func TestFacade_NewDisabledReturnsNoopEmitter(t *testing.T) {
	t.Parallel()

	emitter, err := streaming.New(context.Background(), streaming.Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := emitter.Emit(context.Background(), streaming.EmitRequest{}); err != nil {
		t.Fatalf("noop Emit() error = %v", err)
	}
	if err := emitter.Close(); err != nil {
		t.Fatalf("noop Close() error = %v", err)
	}
}

func TestFacade_NewProducerDescriptorAndClose(t *testing.T) {
	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	producer, err := streaming.NewProducer(context.Background(), streaming.Config{
		Enabled:               true,
		Brokers:               []string{"127.0.0.1:9092"},
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    1,
		CloudEventsSource:     "//test/source",
		Compression:           "none",
		RecordRetries:         1,
		RecordDeliveryTimeout: time.Second,
		RequiredAcks:          "all",
		CloseTimeout:          time.Second,
	}, streaming.WithCatalog(catalog))
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })

	descriptor, err := producer.Descriptor(streaming.PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//test/source",
	})
	if err != nil {
		t.Fatalf("Descriptor() error = %v", err)
	}
	if descriptor.ProducerID == "" {
		t.Fatal("Descriptor() did not populate ProducerID")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestFacade_NilProducerMethodsAreSafe(t *testing.T) {
	t.Parallel()

	var producer *streaming.Producer

	if err := producer.Emit(context.Background(), streaming.EmitRequest{}); !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("nil Emit() error = %v; want ErrNilProducer", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("nil Close() error = %v; want nil", err)
	}
	if _, err := producer.Descriptor(streaming.PublisherDescriptor{}); !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("nil Descriptor() error = %v; want ErrNilProducer", err)
	}
}

func TestFacade_ManifestAndCloudEventsWrappers(t *testing.T) {
	t.Parallel()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	manifest, err := streaming.BuildManifest(streaming.PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//svc",
	}, catalog)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	if len(manifest.Events) != 1 || manifest.Events[0].Key != "transaction.created" {
		t.Fatalf("manifest events = %+v; want transaction.created", manifest.Events)
	}

	headers := streaming.BuildCloudEventsHeaders(streaming.Event{
		EventID:       "event-1",
		Source:        "//svc",
		Timestamp:     time.Unix(0, 0).UTC(),
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
	})
	if len(headers) == 0 {
		t.Fatal("BuildCloudEventsHeaders() returned no headers")
	}

	parsed, err := streaming.ParseCloudEventsHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("event-1")},
		{Key: "ce-source", Value: []byte("//svc")},
		{Key: "ce-type", Value: []byte("studio.lerian.transaction.created")},
		{Key: "ce-time", Value: []byte(time.Unix(0, 0).UTC().Format(time.RFC3339Nano))},
	})
	if err != nil {
		t.Fatalf("ParseCloudEventsHeaders() error = %v", err)
	}
	if parsed.EventID != "event-1" || parsed.Source != "//svc" {
		t.Fatalf("parsed event = %+v", parsed)
	}
}
