//go:build unit

package streaming

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestManifest_BuildManifest(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(
		EventDefinition{
			Key:           "transaction.created",
			ResourceType:  "transaction",
			EventType:     "created",
			Description:   "Transaction was created.",
			DefaultPolicy: DeliveryPolicy{Enabled: true, Outbox: OutboxModeAlways},
		},
		EventDefinition{
			Key:           "account.closed",
			ResourceType:  "account",
			EventType:     "closed",
			SystemEvent:   true,
			DataSchema:    "https://schemas.example/account.closed.json",
			DefaultPolicy: DeliveryPolicy{Enabled: true, DLQ: DLQModeNever},
		},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	manifest, err := BuildManifest(PublisherDescriptor{
		ServiceName:     "transaction-service",
		SourceBase:      "//lerian.midaz/transaction-service",
		RoutePath:       "/streaming",
		OutboxSupported: true,
		AppVersion:      "v1.2.3",
		LibVersion:      "v0.1.0",
	}, catalog)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	if manifest.Version != ManifestVersion {
		t.Errorf("Version = %q; want %q", manifest.Version, ManifestVersion)
	}
	if manifest.Publisher.ServiceName != "transaction-service" {
		t.Errorf("Publisher.ServiceName = %q", manifest.Publisher.ServiceName)
	}
	if len(manifest.Events) != 2 {
		t.Fatalf("len(Events) = %d; want 2", len(manifest.Events))
	}
	if manifest.Events[0].Key != "account.closed" {
		t.Errorf("Events[0].Key = %q; want sorted key account.closed", manifest.Events[0].Key)
	}
	if manifest.Events[0].Topic != "lerian.streaming.account.closed" {
		t.Errorf("Events[0].Topic = %q", manifest.Events[0].Topic)
	}
	if !manifest.Events[0].SystemEvent {
		t.Error("Events[0].SystemEvent = false; want true")
	}
	if manifest.Events[1].DefaultPolicy.Outbox != OutboxModeAlways {
		t.Errorf("Events[1].DefaultPolicy.Outbox = %q; want always", manifest.Events[1].DefaultPolicy.Outbox)
	}

	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"serviceName":"transaction-service"`) {
		t.Errorf("manifest JSON = %s; want camelCase publisher metadata", string(encoded))
	}
}

func TestManifest_InvalidDescriptor(t *testing.T) {
	t.Parallel()

	_, err := BuildManifest(PublisherDescriptor{}, Catalog{})
	if !errors.Is(err, ErrInvalidPublisherDescriptor) {
		t.Fatalf("BuildManifest() error = %v; want ErrInvalidPublisherDescriptor", err)
	}
}

// TestBuildManifest_EmptyCatalogYieldsEmptyArray asserts that an empty
// catalog serializes as "events":[] rather than "events":null. Downstream
// manifest consumers relying on an array shape must not break when a
// producer is constructed before any event definition is registered.
func TestBuildManifest_EmptyCatalogYieldsEmptyArray(t *testing.T) {
	t.Parallel()

	manifest, err := BuildManifest(PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//s",
	}, Catalog{})
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	if !strings.Contains(string(encoded), `"events":[]`) {
		t.Errorf("manifest JSON = %s; want literal \"events\":[]", string(encoded))
	}

	if strings.Contains(string(encoded), `"events":null`) {
		t.Errorf("manifest JSON = %s; must NOT contain \"events\":null", string(encoded))
	}
}

// TestManifest_ProducerIDRoundTrips asserts that a descriptor populated with
// ProducerID survives the BuildManifest pipeline and round-trips correctly
// through JSON — both on the document's publisher metadata and on the
// decoded struct.
func TestManifest_ProducerIDRoundTrips(t *testing.T) {
	t.Parallel()

	const producerID = "01939c11-1d49-7abc-bd3f-1fa8cafe1234"

	catalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	manifest, err := BuildManifest(PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//s",
		ProducerID:  producerID,
	}, catalog)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	if manifest.Publisher.ProducerID != producerID {
		t.Errorf("manifest.Publisher.ProducerID = %q; want %q", manifest.Publisher.ProducerID, producerID)
	}

	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	if !strings.Contains(string(encoded), `"producerId":"`+producerID+`"`) {
		t.Errorf("manifest JSON = %s; want producerId to round-trip", string(encoded))
	}

	var decoded ManifestDocument
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if decoded.Publisher.ProducerID != producerID {
		t.Errorf("decoded.Publisher.ProducerID = %q; want %q", decoded.Publisher.ProducerID, producerID)
	}
}
