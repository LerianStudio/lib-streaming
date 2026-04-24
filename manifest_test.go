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
