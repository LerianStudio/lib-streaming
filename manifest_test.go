//go:build unit

package streaming

import (
	"bytes"
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

// TestManifest_DeterministicJSON asserts that BuildManifest + json.Marshal
// produce byte-identical output for the same (descriptor, catalog) inputs,
// and that the output is independent of the insertion order into NewCatalog
// (Catalog sorts internally by Key). Downstream contract-diffing tools depend
// on this determinism to detect real schema drift vs. cosmetic reordering.
func TestManifest_DeterministicJSON(t *testing.T) {
	t.Parallel()

	defA := EventDefinition{
		Key:           "account.closed",
		ResourceType:  "account",
		EventType:     "closed",
		DefaultPolicy: DeliveryPolicy{Enabled: true, DLQ: DLQModeNever},
	}
	defB := EventDefinition{
		Key:           "transaction.created",
		ResourceType:  "transaction",
		EventType:     "created",
		DefaultPolicy: DeliveryPolicy{Enabled: true, Outbox: OutboxModeAlways},
	}
	descriptor := PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//s",
	}

	catalog1, err := NewCatalog(defA, defB)
	if err != nil {
		t.Fatalf("NewCatalog(defA, defB) error = %v", err)
	}
	catalog2, err := NewCatalog(defA, defB)
	if err != nil {
		t.Fatalf("NewCatalog(defA, defB) error = %v", err)
	}
	// Different insertion order — output must still be byte-identical because
	// Catalog sorts by Key internally.
	catalog3, err := NewCatalog(defB, defA)
	if err != nil {
		t.Fatalf("NewCatalog(defB, defA) error = %v", err)
	}

	manifest1, err := BuildManifest(descriptor, catalog1)
	if err != nil {
		t.Fatalf("BuildManifest(1) error = %v", err)
	}
	manifest2, err := BuildManifest(descriptor, catalog2)
	if err != nil {
		t.Fatalf("BuildManifest(2) error = %v", err)
	}
	manifest3, err := BuildManifest(descriptor, catalog3)
	if err != nil {
		t.Fatalf("BuildManifest(3) error = %v", err)
	}

	out1, err := json.Marshal(manifest1)
	if err != nil {
		t.Fatalf("json.Marshal(1) error = %v", err)
	}
	out2, err := json.Marshal(manifest2)
	if err != nil {
		t.Fatalf("json.Marshal(2) error = %v", err)
	}
	out3, err := json.Marshal(manifest3)
	if err != nil {
		t.Fatalf("json.Marshal(3) error = %v", err)
	}

	if !bytes.Equal(out1, out2) {
		t.Errorf("manifest JSON not deterministic across repeated builds:\nout1 = %s\nout2 = %s", out1, out2)
	}
	if !bytes.Equal(out1, out3) {
		t.Errorf("manifest JSON not independent of catalog insertion order:\nout1 = %s\nout3 = %s", out1, out3)
	}
}

// TestManifestVersion_IsStable pins the manifest wire-version as a literal.
// Tautological-looking but load-bearing: the next minor bump MUST be a
// deliberate decision, not a silent change.
func TestManifestVersion_IsStable(t *testing.T) {
	t.Parallel()

	if ManifestVersion != "1.0.0" {
		t.Errorf("ManifestVersion = %q; want %q (bumping this constant is an operational decision — coordinate with downstream consumers)", ManifestVersion, "1.0.0")
	}
}
