//go:build unit

package manifest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
)

func descriptorFixture() PublisherDescriptor {
	return PublisherDescriptor{
		ServiceName: "lib-streaming-test",
		SourceBase:  "svc://lib-streaming-test",
		AppVersion:  "0.0.0",
		LibVersion:  "0.0.0",
	}
}

func definitionFixture(t *testing.T) contract.EventDefinition {
	t.Helper()

	def, err := contract.NewEventDefinition(contract.EventDefinition{
		Key:           "transaction.created",
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
		Description:   "fixture",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}

	return def
}

func catalogFixture(t *testing.T) Catalog {
	t.Helper()

	cat, err := contract.NewCatalog(definitionFixture(t))
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return cat
}

// TestBuildManifest_EmptyTableOmitsRoutesField pins the contract that an
// empty RouteTable yields a catalog-only document with the routes field
// absent from the JSON wire shape.
func TestBuildManifest_EmptyTableOmitsRoutesField(t *testing.T) {
	t.Parallel()

	doc, err := BuildManifest(descriptorFixture(), catalogFixture(t), RouteTable{})
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	if doc.Routes != nil {
		t.Errorf("BuildManifest() Routes = %v; want nil", doc.Routes)
	}

	if doc.Version != ManifestVersion {
		t.Errorf("BuildManifest() Version = %q; want %q", doc.Version, ManifestVersion)
	}

	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	if strings.Contains(string(body), "\"routes\"") {
		t.Errorf("manifest JSON unexpectedly contains \"routes\" key: %s", string(body))
	}
}

func TestBuildManifest_KafkaRoute(t *testing.T) {
	t.Parallel()

	routes, err := contract.NewRouteTable(
		contract.RouteDefinition{
			Key:           "transaction.created.kafka.primary",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		},
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	doc, err := BuildManifest(descriptorFixture(), catalogFixture(t), routes)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	if got := len(doc.Routes); got != 1 {
		t.Fatalf("len(Routes) = %d; want 1", got)
	}

	if doc.Version != ManifestVersion {
		t.Errorf("BuildManifest() Version = %q; want %q", doc.Version, ManifestVersion)
	}

	r := doc.Routes[0]

	if r.Key != "transaction.created.kafka.primary" {
		t.Errorf("Key = %q; want transaction.created.kafka.primary", r.Key)
	}
	if r.DefinitionKey != "transaction.created" {
		t.Errorf("DefinitionKey = %q; want transaction.created", r.DefinitionKey)
	}
	if r.Target != "primary" {
		t.Errorf("Target = %q; want primary", r.Target)
	}
	if r.Transport != contract.TransportKafkaLike {
		t.Errorf("Transport = %q; want kafka", r.Transport)
	}
	if r.Destination != "lerian.streaming.transaction.created" {
		t.Errorf("Destination = %q; want lerian.streaming.transaction.created", r.Destination)
	}
	if !r.Required {
		t.Errorf("Required = false; want true (default requirement)")
	}
	if r.DLQConfigured {
		t.Errorf("DLQConfigured = true; want false (no route-level DLQ)")
	}

	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if !strings.Contains(string(body), "\"routes\"") {
		t.Errorf("routed manifest JSON missing \"routes\" key: %s", string(body))
	}
}

func TestBuildManifest_AllTransportsRendered(t *testing.T) {
	t.Parallel()

	def := definitionFixture(t)
	def2, err := contract.NewEventDefinition(contract.EventDefinition{
		Key:           "ledger.posted",
		ResourceType:  "ledger",
		EventType:     "posted",
		SchemaVersion: "1.0.0",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}

	cat, err := contract.NewCatalog(def, def2)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	dlq := contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created.dlq"}

	routes, err := contract.NewRouteTable(
		contract.RouteDefinition{
			Key:           "transaction.created.kafka.primary",
			DefinitionKey: def.Key,
			Target:        "kafka-primary",
			Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
			Requirement:   contract.RouteRequired,
			DLQ:           &dlq,
		},
		contract.RouteDefinition{
			Key:           "transaction.created.sqs.shadow",
			DefinitionKey: def.Key,
			Target:        "sqs-shadow",
			Destination:   contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
			Requirement:   contract.RouteOptional,
		},
		contract.RouteDefinition{
			Key:           "transaction.created.rabbitmq.bus",
			DefinitionKey: def.Key,
			Target:        "rabbitmq-bus",
			Destination:   contract.Destination{Kind: contract.TransportRabbitMQ, Name: "events", Address: "tx.created"},
		},
		contract.RouteDefinition{
			Key:           "ledger.posted.eventbridge.bus",
			DefinitionKey: def2.Key,
			Target:        "eventbridge-bus",
			Destination:   contract.Destination{Kind: contract.TransportEventBridge, Name: "default-bus"},
		},
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	doc, err := BuildManifest(descriptorFixture(), cat, routes)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	if got := len(doc.Routes); got != 4 {
		t.Fatalf("len(Routes) = %d; want 4", got)
	}

	byKey := make(map[string]ManifestRoute, len(doc.Routes))
	for _, r := range doc.Routes {
		byKey[r.Key] = r
	}

	cases := []struct {
		key             string
		wantTransport   contract.TransportKind
		wantDestination string
		wantRequired    bool
		wantDLQ         bool
	}{
		{"transaction.created.kafka.primary", contract.TransportKafkaLike, "lerian.streaming.transaction.created", true, true},
		{"transaction.created.sqs.shadow", contract.TransportSQS, "https://sqs.us-east-1.amazonaws.com/123/q", false, false},
		{"transaction.created.rabbitmq.bus", contract.TransportRabbitMQ, "events/tx.created", true, false},
		{"ledger.posted.eventbridge.bus", contract.TransportEventBridge, "default-bus", true, false},
	}

	for _, tc := range cases {
		got, ok := byKey[tc.key]
		if !ok {
			t.Errorf("manifest missing route %q", tc.key)
			continue
		}

		if got.Transport != tc.wantTransport {
			t.Errorf("%s Transport = %q; want %q", tc.key, got.Transport, tc.wantTransport)
		}
		if got.Destination != tc.wantDestination {
			t.Errorf("%s Destination = %q; want %q", tc.key, got.Destination, tc.wantDestination)
		}
		if got.Required != tc.wantRequired {
			t.Errorf("%s Required = %v; want %v", tc.key, got.Required, tc.wantRequired)
		}
		if got.DLQConfigured != tc.wantDLQ {
			t.Errorf("%s DLQConfigured = %v; want %v", tc.key, got.DLQConfigured, tc.wantDLQ)
		}
	}
}

func TestBuildManifest_OrderingDeterministic(t *testing.T) {
	t.Parallel()

	def := definitionFixture(t)
	cat, err := contract.NewCatalog(def)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	routes, err := contract.NewRouteTable(
		contract.RouteDefinition{
			Key:           "transaction.created.kafka.zeta",
			DefinitionKey: def.Key,
			Target:        "zeta",
			Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic.zeta"},
		},
		contract.RouteDefinition{
			Key:           "transaction.created.kafka.alpha",
			DefinitionKey: def.Key,
			Target:        "alpha",
			Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic.alpha"},
		},
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	doc1, err := BuildManifest(descriptorFixture(), cat, routes)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	doc2, err := BuildManifest(descriptorFixture(), cat, routes)
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}

	body1, _ := json.Marshal(doc1)
	body2, _ := json.Marshal(doc2)

	if string(body1) != string(body2) {
		t.Errorf("BuildManifest() not byte-stable across builds:\n%s\nvs\n%s", body1, body2)
	}

	// Alphabetical by route key within the same definition.
	if doc1.Routes[0].Key != "transaction.created.kafka.alpha" {
		t.Errorf("Routes[0].Key = %q; want transaction.created.kafka.alpha (alpha < zeta)", doc1.Routes[0].Key)
	}
}
