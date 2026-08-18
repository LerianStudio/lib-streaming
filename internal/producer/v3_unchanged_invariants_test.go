//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// This file pins the surfaces the v3 contract redesign explicitly left ALONE.
// They are easy to break by accident while the topic, source, ce-type, route,
// and manifest contracts all move around them, and each one is load-bearing
// for an already-deployed operational behaviour.

// TestUnchanged_OutboxEventTypeIsStableAndDBOnly pins two things at once:
// the outbox row's EventType literal has not drifted, and it never reaches the
// wire. It is a DATABASE discriminator for the relay's row lookup — an event
// type in the CloudEvents sense it is not, and a consumer must never see it.
func TestUnchanged_OutboxEventTypeIsStableAndDBOnly(t *testing.T) {
	t.Parallel()

	if StreamingOutboxEventType != "lerian.streaming.publish" {
		t.Fatalf("StreamingOutboxEventType = %q; want the stable DB-only literal", StreamingOutboxEventType)
	}

	event := Event{
		TenantID:      "t-1",
		ResourceType:  "transaction",
		EventType:     "created",
		EventID:       "evt-1",
		SchemaVersion: "1.0.0",
		Source:        "midaz-ledger",
		Payload:       json.RawMessage(`{"k":"v"}`),
	}
	(&event).ApplyDefaults()

	for _, h := range buildTransportHeaders(context.Background(), event) {
		if strings.Contains(string(h.Value), StreamingOutboxEventType) {
			t.Fatalf("header %s carries the DB-only outbox event type %q; it must never reach the wire",
				h.Key, StreamingOutboxEventType)
		}
	}
}

// TestChanged_OutboxEnvelopeVersionIsTwo pins the v3 envelope-version bump and
// the rejection of version-1 rows.
//
// The SHAPE did not change — no field was added, removed, or retyped — but the
// MEANING of the persisted Destination did: a v2 row holds a per-event topic
// ("midaz-ledger.transaction.created"), a v3 row holds the app topic. A v3
// relay draining a v2 row would publish it verbatim to a topic no consumer
// subscribes to any more: green dashboards, zero delivery. Strict version
// equality turns that into a loud decode failure instead.
func TestChanged_OutboxEnvelopeVersionIsTwo(t *testing.T) {
	t.Parallel()

	if contract.OutboxEnvelopeVersion != 2 {
		t.Fatalf("OutboxEnvelopeVersion = %d; want 2 (the persisted Destination MEANING changed in v3)",
			contract.OutboxEnvelopeVersion)
	}

	v1 := validOutboxEnvelope()
	v1.Version = 1

	if err := v1.ValidateShape(); !errors.Is(err, contract.ErrInvalidOutboxEnvelope) {
		t.Fatalf("ValidateShape() on a version-1 envelope error = %v; want ErrInvalidOutboxEnvelope", err)
	}
}

// validOutboxEnvelope returns an envelope that passes ValidateShape, so a test
// can flip exactly one field and attribute the failure to it.
func validOutboxEnvelope() contract.OutboxEnvelope {
	return contract.OutboxEnvelope{
		Version:       contract.OutboxEnvelopeVersion,
		RouteKey:      "primary.kafka",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Transport:     contract.TransportKafkaLike,
		Destination:   contract.Destination{Kind: contract.TransportKafkaLike, Name: contract.AppTopic("midaz-ledger")},
		AggregateID:   uuid.New(),
		Requirement:   contract.RouteRequired,
		Policy:        contract.DefaultDeliveryPolicy(),
		Event: contract.Event{
			TenantID:      "t-1",
			ResourceType:  "transaction",
			EventType:     "created",
			EventID:       "evt-1",
			SchemaVersion: "1.0.0",
			Source:        "midaz-ledger",
		},
	}
}

// TestUnchanged_OutboxEnvelopeCarriesAppTopicDestination pins the one thing
// that DID change inside the unchanged shape: the persisted destination is now
// the application's topic, so a replay lands on exactly the topic a live Emit
// would have used.
func TestUnchanged_OutboxEnvelopeCarriesAppTopicDestination(t *testing.T) {
	t.Parallel()

	const source = "midaz-ledger"

	table, err := autoGenerateKafkaRoutes(source, nil)
	if err != nil {
		t.Fatalf("autoGenerateKafkaRoutes() error = %v", err)
	}

	routes := table.Routes("transaction.created")
	if len(routes) != 1 {
		t.Fatalf("routes = %d; want the single catch-all route", len(routes))
	}

	if got, want := routes[0].Destination.Name, contract.AppTopic(source); got != want {
		t.Fatalf("persisted destination = %q; want the app topic %q", got, want)
	}

	event := Event{Source: source, ResourceType: "transaction", EventType: "created"}
	if got := event.Topic(); got != routes[0].Destination.Name {
		t.Fatalf("replay destination %q != live Emit topic %q", routes[0].Destination.Name, got)
	}
}

// TestChanged_PartitionKeyFallbackChain pins the v3 partition-key chain:
// TenantID, else Subject, else EventID; system events keep "system:"+EventType.
//
// The topic collapse concentrates far more traffic on one topic, so the
// partition key is the ONLY thing left spreading it. Returning the empty
// TenantID for a single-tenant service pinned that whole stream to one
// partition — franz-go's sticky-key partitioner branches on key != nil, and
// []byte("") is not nil, so it took the murmur2 path on a constant.
func TestChanged_PartitionKeyFallbackChain(t *testing.T) {
	t.Parallel()

	business := Event{TenantID: "t-abc", ResourceType: "transaction", EventType: "created", Subject: "agg-1"}
	if got := business.PartitionKey(); got != "t-abc" {
		t.Errorf("business PartitionKey() = %q; want the tenant id", got)
	}

	singleTenant := Event{ResourceType: "transaction", EventType: "created", Subject: "agg-1", EventID: "evt-1"}
	if got := singleTenant.PartitionKey(); got != "agg-1" {
		t.Errorf("tenant-less PartitionKey() = %q; want the subject (per-aggregate ordering)", got)
	}

	subjectless := Event{ResourceType: "transaction", EventType: "created", EventID: "evt-1"}
	if got := subjectless.PartitionKey(); got != "evt-1" {
		t.Errorf("tenant-less subject-less PartitionKey() = %q; want the event id (spread, no ordering)", got)
	}

	system := Event{TenantID: "ignored", EventType: "reaper_pass", SystemEvent: true}
	if got := system.PartitionKey(); got != "system:reaper_pass" {
		t.Errorf("system PartitionKey() = %q; want system:reaper_pass", got)
	}
}

// TestUnchanged_TenantNeverInTopology pins that tenant identity still cannot
// enter a topic name, a route key, or a destination attribute. With one topic
// per app the temptation to shard by tenant in the NAME is stronger, so the
// guard is re-pinned against the v3 destination shape.
func TestUnchanged_TenantNeverInTopology(t *testing.T) {
	t.Parallel()

	topological := []contract.RouteDefinition{
		{
			Key:         "primary.kafka",
			Target:      "primary",
			Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.lender.${tenant_id}"},
		},
		{
			Key:         "tenant_id.kafka",
			Target:      "primary",
			Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "lerian.streaming.lender"},
		},
		{
			Key:    "primary.kafka",
			Target: "primary",
			Destination: contract.Destination{
				Kind:       contract.TransportKafkaLike,
				Name:       "lerian.streaming.lender",
				Attributes: map[string]string{"shard": "{tenant}"},
			},
		},
	}

	for i, route := range topological {
		if _, err := contract.NewRouteDefinition(route); err == nil {
			t.Errorf("case %d: NewRouteDefinition accepted tenant-scoped topology %+v", i, route.Destination)
		}
	}

	// ce-tenantid remains the ONLY place a tenant travels.
	event := Event{
		TenantID:      "t-abc",
		ResourceType:  "transaction",
		EventType:     "created",
		EventID:       "evt-1",
		SchemaVersion: "1.0.0",
		Source:        "lender",
	}
	(&event).ApplyDefaults()

	if strings.Contains(event.Topic(), event.TenantID) {
		t.Errorf("topic %q leaks the tenant id", event.Topic())
	}

	carriers := 0

	for _, h := range buildTransportHeaders(context.Background(), event) {
		if string(h.Value) == event.TenantID {
			carriers++

			if h.Key != "ce-tenantid" {
				t.Errorf("header %s carries the tenant id; only ce-tenantid may", h.Key)
			}
		}
	}

	if carriers != 1 {
		t.Errorf("tenant id appears on %d headers; want exactly ce-tenantid", carriers)
	}
}
