//go:build unit

package streaming_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/billing"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These end-to-end tests drive the ACTUAL construction path a consuming service
// (e.g. billing-api) uses: the root package exposes only NewBuilder
// (multi-target, explicit routes). RouteOverrides merges via Builder.mergedRoutes
// using the shared contract.MergeRouteOverrides helper (REPLACE-by-DefinitionKey),
// so a billing override lands billing events on the fixed billing.Topic and an
// override sharing a base route's DefinitionKey replaces it (no double-publish).
// The merge-semantics unit truth-table lives in
// internal/contract/route_merge_test.go.

const overrideDomainTopic = "lerian.streaming.transaction.override"

// overrideKfakeTarget mirrors builderKfakeTarget but seeds every topic these
// E2E tests produce to — including the fixed billing.Topic and the replace-case
// override topic. The producer's kgo client does not request auto-creation on
// produce, so an unseeded destination fails with UNKNOWN_TOPIC_OR_PARTITION.
func overrideKfakeTarget(t *testing.T) (streaming.TargetConfig, *kfake.Cluster) {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(3),
		kfake.SeedTopics(3,
			"lerian.streaming.transaction.created",
			"lerian.streaming.account.opened",
			overrideDomainTopic,
			billing.Topic,
		),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}

	t.Cleanup(cluster.Close)

	target := streaming.TargetConfig{
		Name:     "primary",
		Kind:     streaming.TransportKafkaLike,
		Brokers:  cluster.ListenAddrs(),
		ClientID: "route-overrides-e2e",
	}

	return target, cluster
}

// overrideDomainRoute builds a required Kafka route for a domain definition
// pointing at an explicit topic, mirroring builderRoute for arbitrary keys.
func overrideDomainRoute(definitionKey, routeKey, topic string) streaming.RouteDefinition {
	return streaming.RouteDefinition{
		Key:           routeKey,
		DefinitionKey: definitionKey,
		Target:        "primary",
		Destination:   streaming.KafkaTopic(topic),
		Requirement:   streaming.RouteRequired,
	}
}

// overrideE2ECatalog returns a catalog holding two domain definitions plus the
// billing definition, mirroring builderCatalogWithDefinitions.
func overrideE2ECatalog(t *testing.T) streaming.Catalog {
	t.Helper()

	return builderCatalogWithDefinitions(t,
		streaming.EventDefinition{Key: "transaction.created", ResourceType: "transaction", EventType: "created"},
		streaming.EventDefinition{Key: "account.opened", ResourceType: "account", EventType: "opened"},
		billing.Definition(),
	)
}

// fetchRecordsOnTopic consumes topic from the start in a fresh group and returns
// the records read within a short window. A genuine fetch error (unknown topic,
// broker failure) is a test failure; only a context deadline/cancel — the normal
// signal that a (seeded, empty) topic had nothing to read — yields an empty
// result. Callers assert on len, so a missing expected record surfaces as a
// clear count mismatch rather than being masked.
func fetchRecordsOnTopic(t *testing.T, cluster *kfake.Cluster, topic, group string) []*kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("consumer init error = %v", err)
	}
	defer consumer.Close()

	fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetches := consumer.PollFetches(fetchCtx)
	if ferr := fetches.Err(); ferr != nil &&
		!errors.Is(ferr, context.DeadlineExceeded) && !errors.Is(ferr, context.Canceled) {
		t.Fatalf("PollFetches(%q) error = %v", topic, ferr)
	}

	var records []*kgo.Record

	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})

	return records
}

func emitBillingEvent(t *testing.T, emitter streaming.Emitter) {
	t.Helper()

	payload := billing.MustMarshal(billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionID: "sub_123",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, streaming.EmitRequest{
		DefinitionKey: billing.Definition().Key,
		TenantID:      "tenant-1",
		Subject:       "billing-1",
		Payload:       payload,
	}); err != nil {
		t.Fatalf("Emit(billing) error = %v", err)
	}
}

func emitDomainEvent(t *testing.T, emitter streaming.Emitter) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Subject:       "tx-1",
		Payload:       []byte(`{"amount":100}`),
	}); err != nil {
		t.Fatalf("Emit(domain) error = %v", err)
	}
}

// buildWithOverrides constructs a Builder-backed emitter with two domain routes
// plus the billing route supplied through RouteOverrides.
func buildWithOverrides(t *testing.T, target streaming.TargetConfig) streaming.Emitter {
	t.Helper()

	emitter, err := streaming.NewBuilder().
		Source("//route-overrides-e2e").
		Catalog(overrideE2ECatalog(t)).
		Routes(
			overrideDomainRoute("transaction.created", "transaction-created.kafka.primary", "lerian.streaming.transaction.created"),
			overrideDomainRoute("account.opened", "account-opened.kafka.primary", "lerian.streaming.account.opened"),
		).
		RouteOverrides(billing.Route()).
		Target(target).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	return emitter
}

// TestRouteOverrides_MergesWithAutoGenerated verifies a billing override route
// lands a billing event on the fixed billing.Topic (decoded and field-checked,
// so "1 record" proves it is the RIGHT record) and not on any domain topic.
func TestRouteOverrides_MergesWithAutoGenerated(t *testing.T) {
	target, cluster := overrideKfakeTarget(t)

	emitter := buildWithOverrides(t, target)
	emitBillingEvent(t, emitter)

	records := fetchRecordsOnTopic(t, cluster, billing.Topic, "e2e-billing-lands")
	if len(records) != 1 {
		t.Fatalf("records on billing.Topic %q = %d; want 1", billing.Topic, len(records))
	}

	var got billing.BillablePayload
	if err := json.Unmarshal(records[0].Value, &got); err != nil {
		t.Fatalf("decode billing record value = %v", err)
	}

	if got.Metric != "api_calls" || got.SubscriptionID != "sub_123" {
		t.Errorf("billing record = {Metric:%q SubscriptionID:%q}; want {api_calls sub_123}", got.Metric, got.SubscriptionID)
	}

	if leaked := fetchRecordsOnTopic(t, cluster, "lerian.streaming.transaction.created", "e2e-billing-not-on-domain"); len(leaked) != 0 {
		t.Fatalf("billing event leaked onto domain topic: got %d records; want 0", len(leaked))
	}
}

// TestRouteOverrides_DomainEventsUnaffected verifies a domain event still lands
// on its configured route Destination topic and NOT on the billing topic when a
// billing override is present.
func TestRouteOverrides_DomainEventsUnaffected(t *testing.T) {
	target, cluster := overrideKfakeTarget(t)

	emitter := buildWithOverrides(t, target)
	emitDomainEvent(t, emitter)

	records := fetchRecordsOnTopic(t, cluster, "lerian.streaming.transaction.created", "e2e-domain-lands")
	if len(records) != 1 {
		t.Fatalf("records on domain topic = %d; want 1", len(records))
	}

	if string(records[0].Value) != `{"amount":100}` {
		t.Errorf("domain record value = %q; want %q", string(records[0].Value), `{"amount":100}`)
	}

	if leaked := fetchRecordsOnTopic(t, cluster, billing.Topic, "e2e-domain-not-on-billing"); len(leaked) != 0 {
		t.Fatalf("domain event leaked onto billing.Topic: got %d records; want 0", len(leaked))
	}
}

// TestRouteOverrides_ReplacesSameDefinitionKey pins the BLOCKER-1 fix at the
// Builder level: an override sharing a base route's DefinitionKey REPLACES it,
// so the event lands ONLY on the override topic — not double-published to both
// the base and the override destination.
func TestRouteOverrides_ReplacesSameDefinitionKey(t *testing.T) {
	target, cluster := overrideKfakeTarget(t)

	emitter, err := streaming.NewBuilder().
		Source("//route-overrides-replace").
		Catalog(builderCatalog(t)).
		// Base route sends transaction.created to the .created topic...
		Routes(overrideDomainRoute("transaction.created", "transaction-created.kafka.primary", "lerian.streaming.transaction.created")).
		// ...but an override for the SAME DefinitionKey (different route Key)
		// redirects it to the override topic.
		RouteOverrides(overrideDomainRoute("transaction.created", "transaction-created.kafka.override", overrideDomainTopic)).
		Target(target).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	emitDomainEvent(t, emitter)

	if records := fetchRecordsOnTopic(t, cluster, overrideDomainTopic, "e2e-replace-override"); len(records) != 1 {
		t.Fatalf("records on override topic = %d; want 1 (override replaces base route)", len(records))
	}

	if base := fetchRecordsOnTopic(t, cluster, "lerian.streaming.transaction.created", "e2e-replace-base"); len(base) != 0 {
		t.Fatalf("event double-published to the base topic: got %d records; want 0 (override must REPLACE)", len(base))
	}
}

// TestRouteOverrides_NilReceiver is a black-box restatement of the nil-receiver
// contract (the white-box field-level assertion lives in
// TestBuilder_RouteOverrides_NilReceiverIsSafe from Task 1.2.1). Kept light and
// non-duplicative: it pins the public-API guarantee that a nil Builder is safe.
func TestRouteOverrides_NilReceiver(t *testing.T) {
	t.Parallel()

	var b *streaming.Builder

	if got := b.RouteOverrides(billing.Route()); got != nil {
		t.Errorf("RouteOverrides(nil-receiver) = %v; want nil", got)
	}
}

// TestRouteOverrides_EmptySlice verifies that calling RouteOverrides with no
// arguments is a no-op: Build succeeds on the base routes and a domain event
// still lands normally.
func TestRouteOverrides_EmptySlice(t *testing.T) {
	target, cluster := overrideKfakeTarget(t)

	emitter, err := streaming.NewBuilder().
		Source("//route-overrides-empty").
		Catalog(builderCatalog(t)).
		Routes(overrideDomainRoute("transaction.created", "transaction-created.kafka.primary", "lerian.streaming.transaction.created")).
		RouteOverrides().
		Target(target).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	emitDomainEvent(t, emitter)

	if records := fetchRecordsOnTopic(t, cluster, "lerian.streaming.transaction.created", "e2e-empty-overrides"); len(records) != 1 {
		t.Fatalf("records on domain topic with empty overrides = %d; want 1", len(records))
	}
}
