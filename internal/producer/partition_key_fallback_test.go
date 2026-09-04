//go:build unit

package producer

import (
	"context"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

// TestResolvePartitionKey_FallsBackWhenOverrideReturnsEmpty pins the guard on
// the WithPartitionKey seam.
//
// An override that returns "" is not "no key": franz-go's sticky-key
// partitioner branches on key != nil, and []byte("") is not nil, so every such
// record hashes to murmur2 of a constant and pins the whole stream to ONE
// partition. That is a silent throughput cliff and a silent ordering change —
// the exact failure the Subject/EventID rungs of Event.PartitionKey() exist to
// prevent, reintroduced through the override.
func TestResolvePartitionKey_FallsBackWhenOverrideReturnsEmpty(t *testing.T) {
	t.Parallel()

	event := sampleEvent() // TenantID "t-abc" -> PartitionKey() == "t-abc"

	tests := []struct {
		name   string
		partFn func(Event) string
		want   string
	}{
		{"no override", nil, "t-abc"},
		{"override returns a key", func(Event) string { return "custom" }, "custom"},
		{"override returns empty", func(Event) string { return "" }, "t-abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &Producer{partFn: tt.partFn}

			if got := p.resolvePartitionKey(event); got != tt.want {
				t.Errorf("resolvePartitionKey() = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestEmitMulti_EmptyPartitionKeyOverrideDoesNotReachTheAdapter drives the same
// guard end to end: the record handed to the transport carries the event's own
// key, not the empty string the override produced.
func TestEmitMulti_EmptyPartitionKeyOverrideDoesNotReachTheAdapter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary",
			"lerian.streaming.transaction.created", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc-multi-test"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: primary}},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithPartitionKey(func(Event) string { return "" }),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	if err := p.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	messages := primary.Messages()
	if len(messages) != 1 {
		t.Fatalf("published = %d; want 1", len(messages))
	}

	if got := messages[0].Key; got != "t-abc" {
		t.Errorf("published partition key = %q; want the event's own key %q", got, "t-abc")
	}
}

// TestDeriveOutboxAggregateID_FallsBackWhenOverrideReturnsEmpty pins the same
// collapse on the outbox side: an empty override would hash EVERY row of every
// tenant onto one aggregate id, destroying the per-aggregate correlation the
// deterministic derivation exists for.
func TestDeriveOutboxAggregateID_FallsBackWhenOverrideReturnsEmpty(t *testing.T) {
	t.Parallel()

	first := sampleEvent()

	second := sampleEvent()
	second.TenantID = "t-xyz"

	p := &Producer{partFn: func(Event) string { return "" }}

	if got, want := mustAggregateID(p, first), defaultAggregateID(first); got != want {
		t.Errorf("deriveOutboxAggregateID(first) = %s; want the un-overridden %s", got, want)
	}

	if mustAggregateID(p, first) == mustAggregateID(p, second) {
		t.Error("two tenants collapsed onto one aggregate id; the empty override was not guarded")
	}
}
