//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/v2/log"
)

// TestResolveEvent_NilReceiverReturnsZero: nil *Producer must return a zero
// resolvedEvent plus ErrNilProducer rather than panicking. Matches the
// nil-safety contract on Emit/Descriptor/Healthy.
func TestResolveEvent_NilReceiverReturnsZero(t *testing.T) {
	t.Parallel()

	var p *Producer

	got, err := p.resolveEventAllowDisabled(EmitRequest{})
	if !errors.Is(err, ErrNilProducer) {
		t.Fatalf("nil.resolveEvent err = %v; want ErrNilProducer", err)
	}

	// resolvedEvent holds scalar strings + an Event (json.RawMessage) so it
	// is not comparable; assert zeroness via the scalar fields instead.
	if got.Topic != "" || got.DefinitionKey != "" || got.Event.EventID != "" {
		t.Errorf("nil.resolveEvent result = %+v; want zero resolvedEvent", got)
	}
}

// TestResolveEvent_MissingSourceReturnsErrMissingSource: Config.validate
// rejects an empty CloudEventsSource at construction, so we build a real
// Producer and then zero the field to exercise the in-resolveEvent guard.
// This proves the function surfaces ErrMissingSource even in the defensive
// case where validation somehow did not run.
// TestNewProducer_RejectsMissingOrMalformedSource pins that the ce-source gate
// is a CONSTRUCTION-time gate, not a per-Emit one.
//
// The source is the sole input to the application's topic, its DLQ name, and
// the manifest's advertised topic — and it is immutable once the Producer
// exists. Validating it once at construction is what makes "validate before
// you derive" true by construction instead of re-proving a fact about a
// constant on every Emit. A service with a malformed source now fails to
// start rather than failing on its first business event.
func TestNewProducer_RejectsMissingOrMalformedSource(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   error
	}{
		{"empty", "", ErrMissingSource},
		{"v2 uri shape", "//lerian.midaz/tx", ErrInvalidSource},
		{"capitalized", "Lender", ErrInvalidSource},
		{"dotted namespace", "lerian.midaz", ErrInvalidSource},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := kfakeConfig(t)
			cfg.CloudEventsSource = tc.source

			emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
			if err == nil {
				_ = emitter.Close()
				t.Fatalf("New(source=%q) = nil error; want %v at CONSTRUCTION", tc.source, tc.want)
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("New(source=%q) err = %v; want %v", tc.source, err, tc.want)
			}
		})
	}
}

// TestResolveEvent_ZeroTimestampGetsFallback: resolveEvent fills a zero
// Timestamp with time.Now().UTC(). Captured bounds around the call must
// contain the resolved timestamp, and the timezone must be UTC.
func TestResolveEvent_ZeroTimestampGetsFallback(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	req := sampleRequest()
	req.Timestamp = time.Time{}

	before := time.Now().UTC()
	resolved, err := p.resolveEventAllowDisabled(req)
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("resolveEvent err = %v; want nil", err)
	}

	if resolved.Event.Timestamp.IsZero() {
		t.Fatal("resolved.Event.Timestamp is zero; want fallback-populated")
	}

	if resolved.Event.Timestamp.Before(before) || resolved.Event.Timestamp.After(after) {
		t.Errorf("resolved.Event.Timestamp = %v; want in [%v, %v]", resolved.Event.Timestamp, before, after)
	}

	if loc := resolved.Event.Timestamp.Location(); loc != time.UTC {
		t.Errorf("resolved.Event.Timestamp.Location() = %v; want UTC", loc)
	}
}

// TestResolveEvent_SystemEventLegalWithoutTenantID: when the catalog marks a
// definition SystemEvent=true and the Producer was built with
// WithAllowSystemEvents, an empty TenantID is legal. The resolved event
// preserves the empty TenantID and produces a "system:..." partition key.
//
// Note: the dead defensive `if topic == ""` check at resolved_event.go:74
// is unreachable from this path — Event.Topic() only returns "" for a nil
// receiver, which cannot happen here. We do not exercise that branch.
func TestResolveEvent_SystemEventLegalWithoutTenantID(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	catalog, err := NewCatalog(EventDefinition{
		Key:          "ledger.rolled_over",
		ResourceType: "ledger",
		EventType:    "rolled_over",
		SystemEvent:  true,
	})
	if err != nil {
		t.Fatalf("NewCatalog err = %v", err)
	}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	resolved, err := p.resolveEventAllowDisabled(EmitRequest{
		DefinitionKey: "ledger.rolled_over",
		// TenantID intentionally empty.
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("resolveEvent err = %v; want nil", err)
	}

	if resolved.Event.TenantID != "" {
		t.Errorf("resolved.Event.TenantID = %q; want empty", resolved.Event.TenantID)
	}

	if !resolved.Event.SystemEvent {
		t.Error("resolved.Event.SystemEvent = false; want true")
	}

	partitionKey := resolved.Event.PartitionKey()
	if !strings.HasPrefix(partitionKey, "system:") {
		t.Errorf("PartitionKey() = %q; want prefix system:", partitionKey)
	}
}

// TestResolveEvent_SystemEventDropsTenantID pins that a system event never
// carries ce-tenantid, no matter what the caller passed.
//
// The contract says so in two places — Event.SystemEvent's godoc ("omits
// ce-tenantid from headers") and the header builder, which emits ce-tenantid
// whenever TenantID is non-empty. Those two disagreed: a system event with a
// TenantID set shipped the header anyway, so a platform-level event arrived
// looking tenant-scoped and any consumer filtering on ce-tenantid would route
// it into one tenant's processing.
func TestResolveEvent_SystemEventDropsTenantID(t *testing.T) {
	t.Parallel()

	cfg, _ := kfakeConfig(t)

	catalog, err := NewCatalog(EventDefinition{
		Key:          "ledger.rolled_over",
		ResourceType: "ledger",
		EventType:    "rolled_over",
		SystemEvent:  true,
	})
	if err != nil {
		t.Fatalf("NewCatalog err = %v", err)
	}

	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
		WithAllowSystemEvents(),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	resolved, err := p.resolveEventAllowDisabled(EmitRequest{
		DefinitionKey: "ledger.rolled_over",
		TenantID:      "t-should-be-dropped",
		Payload:       []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("resolveEvent err = %v", err)
	}

	if resolved.Event.TenantID != "" {
		t.Fatalf("system event TenantID = %q; want empty", resolved.Event.TenantID)
	}

	for _, h := range buildTransportHeaders(context.Background(), resolved.Event) {
		if h.Key == "ce-tenantid" {
			t.Fatalf("system event emitted ce-tenantid = %q; the contract omits it", string(h.Value))
		}
	}

	if got := resolved.Event.PartitionKey(); got != "system:"+resolved.Event.EventType {
		t.Errorf("system PartitionKey() = %q; want the system prefix", got)
	}
}
