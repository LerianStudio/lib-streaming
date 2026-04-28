//go:build unit

package streaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

func TestProducer_Emit_OutboxAlwaysPolicySkipsDirectPublish(t *testing.T) {
	cfg, cluster := kfakeConfig(t)
	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	request := sampleRequest()
	request.PolicyOverride = DeliveryPolicyOverride{
		Direct: DirectModeSkip,
		Outbox: OutboxModeAlways,
	}

	if err := emitter.Emit(context.Background(), request); err != nil {
		t.Fatalf("Emit err = %v; want nil", err)
	}

	if got := fakeRepo.createdCount(); got != 1 {
		t.Fatalf("outbox created count = %d; want 1", got)
	}

	row := fakeRepo.firstCreated()
	if row.EventType != StreamingOutboxEventType {
		t.Fatalf("row.EventType = %q; want %q", row.EventType, StreamingOutboxEventType)
	}

	consumer := newConsumer(t, cluster, "lerian.streaming.transaction.created")
	fetchCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	fetches := consumer.PollFetches(fetchCtx)
	var produced int
	fetches.EachRecord(func(_ *kgo.Record) { produced++ })
	if produced != 0 {
		t.Fatalf("direct records = %d; want 0", produced)
	}
}

func TestProducer_Emit_CallPolicyCanDisableEvent(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	enabled := false
	request := sampleRequest()
	request.PolicyOverride = DeliveryPolicyOverride{Enabled: &enabled}

	if err := emitter.Emit(context.Background(), request); !errors.Is(err, ErrEventDisabled) {
		t.Fatalf("Emit err = %v; want ErrEventDisabled", err)
	}
}

func TestProducer_Emit_OutboxNeverPolicyReturnsCircuitOpenWhenBreakerOpen(t *testing.T) {
	cfg, _ := kfakeConfig(t)
	fakeMgr := newFakeCBManager()
	fakeRepo := &fakeOutboxRepo{}

	emitter, err := New(
		context.Background(),
		cfg,
		WithLogger(log.NewNop()),
		WithCatalog(sampleCatalog(t)),
		WithCircuitBreakerManager(fakeMgr),
		WithOutboxRepository(fakeRepo),
	)
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)
	fakeMgr.ForceTransition(p.cbServiceName, circuitbreaker.StateOpen)

	request := sampleRequest()
	request.PolicyOverride = DeliveryPolicyOverride{
		Outbox: OutboxModeNever,
	}

	err = emitter.Emit(context.Background(), request)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Emit err = %v; want ErrCircuitOpen", err)
	}
	if got := fakeRepo.createdCount(); got != 0 {
		t.Fatalf("outbox created count = %d; want 0", got)
	}
}

func TestProducer_Emit_DLQNeverPolicySkipsDLQRoute(t *testing.T) {
	cfg, cluster := kfakeDLQConfig(t)
	sourceTopic := "lerian.streaming.transaction.created"
	injectProduceError(cluster, sourceTopic, kerr.MessageTooLarge.Code)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	request := sampleRequest()
	request.PolicyOverride = DeliveryPolicyOverride{DLQ: DLQModeNever}

	err = emitter.Emit(context.Background(), request)
	if err == nil {
		t.Fatal("Emit err = nil; want source publish error")
	}

	if record := readDLQRecord(t, cluster, dlqTopic(sourceTopic), 500*time.Millisecond); record != nil {
		t.Fatalf("DLQ record was written despite dlq=never")
	}
}
