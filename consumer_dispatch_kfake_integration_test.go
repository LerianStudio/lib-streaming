//go:build integration

package streaming_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/LerianStudio/lib-observability/v2/log"
	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// This file drives the WHOLE consumer contract through the PUBLIC surface
// against a real broker protocol implementation, with LITERAL ce-* headers
// written by hand.
//
// It exists because of a drift class the typed tests structurally cannot catch:
// every unit test builds headers with the same constants the producer writes
// them with, so renaming ce-resourcetype renames both sides at once and nothing
// fails — mutation testing proved exactly that. Here the record is assembled
// from string literals, exactly as a foreign producer or an operator's replay
// script would write it, and the assertion is that the registered handler RAN.
//
// The negative case is the same record with a foreign ce-source: it must be
// quarantined to the DLQ with the source-mismatch cause kind, never dispatched.

const (
	dispatchApp        = "lender"
	dispatchTopic      = "lerian.streaming.lender"
	dispatchDLQTopic   = "lerian.streaming.lender.dlq"
	dispatchGroup      = "consumer-dispatch-kfake"
	dispatchEventKey   = "loan_contract.disbursed"
	dispatchWaitBudget = 20 * time.Second
)

// dispatchCluster spins up a kfake cluster seeded with the app topic and its
// DLQ sibling. One partition keeps polling deterministic.
func dispatchCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(1),
		kfake.SeedTopics(1, dispatchTopic, dispatchDLQTopic),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// literalCEHeaders writes the CloudEvents binary-mode headers with STRING
// LITERALS. Not the headerCE* constants, not BuildCloudEventsHeaders — the
// whole point is that this side of the wire is independent of the producer's
// spelling, so a rename breaks the test instead of hiding in it.
func literalCEHeaders(source string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("evt-kfake-1")},
		{Key: "ce-source", Value: []byte(source)},
		{Key: "ce-type", Value: []byte("studio.lerian." + source + ".loan_contract.disbursed")},
		{Key: "ce-time", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		{Key: "ce-schemaversion", Value: []byte("1.0.0")},
		{Key: "ce-resourcetype", Value: []byte("loan_contract")},
		{Key: "ce-eventtype", Value: []byte("disbursed")},
		{Key: "ce-tenantid", Value: []byte("tenant-abc")},
	}
}

// keepProducingLiteral trickles a literal-header record onto the app topic
// until stop closes. A steady trickle removes kfake's non-deterministic
// fresh-group join latency as a flake source: whenever the group stabilizes, a
// record is waiting at or after the cursor.
//
// It returns a reporter for the FIRST produce failure, valid once the caller
// has closed stop and waited on wg (cl.Close flushes, so every callback has
// fired by then). Discarding produce errors made a broker that never accepted a
// single record surface as "handler never ran" — a consumer bug for a fixture
// fault, in the one test whose whole job is to tell those apart.
func keepProducingLiteral(t *testing.T, cluster *kfake.Cluster, source string, payload []byte, stop <-chan struct{}, wg *sync.WaitGroup) func() error {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("producer client init err = %v", err)
	}

	var (
		mu       sync.Mutex
		firstErr error
	)

	onProduce := func(_ *kgo.Record, err error) {
		if err == nil {
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if firstErr == nil {
			firstErr = err
		}
	}

	wg.Go(func() {
		defer cl.Close()

		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			cl.Produce(context.Background(), &kgo.Record{
				Topic:   dispatchTopic,
				Key:     []byte("tenant-abc"),
				Headers: literalCEHeaders(source),
				Value:   payload,
			}, onProduce)

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})

	return func() error {
		mu.Lock()
		defer mu.Unlock()

		return firstErr
	}
}

// TestIntegration_ConsumerDispatchKfakeRoundTrip produces a record
// with literal ce-headers, subscribes with Apps + On through the public
// builder, and asserts the handler ran with the payload and that the offset was
// committed.
func TestIntegration_ConsumerDispatchKfakeRoundTrip(t *testing.T) {
	cluster := dispatchCluster(t)

	payload := []byte(`{"amount":"1200.00"}`)

	var (
		mu       sync.Mutex
		gotEvent streaming.Event
		gotBody  []byte
	)

	handled := make(chan struct{})

	var once sync.Once

	consumer, err := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group(dispatchGroup).
		Source("test-consumer").
		Apps(dispatchApp).
		On(dispatchEventKey, func(_ context.Context, ev streaming.Event, body []byte) error {
			mu.Lock()
			gotEvent = ev
			gotBody = append([]byte(nil), body...)
			mu.Unlock()

			once.Do(func() { close(handled) })

			return nil
		}).
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	produceErr := keepProducingLiteral(t, cluster, dispatchApp, payload, stop, &producers)

	runDone := make(chan error, 1)

	go func() { runDone <- consumer.Run(ctx) }()

	select {
	case <-handled:
	case <-time.After(dispatchWaitBudget):
		close(stop)
		producers.Wait()
		cancel()
		_ = consumer.Close()

		if err := produceErr(); err != nil {
			t.Fatalf("no record was ever produced: %v", err)
		}

		t.Fatal("handler never ran: a literal-header record on the app topic was not dispatched by event key")
	}

	// The group's committed offset must advance past the handled record — that
	// is what makes this at-least-once rather than at-most-once.
	//
	// Read it while the consumer is STILL RUNNING. Commits are staged during a
	// poll cycle and flushed at its end, so the handler firing does not mean
	// the commit has landed yet; asserting immediately after Close raced that
	// flush and read -1.
	committed := awaitCommittedOffset(t, cluster, dispatchGroup, dispatchTopic, dispatchWaitBudget)

	close(stop)
	producers.Wait()
	cancel()

	if err := produceErr(); err != nil {
		t.Errorf("produce failed: %v", err)
	}

	if err := consumer.Close(); err != nil {
		t.Errorf("Close() = %v; want nil", err)
	}

	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run() = %v; want nil or context.Canceled", err)
		}
	case <-time.After(dispatchWaitBudget):
		t.Fatal("Run did not return after Close")
	}

	mu.Lock()
	defer mu.Unlock()

	if gotEvent.Source != dispatchApp {
		t.Errorf("event.Source = %q; want %q", gotEvent.Source, dispatchApp)
	}

	if gotEvent.ResourceType != "loan_contract" || gotEvent.EventType != "disbursed" {
		t.Errorf("event key parts = %q/%q; want loan_contract/disbursed", gotEvent.ResourceType, gotEvent.EventType)
	}

	if gotEvent.TenantID != "tenant-abc" {
		t.Errorf("event.TenantID = %q; want tenant-abc (from ce-tenantid, never the payload)", gotEvent.TenantID)
	}

	if string(gotBody) != string(payload) {
		t.Errorf("payload = %q; want %q (verbatim)", string(gotBody), string(payload))
	}

	if committed <= 0 {
		t.Errorf("committed offset = %d; want > 0 (the handled record must be committed)", committed)
	}
}

// awaitCommittedOffset polls the group's committed offset for partition 0 until
// it is positive or the budget expires, returning the last value seen.
//
// The retry is the point: the runtime stages a commit watermark during a poll
// cycle and flushes it at the end of that cycle, so a handler that has just
// returned is not yet a committed offset. A single immediate read races the
// flush and observes -1.
func awaitCommittedOffset(t *testing.T, cluster *kfake.Cluster, group, topic string, budget time.Duration) int64 {
	t.Helper()

	deadline := time.Now().Add(budget)

	var last int64 = -1

	for time.Now().Before(deadline) {
		last = committedOffset(t, cluster, group, topic)
		if last > 0 {
			return last
		}

		time.Sleep(100 * time.Millisecond)
	}

	return last
}

// TestIntegration_ConsumerDispatchForeignSourceQuarantines is the negative case: the
// same literal record carrying a foreign ce-source must never reach a handler,
// and must land on the DLQ tagged source_mismatch.
func TestIntegration_ConsumerDispatchForeignSourceQuarantines(t *testing.T) {
	cluster := dispatchCluster(t)

	ran := make(chan struct{}, 1)

	consumer, err := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group(dispatchGroup+"-foreign").
		Apps(dispatchApp).
		On(dispatchEventKey, func(context.Context, streaming.Event, []byte) error {
			select {
			case ran <- struct{}{}:
			default:
			}

			return nil
		}).
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	// "matcher" is a legal source — just not one this consumer subscribed to.
	produceErr := keepProducingLiteral(t, cluster, "matcher", []byte(`{"foreign":true}`), stop, &producers)

	runDone := make(chan error, 1)

	go func() { runDone <- consumer.Run(ctx) }()

	quarantined := awaitDLQRecord(t, cluster, dispatchDLQTopic, dispatchWaitBudget)

	close(stop)
	producers.Wait()
	cancel()

	if err := produceErr(); err != nil {
		t.Errorf("produce failed: %v", err)
	}

	_ = consumer.Close()
	<-runDone

	select {
	case <-ran:
		t.Fatal("handler ran for a foreign ce-source; verification must quarantine before dispatch")
	default:
	}

	headers := map[string]string{}
	for _, h := range quarantined.Headers {
		headers[h.Key] = string(h.Value)
	}

	if got := headers["x-lerian-dlq-cause-kind"]; got != "source_mismatch" {
		t.Errorf("x-lerian-dlq-cause-kind = %q; want source_mismatch", got)
	}

	if got := headers["x-lerian-dlq-source-topic"]; got != dispatchTopic {
		t.Errorf("x-lerian-dlq-source-topic = %q; want %q", got, dispatchTopic)
	}

	if got := headers["ce-source"]; got != "matcher" {
		t.Errorf("quarantined record lost its original ce-source: %q", got)
	}
}

// awaitDLQRecord polls the DLQ topic from the beginning until one record shows
// up or the budget expires.
func awaitDLQRecord(t *testing.T, cluster *kfake.Cluster, topic string, budget time.Duration) *kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("DLQ reader init err = %v", err)
	}

	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("no record landed on the DLQ within the budget; a foreign-source record must quarantine, never vanish")
		}

		var found *kgo.Record

		fetches.EachRecord(func(rec *kgo.Record) {
			if found == nil {
				found = rec
			}
		})

		if found != nil {
			return found
		}
	}
}

// committedOffset reads the group's committed offset for a topic's partition 0.
func committedOffset(t *testing.T, cluster *kfake.Cluster, group, topic string) int64 {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("offset reader init err = %v", err)
	}

	defer cl.Close()

	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group

	reqTopic := kmsg.NewOffsetFetchRequestTopic()
	reqTopic.Topic = topic
	reqTopic.Partitions = []int32{0}
	req.Topics = append(req.Topics, reqTopic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("OffsetFetch err = %v", err)
	}

	for _, rt := range resp.Topics {
		for _, rp := range rt.Partitions {
			if rt.Topic == topic && rp.Partition == 0 {
				return rp.Offset
			}
		}
	}

	return -1
}
