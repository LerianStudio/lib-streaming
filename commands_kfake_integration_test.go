//go:build integration

package streaming_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/v2/log"
	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// This file drives the COMMANDS QUEUE end to end against a real broker
// protocol: a producer with a mixed catalog splits facts from commands across
// two topics, and a consumer subscribed to both applies opposite unmatched
// verdicts to each.
//
// It is the consignado rail in miniature. lender publishes facts the gateway
// may ignore and commands the gateway must act on, all from one catalog. The
// case that matters is the third record: a command key the gateway has NO
// handler for. Under fact semantics it would be skipped and committed forever
// — money-path work lost with green dashboards on both sides. Here it must land
// in the gateway's own DLQ tagged unhandled_key, while an unmatched FACT on the
// sibling stream is still ignored.

const (
	cmdProducerApp   = "lender"
	cmdFactTopic     = "lerian.streaming.lender"
	cmdCommandsTopic = "lerian.streaming.lender.commands"
	cmdProducerDLQ   = "lerian.streaming.lender.dlq"

	// The CONSUMING application and its OWN dead-letter topic. An unhandled
	// command quarantines HERE, never on the producer's DLQ — consuming does
	// not widen an application's write grant.
	cmdConsumerApp = "br-consignado-gw"
	cmdConsumerDLQ = "lerian.streaming.br-consignado-gw.dlq"

	cmdGroup       = "commands-kfake"
	cmdWaitBudget  = 20 * time.Second
	cmdFactKey     = "loan.disbursed"
	cmdHandledKey  = "margin.reserve"
	cmdOrphanedKey = "margin.release"
	cmdOrphanFact  = "audit.logged"
)

// commandsCluster seeds every topic the split touches: the producer's fact
// topic and commands queue, the producer's DLQ, and the consumer's own DLQ.
//
// There is deliberately no "lerian.streaming.lender.commands.dlq" — the
// commands queue has no DLQ of its own, and a test that seeded one would hide
// a regression that started writing to it.
func commandsCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(1),
		kfake.SeedTopics(1, cmdFactTopic, cmdCommandsTopic, cmdProducerDLQ, cmdConsumerDLQ),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// commandsCatalog is the mixed catalog: one fact, two commands, one producer.
// The classes are the only thing that separates their destinations.
func commandsCatalog(t *testing.T) streaming.Catalog {
	t.Helper()

	catalog, err := streaming.NewCatalog(
		streaming.EventDefinition{
			Key:          cmdFactKey,
			ResourceType: "loan",
			EventType:    "disbursed",
		},
		streaming.EventDefinition{
			Key:          cmdOrphanFact,
			ResourceType: "audit",
			EventType:    "logged",
		},
		streaming.EventDefinition{
			Key:          cmdHandledKey,
			ResourceType: "margin",
			EventType:    "reserve",
			Class:        streaming.ClassCommand,
		},
		streaming.EventDefinition{
			Key:          cmdOrphanedKey,
			ResourceType: "margin",
			EventType:    "release",
			Class:        streaming.ClassCommand,
		},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

// commandsEmitter builds a real producer over kfake with ONE catch-all Kafka
// route to the app topic — the shape a service actually wires. Every
// destination split below therefore comes from the catalog, not the routes.
func commandsEmitter(t *testing.T, cluster *kfake.Cluster) streaming.Emitter {
	t.Helper()

	appTopic, err := streaming.AppTopic(cmdProducerApp)
	if err != nil {
		t.Fatalf("AppTopic() error = %v", err)
	}

	emitter, err := streaming.NewBuilder().
		Source(cmdProducerApp).
		Catalog(commandsCatalog(t)).
		Routes(streaming.RouteDefinition{
			Key:         "primary.all",
			Target:      "primary",
			Destination: streaming.KafkaTopic(appTopic),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  cluster.ListenAddrs(),
			ClientID: "commands-kfake",
		}).
		Logger(log.NewNop()).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	return emitter
}

// TestIntegration_CommandsSplitAcrossTopics pins the producer half: one
// catalog, one route, two destinations. Facts land on the app topic and
// commands on the ".commands" queue, and nothing lands on a ".commands.dlq"
// that does not exist.
func TestIntegration_CommandsSplitAcrossTopics(t *testing.T) {
	cluster := commandsCluster(t)
	emitter := commandsEmitter(t, cluster)

	for _, key := range []string{cmdFactKey, cmdHandledKey, cmdOrphanedKey} {
		if err := emitter.Emit(context.Background(), streaming.EmitRequest{
			DefinitionKey: key,
			TenantID:      "tenant-abc",
			Payload:       []byte(`{"amount":"1200.00"}`),
		}); err != nil {
			t.Fatalf("Emit(%s) error = %v", key, err)
		}
	}

	facts := commandsAwaitRecords(t, cluster, cmdFactTopic, 1, cmdWaitBudget)
	if len(facts) != 1 {
		t.Fatalf("fact topic carried %d records; want exactly the one fact", len(facts))
	}

	commands := commandsAwaitRecords(t, cluster, cmdCommandsTopic, 2, cmdWaitBudget)
	if len(commands) != 2 {
		t.Fatalf("commands queue carried %d records; want the two commands", len(commands))
	}

	if got := commandsHeader(facts[0], "ce-eventtype"); got != "disbursed" {
		t.Errorf("fact topic carried ce-eventtype %q; want disbursed", got)
	}

	for _, rec := range commands {
		if got := commandsHeader(rec, "ce-resourcetype"); got != "margin" {
			t.Errorf("commands queue carried ce-resourcetype %q; want margin", got)
		}
	}

	// The wire record is byte-identical either way: the QUEUE is the class.
	// A ce-class header would make the classification a runtime string every
	// consumer has to trust, instead of a subscription-time, ACL-visible fact.
	for _, h := range commands[0].Headers {
		if h.Key == "ce-class" {
			t.Errorf("command record carries a ce-class header (%q); the queue is the class, not a header", string(h.Value))
		}
	}
}

// TestIntegration_UnhandledCommandQuarantinesWhileFactIsIgnored is the
// load-bearing case, and the reason the queue exists.
//
// One consumer, two subscriptions, one poll loop. A command it handles runs. A
// command it does NOT handle quarantines to its own DLQ tagged unhandled_key.
// A fact it does not handle is skipped and committed as before. Same consumer,
// same unmatched condition, opposite verdicts — decided by the topic the record
// arrived on.
func TestIntegration_UnhandledCommandQuarantinesWhileFactIsIgnored(t *testing.T) {
	cluster := commandsCluster(t)
	emitter := commandsEmitter(t, cluster)

	handled := make(chan struct{})

	var once sync.Once

	consumer, err := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group(cmdGroup).
		Source(cmdConsumerApp).
		Apps(cmdProducerApp).     // lender's FACTS: lenient
		Commands(cmdProducerApp). // lender's COMMANDS: strict
		OnFrom(cmdProducerApp, "margin.reserve", func(context.Context, streaming.Event, []byte) error {
			once.Do(func() { close(handled) })
			return nil
		}).
		// Deliberately NO handler for margin.release (a command) and none for
		// audit.logged (a fact). Those two are the experiment.
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})

	var producers sync.WaitGroup

	commandsTrickle(t, emitter, stop, &producers)

	runDone := make(chan error, 1)

	go func() { runDone <- consumer.Run(ctx) }()

	select {
	case <-handled:
	case <-time.After(cmdWaitBudget):
		close(stop)
		producers.Wait()
		cancel()
		_ = consumer.Close()
		t.Fatal("the registered command handler never ran")
	}

	quarantined := commandsAwaitRecords(t, cluster, cmdConsumerDLQ, 1, cmdWaitBudget)

	close(stop)
	producers.Wait()
	cancel()

	_ = consumer.Close()
	<-runDone

	if len(quarantined) == 0 {
		t.Fatal("no record was quarantined; an unhandled COMMAND must never be skipped and committed")
	}

	sawUnhandledCommand := false

	for _, rec := range quarantined {
		sourceTopic := commandsHeader(rec, "x-lerian-dlq-source-topic")

		// An unmatched FACT must never reach the DLQ. A fact stream carries
		// everything its producer emits and a consumer handles a handful;
		// quarantining the rest would fail-closed the whole sibling stream.
		if sourceTopic == cmdFactTopic {
			t.Errorf("a record from the FACT topic was quarantined (cause %q); unmatched facts must stay ignored",
				commandsHeader(rec, "x-lerian-dlq-cause-kind"))

			continue
		}

		if sourceTopic != cmdCommandsTopic {
			continue
		}

		if got := commandsHeader(rec, "x-lerian-dlq-cause-kind"); got != "unhandled_key" {
			t.Errorf("x-lerian-dlq-cause-kind = %q; want unhandled_key", got)

			continue
		}

		if got := commandsHeader(rec, "ce-eventtype"); got != "release" {
			t.Errorf("quarantined command ce-eventtype = %q; want release (the key with no handler)", got)
		}

		sawUnhandledCommand = true
	}

	if !sawUnhandledCommand {
		t.Error("no unhandled_key quarantine from the commands queue; the strict verdict did not fire")
	}

	// The producer's DLQ must be untouched: the consumer quarantines into its
	// OWN, which is what keeps a consuming app's write grant from widening.
	if got := commandsPeekRecords(t, cluster, cmdProducerDLQ, 2*time.Second); len(got) != 0 {
		t.Errorf("producer DLQ carried %d records; a consumer must quarantine into its own DLQ", len(got))
	}
}

// commandsTrickle emits the fact, the handled command, the unhandled command,
// and the unhandled fact on a loop until stop closes.
//
// A steady trickle removes kfake's non-deterministic fresh-group join latency
// as a flake source: whenever the group stabilizes, records are waiting at or
// after the cursor.
func commandsTrickle(t *testing.T, emitter streaming.Emitter, stop <-chan struct{}, wg *sync.WaitGroup) {
	t.Helper()

	wg.Go(func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			for _, key := range []string{cmdFactKey, cmdOrphanFact, cmdHandledKey, cmdOrphanedKey} {
				_ = emitter.Emit(context.Background(), streaming.EmitRequest{
					DefinitionKey: key,
					TenantID:      "tenant-abc",
					Payload:       []byte(`{"amount":"1200.00"}`),
				})
			}

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	})
}

// commandsAwaitRecords polls topic from the beginning until at least want
// records are visible or the budget expires, returning everything seen.
func commandsAwaitRecords(t *testing.T, cluster *kfake.Cluster, topic string, want int, budget time.Duration) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("reader init for %s err = %v", topic, err)
	}

	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	var out []*kgo.Record

	for len(out) < want && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(rec *kgo.Record) { out = append(out, rec) })
	}

	return out
}

// commandsPeekRecords drains topic for a bounded window and returns whatever is
// there — used to assert a topic stayed EMPTY, where waiting for a count would
// always time out.
func commandsPeekRecords(t *testing.T, cluster *kfake.Cluster, topic string, budget time.Duration) []*kgo.Record {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("reader init for %s err = %v", topic, err)
	}

	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	var out []*kgo.Record

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(rec *kgo.Record) { out = append(out, rec) })
	}

	return out
}

// commandsHeader returns the first value for key, or "".
func commandsHeader(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}
