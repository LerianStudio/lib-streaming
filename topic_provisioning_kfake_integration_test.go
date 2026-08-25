//go:build integration

package streaming_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-observability/v2/log"
	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// This file drives AUTOMATIC TOPIC PROVISIONING against a real broker protocol.
//
// The cluster below seeds NOTHING and has broker-side auto-topic-creation OFF —
// the same hardening every Lerian broker runs (auto_create_topics_enabled=false).
// So a topic existing after Build can only have come from the library's own
// CreateTopics call. That is the whole point: before this, a producer initialized
// cleanly and its FIRST PUBLISH failed, and a consumer on a nonexistent topic
// polled clean forever while consuming nothing.
//
// The ownership rule under test: a runtime ensures every topic it writes UNDER
// ITS OWN SOURCE NAMESPACE. A producer ensures its fact topic and its DLQ; a
// consumer ensures its own DLQ (it is that topic's producer) and its own commands
// queue when it is the app being commanded. Nobody provisions a name outside
// their own namespace, and a commands queue is never provisioned by its emitter.

const (
	provProducerApp      = "lender"
	provFactTopic        = "lerian.streaming.lender"
	provProducerDLQ      = "lerian.streaming.lender.dlq"
	provProducerCommands = "lerian.streaming.lender.commands"

	provConsumerApp      = "br-consignado-gw"
	provConsumerDLQ      = "lerian.streaming.br-consignado-gw.dlq"
	provConsumerCommands = "lerian.streaming.br-consignado-gw.commands"

	provForeignApp       = "midaz"
	provForeignFactTopic = "lerian.streaming.midaz"
	provForeignCommands  = "lerian.streaming.midaz.commands"

	provAutoProvisionFlag = "STREAMING_TOPIC_AUTO_PROVISION"
	provPartitionsFlag    = "STREAMING_TOPIC_PARTITIONS"
	provReplicationFlag   = "STREAMING_TOPIC_REPLICATION_FACTOR"

	provGroup = "topic-provisioning-kfake"
)

// pinProvisionEnv makes every test here hermetic against the host environment.
//
// All three provisioning variables are set explicitly, never inherited. Two
// concrete ways an inherited value breaks this file: an exported
// STREAMING_TOPIC_AUTO_PROVISION=false (a reasonable local setting) turns every
// positive assertion into a failure that reads as a library bug, and a
// STREAMING_TOPIC_REPLICATION_FACTOR above 1 makes kfake answer
// INVALID_REPLICATION_FACTOR — the single-broker cluster below cannot satisfy it,
// so the topic is never created and the failure looks like a provisioning bug
// rather than a test-environment one.
//
// Partitions and replication are pinned to 1 for the same reason: this cluster
// has exactly one broker, so 1/1 is the only combination guaranteed to succeed,
// and it removes the dependency on kfake's own defaulting behaviour.
//
// Called explicitly per test rather than from a build helper, so the
// provisioning-disabled test can pin "false" without a helper overriding it.
func pinProvisionEnv(t *testing.T, autoProvision string) {
	t.Helper()

	t.Setenv(provAutoProvisionFlag, autoProvision)
	t.Setenv(provPartitionsFlag, "1")
	t.Setenv(provReplicationFlag, "1")
}

// provisionCluster is a bare broker: no seeded topics, and NO
// AllowAutoTopicCreation. Any topic that exists after a Build was created by an
// explicit CreateTopics request from the library.
func provisionCluster(t *testing.T) *kfake.Cluster {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.DefaultNumPartitions(1),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster
}

// provisionedTopics lists the topics that actually exist on the broker, sorted.
func provisionedTopics(t *testing.T, cluster *kfake.Cluster) []string {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient err = %v", err)
	}

	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	details, err := kadm.NewClient(client).ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics err = %v", err)
	}

	names := make([]string, 0, len(details))
	for name := range details {
		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

func provisionCatalog(t *testing.T) streaming.Catalog {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "loan.disbursed",
		ResourceType: "loan",
		EventType:    "disbursed",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

// buildProvisioningProducer builds a producer exactly as a service wires one:
// one catch-all Kafka route to its own app topic.
func buildProvisioningProducer(t *testing.T, cluster *kfake.Cluster) streaming.Emitter {
	t.Helper()

	emitter, err := streaming.NewBuilder().
		Source(provProducerApp).
		Catalog(provisionCatalog(t)).
		Routes(streaming.RouteDefinition{
			Key:         "primary.all",
			Target:      "primary",
			Destination: streaming.KafkaTopic(provFactTopic),
			Requirement: streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  cluster.ListenAddrs(),
			ClientID: "topic-provisioning",
		}).
		Logger(log.NewNop()).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	return emitter
}

// TestIntegration_ProducerBuildProvisionsItsFactTopic is the br-consignado-gw
// incident, inverted: the producer initializes against a broker that does not
// have its topic, and the topic exists by the time Build returns. Its first
// publish now lands instead of failing.
func TestIntegration_ProducerBuildProvisionsItsFactTopic(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	if got := provisionedTopics(t, cluster); len(got) != 0 {
		t.Fatalf("precondition: cluster starts with topics %v; want none", got)
	}

	buildProvisioningProducer(t, cluster)

	if got := provisionedTopics(t, cluster); !slices.Contains(got, provFactTopic) {
		t.Errorf("after Build, topics = %v; want %q present", got, provFactTopic)
	}
}

// TestIntegration_ProducerBuildProvisionsItsOwnDLQ closes the produce-only gap.
//
// A producer route-DLQs a routable publish failure into its OWN
// "lerian.streaming.<source>.dlq". That name is in its own source namespace and
// it is the only writer, so it ensures it — a produce-only service has no
// consumer side to create it on its behalf. Left missing, the gap is invisible in
// the worst possible way: DLQ publish failures are logged and counted, never
// returned to the caller, so the forensic copy silently never lands.
func TestIntegration_ProducerBuildProvisionsItsOwnDLQ(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	buildProvisioningProducer(t, cluster)

	got := provisionedTopics(t, cluster)

	for _, want := range []string{provFactTopic, provProducerDLQ} {
		if !slices.Contains(got, want) {
			t.Errorf("after producer Build, topics = %v; want %q present", got, want)
		}
	}
}

// TestIntegration_ProducerBuildExcludesItsCommandsQueue pins the held boundary.
//
// The producer's own commands queue is in its own namespace and a producer with a
// Class: ClassCommand definition does write it — but provisioning it is
// deliberately NOT done, so a command emitted before its addressee exists still
// fails visibly. This test makes that a decision rather than an oversight.
func TestIntegration_ProducerBuildExcludesItsCommandsQueue(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	buildProvisioningProducer(t, cluster)

	if got := provisionedTopics(t, cluster); slices.Contains(got, provProducerCommands) {
		t.Errorf("producer Build provisioned its commands queue %q; that is a deliberate exclusion. topics = %v", provProducerCommands, got)
	}
}

// TestIntegration_ProducerFirstPublishLandsOnAProvisionedTopic proves the point
// of the whole feature end to end: with the broker refusing auto-creation, the
// first Emit succeeds.
func TestIntegration_ProducerFirstPublishLandsOnAProvisionedTopic(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)
	emitter := buildProvisioningProducer(t, cluster)

	if err := emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "loan.disbursed",
		TenantID:      "tenant-abc",
		Payload:       []byte(`{"amount":"1200.00"}`),
	}); err != nil {
		t.Fatalf("first Emit against a freshly provisioned topic error = %v", err)
	}
}

// TestIntegration_ProducerBuildIsIdempotent re-runs the whole construction
// against a broker that already has the topic. TOPIC_ALREADY_EXISTS is a silent
// success: no error, no duplicate, no second topic.
func TestIntegration_ProducerBuildIsIdempotent(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	buildProvisioningProducer(t, cluster)

	first := provisionedTopics(t, cluster)
	if !slices.Contains(first, provFactTopic) {
		t.Fatalf("first Build did not provision %q (got %v)", provFactTopic, first)
	}

	// Second Build sees the topic already there.
	buildProvisioningProducer(t, cluster)

	second := provisionedTopics(t, cluster)
	if !slices.Equal(first, second) {
		t.Errorf("second Build changed the topic set: %v -> %v; want identical", first, second)
	}
}

// TestIntegration_ProvisioningDisabledCreatesNothing is the hardened-environment
// opt-out: topics are pre-provisioned by IaC and the library must not try.
// Construction still succeeds — the knob suppresses provisioning, not startup.
func TestIntegration_ProvisioningDisabledCreatesNothing(t *testing.T) {
	pinProvisionEnv(t, "false")

	cluster := provisionCluster(t)

	buildProvisioningProducer(t, cluster)

	if got := provisionedTopics(t, cluster); len(got) != 0 {
		t.Errorf("with provisioning disabled, topics = %v; want none created", got)
	}
}

// buildProvisioningConsumer builds a consumer subscribed to a FOREIGN app's
// facts, plus optionally its own commands queue.
func buildProvisioningConsumer(t *testing.T, cluster *kfake.Cluster, takeOwnCommands bool) streaming.Consumer {
	t.Helper()

	builder := streaming.NewConsumer().
		Brokers(cluster.ListenAddrs()...).
		Group(provGroup).
		Source(provConsumerApp).
		Apps(provForeignApp).
		OnFrom(provForeignApp, "loan.disbursed", func(context.Context, streaming.Event, []byte) error {
			return nil
		})

	if takeOwnCommands {
		builder = builder.
			Commands(provConsumerApp).
			OnFrom(provConsumerApp, "margin.reserve", func(context.Context, streaming.Event, []byte) error {
				return nil
			})
	}

	consumer, err := builder.
		Options(streaming.WithConsumerLogger(log.NewNop())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("consumer Build() error = %v", err)
	}

	t.Cleanup(func() { _ = consumer.Close() })

	return consumer
}

// TestIntegration_ConsumerBuildProvisionsItsOwnDLQ pins the consumer half of the
// ownership rule. Its own DLQ is created — the consumer is that topic's producer,
// and a terminal record with nowhere to quarantine is silent data loss. The
// FOREIGN app's fact topic it subscribes to is NOT created: that name belongs to
// midaz, and creating it here would mask a typo'd subscription as healthy.
func TestIntegration_ConsumerBuildProvisionsItsOwnDLQ(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	buildProvisioningConsumer(t, cluster, false)

	got := provisionedTopics(t, cluster)

	if !slices.Contains(got, provConsumerDLQ) {
		t.Errorf("after consumer Build, topics = %v; want own DLQ %q present", got, provConsumerDLQ)
	}

	if slices.Contains(got, provForeignFactTopic) {
		t.Errorf("consumer provisioned another application's fact topic %q; topics = %v", provForeignFactTopic, got)
	}

	if slices.Contains(got, provConsumerCommands) {
		t.Errorf("a fact-only consumer provisioned a commands queue %q; topics = %v", provConsumerCommands, got)
	}
}

// TestIntegration_ConsumerBuildProvisionsItsOwnCommandsQueue: a consumer that
// takes commands addressed to ITSELF owns that queue and creates it. It still
// does not create the foreign app's names.
func TestIntegration_ConsumerBuildProvisionsItsOwnCommandsQueue(t *testing.T) {
	pinProvisionEnv(t, "true")

	cluster := provisionCluster(t)

	buildProvisioningConsumer(t, cluster, true)

	got := provisionedTopics(t, cluster)

	for _, want := range []string{provConsumerDLQ, provConsumerCommands} {
		if !slices.Contains(got, want) {
			t.Errorf("after consumer Build, topics = %v; want %q present", got, want)
		}
	}

	for _, unwanted := range []string{provForeignFactTopic, provForeignCommands, provFactTopic, provProducerDLQ} {
		if slices.Contains(got, unwanted) {
			t.Errorf("consumer provisioned a topic it does not own (%q); topics = %v", unwanted, got)
		}
	}
}
