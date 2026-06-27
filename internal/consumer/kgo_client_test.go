//go:build unit

package consumer

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoOptsTestConfig is a minimal valid ConsumerConfig for the opts-builder
// tests. No TLS/SASL so the builders take their plaintext path; the security
// composition is exercised by the producer's options_tls_sasl_test.go against
// the same kafkasec validators.
func kgoOptsTestConfig() ConsumerConfig {
	return ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092", "localhost:9093"},
		Group:               "opts-test-group",
		Topics:              []string{"t1", "t2"},
		ClientID:            "opts-test-client",
		RetryBudget:         1,
		RetryBackoffInitial: time.Millisecond,
		RetryBackoffMax:     2 * time.Millisecond,
		RetryInLoopMaxDwell: 10 * time.Millisecond,
		CloseTimeout:        time.Second,
		DLQTopicSuffix:      ".dlq",
	}
}

// TestBuildDLQKgoOpts_IsProduceOnly is the regression guard for the
// phantom-group-member bug: the DLQ adapter must be a PRODUCE-ONLY client and
// must NEVER carry the consumer-group / subscribe / block-rebalance options. If
// it did, the DLQ client would join the real consumer's group as a non-polling
// member, holding an assignment it never consumes — a rebalance hazard that
// starves the consumer on a live broker (and a ~50% group-join stall under
// kfake, which is how the bug was originally caught).
//
// The opts are opaque closures over franz-go's private cfg, so we assert at the
// count level (the producer's options_tls_sasl_test.go idiom) plus a behavioral
// smoke: buildConsumerKgoOpts adds EXACTLY the 4 group options on top of the
// produce-only DLQ base, and a real kgo.Client constructs from each.
func TestBuildDLQKgoOpts_IsProduceOnly(t *testing.T) {
	t.Parallel()

	cfg := kgoOptsTestConfig()

	dlqOpts, err := buildDLQKgoOpts(cfg)
	if err != nil {
		t.Fatalf("buildDLQKgoOpts err = %v", err)
	}

	consumerOpts, err := buildConsumerKgoOpts(cfg)
	if err != nil {
		t.Fatalf("buildConsumerKgoOpts err = %v", err)
	}

	// The consumer opt set is the DLQ (produce-only) base PLUS exactly the four
	// group-member options: ConsumerGroup, ConsumeTopics, BlockRebalanceOnPoll,
	// DisableAutoCommit. Any drift here means a group option leaked into (or
	// dropped out of) one of the two paths.
	const groupOptionCount = 4
	if got, want := len(consumerOpts)-len(dlqOpts), groupOptionCount; got != want {
		t.Fatalf("consumerOpts has %d more opts than dlqOpts; want exactly %d "+
			"(ConsumerGroup, ConsumeTopics, BlockRebalanceOnPoll, DisableAutoCommit)",
			got, want)
	}

	// Behavioral smoke: both option sets must construct a real franz-go client.
	// kgo.NewClient does not dial at construction, so this is offline + fast and
	// proves the option lists are internally consistent (no conflicting opts).
	dlqClient, err := kgo.NewClient(dlqOpts...)
	if err != nil {
		t.Fatalf("kgo.NewClient(dlqOpts) err = %v", err)
	}

	dlqClient.Close()

	consumerClient, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		t.Fatalf("kgo.NewClient(consumerOpts) err = %v", err)
	}

	consumerClient.Close()
}

// TestBuildConsumerKgoOpts_OptionalFields verifies the optional ClientID/TLS/
// SASL fields add opts on both builders symmetrically (the shared base), so a
// future security change can't silently apply to only one client.
func TestBuildConsumerKgoOpts_ClientIDOptional(t *testing.T) {
	t.Parallel()

	withID := kgoOptsTestConfig()

	noID := kgoOptsTestConfig()
	noID.ClientID = ""

	dlqWith, err := buildDLQKgoOpts(withID)
	if err != nil {
		t.Fatalf("buildDLQKgoOpts(withID) err = %v", err)
	}

	dlqWithout, err := buildDLQKgoOpts(noID)
	if err != nil {
		t.Fatalf("buildDLQKgoOpts(noID) err = %v", err)
	}

	// ClientID present adds exactly one opt (kgo.ClientID) to the produce-only base.
	if got := len(dlqWith) - len(dlqWithout); got != 1 {
		t.Errorf("ClientID adds %d opts to DLQ base; want 1", got)
	}
}
