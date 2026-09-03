//go:build integration

package streaming_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// This file drives streaming.NewAdminClient against a real broker protocol.
//
// The motivating consumer is br-sfn's SPB rail: its lifecycle events are money
// facts that must never be pruned, so it verifies at boot that its application
// topic is provisioned with unlimited retention (retention.ms = -1). A
// client-provisioned topic left on the broker's 7-day default would silently
// blind the institution — the topic exists, publishes succeed, and the facts
// simply stop being there.
//
// Nothing in that check is expressible without an admin client dialed with the
// producer's own TLS/SASL posture, which is the gap NewAdminClient closes.

const (
	adminIntegrationSource = "br-spb"
	retentionConfigKey     = "retention.ms"

	// retentionUnlimited is the required state: lifecycle events are money
	// facts and must never be pruned.
	retentionUnlimited = "-1"
	// retentionSevenDays is the broker default that would silently blind the
	// institution. The test provisions a topic with it deliberately, as the
	// discriminating case: a read that answered with a constant, a default, or
	// the wrong resource's config would report unlimited here and pass a check
	// that must fail.
	retentionSevenDays = "604800000"
)

// TestIntegration_NewAdminClientReadsTopicRetention is the SPB retention check,
// end to end: provision two topics with DIFFERENT retention settings, then read
// both back through the public constructor exactly as the rail will, and
// require each to answer with its own value.
//
// The two-value shape is the point. Asserting only that a topic written with
// -1 reads back -1 proves nothing about the read — the same constant is on both
// sides of the assertion, so a stubbed or resource-blind response would satisfy
// it. Requiring the admin client to tell the two topics apart is what proves it
// is really reading per-topic broker state.
func TestIntegration_NewAdminClientReadsTopicRetention(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.DefaultNumPartitions(1),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	// Derive the names the same way the runtime does, rather than spelling them
	// in the test — a check that verified a hand-written name would keep passing
	// after the derivation changed.
	factTopic, err := streaming.AppTopic(adminIntegrationSource)
	if err != nil {
		t.Fatalf("AppTopic(%q) err = %v", adminIntegrationSource, err)
	}

	dlqTopic, err := streaming.AppDLQTopic(adminIntegrationSource)
	if err != nil {
		t.Fatalf("AppDLQTopic(%q) err = %v", adminIntegrationSource, err)
	}

	admin, err := streaming.NewAdminClient(streaming.Config{
		Brokers:  cluster.ListenAddrs(),
		ClientID: "admin-client-kfake",
	})
	if err != nil {
		t.Fatalf("NewAdminClient() err = %v; want nil", err)
	}

	// The caller owns the lifecycle: this closes the underlying kgo client.
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The fact topic gets the required unlimited retention; the DLQ is left on
	// the 7-day default so the two answers must differ.
	want := map[string]string{
		factTopic: retentionUnlimited,
		dlqTopic:  retentionSevenDays,
	}

	for topic, retention := range want {
		value := retention

		created, createErr := admin.CreateTopics(ctx, 1, 1, map[string]*string{retentionConfigKey: &value}, topic)
		if createErr != nil {
			t.Fatalf("CreateTopics(%q) err = %v", topic, createErr)
		}

		for _, response := range created {
			if response.Err != nil {
				t.Fatalf("CreateTopics(%q) err = %v", response.Topic, response.Err)
			}
		}
	}

	configs, err := admin.DescribeTopicConfigs(ctx, factTopic, dlqTopic)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs err = %v", err)
	}

	if len(configs) != len(want) {
		t.Fatalf("DescribeTopicConfigs returned %d resources; want %d", len(configs), len(want))
	}

	seen := make(map[string]string, len(want))

	for _, resource := range configs {
		if resource.Err != nil {
			t.Fatalf("DescribeTopicConfigs(%q) err = %v", resource.Name, resource.Err)
		}

		got, ok := topicConfigValue(resource.Configs, retentionConfigKey)
		if !ok {
			t.Errorf("DescribeTopicConfigs(%q): %q absent from the response", resource.Name, retentionConfigKey)
			continue
		}

		seen[resource.Name] = got
	}

	for topic, expected := range want {
		got, ok := seen[topic]
		if !ok {
			t.Errorf("DescribeTopicConfigs did not answer for %q", topic)
			continue
		}

		if got != expected {
			t.Errorf("DescribeTopicConfigs(%q): %s = %q; want %q", topic, retentionConfigKey, got, expected)
		}
	}
}

// topicConfigValue finds one config by name in a describe response. A config
// with a nil value is reported as absent: the caller cannot act on "the broker
// named this setting but told us nothing about it", and treating that as a
// match would turn an unanswered retention question into a passing check.
func topicConfigValue(configs []kadm.Config, name string) (string, bool) {
	for _, config := range configs {
		if config.Key == name && config.Value != nil {
			return *config.Value, true
		}
	}

	return "", false
}
