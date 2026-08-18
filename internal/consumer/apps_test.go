//go:build unit

package consumer

import (
	"errors"
	"os"
	"slices"
	"testing"
)

func appsConfig(mutate func(*ConsumerConfig)) ConsumerConfig {
	cfg := DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "test-group"

	mutate(&cfg)

	return cfg
}

// TestConsumerConfig_AppsResolveToAppTopics pins subscribe-by-application:
// naming a producing app subscribes to its ONE topic, so a consumer never has
// to know or hardcode the "lerian.streaming." derivation.
func TestConsumerConfig_AppsResolveToAppTopics(t *testing.T) {
	t.Parallel()

	cfg := appsConfig(func(c *ConsumerConfig) { c.Apps = []string{"lender", "br_consignado_gw"} })

	got := cfg.ResolvedTopics()
	want := []string{"lerian.streaming.lender", "lerian.streaming.br_consignado_gw"}

	if !slices.Equal(got, want) {
		t.Fatalf("ResolvedTopics() = %v; want %v", got, want)
	}
}

// TestConsumerConfig_TopicsEscapeHatchSurvives pins that raw .Topics(...) is
// still first-class and composes with Apps rather than being replaced by it.
func TestConsumerConfig_TopicsEscapeHatchSurvives(t *testing.T) {
	t.Parallel()

	cfg := appsConfig(func(c *ConsumerConfig) {
		c.Apps = []string{"lender"}
		c.Topics = []string{"some.legacy.topic"}
	})

	got := cfg.ResolvedTopics()
	want := []string{"some.legacy.topic", "lerian.streaming.lender"}

	if !slices.Equal(got, want) {
		t.Fatalf("ResolvedTopics() = %v; want %v", got, want)
	}
}

// TestConsumerConfig_AppsDeduplicate pins that naming an app twice, or naming
// an app whose topic was also given raw, yields one subscription.
func TestConsumerConfig_AppsDeduplicate(t *testing.T) {
	t.Parallel()

	cfg := appsConfig(func(c *ConsumerConfig) {
		c.Apps = []string{"lender", "lender"}
		c.Topics = []string{"lerian.streaming.lender"}
	})

	if got := cfg.ResolvedTopics(); !slices.Equal(got, []string{"lerian.streaming.lender"}) {
		t.Fatalf("ResolvedTopics() = %v; want one deduplicated topic", got)
	}
}

// TestConsumerConfig_AppsSatisfyTopicRequirement pins that Apps alone is a
// complete subscription: Validate must not demand Topics as well.
func TestConsumerConfig_AppsSatisfyTopicRequirement(t *testing.T) {
	t.Parallel()

	cfg := appsConfig(func(c *ConsumerConfig) { c.Apps = []string{"lender"} })

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with Apps only = %v; want nil", err)
	}

	empty := appsConfig(func(*ConsumerConfig) {})
	if err := empty.Validate(); !errors.Is(err, ErrMissingTopics) {
		t.Fatalf("Validate() with neither Topics nor Apps = %v; want ErrMissingTopics", err)
	}
}

// TestConsumerConfig_RejectsMalformedApp pins that an app name is held to the
// SAME strict source contract the producer enforces. A consumer that cannot
// name a legal producer would otherwise silently subscribe to a topic no
// producer publishes to and sit there healthy and empty forever.
func TestConsumerConfig_RejectsMalformedApp(t *testing.T) {
	t.Parallel()

	for _, app := range []string{"//lerian.midaz/tx", "Lender", "lerian.midaz", "-lender", ""} {
		cfg := appsConfig(func(c *ConsumerConfig) { c.Apps = []string{app} })
		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate() with Apps=[%q] = nil; want a rejection", app)
		}
	}
}

// TestLoadConsumerConfig_ReadsApps pins the env surface for subscribe-by-app.
func TestLoadConsumerConfig_ReadsApps(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "g")
	t.Setenv("STREAMING_CONSUMER_APPS", "lender, matcher")
	_ = os.Unsetenv("STREAMING_CONSUMER_TOPICS")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if !slices.Equal(cfg.Apps, []string{"lender", "matcher"}) {
		t.Fatalf("Apps = %v; want [lender matcher]", cfg.Apps)
	}

	want := []string{"lerian.streaming.lender", "lerian.streaming.matcher"}
	if got := cfg.ResolvedTopics(); !slices.Equal(got, want) {
		t.Fatalf("ResolvedTopics() = %v; want %v", got, want)
	}
}
