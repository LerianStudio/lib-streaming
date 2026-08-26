//go:build unit

package consumer

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

func validBaseConfig() ConsumerConfig {
	return ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "g",
		Source:              "test-consumer",
		Topics:              []string{"t"},
		RetryBudget:         3,
		RetryBackoffInitial: 100 * time.Millisecond,
		RetryBackoffMax:     time.Second,
		RetryInLoopMaxDwell: time.Second,
		HaltBackoff:         250 * time.Millisecond,
		CloseTimeout:        30 * time.Second,
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*ConsumerConfig)
		wantErr error
	}{
		{"valid", func(*ConsumerConfig) {}, nil},
		{"disabled is always valid", func(c *ConsumerConfig) { c.Enabled = false; c.Brokers = nil }, nil},
		{"missing brokers", func(c *ConsumerConfig) { c.Brokers = nil }, ErrMissingBrokers},
		{"missing group", func(c *ConsumerConfig) { c.Group = "" }, ErrMissingGroup},
		{"missing topics", func(c *ConsumerConfig) { c.Topics = nil }, ErrMissingTopics},
		{"negative retry budget", func(c *ConsumerConfig) { c.RetryBudget = -1 }, ErrInvalidConfigField},
		{"zero in-loop dwell", func(c *ConsumerConfig) { c.RetryInLoopMaxDwell = 0 }, ErrInvalidConfigField},
		{"negative halt backoff", func(c *ConsumerConfig) { c.HaltBackoff = -1 }, ErrInvalidConfigField},
		{"zero retry budget is valid (no in-loop retry)", func(c *ConsumerConfig) { c.RetryBudget = 0 }, nil},
		{"zero halt backoff is valid", func(c *ConsumerConfig) { c.HaltBackoff = 0 }, nil},
		{"zero poll timeout is valid (resolves to default)", func(c *ConsumerConfig) { c.PollTimeout = 0 }, nil},
		{"in-loop dwell above ceiling rejected", func(c *ConsumerConfig) { c.RetryInLoopMaxDwell = maxSafeRetryInLoopDwell + time.Second }, ErrInvalidConfigField},
		{"in-loop dwell at ceiling is valid", func(c *ConsumerConfig) { c.RetryInLoopMaxDwell = maxSafeRetryInLoopDwell }, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v; want nil", err)
				}

				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() = %v; want errors.Is %v", err, tt.wantErr)
			}
		})
	}
}

// TestValidate_SASLRequiresTLS proves the security gate: SASL without TLS is
// rejected unless explicitly opted into plaintext.
func TestValidate_SASLRequiresTLS(t *testing.T) {
	t.Parallel()

	mech := plain.Auth{User: "u", Pass: "p"}.AsMechanism()

	t.Run("SASL without TLS rejected", func(t *testing.T) {
		t.Parallel()

		cfg := validBaseConfig().WithSASL(mech)
		if err := cfg.Validate(); err == nil {
			t.Fatal("Validate() = nil; want SASL-requires-TLS rejection")
		}
	})

	t.Run("SASL with TLS accepted", func(t *testing.T) {
		t.Parallel()

		cfg := validBaseConfig().WithSASL(mech).WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() = %v; want nil", err)
		}
	})

	t.Run("SASL plaintext opt-in accepted", func(t *testing.T) {
		t.Parallel()

		cfg := validBaseConfig().WithSASL(mech).WithAllowPlaintextSASL()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() = %v; want nil", err)
		}
	})
}

func TestLoadConsumerConfig_Defaults(t *testing.T) {
	// Not parallel: mutates process env.
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092, b2:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "topic.a,topic.b")

	cfg, warnings, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if warnings == nil {
		t.Error("warnings slice must never be nil")
	}

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "b1:9092" || cfg.Brokers[1] != "b2:9092" {
		t.Errorf("Brokers = %v; want trimmed CSV split", cfg.Brokers)
	}

	if len(cfg.Topics) != 2 {
		t.Errorf("Topics = %v; want 2", cfg.Topics)
	}

	if cfg.RetryBudget != defaultRetryBudget {
		t.Errorf("RetryBudget = %d; want default %d", cfg.RetryBudget, defaultRetryBudget)
	}

	if cfg.RetryInLoopMaxDwell != defaultRetryInLoopMaxDwell {
		t.Errorf("RetryInLoopMaxDwell = %s; want default %s", cfg.RetryInLoopMaxDwell, defaultRetryInLoopMaxDwell)
	}

}

// TestLoadConsumerConfig_IgnoresRetiredDLQSuffixVar proves the DLQ topic name
// is not configurable from the environment. The two-name ACL contract
// (lerian.streaming.<app> plus its .dlq) is the point of the topic collapse; a
// free-text knob could rename the second half out from under the grant, and
// the suffix was duplicated in two packages to boot.
func TestLoadConsumerConfig_IgnoresRetiredDLQSuffixVar(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "topic.a")
	t.Setenv("STREAMING_CONSUMER_DLQ_SUFFIX", ".quarantine")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if contract.DLQTopicSuffix != ".dlq" {
		t.Fatalf("contract.DLQTopicSuffix = %q; want the single library-owned \".dlq\"", contract.DLQTopicSuffix)
	}

	// The retired var must not leak into the derived DLQ topic either: the
	// consumer quarantines into its own Source-derived name, ignoring the
	// suffix knob entirely.
	if got := contract.AppDLQTopic(cfg.Source); got != "lerian.streaming.test-consumer.dlq" {
		t.Fatalf("AppDLQTopic(%q) = %q; want %q", cfg.Source, got, "lerian.streaming.test-consumer.dlq")
	}
}

func TestLoadConsumerConfig_DisabledSkipsValidation(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "false")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("disabled config must load clean; got %v", err)
	}

	if cfg.Enabled {
		t.Error("Enabled = true; want false")
	}
}

func TestLoadConsumerConfig_EnabledMissingBrokers(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "t")

	_, _, err := LoadConsumerConfig()
	if !errors.Is(err, ErrMissingBrokers) {
		t.Fatalf("LoadConsumerConfig() = %v; want ErrMissingBrokers", err)
	}
}

// TestLoadConsumerConfig_ReadsExpectSources pins the env surface for the
// ce-source allowlist.
//
// Without it the environment surface was incomplete in a way that made one
// documented shape unreachable: Build hard-fails when APPS and TOPICS are both
// set and no explicit allowlist is given, and the allowlist could only be
// stated in code. An operator wiring both from env had no env-only way out.
func TestLoadConsumerConfig_ReadsExpectSources(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_APPS", "lender")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "legacy.topic")
	t.Setenv("STREAMING_CONSUMER_EXPECT_SOURCES", "lender, matcher")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if len(cfg.ExpectSources) != 2 || cfg.ExpectSources[0] != "lender" || cfg.ExpectSources[1] != "matcher" {
		t.Errorf("ExpectSources = %v; want [lender matcher] (trimmed CSV split)", cfg.ExpectSources)
	}
}

// TestLoadConsumerConfig_RejectsMalformedExpectSources holds the env entries to
// the same strict source rule Apps entries obey. A hyphen/underscore typo
// matches no real producer, so it would quarantine 100% of the stream while the
// consumer reports healthy — that must fail at load, not at 3am.
func TestLoadConsumerConfig_RejectsMalformedExpectSources(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "legacy.topic")
	t.Setenv("STREAMING_CONSUMER_EXPECT_SOURCES", "Lender")

	_, _, err := LoadConsumerConfig()
	if !errors.Is(err, ErrInvalidConfigField) {
		t.Fatalf("LoadConsumerConfig() = %v; want ErrInvalidConfigField", err)
	}

	// The env path must also match the field-specific sentinel the fluent
	// ExpectSources(...) path returns, so callers can branch on one error
	// value regardless of where the illegal entry came from.
	if !errors.Is(err, ErrInvalidExpectSource) {
		t.Fatalf("LoadConsumerConfig() = %v; want it to wrap ErrInvalidExpectSource", err)
	}
}
