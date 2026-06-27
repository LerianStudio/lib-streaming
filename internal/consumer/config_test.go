//go:build unit

package consumer

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func validBaseConfig() ConsumerConfig {
	return ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "g",
		Topics:              []string{"t"},
		RetryBudget:         3,
		RetryBackoffInitial: 100 * time.Millisecond,
		RetryBackoffMax:     time.Second,
		RetryInLoopMaxDwell: time.Second,
		HaltBackoff:         250 * time.Millisecond,
		CloseTimeout:        30 * time.Second,
		DLQTopicSuffix:      ".dlq",
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
		{"zero poll timeout is valid (block)", func(c *ConsumerConfig) { c.PollTimeout = 0 }, nil},
		{"whitespace-only DLQ suffix rejected", func(c *ConsumerConfig) { c.DLQTopicSuffix = "  " }, ErrInvalidConfigField},
		{"empty DLQ suffix is valid (defaulted upstream)", func(c *ConsumerConfig) { c.DLQTopicSuffix = "" }, nil},
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

	if cfg.DLQTopicSuffix != DefaultDLQTopicSuffix {
		t.Errorf("DLQTopicSuffix = %q; want default %q", cfg.DLQTopicSuffix, DefaultDLQTopicSuffix)
	}
}

// TestLoadConsumerConfig_BlankSuffixDefaulted proves an env var explicitly set to
// "" (which GetenvOrDefault does NOT substitute) is re-defaulted to ".dlq" so a
// terminal record never republishes into the source topic and loops.
func TestLoadConsumerConfig_BlankSuffixDefaulted(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "topic.a")
	t.Setenv("STREAMING_CONSUMER_DLQ_SUFFIX", "")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if cfg.DLQTopicSuffix != DefaultDLQTopicSuffix {
		t.Errorf("DLQTopicSuffix = %q; want re-defaulted %q", cfg.DLQTopicSuffix, DefaultDLQTopicSuffix)
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
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_TOPICS", "t")

	_, _, err := LoadConsumerConfig()
	if !errors.Is(err, ErrMissingBrokers) {
		t.Fatalf("LoadConsumerConfig() = %v; want ErrMissingBrokers", err)
	}
}
