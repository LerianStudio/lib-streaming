//go:build unit

package streaming

import (
	"bytes"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/v4/internal/consumer"
)

func whiteboxSigningKey(id, source string, seed byte) SigningKey {
	return SigningKey{ID: id, Source: source, Secret: bytes.Repeat([]byte{seed}, MinSigningSecretBytes)}
}

// TestConsumerBuilder_RequireSignaturesFromConfig pins where the verifier's
// ring comes from: the env keys when STREAMING_CONSUMER_REQUIRE_SIGNATURES is
// on, a fluent RequireSignatures ring over them, and nothing at all when
// verification is not required, even with keys present.
func TestConsumerBuilder_RequireSignaturesFromConfig(t *testing.T) {
	t.Parallel()

	envKey := whiteboxSigningKey("lender-env", "lender", 1)
	fluentKey := whiteboxSigningKey("lender-fluent", "lender", 2)

	base := func(require bool) consumer.ConsumerConfig {
		cfg := consumer.DefaultBuilderConfig()
		cfg.Brokers = []string{"localhost:9092"}
		cfg.Group = "svc"
		cfg.Source = "loan-projector"
		cfg.Apps = []string{"lender"}
		cfg.RequireSignatures = require
		cfg.SignatureKeys = []SigningKey{envKey}
		cfg.SignatureMaxSkew = time.Minute

		return cfg
	}

	t.Run("env keys when required", func(t *testing.T) {
		t.Parallel()

		b := NewConsumer().FromConfig(base(true))

		ring, err := b.signatureRing()
		if err != nil || ring == nil {
			t.Fatalf("signatureRing = %v, %v; want a ring", ring, err)
		}

		if got := ring.String(); got != "Keyring{ids:[lender-env]}" {
			t.Errorf("ring = %s; want the env key", got)
		}
	})

	t.Run("fluent ring overrides env keys", func(t *testing.T) {
		t.Parallel()

		ring, err := NewKeyring(fluentKey)
		if err != nil {
			t.Fatal(err)
		}

		b := NewConsumer().FromConfig(base(false)).RequireSignatures(ring)

		got, err := b.signatureRing()
		if err != nil || got == nil {
			t.Fatalf("signatureRing = %v, %v; want a ring", got, err)
		}

		if got.String() != "Keyring{ids:[lender-fluent]}" {
			t.Errorf("ring = %s; want the fluent ring", got.String())
		}
	})

	t.Run("keys without require install nothing", func(t *testing.T) {
		t.Parallel()

		b := NewConsumer().FromConfig(base(false))

		ring, err := b.signatureRing()
		if err != nil || ring != nil {
			t.Fatalf("signatureRing = %v, %v; want nil, nil", ring, err)
		}
	})
}
