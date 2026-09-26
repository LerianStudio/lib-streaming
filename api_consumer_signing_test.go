//go:build unit

package streaming_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// lenderSigningKey is a key bound to lender, the producer signingConsumer reads.
func lenderSigningKey() streaming.SigningKey {
	return streaming.SigningKey{ID: "lender-k1", Source: "lender", Secret: bytes.Repeat([]byte{1}, streaming.MinSigningSecretBytes)}
}

func consumerKeyring(t *testing.T, keys ...streaming.SigningKey) *streaming.Keyring {
	t.Helper()

	ring, err := streaming.NewKeyring(keys...)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	return ring
}

func noopHandlerFunc(context.Context, streaming.Event, []byte) error { return nil }

// signingConsumer is the minimal enabled consumer of lender's facts.
func signingConsumer() *streaming.ConsumerBuilder {
	return streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("svc").
		Source("loan-projector").
		Apps("lender").
		On("loan.created", noopHandlerFunc)
}

func TestConsumerBuilder_RequireSignaturesBuilds(t *testing.T) {
	t.Parallel()

	c, err := signingConsumer().
		RequireSignatures(consumerKeyring(t, lenderSigningKey())).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_ = c.Close()
}

func TestConsumerBuilder_RequireSignaturesWithoutKeysFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() *streaming.ConsumerBuilder
	}{
		{"fluent nil ring", func() *streaming.ConsumerBuilder {
			return signingConsumer().RequireSignatures(nil)
		}},
		{"config requires with no keys", func() *streaming.ConsumerBuilder {
			cfg := streaming.ConsumerConfig{
				Enabled:             true,
				Brokers:             []string{"localhost:9092"},
				Group:               "svc",
				Source:              "loan-projector",
				Apps:                []string{"lender"},
				RequireSignatures:   true,
				RetryBudget:         1,
				RetryBackoffInitial: time.Millisecond,
				RetryBackoffMax:     time.Millisecond,
				RetryInLoopMaxDwell: time.Millisecond,
				CloseTimeout:        time.Second,
			}

			return streaming.NewConsumer().FromConfig(cfg).On("loan.created", noopHandlerFunc)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := tt.build().Build(context.Background()); !errors.Is(err, streaming.ErrConsumerSignatureKeysMissing) {
				t.Errorf("Build err = %v; want ErrConsumerSignatureKeysMissing", err)
			}
		})
	}
}

// TestConsumerBuilder_RequireSignaturesMissingKeyForExpectedSource pins that
// coverage is provable at Build: a consumer that accepts two producers but
// holds a key for one would quarantine the other's whole stream as
// signature_unknown_key while reporting healthy.
func TestConsumerBuilder_RequireSignaturesMissingKeyForExpectedSource(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("svc").
		Source("loan-projector").
		Apps("lender", "matcher").
		OnFrom("lender", "loan.created", noopHandlerFunc).
		OnFrom("matcher", "loan.created", noopHandlerFunc).
		RequireSignatures(consumerKeyring(t, lenderSigningKey())).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrConsumerSignatureKeyMissingForSource) {
		t.Fatalf("Build err = %v; want ErrConsumerSignatureKeyMissingForSource", err)
	}

	if !strings.Contains(err.Error(), `"matcher"`) {
		t.Errorf("err = %v; want it to name the uncovered source", err)
	}
}

func TestConsumerBuilder_DiscardHandlerRefusesRequireSignatures(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("lender-dlq-desk").
		Source("lender-dlq-desk").
		Topics("lerian.streaming.lender.dlq").
		DiscardHandler(noopDiscardHandler{}).
		RequireSignatures(consumerKeyring(t, lenderSigningKey())).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrDiscardHandlerAndHandlerBothSet) {
		t.Errorf("Build err = %v; want ErrDiscardHandlerAndHandlerBothSet", err)
	}
}

// TestConsumerBuilder_DiscardHandlerIgnoresEnvRequireSignatures pins the env
// exemption, the same one the env ce-source allowlist has: one process shares
// one environment across its consumers, and a DLQ reader never verifies, so a
// fleet-wide STREAMING_CONSUMER_REQUIRE_SIGNATURES must not stop it building.
func TestConsumerBuilder_DiscardHandlerIgnoresEnvRequireSignatures(t *testing.T) {
	t.Parallel()

	cfg := streaming.ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "lender-dlq-desk",
		Source:              "lender-dlq-desk",
		Topics:              []string{"lerian.streaming.lender.dlq"},
		RequireSignatures:   true,
		SignatureKeys:       consumerKeyring(t, lenderSigningKey()),
		RetryBudget:         1,
		RetryBackoffInitial: time.Millisecond,
		RetryBackoffMax:     time.Millisecond,
		RetryInLoopMaxDwell: time.Millisecond,
		CloseTimeout:        time.Second,
	}

	c, err := streaming.NewConsumer().FromConfig(cfg).DiscardHandler(noopDiscardHandler{}).Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_ = c.Close()
}

func TestConsumerBuilder_SignatureMaxSkewNegativeFails(t *testing.T) {
	t.Parallel()

	_, err := signingConsumer().
		RequireSignatures(consumerKeyring(t, lenderSigningKey())).
		SignatureMaxSkew(-time.Second).
		Build(context.Background())

	if !errors.Is(err, streaming.ErrConsumerInvalidConfigField) {
		t.Errorf("Build err = %v; want ErrConsumerInvalidConfigField", err)
	}
}
