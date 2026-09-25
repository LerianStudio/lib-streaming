//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/sasl/plain"

	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// TestConsumerBuilder_TLSSASLFromConfig mirrors the producer's
// builder_tls_sasl_from_config_test.go on the consumer: config-derived errors
// are deferred to Build, disabled features are no-ops, and the SASL-requires-TLS
// gate still runs. A nil want means Build must succeed.
func TestConsumerBuilder_TLSSASLFromConfig(t *testing.T) {
	t.Parallel()

	badCA := streaming.Config{TLSEnabled: true, TLSCACert: "not-valid-base64==="}
	bogusSASL := streaming.Config{SASLMechanism: "BOGUS", SASLUsername: "alice", SASLPassword: "secret"}
	scram := streaming.Config{SASLMechanism: "SCRAM-SHA-256", SASLUsername: "alice", SASLPassword: "secret"}
	scramPlaintext := scram
	scramPlaintext.SASLAllowPlaintext = true

	cases := []struct {
		name  string
		apply func(*streaming.ConsumerBuilder) *streaming.ConsumerBuilder
		want  error
	}{
		{"empty mechanism is a no-op", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(streaming.Config{})
		}, nil},
		{"invalid mechanism fails at Build", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(bogusSASL)
		}, streaming.ErrInvalidSASLMechanism},
		{"SASL without TLS is refused", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(scram)
		}, streaming.ErrPlaintextSASLNotAllowed},
		{"SASL without TLS builds with SASLAllowPlaintext", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(scramPlaintext)
		}, nil},
		{"SASL over config TLS builds", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.TLSFromConfig(streaming.Config{TLSEnabled: true}).SASLFromConfig(scram)
		}, nil},
		{"empty mechanism keeps the plaintext gate closed", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(streaming.Config{SASLAllowPlaintext: true}).
				SASL(plain.Auth{User: "alice", Pass: "secret"}.AsMechanism())
		}, streaming.ErrPlaintextSASLNotAllowed},
		{"disabled TLS is a no-op", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.TLSFromConfig(streaming.Config{TLSEnabled: false, TLSCACert: "ignored"})
		}, nil},
		{"invalid CA fails at Build", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.TLSFromConfig(badCA)
		}, streaming.ErrInvalidTLSConfig},
		{"first deferred error wins", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.SASLFromConfig(bogusSASL).TLSFromConfig(badCA)
		}, streaming.ErrInvalidSASLMechanism},
		{"disabled consumer stays a no-op", func(b *streaming.ConsumerBuilder) *streaming.ConsumerBuilder {
			return b.Enabled(false).TLSFromConfig(badCA)
		}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := streaming.NewConsumer().
				Brokers("localhost:9092").
				Group("svc").
				Source("test-consumer").
				Topics("loan.created").
				Handler(noopHandler{})

			c, err := tc.apply(base).Build(context.Background())
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Build() err = %v; want nil", err)
				}

				_ = c.Close()

				return
			}

			if !errors.Is(err, tc.want) {
				t.Fatalf("Build() err = %v; want %v", err, tc.want)
			}
		})
	}
}
