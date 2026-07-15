//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
)

func TestBuilder_SASLFromConfig_InvalidMechanismDefersToBuild(t *testing.T) {
	t.Parallel()

	cfg := streaming.Config{
		SASLMechanism: "BOGUS",
		SASLUsername:  "alice",
		SASLPassword:  "secret",
	}

	_, err := streaming.NewBuilder().SASLFromConfig(cfg).Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSASLMechanism) {
		t.Fatalf("Build() err = %v; want ErrInvalidSASLMechanism deferred from SASLFromConfig", err)
	}
}

func TestBuilder_TLSFromConfig_InvalidCADefersToBuild(t *testing.T) {
	t.Parallel()

	cfg := streaming.Config{
		TLSEnabled: true,
		TLSCACert:  "not-valid-base64===",
	}

	_, err := streaming.NewBuilder().TLSFromConfig(cfg).Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidTLSConfig) {
		t.Fatalf("Build() err = %v; want ErrInvalidTLSConfig deferred from TLSFromConfig", err)
	}
}

func TestBuilder_TLSFromConfig_DisabledIsNoop(t *testing.T) {
	t.Parallel()

	// TLS disabled: TLSFromConfig must not set a build error. Build then fails
	// on the missing route table, NOT on a TLS error.
	cfg := streaming.Config{TLSEnabled: false, TLSCACert: "ignored"}

	_, err := streaming.NewBuilder().TLSFromConfig(cfg).Build(context.Background())
	if err == nil {
		t.Fatal("Build() err = nil; want a missing-route error")
	}

	if errors.Is(err, streaming.ErrInvalidTLSConfig) {
		t.Fatalf("Build() err = %v; TLSFromConfig(disabled) must be a no-op", err)
	}
}

func TestBuilder_SASLFromConfig_EmptyMechanismIsNoop(t *testing.T) {
	t.Parallel()

	// Empty mechanism: SASLFromConfig must not set a build error. Build then
	// fails on the missing route table, NOT on a SASL error.
	cfg := streaming.Config{}

	_, err := streaming.NewBuilder().SASLFromConfig(cfg).Build(context.Background())
	if err == nil {
		t.Fatal("Build() err = nil; want a missing-route error")
	}

	if errors.Is(err, streaming.ErrInvalidSASLMechanism) {
		t.Fatalf("Build() err = %v; SASLFromConfig(empty mechanism) must be a no-op", err)
	}
}

// TestBuilder_DeferredError_FirstWins proves the `if b.buildErr == nil` guard:
// when two config-derived setters both fail, the FIRST captured error survives
// regardless of chaining order. Build surfaces buildErr as its first statement,
// so no other Build hard-requirement (Source/Catalog/Routes/Target) needs to be
// set to reach the guard.
func TestBuilder_DeferredError_FirstWins(t *testing.T) {
	t.Parallel()

	// TLSEnabled with a malformed base64 CA -> BuildTLSConfig fails with
	// ErrInvalidTLSConfig.
	badTLScfg := streaming.Config{
		TLSEnabled: true,
		TLSCACert:  "not-valid-base64===",
	}

	// A recognized-looking but unknown mechanism -> BuildSASLMechanism fails
	// with ErrInvalidSASLMechanism.
	badSASLcfg := streaming.Config{
		SASLMechanism: "BOGUS",
		SASLUsername:  "alice",
		SASLPassword:  "secret",
	}

	t.Run("TLS setter first wins", func(t *testing.T) {
		t.Parallel()

		_, err := streaming.NewBuilder().
			TLSFromConfig(badTLScfg).
			SASLFromConfig(badSASLcfg).
			Build(context.Background())
		if !errors.Is(err, streaming.ErrInvalidTLSConfig) {
			t.Fatalf("Build() err = %v; want first-captured ErrInvalidTLSConfig", err)
		}

		if errors.Is(err, streaming.ErrInvalidSASLMechanism) {
			t.Fatalf("Build() err = %v; second-captured SASL error must NOT surface", err)
		}
	})

	t.Run("SASL setter first wins", func(t *testing.T) {
		t.Parallel()

		_, err := streaming.NewBuilder().
			SASLFromConfig(badSASLcfg).
			TLSFromConfig(badTLScfg).
			Build(context.Background())
		if !errors.Is(err, streaming.ErrInvalidSASLMechanism) {
			t.Fatalf("Build() err = %v; want first-captured ErrInvalidSASLMechanism", err)
		}

		if errors.Is(err, streaming.ErrInvalidTLSConfig) {
			t.Fatalf("Build() err = %v; second-captured TLS error must NOT surface", err)
		}
	})
}

// minimalValidBuilder returns a Builder satisfying every Build hard requirement
// (Source, Catalog, Routes, Target) so Build proceeds into the Kafka adapter
// pipeline where the SASL-requires-TLS gate runs. The Kafka client is
// constructed lazily by franz-go, so no live broker is dialed at Build time.
func minimalValidBuilder(t *testing.T) *streaming.Builder {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}

	route := streaming.RouteDefinition{
		Key:           "transaction.created.kafka",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("streaming.transaction.created"),
		Requirement:   streaming.RouteRequired,
	}

	return streaming.NewBuilder().
		Source("//lerian.test/svc").
		Catalog(catalog).
		Routes(route).
		Target(streaming.TargetConfig{
			Name:    "primary",
			Kind:    streaming.TransportKafkaLike,
			Brokers: []string{"localhost:9092"},
		})
}

// TestBuilder_SASLFromConfig_RequiresTLS proves the fail-closed SASL-requires-TLS
// gate runs at Build for the env-driven SASLFromConfig path. With valid
// credentials, no TLS, and SASLAllowPlaintext=false the Build must fail with
// ErrPlaintextSASLNotAllowed; flipping SASLAllowPlaintext=true opens the gate.
func TestBuilder_SASLFromConfig_RequiresTLS(t *testing.T) {
	t.Parallel()

	t.Run("no TLS, plaintext disallowed -> fails closed", func(t *testing.T) {
		t.Parallel()

		cfg := streaming.Config{
			SASLMechanism:      "SCRAM-SHA-256",
			SASLUsername:       "alice",
			SASLPassword:       "secret",
			SASLAllowPlaintext: false,
		}

		_, err := minimalValidBuilder(t).
			SASLFromConfig(cfg).
			Build(context.Background())
		if !errors.Is(err, streaming.ErrPlaintextSASLNotAllowed) {
			t.Fatalf("Build() err = %v; want ErrPlaintextSASLNotAllowed (SASL without TLS must fail closed)", err)
		}
	})

	t.Run("no TLS, plaintext allowed -> gate opens", func(t *testing.T) {
		t.Parallel()

		cfg := streaming.Config{
			SASLMechanism:      "SCRAM-SHA-256",
			SASLUsername:       "alice",
			SASLPassword:       "secret",
			SASLAllowPlaintext: true,
		}

		emitter, err := minimalValidBuilder(t).
			SASLFromConfig(cfg).
			Build(context.Background())
		if errors.Is(err, streaming.ErrPlaintextSASLNotAllowed) {
			t.Fatalf("Build() err = %v; SASLAllowPlaintext=true must open the plaintext gate", err)
		}

		if err != nil {
			t.Fatalf("Build() err = %v; want nil once the plaintext gate is opened", err)
		}

		t.Cleanup(func() { _ = emitter.Close() })
	})
}
