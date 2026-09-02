//go:build unit

package streaming

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/LerianStudio/lib-streaming/v4/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// commandsBuilder is the rail-consumer shape: this application's identity, the
// applications that command it, and one handler per command it accepts.
func commandsBuilder() *ConsumerBuilder {
	return NewConsumer().
		Brokers("localhost:65535").
		Group("gw").
		Source("br-consignado-gw").
		OnFrom("lender", "margin.reserve", func(context.Context, contract.Event, []byte) error { return nil })
}

// TestConsumerBuilder_CommandsDerivesTheAllowlistAndSubscription pins the
// ergonomic payoff: naming the commanding app is the ONLY thing a rail consumer
// says. The commands queue is subscribed and that app's ce-source is accepted,
// with no hand-written topic string and no hand-written allowlist.
//
// Note what is NOT subscribed: lender's fact topic. A gateway that only takes
// lender's commands no longer needs READ on lender's fact stream at all —
// least-privilege the collapsed topic had taken away.
func TestConsumerBuilder_CommandsDerivesTheAllowlistAndSubscription(t *testing.T) {
	t.Parallel()

	b := commandsBuilder().Commands("lender")

	if _, err := b.resolveHandler(); err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	if got := b.cfg.ResolvedTopics(); !slices.Equal(got, []string{"lerian.streaming.lender.commands"}) {
		t.Errorf("ResolvedTopics() = %v; want only the lender commands queue", got)
	}

	if got := b.cfg.ExpectSources; !slices.Equal(got, []string{"lender"}) {
		t.Errorf("ExpectSources = %v; want [lender] derived from Commands", got)
	}
}

// TestConsumerBuilder_AppsAndCommandsSameAppDedupsTheAllowlist pins the legal
// overlap. Two subscriptions, ONE source in the allowlist — and one source
// means a bare On(...) still binds unambiguously, so the terse form survives.
func TestConsumerBuilder_AppsAndCommandsSameAppDedupsTheAllowlist(t *testing.T) {
	t.Parallel()

	b := NewConsumer().
		Brokers("localhost:65535").
		Group("gw").
		Source("br-consignado-gw").
		Apps("lender").
		Commands("lender").
		On("margin.reserve", func(context.Context, contract.Event, []byte) error { return nil })

	if _, err := b.resolveHandler(); err != nil {
		t.Fatalf("resolveHandler() error = %v; a bare On must still bind under one deduped source", err)
	}

	if got := b.cfg.ExpectSources; !slices.Equal(got, []string{"lender"}) {
		t.Errorf("ExpectSources = %v; want [lender] once — the allowlist is a set", got)
	}

	want := []string{"lerian.streaming.lender", "lerian.streaming.lender.commands"}
	if got := b.cfg.ResolvedTopics(); !slices.Equal(got, want) {
		t.Errorf("ResolvedTopics() = %v; want %v", got, want)
	}
}

// TestConsumerBuilder_CommandsWithRawTopicsIsAmbiguous pins that Commands
// counts exactly like Apps in the named-app + raw-Topics refusal. Defaulting
// the allowlist to the commanding app would quarantine every record off the raw
// topics, whose producers were never named.
func TestConsumerBuilder_CommandsWithRawTopicsIsAmbiguous(t *testing.T) {
	t.Parallel()

	b := commandsBuilder().Commands("lender").Topics("legacy.stream")

	if _, err := b.resolveHandler(); !errors.Is(err, consumer.ErrAmbiguousSourceVerification) {
		t.Fatalf("resolveHandler() error = %v; want ErrAmbiguousSourceVerification", err)
	}
}

// TestConsumerBuilder_ExpectSourcesMustCoverCommands pins that an explicit
// allowlist omitting a commanding app fails the build. Subscribing to an app's
// commands queue while refusing its ce-source quarantines every command it
// sends — under strict semantics, the loudest possible way to lose all of them.
func TestConsumerBuilder_ExpectSourcesMustCoverCommands(t *testing.T) {
	t.Parallel()

	b := commandsBuilder().Commands("lender").ExpectSources("matcher")

	if _, err := b.resolveHandler(); !errors.Is(err, consumer.ErrExpectSourcesMissingApp) {
		t.Fatalf("resolveHandler() error = %v; want ErrExpectSourcesMissingApp", err)
	}
}

// TestConsumerBuilder_HandlerAndCommandsFailsBuild pins the precise refusal.
// A whole-stream Handler has no handler registry, so "is this command key
// handled?" has no answer and the strict quarantine cannot be honoured.
// Accepting the combination would leave an operator believing undelivered
// commands are being quarantined while nothing is — the exact silence the
// commands queue exists to break.
func TestConsumerBuilder_HandlerAndCommandsFailsBuild(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:65535").
		Group("gw").
		Source("br-consignado-gw").
		Commands("lender").
		Handler(noopHandler{}).
		Build(context.Background())

	if !errors.Is(err, ErrHandlerAndCommandsBothSet) {
		t.Fatalf("Build() error = %v; want ErrHandlerAndCommandsBothSet", err)
	}
}

// TestConsumerBuilder_CommandsRejectsMalformedApp pins that a commanding app is
// held to the strict ce-source rule. A typo subscribes to a queue that stays
// empty forever — and on a commands queue, empty forever is undelivered work.
func TestConsumerBuilder_CommandsRejectsMalformedApp(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:65535").
		Group("gw").
		Source("br-consignado-gw").
		Commands("Lender").
		OnFrom("Lender", "margin.reserve", func(context.Context, contract.Event, []byte) error { return nil }).
		Build(context.Background())

	if !errors.Is(err, ErrConsumerInvalidConfigField) {
		t.Fatalf("Build() error = %v; want ErrConsumerInvalidConfigField", err)
	}
}

// TestConsumerBuilder_CommandsSetterMapsToConfig pins the setter itself,
// alongside the other setter assertions.
func TestConsumerBuilder_CommandsSetterMapsToConfig(t *testing.T) {
	t.Parallel()

	b := NewConsumer().Commands("lender", "matcher")

	if !slices.Equal(b.cfg.Commands, []string{"lender", "matcher"}) {
		t.Errorf("cfg.Commands = %v; want [lender matcher]", b.cfg.Commands)
	}
}
