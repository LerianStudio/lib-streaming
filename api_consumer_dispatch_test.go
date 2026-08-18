//go:build unit

package streaming

import (
	"context"
	"errors"
	"testing"
)

func noopEventHandler(context.Context, Event, []byte) error { return nil }

type rawStreamHandler struct{}

func (rawStreamHandler) Handle(context.Context, Event, []byte) error { return nil }

// TestConsumerBuilder_AppsResolveSubscription pins subscribe-by-application on
// the public surface: naming producers is enough, and the caller never spells
// the "lerian.streaming." derivation.
func TestConsumerBuilder_AppsResolveSubscription(t *testing.T) {
	t.Parallel()

	b := NewConsumer().Brokers("localhost:9092").Group("g").Apps("lender", "matcher")

	got := b.cfg.ResolvedTopics()
	if len(got) != 2 || got[0] != "lerian.streaming.lender" || got[1] != "lerian.streaming.matcher" {
		t.Fatalf("resolved topics = %v; want the two app topics", got)
	}
}

// TestConsumerBuilder_AppsArmSourceVerification pins the ergonomic payoff:
// subscribing by app also declares the expected producers, so the ce-source
// check every consuming repo used to hand-roll comes for free and cannot
// drift from the subscription it guards.
func TestConsumerBuilder_AppsArmSourceVerification(t *testing.T) {
	t.Parallel()

	b := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		On("loan.disbursed", noopEventHandler)

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	foreign := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), foreign, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(foreign source) = %v; want ErrUnexpectedSource", err)
	}

	own := Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), own, nil); err != nil {
		t.Fatalf("Handle(expected source) = %v; want nil", err)
	}
}

// TestConsumerBuilder_ExplicitExpectSourcesWins pins that an explicit
// ExpectSources is never silently overwritten by the Apps default.
func TestConsumerBuilder_ExplicitExpectSourcesWins(t *testing.T) {
	t.Parallel()

	b := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		ExpectSources("matcher").
		On("loan.disbursed", noopEventHandler)

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	if err := handler.Handle(context.Background(), Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}, nil); err != nil {
		t.Fatalf("Handle(explicitly expected source) = %v; want nil", err)
	}

	if err := handler.Handle(context.Background(), Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(app source) = %v; want ErrUnexpectedSource (explicit list replaces the Apps default)", err)
	}
}

// TestConsumerBuilder_UnmatchedDefaultIgnores pins the safe default at the
// public boundary: sibling events on the app stream are skipped, not
// quarantined.
func TestConsumerBuilder_UnmatchedDefaultIgnores(t *testing.T) {
	t.Parallel()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		On("loan.disbursed", noopEventHandler).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	sibling := Event{Source: "lender", ResourceType: "audit", EventType: "logged"}
	if err := handler.Handle(context.Background(), sibling, nil); err != nil {
		t.Fatalf("Handle(sibling event) = %v; want nil under the ignore default", err)
	}
}

// TestConsumerBuilder_UnmatchedErrorPolicyOptIn pins the strict mode.
func TestConsumerBuilder_UnmatchedErrorPolicyOptIn(t *testing.T) {
	t.Parallel()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		UnmatchedPolicy(UnmatchedError).
		On("loan.disbursed", noopEventHandler).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	sibling := Event{Source: "lender", ResourceType: "audit", EventType: "logged"}
	if err := handler.Handle(context.Background(), sibling, nil); !errors.Is(err, ErrUnhandledEvent) {
		t.Fatalf("Handle(sibling event) = %v; want ErrUnhandledEvent", err)
	}
}

// TestConsumerBuilder_HandlerAndOnAreExclusive pins that wiring both selection
// mechanisms fails loudly rather than silently dropping one set of handlers.
func TestConsumerBuilder_HandlerAndOnAreExclusive(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		Handler(rawStreamHandler{}).
		On("loan.disbursed", noopEventHandler).
		resolveHandler()

	if !errors.Is(err, ErrHandlerAndDispatchBothSet) {
		t.Fatalf("resolveHandler() = %v; want ErrHandlerAndDispatchBothSet", err)
	}
}

// TestConsumerBuilder_RawTopicsEscapeHatch pins that the raw path still works
// and leaves source verification off, so a consumer of a foreign stream is not
// forced to enumerate producers.
func TestConsumerBuilder_RawTopicsEscapeHatch(t *testing.T) {
	t.Parallel()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").
		Topics("some.legacy.topic").
		On("loan.disbursed", noopEventHandler).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	anySource := Event{Source: "whoever", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), anySource, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil (verification is opt-in on the raw path)", err)
	}
}

// TestConsumerBuilder_DispatchWithoutHandlersFails pins that a consumer that
// registered no handlers at all is a wiring bug, not a silent no-op consumer
// that commits an entire stream unread.
func TestConsumerBuilder_DispatchWithoutHandlersFails(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		UnmatchedPolicy(UnmatchedIgnore).
		resolveHandler()

	if err == nil {
		t.Fatal("resolveHandler() = nil; want a rejection for a dispatcher with no handlers")
	}
}

// TestConsumerBuilder_RejectsMalformedApp pins that an app name is held to the
// producer's strict source contract at Build time.
func TestConsumerBuilder_RejectsMalformedApp(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").
		Apps("//lerian.midaz/tx").
		On("loan.disbursed", noopEventHandler).
		Build(context.Background())

	if err == nil {
		t.Fatal("Build() = nil; want a rejection for a malformed app name")
	}
}
