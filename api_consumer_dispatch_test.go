//go:build unit

package streaming

import (
	"context"
	"errors"
	"testing"
)

// trackingHandler returns a HandlerFunc and a pointer to the flag it flips.
//
// Every dispatch test in this file asserts on that flag, never on `err == nil`
// alone: under UnmatchedIgnore a silently DROPPED event and a successfully
// HANDLED one both return nil, so a nil-only assertion cannot tell "the
// handler ran" from "the library swallowed it".
func trackingHandler() (HandlerFunc, *bool) {
	ran := false

	return func(context.Context, Event, []byte) error {
		ran = true

		return nil
	}, &ran
}

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

	handlerFn, ran := trackingHandler()

	b := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		On("loan.disbursed", handlerFn)

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	foreign := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), foreign, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(foreign source) = %v; want ErrUnexpectedSource", err)
	}

	if *ran {
		t.Fatal("handler ran for a foreign ce-source; verification must reject before dispatch")
	}

	own := Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), own, nil); err != nil {
		t.Fatalf("Handle(expected source) = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run for the expected ce-source")
	}
}

// TestConsumerBuilder_ExplicitExpectSourcesReplacesAppsDefault pins that an
// explicit ExpectSources list is the whole allowlist — it is never silently
// widened by, nor overwritten by, the Apps default.
func TestConsumerBuilder_ExplicitExpectSourcesReplacesAppsDefault(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	b := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		ExpectSources("lender", "matcher").
		On("loan.disbursed", handlerFn)

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	matcher := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), matcher, nil); err != nil {
		t.Fatalf("Handle(explicitly expected source) = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run for an explicitly expected source")
	}

	*ran = false

	stranger := Event{Source: "someone-else", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), stranger, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(unlisted source) = %v; want ErrUnexpectedSource", err)
	}

	if *ran {
		t.Fatal("handler ran for a source outside the explicit allowlist")
	}
}

// TestConsumerBuilder_ExpectSourcesMustCoverApps pins that refusing an app's
// own ce-source while subscribing to its topic is a build error.
//
// It is never a filter — it quarantines 100% of that app's stream into the DLQ
// while the consumer reports Healthy. Selection belongs in On(...).
func TestConsumerBuilder_ExpectSourcesMustCoverApps(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		ExpectSources("matcher").
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrExpectSourcesMissingApp) {
		t.Fatalf("resolveHandler() = %v; want ErrExpectSourcesMissingApp", err)
	}
}

// TestConsumerBuilder_RejectsMalformedExpectSource pins that an ExpectSources
// entry is held to the producer's strict source contract at Build time. A
// hyphen/underscore typo there matches no real producer, so every record would
// be quarantined while the consumer reported healthy.
func TestConsumerBuilder_RejectsMalformedExpectSource(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Topics("some.legacy.topic").
		ExpectSources("Lender").
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrInvalidExpectSource) {
		t.Fatalf("resolveHandler() = %v; want ErrInvalidExpectSource", err)
	}

	if !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("resolveHandler() = %v; want the underlying ErrInvalidSource to remain matchable", err)
	}
}

// TestConsumerBuilder_AppsAndTopicsRequireExplicitExpectSources pins the
// refusal to guess when both subscription styles are used.
//
// Defaulting the allowlist to Apps would DLQ every record arriving on the raw
// Topics, whose producers were never named; skipping verification would drop
// the check the Apps subscription paid for. Neither guess is defensible, so
// Build says so instead.
func TestConsumerBuilder_AppsAndTopicsRequireExplicitExpectSources(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	b := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		Topics("some.legacy.topic").
		On("loan.disbursed", handlerFn)

	if _, err := b.resolveHandler(); !errors.Is(err, ErrAmbiguousSourceVerification) {
		t.Fatalf("resolveHandler() = %v; want ErrAmbiguousSourceVerification", err)
	}

	explicitFn, ran := trackingHandler()

	handler, err := NewConsumer().
		Brokers("localhost:9092").
		Group("g").
		Apps("lender").
		Topics("some.legacy.topic").
		ExpectSources("lender", "legacy-writer").
		On("loan.disbursed", explicitFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() with explicit ExpectSources error = %v; want nil", err)
	}

	legacy := Event{Source: "legacy-writer", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), legacy, nil); err != nil {
		t.Fatalf("Handle(raw-topic producer) = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run for a source named explicitly alongside the raw topics")
	}
}

// TestConsumerBuilder_UnmatchedDefaultIgnores pins the safe default at the
// public boundary: sibling events on the app stream are skipped, not
// quarantined — and demonstrably NOT handed to a handler.
func TestConsumerBuilder_UnmatchedDefaultIgnores(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		On("loan.disbursed", handlerFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	sibling := Event{Source: "lender", ResourceType: "audit", EventType: "logged"}
	if err := handler.Handle(context.Background(), sibling, nil); err != nil {
		t.Fatalf("Handle(sibling event) = %v; want nil under the ignore default", err)
	}

	if *ran {
		t.Fatal("the loan.disbursed handler ran for an audit.logged event")
	}
}

// TestConsumerBuilder_UnmatchedErrorPolicyOptIn pins the strict mode.
func TestConsumerBuilder_UnmatchedErrorPolicyOptIn(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		UnmatchedPolicy(UnmatchedError).
		On("loan.disbursed", handlerFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	sibling := Event{Source: "lender", ResourceType: "audit", EventType: "logged"}
	if err := handler.Handle(context.Background(), sibling, nil); !errors.Is(err, ErrUnhandledEvent) {
		t.Fatalf("Handle(sibling event) = %v; want ErrUnhandledEvent", err)
	}

	if *ran {
		t.Fatal("the loan.disbursed handler ran for an audit.logged event")
	}
}

// TestConsumerBuilder_HandlerAndOnAreExclusive pins that wiring both selection
// mechanisms fails loudly rather than silently dropping one set of handlers.
func TestConsumerBuilder_HandlerAndOnAreExclusive(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		Handler(rawStreamHandler{}).
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrHandlerAndDispatchBothSet) {
		t.Fatalf("resolveHandler() = %v; want ErrHandlerAndDispatchBothSet", err)
	}
}

// TestConsumerBuilder_HandlerWithExpectSourcesFailsPrecisely pins that
// combining a whole-stream Handler with ExpectSources gets its OWN error.
//
// ExpectSources allocates the dispatcher internally, which used to read as
// "the caller wanted On(...)" and produced "Handler and On are mutually
// exclusive" — advice pointing at an On call the caller never wrote. Dispatch
// intent now comes from On alone.
func TestConsumerBuilder_HandlerWithExpectSourcesFailsPrecisely(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		Handler(rawStreamHandler{}).
		ExpectSources("lender").
		resolveHandler()

	if !errors.Is(err, ErrHandlerAndExpectSourcesBothSet) {
		t.Fatalf("resolveHandler() = %v; want ErrHandlerAndExpectSourcesBothSet", err)
	}

	if errors.Is(err, ErrHandlerAndDispatchBothSet) {
		t.Fatal("resolveHandler() blamed On(...), which the caller never wrote")
	}
}

// TestConsumerBuilder_HandlerWithUnmatchedPolicyIsAccepted pins the other half
// of the same fix: UnmatchedPolicy also allocates the dispatcher, and it must
// not turn a valid whole-stream Handler build into a mutual-exclusion error.
func TestConsumerBuilder_HandlerWithUnmatchedPolicyIsAccepted(t *testing.T) {
	t.Parallel()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Apps("lender").
		UnmatchedPolicy(UnmatchedError).
		Handler(rawStreamHandler{}).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() = %v; want nil (UnmatchedPolicy is inert without On)", err)
	}

	if _, ok := handler.(rawStreamHandler); !ok {
		t.Fatalf("resolveHandler() returned %T; want the caller's whole-stream handler", handler)
	}
}

// TestConsumerBuilder_RawTopicsEscapeHatch pins that the raw path still works
// and leaves source verification off, so a consumer of a foreign stream is not
// forced to enumerate producers.
func TestConsumerBuilder_RawTopicsEscapeHatch(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, err := NewConsumer().
		Brokers("localhost:9092").Group("g").
		Topics("some.legacy.topic").
		On("loan.disbursed", handlerFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	anySource := Event{Source: "whoever", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), anySource, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil (verification is opt-in on the raw path)", err)
	}

	if !*ran {
		t.Fatal("handler did not run on the raw-topics path")
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

	if !errors.Is(err, ErrNilHandler) {
		t.Fatalf("resolveHandler() = %v; want ErrNilHandler", err)
	}
}

// TestConsumerBuilder_RejectsMalformedApp pins that an app name is held to the
// producer's strict source contract at Build time.
func TestConsumerBuilder_RejectsMalformedApp(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").
		Apps("//lerian.midaz/tx").
		On("loan.disbursed", handlerFn).
		Build(context.Background())

	if !errors.Is(err, ErrInvalidSource) {
		t.Fatalf("Build() = %v; want ErrInvalidSource", err)
	}

	if !errors.Is(err, ErrConsumerInvalidConfigField) {
		t.Fatalf("Build() = %v; want ErrConsumerInvalidConfigField wrapping it", err)
	}
}

// TestConsumerBuilder_FromConfigWiresTheEnvSurface pins that the
// STREAMING_CONSUMER_* surface is reachable end to end: LoadConsumerConfig
// reads it, FromConfig adopts it, and Build honours it.
//
// Without FromConfig every one of those variables — STREAMING_CONSUMER_APPS
// included — was documented and dead.
func TestConsumerBuilder_FromConfigWiresTheEnvSurface(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "loan-projector")
	t.Setenv("STREAMING_CONSUMER_APPS", "lender,matcher")

	cfg, warnings, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if warnings == nil {
		t.Error("LoadConsumerConfig() warnings = nil; the slice is documented as never nil")
	}

	handlerFn, ran := trackingHandler()

	b := NewConsumer().FromConfig(cfg).On("loan.disbursed", handlerFn)

	if got := b.cfg.ResolvedTopics(); len(got) != 2 || got[0] != "lerian.streaming.lender" {
		t.Fatalf("resolved topics = %v; want the two app topics from STREAMING_CONSUMER_APPS", got)
	}

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	own := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), own, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run for an app named via STREAMING_CONSUMER_APPS")
	}
}
