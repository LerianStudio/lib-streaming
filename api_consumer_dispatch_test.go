//go:build unit

package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/v3/internal/consumer"
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

	b := NewConsumer().Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender", "matcher")

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
		Source("test-consumer").
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
		Source("test-consumer").
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
		Source("test-consumer").
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
		Source("test-consumer").
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
		Source("test-consumer").
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
		Source("test-consumer").
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
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
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
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
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
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
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
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
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

// TestConsumerBuilder_HandlerWithUnmatchedPolicyFailsPrecisely pins the third
// dispatch-only knob under the same rule as the other two.
//
// UnmatchedPolicy decides what the DISPATCHER does with a key it has no handler
// for. A whole-stream Handler receives every record and selects for itself, so
// the knob does nothing — and an operator who wrote UnmatchedPolicy(
// UnmatchedError) believes unknown keys are being quarantined when they are
// not. Every dispatch-only knob now errors under Handler; none is inert.
//
// It still must not blame On(...), which the caller never wrote: allocating the
// dispatcher is not the same as asking for dispatch.
func TestConsumerBuilder_HandlerWithUnmatchedPolicyFailsPrecisely(t *testing.T) {
	t.Parallel()

	_, err := NewConsumer().
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
		UnmatchedPolicy(UnmatchedError).
		Handler(rawStreamHandler{}).
		resolveHandler()

	if !errors.Is(err, ErrHandlerAndUnmatchedPolicyBothSet) {
		t.Fatalf("resolveHandler() = %v; want ErrHandlerAndUnmatchedPolicyBothSet", err)
	}

	if errors.Is(err, ErrHandlerAndDispatchBothSet) {
		t.Fatal("resolveHandler() blamed On(...), which the caller never wrote")
	}
}

// TestConsumerBuilder_HandlerRejectsEveryDispatchOnlyKnob is the rule itself, as
// one table: under a whole-stream Handler, each dispatch-only knob fails with
// its own sentinel, and a Handler with none of them builds clean.
func TestConsumerBuilder_HandlerRejectsEveryDispatchOnlyKnob(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	tests := []struct {
		name  string
		apply func(*ConsumerBuilder) *ConsumerBuilder
		want  error
	}{
		{"no dispatch-only knob", func(b *ConsumerBuilder) *ConsumerBuilder { return b }, nil},
		{"On", func(b *ConsumerBuilder) *ConsumerBuilder { return b.On("loan.disbursed", handlerFn) }, ErrHandlerAndDispatchBothSet},
		{"UnmatchedPolicy", func(b *ConsumerBuilder) *ConsumerBuilder { return b.UnmatchedPolicy(UnmatchedError) }, ErrHandlerAndUnmatchedPolicyBothSet},
		{"ExpectSources", func(b *ConsumerBuilder) *ConsumerBuilder { return b.ExpectSources("lender") }, ErrHandlerAndExpectSourcesBothSet},
		{
			"ExpectSources from the environment",
			func(b *ConsumerBuilder) *ConsumerBuilder {
				cfg := b.cfg
				cfg.ExpectSources = []string{"lender"}

				return b.FromConfig(cfg)
			},
			ErrHandlerAndExpectSourcesBothSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := tt.apply(NewConsumer().Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender")).
				Handler(rawStreamHandler{})

			handler, err := b.resolveHandler()

			if tt.want == nil {
				if err != nil {
					t.Fatalf("resolveHandler() = %v; want nil", err)
				}

				if _, ok := handler.(rawStreamHandler); !ok {
					t.Fatalf("resolveHandler() returned %T; want the caller's whole-stream handler", handler)
				}

				return
			}

			if !errors.Is(err, tt.want) {
				t.Fatalf("resolveHandler() = %v; want %v", err, tt.want)
			}

			if handler != nil {
				t.Errorf("resolveHandler() returned a %T alongside its error; want nil", handler)
			}
		})
	}
}

// TestConsumerBuilder_EnvExpectSourcesResolvesTheAppsPlusTopicsRefusal pins the
// env-only escape from the Apps+Topics ambiguity.
//
// Build hard-fails when both subscription styles are set and no allowlist is
// stated, and until STREAMING_CONSUMER_EXPECT_SOURCES existed that shape could
// only be resolved in code — an operator wiring both from the environment had
// no way out at all.
func TestConsumerBuilder_EnvExpectSourcesResolvesTheAppsPlusTopicsRefusal(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	cfg := consumer.DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Apps = []string{"lender"}
	cfg.Topics = []string{"some.legacy.topic"}
	cfg.ExpectSources = []string{"lender", "legacy-writer"}

	handler, err := NewConsumer().
		FromConfig(cfg).
		On("loan.disbursed", handlerFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() = %v; want nil (the env allowlist is an explicit list)", err)
	}

	legacy := Event{Source: "legacy-writer", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), legacy, nil); err != nil {
		t.Fatalf("Handle(raw-topic producer) = %v; want nil (the env allowlist admits it)", err)
	}

	foreign := Event{Source: "someone-else", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), foreign, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(foreign producer) = %v; want ErrUnexpectedSource", err)
	}
}

// TestConsumerBuilder_FluentExpectSourcesOverridesTheEnvironment pins the
// precedence: a fluent ExpectSources call wins over
// STREAMING_CONSUMER_EXPECT_SOURCES, matching every other builder setter
// applied after FromConfig.
func TestConsumerBuilder_FluentExpectSourcesOverridesTheEnvironment(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	cfg := consumer.DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Apps = []string{"lender"}
	cfg.ExpectSources = []string{"lender", "stale-from-env"}

	handler, err := NewConsumer().
		FromConfig(cfg).
		ExpectSources("lender").
		On("loan.disbursed", handlerFn).
		resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() = %v; want nil", err)
	}

	stale := Event{Source: "stale-from-env", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), stale, nil); !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle(env-only source) = %v; want ErrUnexpectedSource (the fluent list replaced it)", err)
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
		Source("test-consumer").
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
		Brokers("localhost:9092").Group("g").Source("test-consumer").Apps("lender").
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
		Source("test-consumer").
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
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "loan-projector")
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
