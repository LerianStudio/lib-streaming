//go:build unit

package streaming

import (
	"context"
	"errors"
	"slices"
	"strings"
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

// newDispatchBuilder returns a builder with the required fields filled, so each
// test states only the knob it is about.
func newDispatchBuilder() *ConsumerBuilder {
	return NewConsumer().Brokers("localhost:9092").Group("g").Source("test-consumer")
}

// resolveWithSources resolves the builder and returns the handler plus the
// ce-source allowlist the RUNTIME will verify against.
//
// Source verification lives in the runtime now, not in the dispatcher, so the
// allowlist — not a Handle call — is what a builder test can assert on.
func resolveWithSources(t *testing.T, b *ConsumerBuilder) (Handler, []string) {
	t.Helper()

	handler, err := b.resolveHandler()
	if err != nil {
		t.Fatalf("resolveHandler() error = %v", err)
	}

	return handler, b.cfg.ExpectSources
}

// TestConsumerBuilder_AppsResolveSubscription pins subscribe-by-application on
// the public surface: naming producers is enough, and the caller never spells
// the "lerian.streaming." derivation.
func TestConsumerBuilder_AppsResolveSubscription(t *testing.T) {
	t.Parallel()

	b := newDispatchBuilder().Apps("lender", "matcher")

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

	b := newDispatchBuilder().Apps("lender").On("loan.disbursed", handlerFn)

	handler, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender"}) {
		t.Fatalf("expected sources = %v; want [lender] derived from Apps", sources)
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

	handlerFn, _ := trackingHandler()

	b := newDispatchBuilder().
		Apps("lender").
		ExpectSources("lender", "matcher").
		OnFrom("lender", "loan.disbursed", handlerFn)

	_, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender", "matcher"}) {
		t.Fatalf("expected sources = %v; want the explicit list verbatim", sources)
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

	_, err := newDispatchBuilder().
		Apps("lender").
		ExpectSources("matcher").
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrExpectSourcesMissingApp) {
		t.Fatalf("resolveHandler() = %v; want ErrExpectSourcesMissingApp", err)
	}
}

// TestConsumerBuilder_EnvExpectSourcesMissingAppNamesTheVariable pins the same
// rule for the environment-supplied list, and pins that the failure NAMES
// STREAMING_CONSUMER_EXPECT_SOURCES.
//
// Diagnosis time is the whole point: an operator reading "ExpectSources(...)
// omits an app" goes hunting for a fluent call that does not exist anywhere in
// the service, when the value came from a fleet-wide environment variable.
func TestConsumerBuilder_EnvExpectSourcesMissingAppNamesTheVariable(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	cfg := consumer.DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Source = "test-consumer"
	cfg.Apps = []string{"lender", "matcher"}
	cfg.ExpectSources = []string{"lender"}

	_, err := NewConsumer().
		FromConfig(cfg).
		OnFrom("lender", "loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrExpectSourcesMissingApp) {
		t.Fatalf("resolveHandler() = %v; want ErrExpectSourcesMissingApp", err)
	}

	if !strings.Contains(err.Error(), "STREAMING_CONSUMER_EXPECT_SOURCES") {
		t.Errorf("error = %q; want it to name the environment variable the list came from", err)
	}
}

// TestConsumerBuilder_RejectsMalformedExpectSource pins that an ExpectSources
// entry is held to the producer's strict source contract at Build time. A
// hyphen/underscore typo there matches no real producer, so every record would
// be quarantined while the consumer reported healthy.
func TestConsumerBuilder_RejectsMalformedExpectSource(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := newDispatchBuilder().
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

	b := newDispatchBuilder().
		Apps("lender").
		Topics("some.legacy.topic").
		On("loan.disbursed", handlerFn)

	if _, err := b.resolveHandler(); !errors.Is(err, ErrAmbiguousSourceVerification) {
		t.Fatalf("resolveHandler() = %v; want ErrAmbiguousSourceVerification", err)
	}

	explicitFn, _ := trackingHandler()

	explicit := newDispatchBuilder().
		Apps("lender").
		Topics("some.legacy.topic").
		ExpectSources("lender", "legacy-writer").
		OnFrom("legacy-writer", "loan.disbursed", explicitFn)

	_, sources := resolveWithSources(t, explicit)

	if !slices.Equal(sources, []string{"lender", "legacy-writer"}) {
		t.Fatalf("expected sources = %v; want both the app and the raw-topic producer", sources)
	}
}

// TestConsumerBuilder_UnmatchedDefaultIgnores pins the safe default at the
// public boundary: sibling events on the app stream are skipped, not
// quarantined — and demonstrably NOT handed to a handler.
func TestConsumerBuilder_UnmatchedDefaultIgnores(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, _ := resolveWithSources(t, newDispatchBuilder().Apps("lender").On("loan.disbursed", handlerFn))

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

	handler, _ := resolveWithSources(t, newDispatchBuilder().
		Apps("lender").
		UnmatchedPolicy(UnmatchedError).
		On("loan.disbursed", handlerFn))

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

	_, err := newDispatchBuilder().
		Apps("lender").
		Handler(rawStreamHandler{}).
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrHandlerAndDispatchBothSet) {
		t.Fatalf("resolveHandler() = %v; want ErrHandlerAndDispatchBothSet", err)
	}
}

// TestConsumerBuilder_HandlerModeArmsSourceVerification is the mode that needed
// it most, and the one that had none.
//
// A whole-stream Handler receives EVERY record on a topic whose write ACL it
// does not control, so it is the mode a foreign write reaches first — and
// ExpectSources used to be a hard build error there, which meant a fleet-wide
// STREAMING_CONSUMER_EXPECT_SOURCES CrashLooped every Handler-mode service with
// no in-API way out. Verification runs in the runtime now, so the allowlist is
// armed in both modes.
func TestConsumerBuilder_HandlerModeArmsSourceVerification(t *testing.T) {
	t.Parallel()

	b := newDispatchBuilder().
		Apps("lender").
		ExpectSources("lender", "legacy-writer").
		Handler(rawStreamHandler{})

	handler, sources := resolveWithSources(t, b)

	if _, ok := handler.(rawStreamHandler); !ok {
		t.Fatalf("resolveHandler() returned %T; want the caller's whole-stream handler", handler)
	}

	if !slices.Equal(sources, []string{"lender", "legacy-writer"}) {
		t.Fatalf("expected sources = %v; want the explicit list armed under a whole-stream Handler", sources)
	}
}

// TestConsumerBuilder_HandlerModeDerivesSourcesFromApps pins that the free
// verification Apps buys applies under a whole-stream Handler too.
func TestConsumerBuilder_HandlerModeDerivesSourcesFromApps(t *testing.T) {
	t.Parallel()

	b := newDispatchBuilder().Apps("lender", "matcher").Handler(rawStreamHandler{})

	_, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender", "matcher"}) {
		t.Fatalf("expected sources = %v; want both apps", sources)
	}
}

// TestConsumerBuilder_HandlerWithUnmatchedPolicyFailsPrecisely pins the
// remaining dispatch-only knob.
//
// UnmatchedPolicy decides what the DISPATCHER does with a key it has no handler
// for. A whole-stream Handler receives every record and selects for itself, so
// the knob does nothing — and an operator who wrote UnmatchedPolicy(
// UnmatchedError) believes unknown keys are being quarantined when they are
// not.
//
// It still must not blame On(...), which the caller never wrote: allocating the
// dispatcher is not the same as asking for dispatch.
func TestConsumerBuilder_HandlerWithUnmatchedPolicyFailsPrecisely(t *testing.T) {
	t.Parallel()

	_, err := newDispatchBuilder().
		Apps("lender").
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
// one table: under a whole-stream Handler, each GENUINELY dispatch-only knob
// fails with its own sentinel, while ExpectSources — no longer dispatch-only —
// builds clean.
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
		{
			"OnFrom",
			func(b *ConsumerBuilder) *ConsumerBuilder { return b.OnFrom("lender", "loan.disbursed", handlerFn) },
			ErrHandlerAndDispatchBothSet,
		},
		{"UnmatchedPolicy", func(b *ConsumerBuilder) *ConsumerBuilder { return b.UnmatchedPolicy(UnmatchedError) }, ErrHandlerAndUnmatchedPolicyBothSet},
		{"ExpectSources is no longer dispatch-only", func(b *ConsumerBuilder) *ConsumerBuilder { return b.ExpectSources("lender") }, nil},
		{
			"ExpectSources from the environment",
			func(b *ConsumerBuilder) *ConsumerBuilder {
				cfg := b.cfg
				cfg.ExpectSources = []string{"lender"}

				return b.FromConfig(cfg)
			},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := tt.apply(newDispatchBuilder().Apps("lender")).Handler(rawStreamHandler{})

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
	cfg.Source = "test-consumer"
	cfg.Apps = []string{"lender"}
	cfg.Topics = []string{"some.legacy.topic"}
	cfg.ExpectSources = []string{"lender", "legacy-writer"}

	b := NewConsumer().FromConfig(cfg).OnFrom("legacy-writer", "loan.disbursed", handlerFn)

	_, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender", "legacy-writer"}) {
		t.Fatalf("expected sources = %v; want the env allowlist verbatim", sources)
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
	cfg.Source = "test-consumer"
	cfg.Apps = []string{"lender"}
	cfg.ExpectSources = []string{"lender", "stale-from-env"}

	b := NewConsumer().
		FromConfig(cfg).
		ExpectSources("lender").
		On("loan.disbursed", handlerFn)

	_, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender"}) {
		t.Fatalf("expected sources = %v; want the fluent list to replace the env one", sources)
	}
}

// TestConsumerBuilder_RawTopicsEscapeHatch pins that the raw path still works
// and leaves source verification off, so a consumer of a foreign stream is not
// forced to enumerate producers.
func TestConsumerBuilder_RawTopicsEscapeHatch(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, sources := resolveWithSources(t, newDispatchBuilder().
		Topics("some.legacy.topic").
		On("loan.disbursed", handlerFn))

	if len(sources) != 0 {
		t.Fatalf("expected sources = %v; want empty (verification is opt-in on the raw path)", sources)
	}

	anySource := Event{Source: "whoever", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), anySource, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run on the raw-topics path")
	}
}

// TestConsumerBuilder_HomonymsFromTwoAppsReachTwoHandlers is the reason the
// dispatch key carries the producing application at all.
//
// "loan.disbursed" from lender and "loan.disbursed" from matcher are different
// facts with different payloads. On a key that ignored the source they
// collapsed into one registration — whichever was written last swallowed both,
// and the wrong handler parsed the wrong payload without an error anywhere.
func TestConsumerBuilder_HomonymsFromTwoAppsReachTwoHandlers(t *testing.T) {
	t.Parallel()

	lenderFn, lenderRan := trackingHandler()
	matcherFn, matcherRan := trackingHandler()

	handler, _ := resolveWithSources(t, newDispatchBuilder().
		Apps("lender", "matcher").
		OnFrom("lender", "loan.disbursed", lenderFn).
		OnFrom("matcher", "loan.disbursed", matcherFn))

	fromLender := Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), fromLender, nil); err != nil {
		t.Fatalf("Handle(lender) = %v; want nil", err)
	}

	if !*lenderRan || *matcherRan {
		t.Fatalf("lender's event reached lender=%v matcher=%v; want only lender", *lenderRan, *matcherRan)
	}

	*lenderRan = false

	fromMatcher := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), fromMatcher, nil); err != nil {
		t.Fatalf("Handle(matcher) = %v; want nil", err)
	}

	if !*matcherRan || *lenderRan {
		t.Fatalf("matcher's event reached lender=%v matcher=%v; want only matcher", *lenderRan, *matcherRan)
	}
}

// TestConsumerBuilder_BareOnFailsUnderMultipleApps pins the ambiguity as a BUILD
// failure rather than a coin flip at runtime.
//
// With two producers in scope a bare event key does not say whose event it is.
// Binding it to one of them silently would hand the other app's payload to the
// wrong handler — and the fleet really does have byte-identical event
// vocabularies across apps, so this is the common case.
func TestConsumerBuilder_BareOnFailsUnderMultipleApps(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := newDispatchBuilder().
		Apps("lender", "matcher").
		On("loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrBareOnWithMultipleApps) {
		t.Fatalf("resolveHandler() = %v; want ErrBareOnWithMultipleApps", err)
	}

	if !strings.Contains(err.Error(), "loan.disbursed") {
		t.Errorf("error = %q; want it to name the ambiguous registration", err)
	}
}

// TestConsumerBuilder_BareOnBindsToTheSoleApp keeps the common case terse: one
// producer, no app argument, everything binds to it.
func TestConsumerBuilder_BareOnBindsToTheSoleApp(t *testing.T) {
	t.Parallel()

	handlerFn, ran := trackingHandler()

	handler, _ := resolveWithSources(t, newDispatchBuilder().
		Apps("lender").
		On("loan.disbursed", handlerFn))

	if err := handler.Handle(context.Background(), Event{Source: "lender", ResourceType: "loan", EventType: "disbursed"}, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("bare On did not bind to the sole subscribed app")
	}
}

// TestConsumerBuilder_OnFromUnknownAppFails pins that a handler nothing could
// ever reach is a wiring mistake, not a filter. Left unchecked it is invisible:
// the build passes, the consumer reports Healthy, and the handler never fires.
func TestConsumerBuilder_OnFromUnknownAppFails(t *testing.T) {
	t.Parallel()

	handlerFn, _ := trackingHandler()

	_, err := newDispatchBuilder().
		Apps("lender").
		OnFrom("matcher", "loan.disbursed", handlerFn).
		resolveHandler()

	if !errors.Is(err, ErrUnknownDispatchApp) {
		t.Fatalf("resolveHandler() = %v; want ErrUnknownDispatchApp", err)
	}
}

// TestConsumerBuilder_DispatchWithoutHandlersFails pins that a consumer that
// registered no handlers at all is a wiring bug, not a silent no-op consumer
// that commits an entire stream unread.
func TestConsumerBuilder_DispatchWithoutHandlersFails(t *testing.T) {
	t.Parallel()

	_, err := newDispatchBuilder().
		Apps("lender").
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

	_, err := newDispatchBuilder().
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

	b := NewConsumer().FromConfig(cfg).OnFrom("matcher", "loan.disbursed", handlerFn)

	if got := b.cfg.ResolvedTopics(); len(got) != 2 || got[0] != "lerian.streaming.lender" {
		t.Fatalf("resolved topics = %v; want the two app topics from STREAMING_CONSUMER_APPS", got)
	}

	handler, sources := resolveWithSources(t, b)

	if !slices.Equal(sources, []string{"lender", "matcher"}) {
		t.Fatalf("expected sources = %v; want both apps from STREAMING_CONSUMER_APPS", sources)
	}

	own := Event{Source: "matcher", ResourceType: "loan", EventType: "disbursed"}
	if err := handler.Handle(context.Background(), own, nil); err != nil {
		t.Fatalf("Handle() = %v; want nil", err)
	}

	if !*ran {
		t.Fatal("handler did not run for an app named via STREAMING_CONSUMER_APPS")
	}
}

// TestConsumerBuilder_DuplicateAllowlistEntriesAreNotAmbiguity pins that a
// repeated app name is still ONE producer.
//
// Ambiguity is decided on the length of the ce-source allowlist, and the
// allowlist was cloned verbatim from whatever the caller supplied. So
// STREAMING_CONSUMER_APPS="lender,lender" — an ordinary CSV paste — and two
// ExpectSources("lender") calls both produced a two-entry allowlist naming the
// same app twice, and a bare On(...) failed the build as "ambiguous between
// lender and lender". The ExpectSources contract already promises the UNION of
// every call, and a union has no duplicates.
func TestConsumerBuilder_DuplicateAllowlistEntriesAreNotAmbiguity(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func(*ConsumerBuilder) *ConsumerBuilder
	}{
		{
			name:  "repeated Apps entry",
			build: func(b *ConsumerBuilder) *ConsumerBuilder { return b.Apps("lender", "lender") },
		},
		{
			name: "repeated ExpectSources calls",
			build: func(b *ConsumerBuilder) *ConsumerBuilder {
				return b.Apps("lender").ExpectSources("lender").ExpectSources("lender")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handlerFn, _ := trackingHandler()

			b := tc.build(newDispatchBuilder()).On("loan.disbursed", handlerFn)

			if _, err := b.resolveHandler(); err != nil {
				t.Fatalf("resolveHandler() = %v; want nil — one app named twice is still one app", err)
			}

			if got := b.cfg.ExpectSources; len(got) != 1 || got[0] != "lender" {
				t.Errorf("ExpectSources = %v; want exactly [lender]", got)
			}
		})
	}
}
