//go:build unit

package consumer

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// TestResolvedTopics_IncludesCommandsQueues pins that naming an app in Commands
// subscribes to its ".commands" queue — and that Apps and Commands compose
// without either shadowing the other. A consumer that both watches lender's
// facts and takes lender's commands subscribes to two distinct topics.
func TestResolvedTopics_IncludesCommandsQueues(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{
		Topics:   []string{"legacy.stream"},
		Apps:     []string{"matcher"},
		Commands: []string{"lender"},
	}

	want := []string{
		"legacy.stream",
		"lerian.streaming.matcher",
		"lerian.streaming.lender.commands",
	}

	if got := cfg.ResolvedTopics(); !slices.Equal(got, want) {
		t.Errorf("ResolvedTopics() = %v; want %v", got, want)
	}
}

// TestResolvedTopics_SameAppInAppsAndCommands pins the legal overlap: lender
// commanding this consumer AND emitting facts it watches is two subscriptions,
// not a conflict.
func TestResolvedTopics_SameAppInAppsAndCommands(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{Apps: []string{"lender"}, Commands: []string{"lender"}}

	want := []string{"lerian.streaming.lender", "lerian.streaming.lender.commands"}
	if got := cfg.ResolvedTopics(); !slices.Equal(got, want) {
		t.Errorf("ResolvedTopics() = %v; want %v", got, want)
	}
}

// TestCommandTopics_IsTheStrictSet pins the set the runtime applies strict
// unmatched semantics to: derived from Commands ONLY. A raw Topics(...) entry
// that happens to spell a commands queue is NOT strict — the escape hatch has
// no allowlist and no class knowledge, so promoting it would quarantine on a
// guess.
func TestCommandTopics_IsTheStrictSet(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{
		Topics:   []string{"lerian.streaming.someone-else.commands"},
		Apps:     []string{"matcher"},
		Commands: []string{"lender"},
	}

	strict := cfg.CommandTopics()

	if _, ok := strict["lerian.streaming.lender.commands"]; !ok {
		t.Errorf("CommandTopics() = %v; want it to contain the lender commands queue", strict)
	}

	if len(strict) != 1 {
		t.Errorf("CommandTopics() = %v; want exactly the Commands-derived entry", strict)
	}
}

// TestConsumerConfig_ValidatesCommandsSources pins that a Commands entry is
// held to the same strict ce-source rule as Apps. A typo there subscribes to a
// topic that stays empty forever while the consumer reports healthy — and on a
// commands queue, "empty forever" is undelivered money-path work.
func TestConsumerConfig_ValidatesCommandsSources(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "g",
		Source:              "gw",
		Commands:            []string{"Lender"},
		RetryBackoffInitial: 1,
		RetryBackoffMax:     1,
		RetryInLoopMaxDwell: 1,
		CloseTimeout:        1,
	}

	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfigField) {
		t.Fatalf("Validate() = %v; want ErrInvalidConfigField for a malformed Commands entry", err)
	}
}

// TestConsumerConfig_CommandsAloneSatisfiesTheSubscription pins that Commands
// counts as a subscription on its own: a pure rail consumer that only takes
// commands need not also name Apps.
func TestConsumerConfig_CommandsAloneSatisfiesTheSubscription(t *testing.T) {
	t.Parallel()

	cfg := DefaultBuilderConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Group = "g"
	cfg.Source = "br-consignado-gw"
	cfg.Commands = []string{"lender"}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v; want nil (Commands alone is a subscription)", err)
	}
}

// commandsRuntime wires a runtime that subscribes to lender's FACT topic and
// lender's COMMANDS queue with one handler registered, so a single test can
// watch both verdicts on the same unmatched key.
func commandsRuntime(t *testing.T, client GroupClient, dlq dlqPublisher) *consumerRuntime {
	t.Helper()

	d := NewDispatcher().OnFrom("lender", "margin.reserve", func(context.Context, contract.Event, []byte) error {
		return nil
	})

	if err := d.Bind("lender"); err != nil {
		t.Fatalf("Bind() error = %v", err)
	}

	return newTestRuntimeCfg(t, func(cfg *ConsumerConfig) {
		cfg.Topics = nil
		cfg.Apps = []string{"lender"}
		cfg.Commands = []string{"lender"}
		cfg.ExpectSources = []string{"lender"}
	}, client, d, dlq)
}

// commandsRecord builds a record on topic carrying the given event key from
// lender, with literal CloudEvents headers.
func commandsRecord(topic, resourceType, eventType string) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Offset:    1,
		Headers: []kgo.RecordHeader{
			{Key: "ce-specversion", Value: []byte("1.0")},
			{Key: "ce-id", Value: []byte("evt-1")},
			{Key: "ce-source", Value: []byte("lender")},
			{Key: "ce-type", Value: []byte("studio.lerian.lender." + resourceType + "." + eventType)},
			{Key: "ce-time", Value: []byte("2026-08-18T12:00:00Z")},
			{Key: "ce-schemaversion", Value: []byte("1.0.0")},
			{Key: "ce-resourcetype", Value: []byte(resourceType)},
			{Key: "ce-eventtype", Value: []byte(eventType)},
		},
		Value: []byte(`{"ok":true}`),
	}
}

// TestHandleRecord_UnmatchedCommandQuarantines is the whole point of the
// feature. A command key this consumer has no handler for is UNDELIVERED WORK
// addressed to it, not noise on someone else's firehose — so it quarantines
// with cause kind unhandled_key instead of being skipped and committed.
//
// This is what turns "lender shipped a new command before the gateway deployed
// its handler" from silent money-path loss with green dashboards into a
// filling DLQ that names the owner.
func TestHandleRecord_UnmatchedCommandQuarantines(t *testing.T) {
	t.Parallel()

	c := commandsRuntime(t, newFakeGroupClient(), &fakeDLQ{})

	disp, _, cause := c.handleRecord(context.Background(),
		commandsRecord("lerian.streaming.lender.commands", "margin", "release"))

	if disp != dispositionDLQ {
		t.Fatalf("disposition = %v; want dispositionDLQ for an unmatched key on a commands queue", disp)
	}

	if cause.kind != dlqCauseUnhandledKey {
		t.Errorf("cause kind = %q; want %q", cause.kind, dlqCauseUnhandledKey)
	}

	if !errors.Is(cause.err, ErrUnhandledEvent) {
		t.Errorf("cause err = %v; want ErrUnhandledEvent", cause.err)
	}
}

// TestHandleRecord_UnmatchedFactStillIgnored pins the other half: strictness is
// PER TOPIC. The same consumer, the same unregistered key, arriving on the fact
// stream, is still skipped and committed — because a fact stream carries
// everything its producer emits and a consumer legitimately handles a handful.
func TestHandleRecord_UnmatchedFactStillIgnored(t *testing.T) {
	t.Parallel()

	dlq := &fakeDLQ{}
	c := commandsRuntime(t, newFakeGroupClient(), dlq)

	disp, _, _ := c.handleRecord(context.Background(),
		commandsRecord("lerian.streaming.lender", "margin", "release"))

	if disp != dispositionCommit {
		t.Fatalf("disposition = %v; want dispositionCommit — unmatched facts stay ignored", disp)
	}

	if dlq.count() != 0 {
		t.Errorf("DLQ publishes = %d; want 0 for an unmatched fact", dlq.count())
	}
}

// TestHandleRecord_MatchedCommandDispatches pins that strictness costs the
// happy path nothing: a command WITH a handler runs it and commits.
func TestHandleRecord_MatchedCommandDispatches(t *testing.T) {
	t.Parallel()

	c := commandsRuntime(t, newFakeGroupClient(), &fakeDLQ{})

	disp, _, _ := c.handleRecord(context.Background(),
		commandsRecord("lerian.streaming.lender.commands", "margin", "reserve"))

	if disp != dispositionCommit {
		t.Fatalf("disposition = %v; want dispositionCommit for a handled command", disp)
	}
}

// TestDispatcher_Handles pins the lookup the runtime's strict gate asks,
// including the wildcard fallback a raw-Topics consumer relies on.
func TestDispatcher_Handles(t *testing.T) {
	t.Parallel()

	d := NewDispatcher().OnFrom("lender", "margin.reserve", func(context.Context, contract.Event, []byte) error {
		return nil
	})

	if !d.Handles("lender", "margin.reserve") {
		t.Error("Handles(lender, margin.reserve) = false; want true")
	}

	if d.Handles("lender", "margin.release") {
		t.Error("Handles(lender, margin.release) = true; want false")
	}

	if d.Handles("matcher", "margin.reserve") {
		t.Error("Handles(matcher, margin.reserve) = true; want false — the app segment is load-bearing")
	}

	bare := NewDispatcher().On("margin.reserve", func(context.Context, contract.Event, []byte) error {
		return nil
	})

	if !bare.Handles("anyone", "margin.reserve") {
		t.Error("an unbound bare registration must match any source")
	}
}
