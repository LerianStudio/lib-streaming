//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

func dispatchEvent(source, resourceType, eventType string) contract.Event {
	return contract.Event{
		Source:       source,
		ResourceType: resourceType,
		EventType:    eventType,
		EventID:      "evt-1",
	}
}

// TestDispatcher_RoutesByEventKey pins the per-event dispatch the topic
// collapse forces: one subscription now delivers a producer's WHOLE stream,
// so a consumer selects with a "<resourceType>.<eventType>" key rather than
// by subscribing to a per-event topic.
func TestDispatcher_RoutesByEventKey(t *testing.T) {
	t.Parallel()

	var got string

	d := NewDispatcher().
		On("loan.disbursed", func(_ context.Context, ev contract.Event, _ []byte) error {
			got = "disbursed:" + ev.EventID
			return nil
		}).
		On("installment.settled", func(_ context.Context, _ contract.Event, _ []byte) error {
			got = "settled"
			return nil
		})

	if err := d.Handle(context.Background(), dispatchEvent("lender", "loan", "disbursed"), nil); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if got != "disbursed:evt-1" {
		t.Errorf("dispatched to %q; want the loan.disbursed handler", got)
	}

	if err := d.Handle(context.Background(), dispatchEvent("lender", "installment", "settled"), nil); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if got != "settled" {
		t.Errorf("dispatched to %q; want the installment.settled handler", got)
	}
}

// TestDispatcher_UnmatchedIgnoredByDefault pins the SAFE default. Under
// one-topic-per-app a consumer receives every event its producer emits, most
// of which it does not care about. Erroring by default would fail-closed the
// entire sibling stream into the DLQ, so unmatched events are skipped and
// committed.
func TestDispatcher_UnmatchedIgnoredByDefault(t *testing.T) {
	t.Parallel()

	d := NewDispatcher().On("loan.disbursed", func(context.Context, contract.Event, []byte) error {
		t.Fatal("registered handler must not run for an unmatched event")
		return nil
	})

	if err := d.Handle(context.Background(), dispatchEvent("lender", "audit", "logged"), nil); err != nil {
		t.Fatalf("Handle() on unmatched event = %v; want nil (ignore is the default)", err)
	}
}

// TestDispatcher_UnmatchedErrorPolicy pins the opt-in strict mode: an
// unmatched event returns ErrUnhandledEvent, which the runtime treats as a
// handler-return error and therefore quarantines fail-closed.
func TestDispatcher_UnmatchedErrorPolicy(t *testing.T) {
	t.Parallel()

	d := NewDispatcher().
		OnUnmatched(UnmatchedError).
		On("loan.disbursed", func(context.Context, contract.Event, []byte) error { return nil })

	err := d.Handle(context.Background(), dispatchEvent("lender", "audit", "logged"), nil)
	if !errors.Is(err, ErrUnhandledEvent) {
		t.Fatalf("Handle() = %v; want ErrUnhandledEvent", err)
	}
}

// TestDispatcher_VerifiesSource pins built-in source verification. Every
// consumer repo hand-rolled this check; an event whose ce-source is not an
// expected producer is a misconfiguration or a foreign write to the app
// topic, and must never reach a business handler.
func TestDispatcher_VerifiesSource(t *testing.T) {
	t.Parallel()

	d := NewDispatcher().
		ExpectSources("lender").
		On("loan.disbursed", func(context.Context, contract.Event, []byte) error {
			t.Fatal("handler must not run for an unexpected ce-source")
			return nil
		})

	err := d.Handle(context.Background(), dispatchEvent("matcher", "loan", "disbursed"), nil)
	if !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle() = %v; want ErrUnexpectedSource", err)
	}

	// The expected producer still dispatches normally.
	ran := false
	d2 := NewDispatcher().
		ExpectSources("lender", "matcher").
		On("loan.disbursed", func(context.Context, contract.Event, []byte) error {
			ran = true
			return nil
		})

	if err := d2.Handle(context.Background(), dispatchEvent("matcher", "loan", "disbursed"), nil); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if !ran {
		t.Error("handler did not run for an expected ce-source")
	}
}

// TestDispatcher_NoExpectedSourcesAcceptsAny pins that verification is opt-in:
// a dispatcher with no declared producers accepts every source, so the raw
// .Topics() escape hatch is not forced into naming its producers.
func TestDispatcher_NoExpectedSourcesAcceptsAny(t *testing.T) {
	t.Parallel()

	ran := false
	d := NewDispatcher().On("loan.disbursed", func(context.Context, contract.Event, []byte) error {
		ran = true
		return nil
	})

	if err := d.Handle(context.Background(), dispatchEvent("whoever", "loan", "disbursed"), nil); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if !ran {
		t.Error("handler did not run; source verification must be opt-in")
	}
}

// TestDispatcher_SourceCheckPrecedesDispatch pins ordering: an unexpected
// source is rejected even when its event key IS registered, and even when the
// unmatched policy would otherwise ignore it.
func TestDispatcher_SourceCheckPrecedesDispatch(t *testing.T) {
	t.Parallel()

	d := NewDispatcher().ExpectSources("lender")

	err := d.Handle(context.Background(), dispatchEvent("matcher", "anything", "at-all"), nil)
	if !errors.Is(err, ErrUnexpectedSource) {
		t.Fatalf("Handle() = %v; want ErrUnexpectedSource even with no matching handler", err)
	}
}

// TestDispatcher_HandlerErrorPropagates pins that a business handler's error
// reaches the runtime unchanged, so the Classifier / fail-closed machinery
// still governs it.
func TestDispatcher_HandlerErrorPropagates(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("downstream down")
	d := NewDispatcher().On("loan.disbursed", func(context.Context, contract.Event, []byte) error {
		return sentinel
	})

	if err := d.Handle(context.Background(), dispatchEvent("lender", "loan", "disbursed"), nil); !errors.Is(err, sentinel) {
		t.Fatalf("Handle() = %v; want the handler's own error", err)
	}
}

// TestDispatcher_DuplicateKeyIsLastWins documents the registration rule: a
// second On for the same key replaces the first rather than silently
// double-dispatching.
func TestDispatcher_DuplicateKeyIsLastWins(t *testing.T) {
	t.Parallel()

	got := ""
	d := NewDispatcher().
		On("loan.disbursed", func(context.Context, contract.Event, []byte) error { got = "first"; return nil }).
		On("loan.disbursed", func(context.Context, contract.Event, []byte) error { got = "second"; return nil })

	if err := d.Handle(context.Background(), dispatchEvent("lender", "loan", "disbursed"), nil); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if got != "second" {
		t.Errorf("dispatched to %q; want the last registration to win", got)
	}
}

// TestDispatcher_NilSafe pins that a nil dispatcher and a nil handler func do
// not panic — the builder path can produce either on a caller wiring mistake.
func TestDispatcher_NilSafe(t *testing.T) {
	t.Parallel()

	var d *Dispatcher
	if err := d.Handle(context.Background(), dispatchEvent("lender", "loan", "disbursed"), nil); !errors.Is(err, ErrNilHandler) {
		t.Errorf("(*Dispatcher)(nil).Handle() = %v; want ErrNilHandler", err)
	}

	live := NewDispatcher().On("loan.disbursed", nil)
	if err := live.Handle(context.Background(), dispatchEvent("lender", "loan", "disbursed"), nil); err != nil {
		t.Errorf("Handle() with a nil registered func = %v; want nil (treated as unregistered)", err)
	}
}
