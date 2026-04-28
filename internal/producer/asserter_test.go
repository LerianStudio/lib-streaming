package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/assert"
	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

// TestNewAsserter_ProducesUsableAsserter pins the contract that
// (*Producer).newAsserter returns a non-nil *assert.Asserter scoped to the
// component "streaming" and the caller-supplied operation. The 7 follow-on
// tasks depend on this helper being present.
func TestNewAsserter_ProducesUsableAsserter(t *testing.T) {
	t.Parallel()

	p := &Producer{logger: log.NewNop()}

	a := p.newAsserter("unit-test")
	if a == nil {
		t.Fatal("newAsserter returned nil; follow-on asserter sites would nil-deref")
	}

	// Smoke-check: a non-nil input must not fire.
	if err := a.NotNil(context.Background(), p, "smoke"); err != nil {
		t.Fatalf("asserter.NotNil unexpectedly fired on non-nil value: %v", err)
	}
}

// TestNewAsserter_NilProducerDoesNotPanic pins nil-receiver safety. Call sites
// that go through the helper must not need to guard on p == nil separately.
// Commons/assert methods are themselves nil-receiver safe, so a nil result is
// acceptable — we only require no panic.
func TestNewAsserter_NilProducerDoesNotPanic(t *testing.T) {
	t.Parallel()

	var p *Producer
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("newAsserter on nil *Producer panicked: %v", r)
		}
	}()
	_ = p.newAsserter("unit-test")
}

// TestNewAsserter_ComponentAndOperation pins the observability labels. Component
// must be "streaming" so every assertion across the library aggregates under
// one component in assertion_failed_total; operation carries the call-site
// discriminator.
func TestNewAsserter_ComponentAndOperation(t *testing.T) {
	t.Parallel()

	p := &Producer{logger: log.NewNop()}
	a := p.newAsserter("component-check")

	err := a.NotNil(context.Background(), nil, "must fire")
	if err == nil {
		t.Fatal("expected asserter.NotNil on untyped nil to return an error")
	}

	var assertErr *assert.AssertionError
	if !errors.As(err, &assertErr) {
		t.Fatalf("expected *assert.AssertionError, got %T: %v", err, err)
	}
	if assertErr.Component != "streaming" {
		t.Errorf("component = %q, want %q", assertErr.Component, "streaming")
	}
	if assertErr.Operation != "component-check" {
		t.Errorf("operation = %q, want %q", assertErr.Operation, "component-check")
	}
}
