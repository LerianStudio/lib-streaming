//go:build unit

package streaming

import (
	"errors"
	"testing"
)

func TestDeliveryPolicyReportsDeliveryPaths(t *testing.T) {
	direct := DefaultDeliveryPolicy().Normalize()
	if err := direct.Validate(); err != nil {
		t.Fatalf("DefaultDeliveryPolicy().Validate() error = %v", err)
	}
	if !direct.directAllowed() {
		t.Fatal("default plan should allow direct publish")
	}
	if !direct.outboxFallbackOnCircuitOpen() {
		t.Fatal("default plan should allow circuit-open outbox fallback")
	}
	if !direct.dlqAllowed() {
		t.Fatal("default plan should allow DLQ routing")
	}
	if !direct.hasDeliveryPath() {
		t.Fatal("default plan should have a delivery path")
	}

	outboxOnly := DeliveryPolicy{
		Enabled: true,
		Direct:  DirectModeSkip,
		Outbox:  OutboxModeAlways,
		DLQ:     DLQModeNever,
	}.Normalize()
	if err := outboxOnly.Validate(); err != nil {
		t.Fatalf("outboxOnly.Validate() error = %v", err)
	}
	if outboxOnly.directAllowed() {
		t.Fatal("outbox-only plan should not allow direct publish")
	}
	if outboxOnly.Direct != DirectModeSkip {
		t.Fatal("outbox-only plan should report skipped direct publish")
	}
	if !outboxOnly.outboxAlways() {
		t.Fatal("outbox-only plan should report always-outbox")
	}
	if outboxOnly.dlqAllowed() {
		t.Fatal("outbox-only plan should not allow DLQ")
	}
	if !outboxOnly.hasDeliveryPath() {
		t.Fatal("outbox-only plan should have a delivery path")
	}
}

func TestDeliveryPolicyRejectsInvalidRuntimePolicy(t *testing.T) {
	err := DeliveryPolicy{
		Enabled: true,
		Direct:  DirectMode("bogus"),
	}.Validate()
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("DeliveryPolicy.Validate() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}
