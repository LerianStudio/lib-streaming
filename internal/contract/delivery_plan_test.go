//go:build unit

package contract_test

import (
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// TestDeliveryPolicyReportsDeliveryPaths exercises the public predicate
// methods through the external test package so the test does not depend on
// any unexported helper. Previously this test lived inside `package contract`
// and called lowercase aliases (directAllowed/outboxAlways/...); the
// dual-export pattern was a maintenance burden because every behavior change
// had to update two methods. The lowercase wrappers were removed in favor of
// converting this test to the external package.
func TestDeliveryPolicyReportsDeliveryPaths(t *testing.T) {
	direct := contract.DefaultDeliveryPolicy().Normalize()
	if err := direct.Validate(); err != nil {
		t.Fatalf("DefaultDeliveryPolicy().Validate() error = %v", err)
	}
	if !direct.DirectAllowed() {
		t.Fatal("default plan should allow direct publish")
	}
	if !direct.OutboxFallbackOnCircuitOpen() {
		t.Fatal("default plan should allow circuit-open outbox fallback")
	}
	if !direct.DLQAllowed() {
		t.Fatal("default plan should allow DLQ routing")
	}
	if !direct.HasDeliveryPath() {
		t.Fatal("default plan should have a delivery path")
	}

	outboxOnly := contract.DeliveryPolicy{
		Enabled: true,
		Direct:  contract.DirectModeSkip,
		Outbox:  contract.OutboxModeAlways,
		DLQ:     contract.DLQModeNever,
	}.Normalize()
	if err := outboxOnly.Validate(); err != nil {
		t.Fatalf("outboxOnly.Validate() error = %v", err)
	}
	if outboxOnly.DirectAllowed() {
		t.Fatal("outbox-only plan should not allow direct publish")
	}
	if outboxOnly.Direct != contract.DirectModeSkip {
		t.Fatal("outbox-only plan should report skipped direct publish")
	}
	if !outboxOnly.OutboxAlways() {
		t.Fatal("outbox-only plan should report always-outbox")
	}
	if outboxOnly.DLQAllowed() {
		t.Fatal("outbox-only plan should not allow DLQ")
	}
	if !outboxOnly.HasDeliveryPath() {
		t.Fatal("outbox-only plan should have a delivery path")
	}
}

func TestDeliveryPolicyRejectsInvalidRuntimePolicy(t *testing.T) {
	err := contract.DeliveryPolicy{
		Enabled: true,
		Direct:  contract.DirectMode("bogus"),
	}.Validate()
	if !errors.Is(err, contract.ErrInvalidDeliveryPolicy) {
		t.Fatalf("DeliveryPolicy.Validate() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}
