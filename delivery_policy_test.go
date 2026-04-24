//go:build unit

package streaming

import (
	"errors"
	"testing"
)

func TestDeliveryPolicy_DefaultAndNormalize(t *testing.T) {
	t.Parallel()

	got := (DeliveryPolicy{}).Normalize()
	want := DefaultDeliveryPolicy()
	if got != want {
		t.Fatalf("Normalize() = %#v; want %#v", got, want)
	}

	disabled := DeliveryPolicy{
		Enabled: false,
		Direct:  DirectModeSkip,
		Outbox:  OutboxModeNever,
		DLQ:     DLQModeNever,
	}.Normalize()
	if disabled.Enabled {
		t.Error("Normalize() should preserve explicit disabled policy")
	}
}

func TestDeliveryPolicy_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy DeliveryPolicy
	}{
		{name: "invalid direct", policy: DeliveryPolicy{Enabled: true, Direct: DirectMode("async")}},
		{name: "invalid outbox", policy: DeliveryPolicy{Enabled: true, Outbox: OutboxMode("sometimes")}},
		{name: "invalid dlq", policy: DeliveryPolicy{Enabled: true, DLQ: DLQMode("maybe")}},
		{name: "skip direct without primary outbox", policy: DeliveryPolicy{Enabled: true, Direct: DirectModeSkip}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.policy.Validate(); !errors.Is(err, ErrInvalidDeliveryPolicy) {
				t.Fatalf("Validate() error = %v; want ErrInvalidDeliveryPolicy", err)
			}
		})
	}
}

func TestDeliveryPolicyOverride_Validate(t *testing.T) {
	t.Parallel()

	override := DeliveryPolicyOverride{Outbox: OutboxMode("queue")}
	if err := override.Validate(); !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("Validate() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

func TestDeliveryPolicyResolver_Precedence(t *testing.T) {
	t.Parallel()

	enabledFalse := false
	enabledTrue := true

	definition := EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
		DefaultPolicy: DeliveryPolicy{
			Enabled: true,
			Direct:  DirectModeDirect,
			Outbox:  OutboxModeFallbackOnCircuitOpen,
			DLQ:     DLQModeOnRoutableFailure,
		},
	}
	configOverride := DeliveryPolicyOverride{
		Enabled: &enabledFalse,
		Outbox:  OutboxModeAlways,
	}
	callOverride := DeliveryPolicyOverride{
		Enabled: &enabledTrue,
		DLQ:     DLQModeNever,
	}

	got, err := ResolveDeliveryPolicy(definition, configOverride, callOverride)
	if err != nil {
		t.Fatalf("ResolveDeliveryPolicy() error = %v", err)
	}

	want := DeliveryPolicy{
		Enabled: true,
		Direct:  DirectModeDirect,
		Outbox:  OutboxModeAlways,
		DLQ:     DLQModeNever,
	}
	if got != want {
		t.Fatalf("ResolveDeliveryPolicy() = %#v; want %#v", got, want)
	}
}

func TestDeliveryPolicyResolver_InvalidOverride(t *testing.T) {
	t.Parallel()

	_, err := ResolveDeliveryPolicy(
		EventDefinition{},
		DeliveryPolicyOverride{Direct: DirectMode("async")},
		DeliveryPolicyOverride{},
	)
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("ResolveDeliveryPolicy() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}
