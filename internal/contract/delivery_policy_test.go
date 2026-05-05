//go:build unit

package contract

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
		EventDefinition{
			Key:           "transaction.created",
			ResourceType:  "transaction",
			EventType:     "created",
			DefaultPolicy: DefaultDeliveryPolicy(),
		},
		DeliveryPolicyOverride{Direct: DirectMode("async")},
		DeliveryPolicyOverride{},
	)
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("ResolveDeliveryPolicy() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

// TestDeliveryPolicyResolver_InvalidCallOverrideRejectsResolution asserts the
// per-call override cannot create an invalid resolved policy on top of an
// otherwise-valid stack. The cross-field rule (Direct=skip requires
// Outbox=always) must be re-validated AFTER the call override is merged —
// not just at definition or config time. Without the post-merge Validate(),
// a service could call Emit with PolicyOverride{Direct: DirectModeSkip}
// against a definition whose default is Outbox=fallback_on_circuit_open and
// silently produce a policy that has no delivery path at all.
func TestDeliveryPolicyResolver_InvalidCallOverrideRejectsResolution(t *testing.T) {
	t.Parallel()

	// Definition default and config override are both valid in isolation
	// AND in combination. The call override layer is what makes the merged
	// policy contradictory: Direct=skip requires Outbox=always, but the
	// stack's Outbox is "never" by virtue of the config override.
	_, err := ResolveDeliveryPolicy(
		EventDefinition{
			Key:           "transaction.created",
			ResourceType:  "transaction",
			EventType:     "created",
			DefaultPolicy: DefaultDeliveryPolicy(),
		},
		DeliveryPolicyOverride{Outbox: OutboxModeNever},
		DeliveryPolicyOverride{Direct: DirectModeSkip},
	)
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("ResolveDeliveryPolicy(invalid call override) err = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

func TestDeliveryPolicyResolver_InvalidConfigLayerCannotBeRescuedByCallOverride(t *testing.T) {
	t.Parallel()

	_, err := ResolveDeliveryPolicy(
		EventDefinition{
			Key:           "transaction.created",
			ResourceType:  "transaction",
			EventType:     "created",
			DefaultPolicy: DefaultDeliveryPolicy(),
		},
		DeliveryPolicyOverride{Direct: DirectModeSkip},
		DeliveryPolicyOverride{Outbox: OutboxModeAlways},
	)
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("ResolveDeliveryPolicy() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

func TestDeliveryPolicyResolver_InvalidDefaultCannotBeRescuedByConfigOverride(t *testing.T) {
	t.Parallel()

	_, err := ResolveDeliveryPolicy(
		EventDefinition{
			Key:          "transaction.created",
			ResourceType: "transaction",
			EventType:    "created",
			DefaultPolicy: DeliveryPolicy{
				Enabled: true,
				Direct:  DirectModeSkip,
				Outbox:  OutboxModeNever,
				DLQ:     DLQModeOnRoutableFailure,
			},
		},
		DeliveryPolicyOverride{Outbox: OutboxModeAlways},
		DeliveryPolicyOverride{},
	)
	if !errors.Is(err, ErrInvalidDeliveryPolicy) {
		t.Fatalf("ResolveDeliveryPolicy() error = %v; want ErrInvalidDeliveryPolicy", err)
	}
}

// TestCloneDeliveryPolicyOverrides_DeepCopiesEnabledPointer asserts the
// Enabled *bool field is deep-copied so mutating the source pointee does NOT
// leak into the clone. Without the deep copy, a cached Config (e.g. from
// LoadConfig) would propagate mutations back into the Producer's snapshot.
func TestCloneDeliveryPolicyOverrides_DeepCopiesEnabledPointer(t *testing.T) {
	t.Parallel()

	enabled := true
	src := map[string]DeliveryPolicyOverride{
		"foo": {Enabled: &enabled},
	}

	dst := cloneDeliveryPolicyOverrides(src)
	if dst["foo"].Enabled == nil {
		t.Fatal("cloneDeliveryPolicyOverrides() Enabled = nil; want non-nil")
	}
	if !*dst["foo"].Enabled {
		t.Fatal("cloneDeliveryPolicyOverrides() *Enabled = false; want true before mutation")
	}

	// Mutate the source pointee. The clone MUST remain unchanged.
	*src["foo"].Enabled = false

	if !*dst["foo"].Enabled {
		t.Error("cloneDeliveryPolicyOverrides() did not deep-copy Enabled pointer: clone reflects source mutation")
	}
}

// TestCloneDeliveryPolicyOverrides_NilAndEmpty asserts the nil/empty input
// contract: both nil and an empty map normalize to a nil return. Callers MUST
// not rely on a non-nil empty-map return — the clone short-circuits on
// len(src)==0.
func TestCloneDeliveryPolicyOverrides_NilAndEmpty(t *testing.T) {
	t.Parallel()

	if got := cloneDeliveryPolicyOverrides(nil); got != nil {
		t.Errorf("cloneDeliveryPolicyOverrides(nil) = %v; want nil", got)
	}
	if got := cloneDeliveryPolicyOverrides(map[string]DeliveryPolicyOverride{}); got != nil {
		t.Errorf("cloneDeliveryPolicyOverrides(empty map) = %v; want nil (normalized)", got)
	}
}
