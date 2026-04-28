package streaming

// ResolveDeliveryPolicy applies delivery policy precedence:
//
//  1. EventDefinition.DefaultPolicy
//  2. Config/runtime override
//  3. Per-call override
//
// The returned policy is Normalize()d and Validate()d exactly once. Callers
// on the Emit hot path (emit.go, publish_outbox.go) MUST trust the returned
// policy and not re-Normalize/Validate — doing so used to add 4+ redundant
// passes per Emit.
func ResolveDeliveryPolicy(
	definition EventDefinition,
	configOverride DeliveryPolicyOverride,
	callOverride DeliveryPolicyOverride,
) (DeliveryPolicy, error) {
	policy := definition.DefaultPolicy.Normalize()

	var err error

	policy, err = applyDeliveryPolicyOverride(policy, configOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

	policy, err = applyDeliveryPolicyOverride(policy, callOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

	// Single Validate() after both overrides are applied. The override
	// paths only mutate already-normalized values; a later invalid value
	// (e.g. direct=skip,outbox=never) can only appear after overrides, so
	// one Validate() here catches every shape.
	if err := policy.Validate(); err != nil {
		return DeliveryPolicy{}, err
	}

	return policy, nil
}

// applyDeliveryPolicyOverride merges override into policy. A zero-value
// override is a no-op fast path (callers on the hot path always pass
// DeliveryPolicyOverride{} when no config/call override is set).
func applyDeliveryPolicyOverride(policy DeliveryPolicy, override DeliveryPolicyOverride) (DeliveryPolicy, error) {
	if override == (DeliveryPolicyOverride{}) {
		return policy, nil
	}

	if err := override.Validate(); err != nil {
		return DeliveryPolicy{}, err
	}

	if override.Enabled != nil {
		policy.Enabled = *override.Enabled
	}

	if override.Direct != "" {
		policy.Direct = override.Direct
	}

	if override.Outbox != "" {
		policy.Outbox = override.Outbox
	}

	if override.DLQ != "" {
		policy.DLQ = override.DLQ
	}

	return policy, nil
}
