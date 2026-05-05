package contract

// ResolveDeliveryPolicy applies delivery policy precedence:
//
//  1. EventDefinition.DefaultPolicy
//  2. Config/runtime override
//  3. Per-call override
//
// The returned policy is Normalize()d and Validate()d after each precedence
// layer. Callers on the Emit hot path (emit.go, publish_outbox.go) MUST trust
// the returned policy and not re-Normalize/Validate.
func ResolveDeliveryPolicy(
	definition EventDefinition,
	configOverride DeliveryPolicyOverride,
	callOverride DeliveryPolicyOverride,
) (DeliveryPolicy, error) {
	policy := definition.DefaultPolicy.Normalize()

	if err := policy.Validate(); err != nil {
		return DeliveryPolicy{}, err
	}

	policy, err := applyDeliveryPolicyOverride(policy, configOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

	if err := policy.Validate(); err != nil {
		return DeliveryPolicy{}, err
	}

	policy, err = applyDeliveryPolicyOverride(policy, callOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

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
