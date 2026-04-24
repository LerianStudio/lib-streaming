package streaming

// ResolveDeliveryPolicy applies delivery policy precedence:
//
//  1. EventDefinition.DefaultPolicy
//  2. Config/runtime override
//  3. Per-call override
func ResolveDeliveryPolicy(
	definition EventDefinition,
	configOverride DeliveryPolicyOverride,
	callOverride DeliveryPolicyOverride,
) (DeliveryPolicy, error) {
	policy := definition.DefaultPolicy.Normalize()
	if err := policy.Validate(); err != nil {
		return DeliveryPolicy{}, err
	}

	var err error

	policy, err = applyDeliveryPolicyOverride(policy, configOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

	policy, err = applyDeliveryPolicyOverride(policy, callOverride)
	if err != nil {
		return DeliveryPolicy{}, err
	}

	return policy, nil
}

func applyDeliveryPolicyOverride(policy DeliveryPolicy, override DeliveryPolicyOverride) (DeliveryPolicy, error) {
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

	return policy.Normalize(), nil
}
