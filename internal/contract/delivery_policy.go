package contract

import "fmt"

// DirectMode controls whether a resolved emit attempts a direct broker publish.
type DirectMode string

const (
	// DirectModeDirect publishes to the resolved Kafka topic.
	DirectModeDirect DirectMode = "direct"
	// DirectModeSkip never publishes directly. Other delivery paths, such as
	// outbox, may still apply based on the resolved policy.
	DirectModeSkip DirectMode = "skip"
)

// OutboxMode controls when the producer writes an event to the app-owned
// outbox boundary.
type OutboxMode string

const (
	// OutboxModeNever disables outbox writes for the event.
	OutboxModeNever OutboxMode = "never"
	// OutboxModeFallbackOnCircuitOpen writes to outbox only when the circuit
	// breaker is open and an outbox writer is configured.
	OutboxModeFallbackOnCircuitOpen OutboxMode = "fallback_on_circuit_open"
	// OutboxModeAlways writes to outbox as the primary delivery path.
	OutboxModeAlways OutboxMode = "always"
)

// DLQMode controls whether routable publish failures are copied to the
// per-topic DLQ.
type DLQMode string

const (
	// DLQModeNever disables DLQ routing for the event.
	DLQModeNever DLQMode = "never"
	// DLQModeOnRoutableFailure keeps the existing DLQ routing rule: all error
	// classes except validation and context cancellation route to DLQ.
	DLQModeOnRoutableFailure DLQMode = "on_routable_failure"
)

// DeliveryPolicy is the fully-resolved, concrete delivery policy for one
// event definition.
//
// The zero value is intentionally treated as "use DefaultDeliveryPolicy" by
// Normalize. That lets callers define EventDefinition values without copying
// boilerplate policy fields, while still allowing disabled defaults by setting
// at least one mode explicitly.
//
// Normalize semantics: Normalize only backfills the Direct/Outbox/DLQ mode
// fields from DefaultDeliveryPolicy. It does NOT set Enabled. A partial policy
// like DeliveryPolicy{Outbox: OutboxModeAlways} remains disabled (Enabled=false)
// after Normalize — to opt into the policy, callers MUST set Enabled=true
// explicitly. The only exception is the full zero value, which normalizes to
// DefaultDeliveryPolicy (Enabled=true).
type DeliveryPolicy struct {
	Enabled bool       `json:"enabled"`
	Direct  DirectMode `json:"direct"`
	Outbox  OutboxMode `json:"outbox"`
	DLQ     DLQMode    `json:"dlq"`
}

// DefaultDeliveryPolicy returns the package default policy applied when an
// EventDefinition does not specify one.
func DefaultDeliveryPolicy() DeliveryPolicy {
	return DeliveryPolicy{
		Enabled: true,
		Direct:  DirectModeDirect,
		Outbox:  OutboxModeFallbackOnCircuitOpen,
		DLQ:     DLQModeOnRoutableFailure,
	}
}

// Normalize fills omitted Direct/Outbox/DLQ modes with package defaults. A full
// zero-value policy normalizes to DefaultDeliveryPolicy (which is Enabled=true).
// For any other partial policy, Normalize does NOT set Enabled — callers MUST
// set Enabled=true explicitly to opt into delivery. See the DeliveryPolicy
// godoc for the full semantics.
func (p DeliveryPolicy) Normalize() DeliveryPolicy {
	defaults := DefaultDeliveryPolicy()
	if p == (DeliveryPolicy{}) {
		return defaults
	}

	if p.Direct == "" {
		p.Direct = defaults.Direct
	}

	if p.Outbox == "" {
		p.Outbox = defaults.Outbox
	}

	if p.DLQ == "" {
		p.DLQ = defaults.DLQ
	}

	return p
}

// Validate reports unsupported delivery mode values.
//
// Contract: Validate ASSUMES a normalized input. Callers must call Normalize()
// before Validate(); the zero-value policy is NOT auto-normalized here.
// ResolveDeliveryPolicy + applyDeliveryPolicyOverride guarantee this contract
// for the emit hot path — re-normalizing inside Validate used to add ~3 allocs
// per Emit on the hot path for zero benefit.
func (p DeliveryPolicy) Validate() error {
	if !isValidDirectMode(p.Direct) {
		return fmt.Errorf("%w: direct=%q", ErrInvalidDeliveryPolicy, p.Direct)
	}

	if !isValidOutboxMode(p.Outbox) {
		return fmt.Errorf("%w: outbox=%q", ErrInvalidDeliveryPolicy, p.Outbox)
	}

	if !isValidDLQMode(p.DLQ) {
		return fmt.Errorf("%w: dlq=%q", ErrInvalidDeliveryPolicy, p.DLQ)
	}

	if p.Enabled && p.Direct == DirectModeSkip && p.Outbox != OutboxModeAlways {
		return fmt.Errorf("%w: direct=skip requires outbox=always", ErrInvalidDeliveryPolicy)
	}

	return nil
}

// DeliveryPolicyOverride carries optional policy changes from config or from
// a single EmitRequest. Nil Enabled means "do not override enabled".
type DeliveryPolicyOverride struct {
	Enabled *bool      `json:"enabled,omitempty"`
	Direct  DirectMode `json:"direct,omitempty"`
	Outbox  OutboxMode `json:"outbox,omitempty"`
	DLQ     DLQMode    `json:"dlq,omitempty"`
}

// Validate reports unsupported non-empty override mode values.
func (o DeliveryPolicyOverride) Validate() error {
	if o.Direct != "" && !isValidDirectMode(o.Direct) {
		return fmt.Errorf("%w: direct=%q", ErrInvalidDeliveryPolicy, o.Direct)
	}

	if o.Outbox != "" && !isValidOutboxMode(o.Outbox) {
		return fmt.Errorf("%w: outbox=%q", ErrInvalidDeliveryPolicy, o.Outbox)
	}

	if o.DLQ != "" && !isValidDLQMode(o.DLQ) {
		return fmt.Errorf("%w: dlq=%q", ErrInvalidDeliveryPolicy, o.DLQ)
	}

	return nil
}

func isValidDirectMode(mode DirectMode) bool {
	switch mode {
	case DirectModeDirect, DirectModeSkip:
		return true
	default:
		return false
	}
}

func isValidOutboxMode(mode OutboxMode) bool {
	switch mode {
	case OutboxModeNever, OutboxModeFallbackOnCircuitOpen, OutboxModeAlways:
		return true
	default:
		return false
	}
}

func isValidDLQMode(mode DLQMode) bool {
	switch mode {
	case DLQModeNever, DLQModeOnRoutableFailure:
		return true
	default:
		return false
	}
}

// DirectAllowed reports whether the policy permits a direct broker publish.
func (p DeliveryPolicy) DirectAllowed() bool {
	return p.Enabled && p.Direct == DirectModeDirect
}

// OutboxAlways reports whether outbox is the primary delivery path.
func (p DeliveryPolicy) OutboxAlways() bool {
	return p.Enabled && p.Outbox == OutboxModeAlways
}

// OutboxFallbackOnCircuitOpen reports whether outbox should absorb the emit
// when the circuit breaker is OPEN.
func (p DeliveryPolicy) OutboxFallbackOnCircuitOpen() bool {
	return p.Enabled && p.Outbox == OutboxModeFallbackOnCircuitOpen
}

// DLQAllowed reports whether routable publish failures should be copied to
// the per-topic DLQ.
func (p DeliveryPolicy) DLQAllowed() bool {
	return p.Enabled && p.DLQ == DLQModeOnRoutableFailure
}

// HasDeliveryPath reports whether the policy permits at least one outbound
// path (direct OR outbox-always). Returns false for disabled policies.
func (p DeliveryPolicy) HasDeliveryPath() bool {
	if !p.Enabled {
		return false
	}

	return p.DirectAllowed() || p.OutboxAlways()
}

func cloneDeliveryPolicyOverrides(src map[string]DeliveryPolicyOverride) map[string]DeliveryPolicyOverride {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]DeliveryPolicyOverride, len(src))
	for key, override := range src {
		if override.Enabled != nil {
			enabled := *override.Enabled
			override.Enabled = &enabled
		}

		dst[key] = override
	}

	return dst
}

func CloneDeliveryPolicyOverrides(src map[string]DeliveryPolicyOverride) map[string]DeliveryPolicyOverride {
	return cloneDeliveryPolicyOverrides(src)
}
