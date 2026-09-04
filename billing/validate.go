package billing

import (
	"errors"
	"fmt"
	"math"
	"strings"

	billingv1 "github.com/LerianStudio/lib-streaming/v4/billing/gen/lerian/streaming/billing/v1"
)

// Anti-abuse ceilings. These are DoS guards, NOT business rules: they are sized
// generously so a realistic Lago billable event never trips them, and exist
// only to bound the work Validate (and, in Phase 2, untrusted inbound payloads)
// will do on a hostile input. Adjust if a legitimate event ever exceeds one.
const (
	maxMetricBytes                  = 255
	maxSubscriptionIDBytes          = 255
	maxProperties                   = 100
	maxPropertyKeyBytes             = 128
	maxPropertyStringValueBytes     = 1024
	maxPreciseTotalAmountCentsBytes = 64
)

// Validate reports whether p satisfies the residual billable-event invariants
// the protobuf schema cannot express structurally: a non-empty metric code and
// external subscription id, a well-formed decimal precise_total_amount_cents
// when present, and the anti-abuse size ceilings above.
//
// Property value TYPES (string OR number) are enforced by the proto `oneof` at
// the schema layer, so Validate no longer re-checks the type discriminator —
// only the string value length cap and the number finiteness guard remain, as
// value guards the schema cannot express (see validateProperties).
//
// It returns a descriptive error rather than a sentinel: a failing payload is a
// construction-time caller bug — the emit path assembles these values, so a
// violation is a programming error to fix, not a runtime condition to branch on
// with errors.Is. A nil payload is reported as an error, never a panic: the
// explicit nil guard below short-circuits to "Metric is required" so the
// contract holds even if the field checks are ever reordered.
func Validate(p *BillablePayload) error {
	if p == nil {
		return errors.New("billing: Metric is required")
	}

	if strings.TrimSpace(p.GetMetric()) == "" {
		return errors.New("billing: Metric is required")
	}

	if len(p.GetMetric()) > maxMetricBytes {
		return fmt.Errorf("billing: Metric exceeds %d bytes", maxMetricBytes)
	}

	if strings.TrimSpace(p.GetSubscriptionId()) == "" {
		return errors.New("billing: SubscriptionID is required")
	}

	if len(p.GetSubscriptionId()) > maxSubscriptionIDBytes {
		return fmt.Errorf("billing: SubscriptionID exceeds %d bytes", maxSubscriptionIDBytes)
	}

	if err := validateProperties(p.GetProperties()); err != nil {
		return err
	}

	return validatePreciseTotalAmountCents(p.GetPreciseTotalAmountCents(), p.PreciseTotalAmountCents != nil)
}

// validatePreciseTotalAmountCents enforces the size cap and decimal-string shape
// only when the field is present. present distinguishes an absent optional field
// (valid) from a present empty string (rejected).
func validatePreciseTotalAmountCents(cents string, present bool) error {
	if !present {
		return nil
	}

	if len(cents) > maxPreciseTotalAmountCentsBytes {
		return fmt.Errorf("billing: PreciseTotalAmountCents exceeds %d bytes", maxPreciseTotalAmountCentsBytes)
	}

	if !isDecimalString(cents) {
		return fmt.Errorf("billing: PreciseTotalAmountCents %q is not a valid decimal string", cents)
	}

	return nil
}

// validateProperties enforces the property count ceiling, the key-length cap,
// the string value-length cap, and number finiteness. The proto oneof already
// guarantees every SET value is a string or a number, so no type discriminator
// check is performed here. Numbers still need a value guard the schema cannot
// express: protobuf `double` fully round-trips NaN and ±Inf, but the downstream
// Lago JSON step cannot represent them, so a non-finite number would silently
// drop the billable event — Validate rejects it here instead.
//
// A nil *PropertyValue, and a present PropertyValue whose oneof variant is unset
// (proto3 cannot structurally require a oneof arm be set), are BOTH rejected:
// either would serialize as an empty PropertyValue that the consumer's
// flattenProperties then drops, silently losing the property.
func validateProperties(properties map[string]*billingv1.PropertyValue) error {
	if len(properties) > maxProperties {
		return fmt.Errorf("billing: too many properties: %d (max %d)", len(properties), maxProperties)
	}

	for key, value := range properties {
		if value == nil {
			return fmt.Errorf("billing: property %q value is nil", key)
		}

		if len(key) > maxPropertyKeyBytes {
			return fmt.Errorf("billing: property key %q exceeds %d bytes", key, maxPropertyKeyBytes)
		}

		switch v := value.GetValue().(type) {
		case *billingv1.PropertyValue_StringValue:
			if len(v.StringValue) > maxPropertyStringValueBytes {
				return fmt.Errorf("billing: property %q string value exceeds %d bytes", key, maxPropertyStringValueBytes)
			}
		case *billingv1.PropertyValue_NumberValue:
			if math.IsNaN(v.NumberValue) || math.IsInf(v.NumberValue, 0) {
				return fmt.Errorf("billing: property %q must be a finite number", key)
			}
		default:
			return fmt.Errorf("billing: property %q has no value set (string or number required)", key)
		}
	}

	return nil
}

// isDecimalString reports whether s matches ^\d+(\.\d+)?$ — one or more digits,
// optionally followed by a single dot and one or more fractional digits. It
// intentionally rejects signs, exponents, leading/trailing dots, and the empty
// string, all of which Lago would reject as an amount. Hand-rolled to keep the
// package stdlib-only and avoid a package-level compiled regexp (whose
// MustCompile form would read as a panic site).
func isDecimalString(s string) bool {
	if s == "" {
		return false
	}

	var dotSeen, digitsBeforeDot, digitsAfterDot bool

	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			if dotSeen {
				digitsAfterDot = true
			} else {
				digitsBeforeDot = true
			}
		case r == '.':
			if dotSeen {
				return false
			}

			dotSeen = true
		default:
			return false
		}
	}

	if !digitsBeforeDot {
		return false
	}

	return !dotSeen || digitsAfterDot
}
