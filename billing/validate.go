package billing

import (
	"errors"
	"fmt"
	"strings"

	billingv1 "github.com/LerianStudio/lib-streaming/v2/billing/gen/lerian/streaming/billing/v1"
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
// the schema layer, so Validate no longer re-checks them — only the string
// value length cap remains, as a size guard.
//
// It returns a descriptive error rather than a sentinel: a failing payload is a
// construction-time caller bug — the emit path assembles these values, so a
// violation is a programming error to fix, not a runtime condition to branch on
// with errors.Is. A nil payload is reported as an error, never a panic: the
// generated getters are nil-safe, so it degrades to "Metric is required".
func Validate(p *BillablePayload) error {
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
// and the string value-length cap. Number values carry no length, and the proto
// oneof already guarantees every value is a string or a number, so no type check
// is performed here. A nil property value is tolerated (nil-safe getters).
func validateProperties(properties map[string]*billingv1.PropertyValue) error {
	if len(properties) > maxProperties {
		return fmt.Errorf("billing: too many properties: %d (max %d)", len(properties), maxProperties)
	}

	for key, value := range properties {
		if len(key) > maxPropertyKeyBytes {
			return fmt.Errorf("billing: property key %q exceeds %d bytes", key, maxPropertyKeyBytes)
		}

		if sv, ok := value.GetValue().(*billingv1.PropertyValue_StringValue); ok {
			if len(sv.StringValue) > maxPropertyStringValueBytes {
				return fmt.Errorf("billing: property %q string value exceeds %d bytes", key, maxPropertyStringValueBytes)
			}
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
