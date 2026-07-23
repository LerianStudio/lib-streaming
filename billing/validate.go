package billing

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Anti-abuse ceilings. These are DoS guards, NOT business rules: they are sized
// generously so a realistic Lago billable event never trips them, and exist
// only to bound the work Validate (and, in Phase 2, untrusted inbound Kafka
// payloads) will do on a hostile input. Adjust if a legitimate event ever
// exceeds one.
const (
	maxMetricBytes                  = 255
	maxSubscriptionIDBytes          = 255
	maxProperties                   = 100
	maxPropertyKeyBytes             = 128
	maxPropertyStringValueBytes     = 1024
	maxPreciseTotalAmountCentsBytes = 64
)

// Validate reports whether the payload satisfies the billable-event invariants
// Lago enforces at ingestion: a non-empty metric code and external subscription
// id, property values restricted to JSON-safe finite scalars, and a well-formed
// decimal amount when present. It also enforces the anti-abuse size ceilings
// above.
//
// It returns a descriptive error rather than a sentinel: a failing payload is a
// construction-time caller bug — the emit path assembles these values, so a
// violation is a programming error to fix, not a runtime condition to branch on
// with errors.Is.
//
// Marshalability guarantee: when Validate returns nil, json.Marshal(p) is
// guaranteed to succeed. Every accepted property value is a finite number or a
// bounded string, and PreciseTotalAmountCents is a plain decimal string — none
// of the inputs json.Marshal rejects (non-finite floats, malformed json.Number)
// can pass Validate. MustMarshal relies on this.
func (p BillablePayload) Validate() error {
	if strings.TrimSpace(p.Metric) == "" {
		return errors.New("billing: Metric is required")
	}

	if len(p.Metric) > maxMetricBytes {
		return fmt.Errorf("billing: Metric exceeds %d bytes", maxMetricBytes)
	}

	if strings.TrimSpace(p.SubscriptionID) == "" {
		return errors.New("billing: SubscriptionID is required")
	}

	if len(p.SubscriptionID) > maxSubscriptionIDBytes {
		return fmt.Errorf("billing: SubscriptionID exceeds %d bytes", maxSubscriptionIDBytes)
	}

	if err := validateTimestamp(p.Timestamp); err != nil {
		return err
	}

	if err := validateProperties(p.Properties); err != nil {
		return err
	}

	return validatePreciseTotalAmountCents(p.PreciseTotalAmountCents)
}

// validateTimestamp keeps the marshalability guarantee honest: time.Time.
// MarshalJSON (RFC 3339) rejects years outside [0,9999], so a Timestamp beyond
// that range would pass Validate yet fail json.Marshal. Reject it here.
func validateTimestamp(ts *time.Time) error {
	if ts == nil {
		return nil
	}

	if y := ts.Year(); y < 0 || y > 9999 {
		return fmt.Errorf("billing: Timestamp year %d is out of the RFC 3339 range [0,9999]", y)
	}

	return nil
}

func validatePreciseTotalAmountCents(cents *string) error {
	if cents == nil {
		return nil
	}

	if len(*cents) > maxPreciseTotalAmountCentsBytes {
		return fmt.Errorf("billing: PreciseTotalAmountCents exceeds %d bytes", maxPreciseTotalAmountCentsBytes)
	}

	if !isDecimalString(*cents) {
		return fmt.Errorf("billing: PreciseTotalAmountCents %q is not a valid decimal string", *cents)
	}

	return nil
}

// validateProperties rejects any property value that is not a JSON-safe scalar
// Lago accepts (a bounded string or a finite number) and enforces the property
// count/key/value ceilings. bool, complex, slices (including []byte), maps, and
// nil are rejected; the error names the offending key and its Go type.
func validateProperties(properties map[string]any) error {
	if len(properties) > maxProperties {
		return fmt.Errorf("billing: too many properties: %d (max %d)", len(properties), maxProperties)
	}

	for key, value := range properties {
		if len(key) > maxPropertyKeyBytes {
			return fmt.Errorf("billing: property key %q exceeds %d bytes", key, maxPropertyKeyBytes)
		}

		if err := validatePropertyValue(key, value); err != nil {
			return err
		}
	}

	return nil
}

// validatePropertyValue accepts a bounded string, any built-in integer kind, a
// finite float32/float64, or a parseable finite json.Number; everything else is
// rejected. Accepting every integer/float kind (not just int/int64/float64)
// matches the numeric types a Go producer naturally hands over.
func validatePropertyValue(key string, value any) error {
	switch v := value.(type) {
	case string:
		if len(v) > maxPropertyStringValueBytes {
			return fmt.Errorf("billing: property %q string value exceeds %d bytes", key, maxPropertyStringValueBytes)
		}

		return nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return nil
	case float32:
		return validateFiniteFloat(key, float64(v))
	case float64:
		return validateFiniteFloat(key, v)
	case json.Number:
		return validateJSONNumber(key, v)
	default:
		return fmt.Errorf("billing: property %q has unsupported type %T (want string or number)", key, value)
	}
}

// validateFiniteFloat rejects NaN and ±Inf, which json.Marshal cannot encode.
func validateFiniteFloat(key string, f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("billing: property %q is a non-finite number", key)
	}

	return nil
}

// validateJSONNumber guarantees the json.Number is something json.Marshal will
// accept: it must parse as a finite float AND be a valid JSON number literal.
// This rejects gibberish ("not-a-number"), the empty string, non-finite forms
// ("NaN", "Inf"), and grammar json.Marshal disallows (leading '+', ".5",
// hex floats) — all of which strconv/ParseFloat would otherwise tolerate.
func validateJSONNumber(key string, n json.Number) error {
	s := n.String()

	// Bound the literal before parsing, mirroring the string-value cap so a
	// json.Number cannot smuggle an unbounded value past the DoS guards.
	if len(s) > maxPropertyStringValueBytes {
		return fmt.Errorf("billing: property %q json.Number value exceeds %d bytes", key, maxPropertyStringValueBytes)
	}

	f, err := n.Float64()
	if err != nil {
		return fmt.Errorf("billing: property %q json.Number %q is not a valid number: %w", key, s, err)
	}

	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("billing: property %q json.Number %q is non-finite", key, s)
	}

	if !json.Valid([]byte(s)) {
		return fmt.Errorf("billing: property %q json.Number %q is not a valid JSON number literal", key, s)
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
