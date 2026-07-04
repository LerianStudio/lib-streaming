package contract

import "strings"

// topicSegmentSeparators are the characters treated as separators when
// trimming the leading/trailing edges of a sanitized source segment. All
// three are valid inside a Kafka topic name, but a name that starts or ends
// with one is undesirable, so they are stripped from the boundaries only.
const topicSegmentSeparators = "-._"

// sanitizeSourceSegment reduces a CloudEvents ce-source value to a single
// topic-name-safe segment used as the SERVICE NAMESPACE prefix of a derived
// topic (see Event.Topic). Downstream Kafka ACLs scope a producer to
// "{sanitize(Source)}.*", so this segment must be deterministic and stable
// for a given service source.
//
// The transformation is:
//
//  1. Lowercase the input.
//  2. Map every rune outside the Kafka topic-name charset [a-z0-9._-] to a
//     single '-'. This flattens URI-ish sources (scheme "://", authority
//     "//", path "/", ":" and spaces) into hyphen-joined segments. Dots and
//     underscores that were already present are meaningful namespace
//     delimiters and are preserved.
//  3. Collapse consecutive '-' runs (including those just introduced) into a
//     single '-'.
//  4. Trim leading and trailing separators ('-', '.', '_') so the segment
//     never begins or ends with a delimiter.
//
// Examples:
//
//	"midaz-ledger"                       -> "midaz-ledger"
//	"MIDAZ-Ledger"                       -> "midaz-ledger"
//	"//lerian.midaz/transaction-service" -> "lerian.midaz-transaction-service"
//	"svc://tenant-cb-test"               -> "svc-tenant-cb-test"
//
// Empty input yields an empty string; empty Source is rejected upstream by
// ErrMissingSource before a real emit reaches topic derivation, so this
// function does not itself signal an error.
func sanitizeSourceSegment(source string) string {
	if source == "" {
		return ""
	}

	lowered := strings.ToLower(source)

	var b strings.Builder
	b.Grow(len(lowered))

	prevHyphen := false

	for _, r := range lowered {
		if isTopicSegmentRune(r) {
			b.WriteRune(r)

			prevHyphen = false

			continue
		}

		// Replacement char: collapse consecutive replacements into one '-'.
		if prevHyphen {
			continue
		}

		b.WriteByte('-')

		prevHyphen = true
	}

	return strings.Trim(b.String(), topicSegmentSeparators)
}

// SanitizeSourceSegment is the exported wrapper over sanitizeSourceSegment.
//
// Exported so the producer package's property tests can exercise the SAME
// implementation that Topic() uses at runtime when reconstructing the
// expected base topic — mirroring the ParseMajorVersion export rationale. It
// carries no behavior of its own; production code calls the unexported form.
func SanitizeSourceSegment(source string) string {
	return sanitizeSourceSegment(source)
}

// isTopicSegmentRune reports whether r is inside the Kafka topic-name charset
// that sanitizeSourceSegment preserves verbatim: ASCII [a-z0-9._-]. Uppercase
// is handled by the caller lowercasing first; non-ASCII runes are replaced.
func isTopicSegmentRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '.' || r == '_' || r == '-':
		return true
	default:
		return false
	}
}
