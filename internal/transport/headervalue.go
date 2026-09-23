package transport

import (
	"strings"
	"unicode/utf8"
)

// HeaderValueReplacement is the rune substituted for every byte a Kafka header
// carries that a text consumer cannot hold. U+FFFD REPLACEMENT CHARACTER, the
// Unicode-standard signal for "something was here and it could not be
// represented".
//
// The substitution is deliberate and visible. Dropping the offending bytes
// silently would turn a hostile value into a clean-looking one, so an operator
// reading a stored value could not tell that the writer sent something the
// column, terminal or log pipeline refused.
const HeaderValueReplacement = "�"

// SanitizeHeaderValue converts raw Kafka header bytes into a Go string that is
// valid UTF-8 and free of U+0000. That is the whole of the promise, and it is
// deliberately narrow.
//
// It is NOT a general "safe to display" or "safe to log" function. A tab, a
// newline and an ANSI escape are all valid UTF-8 and all pass through
// untouched, so a value can still break a log line into two, or move a
// terminal cursor, after this has run. Escaping for a DISPLAY is the
// responsibility of whatever does the displaying, which is the only layer that
// knows what needs escaping for it. What this function removes is the narrower
// set that no text STORE will accept at all.
//
// A Kafka header value is an arbitrary byte slice. Nothing in the protocol, and
// nothing in this library before this function existed, required it to be text
// at all: a producer in any language can put a NUL byte or a truncated UTF-8
// sequence into ce-tenantid, and this library would hand that straight to the
// consumer's handler as a Go string.
//
// Two classes of byte are hostile to a consumer downstream of that string:
//
//   - U+0000. It is VALID UTF-8, so no encoding check catches it, and
//     PostgreSQL still refuses it in any text column with SQLSTATE 22021
//     (invalid byte sequence for encoding "UTF8"). It also terminates C
//     strings, so it truncates values on the way through anything that hands
//     one off.
//   - Invalid UTF-8 sequences. Refused with the same SQLSTATE, rendered as
//     garbage by every log viewer, and rejected outright by encoding/json.
//
// Both refusals are PERMANENT: no amount of retrying makes such a value
// storable. That is what makes this a library concern rather than a consumer
// one. A consumer that treats persistence failures as transient — the correct
// posture when the evidence must not be lost — retries the record forever, so
// one such byte stops a partition at that offset for every tenant behind it.
// Lender hit exactly that with a discard-queue reader; see LerianStudio/lender#340.
//
// Order matters. NUL is replaced FIRST, because it survives an encoding check
// and strings.ToValidUTF8 would leave it in place. Invalid sequences are
// replaced second. A run of invalid bytes collapses into one replacement rune,
// which is what strings.ToValidUTF8 does.
//
// The result may be LONGER in bytes than the input: each replaced byte becomes
// three, so an all-NUL value triples. A caller applying a byte budget MUST
// apply it after this, not before — see dlqheader.ReboundSanitizedErrorMessage,
// which exists because that ordering was got wrong once already.
func SanitizeHeaderValue(value []byte) string {
	text := string(value)

	if isSafeHeaderValue(text) {
		return text
	}

	text = strings.ReplaceAll(text, "\x00", HeaderValueReplacement)

	return strings.ToValidUTF8(text, HeaderValueReplacement)
}

// isSafeHeaderValue reports whether the value needs no work at all, which is
// the overwhelmingly common case: this runs on every header of every record,
// including the hot inbound path, so the clean case does no work beyond the two
// scans and the one string conversion that returning a string requires anyway.
// It does not make the clean path allocation-free — converting bytes to a
// string copies them — it keeps the clean path down to that single unavoidable
// copy instead of a second pass building a new one.
func isSafeHeaderValue(value string) bool {
	return strings.IndexByte(value, 0) < 0 && utf8.ValidString(value)
}
