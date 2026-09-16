package dlqheader

import (
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// The two headers that mark a DLQ entry whose payload was dropped so the record
// would fit. Frozen wire contract like the rest of this package: replay tooling
// branches on PayloadOmitted to know it must not replay the DLQ record
// verbatim. On a CONSUMER quarantine the payload is recoverable from the
// source topic at the partition and offset the source-* headers name; on the
// PRODUCER path the original publish never landed anywhere, so an omitted
// payload is genuinely gone and PayloadBytes is the only trace of it.
const (
	// PayloadOmitted is "true" on a DLQ entry published without its payload.
	// Absent means the payload is present and verbatim.
	PayloadOmitted = "x-lerian-dlq-payload-omitted"
	// PayloadBytes carries the size in bytes of the payload that was dropped,
	// so an operator can size the problem without fetching the source record.
	PayloadBytes = "x-lerian-dlq-payload-bytes"
)

// MaxErrorMessageBytes bounds the ErrorMessage header value.
//
// It exists because that header was the ONE unbounded input on a DLQ record.
// A quarantine copy carries the full original payload plus every original
// header plus the forensic set, so it is strictly LARGER than the record it
// quarantines. A near-cap source record (the producer caps payloads at 1 MiB)
// whose handler returned a long error therefore produced a DLQ copy the broker
// refuses — the quarantine fails, the partition is held back fail-closed, and
// under one-topic-per-app that wedges the producing application's entire
// catalog behind one poison record, forever, while Healthy() stays green.
//
// 4 KiB is generous for a diagnostic string and small enough that the header
// block can never be the thing that tips a record over the broker's limit on
// its own.
const MaxErrorMessageBytes = 4096

// TruncateErrorMessage bounds msg to MaxErrorMessageBytes, appending an
// explicit marker carrying the original length so nobody mistakes a cut string
// for the whole error. The cut is UTF-8 safe: a split multi-byte rune is
// dropped rather than emitted as a replacement character.
func TruncateErrorMessage(msg string) string {
	if len(msg) <= MaxErrorMessageBytes {
		return msg
	}

	marker := fmt.Sprintf(truncationMarkerFormat, len(msg))

	return strings.ToValidUTF8(msg[:MaxErrorMessageBytes-len(marker)], "") + marker
}

// truncationMarkerFormat builds the suffix TruncateErrorMessage appends. Its
// literal shape is a wire contract in the same way the header keys are: a
// reader detects truncation by it.
const truncationMarkerFormat = "...[truncated, %d bytes total]"

// truncationMarkerPrefix is the fixed head of that suffix, the part a reader
// can search for.
const truncationMarkerPrefix = "...[truncated, "

// TruncatedErrorMessageBytes reports whether msg is a CUT error message and, if
// so, how many bytes the original had.
//
// It is the half of the truncation contract a READER needs, and the reason it is
// exported rather than left to the caller: detecting a cut otherwise means
// hardcoding the marker text, which is exactly the restate-and-drift failure the
// exported keys exist to stop. The length bound alone does not answer it — the
// cut output is SHORTER than MaxErrorMessageBytes whenever a split multi-byte
// rune is dropped, so "len(msg) == MaxErrorMessageBytes" is not a test.
func TruncatedErrorMessageBytes(msg string) (int, bool) {
	start := strings.LastIndex(msg, truncationMarkerPrefix)
	if start < 0 {
		return 0, false
	}

	var original int

	if _, err := fmt.Sscanf(msg[start:], truncationMarkerFormat, &original); err != nil {
		return 0, false
	}

	return original, true
}

// IsSizeError reports whether err is a transport's "this record is too large"
// verdict — the one DLQ publish failure worth retrying with the payload
// omitted, because the retry is strictly smaller and can therefore succeed
// where the first attempt could not.
//
// Two shapes cover every transport lib-streaming publishes through: franz-go
// surfaces both the broker's MESSAGE_TOO_LARGE and its own client-side
// pre-flight rejection as kerr.MessageTooLarge, and the SQS / EventBridge
// adapters reject oversize wire messages with contract.ErrPayloadTooLarge
// before any network call.
func IsSizeError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, kerr.MessageTooLarge) || errors.Is(err, contract.ErrPayloadTooLarge)
}

// ReboundSanitizedErrorMessage re-applies the byte budget to an error message a
// READER grew while sanitizing it.
//
// The bound is a promise the reader inherits, not one the writer alone keeps:
// both DLQHeaderErrorMessage and DiscardRecord.ErrorMessage are documented as
// bounded at MaxErrorMessageBytes. That held transitively while the parser was
// a pass-through of a value TruncateErrorMessage had already cut. Sanitizing
// broke it — replacing a byte with U+FFFD costs two more — so a message the
// writer cut to exactly the bound arrives over it, and the documented limit
// stops being a limit for every reader sizing a column or a log field by it.
//
// originalBytes is the length of the value AS IT ARRIVED, before sanitizing. It
// is used only when a fresh marker has to be stamped.
//
// This also bounds a message that never touched the sanitizer. A foreign writer
// that ignores MaxErrorMessageBytes and puts 10 KiB on the wire used to reach
// the consumer at 10 KiB, because the parser passed the header through; it now
// arrives cut to the bound and marked as truncated, like any other cut message.
// That is the documented contract finally being true for every input rather
// than only for values this library wrote, but it IS a change for any consumer
// that had come to rely on the promise being unenforced.
//
// A marker the WRITER stamped is KEPT, never recomputed from what arrived. It
// carries how long the error was before the writer cut it — 14 KiB, say — and
// restamping it with the length of the 4 KiB header would replace the one
// number that says how much was lost with a number that says nothing. It is
// rebuilt from the parsed value rather than sliced out of the input, so a
// foreign writer cannot hand us a "marker" longer than the budget itself.
func ReboundSanitizedErrorMessage(sanitized string, originalBytes int) string {
	if len(sanitized) <= MaxErrorMessageBytes {
		return sanitized
	}

	body := sanitized
	marker := fmt.Sprintf(truncationMarkerFormat, originalBytes)

	if start := strings.LastIndex(sanitized, truncationMarkerPrefix); start >= 0 {
		var original int

		// A parsed marker is adopted ONLY when the canonical form of it is the
		// COMPLETE suffix, and only when it claims a positive length.
		//
		// fmt.Sscanf stops at the end of its format and ignores whatever follows,
		// so a marker-shaped run in the MIDDLE of a message parses exactly as
		// happily as a real trailing one. Treating that as the marker throws away
		// everything after it — and on the re-quarantine path, where a consumer
		// wraps the previous hop's ErrorMessage into a new error, what follows the
		// embedded marker is the CURRENT cause. Measured before this guard: a
		// 6509-byte message came back 4096 bytes long with its tail gone, claiming
		// an original length of 7 that belonged to an inner hop.
		//
		// A non-positive count is not a length either. Re-stamping one would put
		// "...[truncated, -5 bytes total]" in front of a consumer as THIS
		// library's own claim about the message, which is worse than the forged
		// header it came from.
		if _, err := fmt.Sscanf(sanitized[start:], truncationMarkerFormat, &original); err == nil && original > 0 {
			if canonical := fmt.Sprintf(truncationMarkerFormat, original); sanitized[start:] == canonical {
				body, marker = sanitized[:start], canonical
			}
		}
	}

	// The same cut TruncateErrorMessage makes: a split multi-byte rune is
	// DROPPED rather than emitted as a replacement, so the writer's cut and this
	// one cannot disagree about what a cut message looks like.
	if cut := MaxErrorMessageBytes - len(marker); len(body) > cut {
		body = strings.ToValidUTF8(body[:cut], "")
	}

	return body + marker
}
