package dlqheader

import (
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
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

	marker := fmt.Sprintf("...[truncated, %d bytes total]", len(msg))

	return strings.ToValidUTF8(msg[:MaxErrorMessageBytes-len(marker)], "") + marker
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
