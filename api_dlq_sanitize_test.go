//go:build unit

package streaming_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
)

// A Kafka header value is an arbitrary byte slice. A producer in any language
// can put U+0000 or an invalid UTF-8 sequence into one, and until this was
// fixed, ParseDiscardRecord handed those bytes straight to the caller as a Go
// string.
//
// Why the library owns this rather than every consumer: PostgreSQL refuses both
// in a text column with SQLSTATE 22021, and the refusal is PERMANENT. A DLQ
// reader that treats persistence failures as transient — the correct posture
// when the evidence must not be lost — then retries the record forever, writes
// nothing, and stops the dead-letter partition at that offset for every tenant
// behind it. Lender hit exactly that; see LerianStudio/lender#340.

const replacement = "�"

// hostileForensicHeaders is the full forensic block with the two hostile classes
// planted in the three free-text values a producer controls.
func hostileForensicHeaders() []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: streaming.DLQHeaderSourceTopic, Value: []byte("lerian.streaming.gateway\x00")},
		{Key: streaming.DLQHeaderSourcePartition, Value: []byte("3")},
		{Key: streaming.DLQHeaderSourceOffset, Value: []byte("42")},
		{Key: streaming.DLQHeaderCauseKind, Value: []byte(streaming.DLQCauseHandler)},
		{Key: streaming.DLQHeaderErrorClass, Value: []byte{'v', 'a', 'l', 0xff, 0xfe}},
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte("boom\x00 at offset 42")},
		{Key: streaming.DLQHeaderRetryCount, Value: []byte("2")},
		{Key: streaming.DLQHeaderProducerID, Value: []byte("lender-consumer\x00group")},
	}
}

func TestParseDiscardRecord_SanitizesHostileHeaderValues(t *testing.T) {
	t.Parallel()

	got := streaming.ParseDiscardRecord(hostileForensicHeaders(), []byte(`{"loanId":"l-1"}`))

	for field, value := range map[string]string{
		"SourceTopic":  got.SourceTopic,
		"CauseKind":    got.CauseKind,
		"ErrorClass":   got.ErrorClass,
		"ErrorMessage": got.ErrorMessage,
		"ProducerID":   got.ProducerID,
	} {
		if strings.IndexByte(value, 0) >= 0 {
			t.Errorf("%s = %q kept a NUL; a text store refuses it permanently", field, value)
		}

		if !utf8.ValidString(value) {
			t.Errorf("%s = %q is not valid UTF-8; a text store refuses it permanently", field, value)
		}
	}

	// The readable part survives, and the replacement is visible so nobody reads
	// a scrubbed value as an original one.
	if want := "lerian.streaming.gateway" + replacement; got.SourceTopic != want {
		t.Errorf("SourceTopic = %q; want %q", got.SourceTopic, want)
	}

	if want := "boom" + replacement + " at offset 42"; got.ErrorMessage != want {
		t.Errorf("ErrorMessage = %q; want %q", got.ErrorMessage, want)
	}

	if want := "val" + replacement; got.ErrorClass != want {
		t.Errorf("ErrorClass = %q; want %q", got.ErrorClass, want)
	}

	// Sanitizing must not disturb the fields that were already clean, nor the
	// numeric parse of the origin triple, which is what an operator navigates by.
	if got.SourcePartition != 3 || got.SourceOffset != 42 {
		t.Errorf("origin coordinates = %d/%d; want 3/42", got.SourcePartition, got.SourceOffset)
	}

	if got.RetryCount != 2 {
		t.Errorf("RetryCount = %d; want 2", got.RetryCount)
	}

	if string(got.Payload) != `{"loanId":"l-1"}` {
		t.Errorf("Payload = %q; the payload is bytes and is NOT sanitized", got.Payload)
	}
}

// The PAYLOAD is deliberately untouched. It is a byte slice on the way to a
// codec that has its own rules, and rewriting bytes inside it would corrupt a
// record an operator may need to replay verbatim. Only the header-derived
// STRINGS are sanitized.
func TestParseDiscardRecord_LeavesThePayloadBytesAlone(t *testing.T) {
	t.Parallel()

	payload := []byte{'{', 0x00, 0xff, '}'}

	got := streaming.ParseDiscardRecord(forensicHeaders(), payload)

	if string(got.Payload) != string(payload) {
		t.Errorf("Payload = %q; want the original bytes %q", got.Payload, payload)
	}
}

// The truncation path, which is where a byte budget and a byte-growing
// sanitizer meet.
//
// TruncateErrorMessage runs on the WRITER side and bounds the message to
// MaxErrorMessageBytes. Sanitizing happens on the READER side and can only
// grow a value, so a message that was exactly at the bound arrives longer.
// What must survive that is the truncation MARKER: a reader detects a cut
// message by it, and a cut that landed inside the marker — or a sanitizer that
// mangled it — would report a truncated error as a whole one.
func TestParseDiscardRecord_SanitizingKeepsTheTruncationMarkerReadable(t *testing.T) {
	t.Parallel()

	// A message a producer really could emit: over the bound, and carrying a NUL
	// inside the part that survives the cut.
	raw := "boom\x00 " + strings.Repeat("detail ", 2000)

	cut := dlqheader.TruncateErrorMessage(raw)
	if len(cut) > dlqheader.MaxErrorMessageBytes {
		t.Fatalf("TruncateErrorMessage returned %d bytes, over the %d bound", len(cut), dlqheader.MaxErrorMessageBytes)
	}

	if strings.IndexByte(cut, 0) < 0 {
		t.Fatal("this test's premise is wrong: the NUL did not survive truncation, so the parser has nothing to fix")
	}

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderSourceTopic, Value: []byte("lerian.streaming.gateway")},
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte(cut)},
	}, nil)

	if strings.IndexByte(got.ErrorMessage, 0) >= 0 {
		t.Errorf("ErrorMessage = %q kept a NUL through the truncation path", got.ErrorMessage)
	}

	if !utf8.ValidString(got.ErrorMessage) {
		t.Errorf("ErrorMessage = %q is not valid UTF-8", got.ErrorMessage)
	}

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated {
		t.Fatalf("ErrorMessage = %q no longer reads as truncated: a cut error would be reported as a whole one", got.ErrorMessage)
	}

	if original != len(raw) {
		t.Errorf("TruncatedErrorMessageBytes = %d; want the original length %d", original, len(raw))
	}
}

// ═══ The byte budget survives a sanitizer that can only GROW the value ═══
//
// DLQHeaderErrorMessage and DiscardRecord.ErrorMessage are both documented as
// bounded at DLQMaxErrorMessageBytes. That bound used to hold transitively: the
// writer cut the message and the parser passed it through untouched. Sanitizing
// broke it — one NUL replaced by U+FFFD costs two more bytes — so a message the
// writer cut to EXACTLY the bound came back over it, and a reader sizing a
// column or a log field by the documented number would have it refused.
//
// Found by review on the first version of the sanitizing fix. The truncation
// test written alongside that fix asserted the message came back clean and
// still read as truncated, and never asserted its LENGTH, which is the one
// thing that had changed.

// errorMessageAt builds a header value of exactly n bytes that contains one NUL,
// so sanitizing it grows it by exactly two.
func errorMessageAt(n int) []byte {
	value := append([]byte("boom\x00"), []byte(strings.Repeat("d", n-5))...)
	if len(value) != n {
		panic("fixture is not the length it claims")
	}

	return value
}

func TestParseDiscardRecord_ErrorMessageStaysWithinItsBudgetAfterSanitizing(t *testing.T) {
	t.Parallel()

	for name, size := range map[string]int{
		"exactly at the limit": streaming.DLQMaxErrorMessageBytes,
		"one byte over":        streaming.DLQMaxErrorMessageBytes + 1,
		"one byte under":       streaming.DLQMaxErrorMessageBytes - 1,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			raw := errorMessageAt(size)

			got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
				{Key: streaming.DLQHeaderErrorMessage, Value: raw},
			}, nil)

			if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
				t.Errorf("ErrorMessage is %d bytes, over the documented bound of %d",
					len(got.ErrorMessage), streaming.DLQMaxErrorMessageBytes)
			}

			if strings.IndexByte(got.ErrorMessage, 0) >= 0 {
				t.Error("ErrorMessage kept a NUL")
			}

			if !utf8.ValidString(got.ErrorMessage) {
				t.Error("the cut landed inside a rune")
			}

			// Whatever else happens, the readable head of the error survives: an
			// operator reads the start of the message, not its tail.
			if !strings.HasPrefix(got.ErrorMessage, "boom"+replacement) {
				t.Errorf("ErrorMessage = %.32q...; want it to start with the sanitized original", got.ErrorMessage)
			}
		})
	}
}

// A message that had to be cut says so, and says how long the ORIGINAL was.
//
// "one byte under" is the non-vacuity half: it does not need cutting, so it must
// NOT come back wearing a truncation marker. Without it, a re-bound that cut
// unconditionally would satisfy every assertion above.
func TestParseDiscardRecord_ReboundingStampsTheOriginalLength(t *testing.T) {
	t.Parallel()

	atLimit := errorMessageAt(streaming.DLQMaxErrorMessageBytes)

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: atLimit},
	}, nil)

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated {
		t.Fatalf("a message that had to be cut must say so; got %.64q", got.ErrorMessage)
	}

	if original != len(atLimit) {
		t.Errorf("TruncatedErrorMessageBytes = %d; want the length as it ARRIVED, %d", original, len(atLimit))
	}

	under := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: errorMessageAt(streaming.DLQMaxErrorMessageBytes - 8)},
	}, nil)

	if _, cut := streaming.TruncatedErrorMessageBytes(under.ErrorMessage); cut {
		t.Error("a message that fits after sanitizing must not be marked as truncated")
	}
}

// A marker the WRITER stamped is preserved, never recomputed.
//
// It carries how long the error was before the writer cut it. Restamping it with
// the length of the arriving header would replace the one number that says how
// much was lost with a number that says nothing — the header's own size, which
// the reader can already measure.
func TestParseDiscardRecord_ReboundingKeepsTheWritersOriginalLength(t *testing.T) {
	t.Parallel()

	// What the writer actually produces: a huge error cut to the bound, carrying
	// a NUL in the surviving head, so the reader's sanitising pushes it over.
	huge := "boom\x00 " + strings.Repeat("detail ", 20000)

	cut := dlqheader.TruncateErrorMessage(huge)
	if len(cut) > streaming.DLQMaxErrorMessageBytes {
		t.Fatalf("the writer's own cut is %d bytes, over its bound", len(cut))
	}

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte(cut)},
	}, nil)

	if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
		t.Errorf("ErrorMessage is %d bytes, over the documented bound", len(got.ErrorMessage))
	}

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated {
		t.Fatal("the writer's truncation marker did not survive the re-bound")
	}

	if original != len(huge) {
		t.Errorf("TruncatedErrorMessageBytes = %d; want the WRITER's original length %d, not the header's %d",
			original, len(huge), len(cut))
	}
}

// A foreign writer's malformed marker must not be able to reach a slice bound.
//
// The marker is rebuilt from the parsed number rather than sliced out of the
// input, so a value that merely LOOKS like a marker — at the very start, or
// followed by kilobytes of text — cannot produce one longer than the budget.
func TestParseDiscardRecord_ReboundingSurvivesAHostileMarker(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]string{
		"marker at the very start": "...[truncated, 5 bytes total]" + strings.Repeat("x", 5000) + "\x00",
		"marker followed by text":  "boom\x00" + strings.Repeat("x", 5000) + "...[truncated, 7 bytes total]tail",
		"marker text with no number": "boom\x00" + strings.Repeat("x", 5000) +
			"...[truncated, lots of bytes total]",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
				{Key: streaming.DLQHeaderErrorMessage, Value: []byte(value)},
			}, nil)

			if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
				t.Errorf("ErrorMessage is %d bytes, over the documented bound", len(got.ErrorMessage))
			}

			if strings.IndexByte(got.ErrorMessage, 0) >= 0 {
				t.Error("ErrorMessage kept a NUL")
			}
		})
	}
}

// THIS LIBRARY'S OWN WRITER puts a NUL on the wire. The hazard is not
// foreign-input-only, which is what makes the read-side bound mandatory rather
// than defensive.
//
// TruncateErrorMessage returns a message below the bound VERBATIM, and the DLQ
// publish path does not run it through contract.HasControlChar — that check
// guards event definitions, emit requests and routes, not this header. So an
// error wrapping a byte slice, a driver message quoting a value, or any
// fmt.Errorf over data is written exactly as it came.
func TestParseDiscardRecord_TheWritersOwnNULIsBoundedByTheReader(t *testing.T) {
	t.Parallel()

	// An error a real handler produces: a driver message quoting the offending
	// value. Padded so the writer's verbatim output sits exactly on the bound.
	handlerError := "insert failed: value \x00 rejected: " +
		strings.Repeat("d", streaming.DLQMaxErrorMessageBytes-len("insert failed: value \x00 rejected: "))

	onTheWire := dlqheader.TruncateErrorMessage(handlerError)
	if onTheWire != handlerError {
		t.Fatal("this test's premise is wrong: the writer no longer emits a below-bound message verbatim")
	}

	if strings.IndexByte(onTheWire, 0) < 0 {
		t.Fatal("this test's premise is wrong: the writer no longer puts the NUL on the wire")
	}

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte(onTheWire)},
	}, nil)

	if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
		t.Errorf("ErrorMessage is %d bytes, over the documented bound of %d",
			len(got.ErrorMessage), streaming.DLQMaxErrorMessageBytes)
	}

	if strings.IndexByte(got.ErrorMessage, 0) >= 0 {
		t.Error("ErrorMessage kept the writer's NUL")
	}

	if !strings.HasPrefix(got.ErrorMessage, "insert failed: value "+replacement+" rejected: ") {
		t.Errorf("ErrorMessage = %.64q...; the readable head of the writer's error must survive", got.ErrorMessage)
	}
}

// The worst case: every byte is a NUL, so sanitising TRIPLES the value.
//
// A bound re-applied by subtracting a fixed slack, rather than by measuring the
// sanitised result, would hold for the one-NUL case and fail here.
func TestParseDiscardRecord_AnAllNULMessageIsStillBounded(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0}, streaming.DLQMaxErrorMessageBytes)

	grown := len(raw) * len(replacement)
	if grown != 3*streaming.DLQMaxErrorMessageBytes {
		t.Fatalf("this test's premise is wrong: the growth factor is not 3, it is %d", grown/len(raw))
	}

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: raw},
	}, nil)

	if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
		t.Errorf("ErrorMessage is %d bytes; sanitising grew %d bytes to %d and the bound must still hold",
			len(got.ErrorMessage), len(raw), grown)
	}

	if !utf8.ValidString(got.ErrorMessage) {
		t.Error("the cut landed inside a replacement rune")
	}

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated {
		t.Fatal("a message this heavily cut must say it was cut")
	}

	if original != len(raw) {
		t.Errorf("TruncatedErrorMessageBytes = %d; want the length as it ARRIVED, %d", original, len(raw))
	}
}

// ═══ A marker-shaped run in the MIDDLE is body, not a marker ═══
//
// fmt.Sscanf stops at the end of its format and ignores whatever follows, so a
// marker-shaped run anywhere in the message parses exactly as happily as a real
// trailing one. Adopting it throws away everything after it and re-stamps the
// consumer-facing value with a length lifted from somewhere inside the body.

// The re-quarantine path, which is where this library's own writer produces the
// shape. A consumer that re-quarantines wraps the PREVIOUS hop's ErrorMessage —
// which ends in a marker — into a new error, so the old marker is now embedded
// and the CURRENT cause follows it.
func TestParseDiscardRecord_ReQuarantineKeepsTheCurrentCause(t *testing.T) {
	t.Parallel()

	previousHop := "connection reset by peer...[truncated, 18027 bytes total]"

	// The wrapper carries a NUL, which is what pushes a message that was within
	// the bound over it once the reader sanitizes — the whole reason the re-bound
	// runs at all.
	currentCause := ": handler refused again\x00 " + strings.Repeat("current stack frame ", 250)
	onTheWire := "re-quarantined: " + previousHop + currentCause

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte(onTheWire)},
	}, nil)

	if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
		t.Errorf("ErrorMessage is %d bytes, over the documented bound", len(got.ErrorMessage))
	}

	if !strings.Contains(got.ErrorMessage, "handler refused again") {
		t.Errorf("the CURRENT cause was discarded as if it came after a real marker; got %d bytes: %.80q",
			len(got.ErrorMessage), got.ErrorMessage)
	}

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated {
		t.Fatal("a cut message must say it was cut")
	}

	if original == 18027 {
		t.Error("the length was lifted from a marker embedded two hops ago, not measured on this message")
	}

	if original != len(onTheWire) {
		t.Errorf("TruncatedErrorMessageBytes = %d; want the length as it ARRIVED, %d", original, len(onTheWire))
	}
}

func TestParseDiscardRecord_AMarkerIsAdoptedOnlyAsTheCompleteSuffix(t *testing.T) {
	t.Parallel()

	padding := strings.Repeat("d", streaming.DLQMaxErrorMessageBytes)

	for name, testCase := range map[string]struct {
		value     string
		mustKeep  string
		mustClaim func(arrived int) int
	}{
		"marker followed by a tail is body": {
			value:     "boom\x00 " + padding + "...[truncated, 7 bytes total]tail-that-matters",
			mustKeep:  "boom",
			mustClaim: func(arrived int) int { return arrived },
		},
		"a bracket in the body before a real marker": {
			value:     "boom\x00 got ] here " + padding + "...[truncated, 900 bytes total]",
			mustKeep:  "boom",
			mustClaim: func(int) int { return 900 },
		},
		"an inner marker plus a real trailing one": {
			value:     "boom\x00 ...[truncated, 7 bytes total] then " + padding + "...[truncated, 900 bytes total]",
			mustKeep:  "boom",
			mustClaim: func(int) int { return 900 },
		},
		"a negative count is not a length": {
			value:     "boom\x00 " + padding + "...[truncated, -5 bytes total]",
			mustKeep:  "boom",
			mustClaim: func(arrived int) int { return arrived },
		},
		"a zero count is not a length": {
			value:     "boom\x00 " + padding + "...[truncated, 0 bytes total]",
			mustKeep:  "boom",
			mustClaim: func(arrived int) int { return arrived },
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
				{Key: streaming.DLQHeaderErrorMessage, Value: []byte(testCase.value)},
			}, nil)

			if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
				t.Errorf("ErrorMessage is %d bytes, over the documented bound", len(got.ErrorMessage))
			}

			if !strings.HasPrefix(got.ErrorMessage, testCase.mustKeep) {
				t.Errorf("the head of the error must survive; got %.60q", got.ErrorMessage)
			}

			original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
			if !truncated {
				t.Fatalf("a cut message must say it was cut; got %.60q", got.ErrorMessage)
			}

			want := testCase.mustClaim(len(testCase.value))
			if original != want {
				t.Errorf("TruncatedErrorMessageBytes = %d; want %d", original, want)
			}

			// Asserted as an exact SUFFIX, not a substring: a forged count
			// re-stamped as this library's own claim has to show up here, and a
			// substring check for "0 bytes total" happily matches "900 bytes
			// total" and reports nothing.
			if suffix := fmt.Sprintf("...[truncated, %d bytes total]", want); !strings.HasSuffix(got.ErrorMessage, suffix) {
				t.Errorf("message must END with %q; got %q", suffix, got.ErrorMessage[len(got.ErrorMessage)-40:])
			}
		})
	}
}

// A foreign writer that ignores the bound is bounded too, even with nothing to
// sanitize. This is a behaviour change for any consumer that had come to rely
// on the documented bound being unenforced on the read side.
func TestParseDiscardRecord_ACleanOversizeMessageIsBoundedToo(t *testing.T) {
	t.Parallel()

	clean := strings.Repeat("plain ascii detail ", 600)
	if strings.IndexByte(clean, 0) >= 0 || !utf8.ValidString(clean) {
		t.Fatal("this fixture is supposed to need no sanitizing at all")
	}

	got := streaming.ParseDiscardRecord([]kgo.RecordHeader{
		{Key: streaming.DLQHeaderErrorMessage, Value: []byte(clean)},
	}, nil)

	if len(got.ErrorMessage) > streaming.DLQMaxErrorMessageBytes {
		t.Errorf("ErrorMessage is %d bytes; the bound holds for every input, not only sanitized ones",
			len(got.ErrorMessage))
	}

	original, truncated := streaming.TruncatedErrorMessageBytes(got.ErrorMessage)
	if !truncated || original != len(clean) {
		t.Errorf("a cut message must say so and name its arriving length; got truncated=%v original=%d want %d",
			truncated, original, len(clean))
	}
}
