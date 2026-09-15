//go:build unit

package streaming_test

import (
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
