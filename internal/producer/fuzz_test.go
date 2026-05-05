//go:build unit

package producer

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FuzzParseCloudEventsHeaders drives ParseCloudEventsHeaders with arbitrary
// header bytes. Invariant: never panics. When parsing fails, the returned
// Event must be the zero value — partial population on error would leak
// stale fields into caller code that reads parsed.X without checking err.
//
// Seed corpus covers happy paths, malformed inputs, and known injection
// shapes (CRLF, NUL, max-length keys, BOMs, multi-byte UTF-8) so the
// fuzzer's mutation budget can prioritize exploring around realistic
// adversarial inputs rather than rediscovering them from scratch.
func FuzzParseCloudEventsHeaders(f *testing.F) {
	// === HAPPY PATH SEEDS ===
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-valid-1")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.transaction.created")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
		{Key: "ce-tenantid", Value: []byte("t-seed")},
	}))
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))
	// === MALFORMED INPUT SEEDS ===
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-bad-time")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("not-a-timestamp")},
	}))
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-dup-1")},
		{Key: "ce-id", Value: []byte("seed-dup-2")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("")},
		{Key: "ce-source", Value: []byte("")},
		{Key: "ce-type", Value: []byte("")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))
	// === INJECTION-PATTERN SEEDS ===
	// CRLF injection in a header value — would corrupt downstream log streams
	// if accepted. Parser MUST either reject or pass through verbatim without
	// interpreting the embedded newline as a record boundary.
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-crlf\r\nce-source: //attacker")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))
	// NUL byte in header value — string truncation hazard for downstream C
	// consumers; parser MUST handle without truncating its own decoding.
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-with-nul\x00trailing")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))
	// 255-byte header key (the encoder limit). Boundary case: keys at exactly
	// the limit must encode + decode round-trip cleanly.
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-long-key")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
		{Key: "x-extension-" + strings.Repeat("k", 240), Value: []byte("v")},
	}))
	// Multi-byte UTF-8 in a value (emoji / non-ASCII printable). Parser MUST
	// not corrupt UTF-8 or interpret the bytes as control chars.
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-utf8-emoji-✓")},
		{Key: "ce-source", Value: []byte("//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
		{Key: "ce-subject", Value: []byte("订单-1234")},
	}))
	// UTF-8 BOM at start of value. Some upstream tooling injects BOMs;
	// parser MUST not treat the BOM as a value-significant prefix.
	f.Add(encodeHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("seed-bom")},
		{Key: "ce-source", Value: []byte("\xef\xbb\xbf//seed/service")},
		{Key: "ce-type", Value: []byte("studio.lerian.x.y")},
		{Key: "ce-time", Value: []byte("2026-04-18T00:00:00Z")},
	}))

	f.Fuzz(func(t *testing.T, raw []byte) {
		event, err := ParseCloudEventsHeaders(decodeHeaders(raw))
		if err == nil {
			return
		}

		// Tightened invariant per code review: the godoc promises "zero
		// Event + non-nil error" on failure. Anything less than full-zero
		// risks leaking stale fields into callers that read parsed.X
		// without checking err. Event has comparable scalar fields plus a
		// json.RawMessage (slice) — compare each scalar against the zero
		// value AND require Payload to be empty.
		//
		// We can't use `event != (Event{})` because the slice field makes
		// the struct non-comparable; the field-by-field check is the
		// idiomatic Go workaround.
		if event.TenantID != "" ||
			event.ResourceType != "" ||
			event.EventType != "" ||
			event.EventID != "" ||
			event.SchemaVersion != "" ||
			!event.Timestamp.IsZero() ||
			event.Source != "" ||
			event.Subject != "" ||
			event.DataContentType != "" ||
			event.DataSchema != "" ||
			event.SystemEvent ||
			len(event.Payload) != 0 {
			t.Fatalf("invariant violated: err=%v but Event has populated fields (%+v)", err, event)
		}
	})
}

func encodeHeaders(headers []kgo.RecordHeader) []byte {
	out := make([]byte, 0, 64)

	for _, h := range headers {
		keyBytes := []byte(h.Key)
		if len(keyBytes) > 255 || len(h.Value) > 255 {
			continue
		}

		out = append(out, byte(len(keyBytes)))
		out = append(out, keyBytes...)
		out = append(out, byte(len(h.Value)))
		out = append(out, h.Value...)
	}

	return out
}

func decodeHeaders(raw []byte) []kgo.RecordHeader {
	var headers []kgo.RecordHeader

	i := 0
	for i < len(raw) {
		if i+1 > len(raw) {
			return headers
		}

		keyLen := int(raw[i])
		i++

		if i+keyLen > len(raw) {
			return headers
		}

		key := string(raw[i : i+keyLen])
		i += keyLen

		if i+1 > len(raw) {
			return headers
		}

		valLen := int(raw[i])
		i++

		if i+valLen > len(raw) {
			return headers
		}

		val := make([]byte, valLen)
		copy(val, raw[i:i+valLen])
		i += valLen

		headers = append(headers, kgo.RecordHeader{Key: key, Value: val})
	}

	return headers
}
