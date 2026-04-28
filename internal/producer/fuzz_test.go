//go:build unit

package producer

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FuzzParseCloudEventsHeaders drives ParseCloudEventsHeaders with arbitrary
// header bytes. Invariant: never panics. When parsing fails, returns the zero
// Event + a non-nil error; when it succeeds, returns an Event + nil.
func FuzzParseCloudEventsHeaders(f *testing.F) {
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

	f.Fuzz(func(t *testing.T, raw []byte) {
		event, err := ParseCloudEventsHeaders(decodeHeaders(raw))
		if err != nil && event.EventID != "" {
			t.Fatalf("invariant violated: err=%v AND populated Event (EventID=%q)", err, event.EventID)
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
