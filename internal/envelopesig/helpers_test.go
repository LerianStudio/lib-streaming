//go:build unit

package envelopesig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// testSecret returns a deterministic secret of n bytes starting at seed.
func testSecret(seed byte, n int) Secret {
	s := make(Secret, n)
	for i := range s {
		s[i] = seed + byte(i)
	}

	return s
}

// fullEvent is an event that populates every one of the 13 CloudEvents headers
// the codec can emit.
func fullEvent() cloudevents.Event {
	return cloudevents.Event{
		EventID:         "0190a8e2-0000-7000-8000-000000000001",
		Source:          "ledger",
		ResourceType:    "transaction",
		EventType:       "created",
		SchemaVersion:   "1.0.0",
		Timestamp:       time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		TenantID:        "tenant-1",
		Subject:         "acc-1",
		DataContentType: "application/json",
		DataSchema:      "https://schemas.lerian.studio/transaction/created/1.0.0",
		SystemEvent:     true,
	}
}

func fixedClock(ts time.Time) func() time.Time {
	return func() time.Time { return ts }
}

func mustKeyring(t testing.TB, keys ...Key) *Keyring {
	t.Helper()

	ring, err := NewKeyring(keys...)
	require.NoError(t, err)

	return ring
}

func mustSigner(t testing.TB, ring *Keyring, id, source string, opts ...Option) *Signer {
	t.Helper()

	s, err := NewSigner(ring, id, source, opts...)
	require.NoError(t, err)

	return s
}

func mustVerifier(t testing.TB, ring *Keyring, maxSkew time.Duration, opts ...Option) *Verifier {
	t.Helper()

	v, err := NewVerifier(ring, maxSkew, opts...)
	require.NoError(t, err)

	return v
}

func toRecordHeaders(headers []transport.Header) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}

	return out
}

func headersFor(ev cloudevents.Event) []transport.Header {
	return cloudevents.BuildTransportHeaders(ev)
}
