//go:build unit

package envelopesig

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

func headerValue(headers []transport.Header, key string) (string, bool) {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

// TestCanonical_GoldenVector pins the wire format. The expected value was
// computed OUTSIDE this code base (an independent Python HMAC over the
// documented canonical layout), so a change to the field order, the length
// prefix, the domain tag or the value encoding fails here instead of silently
// splitting producers and consumers running different library versions.
func TestCanonical_GoldenVector(t *testing.T) {
	t.Parallel()

	ev := cloudevents.Event{
		EventID:         "0190a8e2-0000-7000-8000-000000000001",
		Source:          "ledger",
		ResourceType:    "transaction",
		EventType:       "created",
		SchemaVersion:   "1.0.0",
		Timestamp:       time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		TenantID:        "tenant-1",
		Subject:         "acc-1",
		DataContentType: "application/json",
	}

	secret := make(Secret, 32)
	for i := range secret {
		secret[i] = byte(i)
	}

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: secret})
	signer := mustSigner(t, ring, "k1", "ledger",
		WithClock(fixedClock(time.Date(2026, 9, 26, 12, 0, 5, 123456789, time.UTC))))

	signed := signer.Sign(headersFor(ev), []byte(`{"amount":"10.00"}`))

	sig, ok := headerValue(signed, HeaderSignature)
	require.True(t, ok)
	assert.Equal(t, "v1.AJbY2MqfEv5EvFyAS70emQDM59tj3lpf0PBYeUW0ho4", sig)

	kid, _ := headerValue(signed, HeaderKeyID)
	assert.Equal(t, "k1", kid)

	ts, _ := headerValue(signed, HeaderSignedAt)
	assert.Equal(t, "2026-09-26T12:00:05.123456789Z", ts)
}

// TestSignedFields_CoverEveryCodecHeader keeps the signed set equal to every
// ce-* header the codec can emit. A header outside the signature is a header
// an attacker can rewrite under a valid MAC — ce-eventtype selects the handler,
// ce-schemaversion selects the payload parser — so a new codec header that is
// not added here must fail the build of the test suite.
func TestSignedFields_CoverEveryCodecHeader(t *testing.T) {
	t.Parallel()

	emitted := make([]string, 0, 13)
	for _, h := range headersFor(fullEvent()) {
		emitted = append(emitted, h.Key)
	}

	signed := append([]string(nil), signedEnvelopeHeaders[:]...)

	sort.Strings(emitted)
	sort.Strings(signed)
	assert.Equal(t, emitted, signed)
	assert.Len(t, signed, 13)
}

func TestCanonical_LengthPrefixDefeatsConcatenation(t *testing.T) {
	t.Parallel()

	// Two field sets whose plain concatenation is identical ("ab"+"c" and
	// "a"+"bc") must not produce the same canonical bytes.
	var left, right fieldSet

	left[idxSource] = field{present: true, value: []byte("ab")}
	left[idxType] = field{present: true, value: []byte("c")}
	right[idxSource] = field{present: true, value: []byte("a")}
	right[idxType] = field{present: true, value: []byte("bc")}

	assert.NotEqual(t, canonical(&left, nil), canonical(&right, nil))
}

func TestCanonical_AbsentDiffersFromEmpty(t *testing.T) {
	t.Parallel()

	var absent, empty fieldSet

	empty[idxSubject] = field{present: true, value: []byte{}}

	assert.NotEqual(t, canonical(&absent, nil), canonical(&empty, nil))
}

func TestCanonical_BodyIsBound(t *testing.T) {
	t.Parallel()

	var fs fieldSet

	assert.NotEqual(t, canonical(&fs, []byte("a")), canonical(&fs, []byte("b")))
	assert.Equal(t, canonical(&fs, nil), canonical(&fs, []byte{}))
}
