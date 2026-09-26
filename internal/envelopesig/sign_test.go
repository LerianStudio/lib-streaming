//go:build unit

package envelopesig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

func TestSigner_NilIsNoop(t *testing.T) {
	t.Parallel()

	var signer *Signer

	headers := headersFor(fullEvent())
	got := signer.Sign(headers, []byte(`{}`))
	assert.Equal(t, headers, got)
}

func TestSigner_RejectsKeyOfOtherSource(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "lender", Secret: testSecret(1, 32)})

	_, err := NewSigner(ring, "k1", "ledger")
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Contains(t, err.Error(), `"lender"`)
	assert.Contains(t, err.Error(), `"ledger"`)
}

func TestSigner_RejectsUnknownOrMissingKey(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})

	_, err := NewSigner(ring, "k2", "ledger")
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)

	_, err = NewSigner(ring, "", "ledger")
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)

	_, err = NewSigner(nil, "k1", "ledger")
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
}

// TestSigner_AppendsWithoutMutating proves the input slice is left exactly as
// it was: the producer shares one header slice across every route of an Emit.
func TestSigner_AppendsWithoutMutating(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	signer := mustSigner(t, ring, "k1", "ledger")

	base := headersFor(fullEvent())
	// Spare capacity is where an in-place append would clobber a caller.
	headers := make([]transport.Header, len(base), len(base)+8)
	copy(headers, base)

	snapshot := append([]transport.Header(nil), headers...)
	signed := signer.Sign(headers, []byte(`{}`))

	assert.Equal(t, snapshot, headers)
	assert.Equal(t, snapshot, headers[:len(snapshot)])
	require.Len(t, signed, len(headers)+3)
	assert.Equal(t, HeaderKeyID, signed[len(headers)].Key)
	assert.Equal(t, HeaderSignedAt, signed[len(headers)+1].Key)
	assert.Equal(t, HeaderSignature, signed[len(headers)+2].Key)
}

// TestSigner_ReplacesExistingSignature re-signs a header set that already
// carries a signature (a republish) without duplicating the three keys, which
// a verifier would refuse.
func TestSigner_ReplacesExistingSignature(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	first := mustSigner(t, ring, "k1", "ledger",
		WithClock(fixedClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))))
	second := mustSigner(t, ring, "k1", "ledger",
		WithClock(fixedClock(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC))))

	resigned := second.Sign(first.Sign(headersFor(fullEvent()), []byte(`{}`)), []byte(`{}`))

	count := 0

	for _, h := range resigned {
		if h.Key == HeaderKeyID || h.Key == HeaderSignedAt || h.Key == HeaderSignature {
			count++
		}
	}

	assert.Equal(t, 3, count)

	ts, _ := headerValue(resigned, HeaderSignedAt)
	assert.Equal(t, "2026-02-01T00:00:00Z", ts)
	require.NoError(t, mustVerifier(t, ring, 0).Verify(toRecordHeaders(resigned), []byte(`{}`)))
}

// TestSigner_TimestampIsSigningInstantUTC pins ce-sigts to the signer's clock
// in UTC, independent of ce-time.
func TestSigner_TimestampIsSigningInstantUTC(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	local := time.FixedZone("BRT", -3*60*60)
	signer := mustSigner(t, ring, "k1", "ledger",
		WithClock(fixedClock(time.Date(2026, 9, 26, 9, 30, 0, 0, local))))

	signed := signer.Sign(headersFor(fullEvent()), []byte(`{}`))

	ts, _ := headerValue(signed, HeaderSignedAt)
	assert.Equal(t, "2026-09-26T12:30:00Z", ts)

	ceTime, _ := headerValue(signed, "ce-time")
	assert.Equal(t, "2026-09-26T12:00:00Z", ceTime)
}

func TestStrip_RemovesOnlySignatureHeadersAndCopies(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, MinSecretBytes)})
	signed := mustSigner(t, ring, "k1", "ledger").Sign(headersFor(fullEvent()), []byte(`{}`))

	stripped := Strip(signed)

	require.Len(t, stripped, len(signed)-3)

	for _, h := range stripped {
		require.False(t, isSignatureHeader(h.Key), "header %s survived Strip", h.Key)
	}

	require.Equal(t, headersFor(fullEvent()), stripped)

	stripped[0].Key = "mutated"
	require.NotEqual(t, "mutated", signed[0].Key, "Strip must return a fresh slice")
}
