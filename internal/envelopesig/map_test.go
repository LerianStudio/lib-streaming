//go:build unit

package envelopesig

import (
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// toMapHeaders is what the RabbitMQ adapter hands its publisher: every header
// as a fresh []byte under its own key.
func toMapHeaders(headers []transport.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for _, h := range headers {
		out[h.Key] = append([]byte(nil), h.Value...)
	}

	return out
}

// asStrings is the same table with every value a string, the form an AMQP
// client typically writes its own headers in.
func asStrings(headers map[string]any) map[string]any {
	out := make(map[string]any, len(headers))
	for k, v := range headers {
		if b, ok := v.([]byte); ok {
			out[k] = string(b)

			continue
		}

		out[k] = v
	}

	return out
}

func mapToRecordHeaders(headers map[string]any) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		switch value := v.(type) {
		case []byte:
			out = append(out, kgo.RecordHeader{Key: k, Value: value})
		case string:
			out = append(out, kgo.RecordHeader{Key: k, Value: []byte(value)})
		}
	}

	return out
}

func withMapValue(headers map[string]any, key string, value any) map[string]any {
	out := maps.Clone(headers)
	out[key] = value

	return out
}

func withoutMapKey(headers map[string]any, key string) map[string]any {
	out := maps.Clone(headers)
	delete(out, key)

	return out
}

func ledgerRing(t *testing.T) *Keyring {
	t.Helper()

	return mustKeyring(t,
		Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)},
		Key{ID: "k9", Source: "lender", Secret: testSecret(9, 32)},
	)
}

func signedMap(t *testing.T, ring *Keyring, id, source string) map[string]any {
	t.Helper()

	signer := mustSigner(t, ring, id, source, WithClock(fixedClock(signedAt)))

	out, err := signer.SignMap(toMapHeaders(headersFor(fullEvent())), testBody)
	require.NoError(t, err)

	return out
}

func TestSignMap_VerifyMapRoundTrip(t *testing.T) {
	t.Parallel()

	ring := ledgerRing(t)
	signed := signedMap(t, ring, "k1", "ledger")

	for _, key := range []string{HeaderKeyID, HeaderSignedAt, HeaderSignature} {
		assert.IsType(t, []byte(nil), signed[key], "%s is written as []byte", key)
	}

	verifier := mustVerifier(t, ring, 0)
	require.NoError(t, verifier.VerifyMap(signed, testBody))
	require.NoError(t, verifier.VerifyMap(asStrings(signed), testBody), "string and []byte values are the same bytes")
}

func TestSignMap_StringAndBytesInputsSignIdentically(t *testing.T) {
	t.Parallel()

	ring := ledgerRing(t)
	signer := mustSigner(t, ring, "k1", "ledger", WithClock(fixedClock(signedAt)))
	headers := toMapHeaders(headersFor(fullEvent()))

	fromBytes, err := signer.SignMap(headers, testBody)
	require.NoError(t, err)

	fromStrings, err := signer.SignMap(asStrings(headers), testBody)
	require.NoError(t, err)

	assert.Equal(t, fromBytes[HeaderSignature], fromStrings[HeaderSignature])
}

func TestVerifyMap_Matrix(t *testing.T) {
	t.Parallel()

	ring := ledgerRing(t)
	valid := signedMap(t, ring, "k1", "ledger")

	lenderRing := mustKeyring(t, Key{ID: "k9", Source: "lender", Secret: testSecret(9, 32)})
	lenderSigner := mustSigner(t, lenderRing, "k9", "lender", WithClock(fixedClock(signedAt)))
	// A lender key's genuine MAC over a record claiming ce-source=ledger:
	// signed through the header-slice path, which does not check the source.
	forgedByLender := toMapHeaders(lenderSigner.Sign(headersFor(fullEvent()), testBody))

	cases := []struct {
		name    string
		headers map[string]any
		body    []byte
		want    error
	}{
		{name: "valid", headers: valid, body: testBody},
		{name: "unsigned header added", headers: withMapValue(valid, "X-Tenant-ID", "tenant-1"), body: testBody},
		{name: "non-bytes unsigned header", headers: withMapValue(valid, "x-delivery-count", int64(3)), body: testBody},
		{name: "unsigned", headers: toMapHeaders(headersFor(fullEvent())), body: testBody, want: ErrSignatureMissing},
		{name: "missing sig", headers: withoutMapKey(valid, HeaderSignature), body: testBody, want: ErrSignatureMissing},
		{name: "missing kid", headers: withoutMapKey(valid, HeaderKeyID), body: testBody, want: ErrSignatureMissing},
		{name: "unknown kid", headers: withMapValue(valid, HeaderKeyID, []byte("k2")), body: testBody, want: ErrSignatureUnknownKey},
		{name: "body tampered", headers: valid, body: []byte(`{"amount":"99.00"}`), want: ErrSignatureInvalid},
		{name: "ce-tenantid tampered", headers: withMapValue(valid, "ce-tenantid", []byte("tenant-2")), body: testBody, want: ErrSignatureInvalid},
		{name: "ce-subject dropped", headers: withoutMapKey(valid, "ce-subject"), body: testBody, want: ErrSignatureInvalid},
		{name: "key bound to other source", headers: forgedByLender, body: testBody, want: ErrSignatureInvalid},
		{name: "non-bytes signed header", headers: withMapValue(valid, "ce-time", time.Now()), body: testBody, want: ErrSignatureInvalid},
		{name: "non-bytes sig", headers: withMapValue(valid, HeaderSignature, 42), body: testBody, want: ErrSignatureInvalid},
		{name: "nil signed value", headers: withMapValue(valid, "ce-subject", nil), body: testBody, want: ErrSignatureInvalid},
		{name: "empty sig", headers: withMapValue(valid, HeaderSignature, []byte(nil)), body: testBody, want: ErrSignatureInvalid},
	}

	verifier := mustVerifier(t, ring, 0)

	for _, tc := range cases {
		err := verifier.VerifyMap(tc.headers, tc.body)
		if tc.want == nil {
			require.NoError(t, err, tc.name)

			continue
		}

		require.ErrorIs(t, err, tc.want, tc.name)

		for _, other := range []error{ErrSignatureMissing, ErrSignatureUnknownKey, ErrSignatureInvalid} {
			if !errors.Is(tc.want, other) {
				assert.NotErrorIs(t, err, other, "%s: kinds must be disjoint", tc.name)
			}
		}
	}
}

func TestVerifyMap_NilVerifierFailsClosed(t *testing.T) {
	t.Parallel()

	var verifier *Verifier

	require.ErrorIs(t, verifier.VerifyMap(signedMap(t, ledgerRing(t), "k1", "ledger"), testBody), ErrSignatureInvalid)
}

func TestSignMap_ReplacesPreviousSignature(t *testing.T) {
	t.Parallel()

	ring := ledgerRing(t)
	first := signedMap(t, ring, "k1", "ledger")

	later := mustSigner(t, ring, "k1", "ledger", WithClock(fixedClock(signedAt.Add(time.Hour))))

	resigned, err := later.SignMap(first, testBody)
	require.NoError(t, err)

	assert.Len(t, resigned, len(first), "the three signature headers are replaced, never duplicated")
	assert.NotEqual(t, first[HeaderSignedAt], resigned[HeaderSignedAt])
	require.NoError(t, mustVerifier(t, ring, 0).VerifyMap(resigned, testBody))
}

func TestSignMap_NeverMutatesInput(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t, ledgerRing(t), "k1", "ledger")
	input := withMapValue(toMapHeaders(headersFor(fullEvent())), HeaderSignature, []byte("v1.stale"))
	snapshot := maps.Clone(input)

	_, err := signer.SignMap(input, testBody)
	require.NoError(t, err)

	assert.Equal(t, snapshot, input)
}

func TestSignMap_RefusesRecordOfAnotherSource(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t, ledgerRing(t), "k1", "ledger")
	headers := toMapHeaders(headersFor(fullEvent()))

	for name, input := range map[string]map[string]any{
		"foreign source": withMapValue(headers, "ce-source", []byte("lender")),
		"missing source": withoutMapKey(headers, "ce-source"),
	} {
		out, err := signer.SignMap(input, testBody)
		require.ErrorIs(t, err, contract.ErrSigningSourceMismatch, name)
		assert.Nil(t, out, name)
	}
}

func TestSignMap_RefusesUnsupportedSignedValue(t *testing.T) {
	t.Parallel()

	signer := mustSigner(t, ledgerRing(t), "k1", "ledger")

	out, err := signer.SignMap(withMapValue(toMapHeaders(headersFor(fullEvent())), "ce-time", time.Now()), testBody)
	require.ErrorIs(t, err, contract.ErrUnsupportedHeaderValue)
	assert.True(t, contract.IsCallerError(err))
	assert.Contains(t, err.Error(), "ce-time")
	assert.Nil(t, out)

	// A header outside the signature may hold any value the transport takes.
	out, err = signer.SignMap(withMapValue(toMapHeaders(headersFor(fullEvent())), "x-priority", int32(5)), testBody)
	require.NoError(t, err)
	assert.Equal(t, int32(5), out["x-priority"])
}

func TestSignMap_NilSignerFailsClosed(t *testing.T) {
	t.Parallel()

	var signer *Signer

	out, err := signer.SignMap(toMapHeaders(headersFor(fullEvent())), testBody)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Nil(t, out)
}

// TestSignature_OneEncodingAcrossHeaderForms proves the map and the record
// forms sign the same canonical bytes: each verifies what the other signed.
func TestSignature_OneEncodingAcrossHeaderForms(t *testing.T) {
	t.Parallel()

	ring := ledgerRing(t)
	signer := mustSigner(t, ring, "k1", "ledger", WithClock(fixedClock(signedAt)))
	verifier := mustVerifier(t, ring, 0)

	fromSlice := signer.Sign(headersFor(fullEvent()), testBody)
	require.NoError(t, verifier.VerifyMap(toMapHeaders(fromSlice), testBody))

	fromMap, err := signer.SignMap(toMapHeaders(headersFor(fullEvent())), testBody)
	require.NoError(t, err)
	require.NoError(t, verifier.Verify(mapToRecordHeaders(fromMap), testBody))

	assert.Equal(t, string(toMapHeaders(fromSlice)[HeaderSignature].([]byte)), string(fromMap[HeaderSignature].([]byte)))
}
