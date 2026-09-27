//go:build unit

package envelopesig

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

var (
	signedAt = time.Date(2026, 9, 26, 12, 0, 5, 0, time.UTC)
	testBody = []byte(`{"amount":"10.00"}`)
)

// signedRecordHeaders returns the record headers a producer holding key would
// publish for fullEvent().
func signedRecordHeaders(t *testing.T, ring *Keyring, id, source string) []kgo.RecordHeader {
	t.Helper()

	signer := mustSigner(t, ring, id, source, WithClock(fixedClock(signedAt)))

	return toRecordHeaders(signer.Sign(headersFor(fullEvent()), testBody))
}

func withoutHeader(headers []kgo.RecordHeader, key string) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(headers))

	for _, h := range headers {
		if h.Key != key {
			out = append(out, h)
		}
	}

	return out
}

func withHeader(headers []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(headers))
	copy(out, headers)

	for i := range out {
		if out[i].Key == key {
			out[i] = kgo.RecordHeader{Key: key, Value: []byte(value)}
		}
	}

	return out
}

func TestVerify_Matrix(t *testing.T) {
	t.Parallel()

	ledgerKey := Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)}
	lenderKey := Key{ID: "k9", Source: "lender", Secret: testSecret(9, 32)}
	ring := mustKeyring(t, ledgerKey, lenderKey)
	valid := signedRecordHeaders(t, ring, "k1", "ledger")

	// A key of ANOTHER source signing a record that claims ce-source=ledger:
	// the MAC is genuine, the claim is forged.
	forgedByLender := func() []kgo.RecordHeader {
		lenderRing := mustKeyring(t, Key{ID: "k9", Source: "lender", Secret: testSecret(9, 32)})
		signer := mustSigner(t, lenderRing, "k9", "lender", WithClock(fixedClock(signedAt)))

		return toRecordHeaders(signer.Sign(headersFor(fullEvent()), testBody))
	}()

	cases := []struct {
		name    string
		headers []kgo.RecordHeader
		body    []byte
		want    error
	}{
		{name: "valid", headers: valid, body: testBody},
		{name: "missing kid", headers: withoutHeader(valid, HeaderKeyID), body: testBody, want: ErrSignatureMissing},
		{name: "missing sigts", headers: withoutHeader(valid, HeaderSignedAt), body: testBody, want: ErrSignatureMissing},
		{name: "missing sig", headers: withoutHeader(valid, HeaderSignature), body: testBody, want: ErrSignatureMissing},
		{name: "unsigned", headers: toRecordHeaders(headersFor(fullEvent())), body: testBody, want: ErrSignatureMissing},
		{name: "unknown kid", headers: withHeader(valid, HeaderKeyID, "k2"), body: testBody, want: ErrSignatureUnknownKey},
		{name: "malformed kid", headers: withHeader(valid, HeaderKeyID, "K1\n"), body: testBody, want: ErrSignatureInvalid},
		{name: "body flipped", headers: valid, body: []byte(`{"amount":"99.00"}`), want: ErrSignatureInvalid},
		{name: "body empty", headers: valid, body: nil, want: ErrSignatureInvalid},
		{name: "key bound to other source", headers: forgedByLender, body: testBody, want: ErrSignatureInvalid},
		{name: "v2 prefix", headers: withHeader(valid, HeaderSignature, "v2."+strings.TrimPrefix(headerOf(valid, HeaderSignature), "v1.")), body: testBody, want: ErrSignatureInvalid},
		{name: "no prefix", headers: withHeader(valid, HeaderSignature, strings.TrimPrefix(headerOf(valid, HeaderSignature), "v1.")), body: testBody, want: ErrSignatureInvalid},
		{name: "bad base64", headers: withHeader(valid, HeaderSignature, "v1.!!!not-base64!!!"), body: testBody, want: ErrSignatureInvalid},
		{name: "short mac", headers: withHeader(valid, HeaderSignature, "v1.AAAA"), body: testBody, want: ErrSignatureInvalid},
		{name: "empty sig", headers: withHeader(valid, HeaderSignature, ""), body: testBody, want: ErrSignatureInvalid},
		{name: "bad sigts", headers: withHeader(valid, HeaderSignedAt, "yesterday"), body: testBody, want: ErrSignatureInvalid},
		{name: "sigts moved", headers: withHeader(valid, HeaderSignedAt, "2026-09-26T12:00:06Z"), body: testBody, want: ErrSignatureInvalid},
		{name: "duplicate ce-source", headers: append(append([]kgo.RecordHeader(nil), valid...), kgo.RecordHeader{Key: "ce-source", Value: []byte("ledger")}), body: testBody, want: ErrSignatureInvalid},
		{name: "duplicate sig", headers: append(append([]kgo.RecordHeader(nil), valid...), kgo.RecordHeader{Key: HeaderSignature, Value: []byte(headerOf(valid, HeaderSignature))}), body: testBody, want: ErrSignatureInvalid},
		{name: "unsigned header added", headers: append(append([]kgo.RecordHeader(nil), valid...), kgo.RecordHeader{Key: "traceparent", Value: []byte("00-abc-def-01")}), body: testBody},
	}

	// Every one of the 13 signed ce-* headers, flipped, must break the MAC.
	for _, key := range signedEnvelopeHeaders {
		if key == "ce-source" {
			// A flipped ce-source also breaks the key binding; covered above
			// and below, asserted separately so the reason is precise.
			continue
		}

		cases = append(cases, struct {
			name    string
			headers []kgo.RecordHeader
			body    []byte
			want    error
		}{name: "flipped " + key, headers: withHeader(valid, key, headerOf(valid, key)+"x"), body: testBody, want: ErrSignatureInvalid})
	}

	// Removing an optional signed header (absent vs present) breaks the MAC.
	cases = append(cases, struct {
		name    string
		headers []kgo.RecordHeader
		body    []byte
		want    error
	}{name: "dropped ce-subject", headers: withoutHeader(valid, "ce-subject"), body: testBody, want: ErrSignatureInvalid})

	verifier := mustVerifier(t, ring, 0, WithClock(fixedClock(signedAt.Add(72*time.Hour))))

	for _, tc := range cases {
		err := verifier.Verify(tc.headers, tc.body)
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

func TestVerify_ForeignSourceClaim(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	valid := signedRecordHeaders(t, ring, "k1", "ledger")

	err := mustVerifier(t, ring, 0).Verify(withHeader(valid, "ce-source", "lender"), testBody)
	require.ErrorIs(t, err, ErrSignatureInvalid)
	assert.Contains(t, err.Error(), `"lender"`)
	assert.Contains(t, err.Error(), `"k1"`)
}

func TestVerify_RotationOverlapAcceptsBothKeys(t *testing.T) {
	t.Parallel()

	oldKey := Key{ID: "2026-08", Source: "ledger", Secret: testSecret(1, 32)}
	newKey := Key{ID: "2026-09", Source: "ledger", Secret: testSecret(2, 32)}
	verifier := mustVerifier(t, mustKeyring(t, oldKey, newKey), 0)

	for _, key := range []Key{oldKey, newKey} {
		headers := signedRecordHeaders(t, mustKeyring(t, key), key.ID, "ledger")
		require.NoError(t, verifier.Verify(headers, testBody), key.ID)
	}

	// Once the old key leaves the verifier's ring, its records are unknown.
	onlyNew := mustVerifier(t, mustKeyring(t, newKey), 0)
	err := onlyNew.Verify(signedRecordHeaders(t, mustKeyring(t, oldKey), oldKey.ID, "ledger"), testBody)
	require.ErrorIs(t, err, ErrSignatureUnknownKey)
}

// TestVerify_WrongSecretSameKeyID is a producer and a consumer that disagree
// on the bytes behind one key id: a genuine signature, the wrong key.
func TestVerify_WrongSecretSameKeyID(t *testing.T) {
	t.Parallel()

	producerRing := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	consumerRing := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(2, 32)})

	err := mustVerifier(t, consumerRing, 0).Verify(signedRecordHeaders(t, producerRing, "k1", "ledger"), testBody)
	require.ErrorIs(t, err, ErrSignatureInvalid)
}

func TestVerify_OldSignatureAcceptedWhenSkewDisabled(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	headers := signedRecordHeaders(t, ring, "k1", "ledger")

	// A year of consumer lag is still a legitimate backlog.
	verifier := mustVerifier(t, ring, 0, WithClock(fixedClock(signedAt.AddDate(1, 0, 0))))
	require.NoError(t, verifier.Verify(headers, testBody))
}

func TestVerify_MaxSkewRejectsWhenSet(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	headers := signedRecordHeaders(t, ring, "k1", "ledger")

	within := mustVerifier(t, ring, time.Minute, WithClock(fixedClock(signedAt.Add(59*time.Second))))
	require.NoError(t, within.Verify(headers, testBody))

	late := mustVerifier(t, ring, time.Minute, WithClock(fixedClock(signedAt.Add(61*time.Second))))
	require.ErrorIs(t, late.Verify(headers, testBody), ErrSignatureInvalid)

	early := mustVerifier(t, ring, time.Minute, WithClock(fixedClock(signedAt.Add(-61*time.Second))))
	require.ErrorIs(t, early.Verify(headers, testBody), ErrSignatureInvalid)
}

func TestNewVerifier_Rejects(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})

	_, err := NewVerifier(nil, 0)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)

	_, err = NewVerifier(ring, -time.Second)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
}

// TestVerify_NilVerifierFailsClosed: a verifier that is not there must not
// read as "verified".
func TestVerify_NilVerifierFailsClosed(t *testing.T) {
	t.Parallel()

	var verifier *Verifier

	require.ErrorIs(t, verifier.Verify(nil, nil), ErrSignatureInvalid)
}

// TestVerify_ErrorNeverCarriesMACOrSecret keeps key material and the expected
// MAC out of the error text, which lands verbatim on a DLQ header.
func TestVerify_ErrorNeverCarriesMACOrSecret(t *testing.T) {
	t.Parallel()

	secret := Secret("S3CR3T-S3CR3T-S3CR3T-S3CR3T-S3CR3T")
	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: secret})
	valid := signedRecordHeaders(t, ring, "k1", "ledger")
	expectedSig := strings.TrimPrefix(headerOf(valid, HeaderSignature), "v1.")

	err := mustVerifier(t, ring, 0).Verify(valid, []byte(`{"amount":"99.00"}`))
	require.ErrorIs(t, err, ErrSignatureInvalid)
	assert.NotContains(t, err.Error(), "S3CR3T")
	assert.NotContains(t, err.Error(), expectedSig)
}

// TestVerify_ErrorBoundsHostileHeaderValues: kid and source come from the
// wire, so their echo in the error text is quoted and bounded.
func TestVerify_ErrorBoundsHostileHeaderValues(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	valid := signedRecordHeaders(t, ring, "k1", "ledger")
	hostile := "k1\nlevel=error msg=forged " + strings.Repeat("A", 10_000)

	err := mustVerifier(t, ring, 0).Verify(withHeader(valid, HeaderKeyID, hostile), testBody)
	require.ErrorIs(t, err, ErrSignatureInvalid)
	assert.NotContains(t, err.Error(), "\n")
	assert.Less(t, len(err.Error()), 512)
}

func headerOf(headers []kgo.RecordHeader, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}

// TestVerify_UsesConstantTimeCompare pins the MAC comparison to hmac.Equal.
// Timing cannot be measured reliably in a unit test (and this suite never
// generates CPU load to try), so the implementation choice is what is pinned:
// verify.go must call hmac.Equal and must not reach for any variable-time
// comparison on byte slices or strings.
func TestVerify_UsesConstantTimeCompare(t *testing.T) {
	t.Parallel()

	file, err := parser.ParseFile(token.NewFileSet(), "verify.go", nil, 0)
	require.NoError(t, err)

	forbidden := map[string]bool{
		"bytes.Equal": true, "bytes.Compare": true, "reflect.DeepEqual": true,
		"strings.EqualFold": true, "strings.Compare": true,
	}

	var usesHMACEqual bool

	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SelectorExpr:
			pkg, ok := node.X.(*ast.Ident)
			if !ok {
				return true
			}

			name := pkg.Name + "." + node.Sel.Name
			if name == "hmac.Equal" {
				usesHMACEqual = true
			}

			assert.False(t, forbidden[name], "verify.go must not call %s", name)
			assert.NotEqual(t, "subtle", pkg.Name, "verify.go must compare the MAC with hmac.Equal, not crypto/subtle directly")
		case *ast.BinaryExpr:
			if node.Op != token.EQL && node.Op != token.NEQ {
				return true
			}

			if isNil(node.X) || isNil(node.Y) {
				return true
			}

			for _, operand := range []ast.Expr{node.X, node.Y} {
				if ident, ok := operand.(*ast.Ident); ok {
					assert.False(t, macMaterial(ident.Name),
						"verify.go compares %s with %s; MAC material goes through hmac.Equal only", ident.Name, node.Op)
				}
			}
		}

		return true
	})

	assert.True(t, usesHMACEqual, "verify.go must compare the MAC with hmac.Equal")
}

func isNil(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)

	return ok && ident.Name == "nil"
}

// macMaterial names the identifiers verify.go uses for MAC bytes.
func macMaterial(name string) bool {
	lower := strings.ToLower(name)

	return strings.Contains(lower, "mac") ||
		lower == "received" || lower == "presented" || lower == "encoded" || lower == "expected"
}
