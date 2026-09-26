//go:build unit

package envelopesig

import (
	"bytes"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// decodeFuzzHeaders splits blob into records separated by 0x1E, each "key\x1Fvalue".
func decodeFuzzHeaders(blob []byte) []kgo.RecordHeader {
	var headers []kgo.RecordHeader

	for _, raw := range bytes.Split(blob, []byte{0x1E}) {
		key, value, _ := bytes.Cut(raw, []byte{0x1F})
		headers = append(headers, kgo.RecordHeader{Key: string(key), Value: value})
	}

	return headers
}

func encodeFuzzHeaders(headers []kgo.RecordHeader) []byte {
	var buf bytes.Buffer

	for i, h := range headers {
		if i > 0 {
			buf.WriteByte(0x1E)
		}

		buf.WriteString(h.Key)
		buf.WriteByte(0x1F)
		buf.Write(h.Value)
	}

	return buf.Bytes()
}

// FuzzVerify_NeverPanics drives Verify with arbitrary headers and bodies. The
// invariants: never panic, and every refusal is exactly one of the three
// documented kinds, so the consumer always has a cause kind to stamp.
func FuzzVerify_NeverPanics(f *testing.F) {
	ring := mustKeyring(f, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})
	signer := mustSigner(f, ring, "k1", "ledger", WithClock(fixedClock(signedAt)))
	valid := toRecordHeaders(signer.Sign(headersFor(fullEvent()), testBody))

	f.Add(encodeFuzzHeaders(valid), testBody)
	f.Add(encodeFuzzHeaders(toRecordHeaders(headersFor(fullEvent()))), testBody)
	f.Add(encodeFuzzHeaders(withHeader(valid, HeaderSignature, "v1.")), testBody)
	f.Add(encodeFuzzHeaders(withHeader(valid, HeaderKeyID, "\x00\xff")), []byte{})
	f.Add(encodeFuzzHeaders(withHeader(valid, HeaderSignedAt, "")), []byte(nil))
	f.Add([]byte{}, []byte{})

	verifier := mustVerifier(f, ring, 0)

	f.Fuzz(func(t *testing.T, blob, body []byte) {
		err := verifier.Verify(decodeFuzzHeaders(blob), body)
		if err == nil {
			return
		}

		kinds := 0

		for _, kind := range []error{ErrSignatureMissing, ErrSignatureUnknownKey, ErrSignatureInvalid} {
			if errors.Is(err, kind) {
				kinds++
			}
		}

		if kinds != 1 {
			t.Fatalf("refusal %q matches %d kinds, want exactly 1", err, kinds)
		}
	})
}
