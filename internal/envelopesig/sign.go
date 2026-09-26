package envelopesig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// Option adjusts a Signer or Verifier at construction.
type Option func(*options)

type options struct {
	now func() time.Time
}

// WithClock replaces the wall clock. A nil clock is ignored.
func WithClock(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

func resolveOptions(opts []Option) options {
	resolved := options{now: time.Now}

	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}

	return resolved
}

// Signer signs outgoing envelopes with one active key. Immutable after
// construction and safe for concurrent use. A nil *Signer is the "signing not
// configured" state: Sign returns its input unchanged.
type Signer struct {
	key Key
	now func() time.Time
}

// NewSigner returns a signer for the key activeKeyID in ring. It refuses a nil
// ring, an id absent from it, and a key bound to a source other than the
// producer's own — a producer can only ever vouch for itself.
func NewSigner(ring *Keyring, activeKeyID, source string, opts ...Option) (*Signer, error) {
	key, ok := ring.lookup(activeKeyID)
	if !ok {
		return nil, fmt.Errorf("%w: active signing key %s is not in the keyring %s",
			contract.ErrInvalidSigningKey, quoteBounded([]byte(activeKeyID)), ring)
	}

	if key.Source != source {
		return nil, fmt.Errorf("%w: active signing key %q is bound to source %q, the producer is %q",
			contract.ErrInvalidSigningKey, key.ID, key.Source, source)
	}

	return &Signer{key: key, now: resolveOptions(opts).now}, nil
}

// Sign returns a NEW header slice: headers without any previous signature,
// followed by ce-sigkid, ce-sigts (the signer's clock, now) and ce-sig computed
// over them and body. The input slice and its elements are never modified,
// because one header slice is shared by every route of an Emit.
//
// Replacing a previous signature (rather than appending a second one) is what
// makes a republish verifiable: a verifier refuses a duplicated signature key.
func (s *Signer) Sign(headers []transport.Header, body []byte) []transport.Header {
	if s == nil {
		return headers
	}

	signedAt := []byte(s.now().UTC().Format(time.RFC3339Nano))
	keyID := []byte(s.key.ID)

	out := make([]transport.Header, 0, len(headers)+3)

	var fields fieldSet

	for _, h := range headers {
		if isSignatureHeader(h.Key) {
			continue
		}

		out = append(out, h)

		if i, ok := fieldIndex[h.Key]; ok {
			fields[i] = field{present: true, value: h.Value}
		}
	}

	fields[idxKeyID] = field{present: true, value: keyID}
	fields[idxSignedAt] = field{present: true, value: signedAt}

	mac := hmac.New(sha256.New, s.key.Secret)
	_, _ = mac.Write(canonical(&fields, body))

	signature := signatureVersionPrefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return append(out,
		transport.Header{Key: HeaderKeyID, Value: keyID},
		transport.Header{Key: HeaderSignedAt, Value: signedAt},
		transport.Header{Key: HeaderSignature, Value: []byte(signature)},
	)
}

// isSignatureHeader reports whether key is one of the three signature headers.
func isSignatureHeader(key string) bool {
	return key == HeaderKeyID || key == HeaderSignedAt || key == HeaderSignature
}
