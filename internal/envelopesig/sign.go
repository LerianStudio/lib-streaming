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
// producer's own — a producer can only ever vouch for itself. An activeKeyID
// the id pattern refuses is never echoed: it may be a secret in the wrong slot.
func NewSigner(ring *Keyring, activeKeyID, source string, opts ...Option) (*Signer, error) {
	if !ValidKeyID(activeKeyID) {
		return nil, fmt.Errorf("%w: active signing key id is not a legal key id (must match %s)",
			contract.ErrInvalidSigningKey, keyIDPattern.String())
	}

	key, ok := ring.lookup(activeKeyID)
	if !ok {
		return nil, fmt.Errorf("%w: active signing key %q is not in the keyring %s",
			contract.ErrInvalidSigningKey, activeKeyID, ring)
	}

	if key.Source != source {
		return nil, fmt.Errorf("%w: active signing key %q is bound to source %q, the producer is %q",
			contract.ErrInvalidSigningKey, key.ID, key.Source, source)
	}

	return &Signer{key: key, now: resolveOptions(opts).now}, nil
}

// CheckSource reports whether the signer may sign a record claiming source:
// only a record of the source its key is bound to. NewSigner checks the
// binding once against the producer's configured source; a publisher calls
// CheckSource per record for records it did not build itself — an outbox row
// persisted under an earlier or another source — so it fails that record
// instead of publishing a signature every verifier is certain to refuse.
// Returns nil on a nil *Signer (signing not configured) and otherwise wraps
// contract.ErrSigningSourceMismatch, which is not a caller error: the record is
// fine, the producer holding it is the wrong one to sign it.
func (s *Signer) CheckSource(source string) error {
	if s == nil || source == s.key.Source {
		return nil
	}

	return fmt.Errorf("%w: record claims source %s but active signing key %q is bound to source %q",
		contract.ErrSigningSourceMismatch, quoteBounded([]byte(source)), s.key.ID, s.key.Source)
}

// Sign returns a NEW header slice: headers without any previous signature,
// followed by ce-sigkid, ce-sigts (the signer's clock, now) and ce-sig computed
// over them and body. The input slice and its elements are never modified,
// because one header slice is shared by every route of an Emit.
//
// Replacing a previous signature (rather than appending a second one) is what
// makes a republish verifiable: a verifier refuses a duplicated signature key.
//
// Sign does not check the record's ce-source: the producer builds its own
// headers and calls CheckSource for a record it did not build.
func (s *Signer) Sign(headers []transport.Header, body []byte) []transport.Header {
	if s == nil {
		return headers
	}

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

	keyID, signedAt, signature := s.seal(&fields, body)

	return append(out,
		transport.Header{Key: HeaderKeyID, Value: keyID},
		transport.Header{Key: HeaderSignedAt, Value: signedAt},
		transport.Header{Key: HeaderSignature, Value: signature},
	)
}

// SignMap is Sign over a header table — the shape an AMQP client's headers
// take — for a publisher outside the library's producer. It returns a NEW map
// holding every input entry except a previous signature, plus ce-sigkid,
// ce-sigts and ce-sig as []byte; the input map is never modified (values are
// shared, not copied). A signed ce-* header may hold []byte or string, the
// same bytes either way; any other type fails with
// contract.ErrUnsupportedHeaderValue. Entries outside the signature may hold
// anything the transport accepts.
//
// Unlike Sign, SignMap applies the source binding itself: a table whose
// ce-source is absent or is not the key's source fails with
// contract.ErrSigningSourceMismatch, because the caller built those headers.
// A nil *Signer fails closed with contract.ErrInvalidSigningKey: outside the
// producer there is no "signing off" state to fall back to.
func (s *Signer) SignMap(headers map[string]any, body []byte) (map[string]any, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: signer not configured", contract.ErrInvalidSigningKey)
	}

	out := make(map[string]any, len(headers)+3)

	var fields fieldSet

	for key, value := range headers {
		if isSignatureHeader(key) {
			continue
		}

		out[key] = value

		i, ok := fieldIndex[key]
		if !ok {
			continue
		}

		raw, ok := headerBytes(value)
		if !ok {
			return nil, fmt.Errorf("%w: header %s holds a %T", contract.ErrUnsupportedHeaderValue, key, value)
		}

		fields[i] = field{present: true, value: raw}
	}

	if !fields[idxSource].present {
		return nil, fmt.Errorf("%w: record carries no ce-source; active signing key %q is bound to source %q",
			contract.ErrSigningSourceMismatch, s.key.ID, s.key.Source)
	}

	if err := s.CheckSource(string(fields[idxSource].value)); err != nil {
		return nil, err
	}

	keyID, signedAt, signature := s.seal(&fields, body)
	out[HeaderKeyID] = keyID
	out[HeaderSignedAt] = signedAt
	out[HeaderSignature] = signature

	return out, nil
}

// seal stamps the key id and the signing instant into fields and returns
// them with the ce-sig value computed over fields and body.
func (s *Signer) seal(fields *fieldSet, body []byte) (keyID, signedAt, signature []byte) {
	keyID = []byte(s.key.ID)
	signedAt = []byte(s.now().UTC().Format(time.RFC3339Nano))

	fields[idxKeyID] = field{present: true, value: keyID}
	fields[idxSignedAt] = field{present: true, value: signedAt}

	mac := hmac.New(sha256.New, s.key.Secret)
	_, _ = mac.Write(canonical(fields, body))

	signature = []byte(signatureVersionPrefix + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))

	return keyID, signedAt, signature
}

// headerBytes returns the raw bytes of a header-table value. The library's
// own adapters write []byte and AMQP clients commonly write string; nothing
// else can be signed without re-formatting it.
func headerBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

// Strip returns headers without the three signature headers, as a fresh slice;
// the input is never modified. A publisher uses it for a copy that must not
// carry a signature — for example a record whose body was dropped, which a
// signature over the empty body would turn into a verifiable event.
func Strip(headers []transport.Header) []transport.Header {
	out := make([]transport.Header, 0, len(headers))

	for _, h := range headers {
		if !isSignatureHeader(h.Key) {
			out = append(out, h)
		}
	}

	return out
}

// isSignatureHeader reports whether key is one of the three signature headers.
func isSignatureHeader(key string) bool {
	return key == HeaderKeyID || key == HeaderSignedAt || key == HeaderSignature
}
