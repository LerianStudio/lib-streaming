package envelopesig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// The three verification outcomes. They are disjoint and each has a different
// owner: a missing signature is a rollout gap (the producer does not sign
// yet), an unknown key id is key distribution (the verifier lacks a key the
// producer uses), and an invalid signature is forgery, tampering or a wrong
// key. None of them is a caller error: they describe a record on the wire.
var (
	ErrSignatureMissing    = errors.New("streaming: envelope signature missing")
	ErrSignatureUnknownKey = errors.New("streaming: envelope signature key id unknown")
	ErrSignatureInvalid    = errors.New("streaming: envelope signature invalid")
)

// maxEchoBytes bounds how much of a wire value an error text repeats. Kid and
// source come from the record, so an unbounded echo would let a producer fill
// the DLQ error header with its own text.
const maxEchoBytes = 96

// Verifier checks incoming envelopes against a keyring. Immutable after
// construction and safe for concurrent use.
type Verifier struct {
	ring    *Keyring
	maxSkew time.Duration
	now     func() time.Time
}

// NewVerifier returns a verifier accepting any key in ring. maxSkew 0 disables
// the age check, which is the safe default: a consumer lagging behind its
// topic sees legitimately old signatures, and rejecting them by age would turn
// backlog into quarantine. A positive maxSkew refuses a record whose ce-sigts
// is further than maxSkew from now in either direction; it is unsafe for any
// consumer that can lag. A nil or empty ring and a negative maxSkew are
// refused with contract.ErrInvalidSigningKey.
func NewVerifier(ring *Keyring, maxSkew time.Duration, opts ...Option) (*Verifier, error) {
	if ring.empty() {
		return nil, fmt.Errorf("%w: verifier keyring is empty", contract.ErrInvalidSigningKey)
	}

	if maxSkew < 0 {
		return nil, fmt.Errorf("%w: signature max skew %s is negative", contract.ErrInvalidSigningKey, maxSkew)
	}

	return &Verifier{ring: ring, maxSkew: maxSkew, now: resolveOptions(opts).now}, nil
}

// Verify checks the signature on a record's raw headers and body. It reads the
// raw header bytes — never a parsed event, whose values are sanitized — and
// refuses any signed or signature header that appears more than once, because
// the codec keeps the LAST value of a repeated key and the verifier must judge
// the exact bytes the handler will see.
//
// It returns nil or an error wrapping exactly one of ErrSignatureMissing (any
// signature header absent), ErrSignatureUnknownKey (a well-formed key id the
// ring does not hold) or ErrSignatureInvalid (everything else). The error text
// names the key id, the claimed source and the reason; it never carries the
// expected MAC or key material. A nil Verifier fails closed.
func (v *Verifier) Verify(headers []kgo.RecordHeader, body []byte) error {
	if v == nil {
		return errVerifierNotConfigured
	}

	fields, presented, err := indexRecordHeaders(headers)
	if err != nil {
		return err
	}

	return v.verify(&fields, presented, body)
}

// VerifyMap is Verify over a header table — the shape an AMQP delivery's
// headers take — for a consumer outside the library's Kafka runtime. A key
// occurs once in a map, so the duplicate check has nothing to judge. A signed
// ce-* header or ce-sig may hold []byte or string, the same bytes either way;
// any other type (nil included) is ErrSignatureInvalid. Entries outside the
// signature are ignored whatever they hold. Outcomes are Verify's.
func (v *Verifier) VerifyMap(headers map[string]any, body []byte) error {
	if v == nil {
		return errVerifierNotConfigured
	}

	fields, presented, err := indexMapHeaders(headers)
	if err != nil {
		return err
	}

	return v.verify(&fields, presented, body)
}

var errVerifierNotConfigured = fmt.Errorf("%w: verifier not configured", ErrSignatureInvalid)

// verify is the one verification core behind both header forms. presented is
// nil when ce-sig is absent.
func (v *Verifier) verify(fields *fieldSet, presented, body []byte) error {
	if !fields[idxKeyID].present || !fields[idxSignedAt].present || presented == nil {
		return fmt.Errorf("%w: record carries no complete %s/%s/%s set",
			ErrSignatureMissing, HeaderKeyID, HeaderSignedAt, HeaderSignature)
	}

	key, err := v.resolveKey(fields)
	if err != nil {
		return err
	}

	kid, claimed := fields[idxKeyID].value, fields[idxSource].value

	received, err := decodeSignature(presented)
	if err != nil {
		return invalid(kid, claimed, err.Error())
	}

	signedAt, parseErr := time.Parse(time.RFC3339Nano, string(fields[idxSignedAt].value))
	if parseErr != nil {
		return invalid(kid, claimed, "malformed "+HeaderSignedAt)
	}

	mac := hmac.New(sha256.New, key.Secret)
	_, _ = mac.Write(canonical(fields, body))

	if !hmac.Equal(received, mac.Sum(nil)) {
		return invalid(kid, claimed, "signature does not match")
	}

	if reason, ok := v.withinSkew(signedAt); !ok {
		return invalid(kid, claimed, reason)
	}

	return nil
}

// resolveKey finds the key the record names and checks that the record's
// ce-source is the source that key speaks for.
func (v *Verifier) resolveKey(fields *fieldSet) (Key, error) {
	kid, claimed := fields[idxKeyID].value, fields[idxSource].value

	if !keyIDPattern.Match(kid) {
		return Key{}, invalid(kid, claimed, "malformed key id")
	}

	key, ok := v.ring.lookup(string(kid))
	if !ok {
		return Key{}, fmt.Errorf("%w: key id %s (claimed source %s) is not in the verifier keyring",
			ErrSignatureUnknownKey, quoteBounded(kid), quoteBounded(claimed))
	}

	if !fields[idxSource].present || string(claimed) != key.Source {
		return Key{}, invalid(kid, claimed, "key is bound to source "+strconv.Quote(key.Source))
	}

	return key, nil
}

// decodeSignature parses a "v1." ce-sig value into its raw MAC bytes.
func decodeSignature(value []byte) ([]byte, error) {
	encoded, hasPrefix := strings.CutPrefix(string(value), signatureVersionPrefix)
	if !hasPrefix {
		return nil, errors.New("unsupported signature format")
	}

	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) != sha256.Size {
		return nil, errors.New("malformed signature value")
	}

	return raw, nil
}

// withinSkew applies the optional age bound; disabled (always true) at 0.
func (v *Verifier) withinSkew(signedAt time.Time) (string, bool) {
	if v.maxSkew <= 0 {
		return "", true
	}

	skew := v.now().Sub(signedAt)
	if skew < 0 {
		skew = -skew
	}

	if skew > v.maxSkew {
		return fmt.Sprintf("signed at %s, outside the %s max skew",
			signedAt.UTC().Format(time.RFC3339Nano), v.maxSkew), false
	}

	return "", true
}

// indexRecordHeaders collects the canonical fields and the presented
// signature, refusing any duplicate among them. presented is nil when ce-sig
// is absent.
func indexRecordHeaders(headers []kgo.RecordHeader) (fieldSet, []byte, error) {
	var (
		fields    fieldSet
		presented []byte
		seenSig   bool
	)

	for _, h := range headers {
		if h.Key == HeaderSignature {
			if seenSig {
				return fieldSet{}, nil, fmt.Errorf("%w: header %s appears more than once", ErrSignatureInvalid, h.Key)
			}

			seenSig = true
			presented = h.Value

			if presented == nil {
				presented = []byte{}
			}

			continue
		}

		i, ok := fieldIndex[h.Key]
		if !ok {
			continue
		}

		if fields[i].present {
			return fieldSet{}, nil, fmt.Errorf("%w: header %s appears more than once", ErrSignatureInvalid, h.Key)
		}

		fields[i] = field{present: true, value: h.Value}
	}

	return fields, presented, nil
}

// indexMapHeaders is indexRecordHeaders over a header table. A signed or
// signature entry whose value is not []byte or string is refused rather than
// re-formatted: the signature covers bytes, and a rendering of any other type
// is not what the producer signed.
func indexMapHeaders(headers map[string]any) (fieldSet, []byte, error) {
	var (
		fields    fieldSet
		presented []byte
	)

	for key, value := range headers {
		i, signed := fieldIndex[key]
		if !signed && key != HeaderSignature {
			continue
		}

		raw, ok := headerBytes(value)
		if !ok {
			return fieldSet{}, nil, fmt.Errorf("%w: header %s holds an unsupported value type %T",
				ErrSignatureInvalid, key, value)
		}

		if key == HeaderSignature {
			presented = raw
			if presented == nil {
				presented = []byte{}
			}

			continue
		}

		fields[i] = field{present: true, value: raw}
	}

	return fields, presented, nil
}

func invalid(kid, claimed []byte, reason string) error {
	return fmt.Errorf("%w: key id %s, claimed source %s: %s",
		ErrSignatureInvalid, quoteBounded(kid), quoteBounded(claimed), reason)
}

// quoteBounded quotes at most maxEchoBytes of a wire value, escaping control
// characters so the echo cannot inject a line into a log or a header.
func quoteBounded(value []byte) string {
	if len(value) <= maxEchoBytes {
		return strconv.Quote(string(value))
	}

	return strconv.Quote(string(value[:maxEchoBytes])) + "..."
}
