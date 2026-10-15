package streaming

import (
	"time"

	"github.com/LerianStudio/lib-streaming/v4/internal/config"
	"github.com/LerianStudio/lib-streaming/v4/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
)

// Envelope signing: an opt-in HMAC-SHA256 signature over every CloudEvents
// header the library writes, the signing instant, the key id and the SHA-256
// of the record body, carried as three CloudEvents extension headers.
//
// A Keyring holds the keys. Each key names the ce-source it speaks for: a
// producer signs only with a key bound to its own source, and a verifier
// accepts a signature only when the record's ce-source equals the source of
// the key that signed it, so a key in a consumer's ring can never vouch for a
// different producer. Secrets come from the service's secret store; a
// SigningSecret renders as a mask under every fmt verb, JSON, text encoding
// and slog, and no error text carries key bytes.
type (
	// SigningKey is one signing key: ID travels in ce-sigkid, Source is the
	// ce-source the key speaks for, Secret is the HMAC key (at least
	// MinSigningSecretBytes).
	SigningKey = envelopesig.Key
	// SigningSecret is HMAC key material that never renders as text.
	SigningSecret = envelopesig.Secret
	// Keyring is an immutable set of signing keys, safe for concurrent use.
	// Rotation is by overlap: verifiers hold the old and the new key id while
	// producers switch from one to the other.
	Keyring = envelopesig.Keyring
)

// MinSigningSecretBytes is the smallest accepted signing secret. Shorter
// secrets are refused, never padded.
const MinSigningSecretBytes = 32

// The three signature headers, CloudEvents binary-mode extension attributes.
// Declared as literals so the strings render on the documentation page; a test
// pins them to the signer's own definition.
const (
	// CloudEventsHeaderSignatureKeyID carries the id of the signing key.
	CloudEventsHeaderSignatureKeyID = "ce-sigkid"
	// CloudEventsHeaderSignedAt carries the signing instant (RFC 3339, UTC) —
	// the instant of the publish, which on an outbox relay is the relay time,
	// not the enqueue time ce-time records.
	CloudEventsHeaderSignedAt = "ce-sigts"
	// CloudEventsHeaderSignature carries "v1." followed by the unpadded
	// base64url HMAC-SHA256.
	CloudEventsHeaderSignature = "ce-sig"
)

// NewKeyring validates keys and returns an immutable ring holding copies of
// them. It fails with ErrInvalidSigningKey for an empty ring, a key id outside
// ^[a-z0-9][a-z0-9._-]{0,63}$, a duplicate id, a Source that is not a legal
// ce-source, or a secret shorter than MinSigningSecretBytes.
func NewKeyring(keys ...SigningKey) (*Keyring, error) {
	return envelopesig.NewKeyring(keys...)
}

// ParseVerificationKeys parses the text form of a verification keyring — the
// csv of <kid>@<source>:<base64 secret> that STREAMING_CONSUMER_SIGNATURE_KEYS
// carries — with the same rules LoadConsumerConfig applies: key id pattern,
// legal ce-source, standard base64, the MinSigningSecretBytes floor, no
// duplicate id. Use it when the value comes from somewhere other than that
// variable (a mounted secret file, a secret-manager call). A value with no
// entry fails with ErrInvalidSigningKey, like an empty ring. Every failure
// wraps ErrInvalidSigningKey and names the entry by position, and by key id
// only once the id is legal; no error carries secret bytes.
func ParseVerificationKeys(csv string) (*Keyring, error) {
	return envelopesig.ParseKeyring(csv)
}

// LoadVerificationKeys reads STREAMING_CONSUMER_SIGNATURE_KEYS into a
// verification keyring whether or not STREAMING_CONSUMER_ENABLED is set, for a
// consumer built fluently (without LoadConsumerConfig and FromConfig) or for a
// verifier outside the Kafka consumer. Parsing is ParseVerificationKeys'. A
// variable with no entry (unset, blank, or only separators) returns a nil ring
// and no error; RequireSignatures(nil) then fails Build with
// ErrConsumerSignatureKeysMissing, so a chain that needs keys fails closed. A malformed value fails with an error wrapping both
// ErrConsumerInvalidConfigField and ErrInvalidSigningKey, identical to the one
// LoadConsumerConfig returns for the same value.
func LoadVerificationKeys() (*Keyring, error) {
	return consumer.LoadSignatureKeys()
}

// ParseSigningKey builds the one-key ring a producer signs with: the secret
// (standard base64, surrounding whitespace ignored) bound to source under
// keyID. Use it when the secret comes from somewhere other than the
// STREAMING_SIGNING_KEY variable. Every failure — an empty or undecodable
// secret, a secret under MinSigningSecretBytes, an illegal key id or source —
// wraps ErrInvalidSigningKey; no error carries secret bytes, and an illegal id
// is never echoed. Pass the ring and keyID to Builder.SignEnvelopes.
func ParseSigningKey(keyID, source, secretBase64 string) (*Keyring, error) {
	secret, err := envelopesig.DecodeSecret(secretBase64)
	if err != nil {
		return nil, err
	}

	return envelopesig.NewKeyring(envelopesig.Key{ID: keyID, Source: source, Secret: secret})
}

// LoadSigningKey reads STREAMING_SIGNING_KEY_ID and STREAMING_SIGNING_KEY
// whether or not STREAMING_ENABLED is set and returns the one-key ring binding
// the secret to source, plus the active key id, ready for
// Builder.SignEnvelopes(ring, activeKeyID). source must be the builder's
// Source, or Build refuses the key.
//
// Neither variable set returns a nil ring, an empty id and no error: signing is
// off, so guard the SignEnvelopes call on ring != nil (SignEnvelopes(nil, "")
// fails Build with ErrInvalidSigningKey). Only one of the two set fails with
// ErrProducerInvalidConfigField. Bad base64, a secret under
// MinSigningSecretBytes, an illegal key id or an illegal source fail with
// ErrInvalidSigningKey. Error texts name the variable, never its value.
func LoadSigningKey(source string) (ring *Keyring, activeKeyID string, err error) {
	return config.LoadSigningKeyring(source)
}

// Signer signs envelopes a service publishes over a transport this library
// does not carry for it — an AMQP client of its own, for instance — with the
// same canonical encoding, headers and key binding as the producer, so any
// Verifier (or a consumer requiring signatures) accepts what it signs. A
// producer built with Builder.SignEnvelopes needs no Signer: every route it
// publishes, RabbitMQTarget included, is already signed. Immutable and safe
// for concurrent use; its rendering never carries key bytes.
type Signer struct {
	signer *envelopesig.Signer
}

// NewSigner returns a Signer for the key activeKeyID in ring, which must be
// bound to source, the ce-source of every record it will sign. It fails with
// ErrInvalidSigningKey on the rules Builder.SignEnvelopes applies: a nil ring,
// an empty, illegal or unknown key id, or a key bound to another source.
func NewSigner(ring *Keyring, activeKeyID, source string) (*Signer, error) {
	signer, err := envelopesig.NewSigner(ring, activeKeyID, source)
	if err != nil {
		return nil, err
	}

	return &Signer{signer: signer}, nil
}

// Sign returns a NEW header table: every entry of headers except a previous
// signature, plus ce-sigkid, ce-sigts (now) and ce-sig as []byte, computed over
// the ce-* entries and body. headers is never modified, and an amqp091
// amqp.Table can be passed and assigned back as is. Build the ce-* entries
// with BuildCloudEventsHeaders; a ce-* entry may hold []byte or string (the
// same bytes either way), any other type fails with ErrUnsupportedHeaderValue.
// Entries outside the signature pass through untouched, whatever they hold.
//
// Sign refuses a table whose ce-source is absent or is not the key's source
// with ErrSigningSourceMismatch: a Signer vouches only for its own source. A
// nil or zero Signer fails closed with ErrInvalidSigningKey.
func (s *Signer) Sign(headers map[string]any, body []byte) (map[string]any, error) {
	var signer *envelopesig.Signer
	if s != nil {
		signer = s.signer
	}

	return signer.SignMap(headers, body)
}

// Verifier checks envelopes a service receives over a transport other than
// this library's Kafka consumer — an AMQP delivery, for instance — before its
// own handler runs, with the same rules ConsumerBuilder.RequireSignatures
// applies. Immutable and safe for concurrent use; its rendering never carries
// key bytes.
type Verifier struct {
	verifier *envelopesig.Verifier
}

// NewVerifier returns a Verifier accepting any key in ring. maxSkew 0 turns the
// age check off, the safe default for a receiver that can lag; a positive
// maxSkew refuses a record signed further than that from now, in either
// direction. A nil or empty ring and a negative maxSkew fail with
// ErrInvalidSigningKey.
func NewVerifier(ring *Keyring, maxSkew time.Duration) (*Verifier, error) {
	verifier, err := envelopesig.NewVerifier(ring, maxSkew)
	if err != nil {
		return nil, err
	}

	return &Verifier{verifier: verifier}, nil
}

// Verify checks the signature on a received header table and body — for an
// AMQP delivery, Verify(d.Headers, d.Body). It returns nil or an error wrapping
// exactly one of ErrSignatureMissing (a signature header absent),
// ErrSignatureUnknownKey (a key id the ring does not hold) or
// ErrSignatureInvalid (body or a signed header changed, a key bound to another
// source, a malformed value, a signed entry holding neither []byte nor string,
// or older than maxSkew). Entries outside the signature are ignored. A nil or
// zero Verifier fails closed with ErrSignatureInvalid. The error text never
// carries the expected MAC or key bytes.
func (v *Verifier) Verify(headers map[string]any, body []byte) error {
	var verifier *envelopesig.Verifier
	if v != nil {
		verifier = v.verifier
	}

	return verifier.VerifyMap(headers, body)
}
