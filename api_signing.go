package streaming

import (
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
// verifier outside the Kafka consumer. Parsing is ParseVerificationKeys'. An
// unset or blank variable returns a nil ring and no error; RequireSignatures(nil)
// then fails Build with ErrConsumerSignatureKeysMissing, so a chain that needs
// keys fails closed. A malformed value fails with an error wrapping both
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
