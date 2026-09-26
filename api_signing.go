package streaming

import "github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"

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
