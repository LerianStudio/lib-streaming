package envelopesig

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// MinSecretBytes is the smallest accepted HMAC-SHA256 secret: the hash output
// size. A shorter key is refused, never padded.
const MinSecretBytes = 32

// keyIDPattern bounds a key id to a short token that is safe in a header, a
// log line and a DLQ error message.
var keyIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// ValidKeyID reports whether id is a legal key id. A caller parsing keys from
// untrusted text uses it to decide whether the id is safe to echo in an error:
// a value in the wrong position of a malformed entry may be secret material.
func ValidKeyID(id string) bool {
	return keyIDPattern.MatchString(id)
}

// Key is one signing key: an id carried on the wire in ce-sigkid, the
// ce-source the key speaks for, and the HMAC secret.
//
// Source is what stops one producer forging another: a verifier accepts a
// signature only when the record's ce-source equals the Source of the key that
// signed it, and a producer may sign only with a key bound to its own source.
// Without it, any key in a consumer's ring could vouch for any source.
type Key struct {
	ID     string
	Source string
	Secret Secret
}

// Keyring is an immutable set of signing keys indexed by id. A producer signs
// with exactly one of them; a verifier accepts any of them, which is how a key
// rotates: publish the new id to verifiers, switch producers to it, retire the
// old id once nothing in flight still carries it.
//
// Each key lives behind a pointer on purpose. fmt cannot call a method on a
// value inside an unexported field, so a Key stored by value would print its
// Secret bytes raw under %+v or a bad verb on a Keyring value; a pointer below
// the top level prints as an address, which is all fmt can ever reach.
type Keyring struct {
	state *keyringState
}

type keyringState struct {
	keys map[string]*Key
}

// NewKeyring validates keys and returns a ring holding deep copies of them, so
// a caller that zeroes or reuses its buffers afterwards does not reach the
// ring. It refuses an empty ring, an invalid or duplicate id, a Source that is
// not a legal ce-source, and a secret shorter than MinSecretBytes, all with
// contract.ErrInvalidSigningKey. Error texts name ids and sources only.
func NewKeyring(keys ...Key) (*Keyring, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: keyring is empty", contract.ErrInvalidSigningKey)
	}

	byID := make(map[string]*Key, len(keys))

	for _, key := range keys {
		if !keyIDPattern.MatchString(key.ID) {
			return nil, fmt.Errorf("%w: key id %s must match %s",
				contract.ErrInvalidSigningKey, quoteBounded([]byte(key.ID)), keyIDPattern.String())
		}

		if _, dup := byID[key.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate key id %q", contract.ErrInvalidSigningKey, key.ID)
		}

		if err := contract.ValidateSource(key.Source); err != nil {
			return nil, fmt.Errorf("%w: key %q: %w", contract.ErrInvalidSigningKey, key.ID, err)
		}

		if len(key.Secret) < MinSecretBytes {
			return nil, fmt.Errorf("%w: key %q: secret is %d bytes, need at least %d",
				contract.ErrInvalidSigningKey, key.ID, len(key.Secret), MinSecretBytes)
		}

		byID[key.ID] = &Key{
			ID:     key.ID,
			Source: key.Source,
			Secret: append(Secret(nil), key.Secret...),
		}
	}

	return &Keyring{state: &keyringState{keys: byID}}, nil
}

// lookup returns the key for id. Nil-safe.
func (k *Keyring) lookup(id string) (Key, bool) {
	if k == nil || k.state == nil {
		return Key{}, false
	}

	key, ok := k.state.keys[id]
	if !ok {
		return Key{}, false
	}

	return *key, true
}

// empty reports whether the ring holds no key. Nil-safe.
func (k *Keyring) empty() bool {
	return k == nil || k.state == nil || len(k.state.keys) == 0
}

// Sources returns the set of ce-source values some key in the ring speaks
// for, as a fresh map the caller may mutate. Nil-safe.
func (k *Keyring) Sources() map[string]struct{} {
	sources := make(map[string]struct{})

	if k.empty() {
		return sources
	}

	for _, key := range k.state.keys {
		sources[key.Source] = struct{}{}
	}

	return sources
}

// String renders the sorted key ids and nothing else. Nil-safe.
func (k *Keyring) String() string {
	ids := make([]string, 0)

	if !k.empty() {
		for id := range k.state.keys {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return "Keyring{ids:[" + strings.Join(ids, " ") + "]}"
}
