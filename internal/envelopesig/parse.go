package envelopesig

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// ParseKeyring parses the text form of a verification keyring: a csv of
// <kid>@<source>:<secret>, the secret standard base64. None of '@', ':' or ','
// can appear in a key id, a source or base64, so the split is unambiguous.
// Whitespace around entries and empty entries are dropped; a value with no
// entry at all is an empty ring and is refused like one.
//
// Every failure wraps contract.ErrInvalidSigningKey. An entry is named by its
// position, and by its key id only once the id is known to be legal: in a
// misordered entry any slot may hold the secret. Neither the raw value nor the
// base64 decoder's message (which quotes input offsets) reaches the error.
func ParseKeyring(csv string) (*Keyring, error) {
	entries := make([]string, 0)

	for entry := range strings.SplitSeq(csv, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			entries = append(entries, entry)
		}
	}

	keys := make([]Key, 0, len(entries))

	for i, entry := range entries {
		key, err := parseKey(entry)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d: %s", contract.ErrInvalidSigningKey, i+1, err.Error())
		}

		keys = append(keys, key)
	}

	return NewKeyring(keys...)
}

// DecodeSecret decodes a secret from standard base64, ignoring surrounding
// whitespace (secret stores often append a newline). An empty value or one
// that does not decode fails with contract.ErrInvalidSigningKey; the error
// never carries the value or the decoder's message. The length floor is the
// keyring's to enforce, so a decoded secret is not yet a usable one.
func DecodeSecret(encoded string) (Secret, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, fmt.Errorf("%w: secret is empty", contract.ErrInvalidSigningKey)
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: secret is not valid standard base64", contract.ErrInvalidSigningKey)
	}

	return Secret(decoded), nil
}

// parseKey splits one <kid>@<source>:<secret> entry. The id and the source are
// checked BEFORE anything echoes them, so an error names the structural fault
// only until both are known to be a legal id and a legal source.
func parseKey(entry string) (Key, error) {
	id, rest, hasSource := strings.Cut(entry, "@")
	source, encoded, hasSecret := strings.Cut(rest, ":")

	switch {
	case !hasSource || !hasSecret:
		return Key{}, errors.New("want <kid>@<source>:<base64 secret>")
	case id == "" || source == "" || encoded == "":
		return Key{}, errors.New("key id, source and secret must all be non-empty")
	case !ValidKeyID(id):
		return Key{}, errors.New("key id must be 1-64 characters of [a-z0-9._-], starting with [a-z0-9]")
	case contract.ValidateSource(source) != nil:
		return Key{}, fmt.Errorf("key %q: source is not a legal ce-source", id)
	}

	secret, err := DecodeSecret(encoded)
	if err != nil {
		return Key{}, fmt.Errorf("key %q: secret is not valid standard base64", id)
	}

	return Key{ID: id, Source: source, Secret: secret}, nil
}
