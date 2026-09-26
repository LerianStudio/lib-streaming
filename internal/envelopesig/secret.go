package envelopesig

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/LerianStudio/lib-observability/v4/constants"
)

// Secret is HMAC key material. Every rendering path — fmt verbs, JSON, text
// encoding, slog — yields the lib-observability obfuscation mask instead of
// the bytes, so a Secret logged by accident, dumped in a config struct or
// echoed in a panic value leaks nothing.
//
// The mask covers renderings that REACH the Secret's methods. fmt cannot call
// a method on a value stored in an unexported struct field, so this package
// never stores a Secret in one directly (see Keyring), and callers must not
// either. Converting to []byte is an explicit act and prints raw bytes.
type Secret []byte

// String returns the obfuscation mask.
func (Secret) String() string { return constants.ObfuscatedValue }

// GoString returns the obfuscation mask for %#v.
func (Secret) GoString() string { return constants.ObfuscatedValue }

// Format renders the obfuscation mask for every verb, including %x and %d,
// which would otherwise print the bytes of a []byte-kinded value.
func (Secret) Format(f fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = f.Write([]byte(strconv.Quote(constants.ObfuscatedValue)))

		return
	}

	_, _ = f.Write([]byte(constants.ObfuscatedValue))
}

// MarshalJSON renders the obfuscation mask as a JSON string.
func (Secret) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(constants.ObfuscatedValue)), nil
}

// MarshalText renders the obfuscation mask.
func (Secret) MarshalText() ([]byte, error) {
	return []byte(constants.ObfuscatedValue), nil
}

// LogValue renders the obfuscation mask under slog.
func (Secret) LogValue() slog.Value { return slog.StringValue(constants.ObfuscatedValue) }
