//go:build unit

package envelopesig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

func TestNewKeyring_RejectsEmpty(t *testing.T) {
	t.Parallel()

	_, err := NewKeyring()
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
}

func TestNewKeyring_RejectsShortSecret(t *testing.T) {
	t.Parallel()

	_, err := NewKeyring(Key{ID: "k1", Source: "ledger", Secret: testSecret(1, MinSecretBytes-1)})
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Contains(t, err.Error(), `"k1"`)
}

func TestNewKeyring_AcceptsMinimumSecret(t *testing.T) {
	t.Parallel()

	_, err := NewKeyring(Key{ID: "k1", Source: "ledger", Secret: testSecret(1, MinSecretBytes)})
	require.NoError(t, err)
}

func TestNewKeyring_RejectsDuplicateID(t *testing.T) {
	t.Parallel()

	_, err := NewKeyring(
		Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)},
		Key{ID: "k1", Source: "lender", Secret: testSecret(2, 32)},
	)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
}

func TestNewKeyring_RejectsInvalidID(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		"", "K1", "-k1", ".k1", "k 1", "k1\n", "k/1", strings.Repeat("a", 65),
	} {
		_, err := NewKeyring(Key{ID: id, Source: "ledger", Secret: testSecret(1, 32)})
		require.ErrorIs(t, err, contract.ErrInvalidSigningKey, "id %q", id)
	}

	for _, id := range []string{"k1", "2026-09.a_b", strings.Repeat("a", 64)} {
		_, err := NewKeyring(Key{ID: id, Source: "ledger", Secret: testSecret(1, 32)})
		require.NoError(t, err, "id %q", id)
	}
}

func TestNewKeyring_RejectsInvalidSource(t *testing.T) {
	t.Parallel()

	for _, source := range []string{"", "Ledger", "led.ger", "//lerian.midaz/ledger"} {
		_, err := NewKeyring(Key{ID: "k1", Source: source, Secret: testSecret(1, 32)})
		require.ErrorIs(t, err, contract.ErrInvalidSigningKey, "source %q", source)
	}
}

func TestKeyring_DeepCopiesSecret(t *testing.T) {
	t.Parallel()

	secret := testSecret(1, 32)
	ring := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: secret})
	pristine := mustKeyring(t, Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)})

	// Mutate the caller's slice after construction: the ring must not see it.
	for i := range secret {
		secret[i] = 0xFF
	}

	signed := mustSigner(t, ring, "k1", "ledger").Sign(
		headersFor(fullEvent()), []byte(`{}`))

	require.NoError(t, mustVerifier(t, pristine, 0).Verify(toRecordHeaders(signed), []byte(`{}`)))
}

func TestKeyring_NilIsSafe(t *testing.T) {
	t.Parallel()

	var ring *Keyring

	_, ok := ring.lookup("k1")
	assert.False(t, ok)
	assert.Empty(t, ring.Sources())
	assert.Equal(t, "Keyring{ids:[]}", ring.String())
}

func TestKeyring_Sources(t *testing.T) {
	t.Parallel()

	ring := mustKeyring(t,
		Key{ID: "a1", Source: "ledger", Secret: testSecret(1, 32)},
		Key{ID: "a2", Source: "ledger", Secret: testSecret(2, 32)},
		Key{ID: "b1", Source: "lender", Secret: testSecret(3, 32)},
	)

	got := ring.Sources()
	assert.Equal(t, map[string]struct{}{"ledger": {}, "lender": {}}, got)

	// A returned map is a copy: mutating it does not reach the ring.
	delete(got, "ledger")
	assert.Contains(t, ring.Sources(), "ledger")
	assert.Equal(t, "Keyring{ids:[a1 a2 b1]}", ring.String())
}

// TestSecret_NeverRenders proves no formatting, encoding or logging path turns
// secret bytes into text.
func TestSecret_NeverRenders(t *testing.T) {
	t.Parallel()

	// Printable bytes, so a leak would be visible as text, not only as hex.
	secret := Secret("S3CR3T-S3CR3T-S3CR3T-S3CR3T-S3CR3T")
	key := Key{ID: "k1", Source: "ledger", Secret: secret}
	ring := mustKeyring(t, key)

	leaks := func(s string) bool {
		return strings.Contains(s, "S3CR3T") ||
			strings.Contains(s, fmt.Sprintf("%x", []byte(secret))) ||
			strings.Contains(s, fmt.Sprint([]byte(secret)))
	}

	subjects := map[string]any{
		"secret":        secret,
		"key":           key,
		"key pointer":   &key,
		"key slice":     []Key{key},
		"ring":          ring,
		"ring value":    *ring,
		"secret in map": map[string]Secret{"s": secret},
	}

	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			out := fmt.Sprintf(verb, subject)
			assert.False(t, leaks(out), "%s rendered with %s leaked: %s", name, verb, out)
		}

		encoded, err := json.Marshal(subject)
		if err == nil {
			assert.False(t, leaks(string(encoded)), "%s json leaked: %s", name, encoded)
		}

		var buf bytes.Buffer

		logger := slog.New(slog.NewJSONHandler(&buf, nil))
		logger.Info("probe", "subject", subject)
		slog.New(slog.NewTextHandler(&buf, nil)).Info("probe", "subject", subject)
		assert.False(t, leaks(buf.String()), "%s slog leaked: %s", name, buf.String())
	}

	assert.Equal(t, constants.ObfuscatedValue, secret.String())
	assert.Equal(t, constants.ObfuscatedValue, secret.GoString())

	text, err := secret.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, constants.ObfuscatedValue, string(text))
}

// TestNewKeyring_ErrorsNeverCarrySecret keeps the refusal texts free of key
// material: they reach logs and boot failures.
func TestNewKeyring_ErrorsNeverCarrySecret(t *testing.T) {
	t.Parallel()

	secret := Secret("S3CR3T-SHORT")
	_, err := NewKeyring(Key{ID: "k1", Source: "ledger", Secret: secret})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3CR3T")
	assert.True(t, errors.Is(err, contract.ErrInvalidSigningKey))
}

// TestNewKeyring_InvalidIDNeverEchoed pins that an id refused by the pattern
// never reaches the error text: the usual way an id is invalid is an operator
// pasting the base64 secret into the id slot, and that error is logged at boot.
func TestNewKeyring_InvalidIDNeverEchoed(t *testing.T) {
	t.Parallel()

	for _, id := range []string{
		"c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3JlNQ==",
		"Zm9vYmFy/K+x",
		"k1\n",
	} {
		_, err := NewKeyring(Key{ID: id, Source: "ledger", Secret: testSecret(1, 32)})
		require.ErrorIs(t, err, contract.ErrInvalidSigningKey, "id %q", id)
		assert.NotContains(t, err.Error(), strings.TrimSpace(id), "id %q echoed", id)
		assert.NotContains(t, err.Error(), id[:min(6, len(id))], "id %q prefix echoed", id)
		assert.Contains(t, err.Error(), "key 1:", "error should name the key position")
	}

	// A legal id stays in the text: it is what the operator greps for.
	_, err := NewKeyring(
		Key{ID: "k1", Source: "ledger", Secret: testSecret(1, 32)},
		Key{ID: "k1", Source: "ledger", Secret: testSecret(2, 32)},
	)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Contains(t, err.Error(), `"k1"`)
}

// TestNewKeyring_IllegalSourceNeverEchoed pins that a refused source is not
// quoted: with the arguments swapped, the source slot holds the secret.
func TestNewKeyring_IllegalSourceNeverEchoed(t *testing.T) {
	t.Parallel()

	const pasted = "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXphYmNkZWY="

	_, err := NewKeyring(Key{ID: "k1", Source: pasted, Secret: testSecret(1, MinSecretBytes)})
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	require.ErrorIs(t, err, contract.ErrInvalidSource)
	assert.NotContains(t, err.Error(), pasted[:12])
	assert.Contains(t, err.Error(), `"k1"`)

	_, err = NewKeyring(Key{ID: "k1", Source: "", Secret: testSecret(1, MinSecretBytes)})
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	require.ErrorIs(t, err, contract.ErrMissingSource)
}
