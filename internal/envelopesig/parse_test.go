//go:build unit

package envelopesig

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// b64Secret returns a distinctive n-byte secret and its standard base64 form.
func b64Secret(seed byte, n int) (Secret, string) {
	secret := testSecret(seed, n)

	return secret, base64.StdEncoding.EncodeToString(secret)
}

func TestParseKeyring_Valid(t *testing.T) {
	t.Parallel()

	s1, b1 := b64Secret('a', 32)
	s2, b2 := b64Secret('b', 48)

	ring, err := ParseKeyring(" lender-2026-08@lender:" + b1 + " , ,lender-2026-09@lender:" + b2 + "\n")
	require.NoError(t, err)
	assert.Equal(t, "Keyring{ids:[lender-2026-08 lender-2026-09]}", ring.String())

	for id, want := range map[string]Secret{"lender-2026-08": s1, "lender-2026-09": s2} {
		key, ok := ring.lookup(id)
		require.True(t, ok, id)
		assert.Equal(t, "lender", key.Source)
		assert.True(t, bytes.Equal(key.Secret, want), "%s secret was not decoded from base64", id)
	}
}

func TestParseKeyring_Refusals(t *testing.T) {
	t.Parallel()

	_, good := b64Secret('c', 32)
	_, short := b64Secret('d', 16)

	tests := []struct {
		name  string
		value string
	}{
		{"blank", "  "},
		{"only separators", " , ,"},
		{"no source separator", "lender-k1:" + good},
		{"no secret separator", "lender-k1@lender"},
		{"empty key id", "@lender:" + good},
		{"empty source", "lender-k1@:" + good},
		{"empty secret", "lender-k1@lender:"},
		{"secret is not base64", "lender-k1@lender:%%%" + good},
		{"secret under 32 bytes", "lender-k1@lender:" + short},
		{"key id outside the pattern", "Lender K1@lender:" + good},
		{"source outside the pattern", "lender-k1@Lender:" + good},
		{"duplicate key id", "lender-k1@lender:" + good + ",lender-k1@lender:" + good},
		{"secret pasted into the id slot", good + "@lender:" + good},
		{"secret pasted into the source slot", "lender-k1@" + good + ":" + good},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ring, err := ParseKeyring(tt.value)
			require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
			assert.Nil(t, ring)
			assert.NotContains(t, err.Error(), good, "error text carries secret material")
			assert.NotContains(t, err.Error(), short, "error text carries secret material")
			assert.NotContains(t, err.Error(), good[:12], "error text carries a prefix of the secret")
		})
	}
}

// TestParseKeyring_NamesEntryByPosition pins the diagnostics: a structural
// fault names the entry by position, and a key id appears only once it is
// known to be a legal id.
func TestParseKeyring_NamesEntryByPosition(t *testing.T) {
	t.Parallel()

	_, good := b64Secret('e', 32)

	_, err := ParseKeyring("lender-k1@lender:" + good + ",lender-k2@lender:%%%")
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Contains(t, err.Error(), "entry 2")
	assert.Contains(t, err.Error(), `"lender-k2"`)

	_, err = ParseKeyring("Bad Id@lender:" + good)
	require.ErrorIs(t, err, contract.ErrInvalidSigningKey)
	assert.Contains(t, err.Error(), "entry 1")
	assert.NotContains(t, err.Error(), "Bad Id")
}

func TestDecodeSecret(t *testing.T) {
	t.Parallel()

	secret, encoded := b64Secret('f', 40)

	got, err := DecodeSecret(" " + encoded + "\n")
	require.NoError(t, err)
	assert.True(t, bytes.Equal(got, secret))

	for _, bad := range []string{"", "   ", "%%%" + encoded} {
		_, err := DecodeSecret(bad)
		require.ErrorIs(t, err, contract.ErrInvalidSigningKey, "%q", bad)
		assert.False(t, strings.Contains(err.Error(), encoded), "error text carries the encoded secret")
	}
}
