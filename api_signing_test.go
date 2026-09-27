//go:build unit

package streaming_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
)

// TestSignatureHeaderConstants_MatchTheCodec pins the literal root constants
// to the one definition the signer and verifier use.
func TestSignatureHeaderConstants_MatchTheCodec(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ce-sigkid", streaming.CloudEventsHeaderSignatureKeyID)
	assert.Equal(t, "ce-sigts", streaming.CloudEventsHeaderSignedAt)
	assert.Equal(t, "ce-sig", streaming.CloudEventsHeaderSignature)
	assert.Equal(t, envelopesig.HeaderKeyID, streaming.CloudEventsHeaderSignatureKeyID)
	assert.Equal(t, envelopesig.HeaderSignedAt, streaming.CloudEventsHeaderSignedAt)
	assert.Equal(t, envelopesig.HeaderSignature, streaming.CloudEventsHeaderSignature)
	assert.Equal(t, envelopesig.MinSecretBytes, streaming.MinSigningSecretBytes)
}

func TestNewKeyring_RootFacade(t *testing.T) {
	t.Parallel()

	secret := make(streaming.SigningSecret, streaming.MinSigningSecretBytes)
	ring, err := streaming.NewKeyring(streaming.SigningKey{ID: "k1", Source: "ledger", Secret: secret})
	require.NoError(t, err)
	assert.Equal(t, "Keyring{ids:[k1]}", ring.String())

	_, err = streaming.NewKeyring(streaming.SigningKey{ID: "k1", Source: "ledger", Secret: secret[:31]})
	require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
	assert.True(t, streaming.IsCallerError(err))
}

func TestIsCallerError_InvalidSigningKey(t *testing.T) {
	t.Parallel()

	assert.True(t, streaming.IsCallerError(streaming.ErrInvalidSigningKey))
	assert.True(t, streaming.IsCallerError(fmt.Errorf("wrap: %w", streaming.ErrInvalidSigningKey)))

	// Verification outcomes are facts about a record on the wire, not caller
	// mistakes: a service must not treat a forged record as its own bug.
	for _, err := range []error{
		streaming.ErrSignatureMissing,
		streaming.ErrSignatureUnknownKey,
		streaming.ErrSignatureInvalid,
	} {
		assert.False(t, streaming.IsCallerError(err), "%v", err)
	}

	// A relayed row of another source is a configuration fault an operator
	// fixes, not a property of the row: as a caller error the documented
	// IsCallerError retry classifier would move it to INVALID on attempt one.
	assert.False(t, streaming.IsCallerError(streaming.ErrSigningSourceMismatch))
	assert.False(t, streaming.IsCallerError(fmt.Errorf("wrap: %w", streaming.ErrSigningSourceMismatch)))
}
