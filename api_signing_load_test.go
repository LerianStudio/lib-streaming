//go:build unit

package streaming_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

// These tests mutate process env, so none of them runs in parallel.

// clearSigningEnv blanks every variable the standalone loaders and the two
// config loaders read, so a test starts from "nothing configured".
func clearSigningEnv(t *testing.T) {
	t.Helper()

	for _, name := range []string{
		"STREAMING_ENABLED",
		"STREAMING_SIGNING_KEY_ID",
		"STREAMING_SIGNING_KEY",
		"STREAMING_CONSUMER_ENABLED",
		"STREAMING_CONSUMER_BROKERS",
		"STREAMING_CONSUMER_GROUP",
		"STREAMING_CONSUMER_APPS",
		"STREAMING_CONSUMER_TOPICS",
		"STREAMING_CONSUMER_COMMANDS",
		"STREAMING_CONSUMER_EXPECT_SOURCES",
		"STREAMING_CONSUMER_REQUIRE_SIGNATURES",
		"STREAMING_CONSUMER_SIGNATURE_KEYS",
		"STREAMING_CLOUDEVENTS_SOURCE",
	} {
		t.Setenv(name, "")
	}
}

func builderSigningSecretB64() string {
	return base64.StdEncoding.EncodeToString(builderSigningSecret())
}

// assertRendersNoSecret fails when any fmt verb or JSON rendering of v carries
// the builder signing secret, raw or base64.
func assertRendersNoSecret(t *testing.T, v any) {
	t.Helper()

	raw, encoded := string(builderSigningSecret()), builderSigningSecretB64()

	renderings := []string{fmt.Sprintf("%v", v), fmt.Sprintf("%+v", v), fmt.Sprintf("%#v", v), fmt.Sprintf("%s", v), fmt.Sprintf("%x", v)}

	if b, err := json.Marshal(v); err == nil {
		renderings = append(renderings, string(b))
	}

	for _, r := range renderings {
		assert.NotContains(t, r, raw, "rendering exposes the raw secret")
		assert.NotContains(t, r, encoded, "rendering exposes the base64 secret")
		assert.NotContains(t, r, encoded[:12], "rendering exposes a prefix of the base64 secret")
	}
}

// signsAndVerifies emits one event through a producer signing with
// (signRing, activeKeyID) and reports how verifyRing judges the record.
func signsAndVerifies(t *testing.T, signRing *streaming.Keyring, activeKeyID string, verifyRing *streaming.Keyring) error {
	t.Helper()

	adapter := fake.NewAdapter(streaming.TransportCustom)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).
		SignEnvelopes(signRing, activeKeyID).
		Build(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	return verifyFakeMessage(t, verifyRing, adapter)
}

func TestLoadVerificationKeys_ReadsWithoutConsumerEnabled(t *testing.T) {
	clearSigningEnv(t)
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", " k1@"+builderSigningSource+":"+builderSigningSecretB64()+"\n")

	ring, err := streaming.LoadVerificationKeys()
	require.NoError(t, err)
	require.NotNil(t, ring)
	assert.Equal(t, "Keyring{ids:[k1]}", ring.String())

	// The loaded key verifies a record its producer signs.
	require.NoError(t, signsAndVerifies(t, builderSigningRing(t, "k1", builderSigningSource), "k1", ring))

	assertRendersNoSecret(t, ring)
	assertRendersNoSecret(t, *ring)
}

// TestLoadVerificationKeys_NoEntryIsNil pins that a variable holding no entry
// — unset, blank, or only separators, as a template of empty key slots
// renders — means "no keys configured", like every other csv variable the
// config loaders read. A consumer that requires signatures still fails closed
// at Build; one that does not keeps booting.
func TestLoadVerificationKeys_NoEntryIsNil(t *testing.T) {
	for name, value := range map[string]string{
		"unset":           "",
		"blank":           " \n",
		"only separators": " , ,",
	} {
		t.Run(name, func(t *testing.T) {
			clearSigningEnv(t)
			t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", value)

			ring, err := streaming.LoadVerificationKeys()
			require.NoError(t, err)
			assert.Nil(t, ring)

			t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
			t.Setenv("STREAMING_CONSUMER_BROKERS", "localhost:9092")
			t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
			t.Setenv("STREAMING_CONSUMER_APPS", builderSigningSource)
			t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "loan-projector")

			cfg, _, err := streaming.LoadConsumerConfig()
			require.NoError(t, err)
			assert.Nil(t, cfg.SignatureKeys)
		})
	}
}

func TestLoadVerificationKeys_MalformedWrapsBothSentinels(t *testing.T) {
	encoded := builderSigningSecretB64()

	for name, value := range map[string]string{
		"secret in the id slot":  encoded + "@" + builderSigningSource + ":" + encoded,
		"secret not base64":      "k1@" + builderSigningSource + ":%%%" + encoded,
		"secret under 32 bytes":  "k1@" + builderSigningSource + ":" + base64.StdEncoding.EncodeToString(builderSigningSecret()[:16]),
		"missing secret section": "k1@" + builderSigningSource,
	} {
		t.Run(name, func(t *testing.T) {
			clearSigningEnv(t)
			t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", value)

			ring, err := streaming.LoadVerificationKeys()
			assert.Nil(t, ring)
			require.ErrorIs(t, err, streaming.ErrConsumerInvalidConfigField)
			require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
			assert.Contains(t, err.Error(), "STREAMING_CONSUMER_SIGNATURE_KEYS")
			assertRendersNoSecret(t, err)
		})
	}
}

// TestLoadVerificationKeys_ParityWithLoadConsumerConfig pins that the
// standalone loader and LoadConsumerConfig build the same ring from the same
// value: both verify the same signed record, and both refuse the same bad
// value with the same sentinels.
func TestLoadVerificationKeys_ParityWithLoadConsumerConfig(t *testing.T) {
	clearSigningEnv(t)
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_APPS", builderSigningSource)
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "loan-projector")
	t.Setenv("STREAMING_CONSUMER_REQUIRE_SIGNATURES", "true")
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", "k1@"+builderSigningSource+":"+builderSigningSecretB64()+",k0@lender:"+builderSigningSecretB64())

	cfg, _, err := streaming.LoadConsumerConfig()
	require.NoError(t, err)

	standalone, err := streaming.LoadVerificationKeys()
	require.NoError(t, err)

	assert.Equal(t, cfg.SignatureKeys.String(), standalone.String())
	assert.Equal(t, cfg.SignatureKeys.Sources(), standalone.Sources())

	signRing := builderSigningRing(t, "k1", builderSigningSource)
	require.NoError(t, signsAndVerifies(t, signRing, "k1", cfg.SignatureKeys))
	require.NoError(t, signsAndVerifies(t, signRing, "k1", standalone))

	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", "k1@"+builderSigningSource+":%%%")

	_, _, cfgErr := streaming.LoadConsumerConfig()
	_, loadErr := streaming.LoadVerificationKeys()

	for _, err := range []error{cfgErr, loadErr} {
		require.ErrorIs(t, err, streaming.ErrConsumerInvalidConfigField)
		require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
	}

	assert.Equal(t, cfgErr.Error(), loadErr.Error())
}

func TestParseVerificationKeys(t *testing.T) {
	t.Parallel()

	ring, err := streaming.ParseVerificationKeys("k1@" + builderSigningSource + ":" + builderSigningSecretB64())
	require.NoError(t, err)
	assert.Equal(t, "Keyring{ids:[k1]}", ring.String())
	assert.Equal(t, map[string]struct{}{builderSigningSource: {}}, ring.Sources())

	for _, bad := range []string{"", "  ", "k1@" + builderSigningSource} {
		ring, err := streaming.ParseVerificationKeys(bad)
		assert.Nil(t, ring)
		require.ErrorIs(t, err, streaming.ErrInvalidSigningKey, "%q", bad)
		assert.True(t, streaming.IsCallerError(err))
		assert.False(t, errors.Is(err, streaming.ErrConsumerInvalidConfigField), "a string parse names no env variable")
	}
}

func TestParseSigningKey(t *testing.T) {
	t.Parallel()

	ring, err := streaming.ParseSigningKey("k1", builderSigningSource, builderSigningSecretB64()+"\n")
	require.NoError(t, err)
	assert.Equal(t, "Keyring{ids:[k1]}", ring.String())
	assert.Equal(t, map[string]struct{}{builderSigningSource: {}}, ring.Sources())
	require.NoError(t, signsAndVerifies(t, ring, "k1", builderSigningRing(t, "k1", builderSigningSource)))

	encoded := builderSigningSecretB64()

	for name, args := range map[string][3]string{
		"empty secret":       {"k1", builderSigningSource, ""},
		"secret not base64":  {"k1", builderSigningSource, "%%%" + encoded},
		"short secret":       {"k1", builderSigningSource, base64.StdEncoding.EncodeToString(builderSigningSecret()[:16])},
		"secret in id":       {encoded, builderSigningSource, encoded},
		"illegal source":     {"k1", "Not A Source", encoded},
		"empty key id":       {"", builderSigningSource, encoded},
		"secret in source":   {"k1", encoded, encoded},
		"whitespace secret":  {"k1", builderSigningSource, "  "},
		"secret with spaces": {"k1", builderSigningSource, "a b c d"},
	} {
		ring, err := streaming.ParseSigningKey(args[0], args[1], args[2])
		assert.Nil(t, ring, name)
		require.ErrorIs(t, err, streaming.ErrInvalidSigningKey, name)
		assertRendersNoSecret(t, err)
	}
}

func TestLoadSigningKey_ReadsWithoutStreamingEnabled(t *testing.T) {
	clearSigningEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", " k1 ")
	t.Setenv("STREAMING_SIGNING_KEY", builderSigningSecretB64()+"\n")

	ring, activeKeyID, err := streaming.LoadSigningKey(builderSigningSource)
	require.NoError(t, err)
	assert.Equal(t, "k1", activeKeyID)
	assert.Equal(t, map[string]struct{}{builderSigningSource: {}}, ring.Sources(), "the key is bound to the source passed in")

	// The ring feeds Builder.SignEnvelopes directly, and what it signs
	// verifies under the same key.
	require.NoError(t, signsAndVerifies(t, ring, activeKeyID, builderSigningRing(t, "k1", builderSigningSource)))

	assertRendersNoSecret(t, ring)
}

func TestLoadSigningKey_UnsetIsOff(t *testing.T) {
	clearSigningEnv(t)

	ring, activeKeyID, err := streaming.LoadSigningKey(builderSigningSource)
	require.NoError(t, err)
	assert.Nil(t, ring)
	assert.Empty(t, activeKeyID)
}

func TestLoadSigningKey_OneOfTwoIsConfigError(t *testing.T) {
	for name, env := range map[string][2]string{
		"id without key": {"k1", ""},
		"key without id": {"", builderSigningSecretB64()},
	} {
		t.Run(name, func(t *testing.T) {
			clearSigningEnv(t)
			t.Setenv("STREAMING_SIGNING_KEY_ID", env[0])
			t.Setenv("STREAMING_SIGNING_KEY", env[1])

			ring, activeKeyID, err := streaming.LoadSigningKey(builderSigningSource)
			assert.Nil(t, ring)
			assert.Empty(t, activeKeyID)
			require.ErrorIs(t, err, streaming.ErrProducerInvalidConfigField)
			assertRendersNoSecret(t, err)
		})
	}
}

func TestLoadSigningKey_UnusableKeyIsInvalidSigningKey(t *testing.T) {
	encoded := builderSigningSecretB64()

	for name, env := range map[string][3]string{
		"secret not base64": {"k1", "%%%" + encoded, builderSigningSource},
		"short secret":      {"k1", base64.StdEncoding.EncodeToString(builderSigningSecret()[:16]), builderSigningSource},
		"secret in id slot": {encoded, encoded, builderSigningSource},
		"illegal source":    {"k1", encoded, "Not A Source"},
	} {
		t.Run(name, func(t *testing.T) {
			clearSigningEnv(t)
			t.Setenv("STREAMING_SIGNING_KEY_ID", env[0])
			t.Setenv("STREAMING_SIGNING_KEY", env[1])

			ring, activeKeyID, err := streaming.LoadSigningKey(env[2])
			assert.Nil(t, ring)
			assert.Empty(t, activeKeyID)
			require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
			assert.False(t, strings.Contains(err.Error(), encoded[:12]), "error echoes a prefix of the secret: %v", err)
			assertRendersNoSecret(t, err)
		})
	}
}

// TestLoadSigningKey_ParityWithLoadConfig pins that the standalone loader and
// LoadConfig + SigningFromConfig sign with the same key.
func TestLoadSigningKey_ParityWithLoadConfig(t *testing.T) {
	clearSigningEnv(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", builderSigningSource)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")
	t.Setenv("STREAMING_SIGNING_KEY", builderSigningSecretB64())

	cfg, _, err := streaming.LoadConfig()
	require.NoError(t, err)

	ring, activeKeyID, err := streaming.LoadSigningKey(cfg.CloudEventsSource)
	require.NoError(t, err)
	assert.Equal(t, cfg.SigningKeyID, activeKeyID)

	fromConfig, err := streaming.NewKeyring(streaming.SigningKey{ID: cfg.SigningKeyID, Source: cfg.CloudEventsSource, Secret: cfg.SigningKey})
	require.NoError(t, err)
	require.NoError(t, signsAndVerifies(t, ring, activeKeyID, fromConfig))
}
