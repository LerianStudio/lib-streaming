//go:build unit

package consumer

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
)

// setSignatureBaseEnv sets the minimum environment for an enabled consumer.
func setSignatureBaseEnv(t *testing.T) {
	t.Helper()

	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "test-consumer")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "b1:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "svc")
	t.Setenv("STREAMING_CONSUMER_APPS", "lender")
}

// secretB64 returns a distinctive n-byte secret and its standard base64 form.
func secretB64(seed byte, n int) ([]byte, string) {
	secret := bytes.Repeat([]byte{seed}, n)

	return secret, base64.StdEncoding.EncodeToString(secret)
}

func TestLoadConsumerConfig_SignatureKeys(t *testing.T) {
	// Not parallel: mutates process env.
	setSignatureBaseEnv(t)

	s1, b1 := secretB64('a', 32)
	s2, b2 := secretB64('b', 48)

	t.Setenv("STREAMING_CONSUMER_REQUIRE_SIGNATURES", "true")
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", fmt.Sprintf(" lender-2026-08@lender:%s , lender-2026-09@lender:%s\n", b1, b2))

	cfg, warnings, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if len(warnings) != 0 {
		t.Errorf("warnings = %v; want none", warnings)
	}

	if !cfg.RequireSignatures {
		t.Error("RequireSignatures = false; want true")
	}

	if got := cfg.SignatureKeys.String(); got != "Keyring{ids:[lender-2026-08 lender-2026-09]}" {
		t.Fatalf("SignatureKeys = %s; want both env keys", got)
	}

	// Each parsed key must verify a record its producer signs with the
	// decoded secret, bound to the parsed source.
	verifier, err := envelopesig.NewVerifier(cfg.SignatureKeys, 0)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	lenderHeaders := withHeader(ceHeaders("tenantA", false), "ce-source", "lender")

	for _, w := range []envelopesig.Key{
		{ID: "lender-2026-08", Source: "lender", Secret: s1},
		{ID: "lender-2026-09", Source: "lender", Secret: s2},
	} {
		if err := verifier.Verify(signed(t, w, lenderHeaders, sigRecordBody), sigRecordBody); err != nil {
			t.Errorf("key %s parsed from env does not verify its producer's record: %v", w.ID, err)
		}
	}

	if cfg.SignatureMaxSkew != 0 {
		t.Errorf("SignatureMaxSkew = %s; want 0 (age check disabled by default)", cfg.SignatureMaxSkew)
	}

	rendered := fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg)
	for _, secret := range [][]byte{s1, s2} {
		if strings.Contains(rendered, string(secret)) {
			t.Error("a fmt rendering of ConsumerConfig exposes a signing secret")
		}
	}

	if strings.Contains(rendered, b1) || strings.Contains(rendered, b2) {
		t.Error("a fmt rendering of ConsumerConfig exposes a base64 signing secret")
	}
}

func TestLoadConsumerConfig_SignatureMalformedEntry(t *testing.T) {
	_, good := secretB64('c', 32)
	_, short := secretB64('d', 16)

	tests := []struct {
		name  string
		value string
	}{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel: mutates process env.
			setSignatureBaseEnv(t)
			t.Setenv("STREAMING_CONSUMER_REQUIRE_SIGNATURES", "true")
			t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", tt.value)

			_, warnings, err := LoadConsumerConfig()

			if !errors.Is(err, ErrInvalidConfigField) {
				t.Errorf("err = %v; want ErrInvalidConfigField", err)
			}

			if !errors.Is(err, contract.ErrInvalidSigningKey) {
				t.Errorf("err = %v; want it to also match ErrInvalidSigningKey", err)
			}

			if err != nil && (strings.Contains(err.Error(), good) || strings.Contains(err.Error(), short)) {
				t.Errorf("err text carries secret material: %v", err)
			}

			for _, w := range warnings {
				if strings.Contains(w, good) || strings.Contains(w, short) {
					t.Errorf("warning carries secret material: %q", w)
				}
			}
		})
	}
}

func TestLoadConsumerConfig_SignatureKeysWithoutRequireWarns(t *testing.T) {
	setSignatureBaseEnv(t)

	_, good := secretB64('e', 32)
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", "lender-k1@lender:"+good)

	cfg, warnings, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if cfg.RequireSignatures {
		t.Error("RequireSignatures = true; keys alone must not switch verification on")
	}

	found := false

	for _, w := range warnings {
		if strings.Contains(w, "STREAMING_CONSUMER_SIGNATURE_KEYS") && strings.Contains(w, "STREAMING_CONSUMER_REQUIRE_SIGNATURES") {
			found = true
		}

		if strings.Contains(w, good) {
			t.Errorf("warning carries secret material: %q", w)
		}
	}

	if !found {
		t.Errorf("warnings = %v; want one naming the inert STREAMING_CONSUMER_SIGNATURE_KEYS", warnings)
	}
}

func TestLoadConsumerConfig_SignatureMaxSkew(t *testing.T) {
	setSignatureBaseEnv(t)
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_MAX_SKEW_MS", "90000")

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig() error = %v", err)
	}

	if cfg.SignatureMaxSkew != 90*time.Second {
		t.Errorf("SignatureMaxSkew = %s; want 1m30s", cfg.SignatureMaxSkew)
	}
}

func TestLoadConsumerConfig_SignatureNegativeMaxSkew(t *testing.T) {
	setSignatureBaseEnv(t)
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_MAX_SKEW_MS", "-1")

	if _, _, err := LoadConsumerConfig(); !errors.Is(err, ErrInvalidConfigField) {
		t.Errorf("err = %v; want ErrInvalidConfigField for a negative skew", err)
	}
}

// TestLoadConsumerConfig_SignatureDisabledLoadsClean pins the rule every other
// variable follows: a disabled consumer loads clean, whatever the rest says.
func TestLoadConsumerConfig_SignatureDisabledLoadsClean(t *testing.T) {
	t.Setenv("STREAMING_CONSUMER_ENABLED", "false")
	t.Setenv("STREAMING_CONSUMER_REQUIRE_SIGNATURES", "true")
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", "garbage")

	if _, _, err := LoadConsumerConfig(); err != nil {
		t.Errorf("LoadConsumerConfig() error = %v; want nil for a disabled consumer", err)
	}
}
