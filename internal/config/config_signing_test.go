//go:build unit

package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// signingTestSecret is 40 distinctive bytes; its base64 is what the tests
// search for in every rendering that must not carry it.
var signingTestSecret = []byte("0123456789abcdefghijklmnopqrstuvwxyzSECR")

func setSigningBaseEnv(t *testing.T) {
	t.Helper()

	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "lerian-test-svc")
}

func TestLoadConfig_SigningValid(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "lerian-test-svc-2026-09")
	t.Setenv("STREAMING_SIGNING_KEY", base64.StdEncoding.EncodeToString(signingTestSecret)+"\n")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SigningKeyID != "lerian-test-svc-2026-09" {
		t.Errorf("SigningKeyID = %q; want lerian-test-svc-2026-09", cfg.SigningKeyID)
	}

	if !bytes.Equal(cfg.SigningKey, signingTestSecret) {
		t.Errorf("SigningKey was not decoded from base64 (len %d)", len(cfg.SigningKey))
	}
}

func TestLoadConfig_SigningUnsetIsOff(t *testing.T) {
	setSigningBaseEnv(t)

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SigningKeyID != "" || len(cfg.SigningKey) != 0 {
		t.Errorf("signing fields = %q/%d bytes; want empty", cfg.SigningKeyID, len(cfg.SigningKey))
	}
}

func TestLoadConfig_SigningIDWithoutKey(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")

	_, _, err := LoadConfig()
	if !errors.Is(err, ErrInvalidConfigField) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidConfigField", err)
	}

	if !strings.Contains(err.Error(), "STREAMING_SIGNING_KEY") {
		t.Errorf("error %q does not name the missing variable", err)
	}
}

func TestLoadConfig_SigningKeyWithoutID(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY", base64.StdEncoding.EncodeToString(signingTestSecret))

	_, _, err := LoadConfig()
	if !errors.Is(err, ErrInvalidConfigField) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidConfigField", err)
	}

	assertNoSecret(t, err.Error())
}

func TestLoadConfig_SigningBadBase64(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")
	t.Setenv("STREAMING_SIGNING_KEY", "not*base64!"+string(signingTestSecret))

	_, _, err := LoadConfig()
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSigningKey", err)
	}

	if strings.Contains(err.Error(), string(signingTestSecret)) {
		t.Errorf("error %q echoes the raw variable value", err)
	}
}

func TestLoadConfig_SigningShortKey(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")
	t.Setenv("STREAMING_SIGNING_KEY", base64.StdEncoding.EncodeToString(signingTestSecret[:31]))

	_, _, err := LoadConfig()
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSigningKey", err)
	}
}

func TestLoadConfig_SigningInvalidKeyID(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "Not A Key Id")
	t.Setenv("STREAMING_SIGNING_KEY", base64.StdEncoding.EncodeToString(signingTestSecret))

	_, _, err := LoadConfig()
	if !errors.Is(err, contract.ErrInvalidSigningKey) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSigningKey", err)
	}
}

func TestLoadConfig_SigningSkippedWhenDisabled(t *testing.T) {
	clearStreamingEnv(t)
	t.Setenv("STREAMING_ENABLED", "false")
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")
	t.Setenv("STREAMING_SIGNING_KEY", "not*base64!")

	if _, _, err := LoadConfig(); err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil when streaming is disabled", err)
	}
}

func TestLoadConfig_SigningNeverInWarnings(t *testing.T) {
	setSigningBaseEnv(t)
	t.Setenv("STREAMING_SIGNING_KEY_ID", "k1")
	t.Setenv("STREAMING_SIGNING_KEY", base64.StdEncoding.EncodeToString(signingTestSecret))
	// Force a warning so the slice is non-empty.
	t.Setenv("STREAMING_ALLOW_PLAINTEXT_SASL", "true")

	cfg, warnings, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if len(warnings) == 0 {
		t.Fatal("expected at least one warning to inspect")
	}

	for _, w := range warnings {
		assertNoSecret(t, w)
	}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x"} {
		assertNoSecret(t, fmt.Sprintf(verb, cfg))
	}
}

func assertNoSecret(t *testing.T, rendered string) {
	t.Helper()

	for _, needle := range []string{
		string(signingTestSecret),
		base64.StdEncoding.EncodeToString(signingTestSecret),
		fmt.Sprintf("%x", signingTestSecret),
		"SECR",
	} {
		if strings.Contains(rendered, needle) {
			t.Fatalf("rendering leaks the signing secret (%q found): %s", needle, rendered)
		}
	}
}
