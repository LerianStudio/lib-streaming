//go:build unit

package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

// caCertPEMBase64 mints a throwaway CA certificate and returns it as a
// base64-encoded PEM block, matching the STREAMING_TLS_CA_CERT wire shape.
func caCertPEMBase64(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "config-ca-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return base64.StdEncoding.EncodeToString(pemBytes)
}

func TestLoadConfig_TLSFieldsParsed(t *testing.T) {
	clearStreamingEnv(t)

	ca := caCertPEMBase64(t)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_TLS_ENABLED", "true")
	t.Setenv("STREAMING_TLS_CA_CERT", ca)

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if !cfg.TLSEnabled {
		t.Error("TLSEnabled = false; want true")
	}

	if cfg.TLSCACert != ca {
		t.Error("TLSCACert not round-tripped from env")
	}

	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("BuildTLSConfig() err = %v; want nil", err)
	}

	if tlsCfg == nil || tlsCfg.RootCAs == nil {
		t.Fatal("BuildTLSConfig() did not populate RootCAs from the CA cert")
	}
}

func TestLoadConfig_SASLFieldsParsed(t *testing.T) {
	clearStreamingEnv(t)

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_TLS_ENABLED", "true")
	t.Setenv("STREAMING_TLS_CA_CERT", caCertPEMBase64(t))
	t.Setenv("STREAMING_SASL_MECHANISM", "SCRAM-SHA-256")
	t.Setenv("STREAMING_SASL_USERNAME", "alice")
	t.Setenv("STREAMING_SASL_PASSWORD", "secret")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SASLMechanism != "SCRAM-SHA-256" {
		t.Errorf("SASLMechanism = %q; want SCRAM-SHA-256", cfg.SASLMechanism)
	}

	if cfg.SASLUsername != "alice" {
		t.Errorf("SASLUsername = %q; want alice", cfg.SASLUsername)
	}

	if cfg.SASLPassword != "secret" {
		t.Errorf("SASLPassword = %q; want secret", cfg.SASLPassword)
	}
}

func TestLoadConfig_DeprecatedAllowPlaintextAliasWarns(t *testing.T) {
	clearStreamingEnv(t)

	// Only the deprecated alias is set (canonical unset).
	t.Setenv("STREAMING_ALLOW_PLAINTEXT_SASL", "true")

	cfg, warnings, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if !cfg.SASLAllowPlaintext {
		t.Error("SASLAllowPlaintext = false; want true from deprecated alias")
	}

	found := false
	for _, w := range warnings {
		if strings.Contains(w, "STREAMING_ALLOW_PLAINTEXT_SASL") && strings.Contains(w, "STREAMING_SASL_ALLOW_PLAINTEXT") {
			found = true
		}

		if strings.Contains(w, "secret") || strings.Contains(w, "password") {
			t.Errorf("warning leaks credential material: %q", w)
		}
	}

	if !found {
		t.Errorf("warnings = %v; want a deprecation note mentioning both env names", warnings)
	}
}

func TestLoadConfig_CanonicalAllowPlaintextWinsOverAlias(t *testing.T) {
	clearStreamingEnv(t)

	// Canonical explicitly false; alias true. Canonical must win and no
	// deprecation warning is emitted because the alias is not consulted.
	t.Setenv("STREAMING_SASL_ALLOW_PLAINTEXT", "false")
	t.Setenv("STREAMING_ALLOW_PLAINTEXT_SASL", "true")

	cfg, warnings, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SASLAllowPlaintext {
		t.Error("SASLAllowPlaintext = true; want false (canonical wins)")
	}

	for _, w := range warnings {
		if strings.Contains(w, "STREAMING_ALLOW_PLAINTEXT_SASL") {
			t.Errorf("unexpected deprecation warning when canonical is set: %q", w)
		}
	}
}

func TestLoadConfig_InvalidSASLMechanismRejected(t *testing.T) {
	clearStreamingEnv(t)

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_SASL_MECHANISM", "GSSAPI")
	t.Setenv("STREAMING_SASL_USERNAME", "alice")
	t.Setenv("STREAMING_SASL_PASSWORD", "hunter2-secret")

	_, _, err := LoadConfig()
	if !errors.Is(err, ErrInvalidSASLMechanism) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSASLMechanism", err)
	}

	// LoadConfig's error is returned directly (NOT run through
	// sanitizeBrokerURL), so a future edit that echoes the credential into
	// the error string would leak the SECRET-annotated password. Lock it.
	if strings.Contains(err.Error(), "hunter2-secret") {
		t.Errorf("LoadConfig() err leaks SASL password: %q", err.Error())
	}
}

func TestLoadConfig_SASLMechanismMissingCredentialsRejected(t *testing.T) {
	clearStreamingEnv(t)

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_SASL_MECHANISM", "PLAIN")
	// username and password intentionally omitted

	_, _, err := LoadConfig()
	if !errors.Is(err, ErrInvalidSASLMechanism) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSASLMechanism", err)
	}
}

func TestLoadConfig_DisabledSkipsTLSSASLValidation(t *testing.T) {
	clearStreamingEnv(t)

	// Disabled: invalid mechanism and bad CA are tolerated because validation
	// is skipped when Enabled=false.
	t.Setenv("STREAMING_ENABLED", "false")
	t.Setenv("STREAMING_SASL_MECHANISM", "TOTALLY-BOGUS")
	t.Setenv("STREAMING_TLS_ENABLED", "true")
	t.Setenv("STREAMING_TLS_CA_CERT", "not-valid-base64===")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig(disabled) err = %v; want nil", err)
	}

	if cfg.Enabled {
		t.Error("cfg.Enabled = true; want false")
	}

	if cfg.SASLMechanism != "TOTALLY-BOGUS" {
		t.Errorf("SASLMechanism = %q; want raw passthrough when disabled", cfg.SASLMechanism)
	}
}
