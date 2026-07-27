//go:build unit

package kafkasec

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
)

// selfSignedCertPEMBase64 mints a throwaway self-signed certificate and returns
// it as a base64-encoded PEM block, matching the STREAMING_TLS_CA_CERT wire
// shape that BuildTLSConfigFromCA consumes.
func selfSignedCertPEMBase64(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kafkasec-ca-test"},
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

func TestBuildTLSConfigFromCA_DisabledReturnsNil(t *testing.T) {
	t.Parallel()

	cfg, err := BuildTLSConfigFromCA(false, selfSignedCertPEMBase64(t))
	if err != nil {
		t.Fatalf("BuildTLSConfigFromCA(disabled) err = %v; want nil", err)
	}

	if cfg != nil {
		t.Fatalf("BuildTLSConfigFromCA(disabled) = %v; want nil config", cfg)
	}
}

func TestBuildTLSConfigFromCA_EmptyCASystemPool(t *testing.T) {
	t.Parallel()

	cfg, err := BuildTLSConfigFromCA(true, "")
	if err != nil {
		t.Fatalf("BuildTLSConfigFromCA(enabled, empty CA) err = %v; want nil", err)
	}

	if cfg == nil {
		t.Fatal("BuildTLSConfigFromCA(enabled, empty CA) = nil; want non-nil config")
	}

	if cfg.RootCAs != nil {
		t.Errorf("RootCAs = %v; want nil (system pool)", cfg.RootCAs)
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x; want TLS 1.2", cfg.MinVersion)
	}

	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true; want false")
	}
}

func TestBuildTLSConfigFromCA_ValidCAPopulatesPool(t *testing.T) {
	t.Parallel()

	cfg, err := BuildTLSConfigFromCA(true, selfSignedCertPEMBase64(t))
	if err != nil {
		t.Fatalf("BuildTLSConfigFromCA(valid CA) err = %v; want nil", err)
	}

	if cfg == nil {
		t.Fatal("BuildTLSConfigFromCA(valid CA) = nil; want non-nil config")
	}

	if cfg.RootCAs == nil {
		t.Error("RootCAs = nil; want a populated pool")
	}

	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true; want false")
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x; want TLS 1.2", cfg.MinVersion)
	}
}

func TestBuildTLSConfigFromCA_InvalidBase64(t *testing.T) {
	t.Parallel()

	_, err := BuildTLSConfigFromCA(true, "not!!valid!!base64===")
	if !errors.Is(err, contract.ErrInvalidTLSConfig) {
		t.Fatalf("BuildTLSConfigFromCA(invalid base64) err = %v; want ErrInvalidTLSConfig", err)
	}
}

func TestBuildTLSConfigFromCA_WellFormedBase64NonCert(t *testing.T) {
	t.Parallel()

	// Valid base64 but the decoded bytes are not a PEM certificate.
	junk := base64.StdEncoding.EncodeToString([]byte("this is not a PEM certificate"))

	_, err := BuildTLSConfigFromCA(true, junk)
	if !errors.Is(err, contract.ErrInvalidTLSConfig) {
		t.Fatalf("BuildTLSConfigFromCA(non-cert PEM) err = %v; want ErrInvalidTLSConfig", err)
	}
}

func TestBuildSASLMechanism_Mechanisms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mechanism string
		want      string
	}{
		{name: "PLAIN", mechanism: "PLAIN", want: "PLAIN"},
		{name: "SCRAM-SHA-256", mechanism: "SCRAM-SHA-256", want: "SCRAM-SHA-256"},
		{name: "SCRAM-SHA-512", mechanism: "SCRAM-SHA-512", want: "SCRAM-SHA-512"},
		{name: "case-insensitive lower plain", mechanism: "plain", want: "PLAIN"},
		{name: "case-insensitive scram", mechanism: "scram-sha-256", want: "SCRAM-SHA-256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mech, err := BuildSASLMechanism(tt.mechanism, "alice", "secret")
			if err != nil {
				t.Fatalf("BuildSASLMechanism(%q) err = %v; want nil", tt.mechanism, err)
			}

			if mech == nil {
				t.Fatalf("BuildSASLMechanism(%q) = nil; want non-nil mechanism", tt.mechanism)
			}

			// Assert the constructed mechanism's identity, not just non-nil.
			// A swapped SCRAM-256/512 constructor would return a non-nil
			// mechanism with the wrong Name(); the identity check catches it.
			if got := mech.Name(); got != tt.want {
				t.Errorf("BuildSASLMechanism(%q).Name() = %q; want %q", tt.mechanism, got, tt.want)
			}
		})
	}
}

func TestBuildSASLMechanism_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	mech, err := BuildSASLMechanism("", "", "")
	if err != nil {
		t.Fatalf("BuildSASLMechanism(\"\") err = %v; want nil", err)
	}

	if mech != nil {
		t.Fatalf("BuildSASLMechanism(\"\") = %v; want nil mechanism", mech)
	}
}

func TestBuildSASLMechanism_UnknownRejected(t *testing.T) {
	t.Parallel()

	const password = "hunter2-secret"

	_, err := BuildSASLMechanism("GSSAPI", "alice", password)
	if !errors.Is(err, contract.ErrInvalidSASLMechanism) {
		t.Fatalf("BuildSASLMechanism(unknown) err = %v; want ErrInvalidSASLMechanism", err)
	}

	// The docstring promises the error never includes the password value.
	// A future edit that echoes credentials into the error string must fail
	// the suite rather than silently leaking the secret.
	if strings.Contains(err.Error(), password) {
		t.Errorf("BuildSASLMechanism(unknown) err leaks password: %q", err.Error())
	}
}

func TestBuildSASLMechanism_MissingCredentialsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		user string
		pass string
	}{
		// "missing user" carries a recognizable password so the negative
		// assertion below is meaningful: the mechanism is rejected for the
		// empty username, yet the supplied password must not be echoed.
		{name: "missing user", user: "", pass: "hunter2-secret"},
		{name: "missing pass", user: "alice", pass: ""},
		{name: "missing both", user: "", pass: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := BuildSASLMechanism("PLAIN", tt.user, tt.pass)
			if !errors.Is(err, contract.ErrInvalidSASLMechanism) {
				t.Fatalf("BuildSASLMechanism(%q,%q) err = %v; want ErrInvalidSASLMechanism", tt.user, tt.pass, err)
			}

			// The docstring promises the error never includes the password
			// value. Only assertable when a password was actually supplied.
			if tt.pass != "" && strings.Contains(err.Error(), tt.pass) {
				t.Errorf("BuildSASLMechanism(%q,...) err leaks password: %q", tt.user, err.Error())
			}
		})
	}
}
