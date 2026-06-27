//go:build unit

package kafkasec

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// TestSASLRequiresTLS is the core gate the producer (and later the consumer)
// depend on: SASL credentials must not cross the wire in cleartext. The only
// rejected combination is SASL present + TLS absent + plaintext NOT explicitly
// allowed.
func TestSASLRequiresTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		hasSASL        bool
		hasTLS         bool
		allowPlaintext bool
		wantErr        bool
	}{
		{name: "SASL without TLS rejected", hasSASL: true, hasTLS: false, allowPlaintext: false, wantErr: true},
		{name: "SASL with TLS accepted", hasSASL: true, hasTLS: true, allowPlaintext: false, wantErr: false},
		{name: "SASL without TLS but opt-in accepted", hasSASL: true, hasTLS: false, allowPlaintext: true, wantErr: false},
		{name: "no SASL no TLS accepted", hasSASL: false, hasTLS: false, allowPlaintext: false, wantErr: false},
		{name: "no SASL with TLS accepted", hasSASL: false, hasTLS: true, allowPlaintext: false, wantErr: false},
		{name: "opt-in alone (no SASL) accepted", hasSASL: false, hasTLS: false, allowPlaintext: true, wantErr: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := SASLRequiresTLS(tt.hasSASL, tt.hasTLS, tt.allowPlaintext)
			if tt.wantErr {
				if !errors.Is(err, contract.ErrPlaintextSASLNotAllowed) {
					t.Fatalf("SASLRequiresTLS(%v,%v,%v) = %v; want ErrPlaintextSASLNotAllowed",
						tt.hasSASL, tt.hasTLS, tt.allowPlaintext, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("SASLRequiresTLS(%v,%v,%v) = %v; want nil",
					tt.hasSASL, tt.hasTLS, tt.allowPlaintext, err)
			}
		})
	}
}

func TestValidateTLSConfig_NilAccepted(t *testing.T) {
	t.Parallel()

	if err := ValidateTLSConfig(nil); err != nil {
		t.Fatalf("ValidateTLSConfig(nil) = %v; want nil", err)
	}
}

func TestValidateTLSConfig_Rejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *tls.Config
	}{
		{name: "InsecureSkipVerify", cfg: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // test verifies rejection
		{name: "MinVersion TLS1.0", cfg: &tls.Config{MinVersion: tls.VersionTLS10}},
		{name: "MaxVersion TLS1.1", cfg: &tls.Config{MaxVersion: tls.VersionTLS11}},
		{name: "Min exceeds Max", cfg: &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS12}},
		{name: "weak cipher suite", cfg: &tls.Config{CipherSuites: []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA}}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateTLSConfig(tt.cfg); !errors.Is(err, contract.ErrInvalidTLSConfig) {
				t.Fatalf("ValidateTLSConfig(%s) = %v; want ErrInvalidTLSConfig", tt.name, err)
			}
		})
	}
}

func TestValidateTLSConfig_ApprovedSuiteAccepted(t *testing.T) {
	t.Parallel()

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
	}
	if err := ValidateTLSConfig(cfg); err != nil {
		t.Fatalf("ValidateTLSConfig(approved suite) = %v; want nil", err)
	}
}

// TestCloneTLSConfigWithDefaults_DefaultsAndDeepCopy proves the clone defaults
// MinVersion to TLS 1.2 and deep-copies the security-critical slices so a
// later caller mutation cannot weaken the stored policy.
func TestCloneTLSConfigWithDefaults_DefaultsAndDeepCopy(t *testing.T) {
	t.Parallel()

	if CloneTLSConfigWithDefaults(nil) != nil {
		t.Fatal("CloneTLSConfigWithDefaults(nil) = non-nil; want nil")
	}

	approved := tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
	caller := &tls.Config{CipherSuites: []uint16{approved}}

	cloned := CloneTLSConfigWithDefaults(caller)
	if cloned.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %#x; want TLS 1.2 default", cloned.MinVersion)
	}
	if &caller.CipherSuites[0] == &cloned.CipherSuites[0] {
		t.Fatal("CipherSuites slice aliased between caller and clone")
	}

	caller.CipherSuites[0] = tls.TLS_RSA_WITH_AES_128_CBC_SHA
	if cloned.CipherSuites[0] != approved {
		t.Errorf("after caller mutation, clone CipherSuites[0] = %#x; want %#x", cloned.CipherSuites[0], approved)
	}
}

// TestCloneTLSConfigWithDefaults_ClonesCAPools proves RootCAs/ClientCAs are
// cloned, not aliased (finding #7): a caller adding a trust root to its own pool
// after passing cfg must not retroactively widen who the stored config trusts.
func TestCloneTLSConfigWithDefaults_ClonesCAPools(t *testing.T) {
	t.Parallel()

	rootCAs := x509.NewCertPool()
	clientCAs := x509.NewCertPool()
	caller := &tls.Config{RootCAs: rootCAs, ClientCAs: clientCAs}

	cloned := CloneTLSConfigWithDefaults(caller)

	if cloned.RootCAs == caller.RootCAs {
		t.Error("RootCAs aliased between caller and clone; want a distinct cloned pool")
	}

	if cloned.ClientCAs == caller.ClientCAs {
		t.Error("ClientCAs aliased between caller and clone; want a distinct cloned pool")
	}

	// Mutating the caller's pool must not alter the clone's trust set.
	caller.RootCAs.AddCert(selfSignedCert(t))
	if caller.RootCAs.Equal(cloned.RootCAs) {
		t.Error("after caller added a cert, clone RootCAs equals caller's; the pool was aliased")
	}
}

// selfSignedCert mints a throwaway self-signed certificate for pool-mutation tests.
func selfSignedCert(t *testing.T) *x509.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "kafkasec-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}

	return cert
}
