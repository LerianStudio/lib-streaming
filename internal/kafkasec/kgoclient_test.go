//go:build unit

package kafkasec

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// adminTestPassword is the credential every case below feeds in, so each
// assertion can also prove it never reaches an error string.
const adminTestPassword = "admin-hunter2-secret"

// mustMechanism builds a mechanism for the option-assembly matrix. A failure
// here is a test-setup bug, not a subject-under-test finding.
func mustMechanism(t *testing.T, name string) sasl.Mechanism {
	t.Helper()

	mechanism, err := BuildSASLMechanism(name, "alice", adminTestPassword)
	if err != nil {
		t.Fatalf("BuildSASLMechanism(%q) err = %v; want nil", name, err)
	}

	return mechanism
}

// TestSecurityKgoOpts_AssemblyMatrix pins the option COUNT the shared assembly
// produces across the TLS × SASL grid. The count is the observable: franz-go
// options are opaque closures, so what this can prove is that a TLS dial adds
// exactly one option, a mechanism adds exactly one more, and neither is added
// when the corresponding feature is off. A silent drop of either would fall
// back to a plaintext, unauthenticated dial against a hardened broker.
//
// Every supported mechanism is exercised because BuildSASLMechanism branches
// per name, and a mechanism that mapped to a nil sasl.Mechanism would be
// dropped here with no error anywhere.
func TestSecurityKgoOpts_AssemblyMatrix(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	tests := []struct {
		name           string
		tlsConfig      *tls.Config
		mechanismName  string
		allowPlaintext bool
		wantOpts       int
	}{
		{name: "no TLS, no SASL", wantOpts: 0},
		{name: "TLS only", tlsConfig: tlsConfig, wantOpts: 1},
		{name: "TLS + PLAIN", tlsConfig: tlsConfig, mechanismName: "PLAIN", wantOpts: 2},
		{name: "TLS + SCRAM-SHA-256", tlsConfig: tlsConfig, mechanismName: "SCRAM-SHA-256", wantOpts: 2},
		{name: "TLS + SCRAM-SHA-512", tlsConfig: tlsConfig, mechanismName: "SCRAM-SHA-512", wantOpts: 2},
		{
			name:           "SASL over plaintext with explicit opt-in",
			mechanismName:  "PLAIN",
			allowPlaintext: true,
			wantOpts:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mechanism sasl.Mechanism
			if tt.mechanismName != "" {
				mechanism = mustMechanism(t, tt.mechanismName)
			}

			opts, err := SecurityKgoOpts(tt.tlsConfig, mechanism, tt.allowPlaintext)
			if err != nil {
				t.Fatalf("SecurityKgoOpts(%s) err = %v; want nil", tt.name, err)
			}

			if len(opts) != tt.wantOpts {
				t.Errorf("SecurityKgoOpts(%s) produced %d options; want %d", tt.name, len(opts), tt.wantOpts)
			}
		})
	}
}

// TestSecurityKgoOpts_FailsClosed pins the two refusals the shared assembly
// owns: a weakened TLS config, and SASL credentials about to cross a plaintext
// connection without an explicit opt-in.
func TestSecurityKgoOpts_FailsClosed(t *testing.T) {
	t.Parallel()

	t.Run("SASL without TLS", func(t *testing.T) {
		t.Parallel()

		opts, err := SecurityKgoOpts(nil, mustMechanism(t, "PLAIN"), false)
		if !errors.Is(err, contract.ErrPlaintextSASLNotAllowed) {
			t.Fatalf("SecurityKgoOpts(SASL, no TLS) err = %v; want ErrPlaintextSASLNotAllowed", err)
		}

		if opts != nil {
			t.Errorf("SecurityKgoOpts(SASL, no TLS) opts = %v; want nil", opts)
		}
	})

	t.Run("InsecureSkipVerify", func(t *testing.T) {
		t.Parallel()

		_, err := SecurityKgoOpts(&tls.Config{InsecureSkipVerify: true}, nil, false) //nolint:gosec // deliberately invalid input: the assertion is that it is REFUSED
		if !errors.Is(err, contract.ErrInvalidTLSConfig) {
			t.Fatalf("SecurityKgoOpts(InsecureSkipVerify) err = %v; want ErrInvalidTLSConfig", err)
		}
	})
}

// TestSecurityKgoOpts_TypedNilMechanismIsNotSASL pins the typed-nil
// normalization. A `var m sasl.Mechanism = (*plain.Mechanism)(nil)` compares
// non-nil, so without the reflect-based check the SASL-requires-TLS gate would
// read "SASL is configured" and refuse a perfectly valid plaintext dial — and,
// with TLS on, would append kgo.SASL(nil) and fail at authentication time.
func TestSecurityKgoOpts_TypedNilMechanismIsNotSASL(t *testing.T) {
	t.Parallel()

	var typedNil sasl.Mechanism = (*typedNilMechanism)(nil)

	opts, err := SecurityKgoOpts(nil, typedNil, false)
	if err != nil {
		t.Fatalf("SecurityKgoOpts(typed-nil mechanism) err = %v; want nil", err)
	}

	if len(opts) != 0 {
		t.Errorf("SecurityKgoOpts(typed-nil mechanism) produced %d options; want 0", len(opts))
	}
}

// typedNilMechanism exists only to be nil through a sasl.Mechanism interface.
type typedNilMechanism struct{}

func (*typedNilMechanism) Name() string { return "typed-nil" }

func (*typedNilMechanism) Authenticate(_ context.Context, _ string) (sasl.Session, []byte, error) {
	return nil, nil, errors.New("typed-nil mechanism must never authenticate")
}

// TestBuildAdminClient_EmptyBrokersFailsClosed pins that an admin client with
// no seed broker is refused rather than silently pinned to franz-go's
// localhost:9092 default — a client that would "work" locally and quietly
// describe the wrong cluster.
func TestBuildAdminClient_EmptyBrokersFailsClosed(t *testing.T) {
	t.Parallel()

	for _, brokers := range [][]string{nil, {}} {
		client, err := BuildAdminClient(brokers, "admin", nil, nil, false)
		if !errors.Is(err, contract.ErrMissingBrokers) {
			t.Fatalf("BuildAdminClient(%v) err = %v; want ErrMissingBrokers", brokers, err)
		}

		if client != nil {
			t.Errorf("BuildAdminClient(%v) = %v; want nil client", brokers, client)
		}
	}
}

// TestBuildAdminClient_BuildsWithSecurityPosture pins the happy paths: a
// plaintext dial, and a TLS + SASL dial. Construction performs no broker I/O,
// so both return a live client against an address nothing is listening on.
func TestBuildAdminClient_BuildsWithSecurityPosture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tlsConfig     *tls.Config
		mechanismName string
	}{
		{name: "plaintext"},
		{name: "TLS + SCRAM-SHA-512", tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}, mechanismName: "SCRAM-SHA-512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mechanism sasl.Mechanism
			if tt.mechanismName != "" {
				mechanism = mustMechanism(t, tt.mechanismName)
			}

			client, err := BuildAdminClient([]string{"broker.lerian.test:9092"}, "admin", tt.tlsConfig, mechanism, false)
			if err != nil {
				t.Fatalf("BuildAdminClient(%s) err = %v; want nil", tt.name, err)
			}

			if client == nil {
				t.Fatal("BuildAdminClient() = nil; want non-nil client")
			}

			client.Close()
		})
	}
}
