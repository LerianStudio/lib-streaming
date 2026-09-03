//go:build unit

package streaming_test

import (
	"errors"
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// adminPassword is fed into every failing case below so each assertion can also
// prove the SASL secret never reaches an error string.
const adminPassword = "admin-hunter2-secret"

// adminBrokers is a non-resolving address. Construction performs no broker I/O
// — franz-go dials lazily — so every happy path below succeeds against it.
var adminBrokers = []string{"broker.lerian.test:9092"}

// TestNewAdminClient_MechanismMatrix pins the happy paths across the SASL
// mechanisms the library supports, plaintext included. Each returns a usable
// admin client with no network I/O at construction.
func TestNewAdminClient_MechanismMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  streaming.Config
	}{
		{
			name: "no TLS, no SASL",
			cfg:  streaming.Config{Brokers: adminBrokers},
		},
		{
			name: "TLS with system trust pool",
			cfg:  streaming.Config{Brokers: adminBrokers, TLSEnabled: true},
		},
		{
			name: "TLS + PLAIN",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "PLAIN",
				SASLUsername:  "alice",
				SASLPassword:  adminPassword,
			},
		},
		{
			name: "TLS + SCRAM-SHA-256",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "SCRAM-SHA-256",
				SASLUsername:  "alice",
				SASLPassword:  adminPassword,
			},
		},
		{
			name: "TLS + SCRAM-SHA-512 (lowercase, padded)",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "  scram-sha-512  ",
				SASLUsername:  "alice",
				SASLPassword:  adminPassword,
			},
		},
		{
			name: "SASL over plaintext with explicit opt-in",
			cfg: streaming.Config{
				Brokers:            adminBrokers,
				SASLMechanism:      "PLAIN",
				SASLUsername:       "alice",
				SASLPassword:       adminPassword,
				SASLAllowPlaintext: true,
			},
		},
		{
			name: "ClientID is applied",
			cfg:  streaming.Config{Brokers: adminBrokers, ClientID: "br-spb-retention-check"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := streaming.NewAdminClient(tt.cfg)
			if err != nil {
				t.Fatalf("NewAdminClient(%s) err = %v; want nil", tt.name, err)
			}

			if client == nil {
				t.Fatal("NewAdminClient() = nil; want non-nil client")
			}

			// The caller owns the client's lifecycle; this is the documented
			// disposal, and it closes the underlying kgo client.
			client.Close()
		})
	}
}

// TestNewAdminClient_FailsClosed pins that the public constructor inherits every
// fail-closed guard of the internal builder, and that none of them renders the
// SASL password into the returned error.
//
// The empty-brokers case is the load-bearing one: without it, franz-go would
// hand back a client pinned to its localhost:9092 default, and a retention check
// running against that client would describe some other cluster — or nothing —
// while reporting success.
func TestNewAdminClient_FailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     streaming.Config
		wantErr error
	}{
		{
			name:    "no brokers",
			cfg:     streaming.Config{},
			wantErr: streaming.ErrProducerMissingBrokers,
		},
		{
			name:    "empty broker slice",
			cfg:     streaming.Config{Brokers: []string{}, TLSEnabled: true},
			wantErr: streaming.ErrProducerMissingBrokers,
		},
		{
			name: "password without username",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "PLAIN",
				SASLPassword:  adminPassword,
			},
			wantErr: streaming.ErrInvalidSASLMechanism,
		},
		{
			name: "username without password",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "PLAIN",
				SASLUsername:  "alice",
			},
			wantErr: streaming.ErrInvalidSASLMechanism,
		},
		{
			name: "unsupported mechanism",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				TLSEnabled:    true,
				SASLMechanism: "GSSAPI",
				SASLUsername:  "alice",
				SASLPassword:  adminPassword,
			},
			wantErr: streaming.ErrInvalidSASLMechanism,
		},
		{
			name: "SASL without TLS and without the opt-in",
			cfg: streaming.Config{
				Brokers:       adminBrokers,
				SASLMechanism: "PLAIN",
				SASLUsername:  "alice",
				SASLPassword:  adminPassword,
			},
			wantErr: streaming.ErrPlaintextSASLNotAllowed,
		},
		{
			name: "malformed base64 CA certificate",
			cfg: streaming.Config{
				Brokers:    adminBrokers,
				TLSEnabled: true,
				TLSCACert:  "not-valid-base64!!!",
			},
			wantErr: streaming.ErrInvalidTLSConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := streaming.NewAdminClient(tt.cfg)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewAdminClient(%s) err = %v; want %v", tt.name, err, tt.wantErr)
			}

			if client != nil {
				t.Errorf("NewAdminClient(%s) = %v; want nil client", tt.name, client)
			}

			if strings.Contains(err.Error(), adminPassword) {
				t.Errorf("NewAdminClient(%s) err leaks the SASL password: %q", tt.name, err.Error())
			}
		})
	}
}
