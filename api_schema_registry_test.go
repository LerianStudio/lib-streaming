//go:build unit

package streaming_test

import (
	"errors"
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v2"
)

// TestNewSchemaRegistryClient_ValidConfigReturnsClient pins the happy path:
// a URL-only config yields a usable client with no network I/O at construction.
func TestNewSchemaRegistryClient_ValidConfigReturnsClient(t *testing.T) {
	t.Parallel()

	client, err := streaming.NewSchemaRegistryClient(streaming.Config{
		SchemaRegistryURL: "https://sr.lerian.test",
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistryClient(valid) err = %v; want nil", err)
	}

	if client == nil {
		t.Fatal("NewSchemaRegistryClient(valid) = nil; want non-nil client")
	}
}

// TestNewSchemaRegistryClient_WithBasicAuthReturnsClient pins that a full
// credential pair is accepted.
func TestNewSchemaRegistryClient_WithBasicAuthReturnsClient(t *testing.T) {
	t.Parallel()

	client, err := streaming.NewSchemaRegistryClient(streaming.Config{
		SchemaRegistryURL:      "https://sr.lerian.test",
		SchemaRegistryUsername: "alice",
		SchemaRegistryPassword: "sr-secret",
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistryClient(basic auth) err = %v; want nil", err)
	}

	if client == nil {
		t.Fatal("NewSchemaRegistryClient(basic auth) = nil; want non-nil client")
	}
}

// TestNewSchemaRegistryClient_FailClosed pins that the public constructor
// inherits the hardened builder's fail-closed guards: an empty URL and a
// partial (XOR) credential both surface ErrInvalidSchemaRegistryConfig, and the
// password is never rendered into the error.
func TestNewSchemaRegistryClient_FailClosed(t *testing.T) {
	t.Parallel()

	const password = "sr-hunter2-secret"

	tests := []struct {
		name string
		cfg  streaming.Config
	}{
		{
			name: "empty URL",
			cfg:  streaming.Config{SchemaRegistryURL: ""},
		},
		{
			name: "password without username",
			cfg: streaming.Config{
				SchemaRegistryURL:      "https://sr.lerian.test",
				SchemaRegistryPassword: password,
			},
		},
		{
			name: "username without password",
			cfg: streaming.Config{
				SchemaRegistryURL:      "https://sr.lerian.test",
				SchemaRegistryUsername: "alice",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := streaming.NewSchemaRegistryClient(tt.cfg)
			if !errors.Is(err, streaming.ErrInvalidSchemaRegistryConfig) {
				t.Fatalf("NewSchemaRegistryClient(%s) err = %v; want ErrInvalidSchemaRegistryConfig", tt.name, err)
			}

			if client != nil {
				t.Errorf("NewSchemaRegistryClient(%s) = %v; want nil client", tt.name, client)
			}

			if strings.Contains(err.Error(), password) {
				t.Errorf("NewSchemaRegistryClient(%s) err leaks password: %q", tt.name, err.Error())
			}
		})
	}
}
