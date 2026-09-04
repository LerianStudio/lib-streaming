//go:build unit

package kafkasec

import (
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

func TestBuildSchemaRegistryClient_ValidURLReturnsClient(t *testing.T) {
	t.Parallel()

	client, err := BuildSchemaRegistryClient("https://sr.lerian.test", "", "")
	if err != nil {
		t.Fatalf("BuildSchemaRegistryClient(valid URL) err = %v; want nil", err)
	}

	if client == nil {
		t.Fatal("BuildSchemaRegistryClient(valid URL) = nil; want non-nil client")
	}
}

func TestBuildSchemaRegistryClient_WithBasicAuthReturnsClient(t *testing.T) {
	t.Parallel()

	client, err := BuildSchemaRegistryClient("https://sr.lerian.test", "alice", "sr-secret")
	if err != nil {
		t.Fatalf("BuildSchemaRegistryClient(basic auth) err = %v; want nil", err)
	}

	if client == nil {
		t.Fatal("BuildSchemaRegistryClient(basic auth) = nil; want non-nil client")
	}
}

func TestBuildSchemaRegistryClient_PartialCredentialsReturnSentinel(t *testing.T) {
	t.Parallel()

	const password = "sr-hunter2-secret"

	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "password without username", username: "", password: password},
		{name: "username without password", username: "alice", password: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := BuildSchemaRegistryClient("https://sr.lerian.test", tt.username, tt.password)
			if !errors.Is(err, contract.ErrInvalidSchemaRegistryConfig) {
				t.Fatalf("BuildSchemaRegistryClient(%q creds) err = %v; want ErrInvalidSchemaRegistryConfig", tt.name, err)
			}

			if client != nil {
				t.Errorf("BuildSchemaRegistryClient(%q creds) = %v; want nil client", tt.name, client)
			}

			// The password must never be rendered into the error.
			if strings.Contains(err.Error(), password) {
				t.Errorf("BuildSchemaRegistryClient(%q creds) err leaks password: %q", tt.name, err.Error())
			}
		})
	}
}

func TestBuildSchemaRegistryClient_EmptyURLReturnsSentinel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		url  string
	}{
		{name: "empty", url: ""},
		{name: "whitespace only", url: "   "},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const password = "sr-hunter2-secret"

			client, err := BuildSchemaRegistryClient(tt.url, "alice", password)
			if !errors.Is(err, contract.ErrInvalidSchemaRegistryConfig) {
				t.Fatalf("BuildSchemaRegistryClient(%q) err = %v; want ErrInvalidSchemaRegistryConfig", tt.url, err)
			}

			if client != nil {
				t.Errorf("BuildSchemaRegistryClient(%q) = %v; want nil client", tt.url, client)
			}

			// The docstring promises the error never includes the password.
			if strings.Contains(err.Error(), password) {
				t.Errorf("BuildSchemaRegistryClient(%q) err leaks password: %q", tt.url, err.Error())
			}
		})
	}
}
