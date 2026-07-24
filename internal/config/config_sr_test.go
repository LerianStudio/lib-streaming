//go:build unit

package config

import (
	"errors"
	"strings"
	"testing"
)

// SR env-var tests use t.Setenv and therefore do NOT call t.Parallel(),
// matching the existing config_tls_sasl_test.go convention.

func TestLoadConfig_SchemaRegistryFieldsParsed(t *testing.T) {
	clearStreamingEnv(t)

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_URL", "https://sr.lerian.test")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_USERNAME", "alice")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_PASSWORD", "sr-secret")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SchemaRegistryURL != "https://sr.lerian.test" {
		t.Errorf("SchemaRegistryURL = %q; want https://sr.lerian.test", cfg.SchemaRegistryURL)
	}

	if cfg.SchemaRegistryUsername != "alice" {
		t.Errorf("SchemaRegistryUsername = %q; want alice", cfg.SchemaRegistryUsername)
	}

	if cfg.SchemaRegistryPassword != "sr-secret" {
		t.Errorf("SchemaRegistryPassword = %q; want sr-secret", cfg.SchemaRegistryPassword)
	}
}

func TestLoadConfig_SchemaRegistryEmptyIsOptional(t *testing.T) {
	clearStreamingEnv(t)

	// An enabled config with no STREAMING_SCHEMA_REGISTRY_* vars must load
	// clean: the registry is only needed on the billing serialize path.
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	if cfg.SchemaRegistryURL != "" {
		t.Errorf("SchemaRegistryURL = %q; want empty", cfg.SchemaRegistryURL)
	}
}

func TestLoadConfig_SchemaRegistryUsernameWithoutPasswordRejected(t *testing.T) {
	clearStreamingEnv(t)

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_URL", "https://sr.lerian.test")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_USERNAME", "alice")
	// password intentionally omitted — a partial credential is a misconfig.

	_, _, err := LoadConfig()
	if !errors.Is(err, ErrInvalidSchemaRegistryConfig) {
		t.Fatalf("LoadConfig() err = %v; want ErrInvalidSchemaRegistryConfig", err)
	}
}

func TestLoadConfig_SchemaRegistryDisabledSkipsValidation(t *testing.T) {
	clearStreamingEnv(t)

	// Disabled: a partial SR credential is tolerated because validation is
	// skipped when Enabled=false (mirrors the SASL/TLS disabled behavior).
	t.Setenv("STREAMING_ENABLED", "false")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_URL", "https://sr.lerian.test")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_USERNAME", "alice")

	cfg, _, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig(disabled) err = %v; want nil", err)
	}

	if cfg.Enabled {
		t.Error("cfg.Enabled = true; want false")
	}

	if cfg.SchemaRegistryURL != "https://sr.lerian.test" {
		t.Errorf("SchemaRegistryURL = %q; want raw passthrough when disabled", cfg.SchemaRegistryURL)
	}
}

// TestLoadConfig_SchemaRegistryErrorNeverLeaksPassword locks the SECRET
// contract on SchemaRegistryPassword: even when a password IS present, the
// username-without-password gate must never fire in a way that renders the
// password. Here a full credential set is valid (no error); a subsequent edit
// that started echoing the password into a config error would be caught by the
// builder-level leak test in kafkasec. This test additionally asserts the
// happy-path password round-trips without appearing in any warning string.
func TestLoadConfig_SchemaRegistryErrorNeverLeaksPassword(t *testing.T) {
	clearStreamingEnv(t)

	const password = "sr-hunter2-secret"

	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "//lerian.test/svc")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_URL", "https://sr.lerian.test")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_USERNAME", "alice")
	t.Setenv("STREAMING_SCHEMA_REGISTRY_PASSWORD", password)

	_, warnings, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() err = %v; want nil", err)
	}

	for _, w := range warnings {
		if strings.Contains(w, password) {
			t.Errorf("warning leaks SR password: %q", w)
		}
	}
}
