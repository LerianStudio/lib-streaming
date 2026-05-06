//go:build unit

package config

import (
	"errors"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// validBaseConfig returns a Config that passes every existing
// (Brokers/Source/Compression/Acks) check AND every range check. T9 tests
// derive from this fixture and mutate ONE field per case so the failure
// signal is unambiguous.
func validBaseConfig() Config {
	return Config{
		Enabled:               true,
		Brokers:               []string{"localhost:9092"},
		ClientID:              "test",
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    10_000,
		Compression:           "lz4",
		RecordRetries:         10,
		RecordDeliveryTimeout: 30 * time.Second,
		RequiredAcks:          "all",
		CBFailureRatio:        0.5,
		CBMinRequests:         10,
		CBTimeout:             30 * time.Second,
		CloseTimeout:          30 * time.Second,
		CloudEventsSource:     "//svc/test",
	}
}

// TestConfig_Validate_BaselineAccepts pins that the fixture is genuinely
// valid — every subsequent test deliberately INVALIDATES one field, so a
// regression here would mask the per-field assertions.
func TestConfig_Validate_BaselineAccepts(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig()
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() on baseline returned error: %v", err)
	}
}

// TestConfig_Validate_CBFailureRatioOutOfRange pins T9: STREAMING_CB_FAILURE_RATIO=2.5
// must be rejected with ErrInvalidConfigField. Without this check, a
// misconfigured ratio silently breaks gobreaker semantics.
func TestConfig_Validate_CBFailureRatioOutOfRange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		ratio float64
	}{
		{"above 1.0", 2.5},
		{"negative", -0.1},
		{"exactly 1.5", 1.5},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			cfg.CBFailureRatio = tc.ratio

			err := cfg.validate()
			if !errors.Is(err, ErrInvalidConfigField) {
				t.Fatalf("validate() error = %v; want ErrInvalidConfigField", err)
			}
		})
	}
}

// TestConfig_Validate_CBFailureRatioZeroAccepted pins that zero falls through
// to the producer's preset-fallback path. The contract documented in
// buildCBConfigFromMulti (internal/producer/producer_multi.go) treats zero
// as "use the HTTP preset"; rejecting it here would break that path.
func TestConfig_Validate_CBFailureRatioZeroAccepted(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig()
	cfg.CBFailureRatio = 0

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() rejected zero CBFailureRatio: %v", err)
	}
}

// TestConfig_Validate_NegativeNonNegativeFields pins T9: BatchLingerMs,
// RecordRetries, CBMinRequests, CBTimeout reject negative values. Zero is
// permitted (preset fallback for the duration/CB fields; valid for
// BatchLingerMs / RecordRetries).
func TestConfig_Validate_NegativeNonNegativeFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"BatchLingerMs negative", func(c *Config) { c.BatchLingerMs = -1 }},
		{"RecordRetries negative", func(c *Config) { c.RecordRetries = -1 }},
		{"CBMinRequests negative", func(c *Config) { c.CBMinRequests = -1 }},
		{"CBTimeout negative", func(c *Config) { c.CBTimeout = -5 * time.Second }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			tc.mutate(&cfg)

			err := cfg.validate()
			if !errors.Is(err, ErrInvalidConfigField) {
				t.Fatalf("validate() error = %v; want ErrInvalidConfigField", err)
			}
		})
	}
}

// TestConfig_Validate_ZeroPositiveFields pins T9: BatchMaxBytes,
// MaxBufferedRecords, RecordDeliveryTimeout, CloseTimeout reject zero or
// negative — these have no preset-fallback path so zero would flow into
// franz-go and surface as a confusing transport-layer error.
func TestConfig_Validate_ZeroPositiveFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"BatchMaxBytes zero", func(c *Config) { c.BatchMaxBytes = 0 }},
		{"BatchMaxBytes negative", func(c *Config) { c.BatchMaxBytes = -1 }},
		{"MaxBufferedRecords zero", func(c *Config) { c.MaxBufferedRecords = 0 }},
		{"MaxBufferedRecords negative", func(c *Config) { c.MaxBufferedRecords = -1 }},
		{"RecordDeliveryTimeout zero", func(c *Config) { c.RecordDeliveryTimeout = 0 }},
		{"RecordDeliveryTimeout negative", func(c *Config) { c.RecordDeliveryTimeout = -5 * time.Second }},
		{"CloseTimeout zero", func(c *Config) { c.CloseTimeout = 0 }},
		{"CloseTimeout negative", func(c *Config) { c.CloseTimeout = -5 * time.Second }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			tc.mutate(&cfg)

			err := cfg.validate()
			if !errors.Is(err, ErrInvalidConfigField) {
				t.Fatalf("validate() error = %v; want ErrInvalidConfigField", err)
			}
		})
	}
}

// TestConfig_Validate_CBPresetFallbackZerosAccepted pins that zero values for
// the preset-fallback fields (CBMinRequests, CBTimeout) are permitted.
// Producer.buildCBConfigFromMulti reads zero as "use the lib-commons HTTP
// preset"; rejecting it at validate() would break that path.
func TestConfig_Validate_CBPresetFallbackZerosAccepted(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig()
	cfg.CBMinRequests = 0
	cfg.CBTimeout = 0

	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() rejected preset-fallback zeros: %v", err)
	}
}

// TestConfig_Validate_IsCallerError pins that ErrInvalidConfigField walks
// the caller-error sentinel chain so consumers can errors.Is or
// streaming.IsCallerError to distinguish bootstrap config bugs from
// runtime infrastructure faults.
func TestConfig_Validate_IsCallerError(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig()
	cfg.CBFailureRatio = 2.5

	err := cfg.validate()
	if !errors.Is(err, ErrInvalidConfigField) {
		t.Fatalf("validate() error = %v; want ErrInvalidConfigField", err)
	}

	if !contract.IsCallerError(err) {
		t.Fatalf("IsCallerError(%v) = false; want true (ErrInvalidConfigField must walk the caller-error chain)", err)
	}
}
