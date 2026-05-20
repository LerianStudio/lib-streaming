package config

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons"
	"github.com/LerianStudio/lib-observability/assert"
	"github.com/LerianStudio/lib-observability/log"
)

// configAsserterComponent matches internal/producer.asserterComponent so
// invariant violations surfaced from Config.validate aggregate under the
// same component axis as runtime invariants.
const configAsserterComponent = "streaming"

// newConfigAsserter constructs an *assert.Asserter for a per-call-site
// operation. The config package has no caller-supplied logger today —
// LoadConfig runs before structured logging is initialized — so we use a
// no-op logger here. The asserter's metric layer (assertion_failed_total)
// still fires after the consuming service calls
// assert.InitAssertionMetrics at bootstrap; the log layer is intentionally
// no-op at this site.
//
// Each NotNil/That/InRange call passes its own ctx (typically
// context.Background() at bootstrap) which takes precedence over the
// fallback ctx wired here.
func newConfigAsserter(operation string) *assert.Asserter {
	return assert.New(context.Background(), log.NewNop(), configAsserterComponent, operation)
}

// Config is the full runtime configuration for a Producer. Every field maps
// to a STREAMING_* environment variable consumed by LoadConfig.
//
// When Enabled is false, New returns a NoopEmitter — calls succeed silently.
// Enabled configs with an empty broker list fail validation with
// ErrMissingBrokers so operator mistakes do not silently disable publishing.
type Config struct {
	// Enabled is the master kill switch. Default: false.
	Enabled bool
	// Brokers is the Redpanda bootstrap list. Default: empty.
	// Required when Enabled=true.
	Brokers []string
	// ClientID is the Kafka client.id used for broker-side diagnostics.
	// Default: hostname-derived.
	ClientID string
	// BatchLingerMs pins franz-go's ProducerLinger to counter the v1.17→v1.20
	// default flip. Default: 5ms.
	BatchLingerMs int
	// BatchMaxBytes caps ProducerBatchMaxBytes. Default: 1 MiB.
	BatchMaxBytes int
	// MaxBufferedRecords is the backpressure ceiling. Default: 10000.
	MaxBufferedRecords int
	// Compression is one of snappy, lz4, zstd, gzip, none. Default: lz4.
	Compression string
	// RecordRetries is franz-go's per-record retry budget. Default: 10.
	RecordRetries int
	// RecordDeliveryTimeout is the per-record delivery cap. Default: 30s.
	RecordDeliveryTimeout time.Duration
	// RequiredAcks is one of all, leader, none. Default: all.
	RequiredAcks string
	// CBFailureRatio is the circuit-breaker trip threshold in (0.0, 1.0].
	// Default: 0.5.
	CBFailureRatio float64
	// CBMinRequests is the minimum observations before evaluating the ratio.
	// Default: 10.
	CBMinRequests int
	// CBTimeout is the open→half-open probe delay. Default: 30s.
	CBTimeout time.Duration
	// CloseTimeout is the max drain+flush window on Close. Default: 30s.
	CloseTimeout time.Duration
	// CloudEventsSource is the ce-source default (required when Enabled=true).
	CloudEventsSource string
	// PolicyOverrides is a map of event definition key -> delivery policy
	// override. Parsed from STREAMING_EVENT_POLICIES.
	PolicyOverrides map[string]DeliveryPolicyOverride
}

// Default values used by LoadConfig when an environment variable is unset.
// Exported-looking constants but unexported — callers go through LoadConfig.
const (
	defaultBatchLingerMs         = 5
	defaultBatchMaxBytes         = 1_048_576
	defaultMaxBufferedRecords    = 10_000
	defaultCompression           = "lz4"
	defaultRecordRetries         = 10
	defaultRecordDeliveryTimeout = 30 * time.Second
	defaultRequiredAcks          = "all"
	defaultCBFailureRatio        = 0.5
	defaultCBMinRequests         = 10
	defaultCBTimeout             = 30 * time.Second
	defaultCloseTimeout          = 30 * time.Second
)

// validCompressionCodecs enumerates the accepted STREAMING_COMPRESSION values.
var validCompressionCodecs = map[string]struct{}{
	"snappy": {},
	"lz4":    {},
	"zstd":   {},
	"gzip":   {},
	"none":   {},
}

// validAcks enumerates the accepted STREAMING_REQUIRED_ACKS values.
var validAcks = map[string]struct{}{
	"all":    {},
	"leader": {},
	"none":   {},
}

// LoadConfig reads every STREAMING_* environment variable, applies defaults
// for missing values, and validates the result.
//
// When Enabled=false, validation is skipped so a disabled config always
// loads clean (the Producer will return a NoopEmitter).
//
// The second return value is a slice of human-readable migration warnings.
// LoadConfig does NOT write to stderr or use a logger — callers decide how
// to surface these (log at startup, skip on test paths, etc.). The slice is
// never nil (empty when there are no warnings) so callers can range-for
// without a nil check.
//
// Errors: ErrMissingBrokers, ErrMissingSource, ErrInvalidCompression,
// ErrInvalidAcks, ErrInvalidConfigField. Each wraps with fmt.Errorf where
// context is added so callers can errors.Is.
func LoadConfig() (Config, []string, error) {
	warnings := make([]string, 0)

	brokers := splitCSV(commons.GetenvOrDefault("STREAMING_BROKERS", ""))
	enabled := commons.GetenvBoolOrDefault("STREAMING_ENABLED", false)

	batchLingerMs, err := getenvIntOrDefaultStrict("STREAMING_BATCH_LINGER_MS", defaultBatchLingerMs, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	batchMaxBytes, err := getenvIntOrDefaultStrict("STREAMING_BATCH_MAX_BYTES", defaultBatchMaxBytes, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	maxBufferedRecords, err := getenvIntOrDefaultStrict("STREAMING_MAX_BUFFERED_RECORDS", defaultMaxBufferedRecords, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	recordRetries, err := getenvIntOrDefaultStrict("STREAMING_RECORD_RETRIES", defaultRecordRetries, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	recordDeliveryTimeoutS, err := getenvIntOrDefaultStrict("STREAMING_RECORD_DELIVERY_TIMEOUT_S", int(defaultRecordDeliveryTimeout.Seconds()), enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	cbFailureRatio, err := getenvFloat64OrDefaultStrict("STREAMING_CB_FAILURE_RATIO", defaultCBFailureRatio, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	cbMinRequests, err := getenvIntOrDefaultStrict("STREAMING_CB_MIN_REQUESTS", defaultCBMinRequests, enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	cbTimeoutS, err := getenvIntOrDefaultStrict("STREAMING_CB_TIMEOUT_S", int(defaultCBTimeout.Seconds()), enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	closeTimeoutS, err := getenvIntOrDefaultStrict("STREAMING_CLOSE_TIMEOUT_S", int(defaultCloseTimeout.Seconds()), enabled)
	if err != nil {
		return Config{}, warnings, err
	}

	policyOverrides, err := parseEventPolicies(commons.GetenvOrDefault("STREAMING_EVENT_POLICIES", ""))
	if err != nil {
		if enabled {
			return Config{}, warnings, err
		}

		policyOverrides = map[string]DeliveryPolicyOverride{}
	}

	cfg := Config{
		Enabled:               enabled,
		Brokers:               brokers,
		ClientID:              commons.GetenvOrDefault("STREAMING_CLIENT_ID", ""),
		BatchLingerMs:         batchLingerMs,
		BatchMaxBytes:         batchMaxBytes,
		MaxBufferedRecords:    maxBufferedRecords,
		Compression:           commons.GetenvOrDefault("STREAMING_COMPRESSION", defaultCompression),
		RecordRetries:         recordRetries,
		RecordDeliveryTimeout: time.Duration(recordDeliveryTimeoutS) * time.Second,
		RequiredAcks:          commons.GetenvOrDefault("STREAMING_REQUIRED_ACKS", defaultRequiredAcks),
		CBFailureRatio:        cbFailureRatio,
		CBMinRequests:         cbMinRequests,
		CBTimeout:             time.Duration(cbTimeoutS) * time.Second,
		CloseTimeout:          time.Duration(closeTimeoutS) * time.Second,
		CloudEventsSource:     commons.GetenvOrDefault("STREAMING_CLOUDEVENTS_SOURCE", ""),
		PolicyOverrides:       policyOverrides,
	}

	if !cfg.Enabled {
		return cfg, warnings, nil
	}

	if err := cfg.validate(); err != nil {
		return cfg, warnings, err
	}

	return cfg, warnings, nil
}

// validate enforces the fields that must be present when Enabled=true.
// Returns the first failure encountered — callers use errors.Is to match.
//
// Range-validation contract for the numeric/duration fields:
//
//   - CBFailureRatio: documented as (0.0, 1.0]. Zero is treated as "use the
//     lib-commons HTTP preset" by producer_multi.buildCBConfigFromMulti, so
//     zero is permitted and skipped here. Nonzero values outside the range
//     are rejected with ErrInvalidConfigField.
//   - BatchLingerMs / RecordRetries / CBMinRequests / CBTimeout: zero is
//     accepted because the producer's preset-fallback path treats zero as
//     "use the documented default". Negative values are rejected.
//   - BatchMaxBytes / MaxBufferedRecords / RecordDeliveryTimeout /
//     CloseTimeout: zero or negative is rejected — these have no meaningful
//     "use default" path through the construction code, so zero would flow
//     into franz-go and surface as a confusing transport error rather than
//     a config-validation rejection at bootstrap.
//
// Each rejection fires the asserter trident
// (assertion_failed_total{component="streaming",operation="config.validate"})
// before returning the wrapped sentinel.
func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return ErrMissingBrokers
	}

	if c.CloudEventsSource == "" {
		return ErrMissingSource
	}

	if _, ok := validCompressionCodecs[c.Compression]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidCompression, c.Compression)
	}

	if _, ok := validAcks[c.RequiredAcks]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidAcks, c.RequiredAcks)
	}

	return c.validateRanges()
}

// validateRanges enforces the numeric/duration field contracts. Split out
// from validate() so the range-check surface stays cohesive and the
// asserter call sites cluster in one place.
func (c Config) validateRanges() error {
	ctx := context.Background()
	a := newConfigAsserter("config.validate")

	// CBFailureRatio: (0.0, 1.0] when nonzero. Zero falls through to the
	// HTTP preset; nonzero values must lie strictly above 0.0 and at most
	// 1.0. STREAMING_CB_FAILURE_RATIO=2.5 silently breaks gobreaker
	// semantics today; gate it here so the misconfiguration fails closed
	// at bootstrap.
	if c.CBFailureRatio != 0 {
		ok := c.CBFailureRatio > 0 && c.CBFailureRatio <= 1.0
		_ = a.That(ctx, ok, "CBFailureRatio must be in (0.0, 1.0] when nonzero",
			"field", "CBFailureRatio",
			"value", c.CBFailureRatio,
		)

		if !ok {
			return fmt.Errorf("%w: CBFailureRatio=%g (must be in (0.0, 1.0])", ErrInvalidConfigField, c.CBFailureRatio)
		}
	}

	// Non-negative checks: zero permitted (preset fallback).
	for _, check := range []struct {
		field string
		value int
	}{
		{"BatchLingerMs", c.BatchLingerMs},
		{"RecordRetries", c.RecordRetries},
		{"CBMinRequests", c.CBMinRequests},
	} {
		ok := check.value >= 0
		_ = a.That(ctx, ok, "config field must be non-negative",
			"field", check.field,
			"value", check.value,
		)

		if !ok {
			return fmt.Errorf("%w: %s=%d (must be non-negative)", ErrInvalidConfigField, check.field, check.value)
		}
	}

	// Duration non-negative: zero permitted (preset fallback).
	{
		ok := c.CBTimeout >= 0
		_ = a.That(ctx, ok, "CBTimeout must be non-negative",
			"field", "CBTimeout",
			"value", c.CBTimeout,
		)

		if !ok {
			return fmt.Errorf("%w: CBTimeout=%s (must be non-negative)", ErrInvalidConfigField, c.CBTimeout)
		}
	}

	// Strictly positive integer counts: zero/negative would cap throughput
	// or buffering at zero with no meaningful "use default" path.
	for _, check := range []struct {
		field string
		value int
	}{
		{"BatchMaxBytes", c.BatchMaxBytes},
		{"MaxBufferedRecords", c.MaxBufferedRecords},
	} {
		ok := check.value > 0
		_ = a.That(ctx, ok, "config field must be positive",
			"field", check.field,
			"value", check.value,
		)

		if !ok {
			return fmt.Errorf("%w: %s=%d (must be positive)", ErrInvalidConfigField, check.field, check.value)
		}
	}

	// Strictly positive durations.
	for _, check := range []struct {
		field string
		value time.Duration
	}{
		{"RecordDeliveryTimeout", c.RecordDeliveryTimeout},
		{"CloseTimeout", c.CloseTimeout},
	} {
		ok := check.value > 0
		_ = a.That(ctx, ok, "config duration must be positive",
			"field", check.field,
			"value", check.value,
		)

		if !ok {
			return fmt.Errorf("%w: %s=%s (must be positive)", ErrInvalidConfigField, check.field, check.value)
		}
	}

	return nil
}

// Validate enforces the fields that must be present when Enabled=true.
func (c Config) Validate() error {
	return c.validate()
}

func getenvIntOrDefaultStrict(key string, defaultValue int, strict bool) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}

	v, err := strconv.ParseInt(raw, 10, 0)
	if err != nil {
		if strict {
			return 0, fmt.Errorf("%w: %s=%q must be an integer", ErrInvalidConfigField, key, raw)
		}

		return defaultValue, nil
	}

	return int(v), nil
}

func getenvFloat64OrDefaultStrict(key string, defaultValue float64, strict bool) (float64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}

	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		if strict {
			return 0, fmt.Errorf("%w: %s=%q must be a float", ErrInvalidConfigField, key, raw)
		}

		return defaultValue, nil
	}

	return v, nil
}

// splitCSV splits a comma-separated broker list and trims whitespace. Empty
// entries are dropped. A fully-empty input yields an empty slice (not nil).
func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{}
	}

	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}

	return result
}

// maxEventPolicyEntries caps the number of entries parseEventPolicies will
// accept from a single STREAMING_EVENT_POLICIES string. A hostile env var
// (e.g. 10 MB of "a.b=c,...") would otherwise build an unbounded map.
const maxEventPolicyEntries = 1024

// maxEventPolicyKeyBytes caps the length of each policy key (the left side
// of "=", minus the trailing ".attr"). 256 matches maxEventIDBytes used
// elsewhere for header-bound identifiers.
const maxEventPolicyKeyBytes = 256

// parseEventPolicies parses STREAMING_EVENT_POLICIES entries in the form:
//
//	transaction.created.enabled=true,transaction.created.outbox=always
//
// Entries may be separated by commas, semicolons, or newlines. Unknown
// attributes and unsupported values return ErrInvalidDeliveryPolicy so policy
// typos do not silently change runtime behavior.
//
// Hostile-env-var guards: entries are capped at maxEventPolicyEntries
// (1024) and keys at maxEventPolicyKeyBytes (256) to bound memory for a
// malicious or accidentally-huge env var.
func parseEventPolicies(s string) (map[string]DeliveryPolicyOverride, error) {
	result := map[string]DeliveryPolicyOverride{}
	if strings.TrimSpace(s) == "" {
		return result, nil
	}

	entries := splitPolicyEntries(s)
	if len(entries) > maxEventPolicyEntries {
		return nil, fmt.Errorf("%w: STREAMING_EVENT_POLICIES has %d entries (max %d)",
			ErrInvalidDeliveryPolicy, len(entries), maxEventPolicyEntries)
	}

	for _, entry := range entries {
		keyAttr, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("%w: malformed policy entry %q", ErrInvalidDeliveryPolicy, entry)
		}

		key, attr, ok := cutLastDot(strings.TrimSpace(keyAttr))
		if !ok || key == "" || attr == "" {
			return nil, fmt.Errorf("%w: malformed policy key %q", ErrInvalidDeliveryPolicy, keyAttr)
		}

		if len(key) > maxEventPolicyKeyBytes {
			return nil, fmt.Errorf("%w: STREAMING_EVENT_POLICIES key exceeds %d bytes: %d",
				ErrInvalidDeliveryPolicy, maxEventPolicyKeyBytes, len(key))
		}

		override := result[key]
		value = strings.TrimSpace(value)

		switch strings.ToLower(strings.TrimSpace(attr)) {
		case "enabled":
			enabled, ok := parsePolicyBool(value)
			if !ok {
				return nil, fmt.Errorf("%w: %s.%s=%q", ErrInvalidDeliveryPolicy, key, attr, value)
			}

			override.Enabled = &enabled
		case "direct":
			override.Direct = DirectMode(value)
		case "outbox":
			override.Outbox = OutboxMode(value)
		case "dlq":
			override.DLQ = DLQMode(value)
		default:
			return nil, fmt.Errorf("%w: unknown policy attribute %q", ErrInvalidDeliveryPolicy, attr)
		}

		result[key] = override
	}

	// Validate each fully-assembled override AFTER all tokens for that key are
	// parsed. Validating per-token rejects order-dependent valid combinations
	// (e.g. direct=skip,outbox=always fails if direct is seen first because
	// the cross-field rule requires outbox=always).
	for key, override := range result {
		if err := override.Validate(); err != nil {
			return nil, fmt.Errorf("key %q: %w", key, err)
		}
	}

	return result, nil
}

func splitPolicyEntries(s string) []string {
	s = strings.NewReplacer(";", ",", "\n", ",").Replace(s)

	entries := strings.Split(s, ",")

	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry != "" {
			result = append(result, entry)
		}
	}

	return result
}

func parsePolicyBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes":
		return true, true
	case "false", "0", "no":
		return false, true
	default:
		return false, false
	}
}

func cutLastDot(s string) (string, string, bool) {
	i := strings.LastIndex(s, ".")
	if i < 0 {
		return "", "", false
	}

	return s[:i], s[i+1:], true
}
