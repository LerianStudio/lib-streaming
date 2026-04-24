package streaming

import (
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons"
)

// Config is the full runtime configuration for a Producer. Every field maps
// to a STREAMING_* environment variable consumed by LoadConfig.
//
// When Enabled is false (or Brokers is empty), New returns a NoopEmitter —
// calls succeed silently. This is the fail-safe for services that cannot
// reach a broker in their current environment.
type Config struct {
	// Enabled is the master kill switch. Default: false.
	Enabled bool
	// Brokers is the Redpanda bootstrap list. Default: ["localhost:9092"].
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
	defaultBroker                = "localhost:9092"
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
// Errors: ErrMissingBrokers, ErrMissingSource, ErrInvalidCompression,
// ErrInvalidAcks. Each wraps with fmt.Errorf so callers can errors.Is.
func LoadConfig() (Config, error) {
	brokers := splitCSV(commons.GetenvOrDefault("STREAMING_BROKERS", defaultBroker))
	enabled := commons.GetenvBoolOrDefault("STREAMING_ENABLED", false)

	policyOverrides, err := parseEventPolicies(commons.GetenvOrDefault("STREAMING_EVENT_POLICIES", ""))
	if err != nil {
		if enabled {
			return Config{}, err
		}

		policyOverrides = map[string]DeliveryPolicyOverride{}
	}

	cfg := Config{
		Enabled:               enabled,
		Brokers:               brokers,
		ClientID:              commons.GetenvOrDefault("STREAMING_CLIENT_ID", ""),
		BatchLingerMs:         int(commons.GetenvIntOrDefault("STREAMING_BATCH_LINGER_MS", int64(defaultBatchLingerMs))),
		BatchMaxBytes:         int(commons.GetenvIntOrDefault("STREAMING_BATCH_MAX_BYTES", int64(defaultBatchMaxBytes))),
		MaxBufferedRecords:    int(commons.GetenvIntOrDefault("STREAMING_MAX_BUFFERED_RECORDS", int64(defaultMaxBufferedRecords))),
		Compression:           commons.GetenvOrDefault("STREAMING_COMPRESSION", defaultCompression),
		RecordRetries:         int(commons.GetenvIntOrDefault("STREAMING_RECORD_RETRIES", int64(defaultRecordRetries))),
		RecordDeliveryTimeout: time.Duration(commons.GetenvIntOrDefault("STREAMING_RECORD_DELIVERY_TIMEOUT_S", int64(defaultRecordDeliveryTimeout.Seconds()))) * time.Second,
		RequiredAcks:          commons.GetenvOrDefault("STREAMING_REQUIRED_ACKS", defaultRequiredAcks),
		CBFailureRatio:        commons.GetenvFloat64OrDefault("STREAMING_CB_FAILURE_RATIO", defaultCBFailureRatio),
		CBMinRequests:         int(commons.GetenvIntOrDefault("STREAMING_CB_MIN_REQUESTS", int64(defaultCBMinRequests))),
		CBTimeout:             time.Duration(commons.GetenvIntOrDefault("STREAMING_CB_TIMEOUT_S", int64(defaultCBTimeout.Seconds()))) * time.Second,
		CloseTimeout:          time.Duration(commons.GetenvIntOrDefault("STREAMING_CLOSE_TIMEOUT_S", int64(defaultCloseTimeout.Seconds()))) * time.Second,
		CloudEventsSource:     commons.GetenvOrDefault("STREAMING_CLOUDEVENTS_SOURCE", ""),
		PolicyOverrides:       policyOverrides,
	}

	if !cfg.Enabled {
		return cfg, nil
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// validate enforces the fields that must be present when Enabled=true.
// Returns the first failure encountered — callers use errors.Is to match.
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

	return nil
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

// parseEventPolicies parses STREAMING_EVENT_POLICIES entries in the form:
//
//	transaction.created.enabled=true,transaction.created.outbox=always
//
// Entries may be separated by commas, semicolons, or newlines. Unknown
// attributes and unsupported values return ErrInvalidDeliveryPolicy so policy
// typos do not silently change runtime behavior.
func parseEventPolicies(s string) (map[string]DeliveryPolicyOverride, error) {
	result := map[string]DeliveryPolicyOverride{}
	if strings.TrimSpace(s) == "" {
		return result, nil
	}

	for _, entry := range splitPolicyEntries(s) {
		keyAttr, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("%w: malformed policy entry %q", ErrInvalidDeliveryPolicy, entry)
		}

		key, attr, ok := cutLastDot(strings.TrimSpace(keyAttr))
		if !ok || key == "" || attr == "" {
			return nil, fmt.Errorf("%w: malformed policy key %q", ErrInvalidDeliveryPolicy, keyAttr)
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

		if err := override.Validate(); err != nil {
			return nil, err
		}

		result[key] = override
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
