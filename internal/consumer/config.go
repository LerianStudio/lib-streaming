package consumer

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-commons/v5/commons"

	"github.com/LerianStudio/lib-streaming/internal/kafkasec"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// Config validation sentinels. Callers use errors.Is to branch on a specific
// misconfiguration. They mirror the producer's ErrMissingBrokers /
// ErrInvalidConfigField naming so a service that already handles producer config
// errors needs no new error vocabulary.
var (
	// ErrMissingBrokers is returned when Enabled=true but no brokers are set.
	ErrMissingBrokers = errors.New("streaming consumer: at least one broker is required")
	// ErrMissingGroup is returned when Enabled=true but the group id is empty.
	ErrMissingGroup = errors.New("streaming consumer: consumer group id is required")
	// ErrMissingTopics is returned when Enabled=true but no topics are set.
	ErrMissingTopics = errors.New("streaming consumer: at least one topic is required")
	// ErrInvalidConfigField is returned for an out-of-range numeric/duration field.
	ErrInvalidConfigField = errors.New("streaming consumer: invalid config field")
	// ErrNilHandler is returned when Enabled=true but no handler was wired.
	ErrNilHandler = errors.New("streaming consumer: handler is required")
)

// ConsumerConfig is the full runtime configuration for a Consumer. It is the
// inbound symmetric counterpart to internal/config.Config (producer). Every
// field maps to a STREAMING_CONSUMER_* environment variable consumed by
// LoadConsumerConfig.
//
// ConsumerConfig is intentionally a SEPARATE shape from the producer Config:
// the producer's batching/acks/compression knobs are meaningless on the
// consume path, and the consumer needs group/poll/retry/DLQ knobs the
// producer has no use for. Sharing one struct would force every field to be
// optional-for-one-side and erase the validation contract.
type ConsumerConfig struct {
	// Enabled is the master kill switch. Default: false. When false,
	// NewConsumer returns a no-op Consumer whose Run blocks until ctx
	// cancellation and Close is a no-op.
	Enabled bool
	// Brokers is the Redpanda/Kafka bootstrap list. Required when
	// Enabled=true. STREAMING_CONSUMER_BROKERS (csv).
	Brokers []string
	// Group is the consumer group id. Required when Enabled=true.
	// STREAMING_CONSUMER_GROUP.
	Group string
	// Topics is the subscription list. Required when Enabled=true.
	// STREAMING_CONSUMER_TOPICS (csv).
	Topics []string
	// ClientID is the Kafka client.id for broker-side diagnostics.
	// STREAMING_CONSUMER_CLIENT_ID.
	ClientID string
	// RetryBudget is the number of IN-LOOP transient-failure retry attempts per
	// record, within a single poll cycle, to absorb a connection blip. It is NOT
	// "retries before DLQ": transients NEVER go to the DLQ (GAP 3). When the
	// in-loop budget is exhausted (a SUSTAINED transient) the runtime seeks back
	// and blocks the partition head-of-line. Default: 3.
	// STREAMING_CONSUMER_RETRY_BUDGET.
	RetryBudget int
	// RetryBackoffInitial is the first in-loop transient-retry backoff.
	// Subsequent retries grow this (capped at RetryBackoffMax). Default: 100ms.
	// STREAMING_CONSUMER_RETRY_BACKOFF_INITIAL_MS.
	RetryBackoffInitial time.Duration
	// RetryBackoffMax caps the per-attempt in-loop backoff. Default: 5s.
	// STREAMING_CONSUMER_RETRY_BACKOFF_MAX_MS.
	RetryBackoffMax time.Duration
	// RetryInLoopMaxDwell HARD-CAPS the AGGREGATE in-loop dwell per record (sum of
	// all in-loop attempts + backoffs). It MUST stay well below the consumer
	// group's rebalance/session timeout: the member holds BlockRebalanceOnPoll for
	// the life of the batch, so a slow in-loop retry risks the member being kicked
	// for exceeding the rebalance timeout (franz-go warns this exact mode,
	// config.go:1944-1953). This is the GAP-4 cap. Sustained transients are
	// absorbed by the CROSS-POLL HaltBackoff path (group unblocked), not in-loop.
	// Default: 1s. STREAMING_CONSUMER_RETRY_INLOOP_MAX_DWELL_MS.
	RetryInLoopMaxDwell time.Duration
	// HaltBackoff is the CROSS-POLL pause applied before re-polling when any
	// partition was halted (sustained transient seek-back), to avoid a hot spin
	// re-fetching the same uncommitted record. The group is UNBLOCKED during this
	// wait (AllowRebalance was called), so it may safely grow to seconds→minutes
	// for a slow downstream. Default: 250ms. STREAMING_CONSUMER_HALT_BACKOFF_MS.
	HaltBackoff time.Duration
	// PollTimeout caps a single PollFetches wait. Zero means block until
	// records or ctx cancellation. Default: 0.
	// STREAMING_CONSUMER_POLL_TIMEOUT_MS.
	PollTimeout time.Duration
	// CloseTimeout bounds graceful drain on Close. Default: 30s.
	// STREAMING_CONSUMER_CLOSE_TIMEOUT_S.
	CloseTimeout time.Duration
	// DLQTopicSuffix is appended to the source topic to derive the DLQ
	// topic (<topic><suffix>). Default: ".dlq". STREAMING_CONSUMER_DLQ_SUFFIX.
	DLQTopicSuffix string

	// tlsConfig / saslMechanism / allowPlaintextSASL mirror the producer's
	// transport-security plumbing. In wave 2 these move to a shared
	// internal/kafkasec package (see docs/design/consumer.md) so producer and
	// consumer validate TLS/SASL identically. For now the consumer holds them
	// here and the runtime will call the (wave-2) shared validators.
	tlsConfig          *tls.Config
	saslMechanism      sasl.Mechanism
	allowPlaintextSASL bool
}

// Default values applied by LoadConsumerConfig when a variable is unset.
const (
	defaultRetryBudget         = 3
	defaultRetryBackoffInitial = 100 * time.Millisecond
	defaultRetryBackoffMax     = 5 * time.Second
	defaultRetryInLoopMaxDwell = 1 * time.Second
	defaultHaltBackoff         = 250 * time.Millisecond
	defaultCloseTimeout        = 30 * time.Second
	// DefaultDLQTopicSuffix is the suffix appended to a source topic to derive its
	// DLQ topic when the caller omits one. Exported so the root builder
	// (NewConsumer) applies the identical default on the programmatic path that
	// LoadConsumerConfig applies on the env path — a blank suffix would derive
	// <topic><""> == the source topic and loop a terminal record forever.
	DefaultDLQTopicSuffix = ".dlq"

	// maxSafeRetryInLoopDwell caps RetryInLoopMaxDwell. The member holds
	// BlockRebalanceOnPoll for the life of the batch (config.go:1944-1953 warns this
	// exact mode), so the aggregate in-loop dwell must stay comfortably below the
	// group session timeout or the member is evicted mid-retry. ConsumerConfig has
	// no session-timeout field, so we bound against a conservative ceiling well
	// under franz-go's ~45s default session timeout. Sustained transients are meant
	// to fall through to the CROSS-POLL HaltBackoff path (group unblocked), not to
	// dwell longer in-loop.
	maxSafeRetryInLoopDwell = 30 * time.Second
)

// DefaultBuilderConfig returns an ENABLED ConsumerConfig with every non-required
// numeric/duration field set to the same default LoadConsumerConfig applies. The
// root builder (NewConsumer) seeds it so a minimal fluent build passes Validate;
// Brokers/Group/Topics/Handler remain the caller's responsibility.
func DefaultBuilderConfig() ConsumerConfig {
	return ConsumerConfig{
		Enabled:             true,
		RetryBudget:         defaultRetryBudget,
		RetryBackoffInitial: defaultRetryBackoffInitial,
		RetryBackoffMax:     defaultRetryBackoffMax,
		RetryInLoopMaxDwell: defaultRetryInLoopMaxDwell,
		HaltBackoff:         defaultHaltBackoff,
		CloseTimeout:        defaultCloseTimeout,
		DLQTopicSuffix:      DefaultDLQTopicSuffix,
	}
}

// LoadConsumerConfig reads every STREAMING_CONSUMER_* environment variable,
// applies defaults, and validates the result when Enabled=true.
//
// The second return value carries human-readable warnings; callers decide how
// to surface them. It is never nil. TLS/SASL are wired programmatically (via
// the builder's TLS/SASL setters), never from the environment — secrets do not
// belong in env-string config (matches the producer's TRD §8 security boundary).
func LoadConsumerConfig() (ConsumerConfig, []string, error) {
	warnings := make([]string, 0)
	enabled := commons.GetenvBoolOrDefault("STREAMING_CONSUMER_ENABLED", false)

	cfg := ConsumerConfig{
		Enabled:             enabled,
		Brokers:             splitCSV(commons.GetenvOrDefault("STREAMING_CONSUMER_BROKERS", "")),
		Group:               commons.GetenvOrDefault("STREAMING_CONSUMER_GROUP", ""),
		Topics:              splitCSV(commons.GetenvOrDefault("STREAMING_CONSUMER_TOPICS", "")),
		ClientID:            commons.GetenvOrDefault("STREAMING_CONSUMER_CLIENT_ID", ""),
		RetryBudget:         int(commons.GetenvIntOrDefault("STREAMING_CONSUMER_RETRY_BUDGET", defaultRetryBudget)),
		RetryBackoffInitial: getenvMsOrDefault("STREAMING_CONSUMER_RETRY_BACKOFF_INITIAL_MS", defaultRetryBackoffInitial),
		RetryBackoffMax:     getenvMsOrDefault("STREAMING_CONSUMER_RETRY_BACKOFF_MAX_MS", defaultRetryBackoffMax),
		RetryInLoopMaxDwell: getenvMsOrDefault("STREAMING_CONSUMER_RETRY_INLOOP_MAX_DWELL_MS", defaultRetryInLoopMaxDwell),
		HaltBackoff:         getenvMsOrDefault("STREAMING_CONSUMER_HALT_BACKOFF_MS", defaultHaltBackoff),
		PollTimeout:         getenvMsOrDefault("STREAMING_CONSUMER_POLL_TIMEOUT_MS", 0),
		CloseTimeout:        getenvSecOrDefault("STREAMING_CONSUMER_CLOSE_TIMEOUT_S", defaultCloseTimeout),
		DLQTopicSuffix:      commons.GetenvOrDefault("STREAMING_CONSUMER_DLQ_SUFFIX", DefaultDLQTopicSuffix),
	}

	// GetenvOrDefault only substitutes the default when the var is UNSET; an env
	// var explicitly set to "" slips through as a blank suffix. Re-apply the
	// default so <topic><suffix> never collides with the source topic.
	if cfg.DLQTopicSuffix == "" {
		cfg.DLQTopicSuffix = DefaultDLQTopicSuffix
	}

	if !cfg.Enabled {
		return cfg, warnings, nil
	}

	if err := cfg.Validate(); err != nil {
		return cfg, warnings, err
	}

	return cfg, warnings, nil
}

// Validate enforces the fields required when Enabled=true. Returns the first
// failure; callers use errors.Is. A disabled config is always valid (the
// builder returns a no-op consumer before reaching here).
func (c ConsumerConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	if len(c.Brokers) == 0 {
		return ErrMissingBrokers
	}

	if c.Group == "" {
		return ErrMissingGroup
	}

	if len(c.Topics) == 0 {
		return ErrMissingTopics
	}

	if c.RetryBudget < 0 {
		return fmt.Errorf("%w: RetryBudget=%d (must be >= 0)", ErrInvalidConfigField, c.RetryBudget)
	}

	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"RetryBackoffInitial", c.RetryBackoffInitial},
		{"RetryBackoffMax", c.RetryBackoffMax},
		{"RetryInLoopMaxDwell", c.RetryInLoopMaxDwell},
		{"CloseTimeout", c.CloseTimeout},
	} {
		if d.value <= 0 {
			return fmt.Errorf("%w: %s=%s (must be positive)", ErrInvalidConfigField, d.name, d.value)
		}
	}

	// Bound the aggregate in-loop dwell below the group rebalance/session window:
	// it holds BlockRebalanceOnPoll for the batch, so an over-long dwell gets the
	// member evicted mid-retry. See maxSafeRetryInLoopDwell.
	if c.RetryInLoopMaxDwell > maxSafeRetryInLoopDwell {
		return fmt.Errorf("%w: RetryInLoopMaxDwell=%s exceeds the safe ceiling %s (holds BlockRebalanceOnPoll; would risk rebalance-timeout eviction mid-retry)", ErrInvalidConfigField, c.RetryInLoopMaxDwell, maxSafeRetryInLoopDwell)
	}

	// HaltBackoff and PollTimeout may be zero (zero PollTimeout = block; zero
	// HaltBackoff = re-poll immediately), but never negative.
	if c.HaltBackoff < 0 {
		return fmt.Errorf("%w: HaltBackoff=%s (must be >= 0)", ErrInvalidConfigField, c.HaltBackoff)
	}

	if c.PollTimeout < 0 {
		return fmt.Errorf("%w: PollTimeout=%s (must be >= 0)", ErrInvalidConfigField, c.PollTimeout)
	}

	// A whitespace-only suffix trims to empty -> <topic><suffix> collides with the
	// source topic and a terminal record loops back into the SUBSCRIBED stream
	// instead of quarantining. An EMPTY suffix is defaulted to DefaultDLQTopicSuffix
	// upstream (NewConsumer / LoadConsumerConfig); a non-empty-but-blank one is a
	// caller mistake we reject rather than silently default over.
	if c.DLQTopicSuffix != "" && strings.TrimSpace(c.DLQTopicSuffix) == "" {
		return fmt.Errorf("%w: DLQTopicSuffix is whitespace-only (would collide with the source topic)", ErrInvalidConfigField)
	}

	// Transport-security gate (shared with the producer via internal/kafkasec):
	// reject a weakening TLS config and SASL-without-TLS unless explicitly opted
	// into plaintext. SASL credentials must never cross the network in cleartext.
	if err := kafkasec.ValidateTLSConfig(c.tlsConfig); err != nil {
		return err
	}

	hasSASL := !transport.IsNilInterface(c.saslMechanism)

	return kafkasec.SASLRequiresTLS(hasSASL, c.tlsConfig != nil, c.allowPlaintextSASL)
}

// getenvMsOrDefault reads a millisecond-valued env var, falling back to def on
// absence or a non-integer value (lenient — config loading never panics).
func getenvMsOrDefault(key string, def time.Duration) time.Duration {
	return time.Duration(commons.GetenvIntOrDefault(key, def.Milliseconds())) * time.Millisecond
}

// getenvSecOrDefault reads a second-valued env var, falling back to def.
func getenvSecOrDefault(key string, def time.Duration) time.Duration {
	return time.Duration(commons.GetenvIntOrDefault(key, int64(def.Seconds()))) * time.Second
}

// splitCSV splits a comma-separated list and trims whitespace, dropping empty
// entries. A fully-empty input yields an empty (non-nil) slice. Mirrors
// internal/config.splitCSV — duplicated rather than exported across the package
// boundary because it is three lines and config is a sibling internal package.
func splitCSV(s string) []string {
	result := make([]string, 0)

	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			result = append(result, p)
		}
	}

	return result
}

// WithTLSConfig sets the validated TLS config used for broker dials.
func (c ConsumerConfig) WithTLSConfig(cfg *tls.Config) ConsumerConfig {
	c.tlsConfig = cfg
	return c
}

// WithSASL sets the SASL mechanism. SASL requires TLS unless AllowPlaintextSASL.
func (c ConsumerConfig) WithSASL(m sasl.Mechanism) ConsumerConfig {
	c.saslMechanism = m
	return c
}

// WithAllowPlaintextSASL permits SASL without TLS for local/dev only.
func (c ConsumerConfig) WithAllowPlaintextSASL() ConsumerConfig {
	c.allowPlaintextSASL = true
	return c
}
