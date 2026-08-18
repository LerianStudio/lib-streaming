package consumer

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-commons/v6/commons"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/kafkasec"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
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
	// ErrMissingTopics is returned when Enabled=true but neither Topics nor
	// Apps resolves to a subscription.
	ErrMissingTopics = errors.New("streaming consumer: at least one topic or app is required")
	// ErrInvalidConfigField is returned for an out-of-range numeric/duration field.
	ErrInvalidConfigField = errors.New("streaming consumer: invalid config field")
	// ErrNilHandler is returned when Enabled=true but no handler was wired.
	ErrNilHandler = errors.New("streaming consumer: handler is required")
	// ErrMissingConsumerSource is returned when Enabled=true but the consuming
	// application has no ce-source identity. It is REQUIRED, not optional: it
	// names the consumer's own DLQ topic, and every enabled consumer has a DLQ
	// path. Without it a terminal record has nowhere to quarantine.
	ErrMissingConsumerSource = errors.New(
		"streaming consumer: ce-source identity is required — set STREAMING_CLOUDEVENTS_SOURCE or call ConsumerBuilder.Source(...); it names this application's own DLQ topic (lerian.streaming.<source>.dlq)")
)

// Builder-shape sentinels. Every one of these is a wiring mistake the builder
// can prove at Build time, and each says exactly which combination is
// contradictory — a shared "invalid configuration" error would leave the
// adopter guessing which of two knobs to remove.
//
// They live here rather than at the root facade so the root can alias them and
// internal/consumer stays the single owner of the consumer error vocabulary.
var (
	// ErrHandlerAndDispatchBothSet is returned when a consumer wires both
	// Handler (whole-stream) and On (per-event dispatch). They are two
	// different answers to "who selects events", and silently preferring one
	// would drop the other's handlers without a word.
	ErrHandlerAndDispatchBothSet = errors.New(
		"streaming consumer: Handler(...) and On(...) are mutually exclusive — use On for per-event dispatch, Handler for the raw stream")

	// ErrBareOnWithMultipleApps is returned when a consumer subscribed to more
	// than one producing application registers a handler with a bare
	// On(eventKey, ...). With two producers in scope the key alone does not say
	// whose event it is — and two apps publishing the same event name is the
	// normal case, not a corner one. Binding to whichever record arrived would
	// hand one app's payload to the other app's handler, silently.
	ErrBareOnWithMultipleApps = errors.New(
		"streaming consumer: On(...) is ambiguous when the consumer subscribes to more than one producing application — register with OnFrom(app, eventKey, handler)")

	// ErrUnknownDispatchApp is returned when OnFrom names an application the
	// consumer does not accept. That handler could never receive a record, so
	// it is a wiring mistake, not a filter.
	ErrUnknownDispatchApp = errors.New(
		"streaming consumer: OnFrom(...) names an application this consumer does not subscribe to — no record could ever reach that handler")

	// ErrHandlerAndUnmatchedPolicyBothSet is returned when a whole-stream
	// Handler is combined with UnmatchedPolicy. The policy decides what the
	// DISPATCHER does with a key it has no handler for; a raw Handler receives
	// every record and selects for itself, so the knob would be silently inert
	// — and an operator who set it believes unknown keys are being quarantined.
	ErrHandlerAndUnmatchedPolicyBothSet = errors.New(
		"streaming consumer: UnmatchedPolicy(...) applies to On(...) dispatch only — a whole-stream Handler(...) receives every record and selects for itself")

	// ErrAmbiguousSourceVerification is returned when Apps(...) and Topics(...)
	// are BOTH set and no explicit ExpectSources(...) was given. Defaulting the
	// allowlist to Apps would quarantine 100% of the raw Topics stream, whose
	// producers were never named; refusing to verify would silently drop the
	// check the Apps subscription paid for. The adopter has to say which.
	ErrAmbiguousSourceVerification = errors.New(
		"streaming consumer: Apps(...) and Topics(...) are both set — state ExpectSources(...) explicitly, since the Apps-derived allowlist would quarantine every record from the raw topics")

	// ErrExpectSourcesMissingApp is returned when an explicit ExpectSources
	// list omits an application named in Apps. Subscribing to an app's topic
	// while refusing its ce-source quarantines that whole stream, which is
	// always a bug.
	ErrExpectSourcesMissingApp = errors.New(
		"streaming consumer: ExpectSources(...) omits an app named in Apps(...) — its entire stream would be quarantined")

	// ErrInvalidExpectSource is returned when an ExpectSources entry is not a
	// legal ce-source. A hyphen/underscore typo there matches nothing, so the
	// consumer quarantines 100% of its stream while reporting healthy.
	ErrInvalidExpectSource = errors.New(
		"streaming consumer: ExpectSources(...) entry is not a legal ce-source")
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
	// Source is THIS application's ce-source — the same identity its producer
	// side publishes under, read from the same STREAMING_CLOUDEVENTS_SOURCE
	// variable, because one service has one identity.
	//
	// It is REQUIRED when Enabled=true, and it does one job: it names the
	// consumer's OWN dead-letter topic, "lerian.streaming.<Source>.dlq". A
	// consumer quarantines into its own DLQ, never the producer's — so a Kafka
	// ACL grants every application exactly two writes (its topic and its .dlq)
	// whether it produces, consumes, or both, and a filling DLQ names the team
	// that owns the fix.
	//
	// Held to the same strict source contract the producer enforces
	// (contract.ValidateSource): one dot-free lowercase segment.
	Source string
	// Topics is the RAW subscription list — an escape hatch for topics this
	// library did not derive (legacy streams, third-party producers).
	// STREAMING_CONSUMER_TOPICS (csv).
	//
	// Either Topics or Apps must be non-empty when Enabled=true; they compose.
	Topics []string
	// Apps names the PRODUCING APPLICATIONS to subscribe to, by ce-source.
	// Each resolves to that application's one topic
	// ("lerian.streaming.<app>"), so a consumer never hardcodes the topic
	// derivation. STREAMING_CONSUMER_APPS (csv).
	//
	// Naming apps here also feeds the consumer's built-in source
	// verification: the builder wires them as the Dispatcher's expected
	// producers, so an event carrying a foreign ce-source is quarantined
	// instead of dispatched. Each entry is held to the same strict source
	// contract the producer enforces (contract.ValidateSource) — a name no
	// producer could legally publish under would otherwise subscribe to a
	// topic that stays empty forever while the consumer reports healthy.
	Apps []string
	// ExpectSources is the RESOLVED ce-source allowlist the runtime verifies
	// every record against, before either handler mode is invoked. An empty
	// list means verification is off (the raw Topics escape hatch, whose
	// producers were never named).
	//
	// On the env surface (STREAMING_CONSUMER_EXPECT_SOURCES, csv) it declares
	// the allowlist explicitly, for the shapes Apps alone cannot express: it
	// REPLACES the allowlist Apps would have implied, must COVER every entry in
	// Apps, and every entry is held to the same strict source contract the
	// producer enforces. Its reason to exist: setting BOTH Apps and Topics
	// without an explicit allowlist is a hard Build failure (neither defaulting
	// to Apps — which quarantines the whole raw-topics stream — nor skipping the
	// check is a defensible guess), and without this variable that shape had no
	// env-only resolution at all.
	//
	// It applies in BOTH handler modes. A whole-stream Handler(...) needs it
	// most: it sees every record on a topic whose write ACL it does not own.
	// ConsumerBuilder.ExpectSources(...) called on the builder overrides it.
	ExpectSources []string
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
		Source:              commons.GetenvOrDefault("STREAMING_CLOUDEVENTS_SOURCE", ""),
		Topics:              splitCSV(commons.GetenvOrDefault("STREAMING_CONSUMER_TOPICS", "")),
		Apps:                splitCSV(commons.GetenvOrDefault("STREAMING_CONSUMER_APPS", "")),
		ExpectSources:       splitCSV(commons.GetenvOrDefault("STREAMING_CONSUMER_EXPECT_SOURCES", "")),
		ClientID:            commons.GetenvOrDefault("STREAMING_CONSUMER_CLIENT_ID", ""),
		RetryBudget:         int(commons.GetenvIntOrDefault("STREAMING_CONSUMER_RETRY_BUDGET", defaultRetryBudget)),
		RetryBackoffInitial: getenvMsOrDefault("STREAMING_CONSUMER_RETRY_BACKOFF_INITIAL_MS", defaultRetryBackoffInitial),
		RetryBackoffMax:     getenvMsOrDefault("STREAMING_CONSUMER_RETRY_BACKOFF_MAX_MS", defaultRetryBackoffMax),
		RetryInLoopMaxDwell: getenvMsOrDefault("STREAMING_CONSUMER_RETRY_INLOOP_MAX_DWELL_MS", defaultRetryInLoopMaxDwell),
		HaltBackoff:         getenvMsOrDefault("STREAMING_CONSUMER_HALT_BACKOFF_MS", defaultHaltBackoff),
		PollTimeout:         getenvMsOrDefault("STREAMING_CONSUMER_POLL_TIMEOUT_MS", 0),
		CloseTimeout:        getenvSecOrDefault("STREAMING_CONSUMER_CLOSE_TIMEOUT_S", defaultCloseTimeout),
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

	// The consumer's own identity gates Build unconditionally: every enabled
	// consumer has a DLQ path, and the DLQ topic is derived from it.
	if c.Source == "" {
		return ErrMissingConsumerSource
	}

	if err := contract.ValidateSource(c.Source); err != nil {
		return fmt.Errorf("%w: source %q: %w", ErrInvalidConfigField, c.Source, err)
	}

	for _, app := range c.Apps {
		if err := contract.ValidateSource(app); err != nil {
			return fmt.Errorf("%w: app %q: %w", ErrInvalidConfigField, app, err)
		}
	}

	for _, source := range c.ExpectSources {
		if err := contract.ValidateSource(source); err != nil {
			return fmt.Errorf("%w: expect source %q: %w", ErrInvalidConfigField, source, err)
		}
	}

	if len(c.ResolvedTopics()) == 0 {
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

// ResolvedTopics returns the full subscription list: the raw Topics followed
// by one derived topic per entry in Apps, deduplicated while preserving
// first-seen order so the franz-go subscription is deterministic.
//
// This is the single place Apps becomes topics; both Validate and the group
// client read it, so the two cannot disagree about what is subscribed.
func (c ConsumerConfig) ResolvedTopics() []string {
	topics := make([]string, 0, len(c.Topics)+len(c.Apps))
	seen := make(map[string]struct{}, len(c.Topics)+len(c.Apps))

	add := func(topic string) {
		if topic == "" {
			return
		}

		if _, dup := seen[topic]; dup {
			return
		}

		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}

	for _, topic := range c.Topics {
		add(topic)
	}

	for _, app := range c.Apps {
		add(contract.AppTopic(app))
	}

	return topics
}
