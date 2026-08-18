package contract

import (
	"fmt"
	"regexp"
)

// TopicPrefix is the fixed namespace every lib-streaming topic carries.
// Combined with the producing application's ce-source it yields the ONE
// topic that application publishes to (see AppTopic).
const TopicPrefix = "lerian.streaming."

// DLQTopicSuffix is appended to an app topic to derive its dead-letter
// topic. Under the v3 one-topic-per-app contract there is effectively one
// DLQ per application, but the per-topic derivation semantic is unchanged:
// the DLQ of topic T is always T + ".dlq".
const DLQTopicSuffix = ".dlq"

// CommandsTopicSuffix is appended to an app topic to derive the queue that
// carries that application's service-to-service COMMANDS.
//
// Commands are separated from facts because their unmatched semantics are
// opposite. A fact stream is a firehose a consumer legitimately ignores most
// of; a command stream is addressed TO the consumer, so a key it has no
// handler for is undelivered work, not noise. Splitting the queue is what
// lets the consumer apply strict semantics to one and lenient to the other
// without a per-key registry.
//
// There is deliberately NO ".commands.dlq". A consumer quarantines into its
// OWN ".dlq" and a producer route-DLQs a failed command publish into its OWN
// ".dlq"; both already exist, and a fourth name would widen every
// command-emitting app's write grant for nothing.
const CommandsTopicSuffix = ".commands"

// MaxKafkaTopicNameBytes is Kafka's protocol-level topic-name limit.
const MaxKafkaTopicNameBytes = 249

// maxSourceSegmentBytes is the largest ce-source that still yields a legal
// commands topic name. The commands topic is the longest name derived from a
// source (prefix + source + ".commands"), so bounding against it bounds every
// derived name at once.
//
// The rule is UNIFORM across applications: every app CAN emit commands, so
// the bound is the same whether or not this one's catalog holds any today. A
// per-app bound would make adding the first command definition a
// source-rename event.
const maxSourceSegmentBytes = MaxKafkaTopicNameBytes - len(TopicPrefix) - len(CommandsTopicSuffix)

// sourcePattern is the STRICT v3 ce-source shape: one dot-free lowercase
// segment, starting with an alphanumeric, continuing with alphanumerics,
// hyphens, or underscores.
//
// Dots are excluded deliberately. The source IS a single segment of the
// derived topic name; allowing a dot would let one application claim topic
// namespace that reads as several, defeating the "one topic per producing
// application" contract and muddying Kafka ACL scoping.
var sourcePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// ValidateSource reports whether source is a legal v3 ce-source.
//
// v3 REJECTS an invalid source; it never rewrites one. The v2 lossy
// normalization (sanitizeSourceSegment: lowercase, punctuation-fold,
// separator-collapse) is DELETED. That transformation could map two
// distinct services onto one topic namespace and one Kafka ACL scope
// without either owner noticing — a silent cross-service collision.
// Rejecting at the three entry points where a source is first seen
// (config validation, Builder validation, producer preflight) turns that
// class of bug into a startup failure with a precise message.
//
// Returns ErrMissingSource for an empty source and an ErrInvalidSource-
// wrapped error for a malformed or over-long one, so callers keep the
// existing errors.Is vocabulary.
func ValidateSource(source string) error {
	if source == "" {
		return ErrMissingSource
	}

	if len(source) > maxSourceSegmentBytes {
		return fmt.Errorf("%w: source exceeds %d bytes (derived topic %q + %q must fit Kafka's %d-byte limit)",
			ErrInvalidSource, maxSourceSegmentBytes, TopicPrefix, CommandsTopicSuffix, MaxKafkaTopicNameBytes)
	}

	if !sourcePattern.MatchString(source) {
		return fmt.Errorf("%w: source %q must be a single dot-free lowercase segment matching %s",
			ErrInvalidSource, source, sourcePattern.String())
	}

	return nil
}

// AppTopic derives the ONE topic a producing application publishes to.
//
// Every event that application emits — business fact or service-to-service
// command, every resource type, every event type, every schema version —
// rides this single topic. Consumers subscribe to the app stream and
// dispatch per event using the ce-resourcetype / ce-eventtype headers.
//
// The source is expected to be pre-validated by ValidateSource at config,
// Builder, and preflight time; AppTopic itself performs no validation so it
// stays allocation-predictable on the per-Emit path.
func AppTopic(source string) string {
	return TopicPrefix + source
}

// AppDLQTopic derives the dead-letter topic for an application's stream.
func AppDLQTopic(source string) string {
	return AppTopic(source) + DLQTopicSuffix
}

// AppCommandsTopic derives the queue carrying an application's
// service-to-service COMMANDS — the events another service must act on, as
// opposed to the facts it may ignore.
//
// It is the second stream a producing application writes, and the one a rail
// consumer subscribes to when it is being commanded. Its whole reason to
// exist is the unmatched verdict: a command key with no registered handler
// is quarantined, never skipped, so a producer shipping a new command ahead
// of its consumer's handler fails loudly instead of losing work behind green
// dashboards.
func AppCommandsTopic(source string) string {
	return AppTopic(source) + CommandsTopicSuffix
}
