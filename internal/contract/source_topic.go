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

// MaxKafkaTopicNameBytes is Kafka's protocol-level topic-name limit.
const MaxKafkaTopicNameBytes = 249

// maxSourceSegmentBytes is the largest ce-source that still yields a legal
// DLQ topic name. The DLQ topic is the longest name derived from a source
// (prefix + source + ".dlq"), so bounding against it bounds every derived
// name at once.
const maxSourceSegmentBytes = MaxKafkaTopicNameBytes - len(TopicPrefix) - len(DLQTopicSuffix)

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
			ErrInvalidSource, maxSourceSegmentBytes, TopicPrefix, DLQTopicSuffix, MaxKafkaTopicNameBytes)
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
