package contract

import (
	"fmt"
	"strconv"
	"strings"
)

// ResolveDestination returns the destination the relay must publish this
// envelope to under the CURRENT topology, which is not always the destination
// that was persisted.
//
// For a version-2 row the persisted Destination is already current and is
// returned unchanged — this is the hot path and it allocates nothing beyond
// the existing value copy.
//
// For a version-1 row (written by lib-streaming v2) the persisted Kafka
// destination is STALE. v2 derived one topic per event from
// Event.Topic() — "{sanitize(Source)}.{ResourceType}.{EventType}" plus a
// ".v{major}" suffix once SchemaVersion reached 2.0.0, e.g.
// "midaz-ledger.transaction.created". v3 collapsed that to ONE topic per
// producing application, "lerian.streaming.{Source}", and nothing subscribes
// to the old names any more. Publishing the persisted name verbatim would be
// delivered, acknowledged, and read by nobody.
//
// So the topic is RE-DERIVED from the persisted Event.Source, which is the
// same input the live Emit path uses (appTopicRoutes -> AppTopic). A v1 row
// therefore lands on exactly the topic a v2-era event would land on had it
// been emitted today, and current consumers see it.
//
// Non-Kafka transports are returned unchanged at every version. The v1 -> v2
// meaning change was confined to Kafka topic naming; an SQS queue URL, a
// RabbitMQ exchange plus routing key, and an EventBridge bus name all address
// broker-side resources whose identity v3 did not touch.
//
// Commands need no special case. The ".commands" queue and the EventClass
// that routes to it were introduced after version 1, so every version-1 row
// is a business FACT by construction and AppTopic is the whole answer. The
// live path's commandRoute rewrite runs before persist, so a version-2
// command row already carries its commands queue and is returned unchanged.
//
// A structural failure returns ErrLegacyOutboxRowUnroutable. It is
// deliberately NOT a caller error so the relay keeps the row instead of
// letting the dispatcher invalidate it on the first attempt; see the sentinel
// for the full reasoning.
func (e OutboxEnvelope) ResolveDestination() (Destination, error) {
	if e.Version != OutboxEnvelopeVersionLegacy || e.Destination.Kind != TransportKafkaLike {
		return e.Destination, nil
	}

	if err := ValidateSource(e.Event.Source); err != nil {
		// The cause is rendered with %v so the operator still reads the precise
		// reason in the log and in outbox_events.last_error, without it
		// entering the error chain.
		//nolint:errorlint // %v is REQUIRED here, not an oversight. ValidateSource
		// returns ErrMissingSource / ErrInvalidSource, both in callerErrorSentinels;
		// %w would make IsCallerError true and the dispatcher would destroy this
		// durable row on its first attempt. TestResolveDestinationUnroutableSourceStaysRetryable
		// asserts the cause is NOT in the chain and fails if this is "fixed".
		return Destination{}, fmt.Errorf(
			"%w: version-1 row source %q cannot derive an application topic: %v",
			ErrLegacyOutboxRowUnroutable, e.Event.Source, err,
		)
	}

	// Only rewrite what v2 DERIVED. v2 synthesized one route per catalog
	// definition pointing at EventDefinition.Topic(source), but
	// MergeRouteOverrides let a service point a Kafka route anywhere it
	// liked, and such a name is an operator's deliberate choice that v3
	// never invalidated. Rewriting it would silently move a stream the
	// operator still runs a consumer on.
	if e.Destination.Name != legacyDerivedTopic(e.Event) {
		return e.Destination, nil
	}

	resolved := e.Destination
	resolved.Name = AppTopic(e.Event.Source)

	return resolved, nil
}

// legacyDerivedTopic reproduces the topic lib-streaming v2 derived for an
// event: "{sanitizeSourceSegmentV2(Source)}.{ResourceType}.{EventType}", plus
// ".v{major}" once SchemaVersion reached 2.0.0. It exists to tell a v2
// AUTO-DERIVED destination apart from an operator's explicit route override,
// which must not be rewritten.
//
// It MUST use the v2 sanitizer rather than the raw Source. An earlier version
// of this function skipped it, reasoning that the sanitizer is the identity
// for any source v4's ValidateSource accepts. That reasoning was wrong in one
// specific way, and the consequence was severe.
//
// `sourcePattern` is ^[a-z0-9][a-z0-9_-]*$. It anchors the FIRST rune to
// [a-z0-9] but places no constraint on the LAST, so "midaz-ledger-", "slc_"
// and "svc--" are all legal v4 sources. v2's sanitizer ends with
// strings.Trim(s, "-._"), which strips those trailing separators, so v2
// persisted "midaz-ledger.transaction.created" for source "midaz-ledger-".
// Deriving from the raw source yields "midaz-ledger-.transaction.created",
// the two never match, the override gate concludes an operator chose the name
// and ResolveDestination returns the STALE per-event topic unchanged. The
// broker auto-creates it, the publish succeeds, and the row is marked
// PUBLISHED with no consumer anywhere — strictly worse than the INVALID it
// replaced, because INVALID at least retains the row.
//
// The major-version rule mirrors v2 exactly: it used ParseMajorVersion, which
// collapses an unparseable SchemaVersion to 0 and therefore falls through to
// the base form. parseMajorVersionStrict's ok=false is that same case.
func legacyDerivedTopic(event Event) string {
	base := sanitizeSourceSegmentV2(event.Source) + "." + event.ResourceType + "." + event.EventType

	major, ok := parseMajorVersionStrict(event.SchemaVersion)
	if !ok || major < 2 {
		return base
	}

	return base + ".v" + strconv.Itoa(major)
}

// legacyTopicSegmentSeparators are the characters v2 trimmed from the edges of
// a sanitized source segment. Copied from tag v2.1.0.
const legacyTopicSegmentSeparators = "-._"

// sanitizeSourceSegmentV2 is lib-streaming v2's sanitizeSourceSegment, copied
// byte for byte from tag v2.1.0 (internal/contract/source_topic.go).
//
// It is resurrected ONLY to recognise a name v2 wrote. v3 deleted this
// transformation deliberately — it is lossy, so two distinct services could
// normalize onto one topic namespace and one ACL scope without either owner
// noticing — and nothing here reintroduces it to any write path. v4 still
// REJECTS a malformed source rather than folding it; this function never
// decides where an event goes, only whether a persisted name is one v2 would
// have generated.
//
// The transformation: lowercase; map every rune outside [a-z0-9._-] to a
// single '-', collapsing consecutive replacements; then trim '-', '.' and '_'
// from both ends.
func sanitizeSourceSegmentV2(source string) string {
	if source == "" {
		return ""
	}

	lowered := strings.ToLower(source)

	var b strings.Builder

	b.Grow(len(lowered))

	prevHyphen := false

	for _, r := range lowered {
		if isLegacyTopicSegmentRune(r) {
			b.WriteRune(r)

			prevHyphen = false

			continue
		}

		// Replacement char: collapse consecutive replacements into one '-'.
		if prevHyphen {
			continue
		}

		b.WriteByte('-')

		prevHyphen = true
	}

	return strings.Trim(b.String(), legacyTopicSegmentSeparators)
}

// isLegacyTopicSegmentRune reports whether r is inside the Kafka topic-name
// charset v2 preserved. Copied from tag v2.1.0.
func isLegacyTopicSegmentRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '.' || r == '_' || r == '-':
		return true
	default:
		return false
	}
}
