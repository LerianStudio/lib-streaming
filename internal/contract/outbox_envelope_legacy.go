package contract

import "fmt"

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
		// %v on the cause, NOT %w, and the linter's "always wrap" rule does
		// not apply here. ValidateSource returns ErrMissingSource /
		// ErrInvalidSource, both of which are in callerErrorSentinels;
		// splicing either into this chain would make IsCallerError true and
		// send the row straight to INVALID — destroying the durable row this
		// whole path exists to preserve. The cause is kept as text so the
		// operator still reads the precise reason in the log and in
		// outbox_events.last_error.
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

	resolved := e.Destination
	resolved.Name = AppTopic(e.Event.Source)

	return resolved, nil
}
