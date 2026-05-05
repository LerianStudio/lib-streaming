package producer

// DLQ header keys (TRD §C8). Every DLQ message carries all six; none are
// optional. Keeping them colocated here (not in cloudevents.go) because they
// are NOT CloudEvents context attributes — they are Lerian-specific
// operational metadata that sits alongside the ce-* headers on DLQ messages.
//
// Tenant identity is carried exclusively in the CloudEvents ce-tenantid
// header (set by buildCloudEventsHeaders). There is no x-lerian-dlq-tenant-id
// — duplicating tenant data across header namespaces would widen the wire
// contract beyond the documented six DLQ headers and force every consumer
// to reconcile two sources of truth.
const (
	dlqHeaderSourceTopic    = "x-lerian-dlq-source-topic"
	dlqHeaderErrorClass     = "x-lerian-dlq-error-class"
	dlqHeaderErrorMessage   = "x-lerian-dlq-error-message"
	dlqHeaderRetryCount     = "x-lerian-dlq-retry-count"
	dlqHeaderFirstFailureAt = "x-lerian-dlq-first-failure-at"
	dlqHeaderProducerID     = "x-lerian-dlq-producer-id"
)

// dlqTopicSuffix is the literal suffix appended to the source topic to
// derive the per-topic DLQ name. Per-topic DLQ (TRD §C8) is preferred over
// a global DLQ because it lets operators scope replay tooling to a single
// topic and keeps failure-class cardinality proportional to topic count,
// not to the product of topic × error_class.
const dlqTopicSuffix = ".dlq"

// dlqTopic derives "{source}.dlq" from the source topic name. Split out as
// a named helper so the naming convention surfaces in exactly one place.
func dlqTopic(sourceTopic string) string {
	return sourceTopic + dlqTopicSuffix
}

// isDLQRoutable reports whether a classified error should route to the
// per-topic DLQ. Per TRD §C9 retry + DLQ policy table, DLQ routing applies
// to every class EXCEPT caller-cancel (ClassContextCanceled — caller
// already gave up) and caller-validation (ClassValidation — caller sees
// the error and corrects its own input).
//
// The other six classes (serialization, auth, topic_not_found,
// broker_unavailable, network_timeout, broker_overloaded) ALL route to DLQ.
func isDLQRoutable(cls ErrorClass) bool {
	switch cls {
	case ClassValidation, ClassContextCanceled:
		return false
	default:
		return true
	}
}

// extractRetryCount reports the franz-go retry counter at the time of
// exhaustion. franz-go does NOT export a stable API for retrieving the
// retry counter off kgo.ErrRecordRetries — the internal metadata is
// package-private. Returning 0 is the conservative choice: the DLQ message
// still carries a well-formed x-lerian-dlq-retry-count header with a
// predictable shape, and ops tooling knows that 0 means "unknown" in v2,
// not "no retries attempted."
func extractRetryCount(_ error) int {
	return 0
}
