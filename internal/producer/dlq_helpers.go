package producer

// The six DLQ forensic header keys live in internal/dlqheader so the
// consumer's DLQ publisher (later wave) reuses the exact same string values
// — a shared wire contract, not a producer-private detail.

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
