// Package dlqheader holds the forensic header keys stamped onto every DLQ
// message. They live here (not in cloudevents) because they are NOT
// CloudEvents context attributes — they are Lerian-specific operational
// metadata that sits alongside the ce-* headers.
//
// The exact string values are a wire contract: the producer writes them and
// the consumer's DLQ publisher reuses the SAME values. Changing a value
// silently breaks any consumer reading producer-written DLQ headers, so these
// strings are frozen.
//
// Tenant identity is carried exclusively in the CloudEvents ce-tenantid
// header. There is deliberately no SourceTenantID key — duplicating tenant
// data across header namespaces would widen the wire contract beyond the
// documented six DLQ headers and force every consumer to reconcile two
// sources of truth.
package dlqheader

// The six DLQ forensic header keys (TRD §C8). Every DLQ message carries all
// six; none are optional.
const (
	SourceTopic    = "x-lerian-dlq-source-topic"
	ErrorClass     = "x-lerian-dlq-error-class"
	ErrorMessage   = "x-lerian-dlq-error-message"
	RetryCount     = "x-lerian-dlq-retry-count"
	FirstFailureAt = "x-lerian-dlq-first-failure-at"
	ProducerID     = "x-lerian-dlq-producer-id"
)

// The two consumer-specific DLQ forensic header keys. A consumed record carries
// a source partition and offset the producer never has (the producer quarantines
// before any broker assigns them), so they are NOT part of the frozen six. They
// are equally a wire contract — replay/forensic tooling reads them to locate the
// poison record in the source topic — so their string values are frozen too.
const (
	SourcePartition = "x-lerian-dlq-source-partition"
	SourceOffset    = "x-lerian-dlq-source-offset"
)
