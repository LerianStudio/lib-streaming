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
// documented x-lerian-dlq-* set and force every consumer to reconcile two
// sources of truth.
//
// That set is nine hop-scoped keys, replaced rather than appended when an entry
// is quarantined again: six on every DLQ message, and three (source partition,
// source offset, cause kind) only on a consumer quarantine, since the producer
// quarantines before any broker assigns a coordinate. Plus two payload markers,
// carried across hops and present only when the payload had to be dropped.
// Counted anywhere else, the number drifts; hopHeaders below is the set.
//
// The package also owns the two size rules every DLQ writer obeys —
// MaxErrorMessageBytes / TruncateErrorMessage for the one unbounded header
// value, and IsSizeError plus the PayloadOmitted / PayloadBytes markers for the
// payload-omitted retry — because a DLQ record is strictly larger than the
// record it quarantines and must still fit. See budget.go.
package dlqheader

// hopHeaders are the nine keys describing ONE quarantine event: who
// quarantined the record, why, when, and from which coordinate. A DLQ writer
// strips them from the headers it copies before stamping its own, so a record
// carries exactly one of each — always describing the MOST RECENT quarantine.
//
// It matters because a quarantine copy can itself be quarantined: a DLQ reader
// whose handler returns terminal re-quarantines an entry that already carries
// the full set. Appending a second set would leave two values for every key — a
// reader cannot tell which quarantine each describes — and the block would grow
// by nine keys per hop on a record already strictly larger than the one it
// quarantines, which is the size wedge MaxErrorMessageBytes exists to prevent.
//
// The chain back survives as a linked list instead: each entry's origin triple
// names the topic and offset the failing consumer actually read, one hop at a
// time.
//
// PayloadOmitted and PayloadBytes are deliberately NOT in this set. They do not
// describe a quarantine; they describe the PAYLOAD the record carries, relative
// to the business record it came from — "what you see is not the original, and
// the original was N bytes". That stays true at every later hop, so they travel
// forward instead of being stripped. Dropping them would let a re-quarantined
// slim entry claim a genuinely empty payload, and would lose the only surviving
// record of the original size.
var hopHeaders = map[string]struct{}{
	SourceTopic:     {},
	ErrorClass:      {},
	ErrorMessage:    {},
	RetryCount:      {},
	FirstFailureAt:  {},
	ProducerID:      {},
	SourcePartition: {},
	SourceOffset:    {},
	CauseKind:       {},
}

// IsHopHeader reports whether key describes a single quarantine event rather
// than the record being quarantined. See hopHeaders.
func IsHopHeader(key string) bool {
	_, ok := hopHeaders[key]

	return ok
}

// The DLQ forensic header keys every writer stamps, producer or consumer
// (TRD §C8). None of them are optional.
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
// before any broker assigns them), so they are NOT in the block above. They
// are equally a wire contract — replay/forensic tooling reads them to locate the
// poison record in the source topic — so their string values are frozen too.
const (
	SourcePartition = "x-lerian-dlq-source-partition"
	SourceOffset    = "x-lerian-dlq-source-offset"
	// CauseKind names WHICH gate quarantined the record: "codec",
	// "handler", "source_mismatch", or "unhandled_key". The
	// x-lerian-dlq-error-message header carries the sanitized underlying
	// error; this one is the low-cardinality bucket an operator filters and
	// alerts on. A DLQ where every entry says the same thing tells nobody
	// what broke.
	CauseKind = "x-lerian-dlq-cause-kind"
)

// The four cause kinds stamped on the CauseKind header. Low-cardinality by
// design: an operator filters and alerts on this, then reads the sanitized
// underlying error from the ErrorMessage header.
//
// They live here, beside the header key whose values they are, because the
// values are as much a wire contract as the keys: a reader that buckets a DLQ
// by cause compares against these strings, so changing one silently
// reclassifies every entry an operator has an alert on. The root facade
// restates them as literals (so they render on the public documentation page)
// and a test pins the two sets equal.
//
// They exist because every DLQ entry used to carry the SAME message. A
// consumer's DLQ filling up told an operator that something was terminal and
// nothing else — a codec fault (the producer's wire format drifted), a source
// mismatch (a foreign write, or a misconfigured allowlist), an unhandled key
// (this consumer's registrations drifted behind the producer's catalog) and a
// genuine business rejection were indistinguishable, and they have four
// different owners and four different fixes.
const (
	// CauseCodec: the CloudEvents headers would not decode. The record is
	// poison and can never parse; the producer's wire format is the suspect.
	CauseCodec = "codec"
	// CauseHandler: the service handler returned a terminal error. The
	// business rejection is the suspect.
	CauseHandler = "handler"
	// CauseSourceMismatch: the event's ce-source was not an expected producer.
	// Either a foreign write to the topic, or an ExpectSources allowlist that
	// drifted from what actually publishes there.
	CauseSourceMismatch = "source_mismatch"
	// CauseUnhandledKey: no handler registered for the event key. This
	// consumer's On(...) registrations have drifted behind the producer's
	// catalog.
	CauseUnhandledKey = "unhandled_key"
)
