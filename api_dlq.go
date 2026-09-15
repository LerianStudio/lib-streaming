package streaming

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
)

// The dead-letter forensic contract, exposed at the root facade so a service
// can DRAIN its own ".dlq" topic with this library instead of opening a raw
// franz-go client against it.
//
// The library provisions the topic, quarantines into it, and stamps eleven
// forensic headers on every entry. Reading them back is the other half: a
// quarantined record is durable but invisible until something turns it into a
// row on an exception desk saying WHAT died, for WHICH tenant, WHY, and WHERE
// it came from.
//
// Three pieces, in the order a reader uses them:
//
//   - DiscardHandler, the consumer seam. NewConsumer().Topics(dlqTopic).
//     DiscardHandler(h) hands each entry to HandleDiscard already decoded.
//   - DiscardRecord + ParseDiscardRecord, the typed entry and its parser, for
//     tooling that holds the headers itself (a replayer, a one-off drain).
//   - The header-key and cause-kind constants, for anything that has to name a
//     key directly — a Kafka console filter, an index definition, a test.
//
// The header string VALUES are a frozen wire contract: the producer writes
// them, the consumer's DLQ publisher reuses the same values, and replay tooling
// reads them. They are exported here so nobody has to restate them and drift.

// The six shared DLQ forensic header keys. Every DLQ entry carries all six,
// whether a producer or a consumer wrote it; none are optional.
const (
	// DLQHeaderSourceTopic names the topic the quarantined record came from.
	// With one topic per producing application the ".dlq" name no longer
	// implies it, so this header plus the partition and offset below are the
	// only route back to the poison record.
	DLQHeaderSourceTopic = dlqheader.SourceTopic
	// DLQHeaderErrorClass carries the transport's classification of the cause.
	DLQHeaderErrorClass = dlqheader.ErrorClass
	// DLQHeaderErrorMessage carries the sanitized underlying error, bounded at
	// DLQMaxErrorMessageBytes.
	DLQHeaderErrorMessage = dlqheader.ErrorMessage
	// DLQHeaderRetryCount carries the in-loop transient retries consumed before
	// the terminal verdict.
	DLQHeaderRetryCount = dlqheader.RetryCount
	// DLQHeaderFirstFailureAt carries the RFC3339Nano quarantine stamp.
	DLQHeaderFirstFailureAt = dlqheader.FirstFailureAt
	// DLQHeaderProducerID carries the identity that quarantined the record —
	// the consumer group id on a consumer quarantine.
	DLQHeaderProducerID = dlqheader.ProducerID
)

// The three consumer-specific DLQ forensic header keys. A consumed record
// carries an origin partition and offset the producer never has (it quarantines
// before any broker assigns them), which is why these are not among the six.
const (
	// DLQHeaderSourcePartition carries the origin partition.
	DLQHeaderSourcePartition = dlqheader.SourcePartition
	// DLQHeaderSourceOffset carries the origin offset.
	DLQHeaderSourceOffset = dlqheader.SourceOffset
	// DLQHeaderCauseKind names WHICH gate quarantined the record — one of the
	// four DLQCause* values. It is the low-cardinality bucket an operator
	// filters and alerts on; the sanitized error text is in
	// DLQHeaderErrorMessage.
	DLQHeaderCauseKind = dlqheader.CauseKind
)

// The two payload-omitted markers. A quarantine copy is strictly LARGER than
// the record it quarantines (same payload, same headers, plus the forensic
// set), so a near-cap record is republished WITHOUT its payload rather than
// failing the quarantine and wedging the partition. These two say so, which is
// how a reader tells "this payload is genuinely absent" from "I failed to read
// it".
const (
	// DLQHeaderPayloadOmitted is "true" on an entry published without its
	// payload. Absent means the payload is present and verbatim.
	DLQHeaderPayloadOmitted = dlqheader.PayloadOmitted
	// DLQHeaderPayloadBytes carries the size of the payload that was dropped,
	// so an operator can size the problem without fetching the source record.
	DLQHeaderPayloadBytes = dlqheader.PayloadBytes
)

// The four cause kinds stamped on DLQHeaderCauseKind. They have four different
// owners and four different fixes, which is the whole reason the header exists.
const (
	// DLQCauseCodec: the CloudEvents headers would not decode. The producer's
	// wire format is the suspect.
	DLQCauseCodec = dlqheader.CauseCodec
	// DLQCauseHandler: the service handler returned a terminal error. The
	// business rejection is the suspect.
	DLQCauseHandler = dlqheader.CauseHandler
	// DLQCauseSourceMismatch: the record's ce-source was not an expected
	// producer — a foreign write, or an allowlist that drifted.
	DLQCauseSourceMismatch = dlqheader.CauseSourceMismatch
	// DLQCauseUnhandledKey: no handler registered for the event key — this
	// consumer's registrations drifted behind the producer's catalog.
	DLQCauseUnhandledKey = dlqheader.CauseUnhandledKey
)

// DLQMaxErrorMessageBytes is the bound every DLQ writer applies to
// DLQHeaderErrorMessage. A longer error is cut and carries an explicit marker
// with its original length, so a reader never mistakes a truncated string for
// the whole error.
const DLQMaxErrorMessageBytes = dlqheader.MaxErrorMessageBytes

// DiscardRecord is one entry of a ".dlq" topic, decoded: the origin triple, the
// cause, the forensic metadata, the payload, and the original CloudEvents
// envelope (so Event.TenantID is the tenant that owned the poison record).
//
// Every field is best-effort and nothing here ever fails to parse — see
// DiscardHandler for why that posture is mandatory on a DLQ.
type DiscardRecord = dlqheader.DiscardRecord

// DiscardHandler is the consumer seam for reading your own ".dlq" topic. A
// consumer wired with ConsumerBuilder.DiscardHandler hands it each quarantine
// entry already decoded, instead of the flattened CloudEvents envelope a plain
// Handler receives — which is the only way to reach the forensic headers, since
// the codec drops every non-ce-* header before a Handler runs.
//
//	type desk struct{}
//
//	func (desk) HandleDiscard(ctx context.Context, r streaming.DiscardRecord) error {
//	    // r.Event.TenantID — the tenant that owned the poison record
//	    // r.CauseKind      — why it died, one of the DLQCause* values
//	    // r.SourceTopic / r.SourcePartition / r.SourceOffset — where from
//	    return nil // never terminal: this consumer's DLQ is this topic
//	}
//
//	c, err := streaming.NewConsumer().
//	    Brokers(brokers...).
//	    Group("lender-dlq-desk").
//	    Topics(dlqTopic).
//	    DiscardHandler(desk{}).
//	    Build(ctx)
type DiscardHandler = consumer.DiscardHandler

// ParseDiscardRecord decodes one ".dlq" record's headers and value into a
// DiscardRecord. It never fails: a missing or malformed header leaves its field
// at the zero value, because a ".dlq" topic legitimately holds records whose own
// CloudEvents envelope does not parse and rejecting them is not an option a DLQ
// reader has.
//
// A consumer wired with DiscardHandler gets this for free. Use it directly only
// for tooling that holds the headers itself.
//
// Note that the origin coordinates come from the HEADERS, not from the record's
// own topic/partition/offset — those are the DLQ's, not the poison record's.
func ParseDiscardRecord(headers []kgo.RecordHeader, payload []byte) DiscardRecord {
	return dlqheader.ParseRecord(headers, payload)
}
