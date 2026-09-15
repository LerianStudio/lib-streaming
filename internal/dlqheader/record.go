package dlqheader

import (
	"math"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
)

// The four cause kinds stamped on the CauseKind header. Low-cardinality by
// design: an operator filters and alerts on this, then reads the sanitized
// underlying error from ErrorMessage.
//
// They live here, beside the header key whose values they are, because the
// values are as much a wire contract as the keys: a reader that buckets a DLQ
// by cause compares against these strings, so changing one silently reclassifies
// every entry an operator has an alert on. The consumer runtime stamps them.
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

// DiscardRecord is one entry of a ".dlq" topic, decoded: WHAT was quarantined,
// for WHICH tenant, WHY it died, and WHERE it came from.
//
// Every field is best-effort. ParseRecord NEVER fails, and that is the point: a
// DLQ reader's own ce-source is the application whose ".dlq" it drains, so a
// terminal verdict on a malformed entry would quarantine the failure back onto
// the topic it is draining — a self-feeding loop. A ".dlq" topic legitimately
// holds records whose own CloudEvents envelope does not parse (that is exactly
// what CauseKind == CauseCodec means, and the quarantine copy is header- and
// payload-verbatim), so "unparseable" is normal content here, not an error
// condition. A missing or malformed header leaves its field at the zero value
// and the reader decides.
type DiscardRecord struct {
	// SourceTopic, SourcePartition and SourceOffset are the origin triple: the
	// only route back to the poison record, since the ".dlq" topic name no
	// longer implies which topic the record came from.
	//
	// They are also the stable natural key for deduping a redelivered or
	// replayed quarantine. ce-id is NOT that key: the documented replay path
	// re-delivers to the consumer, which may quarantine the same event again,
	// and two genuinely distinct quarantines share one ce-id.
	//
	// An empty SourceTopic means the triple is absent — partition 0 and offset
	// 0 are legitimate values, so the topic is the field to test, never the
	// numbers.
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64

	// CauseKind is the low-cardinality bucket naming WHICH gate quarantined the
	// record: CauseCodec, CauseHandler, CauseSourceMismatch or
	// CauseUnhandledKey. A value outside that set came from a writer this
	// version does not know; it travels through verbatim rather than being
	// normalized away.
	CauseKind string
	// ErrorClass is the transport adapter's classification of the cause.
	// Forensic metadata only, never a routing decision.
	ErrorClass string
	// ErrorMessage is the sanitized underlying error, bounded at
	// MaxErrorMessageBytes and carrying an explicit marker when it was cut.
	ErrorMessage string
	// RetryCount is how many in-loop transient retries were consumed before the
	// terminal verdict.
	RetryCount int
	// FirstFailureAt is when the quarantine verdict was stamped. For a record
	// that failed terminally on its first attempt the two coincide; for one
	// that first failed transiently and was quarantined on a later poll, this
	// lags the true first failure.
	FirstFailureAt time.Time
	// ProducerID is the identity that quarantined the record — the consumer
	// group id on a consumer quarantine.
	ProducerID string

	// PayloadOmitted reports that the quarantine copy was published WITHOUT its
	// payload because the record with it would not fit. It is the field that
	// distinguishes "this payload is genuinely absent" from "I failed to read
	// it": when it is true, Payload is empty on purpose and PayloadBytes says
	// how large the dropped payload was. On a CONSUMER quarantine the payload
	// is still recoverable from the source topic at the origin triple.
	PayloadOmitted bool
	// PayloadBytes is the size of the payload that was dropped. Zero unless
	// PayloadOmitted.
	PayloadBytes int

	// Event is the ORIGINAL CloudEvents envelope, preserved verbatim on the
	// quarantine copy — so Event.TenantID is the tenant that owned the poison
	// record, and Event.Source is the application that produced it, never the
	// one that quarantined it (that is ProducerID).
	//
	// Event.Payload is NOT populated: the envelope is parsed from headers only,
	// matching ParseCloudEventsHeaders. Read Payload instead.
	Event contract.Event
	// EnvelopeError is why Event could not be parsed, and nil when it parsed.
	// It is the difference between a tenant that is empty because the
	// deployment is single-tenant and one that is empty because the headers
	// were garbage — which, on a DLQ, is a routine and meaningful distinction.
	EnvelopeError error

	// Payload is the quarantined record's value, verbatim. Empty when
	// PayloadOmitted.
	Payload []byte
}

// ParseRecord decodes one ".dlq" record's headers and value into a
// DiscardRecord. It never fails; see DiscardRecord.
func ParseRecord(headers []kgo.RecordHeader, payload []byte) DiscardRecord {
	index := make(map[string]string, len(headers))
	for _, h := range headers {
		index[h.Key] = string(h.Value)
	}

	event, envelopeErr := cloudevents.ParseCloudEventsHeaders(headers)

	record := DiscardRecord{
		SourceTopic:     index[SourceTopic],
		SourcePartition: parsePartition(index[SourcePartition]),
		SourceOffset:    parseInt(index[SourceOffset]),
		CauseKind:       index[CauseKind],
		ErrorClass:      index[ErrorClass],
		ErrorMessage:    index[ErrorMessage],
		RetryCount:      int(parseInt(index[RetryCount])),
		FirstFailureAt:  parseTime(index[FirstFailureAt]),
		ProducerID:      index[ProducerID],
		PayloadOmitted:  index[PayloadOmitted] == "true",
		PayloadBytes:    int(parseInt(index[PayloadBytes])),
		Event:           event,
		EnvelopeError:   envelopeErr,
		Payload:         payload,
	}

	return record
}

// parseInt returns 0 for an absent or malformed value — never an error, so a
// half-written header block still yields a usable record.
func parseInt(value string) int64 {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}

	return n
}

// parsePartition returns 0 for an absent, malformed, or out-of-range partition.
// The explicit bound is what makes the narrowing conversion provably safe rather
// than merely safe by the bit size passed to ParseInt.
func parsePartition(value string) int32 {
	n := parseInt(value)
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0
	}

	return int32(n)
}

// parseTime returns the zero time for an absent or malformed stamp.
func parseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}

	return t.UTC()
}
