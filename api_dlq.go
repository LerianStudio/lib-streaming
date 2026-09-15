package streaming

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// The dead-letter forensic contract, exposed at the root facade so a service
// can DRAIN a ".dlq" topic with this library instead of opening a raw franz-go
// client against it.
//
// The library provisions the topic, quarantines into it, and stamps forensic
// headers on every entry. Reading them back is the other half: a quarantined
// record is durable but invisible until something turns it into a row on an
// exception desk saying WHAT died, for WHICH tenant, WHY, and WHERE it came
// from.
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
// reads them. They are written here as LITERALS rather than as aliases into the
// package that stamps them, so the actual wire string is visible on this
// package's documentation page — an alias renders as a name, and a reader who
// cannot see the string restates it and drifts, which is the failure these
// exports exist to prevent. TestDLQHeaderConstants_MatchTheWriter pins every
// one of them against the writer's own constant, so the two cannot diverge.

// The six shared DLQ forensic header keys. Every DLQ entry carries all six,
// whether a producer or a consumer wrote it; none are optional.
const (
	// DLQHeaderSourceTopic names the topic the quarantined record came from.
	// With one topic per producing application the ".dlq" name no longer
	// implies it, so this header plus the partition and offset below are the
	// only route back to the poison record.
	DLQHeaderSourceTopic = "x-lerian-dlq-source-topic"
	// DLQHeaderErrorClass carries the transport's classification of the cause.
	DLQHeaderErrorClass = "x-lerian-dlq-error-class"
	// DLQHeaderErrorMessage carries the sanitized underlying error, bounded at
	// DLQMaxErrorMessageBytes. Use TruncatedErrorMessageBytes to tell a cut
	// message from a whole one.
	DLQHeaderErrorMessage = "x-lerian-dlq-error-message"
	// DLQHeaderRetryCount carries the in-loop transient retries consumed before
	// the terminal verdict.
	DLQHeaderRetryCount = "x-lerian-dlq-retry-count"
	// DLQHeaderFirstFailureAt carries the RFC3339Nano quarantine stamp.
	DLQHeaderFirstFailureAt = "x-lerian-dlq-first-failure-at"
	// DLQHeaderProducerID carries the identity that quarantined the record —
	// the consumer group id on a consumer quarantine.
	DLQHeaderProducerID = "x-lerian-dlq-producer-id"
)

// The two consumer-specific DLQ forensic header keys. A CONSUMED record carries
// an origin partition and offset the producer never has (it quarantines before
// any broker assigns them), which is why these are not among the six: a
// producer-written entry legitimately has an origin topic and no coordinates.
const (
	// DLQHeaderSourcePartition carries the origin partition.
	DLQHeaderSourcePartition = "x-lerian-dlq-source-partition"
	// DLQHeaderSourceOffset carries the origin offset.
	DLQHeaderSourceOffset = "x-lerian-dlq-source-offset"
	// DLQHeaderCauseKind names WHICH gate quarantined the record — one of the
	// four DLQCause* values. It is the low-cardinality bucket an operator
	// filters and alerts on; the sanitized error text is in
	// DLQHeaderErrorMessage.
	DLQHeaderCauseKind = "x-lerian-dlq-cause-kind"
)

// The two payload-omitted markers, present only on an entry whose payload was
// dropped. A quarantine copy is strictly LARGER than the record it quarantines
// (same payload, same headers, plus the forensic set), so a near-cap record is
// republished WITHOUT its payload rather than failing the quarantine and
// wedging the partition. These two say so, which is how a reader tells "this
// payload is genuinely absent" from "I failed to read it".
const (
	// DLQHeaderPayloadOmitted is "true" on an entry published without its
	// payload. Absent means the payload is present and verbatim.
	DLQHeaderPayloadOmitted = "x-lerian-dlq-payload-omitted"
	// DLQHeaderPayloadBytes carries the size of the payload that was dropped,
	// so an operator can size the problem without fetching the source record.
	DLQHeaderPayloadBytes = "x-lerian-dlq-payload-bytes"
)

// The four cause kinds stamped on DLQHeaderCauseKind. They have four different
// owners and four different fixes, which is the whole reason the header exists.
const (
	// DLQCauseCodec: the CloudEvents headers would not decode. The producer's
	// wire format is the suspect.
	DLQCauseCodec = "codec"
	// DLQCauseHandler: the service handler returned a terminal error. The
	// business rejection is the suspect.
	DLQCauseHandler = "handler"
	// DLQCauseSourceMismatch: the record's ce-source was not an expected
	// producer — a foreign write, or an allowlist that drifted.
	DLQCauseSourceMismatch = "source_mismatch"
	// DLQCauseUnhandledKey: no handler registered for the event key — this
	// consumer's registrations drifted behind the producer's catalog.
	DLQCauseUnhandledKey = "unhandled_key"
)

// DLQMaxErrorMessageBytes is the bound every DLQ writer applies to
// DLQHeaderErrorMessage. A longer error is cut and carries a marker with its
// original length; TruncatedErrorMessageBytes is how a reader detects that.
//
// The bound alone is NOT a truncation test: the cut output is shorter than this
// whenever a split multi-byte rune is dropped.
const DLQMaxErrorMessageBytes = dlqheader.MaxErrorMessageBytes

// ErrMalformedOriginCoordinates reports that a discard record carried an origin
// header this library could not parse. See DiscardRecord.HeaderError.
var ErrMalformedOriginCoordinates = errors.New("streaming: dlq record has a malformed origin coordinate")

// DiscardRecord is one entry of a ".dlq" topic, decoded: WHAT was quarantined,
// for WHICH tenant, WHY it died, and WHERE it came from.
//
// ParseDiscardRecord never fails, and that is the point: a DLQ reader's own
// ce-source is the application whose quarantines it drains, so treating an
// unreadable entry as an error would turn the normal content of the topic into
// a failure. A ".dlq" topic legitimately holds records whose own CloudEvents
// envelope does not parse — that is exactly what a DLQCauseCodec entry is — so
// "unparseable" is content here, not an error condition. A missing or malformed
// header leaves its field at the zero value, and the two error fields say which
// kind of unreadable it was.
type DiscardRecord struct {
	// SourceTopic, SourcePartition and SourceOffset are the origin triple: the
	// only route back to the poison record, since the ".dlq" topic name no
	// longer implies which topic the record came from.
	//
	// They are also the stable natural key for deduping a redelivered or
	// replayed quarantine. ce-id is NOT that key: the documented replay path
	// re-delivers to the consumer, which may quarantine the same event again,
	// and two genuinely distinct quarantines then share one ce-id.
	//
	// The triple FAILS CLOSED as a unit. If any of the three headers is present
	// but unreadable — malformed, out of range, or NEGATIVE, since no partition
	// or offset can be below zero — all three are zeroed and HeaderError says
	// so, because a half-parsed triple is worse than none: "topic/0/42" is a plausible-looking
	// coordinate pointing at the wrong record, and as a dedup key it silently
	// merges quarantines that are genuinely distinct. An ABSENT coordinate is
	// not a failure — a producer-side quarantine legitimately has no partition
	// or offset.
	//
	// An empty SourceTopic means no usable triple. Test that field, never the
	// numbers: partition 0 and offset 0 are legitimate values.
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64

	// CauseKind is the low-cardinality bucket naming WHICH gate quarantined the
	// record: DLQCauseCodec, DLQCauseHandler, DLQCauseSourceMismatch or
	// DLQCauseUnhandledKey. A value outside that set came from a writer this
	// version does not know; it travels through verbatim rather than being
	// normalized away.
	CauseKind string
	// ErrorClass is the transport adapter's classification of the cause.
	// Forensic metadata only, never a routing decision.
	ErrorClass string
	// ErrorMessage is the sanitized underlying error, bounded at
	// DLQMaxErrorMessageBytes. Pass it to TruncatedErrorMessageBytes to learn
	// whether it was cut and how long the original was.
	ErrorMessage string
	// RetryCount is how many in-loop transient retries were consumed before the
	// terminal verdict.
	RetryCount int
	// FirstFailureAt is when the quarantine verdict was stamped, in UTC. For a
	// record that failed terminally on its first attempt the two coincide; for
	// one that first failed transiently and was quarantined on a later poll,
	// this lags the true first failure.
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
	//
	// The promise holds at EVERY hop. Unlike the nine forensic headers, which
	// describe one quarantine and are replaced each time a record is
	// re-quarantined, the two payload markers describe the RECORD's payload
	// relative to the business record it came from — "what you see is not the
	// original, and the original was PayloadBytes bytes" — which stays true
	// however many times the entry is quarantined again. A DLQ writer carries
	// them forward, so a re-quarantined slim entry still reports true here
	// instead of claiming a genuinely empty payload.
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
	Event Event
	// EnvelopeError is why Event could not be parsed, and nil when it parsed.
	// It is the difference between a tenant that is empty because the
	// deployment is single-tenant and one that is empty because the headers
	// were garbage — which, on a DLQ, is a routine and meaningful distinction.
	EnvelopeError error
	// HeaderError is why a forensic x-lerian-dlq-* header could not be read, and
	// nil when every present one parsed. It wraps ErrMalformedOriginCoordinates
	// when the unreadable header belongs to the origin triple, which is the case
	// that also zeroes the whole triple.
	HeaderError error

	// Payload is the quarantined record's value, verbatim. Empty when
	// PayloadOmitted.
	Payload []byte
}

// DiscardHandler is the consumer seam for reading a ".dlq" topic. A consumer
// wired with ConsumerBuilder.DiscardHandler hands it each quarantine entry
// already decoded, instead of the flattened CloudEvents envelope a plain
// Handler receives — which is the only way to reach the forensic headers, since
// the codec drops every non-ce-* header before a Handler runs.
//
//	type desk struct{}
//
//	func (desk) HandleDiscard(ctx context.Context, r streaming.DiscardRecord) error {
//	    // r.Event.TenantID — the tenant that owned the poison record
//	    // r.CauseKind      — why it died, one of the DLQCause* values
//	    // r.SourceTopic / r.SourcePartition / r.SourceOffset — where from
//	    return nil
//	}
//
//	c, err := streaming.NewConsumer().
//	    Brokers(brokers...).
//	    Group("lender-dlq-desk").
//	    Source("lender-dlq-desk"). // NOT "lender": see ConsumerBuilder.DiscardHandler
//	    Topics("lerian.streaming.lender.dlq").
//	    DiscardHandler(desk{}).
//	    Build(ctx)
type DiscardHandler interface {
	HandleDiscard(ctx context.Context, record DiscardRecord) error
}

// TruncatedErrorMessageBytes reports whether a DiscardRecord.ErrorMessage is a
// CUT error message and, if so, how many bytes the original had.
//
// Detecting a cut otherwise means hardcoding the marker text, which is the
// restate-and-drift failure these exports exist to stop. DLQMaxErrorMessageBytes
// does not answer it: the cut output is shorter than the bound whenever a split
// multi-byte rune is dropped.
func TruncatedErrorMessageBytes(message string) (int, bool) {
	return dlqheader.TruncatedErrorMessageBytes(message)
}

// ParseDiscardRecord decodes one ".dlq" record's headers and value into a
// DiscardRecord. It never returns an error; unreadable input is reported on the
// record's own EnvelopeError and HeaderError fields, and the origin triple fails
// closed as a unit. See DiscardRecord.
//
// A consumer wired with DiscardHandler gets this for free. Use it directly only
// for tooling that holds the headers itself.
//
// The origin coordinates come from the HEADERS, never from the record's own
// topic/partition/offset — those are the DLQ's, not the poison record's.
//
// A duplicate key resolves to its LAST value. Records this library writes never
// carry one: a DLQ writer replaces the nine hop-scoped forensic headers rather
// than appending to them, and carries the two payload markers forward, so
// re-quarantining a quarantine copy still yields exactly one value per key. The
// rule is stated for records a foreign writer produced.
// Header VALUES are sanitized on the way into the index: a Kafka header carries
// arbitrary bytes, and a value holding U+0000 or an invalid UTF-8 sequence is
// refused permanently by any text store the reader hands it to. See
// transport.SanitizeHeaderValue. Keys are not touched — a key is matched against
// this library's own frozen constants, so a hostile one simply matches nothing.
func ParseDiscardRecord(headers []kgo.RecordHeader, payload []byte) DiscardRecord {
	index := make(map[string]string, len(headers))
	for _, h := range headers {
		index[h.Key] = transport.SanitizeHeaderValue(h.Value)
	}

	event, envelopeErr := ParseCloudEventsHeaders(headers)

	record := DiscardRecord{
		CauseKind:      index[DLQHeaderCauseKind],
		ErrorClass:     index[DLQHeaderErrorClass],
		ErrorMessage:   index[DLQHeaderErrorMessage],
		ProducerID:     index[DLQHeaderProducerID],
		PayloadOmitted: index[DLQHeaderPayloadOmitted] == "true",
		Event:          event,
		EnvelopeError:  envelopeErr,
		Payload:        payload,
	}

	// Three non-origin headers can be malformed, plus at most two origin ones.
	malformed := make([]string, 0, 3)

	record.RetryCount, malformed = readInt(index, DLQHeaderRetryCount, malformed)
	record.PayloadBytes, malformed = readInt(index, DLQHeaderPayloadBytes, malformed)
	record.FirstFailureAt, malformed = readTime(index, DLQHeaderFirstFailureAt, malformed)

	origin, originMalformed := readOrigin(index)
	record.SourceTopic, record.SourcePartition, record.SourceOffset = origin.topic, origin.partition, origin.offset

	record.HeaderError = headerError(append(malformed, originMalformed...), len(originMalformed) > 0)

	return record
}

// originCoordinates is the parsed origin triple.
type originCoordinates struct {
	topic     string
	partition int32
	offset    int64
}

// readOrigin parses the origin triple as a UNIT. Any present-but-unreadable
// member zeroes all three: a half-parsed triple is a plausible-looking
// coordinate pointing at the wrong record, and as the documented dedup key it
// would silently merge distinct quarantines. An ABSENT member is not a failure —
// a producer-side quarantine has no partition or offset.
func readOrigin(index map[string]string) (originCoordinates, []string) {
	var malformed []string

	origin := originCoordinates{topic: index[DLQHeaderSourceTopic]}

	if raw, ok := index[DLQHeaderSourcePartition]; ok {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n < 0 || n > math.MaxInt32 {
			malformed = append(malformed, DLQHeaderSourcePartition)
		} else {
			origin.partition = int32(n)
		}
	}

	if raw, ok := index[DLQHeaderSourceOffset]; ok {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			malformed = append(malformed, DLQHeaderSourceOffset)
		} else {
			origin.offset = n
		}
	}

	if len(malformed) > 0 {
		return originCoordinates{}, malformed
	}

	return origin, nil
}

// readInt returns the count at key, 0 when absent, and 0 plus the key recorded
// as malformed when present but unreadable.
//
// A NEGATIVE value is unreadable. Every numeric header this library writes is a
// count or a coordinate — a retry tally, a byte size, a partition, an offset —
// and none can be below zero, so a negative one did not come from a DLQ writer
// this version understands. Passing it through would put "-1 retries" or "-8
// bytes" on an exception desk as though it were measured.
//
// strconv.Atoi parses at the platform's int width, so a value beyond the native
// int range is rejected here rather than silently wrapping on a 32-bit build.
func readInt(index map[string]string, key string, malformed []string) (int, []string) {
	raw, ok := index[key]
	if !ok {
		return 0, malformed
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0, append(malformed, key)
	}

	return n, malformed
}

// readTime returns the UTC stamp at key, the zero time when absent, and the zero
// time plus the key recorded as malformed when present but unreadable.
func readTime(index map[string]string, key string, malformed []string) (time.Time, []string) {
	raw, ok := index[key]
	if !ok {
		return time.Time{}, malformed
	}

	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, append(malformed, key)
	}

	return t.UTC(), malformed
}

// headerError builds the single error naming every unreadable header, wrapping
// ErrMalformedOriginCoordinates when one of them was an origin coordinate.
func headerError(malformed []string, origin bool) error {
	if len(malformed) == 0 {
		return nil
	}

	if origin {
		return fmt.Errorf("%w: unreadable headers %v; the origin triple was discarded rather than half-parsed",
			ErrMalformedOriginCoordinates, malformed)
	}

	return fmt.Errorf("streaming: dlq record has unreadable headers %v", malformed)
}
