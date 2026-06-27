package consumer

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// Handler is the ONLY interface a consuming service implements. The library
// owns commit, retry, seek-back, DLQ, tenant scoping, and rebalance safety.
//
// payload is the raw record value (the same bytes the producer published as
// the CloudEvents data section). event carries the parsed CloudEvents context
// attributes — crucially event.TenantID, derived from ce-tenantid via the
// existing codec, NEVER from the payload body.
//
// Handle returning nil = success (the record's offset becomes eligible to
// commit). A non-nil error feeds the disposition state machine, classified by
// error SOURCE (docs/design/consumer.md §7a):
//   - A handler-return error is TERMINAL -> DLQ by default (fail-closed): an
//     error the library does not recognize quarantines ONE record (per-record
//     blast radius, alertable, replayable) rather than wedging the partition.
//     The optional Classifier RECLASSIFIES a known-transient handler error
//     (downstream blip — Midaz/Postgres down) BACK to retry.
//   - A reclassified transient is retried IN-LOOP within a single poll cycle up
//     to RetryBudget; transients NEVER go to the DLQ. A SUSTAINED transient
//     seeks back + blocks its partition head-of-line ("block beats lose").
//   - Codec-decode faults (malformed CloudEvents) are ALWAYS terminal -> DLQ,
//     classified before Handle is ever reached; they are not reclassifiable.
//
// There is NO budget-exhausted -> DLQ path.
type Handler interface {
	Handle(ctx context.Context, event contract.Event, payload []byte) error
}

// Classifier is an OPTIONAL service-supplied hook that RECLASSIFIES a known
// HANDLER-return error as transient (retryable), flipping it off the fail-closed
// terminal default. It runs ONLY for handler-return errors (NOT transport errors,
// which the transport seam classifies, and NOT codec-decode faults, which are
// always terminal). Returning true means "this is a recoverable downstream blip —
// retry it" (e.g. Midaz/Postgres temporarily down); false (or no Classifier) lets
// the error take the fail-closed TERMINAL -> DLQ path.
//
// Money-path / at-least-once-critical consumers MUST supply a Classifier marking
// their known-transient downstream errors as retry — else a transient outage
// over-quarantines into the DLQ (recoverable: the DLQ is replayable, nothing
// lost). The fail-closed default is deliberate: an unrecognized error quarantines
// ONE record (alertable, replayable) instead of wedging the partition.
//
// Classifier is deliberately a func, not an errors.Is sentinel match: error
// recognition must not depend on fragile per-error-value comparisons across
// module boundaries (Req 2).
type Classifier func(err error) bool

// GroupClient is the NARROW franz-go seam the runtime decision logic depends
// on. Factoring the commit/seek/retry/DLQ decisions behind this interface
// makes them unit-testable against a deterministic SCRIPTED FAKE instead of a
// flaky kfake group rejoin (Req 5).
//
// The production implementation wraps *kgo.Client (see kgoGroupClient). The
// method set is exactly what the state machine touches — nothing more — so the
// fake stays small and the real wrapper stays a thin pass-through.
type GroupClient interface {
	// PollFetches blocks for the next batch. With BlockRebalanceOnPoll set,
	// the returned batch is rebalance-frozen until AllowRebalance is called.
	PollFetches(ctx context.Context) kgo.Fetches
	// CommitRecords commits the per-partition watermark (max offset+1) for
	// the supplied records, synchronously.
	CommitRecords(ctx context.Context, recs ...*kgo.Record) error
	// SetOffsets forces the in-session consume cursor — used for seek-back on
	// a halted partition so franz-go does not advance past an uncommitted
	// earlier failure across polls (Req 1).
	SetOffsets(offsets map[string]map[int32]kgo.EpochOffset)
	// AllowRebalance releases a rebalance frozen by BlockRebalanceOnPoll. The
	// runtime calls it exactly once per poll cycle, AFTER all seek-backs are
	// staged, so SetOffsets cannot race a group revoke (Req 3).
	AllowRebalance()
	// Close shuts the client and leaves the group.
	Close()
}

// codecFunc is the tenant/CloudEvents header decoder seam. Production wires
// cloudevents.ParseCloudEventsHeaders; tests can inject a deterministic stub.
// ce-tenantid -> Event.TenantID happens here, never from the payload (the
// doc.go "single biggest operational invariant").
type codecFunc func(headers []kgo.RecordHeader) (contract.Event, error)

// defaultCodec is the production header decoder.
func defaultCodec(headers []kgo.RecordHeader) (contract.Event, error) {
	return cloudevents.ParseCloudEventsHeaders(headers)
}

// dlqPublisher is the seam the runtime uses to republish a poison/terminal
// record to <topic><DLQTopicSuffix>. Production wires transportDLQPublisher
// (the internal transport.TransportAdapter seam — NOT the public Emitter, whose
// catalog/payload/header gates would reject the very poison we must
// quarantine; see docs/design/consumer.md §1). Tests inject a recording fake.
type dlqPublisher interface {
	// PublishDLQ republishes rec to its derived DLQ topic with forensic
	// metadata headers. It must be synchronous and return only after the DLQ
	// record is acknowledged, so the source offset is committed strictly
	// after the quarantine copy is durable.
	PublishDLQ(ctx context.Context, rec *kgo.Record, cause error, retryCount int) error
}

// transportDLQPublisher is the PRODUCTION dlqPublisher. It republishes poison
// records over the internal transport.TransportAdapter seam — mirroring the
// producer's own DLQ path (internal/producer/publish_dlq_route.go:124-130:
// transport.TransportMessage{Payload, Headers, Destination} -> adapter.Publish),
// which bypasses every public-Emitter gate. The adapter is constructed by Build
// from the SAME Brokers/TLS/SASL config the consumer reads with.
type transportDLQPublisher struct {
	adapter transport.TransportAdapter
	suffix  string // DLQ topic suffix, e.g. ".dlq"
	groupID string // written as the quarantining identity (x-lerian-dlq-producer-id)
}

// PublishDLQ builds a payload-verbatim transport.TransportMessage targeting
// <rec.Topic><suffix>, attaches the forensic headers (the original CloudEvents
// headers preserved verbatim, plus the six shared dlqheader keys and the two
// consumer-specific ones), and publishes via the transport adapter. Synchronous:
// it returns only after the adapter acknowledges, so the source offset is
// committed strictly after the quarantine copy is durable.
//
// firstFailureAt is the time of the first handler attempt for this record; the
// runtime threads it so the header reflects when the record first failed, not
// when it was finally quarantined.
func (p *transportDLQPublisher) PublishDLQ(ctx context.Context, rec *kgo.Record, cause error, retryCount int) error {
	if p == nil || transport.IsNilInterface(p.adapter) {
		return contract.ErrNilProducer
	}

	if rec == nil {
		return contract.ErrNilProducer
	}

	// The error class is the transport adapter's classification of the cause.
	// For codec/handler poison this is typically ClassValidation/broker_unavailable;
	// it is forensic metadata only, never a routing decision (routing is decided
	// upstream by the runtime's classify-by-source).
	cls := p.adapter.Classify(cause)

	causeMessage := ""
	if cause != nil {
		causeMessage = contract.SanitizeBrokerURL(cause.Error())
	}

	// Preserve the original CloudEvents headers verbatim, then append the eight
	// forensic headers. The ce-* headers carry ce-tenantid, so tenant identity
	// travels with the quarantined record without a duplicate dlqheader key.
	headers := make([]transport.Header, 0, len(rec.Headers)+8)
	for _, h := range rec.Headers {
		headers = append(headers, transport.Header{Key: h.Key, Value: h.Value})
	}

	headers = append(headers,
		transport.Header{Key: dlqheader.SourceTopic, Value: []byte(rec.Topic)},
		transport.Header{Key: dlqheader.ErrorClass, Value: []byte(cls)},
		transport.Header{Key: dlqheader.ErrorMessage, Value: []byte(causeMessage)},
		transport.Header{Key: dlqheader.RetryCount, Value: []byte(strconv.Itoa(retryCount))},
		transport.Header{Key: dlqheader.FirstFailureAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		transport.Header{Key: dlqheader.ProducerID, Value: []byte(p.groupID)},
		transport.Header{Key: dlqheader.SourcePartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		transport.Header{Key: dlqheader.SourceOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
	)

	message := transport.TransportMessage{
		Destination: contract.Destination{
			Kind: contract.TransportKafkaLike,
			Name: rec.Topic + p.suffix,
		},
		Payload: rec.Value, // payload-verbatim
		Headers: headers,
	}

	return p.adapter.Publish(ctx, transport.CloneMessage(message))
}
