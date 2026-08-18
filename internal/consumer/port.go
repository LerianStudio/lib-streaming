package consumer

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v3/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
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
// record to THIS consumer application's own DLQ topic. Production wires
// transportDLQPublisher (the internal transport.TransportAdapter seam — NOT the
// public Emitter, whose catalog/payload/header gates would reject the very
// poison we must quarantine; see docs/design/consumer.md §1). Tests inject a
// recording fake.
type dlqPublisher interface {
	// PublishDLQ republishes rec to the consumer's own DLQ topic with forensic
	// metadata headers. It must be synchronous and return only after the DLQ
	// record is acknowledged, so the source offset is committed strictly
	// after the quarantine copy is durable.
	PublishDLQ(ctx context.Context, rec *kgo.Record, cause error, causeKind string, retryCount int) error
	// Close flushes and shuts the underlying publisher (its own produce-side
	// client). The runtime calls it from consumerRuntime.Close so the DLQ client
	// and any buffered quarantine writes are not leaked/stranded. Idempotent.
	Close(ctx context.Context) error
}

// transportDLQPublisher is the PRODUCTION dlqPublisher. It republishes poison
// records over the internal transport.TransportAdapter seam — mirroring the
// producer's own DLQ path (internal/producer/publish_dlq_route.go:124-130:
// transport.TransportMessage{Payload, Headers, Destination} -> adapter.Publish),
// which bypasses every public-Emitter gate. The adapter is constructed by Build
// from the SAME Brokers/TLS/SASL config the consumer reads with.
type transportDLQPublisher struct {
	adapter transport.TransportAdapter
	// dlqTopic is THIS consumer application's own dead-letter topic,
	// "lerian.streaming.<consumer-source>.dlq" — never the producer's.
	// Quarantining is the consuming application's act, so it lands on the
	// consuming application's topic: every app writes exactly two names, its
	// topic and its .dlq, and a filling DLQ names the team that owns the fix.
	dlqTopic string
	groupID  string // written as the quarantining identity (x-lerian-dlq-producer-id)
}

// PublishDLQ builds a payload-verbatim transport.TransportMessage targeting this
// CONSUMER's own DLQ topic, attaches the forensic headers (the original
// CloudEvents headers preserved verbatim, plus the six shared dlqheader keys and
// the three consumer-specific ones), and publishes via the transport adapter.
// Synchronous: it returns only after the adapter acknowledges, so the source
// offset is committed strictly after the quarantine copy is durable.
//
// Because the DLQ topic no longer implies the source topic, the origin
// coordinates are the only route back to the poison record — x-lerian-dlq-
// source-topic / -source-partition / -source-offset carry them on every entry.
//
// SIZE: a quarantine copy is strictly LARGER than the record it quarantines
// (same payload, same headers, plus the forensic set), so a near-cap record can
// fail to fit. When the transport refuses it on size, PublishDLQ retries ONCE
// with the payload omitted and says so in the headers; the payload stays
// recoverable from the source topic via the origin coordinates. Anything else
// fails on the first attempt and the runtime halts the partition fail-closed.
//
// firstFailureAt is stamped time.Now() at publish — the QUARANTINE-verdict time,
// not necessarily the first-ever failure. For a record that fails terminally on
// attempt 0 (codec faults, default-classified handler errors) the two coincide.
// A record that first fails TRANSIENTLY, seeks back, and is DLQ'd on a later
// poll's terminal verdict has an earlier true first-failure that is NOT carried
// through the seam — the stamp then reflects quarantine time, lagging the first
// failure. This is forensic metadata only (never a routing decision), so the skew
// on the retry-then-terminal path is accepted, not threaded as a captured time.
func (p *transportDLQPublisher) PublishDLQ(ctx context.Context, rec *kgo.Record, cause error, causeKind string, retryCount int) error {
	if p == nil || transport.IsNilInterface(p.adapter) {
		return contract.ErrNilProducer
	}

	if rec == nil {
		return contract.ErrNilProducer
	}

	headers := p.forensicHeaders(rec, cause, causeKind, retryCount)

	message := transport.TransportMessage{
		Destination: contract.Destination{
			Kind: contract.TransportKafkaLike,
			Name: p.dlqTopic,
		},
		Key:     string(rec.Key), // preserve the original key: verbatim republish + stable DLQ partitioning for replay
		Payload: rec.Value,       // payload-verbatim
		Headers: headers,
	}

	err := p.adapter.Publish(ctx, transport.CloneMessage(message))
	if err == nil || !dlqheader.IsSizeError(err) {
		return err
	}

	// The record cannot fit in the DLQ with its payload. Quarantining a marked
	// copy WITHOUT the payload beats failing the quarantine: a failed quarantine
	// is fail-closed, and fail-closed on a record that can NEVER be quarantined
	// is a partition wedged forever — under one topic per app, the producing
	// application's whole catalog stuck behind one record.
	message.Payload = nil
	message.Headers = append(slices.Clone(headers),
		transport.Header{Key: dlqheader.PayloadOmitted, Value: []byte("true")},
		transport.Header{Key: dlqheader.PayloadBytes, Value: []byte(strconv.Itoa(len(rec.Value)))},
	)

	if slimErr := p.adapter.Publish(ctx, transport.CloneMessage(message)); slimErr != nil {
		return fmt.Errorf("dlq record too large and the payload-omitted retry also failed: %w", slimErr)
	}

	return nil
}

// forensicHeaders returns the original record headers verbatim followed by the
// nine forensic keys. The ce-* headers carry ce-tenantid, so tenant identity
// travels with the quarantined record without a duplicate dlqheader key.
func (p *transportDLQPublisher) forensicHeaders(rec *kgo.Record, cause error, causeKind string, retryCount int) []transport.Header {
	// The error class is the transport adapter's classification of the cause.
	// For codec/handler poison this is typically ClassValidation/broker_unavailable;
	// it is forensic metadata only, never a routing decision (routing is decided
	// upstream by the runtime's classify-by-source).
	cls := p.adapter.Classify(cause)

	causeMessage := ""
	if cause != nil {
		// Bounded: this is the one unbounded value on the record, and the copy
		// has to fit where the original did. See dlqheader.MaxErrorMessageBytes.
		causeMessage = dlqheader.TruncateErrorMessage(contract.SanitizeBrokerURL(cause.Error()))
	}

	headers := make([]transport.Header, 0, len(rec.Headers)+9)
	for _, h := range rec.Headers {
		headers = append(headers, transport.Header{Key: h.Key, Value: h.Value})
	}

	return append(headers,
		transport.Header{Key: dlqheader.SourceTopic, Value: []byte(rec.Topic)},
		transport.Header{Key: dlqheader.ErrorClass, Value: []byte(cls)},
		transport.Header{Key: dlqheader.ErrorMessage, Value: []byte(causeMessage)},
		transport.Header{Key: dlqheader.RetryCount, Value: []byte(strconv.Itoa(retryCount))},
		transport.Header{Key: dlqheader.FirstFailureAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		transport.Header{Key: dlqheader.ProducerID, Value: []byte(p.groupID)},
		transport.Header{Key: dlqheader.SourcePartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		transport.Header{Key: dlqheader.SourceOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		transport.Header{Key: dlqheader.CauseKind, Value: []byte(causeKind)},
	)
}

// Close flushes and shuts the DLQ adapter's produce-side client. Idempotent and
// nil-safe: a publisher with no adapter (defensive) is a no-op. consumerRuntime.
// Close calls this alongside the consume-client close so the second franz-go
// client Build created for DLQ publishing is not leaked.
func (p *transportDLQPublisher) Close(ctx context.Context) error {
	if p == nil || transport.IsNilInterface(p.adapter) {
		return nil
	}

	return p.adapter.Close(ctx)
}
