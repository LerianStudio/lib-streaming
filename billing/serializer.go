package billing

import (
	"context"
	"errors"
	"fmt"

	// Blank import required for the //go:embed directive below.
	_ "embed"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	billingv1 "github.com/LerianStudio/lib-streaming/v4/billing/gen/lerian/streaming/billing/v1"
)

// schemaSubject is the Schema Registry subject the billable-payload schema is
// registered under. It is the TopicNameStrategy "-value" subject for
// billing.Topic's registry-facing name: the wire contract is a single, registry-
// enforced source of truth shared by every producer and consumer.
const schemaSubject = "lerian.streaming.billing.recorded-value"

// billableProtoSchema is the exact .proto source registered as the schema text.
// Embedding the file (rather than re-declaring it as a Go string literal) keeps
// the registered schema byte-identical to the generated Go type's source of
// truth, so the registry and the generated BillablePayload never drift.
//
//go:embed proto/lerian/streaming/billing/v1/billable_payload.proto
var billableProtoSchema string

// Serializer encodes a BillablePayload into the Confluent-Protobuf wire format
// for the billing emit path. It resolves the schema id ONCE at construction and
// caches it inside a franz-go serde, so Serialize performs no per-call registry
// I/O.
//
// The emitted bytes carry the Confluent wire prefix ([0x00][4-byte BE schema id]
// [message-index]) ahead of the binary protobuf body — the format a Schema-
// Registry-aware consumer expects. Producers set the payload content type to
// DataContentType so the produce path ships these bytes verbatim rather than
// treating them as JSON.
type Serializer struct {
	serde *sr.Serde
}

// NewSerializer resolves the billable-payload schema id against client and
// returns a Serializer bound to it. It looks the schema up first and falls back
// to creating it, so a first-run producer self-registers the schema while
// steady-state producers reuse the already-registered id.
//
// client must be a ready *sr.Client (the caller wires it from configuration);
// NewSerializer reads no environment or config itself. A nil client is a wiring
// bug and is reported as an error rather than deferred to a nil-dereference on
// first use.
func NewSerializer(ctx context.Context, client *sr.Client) (*Serializer, error) {
	if client == nil {
		return nil, errors.New("billing: schema registry client is required")
	}

	schema := sr.Schema{Schema: billableProtoSchema, Type: sr.TypeProtobuf}

	subjectSchema, err := client.LookupSchema(ctx, schemaSubject, schema)
	if err != nil {
		subjectSchema, err = client.CreateSchema(ctx, schemaSubject, schema)
		if err != nil {
			return nil, fmt.Errorf("billing: resolve schema id for subject %q: %w", schemaSubject, err)
		}
	}

	serde := sr.NewSerde()
	// sr.Index(0) is MANDATORY for protobuf: BillablePayload is the first
	// top-level message in the .proto, so its message-index is [0]. The default
	// ConfluentHeader then frames the id and index ahead of the body — no manual
	// framing here.
	//
	// Only an EncodeFn is registered: the Serializer is an encode-only producer
	// path (there is no Decode entry point), so a DecodeFn would be dead code.
	// sr.Register accepts an encode-only registration.
	serde.Register(
		subjectSchema.ID,
		&billingv1.BillablePayload{},
		sr.Index(0),
		sr.EncodeFn(func(v any) ([]byte, error) {
			msg, ok := v.(*billingv1.BillablePayload)
			if !ok {
				return nil, fmt.Errorf("billing: encode expected *BillablePayload, got %T", v)
			}

			return proto.Marshal(msg)
		}),
	)

	return &Serializer{serde: serde}, nil
}

// Serialize validates p and returns its Confluent-Protobuf encoding. It runs
// Validate first and RETURNS any violation as an error — it never panics, even
// on a caller-constructed invalid payload — so the emit path branches on the
// error rather than crashing the producer.
//
// A nil receiver or an uninitialized serde (a Serializer not built through
// NewSerializer) is reported as an error rather than a nil-dereference panic,
// so a Phase 2 caller that mis-wires construction fails closed.
//
// Serialize is safe for concurrent use by multiple goroutines once the
// Serializer is constructed: sr.Serde.Encode is concurrency-safe, and the
// serde is not mutated after NewSerializer returns.
func (s *Serializer) Serialize(p *BillablePayload) ([]byte, error) {
	if s == nil || s.serde == nil {
		return nil, errors.New("billing: serializer is not initialized; construct it with NewSerializer")
	}

	if err := Validate(p); err != nil {
		return nil, err
	}

	return s.serde.Encode(p)
}
