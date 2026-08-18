//go:build unit

package producer

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/LerianStudio/lib-observability/v2/log"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/dlqheader"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
)

// sizeCappedRouteAdapter fails every publish to the source topic (so the DLQ
// path runs) and enforces a byte budget on the DLQ topic, refusing anything
// over it with the same MESSAGE_TOO_LARGE verdict franz-go surfaces.
//
// It models the real shape of the problem: a DLQ copy carries the payload plus
// every original header plus the forensic set, so it is strictly larger than
// the record it quarantines and can be refused where the original was accepted.
type sizeCappedRouteAdapter struct {
	mu          sync.Mutex
	sourceTopic string
	sourceErr   error
	dlqMaxBytes int
	dlqMessages []transport.TransportMessage
}

func (*sizeCappedRouteAdapter) Kind() contract.TransportKind { return contract.TransportKafkaLike }

func (a *sizeCappedRouteAdapter) Publish(_ context.Context, message transport.TransportMessage) error {
	if message.Destination.Name == a.sourceTopic {
		return a.sourceErr
	}

	size := len(message.Payload)
	for _, h := range message.Headers {
		size += len(h.Key) + len(h.Value)
	}

	if a.dlqMaxBytes > 0 && size > a.dlqMaxBytes {
		return kerr.MessageTooLarge
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.dlqMessages = append(a.dlqMessages, transport.CloneMessage(message))

	return nil
}

func (*sizeCappedRouteAdapter) Healthy(context.Context) error { return nil }
func (*sizeCappedRouteAdapter) Flush(context.Context) error   { return nil }
func (*sizeCappedRouteAdapter) Close(context.Context) error   { return nil }

func (*sizeCappedRouteAdapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	return contract.ClassBrokerUnavailable
}

func (a *sizeCappedRouteAdapter) dlq() []transport.TransportMessage {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]transport.TransportMessage(nil), a.dlqMessages...)
}

// dlqHeader reads one header off a recorded DLQ message.
func dlqHeader(message transport.TransportMessage, key string) (string, bool) {
	for _, h := range message.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

// newSizeCappedProducer wires a single-target producer whose only route fails
// and whose DLQ enforces dlqMaxBytes.
func newSizeCappedProducer(t *testing.T, adapter *sizeCappedRouteAdapter) *Producer {
	t.Helper()

	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", adapter.sourceTopic, contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		context.Background(),
		MultiProducerConfig{Source: "svc-dlq-size"},
		nil,
		[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: adapter}},
		routes,
		sampleCatalog(t),
		WithLogger(log.NewNop()),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	t.Cleanup(func() { _ = p.Close() })

	return p
}

// TestPublishRouteDLQ_OversizeCopyFallsBackToPayloadOmitted proves a DLQ copy
// the broker refuses on size is still quarantined — without its payload, and
// marked as such.
//
// A DLQ record is strictly larger than the record it quarantines, so a near-cap
// payload has no room for the forensic headers. Losing the entry entirely loses
// the only evidence the emit ever happened; keeping the headers keeps the event
// id, tenant, and error class, which is what an operator actually triages on.
func TestPublishRouteDLQ_OversizeCopyFallsBackToPayloadOmitted(t *testing.T) {
	t.Parallel()

	adapter := &sizeCappedRouteAdapter{
		sourceTopic: "lerian.streaming.svc-dlq-size",
		sourceErr:   errors.New("simulated source publish failure"),
		dlqMaxBytes: 2000,
	}

	p := newSizeCappedProducer(t, adapter)

	request := eventToRequest(sampleEvent())
	request.Payload = []byte(`{"blob":"` + strings.Repeat("p", 8000) + `"}`)

	if err := p.Emit(context.Background(), request); err == nil {
		t.Fatal("Emit() = nil; want the source-route failure to surface")
	}

	msgs := adapter.dlq()
	if len(msgs) != 1 {
		t.Fatalf("DLQ accepted %d messages; want 1 (the payload-omitted retry)", len(msgs))
	}

	if len(msgs[0].Payload) != 0 {
		t.Errorf("slim DLQ copy carries %d payload bytes; want 0", len(msgs[0].Payload))
	}

	if got, ok := dlqHeader(msgs[0], dlqheader.PayloadOmitted); !ok || got != "true" {
		t.Errorf("%s = %q (present=%v); want %q", dlqheader.PayloadOmitted, got, ok, "true")
	}

	if got, ok := dlqHeader(msgs[0], dlqheader.PayloadBytes); !ok || got != strconv.Itoa(len(request.Payload)) {
		t.Errorf("%s = %q (present=%v); want %q", dlqheader.PayloadBytes, got, ok, strconv.Itoa(len(request.Payload)))
	}
}

// TestPublishRouteDLQ_BoundsTheErrorMessageHeader proves the sanitized cause
// string cannot grow a DLQ copy without limit. It is the one unbounded input on
// the record, and the copy has to fit where the original did.
func TestPublishRouteDLQ_BoundsTheErrorMessageHeader(t *testing.T) {
	t.Parallel()

	adapter := &sizeCappedRouteAdapter{
		sourceTopic: "lerian.streaming.svc-dlq-size",
		sourceErr:   errors.New(strings.Repeat("e", dlqheader.MaxErrorMessageBytes*3)),
	}

	p := newSizeCappedProducer(t, adapter)

	if err := p.Emit(context.Background(), eventToRequest(sampleEvent())); err == nil {
		t.Fatal("Emit() = nil; want the source-route failure to surface")
	}

	msgs := adapter.dlq()
	if len(msgs) != 1 {
		t.Fatalf("DLQ accepted %d messages; want 1", len(msgs))
	}

	got, ok := dlqHeader(msgs[0], dlqheader.ErrorMessage)
	if !ok {
		t.Fatal("error-message header missing")
	}

	if len(got) > dlqheader.MaxErrorMessageBytes {
		t.Errorf("error-message header = %d bytes; want <= %d", len(got), dlqheader.MaxErrorMessageBytes)
	}
}
