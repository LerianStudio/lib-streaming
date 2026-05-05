//go:build unit

package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

type fakePublisher struct {
	calls []fakeCall
	err   error
}

type fakeCall struct {
	exchange    string
	routingKey  string
	contentType string
	body        []byte
	headers     map[string]any
}

func (f *fakePublisher) Publish(_ context.Context, exchange, routingKey, contentType string, body []byte, headers map[string]any) error {
	f.calls = append(f.calls, fakeCall{
		exchange:    exchange,
		routingKey:  routingKey,
		contentType: contentType,
		body:        append([]byte(nil), body...),
		headers:     headers,
	})

	return f.err
}

type pingPublisher struct {
	*fakePublisher
	pingErr error
}

func (p *pingPublisher) Ping(_ context.Context) error { return p.pingErr }

func sampleMessage() transport.TransportMessage {
	return transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportRabbitMQ, Name: "events", Address: "tx.created"},
		TenantID:    "t-1",
		Key:         "p-1",
		Payload:     []byte(`{"hello":"rabbit"}`),
		Headers: []transport.Header{
			{Key: "ce-id", Value: []byte("evt-1")},
			{Key: "ce-source", Value: []byte("svc://test")},
			{Key: "ce-datacontenttype", Value: []byte("application/json")},
		},
		Attributes: map[string]string{"x-trace": "trc-1"},
	}
}

func TestRabbitMQ_New_RejectsNilPublisher(t *testing.T) {
	t.Parallel()

	if _, err := New(nil); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(nil) error = %v; want ErrNilProducer", err)
	}
}

// TestRabbitMQ_New_RejectsTypedNilPublisher pins the typed-nil-interface
// contract: a statically-typed nil pointer (interface header carries a
// non-nil type tag) MUST surface as ErrNilProducer instead of constructing
// an adapter that NPEs on first Publish. Regression guard for the
// reflect-based IsNilInterface helper at internal/transport.
func TestRabbitMQ_New_RejectsTypedNilPublisher(t *testing.T) {
	t.Parallel()

	var p *fakePublisher

	if _, err := New(p); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(typed-nil) error = %v; want ErrNilProducer", err)
	}
}

func TestRabbitMQ_Publish_ForwardsExchangeAndRoutingKey(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{}
	a, err := New(pub)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := a.Publish(context.Background(), sampleMessage()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := len(pub.calls); got != 1 {
		t.Fatalf("calls = %d; want 1", got)
	}

	call := pub.calls[0]
	if call.exchange != "events" {
		t.Errorf("exchange = %q; want events", call.exchange)
	}
	if call.routingKey != "tx.created" {
		t.Errorf("routingKey = %q; want tx.created", call.routingKey)
	}
	if call.contentType != "application/json" {
		t.Errorf("contentType = %q; want application/json", call.contentType)
	}
	if string(call.body) != `{"hello":"rabbit"}` {
		t.Errorf("body = %q", string(call.body))
	}

	// Headers must include ce-* envelope and the caller attribute.
	if call.headers == nil {
		t.Fatal("headers = nil; want populated map")
	}
	gotID, ok := call.headers["ce-id"].([]byte)
	if !ok || string(gotID) != "evt-1" {
		t.Errorf("headers[ce-id] = %v; want evt-1", call.headers["ce-id"])
	}
	if gotTrace, ok := call.headers["x-trace"].(string); !ok || gotTrace != "trc-1" {
		t.Errorf("headers[x-trace] = %v; want trc-1", call.headers["x-trace"])
	}
}

func TestRabbitMQ_Publish_RejectsKafkaDestination(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{}
	a, _ := New(pub)
	msg := sampleMessage()
	msg.Destination.Kind = contract.TransportKafkaLike
	msg.Destination.Address = ""

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Errorf("Publish() error = %v; want ErrInvalidDestination", err)
	}
}

func TestRabbitMQ_Publish_DefaultContentTypeWhenHeaderAbsent(t *testing.T) {
	t.Parallel()

	pub := &fakePublisher{}
	a, _ := New(pub)
	msg := sampleMessage()
	msg.Headers = msg.Headers[:2] // strip ce-datacontenttype

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := pub.calls[0].contentType; got != DefaultContentType {
		t.Errorf("contentType = %q; want %q", got, DefaultContentType)
	}
}

func TestRabbitMQ_Healthy_DelegatesToPing(t *testing.T) {
	t.Parallel()

	pingErr := errors.New("rabbit down")
	pub := &pingPublisher{fakePublisher: &fakePublisher{}, pingErr: pingErr}
	a, _ := New(pub)

	if err := a.Healthy(context.Background()); !errors.Is(err, pingErr) {
		t.Errorf("Healthy() error = %v; want %v", err, pingErr)
	}
}

func TestRabbitMQ_Healthy_NoPingNoOp(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakePublisher{})

	if err := a.Healthy(context.Background()); err != nil {
		t.Errorf("Healthy() error = %v; want nil", err)
	}
}

func TestRabbitMQ_Classify(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakePublisher{})

	cases := []struct {
		err  error
		want contract.ErrorClass
	}{
		{nil, ""},
		{context.Canceled, contract.ClassContextCanceled},
		{contract.ErrPayloadTooLarge, contract.ClassValidation},
		{contract.ErrInvalidDestination, contract.ClassValidation},
		{errors.New("NO_ROUTE: exchange not found"), contract.ClassTopicNotFound},
		{errors.New("ACCESS_REFUSED: not authorized"), contract.ClassAuth},
		{errors.New("PRECONDITION_FAILED"), contract.ClassBrokerOverloaded},
		{errors.New("connection timed out"), contract.ClassNetworkTimeout},
		{errors.New("some unknown amqp error"), contract.ClassBrokerUnavailable},
	}

	for _, tc := range cases {
		if got := a.Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %q; want %q", tc.err, got, tc.want)
		}
	}
}

func TestRabbitMQ_FlushAndClose_NoOps(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakePublisher{})
	if err := a.Flush(context.Background()); err != nil {
		t.Errorf("Flush() error = %v; want nil", err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
}

func TestRabbitMQ_Kind(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakePublisher{})
	if got := a.Kind(); got != contract.TransportRabbitMQ {
		t.Errorf("Kind() = %q; want %q", got, contract.TransportRabbitMQ)
	}
}
