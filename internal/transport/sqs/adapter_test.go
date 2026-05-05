//go:build unit

package sqs

import (
	"context"
	"errors"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

type fakeClient struct {
	calls   []fakeCall
	sendErr error
	pingErr error
}

type fakeCall struct {
	queueURL   string
	body       []byte
	attributes []Attribute
}

func (f *fakeClient) SendMessage(_ context.Context, queueURL string, body []byte, attributes []Attribute) error {
	f.calls = append(f.calls, fakeCall{queueURL: queueURL, body: append([]byte(nil), body...), attributes: append([]Attribute(nil), attributes...)})
	return f.sendErr
}

type fakeClientPing struct {
	*fakeClient
}

func (f *fakeClientPing) Ping(_ context.Context) error { return f.pingErr }

func newPingClient(send, ping error) *fakeClientPing {
	return &fakeClientPing{fakeClient: &fakeClient{sendErr: send, pingErr: ping}}
}

func sampleMessage(queueURL string, payload []byte) transport.TransportMessage {
	return transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportSQS, Address: queueURL},
		TenantID:    "t-1",
		Key:         "p-1",
		Payload:     payload,
		Headers: []transport.Header{
			{Key: "ce-id", Value: []byte("evt-1")},
			{Key: "ce-source", Value: []byte("svc://test")},
		},
		Attributes: map[string]string{"x-trace": "trc-1"},
	}
}

func TestSQS_New_RejectsNilClient(t *testing.T) {
	t.Parallel()

	if _, err := New(nil, "https://sqs.us-east-1.amazonaws.com/123/q"); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(nil) error = %v; want ErrNilProducer", err)
	}
}

// TestSQS_New_RejectsTypedNilClient pins the typed-nil-interface contract:
// passing a statically-typed nil pointer (the classic Go gotcha — interface
// header carries a non-nil type tag, so `client == nil` is false) MUST
// surface as ErrNilProducer instead of constructing an adapter that NPEs on
// first Publish. Regression guard for the reflect-based IsNilInterface
// helper at internal/transport.IsNilInterface.
func TestSQS_New_RejectsTypedNilClient(t *testing.T) {
	t.Parallel()

	var c *fakeClient // typed-nil pointer; assigning to interface yields typed-nil interface

	if _, err := New(c, "https://sqs.us-east-1.amazonaws.com/123/q"); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(typed-nil) error = %v; want ErrNilProducer", err)
	}
}

func TestSQS_New_ValidatesDefaultQueueURL(t *testing.T) {
	t.Parallel()

	_, err := New(&fakeClient{}, "ftp://bad-scheme/queue")
	if err == nil {
		t.Errorf("New() with bad URL: error = nil; want validation failure")
	}
}

func TestSQS_Publish_ForwardsBodyAndAttributes(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, err := New(client, "")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{"hello":"world"}`))

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := len(client.calls); got != 1 {
		t.Fatalf("calls = %d; want 1", got)
	}

	call := client.calls[0]
	if call.queueURL != "https://sqs.us-east-1.amazonaws.com/123/q" {
		t.Errorf("queueURL = %q", call.queueURL)
	}
	if string(call.body) != `{"hello":"world"}` {
		t.Errorf("body = %q", string(call.body))
	}

	wantKeys := map[string]string{
		"ce-id":     "evt-1",
		"ce-source": "svc://test",
		"x-trace":   "trc-1",
	}
	got := map[string]string{}
	for _, a := range call.attributes {
		got[a.Key] = string(a.Value)
	}
	for k, v := range wantKeys {
		if got[k] != v {
			t.Errorf("attribute %q = %q; want %q", k, got[k], v)
		}
	}
}

func TestSQS_Publish_HonorsDestinationAddress(t *testing.T) {
	t.Parallel()

	// Destination.Validate enforces a non-empty SQS Address at the
	// contract layer, so the adapter's defaultQueueURL fallback is a
	// belt-and-suspenders guard rather than a publish-time path. This
	// test confirms a fully-populated Destination wins over the
	// configured default — the documented precedence.
	defaultURL := "https://sqs.us-east-1.amazonaws.com/123/default"
	overrideURL := "https://sqs.us-east-1.amazonaws.com/123/override"
	client := &fakeClient{}
	a, err := New(client, defaultURL)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	msg := sampleMessage(overrideURL, []byte(`{}`))

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if client.calls[0].queueURL != overrideURL {
		t.Errorf("queueURL = %q; want %q (Destination.Address must win)", client.calls[0].queueURL, overrideURL)
	}
}

func TestSQS_Publish_RejectsWrongDestinationKind(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	msg.Destination.Kind = contract.TransportRabbitMQ
	msg.Destination.Name = "exchange"
	msg.Destination.Address = "rk"

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Errorf("Publish() with rabbitmq destination: error = %v; want ErrInvalidDestination", err)
	}
}

func TestSQS_Publish_PayloadCap(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")
	tooBig := make([]byte, MaxBodyBytes+1)
	for i := range tooBig {
		tooBig[i] = 'a'
	}

	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", tooBig)

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Errorf("Publish() error = %v; want ErrPayloadTooLarge", err)
	}
}

func TestSQS_Healthy_DelegatesToPing(t *testing.T) {
	t.Parallel()

	pingErr := errors.New("sqs ping down")
	client := newPingClient(nil, pingErr)
	a, _ := New(client, "")

	if err := a.Healthy(context.Background()); !errors.Is(err, pingErr) {
		t.Errorf("Healthy() error = %v; want %v", err, pingErr)
	}
}

func TestSQS_Healthy_NoPingNoOp(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")

	if err := a.Healthy(context.Background()); err != nil {
		t.Errorf("Healthy() with no Ping support error = %v; want nil", err)
	}
}

func TestSQS_Classify(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")

	cases := []struct {
		err  error
		want contract.ErrorClass
	}{
		{nil, ""},
		{context.Canceled, contract.ClassContextCanceled},
		{context.DeadlineExceeded, contract.ClassContextCanceled},
		{contract.ErrPayloadTooLarge, contract.ClassValidation},
		{contract.ErrInvalidDestination, contract.ClassValidation},
		{errors.New("QueueDoesNotExist"), contract.ClassTopicNotFound},
		{errors.New("AccessDenied"), contract.ClassAuth},
		{errors.New("Throttling"), contract.ClassBrokerOverloaded},
		{errors.New("connection timed out"), contract.ClassNetworkTimeout},
		{errors.New("some unknown sqs error"), contract.ClassBrokerUnavailable},
	}

	for _, tc := range cases {
		if got := a.Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %q; want %q", tc.err, got, tc.want)
		}
	}
}

func TestSQS_Publish_PropagatesClientError(t *testing.T) {
	t.Parallel()

	clientErr := errors.New("Throttling: rate exceeded")
	client := &fakeClient{sendErr: clientErr}
	a, _ := New(client, "")

	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	if err := a.Publish(context.Background(), msg); !errors.Is(err, clientErr) {
		t.Errorf("Publish() error = %v; want %v", err, clientErr)
	}

	if got := a.Classify(clientErr); got != contract.ClassBrokerOverloaded {
		t.Errorf("Classify(throttling) = %q; want %q", got, contract.ClassBrokerOverloaded)
	}
}

func TestSQS_Kind(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")
	if got := a.Kind(); got != contract.TransportSQS {
		t.Errorf("Kind() = %q; want %q", got, contract.TransportSQS)
	}
}

func TestSQS_FlushAndClose_NoOps(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")
	if err := a.Flush(context.Background()); err != nil {
		t.Errorf("Flush() error = %v; want nil", err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
}
