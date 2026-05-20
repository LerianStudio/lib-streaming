//go:build unit

package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

	client := &fakeClient{}
	a, _ := New(client, "")
	tooBig := make([]byte, MaxBodyBytes+1)
	for i := range tooBig {
		tooBig[i] = 'a'
	}

	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", tooBig)

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Errorf("Publish() error = %v; want ErrPayloadTooLarge", err)
	}
	if len(client.calls) != 0 {
		t.Fatalf("SendMessage calls = %d; want 0 for oversized payload", len(client.calls))
	}
}

func TestSQS_Publish_AttributeWireSizeCap(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	msg.Attributes = map[string]string{"oversized": strings.Repeat("a", MaxBodyBytes)}

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Fatalf("Publish() error = %v; want ErrPayloadTooLarge", err)
	}
	if len(client.calls) != 0 {
		t.Fatalf("SendMessage calls = %d; want 0 for oversized wire message", len(client.calls))
	}
}

func TestSQS_Publish_PacksOverflowAttributes(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	msg.Headers = append(msg.Headers,
		transport.Header{Key: "ce-specversion", Value: []byte("1.0")},
		transport.Header{Key: "ce-type", Value: []byte("studio.lerian.transaction.created")},
		transport.Header{Key: "ce-time", Value: []byte("2026-05-04T12:00:00Z")},
		transport.Header{Key: "ce-schemaversion", Value: []byte("1.0.0")},
		transport.Header{Key: "ce-resourcetype", Value: []byte("transaction")},
		transport.Header{Key: "ce-eventtype", Value: []byte("created")},
		transport.Header{Key: "ce-tenantid", Value: []byte("tenant-1")},
		transport.Header{Key: "ce-subject", Value: []byte("tx-1")},
		transport.Header{Key: "ce-datacontenttype", Value: []byte("application/json")},
		transport.Header{Key: "x-lerian-dlq-error-class", Value: []byte("auth")},
	)

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v; want nil with packed overflow metadata", err)
	}
	if len(client.calls) != 1 {
		t.Fatalf("SendMessage calls = %d; want 1", len(client.calls))
	}
	if got := len(client.calls[0].attributes); got > maxMessageAttributes {
		t.Fatalf("attributes = %d; want <= %d", got, maxMessageAttributes)
	}

	attrs := map[string]string{}
	for _, attr := range client.calls[0].attributes {
		attrs[attr.Key] = string(attr.Value)
	}

	if attrs["ce-tenantid"] != "tenant-1" {
		t.Fatalf("ce-tenantid = %q; want tenant-1", attrs["ce-tenantid"])
	}

	var overflow map[string]string
	if err := json.Unmarshal([]byte(attrs[metadataOverflowAttribute]), &overflow); err != nil {
		t.Fatalf("overflow metadata unmarshal error = %v; raw=%q", err, attrs[metadataOverflowAttribute])
	}
	if overflow["ce-subject"] != "tx-1" {
		t.Fatalf("overflow ce-subject = %q; want tx-1", overflow["ce-subject"])
	}
	if overflow["x-lerian-dlq-error-class"] != "auth" {
		t.Fatalf("overflow dlq class = %q; want auth", overflow["x-lerian-dlq-error-class"])
	}
}

func TestSQS_Publish_KeepsTraceHeadersTopLevelWhenAttributesOverflow(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	msg.Headers = append(msg.Headers,
		transport.Header{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
		transport.Header{Key: "tracestate", Value: []byte("vendor=value")},
		transport.Header{Key: "ce-specversion", Value: []byte("1.0")},
		transport.Header{Key: "ce-type", Value: []byte("studio.lerian.transaction.created")},
		transport.Header{Key: "ce-time", Value: []byte("2026-05-04T12:00:00Z")},
		transport.Header{Key: "ce-schemaversion", Value: []byte("1.0.0")},
		transport.Header{Key: "ce-resourcetype", Value: []byte("transaction")},
		transport.Header{Key: "ce-eventtype", Value: []byte("created")},
		transport.Header{Key: "ce-tenantid", Value: []byte("tenant-1")},
		transport.Header{Key: "ce-datacontenttype", Value: []byte("application/json")},
	)

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	attrs := map[string]string{}
	for _, attr := range client.calls[0].attributes {
		attrs[attr.Key] = string(attr.Value)
	}
	if attrs["traceparent"] == "" {
		t.Fatalf("traceparent missing from top-level attributes: %#v", attrs)
	}
	if attrs["tracestate"] == "" {
		t.Fatalf("tracestate missing from top-level attributes: %#v", attrs)
	}
}

func TestSQS_Publish_AttributeBoundary(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com/123/q", []byte(`{}`))
	msg.Headers = nil
	msg.Attributes = map[string]string{}
	for i := 0; i < maxMessageAttributes; i++ {
		msg.Attributes[string(rune('a'+i))] = "v"
	}

	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() exactly cap error = %v; want nil", err)
	}
	if got := len(client.calls[0].attributes); got != maxMessageAttributes {
		t.Fatalf("attributes = %d; want %d", got, maxMessageAttributes)
	}

	msg.Attributes["z"] = "v"
	if err := a.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() over cap packed error = %v; want nil", err)
	}
	if got := len(client.calls[1].attributes); got > maxMessageAttributes {
		t.Fatalf("packed attributes = %d; want <= %d", got, maxMessageAttributes)
	}
}

func TestSQS_Publish_RejectsNonAWSHost(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://example.com/123/q", []byte(`{}`))

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Fatalf("Publish() error = %v; want ErrInvalidDestination", err)
	}
	if len(client.calls) != 0 {
		t.Fatalf("SendMessage calls = %d; want 0 for non-AWS host", len(client.calls))
	}
}

func TestSQS_Publish_RejectsQueueURLWithoutPath(t *testing.T) {
	t.Parallel()

	client := &fakeClient{}
	a, _ := New(client, "")
	msg := sampleMessage("https://sqs.us-east-1.amazonaws.com", []byte(`{}`))

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Fatalf("Publish() error = %v; want ErrInvalidDestination", err)
	}
	if len(client.calls) != 0 {
		t.Fatalf("SendMessage calls = %d; want 0 for URL without queue path", len(client.calls))
	}
}

func TestSQS_Publish_RejectsUnsafeQueueURLShapes(t *testing.T) {
	t.Parallel()

	tests := []string{
		"https://user:pass@sqs.us-east-1.amazonaws.com/123/q",
		"https://sqs.us-east-1.amazonaws.com/123/q?X-Amz-Signature=abc",
		"https://sqs.us-east-1.amazonaws.com/123/q#token=secret",
		"//sqs.us-east-1.amazonaws.com/123/q",
		"https://sqs.us-east-1.amazonaws.com/",
		"https://sqs.us-east-1.amazonaws.com/123",
	}

	for _, queueURL := range tests {
		queueURL := queueURL
		t.Run(queueURL, func(t *testing.T) {
			t.Parallel()

			client := &fakeClient{}
			a, _ := New(client, "")
			msg := sampleMessage(queueURL, []byte(`{}`))

			if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
				t.Fatalf("Publish() error = %v; want ErrInvalidDestination", err)
			}
			if len(client.calls) != 0 {
				t.Fatalf("SendMessage calls = %d; want 0 for unsafe queue URL", len(client.calls))
			}
		})
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

func TestSQS_Healthy_NoPingFails(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{}, "")

	if err := a.Healthy(context.Background()); err == nil {
		t.Errorf("Healthy() with no Ping support error = nil; want failure")
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
