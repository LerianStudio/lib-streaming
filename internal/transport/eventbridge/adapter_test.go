//go:build unit

package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

type fakeClient struct {
	calls []fakeCall
	err   error
}

type fakeCall struct {
	entries []Entry
}

func (f *fakeClient) PutEvents(_ context.Context, entries []Entry) error {
	f.calls = append(f.calls, fakeCall{entries: append([]Entry(nil), entries...)})
	return f.err
}

type fakeResultClient struct {
	fakeClient
	result PutEventsResult
}

func (f *fakeResultClient) PutEventsWithResult(_ context.Context, entries []Entry) (PutEventsResult, error) {
	f.calls = append(f.calls, fakeCall{entries: append([]Entry(nil), entries...)})
	return f.result, f.err
}

type fakePingClient struct {
	*fakeClient
	pingErr error
}

func (f *fakePingClient) Ping(_ context.Context) error { return f.pingErr }

func sampleMessage() transport.TransportMessage {
	return transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportEventBridge, Name: "default-bus"},
		TenantID:    "t-1",
		Key:         "p-1",
		Payload:     []byte(`{"hello":"eb"}`),
		Headers: []transport.Header{
			{Key: "ce-id", Value: []byte("evt-1")},
			{Key: "ce-source", Value: []byte("svc://test")},
			{Key: "ce-type", Value: []byte("transaction.created")},
			{Key: "ce-time", Value: []byte("2026-05-04T12:00:00Z")},
			{Key: "ce-specversion", Value: []byte("1.0")},
			{Key: "ce-datacontenttype", Value: []byte("application/json")},
			{Key: "ce-tenantid", Value: []byte("t-1")},
			{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
			{Key: "tracestate", Value: []byte("vendor=value")},
		},
		Attributes: map[string]string{"eventbridge.resources": "arn:aws:events:us-east-1:123:rule/r1"},
	}
}

func TestEventBridge_New_RejectsNilClient(t *testing.T) {
	t.Parallel()

	if _, err := New(nil); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(nil) error = %v; want ErrNilProducer", err)
	}
}

// TestEventBridge_New_RejectsTypedNilClient pins the typed-nil-interface
// contract: a statically-typed nil pointer (interface header carries a
// non-nil type tag) MUST surface as ErrNilProducer instead of constructing
// an adapter that NPEs on first PutEvents. Regression guard for the
// reflect-based IsNilInterface helper at internal/transport.
func TestEventBridge_New_RejectsTypedNilClient(t *testing.T) {
	t.Parallel()

	var c *fakeClient

	if _, err := New(c); !errors.Is(err, contract.ErrNilProducer) {
		t.Errorf("New(typed-nil) error = %v; want ErrNilProducer", err)
	}
}

func TestEventBridge_Publish_RendersCanonicalDetail(t *testing.T) {
	t.Parallel()

	cli := &fakeClient{}
	a, err := New(cli)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := a.Publish(context.Background(), sampleMessage()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := len(cli.calls); got != 1 {
		t.Fatalf("calls = %d; want 1", got)
	}
	entries := cli.calls[0].entries
	if got := len(entries); got != 1 {
		t.Fatalf("entries = %d; want 1", got)
	}

	e := entries[0]
	if e.EventBusName != "default-bus" {
		t.Errorf("EventBusName = %q; want default-bus", e.EventBusName)
	}
	if e.Source != "svc://test" {
		t.Errorf("Source = %q; want svc://test", e.Source)
	}
	if e.DetailType != "transaction.created" {
		t.Errorf("DetailType = %q; want transaction.created", e.DetailType)
	}
	if got, want := e.Resources, []string{"arn:aws:events:us-east-1:123:rule/r1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Resources = %v; want %v", got, want)
	}

	var detail map[string]json.RawMessage
	if err := json.Unmarshal(e.Detail, &detail); err != nil {
		t.Fatalf("Unmarshal(detail) error = %v", err)
	}

	want := map[string]string{
		"specversion":     `"1.0"`,
		"id":              `"evt-1"`,
		"source":          `"svc://test"`,
		"type":            `"transaction.created"`,
		"time":            `"2026-05-04T12:00:00Z"`,
		"datacontenttype": `"application/json"`,
		"tenantid":        `"t-1"`,
		"traceparent":     `"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"`,
		"tracestate":      `"vendor=value"`,
		"data":            `{"hello":"eb"}`,
	}

	for k, v := range want {
		got, ok := detail[k]
		if !ok {
			t.Errorf("detail missing %q", k)
			continue
		}
		if string(got) != v {
			t.Errorf("detail[%q] = %s; want %s", k, string(got), v)
		}
	}
}

func TestEventBridge_Publish_RejectsWrongKind(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})
	msg := sampleMessage()
	msg.Destination = contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs/q"}

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Errorf("Publish() error = %v; want ErrInvalidDestination", err)
	}
}

func TestEventBridge_Publish_PayloadCap(t *testing.T) {
	t.Parallel()

	cli := &fakeClient{}
	a, _ := New(cli)
	msg := sampleMessage()
	msg.Payload = make([]byte, MaxBodyBytes+1)

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Errorf("Publish() error = %v; want ErrPayloadTooLarge", err)
	}
	if len(cli.calls) != 0 {
		t.Fatalf("PutEvents calls = %d; want 0 for oversized payload", len(cli.calls))
	}
}

func TestEventBridge_Publish_DetailEnvelopeCap(t *testing.T) {
	t.Parallel()

	cli := &fakeClient{}
	a, _ := New(cli)
	msg := sampleMessage()
	msg.Payload = []byte(`"` + strings.Repeat("a", MaxBodyBytes-2) + `"`)

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrPayloadTooLarge) {
		t.Fatalf("Publish() error = %v; want ErrPayloadTooLarge", err)
	}
	if len(cli.calls) != 0 {
		t.Fatalf("PutEvents calls = %d; want 0 for oversized entry", len(cli.calls))
	}
}

func TestEventBridgeEntryWireSize_MatchesJSONEncoding(t *testing.T) {
	t.Parallel()

	entry := Entry{
		EventBusName: "default-bus",
		Source:       "svc://test",
		DetailType:   "transaction.created",
		Detail:       []byte(`{"quoted":"value","html":"<&>","list":["a","b"]}`),
		Resources:    []string{"arn:aws:events:us-east-1:123:rule/r1", "arn:aws:events:us-east-1:123:rule/r2"},
	}

	wireEntry := struct {
		EventBusName string   `json:"EventBusName,omitempty"`
		Source       string   `json:"Source"`
		DetailType   string   `json:"DetailType"`
		Detail       string   `json:"Detail"`
		Time         string   `json:"Time,omitempty"`
		Resources    []string `json:"Resources,omitempty"`
	}{
		EventBusName: entry.EventBusName,
		Source:       entry.Source,
		DetailType:   entry.DetailType,
		Detail:       string(entry.Detail),
		Resources:    entry.Resources,
	}

	encoded, err := json.Marshal(wireEntry)
	if err != nil {
		t.Fatalf("Marshal(wireEntry) error = %v", err)
	}

	if got := eventBridgeEntryWireSize(entry); got != len(encoded) {
		t.Fatalf("eventBridgeEntryWireSize() = %d; want %d", got, len(encoded))
	}
}

func TestEventBridge_Publish_PerEntryFailureReturnsError(t *testing.T) {
	t.Parallel()

	cli := &fakeResultClient{result: PutEventsResult{
		FailedEntryCount: 1,
		Entries: []PutEventsEntryResult{{
			ErrorCode:    "AccessDeniedException",
			ErrorMessage: "denied password=secret",
		}},
	}}
	a, _ := New(cli)

	err := a.Publish(context.Background(), sampleMessage())
	if err == nil || !strings.Contains(err.Error(), "AccessDeniedException") {
		t.Fatalf("Publish() error = %v; want per-entry failure", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("Publish() error = %v; leaked unsanitized provider detail", err)
	}
}

func TestEventBridge_Publish_ResultBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		result PutEventsResult
		want   string
	}{
		{
			name: "failed count without details",
			result: PutEventsResult{
				FailedEntryCount: 1,
			},
			want: "failed 1 entries",
		},
		{
			name: "entry detail despite zero failed count",
			result: PutEventsResult{
				Entries: []PutEventsEntryResult{{ErrorCode: "InternalFailure", ErrorMessage: "provider failed"}},
			},
			want: "entry failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &fakeResultClient{result: tt.result}
			a, _ := New(cli)

			err := a.Publish(context.Background(), sampleMessage())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Publish() error = %v; want containing %q", err, tt.want)
			}
		})
	}
}

func TestEventBridge_Publish_LegacyClientDoesNotRequireResultCapability(t *testing.T) {
	t.Parallel()

	cli := &fakeClient{}
	a, _ := New(cli)

	if err := a.Publish(context.Background(), sampleMessage()); err != nil {
		t.Fatalf("Publish() legacy client error = %v; want nil", err)
	}
	if len(cli.calls) != 1 {
		t.Fatalf("PutEvents calls = %d; want 1", len(cli.calls))
	}
}

func TestEventBridge_Classify_PerEntryAuthFailure(t *testing.T) {
	t.Parallel()

	cli := &fakeResultClient{result: PutEventsResult{
		FailedEntryCount: 1,
		Entries:          []PutEventsEntryResult{{ErrorCode: "AccessDeniedException", ErrorMessage: "denied"}},
	}}
	a, _ := New(cli)

	err := a.Publish(context.Background(), sampleMessage())
	if err == nil {
		t.Fatal("Publish() error = nil; want per-entry failure")
	}
	if got := a.Classify(err); got != contract.ClassAuth {
		t.Fatalf("Classify(per-entry failure) = %q; want %q", got, contract.ClassAuth)
	}
}

func TestEventBridge_Publish_RequiresCEHeaders(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})
	msg := sampleMessage()
	msg.Headers = nil // strip every CE header

	if err := a.Publish(context.Background(), msg); !errors.Is(err, contract.ErrInvalidDestination) {
		t.Errorf("Publish() error = %v; want ErrInvalidDestination (missing CE source/type)", err)
	}
}

func TestEventBridge_Healthy_DelegatesToPing(t *testing.T) {
	t.Parallel()

	pingErr := errors.New("eb ping fail")
	cli := &fakePingClient{fakeClient: &fakeClient{}, pingErr: pingErr}
	a, _ := New(cli)

	if err := a.Healthy(context.Background()); !errors.Is(err, pingErr) {
		t.Errorf("Healthy() error = %v; want %v", err, pingErr)
	}
}

func TestEventBridge_Healthy_NoPingFails(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})

	if err := a.Healthy(context.Background()); err == nil {
		t.Errorf("Healthy() error = nil; want no-Ping failure")
	}
}

func TestEventBridge_Classify(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})

	cases := []struct {
		err  error
		want contract.ErrorClass
	}{
		{nil, ""},
		{context.Canceled, contract.ClassContextCanceled},
		{contract.ErrPayloadTooLarge, contract.ClassValidation},
		{errors.New("ResourceNotFoundException: event bus not found"), contract.ClassTopicNotFound},
		{errors.New("AccessDeniedException"), contract.ClassAuth},
		{errors.New("Throttling: rate exceeded"), contract.ClassBrokerOverloaded},
		{errors.New("connection timed out"), contract.ClassNetworkTimeout},
		{errors.New("UnknownError: black helicopters"), contract.ClassBrokerUnavailable},
	}

	for _, tc := range cases {
		if got := a.Classify(tc.err); got != tc.want {
			t.Errorf("Classify(%v) = %q; want %q", tc.err, got, tc.want)
		}
	}
}

func TestEventBridge_FlushAndClose_NoOps(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})
	if err := a.Flush(context.Background()); err != nil {
		t.Errorf("Flush() error = %v; want nil", err)
	}
	if err := a.Close(context.Background()); err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}
}

func TestEventBridge_Kind(t *testing.T) {
	t.Parallel()

	a, _ := New(&fakeClient{})
	if got := a.Kind(); got != contract.TransportEventBridge {
		t.Errorf("Kind() = %q; want %q", got, contract.TransportEventBridge)
	}
}
