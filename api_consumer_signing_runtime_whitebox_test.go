//go:build unit

package streaming

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/consumer"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
)

// runtimeSigningSecret is a distinctive secret whose every rendering (raw,
// decimal []byte, hex []byte, base64) the redaction test hunts for.
var runtimeSigningSecret = bytes.Repeat([]byte{0xab}, MinSigningSecretBytes)

func secretRenderings(secret []byte) []string {
	return []string{
		string(secret),
		strings.Trim(fmt.Sprint(secret[:4]), "[]"), // "171 171 171 171"
		fmt.Sprintf("%#v", secret[:3])[len("[]byte{"):],
		base64.StdEncoding.EncodeToString(secret),
	}
}

func assertNoSigningSecret(t *testing.T, what, rendered string) {
	t.Helper()

	for _, leak := range secretRenderings(runtimeSigningSecret) {
		if strings.Contains(rendered, leak) {
			t.Errorf("%s renders signing secret material %q:\n%s", what, leak, rendered)
		}
	}
}

// TestConsumerBuilder_SignatureSecretsNeverRender pins the redaction promise
// behind every holder of consumer signing keys, not only the config value: the
// builder and the runtime keep the config in an unexported field, where fmt
// cannot reach a Secret's own mask, so a key held there by value would print
// its bytes under %+v.
func TestConsumerBuilder_SignatureSecretsNeverRender(t *testing.T) {
	// Not parallel: mutates process env.
	t.Setenv("STREAMING_CONSUMER_ENABLED", "true")
	t.Setenv("STREAMING_CONSUMER_BROKERS", "localhost:9092")
	t.Setenv("STREAMING_CONSUMER_GROUP", "loan-projector")
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", "loan-projector")
	t.Setenv("STREAMING_CONSUMER_APPS", "lender")
	t.Setenv("STREAMING_CONSUMER_REQUIRE_SIGNATURES", "true")
	t.Setenv("STREAMING_CONSUMER_SIGNATURE_KEYS", "lender-k1@lender:"+base64.StdEncoding.EncodeToString(runtimeSigningSecret))

	cfg, _, err := LoadConsumerConfig()
	if err != nil {
		t.Fatalf("LoadConsumerConfig: %v", err)
	}

	b := NewConsumer().FromConfig(cfg).On("loan.created", func(context.Context, Event, []byte) error { return nil })

	for _, verb := range []string{"%v", "%+v", "%#v"} {
		assertNoSigningSecret(t, "ConsumerConfig "+verb, fmt.Sprintf(verb, cfg))
		assertNoSigningSecret(t, "*ConsumerBuilder "+verb, fmt.Sprintf(verb, b))
		assertNoSigningSecret(t, "ConsumerBuilder "+verb, fmt.Sprintf(verb, *b))
	}

	c, err := b.Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	t.Cleanup(func() { _ = c.Close() })

	for _, verb := range []string{"%v", "%+v", "%#v"} {
		assertNoSigningSecret(t, "Consumer "+verb, fmt.Sprintf(verb, c))
	}
}

// scriptedSigningClient serves one batch of records, then blocks like an idle
// broker until the poll context ends or the client closes.
type scriptedSigningClient struct {
	mu        sync.Mutex
	pending   []*kgo.Record
	committed chan struct{}
	closeOnce sync.Once
	stop      chan struct{}
}

func newScriptedSigningClient(recs ...*kgo.Record) *scriptedSigningClient {
	return &scriptedSigningClient{pending: recs, committed: make(chan struct{}, 16), stop: make(chan struct{})}
}

func (f *scriptedSigningClient) PollFetches(ctx context.Context) kgo.Fetches {
	f.mu.Lock()
	recs := f.pending
	f.pending = nil
	f.mu.Unlock()

	if len(recs) > 0 {
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{
			Topic:      recs[0].Topic,
			Partitions: []kgo.FetchPartition{{Partition: recs[0].Partition, Records: recs}},
		}}}}
	}

	select {
	case <-ctx.Done():
		return kgo.NewErrFetch(ctx.Err())
	case <-f.stop:
		return kgo.NewErrFetch(kgo.ErrClientClosed)
	}
}

func (f *scriptedSigningClient) CommitRecords(_ context.Context, _ ...*kgo.Record) error {
	f.committed <- struct{}{}

	return nil
}

func (f *scriptedSigningClient) SetOffsets(map[string]map[int32]kgo.EpochOffset) {}

func (f *scriptedSigningClient) AllowRebalance() {}

func (f *scriptedSigningClient) Close() { f.closeOnce.Do(func() { close(f.stop) }) }

// capturingSigningDLQ records the cause kind of every quarantine.
type capturingSigningDLQ struct {
	mu    sync.Mutex
	kinds []string
}

func (d *capturingSigningDLQ) PublishDLQ(_ context.Context, _ *kgo.Record, _ error, causeKind string, _ int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.kinds = append(d.kinds, causeKind)

	return nil
}

func (d *capturingSigningDLQ) Close(context.Context) error { return nil }

func (d *capturingSigningDLQ) causeKinds() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return append([]string(nil), d.kinds...)
}

// lenderRecord is a loan.created fact from lender on its app topic, signed by
// key at signedAt, or unsigned when key is nil.
func lenderRecord(t *testing.T, key *SigningKey, signedAt time.Time) *kgo.Record {
	t.Helper()

	event := Event{
		TenantID:     "tenant-abc",
		ResourceType: "loan",
		EventType:    "created",
		Source:       "lender",
		Payload:      []byte(`{"amount":"10.00"}`),
	}
	event.ApplyDefaults()

	headers := cloudevents.BuildTransportHeaders(event)

	if key != nil {
		ring, err := NewKeyring(*key)
		if err != nil {
			t.Fatalf("NewKeyring: %v", err)
		}

		signer, err := envelopesig.NewSigner(ring, key.ID, key.Source, envelopesig.WithClock(func() time.Time { return signedAt }))
		if err != nil {
			t.Fatalf("NewSigner: %v", err)
		}

		headers = signer.Sign(headers, event.Payload)
	}

	out := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}

	return &kgo.Record{Topic: "lerian.streaming.lender", Headers: out, Value: event.Payload}
}

// runBuilt drives one record through exactly the runtime options Build would
// install for b, over a scripted client and a capturing DLQ, and returns
// the cause kinds it quarantined.
func runBuilt(t *testing.T, b *ConsumerBuilder, rec *kgo.Record) []string {
	t.Helper()

	handler, err := b.resolveReceiver()
	if err != nil {
		t.Fatalf("resolveReceiver: %v", err)
	}

	opts, err := b.runtimeOptions()
	if err != nil {
		t.Fatalf("runtimeOptions: %v", err)
	}

	client := newScriptedSigningClient(rec)
	dlq := &capturingSigningDLQ{}

	c, err := consumer.New(b.cfg, client, handler, append(opts, consumer.WithDLQPublisher(dlq))...)
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- c.Run(ctx) }()

	select {
	case <-client.committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the record was never committed")
	}

	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	_ = c.Close()

	return dlq.causeKinds()
}

// TestConsumerBuilder_RuntimeOptionsInstallTheVerifier pins that a built
// consumer actually verifies: the options Build hands the runtime quarantine an
// unsigned record when signatures are required, enforce SignatureMaxSkew when
// it is set, and install nothing when signatures are not required.
func TestConsumerBuilder_RuntimeOptionsInstallTheVerifier(t *testing.T) {
	t.Parallel()

	key := SigningKey{ID: "lender-k1", Source: "lender", Secret: runtimeSigningSecret}
	now := time.Now()

	ring, err := NewKeyring(key)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	tests := []struct {
		name        string
		configure   func(*ConsumerBuilder) *ConsumerBuilder
		record      func(t *testing.T) *kgo.Record
		wantHandled bool
		wantKinds   []string
	}{
		{
			name:      "required: unsigned record quarantines",
			configure: func(b *ConsumerBuilder) *ConsumerBuilder { return b.RequireSignatures(ring) },
			record:    func(t *testing.T) *kgo.Record { return lenderRecord(t, nil, now) },
			wantKinds: []string{DLQCauseSignatureMissing},
		},
		{
			name: "required: fresh signed record is handled",
			configure: func(b *ConsumerBuilder) *ConsumerBuilder {
				return b.RequireSignatures(ring).SignatureMaxSkew(time.Minute)
			},
			record:      func(t *testing.T) *kgo.Record { return lenderRecord(t, &key, now) },
			wantHandled: true,
		},
		{
			name: "required with skew: record signed an hour ago quarantines",
			configure: func(b *ConsumerBuilder) *ConsumerBuilder {
				return b.RequireSignatures(ring).SignatureMaxSkew(time.Minute)
			},
			record:    func(t *testing.T) *kgo.Record { return lenderRecord(t, &key, now.Add(-time.Hour)) },
			wantKinds: []string{DLQCauseSignatureInvalid},
		},
		{
			name:        "required without skew: record signed an hour ago is handled",
			configure:   func(b *ConsumerBuilder) *ConsumerBuilder { return b.RequireSignatures(ring) },
			record:      func(t *testing.T) *kgo.Record { return lenderRecord(t, &key, now.Add(-time.Hour)) },
			wantHandled: true,
		},
		{
			name:        "not required: unsigned record is handled",
			configure:   func(b *ConsumerBuilder) *ConsumerBuilder { return b },
			record:      func(t *testing.T) *kgo.Record { return lenderRecord(t, nil, now) },
			wantHandled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var handled atomic.Bool

			b := tt.configure(NewConsumer().
				Brokers("localhost:9092").
				Group("svc").
				Source("loan-projector").
				Apps("lender").
				On("loan.created", func(context.Context, Event, []byte) error {
					handled.Store(true)

					return nil
				}))

			kinds := runBuilt(t, b, tt.record(t))

			if got := handled.Load(); got != tt.wantHandled {
				t.Errorf("handler ran = %v; want %v (DLQ cause kinds %v)", got, tt.wantHandled, kinds)
			}

			if strings.Join(kinds, ",") != strings.Join(tt.wantKinds, ",") {
				t.Errorf("DLQ cause kinds = %v; want %v", kinds, tt.wantKinds)
			}
		})
	}
}

// TestConsumerBuilder_DisabledInstallsNoVerifier pins the kill switch: a
// disabled consumer resolves no keyring, so even an unusable signing setup
// cannot fail its Build.
func TestConsumerBuilder_DisabledInstallsNoVerifier(t *testing.T) {
	t.Parallel()

	opts, err := NewConsumer().Enabled(false).RequireSignatures(nil).runtimeOptions()
	if err != nil {
		t.Fatalf("runtimeOptions on a disabled consumer: %v", err)
	}

	if len(opts) != 0 {
		t.Errorf("runtimeOptions on a disabled consumer = %d options; want none", len(opts))
	}
}
