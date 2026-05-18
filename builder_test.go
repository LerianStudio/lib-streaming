//go:build unit

package streaming_test

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/LerianStudio/lib-streaming/internal/producer"
	"github.com/LerianStudio/lib-streaming/internal/transport"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestBuilder_OptionsThreadPartitionKeyToProducer(t *testing.T) {
	cfg, cluster := builderKfakeTarget(t)
	fixedKey := "fixed-builder-key"

	emitter, err := streaming.NewBuilder().
		Source("//builder-test").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(cfg).
		Options(streaming.WithPartitionKey(func(streaming.Event) string { return fixedKey })).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	request := streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Subject:       "tx-1",
		Payload:       []byte(`{"amount":100}`),
	}
	if err := emitter.Emit(ctx, request); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics("lerian.streaming.transaction.created"),
		kgo.ConsumerGroup("builder-options-partition-key"),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("consumer init error = %v", err)
	}
	t.Cleanup(consumer.Close)

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fetchCancel()

	fetches := consumer.PollFetches(fetchCtx)
	if err := fetches.Err(); err != nil {
		t.Fatalf("PollFetches() error = %v", err)
	}

	var got *kgo.Record
	fetches.EachRecord(func(record *kgo.Record) {
		if got == nil {
			got = record
		}
	})
	if got == nil {
		t.Fatal("no record fetched")
	}
	if string(got.Key) != fixedKey {
		t.Fatalf("record key = %q; want %q", got.Key, fixedKey)
	}
}

func TestBuilder_DedicatedOptionMethodsBuildWithoutValidationFailure(t *testing.T) {
	t.Parallel()

	emitter, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(builderKafkaTarget()).
		PartitionKey(func(streaming.Event) string { return "ignored-without-emit" }).
		TLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}).
		SASL(plain.Auth{User: "user", Pass: "secret"}.AsMechanism()).
		OutboxRepository(nil).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if emitter == nil {
		t.Fatal("Build() emitter = nil; want emitter")
	}
	if err := emitter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// blockingCloseAdapter is a transport adapter whose Close() blocks until
// its ctx fires. Lets us observe the resolved CloseTimeout via wall-clock
// measurement of CloseContext: a 50ms timeout produces a Close that
// returns in ~50ms; a 30s timeout would dominate the test runtime and
// is therefore avoided here. The pair (5ms, 50ms) gives an order-of-
// magnitude separation that survives CI flakiness.
type blockingCloseAdapter struct {
	kind streaming.TransportKind
}

func (a *blockingCloseAdapter) Kind() streaming.TransportKind { return a.kind }
func (a *blockingCloseAdapter) Publish(_ context.Context, _ transport.TransportMessage) error {
	return nil
}
func (a *blockingCloseAdapter) Healthy(context.Context) error { return nil }
func (a *blockingCloseAdapter) Flush(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *blockingCloseAdapter) Close(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (a *blockingCloseAdapter) Classify(error) streaming.ErrorClass {
	return streaming.ClassBrokerUnavailable
}

// TestBuilder_CloseTimeoutPreservesFluentCallOrder pins the fluent
// call-order semantics for CloseTimeout: the LAST writer (whether the
// dedicated CloseTimeout() setter OR Options(WithCloseTimeout(...)))
// wins when multiple are interleaved.
//
// Refactored away from reflection (former producerCloseTimeout helper):
// rather than reach into the unexported `inner.closeTimeout` field, we
// observe behavior — a transport adapter whose Close() blocks on ctx
// returns when the resolved CloseTimeout fires. Wall-clock measurement
// then witnesses which value won the call-order race.
//
// The (5ms vs 50ms) pair is a 10× separation, well above the floor of
// CI-induced jitter on goroutine scheduling. Using a 30s value would
// dominate the test runtime; using 1ms is too tight for reliable wall-
// clock distinction.
func TestBuilder_CloseTimeoutPreservesFluentCallOrder(t *testing.T) {
	t.Parallel()

	const (
		shortTimeout = 5 * time.Millisecond
		longTimeout  = 50 * time.Millisecond
		tolerance    = 250 * time.Millisecond // very generous to absorb scheduler jitter
	)

	customKind := streaming.TransportCustom
	makeBuilder := func() *streaming.Builder {
		catalog := builderCatalog(t)
		return streaming.NewBuilder().
			Source("svc://close-timeout-test").
			Catalog(catalog).
			Routes(streaming.RouteDefinition{
				Key:           "transaction.created.custom.primary",
				DefinitionKey: "transaction.created",
				Target:        "primary",
				Destination: streaming.Destination{
					Kind: customKind,
					Name: "custom-sink",
				},
				Requirement: streaming.RouteRequired,
			}).
			Target(streaming.TargetConfig{Name: "primary", Kind: customKind}).
			Logger(log.NewNop()).
			RegisterTransport(customKind, func(_ context.Context, _ producer.TransportAdapterOptions) (transport.TransportAdapter, error) {
				return &blockingCloseAdapter{kind: customKind}, nil
			})
	}

	tests := []struct {
		name  string
		build func() *streaming.Builder
		want  time.Duration
	}{
		{
			name: "dedicated call after Options wins",
			build: func() *streaming.Builder {
				return makeBuilder().
					Options(streaming.WithCloseTimeout(shortTimeout)).
					CloseTimeout(longTimeout)
			},
			want: longTimeout,
		},
		{
			name: "Options after dedicated call wins",
			build: func() *streaming.Builder {
				return makeBuilder().
					CloseTimeout(longTimeout).
					Options(streaming.WithCloseTimeout(shortTimeout))
			},
			want: shortTimeout,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			emitter, err := tt.build().Build(context.Background())
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}

			start := time.Now()
			if closeErr := emitter.Close(); closeErr != nil {
				// Close MAY return a context-cancelled-style error from
				// the blocking adapter; that is the documented behavior
				// of the deadline-bounded close path. We only care about
				// the wall-clock window here.
				t.Logf("Close() returned (expected) err = %v", closeErr)
			}
			elapsed := time.Since(start)

			// Observable: Close completes within (resolved timeout +
			// tolerance) and not significantly faster than the resolved
			// timeout (which would indicate Close didn't actually wait).
			lower := tt.want - 2*time.Millisecond
			upper := tt.want + tolerance
			if elapsed < lower || elapsed > upper {
				t.Errorf("Close() elapsed = %v; want in [%v, %v] (resolved timeout = %v)", elapsed, lower, upper, tt.want)
			}
		})
	}
}

func TestBuilder_BuildNilBuilder(t *testing.T) {
	t.Parallel()

	var builder *streaming.Builder
	_, err := builder.Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want streaming.ErrInvalidRouteDefinition", err)
	}
}

func TestBuilder_SASLWithoutTLSRejectsPlaintext(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(builderKafkaTarget()).
		SASL(plain.Auth{User: "u", Pass: "p"}.AsMechanism()).
		Build(context.Background())
	if !errors.Is(err, streaming.ErrPlaintextSASLNotAllowed) {
		t.Fatalf("Build() error = %v; want streaming.ErrPlaintextSASLNotAllowed", err)
	}
}

func TestBuilder_SASLWithAllowPlaintextSASLSucceeds(t *testing.T) {
	t.Parallel()

	emitter, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(builderKafkaTarget()).
		SASL(plain.Auth{User: "u", Pass: "p"}.AsMechanism()).
		AllowPlaintextSASL().
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v; want nil", err)
	}
	if emitter == nil {
		t.Fatal("Build() emitter = nil; want emitter")
	}
	if err := emitter.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestBuilder_TLSConfigWithInsecureSkipVerifyRejectsConfig(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(builderKafkaTarget()).
		TLSConfig(&tls.Config{InsecureSkipVerify: true}). //nolint:gosec // test verifies rejection
		Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidTLSConfig) {
		t.Fatalf("Build() error = %v; want streaming.ErrInvalidTLSConfig", err)
	}
}

// TestBuilder_TargetNameWithControlCharRejected pins the target.Name
// sanitization invariant. The CB recovery goroutine drives state
// transitions on every registered target, reliably surfacing target.Name
// into operator logs via the per-target StateChangeListener — a target
// name carrying a newline or other control character would inject
// crafted lines into the log stream. Closes a latent log-injection vector
// that the recovery goroutine amplifies even in emit-only services.
func TestBuilder_TargetNameWithControlCharRejected(t *testing.T) {
	t.Parallel()

	target := builderKafkaTarget()
	target.Name = "primary\nattacker-injected" // newline = control char

	_, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(target).
		Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want streaming.ErrInvalidRouteDefinition", err)
	}
}

// TestBuilder_TargetNameTooLongRejected pins the target.Name length cap.
// Same vector as control-char injection: an oversize name would log-bomb
// the operator log stream. The cap matches MaxEventIDBytes so target and
// route fields share a symmetric validation surface.
func TestBuilder_TargetNameTooLongRejected(t *testing.T) {
	t.Parallel()

	target := builderKafkaTarget()
	target.Name = strings.Repeat("a", 257) // > MaxEventIDBytes (256)

	_, err := streaming.NewBuilder().
		Source("svc://ledger").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("lerian.streaming.transaction.created")).
		Target(target).
		Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want streaming.ErrInvalidRouteDefinition", err)
	}
}

func builderCatalog(t *testing.T) streaming.Catalog {
	t.Helper()

	return builderCatalogWithDefinitions(t, streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
}

func builderCatalogWithDefinitions(t *testing.T, definitions ...streaming.EventDefinition) streaming.Catalog {
	t.Helper()

	catalog, err := streaming.NewCatalog(definitions...)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

func builderRoute(topic string) streaming.RouteDefinition {
	return streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic(topic),
		Requirement:   streaming.RouteRequired,
	}
}

func builderRouteSQS(queueURL string) streaming.RouteDefinition {
	return streaming.RouteDefinition{
		Key:           "transaction.created.sqs.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.SQSQueueURL(queueURL),
		Requirement:   streaming.RouteRequired,
	}
}

func builderKafkaTarget() streaming.TargetConfig {
	return streaming.TargetConfig{
		Name:     "primary",
		Kind:     streaming.TransportKafkaLike,
		Brokers:  []string{"127.0.0.1:9092"},
		ClientID: "builder-test",
	}
}

func builderKfakeTarget(t *testing.T) (streaming.TargetConfig, *kfake.Cluster) {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(3),
		kfake.SeedTopics(3, "lerian.streaming.transaction.created"),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	target := builderKafkaTarget()
	target.Brokers = cluster.ListenAddrs()

	return target, cluster
}
