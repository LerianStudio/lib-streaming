//go:build unit

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

func TestAdapter_NewAdapterFromClient_RejectsNil(t *testing.T) {
	t.Parallel()

	adapter, err := NewAdapterFromClient(nil)
	if !errors.Is(err, contract.ErrNilProducer) {
		t.Fatalf("NewAdapterFromClient(nil) err = %v; want ErrNilProducer", err)
	}

	if adapter != nil {
		t.Fatalf("NewAdapterFromClient(nil) adapter = %v; want nil", adapter)
	}
}

func TestAdapter_NilReceiverReturnsSentinel(t *testing.T) {
	t.Parallel()

	// Direct (*Adapter)(nil) exercise: defense-in-depth path that fires
	// when something hand-builds a nil *Adapter pointer (e.g. a test
	// double). Constructors no longer permit nil clients to slip through.
	var adapter *Adapter

	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "publish",
			run: func() error {
				return adapter.Publish(ctx, transport.TransportMessage{
					Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic"},
				})
			},
		},
		{
			name: "healthy",
			run: func() error {
				return adapter.Healthy(ctx)
			},
		},
		{
			name: "flush",
			run: func() error {
				return adapter.Flush(ctx)
			},
		},
		{
			name: "close",
			run: func() error {
				return adapter.Close(ctx)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.run(); !errors.Is(err, contract.ErrNilProducer) {
				t.Fatalf("%s err = %v; want ErrNilProducer", tt.name, err)
			}
		})
	}
}

func TestAdapter_PublishRejectsInvalidDestinationShape(t *testing.T) {
	t.Parallel()

	// Per-Publish destination shape checks that the Kafka adapter still
	// owns: kind, empty topic, address-forbidden, attributes-forbidden.
	//
	// Security validation (credential-bearing topic names, credential
	// attributes) moved to construction time inside NewRouteDefinition;
	// see internal/contract/route.go's validateDestinationSecurity. The
	// per-Publish hot path no longer re-runs that scan, so the explicit
	// security cases live in route_test.go (contract layer) rather than
	// here. The adapter-level test enumerates the cheap last-line guards
	// that catch hand-built TransportMessages bypassing the routed path.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)

	adapter, err := NewAdapterFromClient(client)
	if err != nil {
		t.Fatalf("NewAdapterFromClient() error = %v", err)
	}

	tests := []struct {
		name        string
		destination contract.Destination
	}{
		{
			name:        "non kafka destination",
			destination: contract.Destination{Kind: contract.TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue"},
		},
		{
			name:        "empty topic",
			destination: contract.Destination{Kind: contract.TransportKafkaLike},
		},
		{
			name:        "kafka address forbidden",
			destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic", Address: "https://broker.example.test/topic"},
		},
		{
			name:        "kafka attributes forbidden",
			destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "topic", Attributes: map[string]string{"compression": "lz4"}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := adapter.Publish(context.Background(), transport.TransportMessage{Destination: tt.destination})
			if !errors.Is(err, contract.ErrInvalidDestination) {
				t.Fatalf("Publish() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestAdapter_PublishMapsMessageToKafkaRecord(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(3),
		kfake.SeedTopics(3, "adapter.topic"),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)

	adapter, err := NewAdapterFromClient(client)
	if err != nil {
		t.Fatalf("NewAdapterFromClient() error = %v", err)
	}
	message := transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "adapter.topic"},
		Key:         "record-key",
		Payload:     []byte(`{"ok":true}`),
		Headers: []transport.Header{
			{Key: "h1", Value: []byte("v1")},
			{Key: "h2", Value: []byte("v2")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := adapter.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics("adapter.topic"),
		kgo.ConsumerGroup("adapter-publish-mapping"),
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
	if got.Topic != "adapter.topic" {
		t.Fatalf("record topic = %q; want adapter.topic", got.Topic)
	}
	if string(got.Key) != "record-key" {
		t.Fatalf("record key = %q; want record-key", got.Key)
	}
	if string(got.Value) != string(message.Payload) {
		t.Fatalf("record value = %q; want %q", got.Value, message.Payload)
	}
	if len(got.Headers) != 2 || got.Headers[0].Key != "h1" || string(got.Headers[0].Value) != "v1" || got.Headers[1].Key != "h2" || string(got.Headers[1].Value) != "v2" {
		t.Fatalf("record headers = %#v; want h1/h2", got.Headers)
	}
}

func TestAdapter_PublishHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation(), kfake.SeedTopics(1, "adapter.canceled"))
	if err != nil {
		t.Fatalf("kfake.NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Close)

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient() error = %v", err)
	}
	t.Cleanup(client.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	adapter, err := NewAdapterFromClient(client)
	if err != nil {
		t.Fatalf("NewAdapterFromClient() error = %v", err)
	}

	err = adapter.Publish(ctx, transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "adapter.canceled"},
		Payload:     []byte(`{}`),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v; want context.Canceled", err)
	}
}

func TestAdapter_Classify(t *testing.T) {
	t.Parallel()

	// Classify uses an untyped receiver (`func (*Adapter) Classify ...`)
	// and never dereferences the receiver, so a (*Adapter)(nil) is a safe
	// dispatch target. NewAdapterFromClient(nil) now returns ErrNilProducer
	// so we exercise the package-level ClassifyError directly — this is
	// the function the (*Adapter) method delegates to.
	tests := []struct {
		name string
		err  error
		want contract.ErrorClass
	}{
		{name: "context canceled", err: context.Canceled, want: contract.ClassContextCanceled},
		{name: "auth", err: kerr.SaslAuthenticationFailed, want: contract.ClassAuth},
		{name: "topic not found", err: kerr.UnknownTopicOrPartition, want: contract.ClassTopicNotFound},
		{name: "serialization", err: kerr.MessageTooLarge, want: contract.ClassSerialization},
		{name: "overloaded", err: kerr.ThrottlingQuotaExceeded, want: contract.ClassBrokerOverloaded},
		{name: "record timeout", err: kgo.ErrRecordTimeout, want: contract.ClassNetworkTimeout},
		{name: "invalid destination", err: contract.ErrInvalidDestination, want: contract.ClassValidation},
		{name: "wrapped invalid destination", err: fmt.Errorf("publish route: %w", contract.ErrInvalidDestination), want: contract.ClassValidation},
		{name: "invalid route definition", err: contract.ErrInvalidRouteDefinition, want: contract.ClassValidation},
		{name: "opaque", err: errors.New("dial tcp: connection refused"), want: contract.ClassBrokerUnavailable},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ClassifyError(tt.err); got != tt.want {
				t.Fatalf("ClassifyError(%v) = %q; want %q", tt.err, got, tt.want)
			}
		})
	}
}
