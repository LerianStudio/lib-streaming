//go:build unit

package kafka

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// TestAdapter_Publish_NonUTF8Payload_ByteEqual proves the Kafka adapter is
// byte-transparent: a non-UTF8 / non-JSON payload (ISO-8859-1 XML, the SFN
// opaque case) lands as the record value byte-for-byte. This is the transport
// half of the opaque-payload contract — CloudEvents binary mode ships
// event.Payload verbatim as the record value.
func TestAdapter_Publish_NonUTF8Payload_ByteEqual(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(3),
		kfake.SeedTopics(3, "opaque.topic"),
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

	// ISO-8859-1 XML: 0xE7 ('ç') is neither valid UTF-8 in isolation nor
	// valid JSON — nothing in the transport may transcode or reject it.
	payload := []byte{'<', 'd', 'o', 'c', '>', 0xE7, '<', '/', 'd', 'o', 'c', '>'}
	message := transport.TransportMessage{
		Destination: contract.Destination{Kind: contract.TransportKafkaLike, Name: "opaque.topic"},
		Key:         "opaque-key",
		Payload:     payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := adapter.Publish(ctx, message); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics("opaque.topic"),
		kgo.ConsumerGroup("opaque-publish-mapping"),
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
	if !bytes.Equal(got.Value, payload) {
		t.Fatalf("record value = %v; want byte-equal to %v", got.Value, payload)
	}
}
