//go:build unit

package producer

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// readDLQRecord polls a kgo consumer until one record lands on topic, or
// the deadline expires. Returns nil when no record is fetched (lets tests
// assert on absence).
func readDLQRecord(t *testing.T, cluster *kfake.Cluster, topic string, timeout time.Duration) *kgo.Record {
	t.Helper()

	consumer := newConsumer(t, cluster, topic)

	fetchCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		fetches := consumer.PollFetches(fetchCtx)

		var got *kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			if got == nil {
				got = r
			}
		})

		if got != nil {
			return got
		}

		if fetchCtx.Err() != nil {
			return nil
		}

		select {
		case <-fetchCtx.Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}

	return nil
}

// kfakeDLQConfig returns a Config + kfake.Cluster pair that pre-seeds both
// the source topic and its derived ".dlq" sibling so DLQ-routing tests can
// observe DLQ records without standing up a real broker. Tuned to keep the
// circuit breaker effectively CLOSED across the test (failure ratio 0.99,
// huge minRequests) and to surface produce errors immediately (zero
// retries, leader acks).
func kfakeDLQConfig(t *testing.T) (Config, *kfake.Cluster) {
	t.Helper()

	sourceTopic := "lerian.streaming.transaction.created"
	dlq := sourceTopic + ".dlq"

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.AllowAutoTopicCreation(),
		kfake.DefaultNumPartitions(3),
		kfake.SeedTopics(3, sourceTopic, dlq),
	)
	if err != nil {
		t.Fatalf("kfake.NewCluster err = %v", err)
	}

	t.Cleanup(cluster.Close)

	return Config{
		Enabled:               true,
		Brokers:               cluster.ListenAddrs(),
		ClientID:              "test-producer",
		BatchLingerMs:         1,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    10_000,
		Compression:           "none",
		RecordRetries:         0,
		RecordDeliveryTimeout: 5 * time.Second,
		RequiredAcks:          "leader",
		CBFailureRatio:        0.99,
		CBMinRequests:         1_000_000,
		CBTimeout:             5 * time.Second,
		CloseTimeout:          5 * time.Second,
		CloudEventsSource:     "//test",
	}, cluster
}

// injectProduceError installs a kfake control hook that responds to every
// Produce request targeting sourceTopic with errCode on every partition.
// Lets tests exercise the error-classification + DLQ paths without standing
// up a real broker.
func injectProduceError(cluster *kfake.Cluster, sourceTopic string, errCode int16) {
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()

		produceReq, ok := req.(*kmsg.ProduceRequest)
		if !ok {
			return nil, nil, false
		}

		topicNames := resolveRequestTopicNames(cluster, produceReq)

		matched := false
		for _, name := range topicNames {
			if name == sourceTopic {
				matched = true
				break
			}
		}

		if !matched {
			return nil, nil, false
		}

		resp, ok := produceReq.ResponseKind().(*kmsg.ProduceResponse)
		if !ok {
			return nil, nil, false
		}

		resp.Version = produceReq.Version

		for i, topic := range produceReq.Topics {
			if topicNames[i] != sourceTopic {
				continue
			}

			topicResp := kmsg.NewProduceResponseTopic()
			topicResp.Topic = topic.Topic
			topicResp.TopicID = topic.TopicID

			for _, partition := range topic.Partitions {
				partResp := kmsg.NewProduceResponseTopicPartition()
				partResp.Partition = partition.Partition
				partResp.ErrorCode = errCode
				topicResp.Partitions = append(topicResp.Partitions, partResp)
			}

			resp.Topics = append(resp.Topics, topicResp)
		}

		return resp, nil, true
	})
}

// resolveRequestTopicNames returns the topic-name slice for a ProduceRequest,
// resolving TopicID (v13+) to the broker-known topic name when the request
// uses the new wire shape. Topics that cannot be resolved fall back to the
// empty string (so callers' equality checks fail gracefully).
func resolveRequestTopicNames(cluster *kfake.Cluster, produceReq *kmsg.ProduceRequest) []string {
	names := make([]string, len(produceReq.Topics))

	for i, topic := range produceReq.Topics {
		if topic.Topic != "" {
			names[i] = topic.Topic
			continue
		}

		info := cluster.TopicIDInfo(topic.TopicID)
		if info != nil {
			names[i] = info.Topic
		}
	}

	return names
}
