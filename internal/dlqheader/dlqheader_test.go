//go:build unit

package dlqheader

import "testing"

// TestHeaderValuesFrozen pins the six DLQ header key strings. These are a wire
// contract: the producer writes them and the consumer's DLQ publisher reads
// them. A value change here silently breaks cross-compat, so any edit to a
// constant must also edit this test — which forces a conscious decision.
func TestHeaderValuesFrozen(t *testing.T) {
	t.Parallel()

	want := map[string]string{
		"SourceTopic":    "x-lerian-dlq-source-topic",
		"ErrorClass":     "x-lerian-dlq-error-class",
		"ErrorMessage":   "x-lerian-dlq-error-message",
		"RetryCount":     "x-lerian-dlq-retry-count",
		"FirstFailureAt": "x-lerian-dlq-first-failure-at",
		"ProducerID":     "x-lerian-dlq-producer-id",
	}

	got := map[string]string{
		"SourceTopic":    SourceTopic,
		"ErrorClass":     ErrorClass,
		"ErrorMessage":   ErrorMessage,
		"RetryCount":     RetryCount,
		"FirstFailureAt": FirstFailureAt,
		"ProducerID":     ProducerID,
	}

	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %q; want %q", name, got[name], w)
		}
	}
}

// TestConsumerHeaderValuesFrozen pins the two consumer-specific DLQ header keys.
// Same wire-contract rule as the six: a value change must be a conscious edit.
func TestConsumerHeaderValuesFrozen(t *testing.T) {
	t.Parallel()

	if SourcePartition != "x-lerian-dlq-source-partition" {
		t.Errorf("SourcePartition = %q; want %q", SourcePartition, "x-lerian-dlq-source-partition")
	}

	if SourceOffset != "x-lerian-dlq-source-offset" {
		t.Errorf("SourceOffset = %q; want %q", SourceOffset, "x-lerian-dlq-source-offset")
	}
}
