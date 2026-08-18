//go:build unit

package consumer

import "testing"

// TestConsumerMetricNames_AreTheLiteralsDashboardsQuery pins every consumer
// metric name as a literal string.
//
// These names are a contract with things this repository cannot see: Grafana
// panels, alert rules, and recording rules in the observability repo, plus the
// names published in AGENTS.md. Renaming one breaks a dashboard silently — the
// panel keeps rendering, it just goes flat forever, which reads as "healthy".
//
// Asserting against the constants themselves would be a tautology, so the
// expectations are written out.
func TestConsumerMetricNames_AreTheLiteralsDashboardsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"dlq total", metricDLQTotal, "streaming_consumer_dlq_total"},
		{"dlq publish failed", metricDLQPublishFailed, "streaming_consumer_dlq_publish_failed_total"},
		{"fetch error", metricFetchError, "streaming_consumer_fetch_error_total"},
		{"fetch error data loss", metricFetchErrorDataLoss, "streaming_consumer_fetch_error_data_loss_total"},
		{"system event", metricSystemEvent, "streaming_consumer_system_event_total"},
		{"partition halted", metricPartitionHalted, "streaming_consumer_partition_halted_total"},
		{"unmatched total", metricUnmatchedTotal, "streaming_consumer_unmatched_total"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.got != tt.want {
				t.Errorf("metric name = %q; want %q", tt.got, tt.want)
			}
		})
	}
}
