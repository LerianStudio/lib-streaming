//go:build unit

package contract

import (
	"testing"
	"time"
)

// TestEvent_Topic pins the v3 TOPIC COLLAPSE: the topic is
// "lerian.streaming." + Source and carries nothing else. Resource type,
// event type, and the v2 ".v<major>" schema suffix are all gone from the
// name — a consumer selects events inside the app stream by the
// ce-resourcetype / ce-eventtype headers, and ce-schemaversion is the sole
// version carrier.
func TestEvent_Topic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		source        string
		resourceType  string
		eventType     string
		schemaVersion string
		want          string
	}{
		{
			name:          "first-major event rides the app topic",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "1.0.0",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "different resource and event, same topic",
			source:        "midaz-ledger",
			resourceType:  "account",
			eventType:     "updated",
			schemaVersion: "1.2.3",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "major >= 2 no longer suffixes the topic",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "2.0.0",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "major 10 no longer suffixes the topic",
			source:        "midaz-ledger",
			resourceType:  "ledger",
			eventType:     "closed",
			schemaVersion: "10.0.0",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "empty schema version rides the app topic",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "unparseable schema version cannot alter the topic",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "not-a-version",
			want:          "lerian.streaming.midaz-ledger",
		},
		{
			name:          "underscored source is preserved verbatim",
			source:        "br_consignado_gw",
			resourceType:  "contract",
			eventType:     "registered",
			schemaVersion: "1.0.0",
			want:          "lerian.streaming.br_consignado_gw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := Event{
				Source:        tt.source,
				ResourceType:  tt.resourceType,
				EventType:     tt.eventType,
				SchemaVersion: tt.schemaVersion,
			}
			if got := e.Topic(); got != tt.want {
				t.Errorf("Event.Topic() = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestEvent_Topic_NilReceiver pins that a nil receiver yields "" rather than
// panicking or fabricating a bare-prefix topic.
func TestEvent_Topic_NilReceiver(t *testing.T) {
	t.Parallel()

	var e *Event
	if got := e.Topic(); got != "" {
		t.Errorf("(*Event)(nil).Topic() = %q; want empty", got)
	}
}

// TestEvent_PartitionKey covers the partition-key fallback chain: TenantID,
// then Subject, then EventID; "system:" + EventType when SystemEvent is true.
//
// The chain exists because of the topic collapse. A single-tenant service
// leaves TenantID empty, and franz-go's sticky-key partitioner branches on
// key != nil — []byte("") is NOT nil, so an empty key hashes to one constant
// partition. In v2 that traffic was spread over per-event topics; in v3 it is
// one topic, so an empty key would pin an entire application's stream to a
// single partition.
func TestEvent_PartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "tenant id is the default partition key",
			event: Event{TenantID: "t-abc", EventType: "created", Subject: "agg-1", EventID: "evt-1"},
			want:  "t-abc",
		},
		{
			name:  "system event uses system prefix + event type",
			event: Event{TenantID: "", EventType: "reaper_pass", SystemEvent: true},
			want:  "system:reaper_pass",
		},
		{
			name: "system event with tenant still uses system prefix",
			event: Event{
				TenantID:    "ignored",
				EventType:   "announce",
				SystemEvent: true,
			},
			want: "system:announce",
		},
		{
			name:  "empty tenant falls back to subject so per-aggregate order survives",
			event: Event{EventType: "created", Subject: "loan-42", EventID: "evt-1"},
			want:  "loan-42",
		},
		{
			name:  "empty tenant and empty subject fall back to event id",
			event: Event{EventType: "created", EventID: "evt-1"},
			want:  "evt-1",
		},
		{
			name:  "no identity at all yields an empty key",
			event: Event{EventType: "created"},
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.event.PartitionKey(); got != tt.want {
				t.Errorf("Event.PartitionKey() = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestEvent_ApplyDefaults ensures auto-population of optional fields when the
// caller leaves them zero: EventID (uuid), Timestamp, SchemaVersion, DataContentType.
// This is the behavioral contract required for DX-A01 (3-line emit).
func TestEvent_ApplyDefaults(t *testing.T) {
	t.Parallel()

	before := time.Now().UTC()
	e := Event{
		ResourceType: "transaction",
		EventType:    "created",
		Source:       "midaz-tx",
	}

	e.ApplyDefaults()

	after := time.Now().UTC()

	if e.EventID == "" {
		t.Error("ApplyDefaults: EventID should be auto-populated")
	}
	if len(e.EventID) != 36 {
		t.Errorf("ApplyDefaults: EventID should be a UUID (36 chars), got len=%d value=%q", len(e.EventID), e.EventID)
	}
	if e.Timestamp.IsZero() {
		t.Error("ApplyDefaults: Timestamp should be auto-populated")
	}
	if e.Timestamp.Before(before) || e.Timestamp.After(after) {
		t.Errorf("ApplyDefaults: Timestamp %v outside expected window [%v, %v]", e.Timestamp, before, after)
	}
	if e.SchemaVersion != "1.0.0" {
		t.Errorf("ApplyDefaults: SchemaVersion = %q; want %q", e.SchemaVersion, "1.0.0")
	}
	if e.DataContentType != "application/json" {
		t.Errorf("ApplyDefaults: DataContentType = %q; want %q", e.DataContentType, "application/json")
	}
}

// TestEvent_ApplyDefaults_PreservesNonZero verifies that explicit values are
// NOT overwritten. Defaults only fill zero values.
func TestEvent_ApplyDefaults_PreservesNonZero(t *testing.T) {
	t.Parallel()

	explicitID := "my-explicit-id"
	explicitTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	explicitSchema := "3.1.4"
	explicitCT := "application/avro"

	e := Event{
		EventID:         explicitID,
		Timestamp:       explicitTime,
		SchemaVersion:   explicitSchema,
		DataContentType: explicitCT,
	}

	e.ApplyDefaults()

	if e.EventID != explicitID {
		t.Errorf("ApplyDefaults overwrote explicit EventID: got %q, want %q", e.EventID, explicitID)
	}
	if !e.Timestamp.Equal(explicitTime) {
		t.Errorf("ApplyDefaults overwrote explicit Timestamp: got %v, want %v", e.Timestamp, explicitTime)
	}
	if e.SchemaVersion != explicitSchema {
		t.Errorf("ApplyDefaults overwrote explicit SchemaVersion: got %q, want %q", e.SchemaVersion, explicitSchema)
	}
	if e.DataContentType != explicitCT {
		t.Errorf("ApplyDefaults overwrote explicit DataContentType: got %q, want %q", e.DataContentType, explicitCT)
	}
}
