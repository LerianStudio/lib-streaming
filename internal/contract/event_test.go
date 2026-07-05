//go:build unit

package contract

import (
	"strings"
	"testing"
	"time"
)

// TestEvent_Topic exercises the semver-major suffix rule from TRD §C1:
// base form always, optional ".v<major>" when the parsed major version is ≥ 2.
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
			name:          "semver 1.0.0 yields base form",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "1.0.0",
			want:          "midaz-ledger.transaction.created",
		},
		{
			name:          "semver 1.2.3 yields base form",
			source:        "midaz-ledger",
			resourceType:  "account",
			eventType:     "updated",
			schemaVersion: "1.2.3",
			want:          "midaz-ledger.account.updated",
		},
		{
			name:          "semver 2.0.0 yields .v2 suffix",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "2.0.0",
			want:          "midaz-ledger.transaction.created.v2",
		},
		{
			name:          "semver 2.3.1 yields .v2 suffix",
			source:        "midaz-ledger",
			resourceType:  "account",
			eventType:     "created",
			schemaVersion: "2.3.1",
			want:          "midaz-ledger.account.created.v2",
		},
		{
			name:          "semver 10.0.0 yields .v10 suffix",
			source:        "midaz-ledger",
			resourceType:  "ledger",
			eventType:     "closed",
			schemaVersion: "10.0.0",
			want:          "midaz-ledger.ledger.closed.v10",
		},
		{
			// Empty schema version is the documented default — Topic
			// returns the base form silently. NewEventDefinition
			// normalizes empty to "1.0.0" upstream, so this branch
			// only exposes raw Event{} usage in tests/benchmarks. No
			// asserter trident fires for the empty-schema case (T8).
			name:          "empty schema version yields base form",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "",
			want:          "midaz-ledger.transaction.created",
		},
		{
			// Malformed non-empty schema version falls through to the
			// base topic — the public Topic() contract is preserved.
			// The construction-time gate in NewEventDefinition catches
			// this earlier so a properly-cataloged event never reaches
			// Topic() with malformed semver; callers building Event
			// structs directly (tests, benchmarks) see the base form.
			// Topic() is a zero-allocation hot-path helper and does NOT
			// fire the asserter trident — see NewEventDefinition's
			// operation="event_definition.schema_version" instead.
			name:          "invalid semver falls through to base form",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "not-a-version",
			want:          "midaz-ledger.transaction.created",
		},
		{
			name:          "semver with v prefix accepted",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "v2.0.0",
			want:          "midaz-ledger.transaction.created.v2",
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

// TestSanitizeSourceSegment pins the ce-source → topic-namespace-segment
// reduction that Event.Topic() prepends. The transformation lowercases,
// maps every rune outside [a-z0-9._-] to a single '-', collapses '-' runs,
// and trims boundary separators. Empty input yields "" — a real empty
// Source never reaches here because ErrMissingSource rejects it upstream,
// but the function itself must stay total and not panic.
func TestSanitizeSourceSegment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "clean service name is preserved verbatim",
			source: "midaz-ledger",
			want:   "midaz-ledger",
		},
		{
			name:   "uppercase is lowercased",
			source: "MIDAZ-Ledger",
			want:   "midaz-ledger",
		},
		{
			name:   "uri-ish source reduces to a deterministic safe segment",
			source: "//lerian.midaz/transaction-service",
			want:   "lerian.midaz-transaction-service",
		},
		{
			name:   "scheme authority collapses to a single hyphen",
			source: "svc://tenant-cb-test",
			want:   "svc-tenant-cb-test",
		},
		{
			name:   "invalid chars are mapped to hyphen and runs collapse",
			source: "Order Service!!!",
			want:   "order-service",
		},
		{
			name:   "consecutive invalid chars collapse to one hyphen",
			source: "a###b",
			want:   "a-b",
		},
		{
			name:   "non-ascii runes are replaced",
			source: "café",
			want:   "caf",
		},
		{
			name:   "dots and underscores inside are preserved as delimiters",
			source: "lerian.midaz_transaction",
			want:   "lerian.midaz_transaction",
		},
		{
			name:   "leading and trailing separators are trimmed",
			source: "._-midaz-ledger-_.",
			want:   "midaz-ledger",
		},
		{
			name:   "empty source yields empty segment (rejected upstream by ErrMissingSource)",
			source: "",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeSourceSegment(tt.source); got != tt.want {
				t.Errorf("sanitizeSourceSegment(%q) = %q; want %q", tt.source, got, tt.want)
			}
		})
	}
}

// TestSanitizeSourceSegment_Deterministic pins that the reduction is stable
// for a given source — downstream Kafka ACLs scope a producer to
// "{sanitize(Source)}.*", so a non-deterministic segment would break topic
// authorization.
func TestSanitizeSourceSegment_Deterministic(t *testing.T) {
	t.Parallel()

	const source = "//lerian.midaz/transaction-service"
	first := sanitizeSourceSegment(source)
	second := sanitizeSourceSegment(source)
	if first != second {
		t.Errorf("sanitizeSourceSegment not deterministic: %q != %q", first, second)
	}
}

// TestEvent_Topic_ServicePrefix pins the service-namespaced derivation:
// Topic() must be "{sanitize(Source)}.<resource>.<event>" so downstream
// Kafka ACLs can scope a producer to its own topics. The fixed
// "lerian.streaming." prefix is GONE.
func TestEvent_Topic_ServicePrefix(t *testing.T) {
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
			name:         "clean service name becomes namespace",
			source:       "midaz-ledger",
			resourceType: "transaction",
			eventType:    "created",
			want:         "midaz-ledger.transaction.created",
		},
		{
			name:          "major >= 2 appends .v suffix under service prefix",
			source:        "midaz-ledger",
			resourceType:  "transaction",
			eventType:     "created",
			schemaVersion: "2.3.1",
			want:          "midaz-ledger.transaction.created.v2",
		},
		{
			name:         "uri-ish source reduces to a clean segment",
			source:       "//lerian.midaz/transaction-service",
			resourceType: "transaction",
			eventType:    "created",
			want:         "lerian.midaz-transaction-service.transaction.created",
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
			got := e.Topic()
			if got != tt.want {
				t.Errorf("Event.Topic() = %q; want %q", got, tt.want)
			}
			if strings.HasPrefix(got, "lerian.streaming.") {
				t.Errorf("Event.Topic() = %q; the fixed lerian.streaming. prefix must be gone", got)
			}
		})
	}
}

// TestEvent_PartitionKey covers the TRD §C1 rules: TenantID by default,
// "system:" + EventType when SystemEvent is true (DX-A05/A06 adjacent).
func TestEvent_PartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "tenant id is the default partition key",
			event: Event{TenantID: "t-abc", EventType: "created"},
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
			name:  "empty tenant and non-system returns empty string",
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
		Source:       "//lerian.midaz/tx-service",
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
