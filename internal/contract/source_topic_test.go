//go:build unit

package contract

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateSource pins the v3 STRICT source contract: a source is a single
// dot-free lowercase segment matching ^[a-z0-9][a-z0-9_-]*$ whose derived app
// topic plus the ".dlq" suffix stays inside Kafka's 249-byte topic-name limit.
//
// v3 REJECTS an invalid source — it never rewrites one. The v2 lossy
// sanitizeSourceSegment normalization is gone, and with it the silent
// collision risk between two distinct services folding onto one topic
// namespace.
func TestValidateSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		source  string
		wantErr error
	}{
		{"simple lowercase", "lender", nil},
		{"digits", "midaz2", nil},
		{"hyphen", "midaz-ledger", nil},
		{"underscore", "br_consignado_gw", nil},
		{"leading digit", "0lender", nil},
		{"single char", "x", nil},
		{"max length", strings.Repeat("a", maxSourceSegmentBytes), nil},

		{"empty", "", ErrMissingSource},
		{"dotted", "lerian.midaz", ErrInvalidSource},
		{"uppercase", "Lender", ErrInvalidSource},
		{"uri shape", "//lerian.midaz/transaction-service", ErrInvalidSource},
		{"scheme shape", "svc://tenant-cb-test", ErrInvalidSource},
		{"leading hyphen", "-lender", ErrInvalidSource},
		{"leading underscore", "_lender", ErrInvalidSource},
		{"trailing space", "lender ", ErrInvalidSource},
		{"slash", "lerian/midaz", ErrInvalidSource},
		{"non-ascii", "lendér", ErrInvalidSource},
		{"control char", "lend\ner", ErrInvalidSource},
		{"separators only", "---", ErrInvalidSource},
		{"too long", strings.Repeat("a", maxSourceSegmentBytes+1), ErrInvalidSource},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateSource(tt.source)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateSource(%q) = %v; want nil", tt.source, err)
				}

				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidateSource(%q) = %v; want %v", tt.source, err, tt.wantErr)
			}
		})
	}
}

// TestAppTopic pins the v3 TOPIC COLLAPSE: one topic per producing
// application. No resource type, no event type, no schema-version suffix —
// ever.
func TestAppTopic(t *testing.T) {
	t.Parallel()

	if got, want := AppTopic("lender"), "lerian.streaming.lender"; got != want {
		t.Errorf("AppTopic(lender) = %q; want %q", got, want)
	}

	if got, want := AppDLQTopic("lender"), "lerian.streaming.lender.dlq"; got != want {
		t.Errorf("AppDLQTopic(lender) = %q; want %q", got, want)
	}
}

// TestAppTopicDLQFitsKafkaLimit pins that the source-length bound is derived
// from the DLQ topic (the longest derived name), not from the base topic.
//
// The numbers are HARDCODED on purpose. Deriving them from
// maxSourceSegmentBytes and MaxKafkaTopicNameBytes — the same constants the
// production code computes with — made the assertion agree with itself: change
// TopicPrefix to "lerian.stream." and both sides move together while every
// deployed topic name silently changes. 228 = 249 - len("lerian.streaming.") -
// len(".dlq"); 249 is Kafka's protocol limit.
func TestAppTopicDLQFitsKafkaLimit(t *testing.T) {
	t.Parallel()

	if maxSourceSegmentBytes != 228 {
		t.Fatalf("maxSourceSegmentBytes = %d; want 228 (249 - len(\"lerian.streaming.\") - len(\".dlq\"))", maxSourceSegmentBytes)
	}

	longest := strings.Repeat("a", 228)
	if got := len(AppDLQTopic(longest)); got != 249 {
		t.Fatalf("len(AppDLQTopic(228-byte source)) = %d; want Kafka's 249-byte limit exactly", got)
	}

	if got := len(AppDLQTopic(strings.Repeat("a", 229))); got <= 249 {
		t.Fatalf("a 229-byte source produced a %d-byte DLQ topic; the bound is off by one", got)
	}
}

// TestEventTopicIsAppTopic pins that Event.Topic() carries no per-event
// component: two different events from the same producer ride one topic.
func TestEventTopicIsAppTopic(t *testing.T) {
	t.Parallel()

	a := Event{Source: "lender", ResourceType: "loan", EventType: "disbursed", SchemaVersion: "1.0.0"}
	b := Event{Source: "lender", ResourceType: "installment", EventType: "settled", SchemaVersion: "7.2.1"}

	if a.Topic() != "lerian.streaming.lender" {
		t.Errorf("a.Topic() = %q; want lerian.streaming.lender", a.Topic())
	}

	if a.Topic() != b.Topic() {
		t.Errorf("topic collapse broken: %q != %q", a.Topic(), b.Topic())
	}
}

// TestEventTopicIgnoresSchemaVersionMajor pins that schema version left the
// topic entirely — ce-schemaversion is the only version carrier in v3.
func TestEventTopicIgnoresSchemaVersionMajor(t *testing.T) {
	t.Parallel()

	for _, version := range []string{"", "1.0.0", "2.0.0", "v9.1.4", "not-semver"} {
		t.Run("schema_version="+version, func(t *testing.T) {
			t.Parallel()

			e := Event{Source: "matcher", ResourceType: "recon", EventType: "matched", SchemaVersion: version}
			if got := e.Topic(); got != "lerian.streaming.matcher" {
				t.Errorf("Topic() with SchemaVersion=%q = %q; want lerian.streaming.matcher", version, got)
			}
		})
	}
}
