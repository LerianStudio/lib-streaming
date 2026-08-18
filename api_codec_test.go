//go:build unit

package streaming_test

import (
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// TestCloudEventsType_ComposesTheWireValue pins the ce-type the root facade
// hands to consumers, as LITERAL strings.
//
// A consumer that matches on ce-type instead of the ce-resourcetype /
// ce-eventtype pair builds the string with this function; deriving the
// expectation from the same constant the producer uses would let a prefix or
// separator change agree with itself on both sides and pass.
//
// The <source> segment is the v3 addition: without it two services publishing
// the same resource and event names produce byte-identical ce-type values, a
// homonym collision the topic collapse makes reachable in practice.
func TestCloudEventsType_ComposesTheWireValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		resourceType string
		eventType    string
		want         string
	}{
		{
			name:         "plain",
			source:       "lender",
			resourceType: "loan",
			eventType:    "disbursed",
			want:         "studio.lerian.lender.loan.disbursed",
		},
		{
			name:         "snake_case resource type travels verbatim",
			source:       "lender",
			resourceType: "loan_contract",
			eventType:    "disbursed",
			want:         "studio.lerian.lender.loan_contract.disbursed",
		},
		{
			name:         "the source segment separates same-named events",
			source:       "matcher",
			resourceType: "loan",
			eventType:    "disbursed",
			want:         "studio.lerian.matcher.loan.disbursed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := streaming.CloudEventsType(tt.source, tt.resourceType, tt.eventType); got != tt.want {
				t.Errorf("CloudEventsType(%q, %q, %q) = %q; want %q",
					tt.source, tt.resourceType, tt.eventType, got, tt.want)
			}
		})
	}
}

// TestCloudEventsType_MatchesTheHeaderThePublishPathWrites closes the loop: the
// facade helper and the real publish path must agree, or a consumer matching on
// ce-type silently matches nothing.
func TestCloudEventsType_MatchesTheHeaderThePublishPathWrites(t *testing.T) {
	t.Parallel()

	event := streaming.Event{
		Source:        "lender",
		ResourceType:  "loan_contract",
		EventType:     "disbursed",
		EventID:       "evt-1",
		SchemaVersion: "1.0.0",
	}

	want := streaming.CloudEventsType(event.Source, event.ResourceType, event.EventType)

	for _, h := range streaming.BuildCloudEventsHeaders(event) {
		if h.Key != "ce-type" {
			continue
		}

		if got := string(h.Value); got != want {
			t.Fatalf("ce-type header = %q; CloudEventsType() = %q", got, want)
		}

		return
	}

	t.Fatal("no ce-type header in BuildCloudEventsHeaders output")
}
