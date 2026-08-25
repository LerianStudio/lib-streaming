//go:build unit

package streaming

import (
	"slices"
	"testing"
)

// TestProducerOwnedTopics pins the PRODUCER half of the ownership rule: a runtime
// ensures every topic it writes UNDER ITS OWN SOURCE NAMESPACE.
//
// For a producer that is its fact topic and its dead-letter topic. Both names are
// derived from its own validated ce-source, so neither can reach into another
// application's namespace.
func TestProducerOwnedTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source string
		want   []string
	}{
		{
			name:   "a producer owns its fact topic and its DLQ",
			source: "lender",
			want:   []string{"lerian.streaming.lender", "lerian.streaming.lender.dlq"},
		},
		{
			name:   "hyphenated sources derive both names unchanged",
			source: "br-consignado-gw",
			want:   []string{"lerian.streaming.br-consignado-gw", "lerian.streaming.br-consignado-gw.dlq"},
		},
		{
			name:   "an empty source yields nothing rather than garbage names",
			source: "",
			want:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := producerOwnedTopics(test.source)

			if !slices.Equal(got, test.want) {
				t.Errorf("producerOwnedTopics(%q) = %v, want %v", test.source, got, test.want)
			}
		})
	}
}

// TestProducerOwnedTopicsExcludesCommandsQueue is a BOUNDARY test, not a
// redundant one. The producer's own commands queue
// ("lerian.streaming.<source>.commands") IS in its own namespace and IS written
// by a producer whose catalog holds a Class: ClassCommand definition — see
// internal/producer/producer_multi.go, which derives it from the producing
// application's own source.
//
// It is deliberately NOT provisioned. This test exists so that stays a decision
// rather than an accident: if the boundary moves, this test fails and forces the
// docs and the PR body to move with it.
func TestProducerOwnedTopicsExcludesCommandsQueue(t *testing.T) {
	t.Parallel()

	if got := producerOwnedTopics("lender"); slices.Contains(got, "lerian.streaming.lender.commands") {
		t.Errorf("producerOwnedTopics included the commands queue (%v); provisioning it is a deliberate exclusion", got)
	}
}
