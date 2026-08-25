//go:build unit

package consumer

import (
	"slices"
	"testing"
)

// TestOwnedTopics pins the OWNERSHIP RULE for consumer-side auto-provisioning:
// a consumer ensures only the topics IT writes or owns the name of. Its own DLQ
// always (it is that topic's producer), and its own commands queue when it is
// the app being commanded. Never another application's topic — those belong to
// their producers, and creating one here would both mask a misconfigured
// subscription and reach outside this application's Kafka grant.
func TestOwnedTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  ConsumerConfig
		want []string
	}{
		{
			name: "a fact-only consumer ensures just its own DLQ",
			cfg: ConsumerConfig{
				Source: "lender",
				Apps:   []string{"midaz", "tracer"},
			},
			want: []string{"lerian.streaming.lender.dlq"},
		},
		{
			name: "subscribing to another app's fact topic never provisions it",
			cfg: ConsumerConfig{
				Source: "lender",
				Apps:   []string{"midaz"},
			},
			want: []string{"lerian.streaming.lender.dlq"},
		},
		{
			name: "taking commands addressed to THIS app ensures its own commands queue",
			cfg: ConsumerConfig{
				Source:   "lender",
				Commands: []string{"lender"},
			},
			want: []string{"lerian.streaming.lender.dlq", "lerian.streaming.lender.commands"},
		},
		{
			name: "taking ANOTHER app's commands queue never provisions it",
			cfg: ConsumerConfig{
				Source:   "lender",
				Commands: []string{"midaz"},
			},
			want: []string{"lerian.streaming.lender.dlq"},
		},
		{
			name: "own commands queue is ensured even when other apps are also commanded",
			cfg: ConsumerConfig{
				Source:   "lender",
				Commands: []string{"midaz", "lender", "tracer"},
			},
			want: []string{"lerian.streaming.lender.dlq", "lerian.streaming.lender.commands"},
		},
		{
			name: "raw Topics escape hatch is never provisioned",
			cfg: ConsumerConfig{
				Source: "lender",
				Topics: []string{"legacy.stream", "lerian.streaming.midaz.commands"},
			},
			want: []string{"lerian.streaming.lender.dlq"},
		},
		{
			name: "a raw Topics entry spelling this app's own commands queue does NOT promote it",
			cfg: ConsumerConfig{
				Source: "lender",
				Topics: []string{"lerian.streaming.lender.commands"},
			},
			want: []string{"lerian.streaming.lender.dlq"},
		},
		{
			name: "an empty source yields nothing rather than a garbage topic name",
			cfg:  ConsumerConfig{Source: ""},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := ownedTopics(test.cfg)

			if !slices.Equal(got, test.want) {
				t.Errorf("ownedTopics() = %v, want %v", got, test.want)
			}
		})
	}
}
