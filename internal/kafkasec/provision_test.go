//go:build unit

package kafkasec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// Not parallel, and neither are its subtests: t.Setenv mutates process state and
// panics in any test with a parallel ancestor.
func TestLoadProvisionConfig(t *testing.T) {
	tests := []struct {
		name            string
		env             map[string]string
		wantEnabled     bool
		wantPartitions  int32
		wantReplication int16
	}{
		{
			name:            "unset is enabled with broker defaults",
			env:             nil,
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "explicit opt-out disables provisioning",
			env:             map[string]string{envAutoProvision: "false"},
			wantEnabled:     false,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "explicit opt-in stays enabled",
			env:             map[string]string{envAutoProvision: "true"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "unparseable enable flag falls back to the ON default",
			env:             map[string]string{envAutoProvision: "banana"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name: "explicit partitions and replication factor are honoured",
			env: map[string]string{
				envPartitions:  "6",
				envReplication: "3",
			},
			wantEnabled:     true,
			wantPartitions:  6,
			wantReplication: 3,
		},
		{
			name:            "explicit -1 is the broker default",
			env:             map[string]string{envPartitions: "-1", envReplication: "-1"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "zero partitions falls back to broker default",
			env:             map[string]string{envPartitions: "0"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "zero replication factor falls back to broker default",
			env:             map[string]string{envReplication: "0"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "negative-but-not-minus-one falls back to broker default",
			env:             map[string]string{envPartitions: "-7", envReplication: "-7"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "partitions above int32 range falls back to broker default",
			env:             map[string]string{envPartitions: strconv.FormatInt(math.MaxInt32+1, 10)},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "replication factor above int16 range falls back to broker default",
			env:             map[string]string{envReplication: strconv.FormatInt(math.MaxInt16+1, 10)},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
		{
			name:            "unparseable numerics fall back to broker defaults",
			env:             map[string]string{envPartitions: "many", envReplication: "lots"},
			wantEnabled:     true,
			wantPartitions:  BrokerDefault,
			wantReplication: BrokerDefault,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Pin all three FIRST so the case is hermetic. A developer or CI
			// runner exporting STREAMING_TOPIC_AUTO_PROVISION=false (a perfectly
			// reasonable local setting) would otherwise fail the "unset" cases,
			// and the failure would read as a library bug. Empty string is
			// equivalent to unset for the commons getters: ParseBool("") and
			// ParseInt("") both fail and return the default, and the empty value
			// suppresses their malformed-value warning.
			for _, key := range []string{envAutoProvision, envPartitions, envReplication} {
				t.Setenv(key, "")
			}

			for key, value := range test.env {
				t.Setenv(key, value)
			}

			got := LoadProvisionConfig()

			if got.Enabled != test.wantEnabled {
				t.Errorf("Enabled = %v, want %v", got.Enabled, test.wantEnabled)
			}

			if got.Partitions != test.wantPartitions {
				t.Errorf("Partitions = %d, want %d", got.Partitions, test.wantPartitions)
			}

			if got.ReplicationFactor != test.wantReplication {
				t.Errorf("ReplicationFactor = %d, want %d", got.ReplicationFactor, test.wantReplication)
			}
		})
	}
}

var errRequestFailed = errors.New("dial tcp 10.0.0.1:9092: connect: connection refused")

func TestInterpretCreateResponses(t *testing.T) {
	t.Parallel()

	const topic = "lerian.streaming.lender"

	tests := []struct {
		name        string
		topics      []string
		responses   kadm.CreateTopicResponses
		requestErr  error
		wantVerdict []provisionVerdict
	}{
		{
			name:      "a fresh topic reports created",
			topics:    []string{topic},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: nil}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeCreated},
			},
		},
		{
			// The verdict records what the broker said (the error is preserved),
			// but the OUTCOME is a success and logProvisionVerdicts stays silent
			// on it — this is the steady state on every restart of every replica.
			name:      "TOPIC_ALREADY_EXISTS is silent success",
			topics:    []string{topic},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: kerr.TopicAlreadyExists}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeAlreadyExists, Err: kerr.TopicAlreadyExists},
			},
		},
		{
			name:      "TOPIC_AUTHORIZATION_FAILED is an unauthorized verdict",
			topics:    []string{topic},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: kerr.TopicAuthorizationFailed}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeUnauthorized, Err: kerr.TopicAuthorizationFailed},
			},
		},
		{
			name:      "CLUSTER_AUTHORIZATION_FAILED is an unauthorized verdict",
			topics:    []string{topic},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: kerr.ClusterAuthorizationFailed}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeUnauthorized, Err: kerr.ClusterAuthorizationFailed},
			},
		},
		{
			name:      "any other broker error is a failed verdict",
			topics:    []string{topic},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: kerr.InvalidReplicationFactor}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeFailed, Err: kerr.InvalidReplicationFactor},
			},
		},
		{
			name:       "a request-level failure fails every requested topic",
			topics:     []string{topic, topic + ".dlq"},
			requestErr: errRequestFailed,
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeFailed, Err: errRequestFailed},
				{Topic: topic + ".dlq", Outcome: outcomeFailed, Err: errRequestFailed},
			},
		},
		{
			name:      "a requested topic missing from the response is a failed verdict, never a silent pass",
			topics:    []string{topic, topic + ".dlq"},
			responses: kadm.CreateTopicResponses{topic: {Topic: topic, Err: nil}},
			wantVerdict: []provisionVerdict{
				{Topic: topic, Outcome: outcomeCreated},
				{Topic: topic + ".dlq", Outcome: outcomeFailed, Err: errTopicAbsentFromResponse},
			},
		},
		{
			name:        "no requested topics yields no verdicts",
			topics:      nil,
			responses:   kadm.CreateTopicResponses{},
			wantVerdict: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := interpretCreateResponses(test.responses, test.requestErr, test.topics)

			if len(got) != len(test.wantVerdict) {
				t.Fatalf("verdict count = %d, want %d (got %+v)", len(got), len(test.wantVerdict), got)
			}

			for i, want := range test.wantVerdict {
				if got[i].Topic != want.Topic {
					t.Errorf("verdict[%d].Topic = %q, want %q", i, got[i].Topic, want.Topic)
				}

				if got[i].Outcome != want.Outcome {
					t.Errorf("verdict[%d].Outcome = %v, want %v", i, got[i].Outcome, want.Outcome)
				}

				if want.Err != nil && !errors.Is(got[i].Err, want.Err) {
					t.Errorf("verdict[%d].Err = %v, want errors.Is %v", i, got[i].Err, want.Err)
				}

				if want.Err == nil && got[i].Err != nil {
					t.Errorf("verdict[%d].Err = %v, want nil", i, got[i].Err)
				}
			}
		})
	}
}

// provisionSpyLogger records every Log call so the failure POSTURE can be
// asserted: an unauthorized creation must produce an operator-actionable WARN,
// never an error return and never silence.
type provisionSpyLogger struct {
	mu      sync.Mutex
	entries []provisionSpyEntry
}

type provisionSpyEntry struct {
	level  int
	msg    string
	fields map[string]any
}

func (s *provisionSpyLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	indexed := make(map[string]any, len(fields))
	for _, f := range log.Fields(fields...) {
		indexed[f.Key] = f.Value
	}

	s.entries = append(s.entries, provisionSpyEntry{level: level, msg: msg, fields: indexed})
}

func (s *provisionSpyLogger) With(...any) log.Logger { return s }
func (s *provisionSpyLogger) WithGroup(string) log.Logger  { return s }
func (s *provisionSpyLogger) Enabled(int) bool       { return true }
func (s *provisionSpyLogger) Sync(context.Context) error   { return nil }

// TestLogProvisionVerdicts_Posture pins what an operator actually sees. The
// message text is load-bearing, not decoration: an unauthorized creation is the
// NORMAL state in a hardened environment, so the line has to say which topic,
// which ACL, and how to opt out — otherwise it reads as a broker fault.
func TestLogProvisionVerdicts_Posture(t *testing.T) {
	t.Parallel()

	const topic = "lerian.streaming.lender"

	cfg := ProvisionConfig{Enabled: true, Partitions: BrokerDefault, ReplicationFactor: BrokerDefault}

	t.Run("already-exists logs nothing at all", func(t *testing.T) {
		t.Parallel()

		spy := &provisionSpyLogger{}
		logProvisionVerdicts(context.Background(), spy, cfg, []provisionVerdict{
			{Topic: topic, Outcome: outcomeAlreadyExists, Err: kerr.TopicAlreadyExists},
		})

		if len(spy.entries) != 0 {
			t.Errorf("already-exists produced %d log entries; want 0 (steady state on every restart)", len(spy.entries))
		}
	})

	t.Run("created logs one INFO naming the topic", func(t *testing.T) {
		t.Parallel()

		spy := &provisionSpyLogger{}
		logProvisionVerdicts(context.Background(), spy, cfg, []provisionVerdict{
			{Topic: topic, Outcome: outcomeCreated},
		})

		if len(spy.entries) != 1 {
			t.Fatalf("created produced %d log entries; want exactly 1", len(spy.entries))
		}

		if spy.entries[0].level != log.LevelInfo {
			t.Errorf("created logged at level %v; want INFO", spy.entries[0].level)
		}

		if got := spy.entries[0].fields["topic"]; got != topic {
			t.Errorf("created entry topic field = %v, want %q", got, topic)
		}
	})

	t.Run("unauthorized logs one actionable WARN", func(t *testing.T) {
		t.Parallel()

		spy := &provisionSpyLogger{}
		logProvisionVerdicts(context.Background(), spy, cfg, []provisionVerdict{
			{Topic: topic, Outcome: outcomeUnauthorized, Err: kerr.TopicAuthorizationFailed},
		})

		if len(spy.entries) != 1 {
			t.Fatalf("unauthorized produced %d log entries; want exactly 1", len(spy.entries))
		}

		entry := spy.entries[0]

		if entry.level != log.LevelWarn {
			t.Errorf("unauthorized logged at level %v; want WARN (it must NOT fail construction)", entry.level)
		}

		if got := entry.fields["topic"]; got != topic {
			t.Errorf("WARN topic field = %v, want %q", got, topic)
		}

		// The two things an operator needs to act: the exact ACL to grant, and
		// the way to turn provisioning off in a pre-provisioned environment.
		if got, ok := entry.fields["required_acl"].(string); !ok || !strings.Contains(got, topic) {
			t.Errorf("WARN required_acl field = %v, want a grant naming %q", entry.fields["required_acl"], topic)
		}

		if got, ok := entry.fields["opt_out"].(string); !ok || !strings.Contains(got, envAutoProvision) {
			t.Errorf("WARN opt_out field = %v, want it to name %s", entry.fields["opt_out"], envAutoProvision)
		}
	})

	t.Run("a dial failure WARN carries no broker credentials", func(t *testing.T) {
		t.Parallel()

		spy := &provisionSpyLogger{}
		leaky := errors.New(`dial "SASL_SSL://admin:sup3rs3cret@broker.internal:9092" refused`)

		logProvisionVerdicts(context.Background(), spy, cfg, []provisionVerdict{
			{Topic: topic, Outcome: outcomeFailed, Err: leaky},
		})

		if len(spy.entries) != 1 {
			t.Fatalf("failed produced %d log entries; want exactly 1", len(spy.entries))
		}

		if spy.entries[0].level != log.LevelWarn {
			t.Errorf("failed logged at level %v; want WARN", spy.entries[0].level)
		}

		rendered, ok := spy.entries[0].fields["error"]
		if !ok {
			t.Fatalf("failed WARN has no error field; fields = %v", spy.entries[0].fields)
		}

		if text := fmt.Sprint(rendered); strings.Contains(text, "sup3rs3cret") {
			t.Errorf("failed WARN leaked a broker credential: %q", text)
		}
	})
}
