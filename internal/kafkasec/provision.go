package kafkasec

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/LerianStudio/lib-streaming/v3/obs"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-commons/v6/commons"
	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// Topic auto-provisioning environment variables. They live here rather than on
// internal/config.Config or ConsumerConfig for one reason: they must be honoured
// on EVERY construction path. The producer Builder is fluent and never loads
// internal/config.Config unless the caller opts in with TLSFromConfig /
// SASLFromConfig, so a knob plumbed through that struct would be silently inert
// on the default Builder path — a documented setting that reads nothing. One
// shared triple, read at the one place that acts on it, is honest instead.
const (
	envAutoProvision = "STREAMING_TOPIC_AUTO_PROVISION"
	envPartitions    = "STREAMING_TOPIC_PARTITIONS"
	envReplication   = "STREAMING_TOPIC_REPLICATION_FACTOR"
)

// BrokerDefault tells the broker to choose the partition count / replication
// factor from its own defaults (num.partitions, default.replication.factor).
// It is the wire value Kafka defines for that, from CreateTopics v4 (KIP-464)
// onward, and it is what lib-streaming sends unless an operator overrides it.
//
// Choosing broker defaults rather than a library opinion is deliberate: the
// right partition count is a capacity decision that belongs to whoever sized
// the cluster, and a hard-coded default here would silently under-partition
// every high-throughput stream on the platform.
const BrokerDefault = -1

// provisionTimeout bounds the CreateTopics round-trip. It exists because
// provisioning is ON by default and runs inside construction: this call is the
// FIRST and only broker I/O Build performs — before it, Build did none at all,
// because franz-go dials lazily — so without a bound a broker outage would turn
// a Build that used to succeed regardless into a hung boot.
//
// The envelope is intentionally NOT configurable — same posture as the CB
// recovery interval and the SQS resolver's retry budget. 10s is far above a
// healthy controller round-trip (sub-100ms) and far below any Lerian readiness
// budget, and on expiry the WARN path fires and construction proceeds.
const provisionTimeout = 10 * time.Second

// errTopicAbsentFromResponse is recorded when the broker's CreateTopics response
// omits a topic we asked for. It is a failed verdict rather than an assumed
// success: a missing response is exactly as unproven as an error, and treating
// silence as compliance is how a provisioning gap hides behind a green boot.
var errTopicAbsentFromResponse = errors.New("broker omitted this topic from the CreateTopics response")

// ProvisionConfig is the resolved auto-provisioning setting.
type ProvisionConfig struct {
	// Enabled defaults to TRUE. Automatic provisioning is the product default:
	// declaring an event is meant to be the whole job. Hardened environments
	// that pre-provision through IaC opt OUT with
	// STREAMING_TOPIC_AUTO_PROVISION=false, and their credential usually lacks
	// CreateTopics anyway — which is a WARN, not a boot failure (see
	// EnsureTopics).
	Enabled bool
	// Partitions is the CreateTopics partition count, or BrokerDefault.
	Partitions int32
	// ReplicationFactor is the CreateTopics replication factor, or BrokerDefault.
	ReplicationFactor int16
}

// LoadProvisionConfig resolves the auto-provisioning knobs from the environment.
//
// It is lenient in the same direction as LoadConfig / LoadConsumerConfig: an
// unparseable or out-of-range value falls back to the documented default rather
// than failing. The fallback for both numerics is BrokerDefault, so a typo
// yields a correctly-sized topic chosen by the broker instead of a topic with a
// nonsense partition count or a refused creation.
func LoadProvisionConfig() ProvisionConfig {
	return ProvisionConfig{
		Enabled:           commons.GetenvBoolOrDefault(envAutoProvision, true),
		Partitions:        resolvePartitions(),
		ReplicationFactor: resolveReplicationFactor(),
	}
}

// resolvePartitions accepts BrokerDefault or any positive count that fits int32.
// Zero is rejected to BrokerDefault because brokers answer a zero partition
// count with INVALID_PARTITIONS — an operator who typed 0 wants "whatever is
// normal", not a failed creation.
func resolvePartitions() int32 {
	raw := commons.GetenvIntOrDefault(envPartitions, BrokerDefault)
	if raw < 1 || raw > math.MaxInt32 {
		return BrokerDefault
	}

	return int32(raw)
}

// resolveReplicationFactor accepts BrokerDefault or any positive factor that
// fits int16 (the wire type).
func resolveReplicationFactor() int16 {
	raw := commons.GetenvIntOrDefault(envReplication, BrokerDefault)
	if raw < 1 || raw > math.MaxInt16 {
		return BrokerDefault
	}

	return int16(raw)
}

// provisionOutcome is the per-topic verdict of one CreateTopics attempt.
type provisionOutcome int

const (
	// outcomeCreated: the topic did not exist and now does.
	outcomeCreated provisionOutcome = iota
	// outcomeAlreadyExists: TOPIC_ALREADY_EXISTS — silent success. This is the
	// steady state on every restart and every replica beyond the first, so it
	// must not produce log noise.
	outcomeAlreadyExists
	// outcomeUnauthorized: the credential lacks CreateTopics on this name. The
	// expected state in a hardened, IaC-provisioned environment.
	outcomeUnauthorized
	// outcomeFailed: anything else, including a request that never reached a
	// broker and a topic the broker did not answer for.
	outcomeFailed
)

// provisionVerdict pairs a requested topic with what the broker said about it.
type provisionVerdict struct {
	Topic   string
	Outcome provisionOutcome
	Err     error
}

// EnsureTopics creates each named topic on the cluster the given client is
// already connected to, and NEVER fails the caller.
//
// # Why it runs inside construction
//
// Every topic name lib-streaming uses is derived from declarations a runtime
// already receives — the app's ce-source, its catalog, its subscriptions. So the
// moment a developer declares an event, the name of the topic it needs is
// knowable, and nothing else in the platform creates it: Lerian brokers run
// auto_create_topics_enabled=false (correct hardening) and the streaming-hub
// reconciler is read-only by design. The gap that leaves is not loud. A producer
// initializes cleanly and its FIRST PUBLISH fails with UNKNOWN_TOPIC_OR_PARTITION;
// a consumer subscribed to a nonexistent topic is INDISTINGUISHABLE from one on
// an idle topic — franz-go surfaces no topic-specific fetch error, so the poll
// loop reports healthy and consumes nothing, forever.
//
// # Why it only warns
//
// A failed creation MUST NOT refuse construction. The reason is one-sided and
// worth stating precisely, because the two runtimes behave very differently
// afterwards:
//
//   - AUTHORIZATION failures are the NORMAL state in a hardened environment,
//     where topics come from IaC and the runtime credential deliberately has no
//     CreateTopics. Boot-refusal there would break exactly the deployments that
//     are configured correctly. This is the load-bearing reason on both sides.
//   - On the PRODUCE side the later failure is genuinely loud: a publish to a
//     missing topic fails with ClassTopicNotFound and the error is RETURNED to
//     the caller, so it is already fail-closed. Note this is NOT the outbox
//     doing the work — outbox fallback covers circuit-open only, and only when a
//     caller wired one; an unwired producer simply gets the error back.
//   - On the CONSUME side nothing is loud, and this WARN is the ONLY signal.
//     A missing subscribed topic is indistinguishable from an idle one, so the
//     consumer reports healthy and consumes nothing. Alert on this log line;
//     there is no metric behind it and no later error to catch.
//
// So the actionable message goes to the log — naming the topic and the missing
// ACL — and construction continues. The credential needs CREATE on the named
// topic (or on the cluster); alternatively set
// STREAMING_TOPIC_AUTO_PROVISION=false and pre-provision through IaC.
//
// # Client ownership
//
// The admin client WRAPS the caller's live *kgo.Client, so the broker dial —
// brokers, TLS, SASL — is byte-identical to the one the runtime already
// validated through this package; there is no second connection configuration to
// drift. Ownership stays with the caller: kadm.Client.Close would close the
// wrapped kgo client, so this function never closes anything.
//
// Kafka-only by construction: it takes a *kgo.Client, so the SQS, RabbitMQ, and
// EventBridge adapters are untouched.
func EnsureTopics(ctx context.Context, client *kgo.Client, logger obs.Logger, topics ...string) {
	cfg := LoadProvisionConfig()
	if !cfg.Enabled || client == nil || len(topics) == 0 {
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if logger == nil {
		logger = log.NewNop()
	}

	// Cap the round-trip while still honouring an earlier caller deadline.
	ctx, cancel := context.WithTimeout(ctx, provisionTimeout)
	defer cancel()

	responses, err := kadm.NewClient(client).CreateTopics(ctx, cfg.Partitions, cfg.ReplicationFactor, nil, topics...)

	logProvisionVerdicts(ctx, logger, cfg, interpretCreateResponses(responses, err, topics))
}

// interpretCreateResponses classifies one CreateTopics attempt into a verdict per
// REQUESTED topic, in request order.
//
// The denominator is the requested list, never the response map. kadm reports
// per-topic authorization failures inside the responses and returns a top-level
// error only when the request itself could not be issued, so both shapes have to
// be folded in — and a requested topic the broker simply did not answer for is
// recorded as failed rather than skipped.
func interpretCreateResponses(responses kadm.CreateTopicResponses, requestErr error, topics []string) []provisionVerdict {
	if len(topics) == 0 {
		return nil
	}

	verdicts := make([]provisionVerdict, 0, len(topics))

	for _, topic := range topics {
		// The request never reached a broker: every requested topic is unproven.
		if requestErr != nil {
			verdicts = append(verdicts, provisionVerdict{Topic: topic, Outcome: outcomeFailed, Err: requestErr})
			continue
		}

		response, ok := responses[topic]
		if !ok {
			verdicts = append(verdicts, provisionVerdict{Topic: topic, Outcome: outcomeFailed, Err: errTopicAbsentFromResponse})
			continue
		}

		verdicts = append(verdicts, provisionVerdict{
			Topic:   topic,
			Outcome: classifyCreateErr(response.Err),
			Err:     response.Err,
		})
	}

	return verdicts
}

// classifyCreateErr maps a per-topic CreateTopics error code to a verdict.
func classifyCreateErr(err error) provisionOutcome {
	switch {
	case err == nil:
		return outcomeCreated
	case errors.Is(err, kerr.TopicAlreadyExists):
		return outcomeAlreadyExists
	case errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed):
		return outcomeUnauthorized
	default:
		return outcomeFailed
	}
}

// logProvisionVerdicts emits the operator-facing record of one attempt: an INFO
// for a topic that was actually created (a real change to the cluster, and the
// line an operator wants when a new stream appears), a WARN carrying the
// remediation for anything unauthorized or failed, and NOTHING for
// already-exists — the steady state on every restart of every replica.
//
// Error text is passed through contract.SanitizeBrokerURL: a dial failure
// renders the broker address, which may carry SASL credentials.
func logProvisionVerdicts(ctx context.Context, logger obs.Logger, cfg ProvisionConfig, verdicts []provisionVerdict) {
	for _, verdict := range verdicts {
		switch verdict.Outcome {
		case outcomeAlreadyExists:
			continue

		case outcomeCreated:
			logger.Log(ctx, obs.LevelInfo,
				"streaming: created Kafka topic automatically",
				"topic", verdict.Topic,
				"partitions", int(cfg.Partitions),
				"replication_factor", int(cfg.ReplicationFactor),
			)

		case outcomeUnauthorized:
			logger.Log(ctx, obs.LevelWarn,
				"streaming: cannot auto-create Kafka topic — this credential lacks CreateTopics on it. "+
					"Grant CREATE on the named topic (or on the CLUSTER) to the principal this service authenticates as, "+
					"or pre-provision the topic through IaC and set STREAMING_TOPIC_AUTO_PROVISION=false to silence this. "+
					"Startup continues. If this topic is one this service PUBLISHES to, a publish will fail and return "+
					"the error to the caller until it exists. If it is one this service CONSUMES, there is no later "+
					"error and no metric — a missing topic looks exactly like an idle one, so this log line is the "+
					"only signal and the service will report healthy while consuming nothing",
				"topic", verdict.Topic,
				"required_acl", "CREATE on TOPIC "+verdict.Topic,
				"opt_out", envAutoProvision+"=false",
				"error", sanitized(verdict.Err),
			)

		default:
			logger.Log(ctx, obs.LevelWarn,
				"streaming: Kafka topic auto-creation failed — startup continues. A publish to this topic will "+
					"fail and return the error to the caller until it exists; a SUBSCRIPTION to it fails silently "+
					"instead (a missing topic is indistinguishable from an idle one), so this log line is the only "+
					"signal on the consume side. Create the topic manually, or set "+
					"STREAMING_TOPIC_AUTO_PROVISION=false if this environment provisions topics through IaC",
				"topic", verdict.Topic,
				"opt_out", envAutoProvision+"=false",
				"error", sanitized(verdict.Err),
			)
		}
	}
}

// sanitized strips broker credentials from an error before it reaches a log
// field. Returns nil unchanged so log.Err stays well-defined.
func sanitized(err error) error {
	if err == nil {
		return nil
	}

	return errors.New(contract.SanitizeBrokerURL(err.Error()))
}
