package consumer

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/kafkasec"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// kgoGroupClient is the production GroupClient backed by a franz-go group
// client. It is a thin pass-through: all decision logic lives in the runtime, so
// this wrapper exists only to make that logic testable against a scripted fake
// (Req 5).
//
// The wrapped client MUST be constructed with kgo.BlockRebalanceOnPoll() (so
// SetOffsets cannot race a revoke, Req 3) plus kgo.ConsumerGroup,
// kgo.ConsumeTopics, kgo.DisableAutoCommit, and the shared TLS/SASL dial options
// (via internal/kafkasec). newKgoGroupClient is the only constructor.
type kgoGroupClient struct {
	client *kgo.Client
}

// buildConsumerKgoOpts assembles the franz-go option list for the consume
// client from validated ConsumerConfig. TLS/SASL go through internal/kafkasec —
// the SAME validation the producer enforces, so the two clients never drift on a
// hardening change. Split out from newKgoGroupClient so the option set is
// unit-inspectable without dialing a broker.
func buildConsumerKgoOpts(cfg ConsumerConfig) ([]kgo.Opt, error) {
	base, err := buildDLQKgoOpts(cfg)
	if err != nil {
		return nil, err
	}

	// Consumer-group + subscribe + commit/rebalance discipline. These are GROUP
	// MEMBER options and must NEVER be applied to the produce-only DLQ client —
	// a non-polling group member holding an assignment starves the real consumer
	// (a rebalance hazard on a live broker; a ~50% group-join stall under kfake).
	return append(base,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		// Req 3: freeze rebalances for the life of each polled batch so
		// SetOffsets (seek-back) cannot race a group revoke. The runtime calls
		// AllowRebalance exactly once per cycle after all seek-backs are staged.
		kgo.BlockRebalanceOnPoll(),
		// Commits are explicit per the at-least-once state machine — never
		// franz-go's background auto-commit, which would commit ahead of the
		// handler and break at-least-once.
		kgo.DisableAutoCommit(),
	), nil
}

// buildDLQKgoOpts assembles the PRODUCE-ONLY franz-go option list for the DLQ
// adapter: brokers, client id, and the shared TLS/SASL hardening — but NONE of
// the consumer-group / subscribe / block-rebalance options. The DLQ adapter only
// ever Produces (transportDLQPublisher.PublishDLQ -> adapter.Publish); giving it
// the consumer-group options would make it a phantom second member of the real
// consumer's group, holding an assignment it never polls. It is also the shared
// security/transport base buildConsumerKgoOpts layers the group options on top of.
func buildDLQKgoOpts(cfg ConsumerConfig) ([]kgo.Opt, error) {
	if err := kafkasec.ValidateTLSConfig(cfg.tlsConfig); err != nil {
		return nil, err
	}

	tlsConfig := kafkasec.CloneTLSConfigWithDefaults(cfg.tlsConfig)

	saslMechanism := cfg.saslMechanism
	if transport.IsNilInterface(saslMechanism) {
		saslMechanism = nil
	}

	if err := kafkasec.SASLRequiresTLS(saslMechanism != nil, tlsConfig != nil, cfg.allowPlaintextSASL); err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
	}

	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	if tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if saslMechanism != nil {
		opts = append(opts, kgo.SASL(saslMechanism))
	}

	return opts, nil
}

// newKgoGroupClient builds the franz-go group client from ConsumerConfig and
// wraps it. Mirrors internal/transport/kafka.NewAdapter on the produce side.
func newKgoGroupClient(_ context.Context, cfg ConsumerConfig) (*kgoGroupClient, error) {
	opts, err := buildConsumerKgoOpts(cfg)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("streaming consumer: kgo client init: %s", contract.SanitizeBrokerURL(err.Error()))
	}

	return &kgoGroupClient{client: client}, nil
}

func (k *kgoGroupClient) PollFetches(ctx context.Context) kgo.Fetches {
	if k == nil || k.client == nil {
		return kgo.Fetches{}
	}

	return k.client.PollFetches(ctx)
}

func (k *kgoGroupClient) CommitRecords(ctx context.Context, recs ...*kgo.Record) error {
	if k == nil || k.client == nil {
		return contract.ErrNilProducer
	}

	return k.client.CommitRecords(ctx, recs...)
}

func (k *kgoGroupClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	if k == nil || k.client == nil {
		return
	}

	k.client.SetOffsets(offsets)
}

func (k *kgoGroupClient) AllowRebalance() {
	if k == nil || k.client == nil {
		return
	}

	k.client.AllowRebalance()
}

func (k *kgoGroupClient) Close() {
	if k == nil || k.client == nil {
		return
	}

	k.client.Close()
}
