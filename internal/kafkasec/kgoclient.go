package kafkasec

import (
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
	"github.com/LerianStudio/lib-streaming/v3/internal/transport"
)

// SecurityKgoOpts is the ONE assembly of the broker-dial security options every
// franz-go client in this module uses: the producer, the consumer (and its
// produce-only DLQ client), and the admin client behind
// streaming.NewAdminClient.
//
// It performs the full four-step gate in a fixed order, because each step
// depends on the previous one:
//
//  1. ValidateTLSConfig rejects a caller-supplied config that would weaken the
//     dial (InsecureSkipVerify, sub-1.2 versions, off-allowlist cipher suites).
//  2. CloneTLSConfigWithDefaults floors MinVersion at TLS 1.2 and re-allocates
//     the policy slices, so no caller-reachable handle can weaken the stored
//     config after construction.
//  3. Typed-nil normalization, because a `var m sasl.Mechanism = (*x)(nil)`
//     compares non-nil and would make the SASL-requires-TLS gate read "SASL is
//     configured" for a mechanism that will never authenticate anything.
//  4. SASLRequiresTLS fails closed on SASL over plaintext unless the caller
//     explicitly opted into the unsafe local/dev path.
//
// The three call sites previously each carried their own copy of this sequence.
// One copy means a hardening change or a CVE response lands everywhere at once —
// which is the whole reason this package exists.
//
// The returned slice carries ONLY the security options. Brokers, client id, and
// every role-specific option (batching, acks, partitioner, consumer group) stay
// at the call site: an admin client must not inherit producer batching, and a
// produce-only DLQ client must not inherit consumer-group membership.
func SecurityKgoOpts(tlsConfig *tls.Config, mechanism sasl.Mechanism, allowPlaintextSASL bool) ([]kgo.Opt, error) {
	if err := ValidateTLSConfig(tlsConfig); err != nil {
		return nil, err
	}

	cloned := CloneTLSConfigWithDefaults(tlsConfig)

	if transport.IsNilInterface(mechanism) {
		mechanism = nil
	}

	if err := SASLRequiresTLS(mechanism != nil, cloned != nil, allowPlaintextSASL); err != nil {
		return nil, err
	}

	opts := make([]kgo.Opt, 0, 2)

	// franz-go's DialTLSConfig clones the config per-dial and auto-fills
	// ServerName from the broker host; callers rarely need to set it. We still
	// pass our own validated clone so caller mutations after option application
	// cannot weaken transport security before the first dial.
	if cloned != nil {
		opts = append(opts, kgo.DialTLSConfig(cloned))
	}

	// kgo.SASL is variadic; we always pass exactly one mechanism. Multi-mechanism
	// negotiation is out of scope until a real auth flow needs it.
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

// BuildAdminClient constructs a Kafka admin client that dials brokers with the
// SAME transport-security posture the producer and consumer use, and returns it
// wrapped in kadm.
//
// It exists because a consuming service cannot assemble this itself: the SASL
// mechanism builder and the TLS/SASL gate live in this internal package, so
// without a reachable constructor a caller wanting `kadm.DescribeTopicConfigs`
// (or any other admin round-trip) would have to re-implement the mechanism
// mapping and the fail-closed rules — a second copy that drifts on the first
// hardening change. streaming.NewAdminClient is the public door to this
// builder, mirroring streaming.NewSchemaRegistryClient.
//
// Behavior:
//   - brokers empty: returns an error wrapping ErrMissingBrokers. An admin
//     client with no seed broker cannot reach a cluster, so it fails closed
//     rather than handing back a client pinned to franz-go's localhost:9092
//     default.
//   - SASL configured without TLS and without the explicit plaintext opt-in:
//     returns an error wrapping ErrPlaintextSASLNotAllowed, inherited from
//     SecurityKgoOpts.
//   - a mechanism/TLS pair that passes the gate: returns a live client.
//
// Constructing the client performs NO broker I/O — franz-go dials lazily, so a
// misconfigured cluster surfaces on the caller's first admin request, not here.
//
// # Lifecycle
//
// The caller OWNS the returned client and MUST call Close on it. Unlike
// EnsureTopics — which wraps a runtime's already-live *kgo.Client and therefore
// never closes anything — this constructor creates the underlying *kgo.Client
// itself, so kadm.Client.Close closing that wrapped client is exactly the
// disposal the caller wants. That is why the shape is a single *kadm.Client and
// not a client plus a separate closer: kadm already exposes the only Close the
// caller needs.
//
// Broker addresses may carry SASL credentials, so a dial-construction failure is
// passed through contract.SanitizeBrokerURL before it reaches the caller.
func BuildAdminClient(brokers []string, clientID string, tlsConfig *tls.Config, mechanism sasl.Mechanism, allowPlaintextSASL bool) (*kadm.Client, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%w: an admin client requires at least one broker", contract.ErrMissingBrokers)
	}

	securityOpts, err := SecurityKgoOpts(tlsConfig, mechanism, allowPlaintextSASL)
	if err != nil {
		return nil, err
	}

	opts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, securityOpts...)
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("streaming admin: kgo client init: %s", contract.SanitizeBrokerURL(err.Error()))
	}

	return kadm.NewClient(client), nil
}
