package producer

import (
	"crypto/tls"
	"fmt"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// maxBatchMaxBytes is the upper bound we will accept for
// Config.BatchMaxBytes. franz-go takes int32 for this option, so anything
// larger than math.MaxInt32 would overflow on the narrowing conversion.
// In practice brokers cap this around 1 MiB - 10 MiB; giving operators room
// for 2 GiB is already far past any real-world setting.
const maxBatchMaxBytes = math.MaxInt32

var approvedTLS12CipherSuites = map[uint16]struct{}{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:       {},
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:         {},
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:       {},
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:         {},
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: {},
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:   {},
}

// buildKgoOpts translates a validated streaming.Config plus the resolved
// emitterOptions into the franz-go option slice. Each option is pinned
// explicitly per TRD risk R1 — franz-go defaults have flipped between
// versions (ProducerLinger 0ms→10ms at v1.17→v1.20), and we refuse to
// silently absorb that kind of drift.
//
// cfg carries env-driven runtime knobs; opts carries caller-supplied wiring
// that must never come from environment variables (TLS certs, SASL secrets)
// per the TRD §8 security boundary. Either argument may be zero-valued.
//
// This function assumes cfg has already passed cfg.Validate(); invalid
// compression codec / acks values surface as ErrInvalidCompression /
// ErrInvalidAcks defensively but the happy path never hits them.
func buildKgoOpts(cfg Config, opts emitterOptions) ([]kgo.Opt, error) {
	codec, err := resolveCompression(cfg.Compression)
	if err != nil {
		return nil, err
	}

	acks, err := resolveAcks(cfg.RequiredAcks)
	if err != nil {
		return nil, err
	}

	// Bounds check before the int→int32 narrowing. Non-positive values
	// fall back to defaultBatchMaxBytes — franz-go validates a minimum
	// of ~512 bytes so passing 0 would either error or clamp unexpectedly.
	batchMaxBytes := cfg.BatchMaxBytes
	if batchMaxBytes <= 0 {
		batchMaxBytes = defaultBatchMaxBytes
	} else if batchMaxBytes > maxBatchMaxBytes {
		batchMaxBytes = maxBatchMaxBytes
	}

	kgoOpts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),

		// Batching. ProducerLinger default flipped between franz-go
		// versions — pinning it here avoids surprise latency changes.
		kgo.ProducerLinger(time.Duration(cfg.BatchLingerMs) * time.Millisecond),
		// #nosec G115 — bounds checked above; batchMaxBytes ≤ math.MaxInt32.
		kgo.ProducerBatchMaxBytes(int32(batchMaxBytes)),

		// Backpressure ceiling. When reached, ProduceSync blocks until a
		// record clears — the caller feels natural pushback instead of
		// unbounded memory growth.
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),

		// Compression preference. A single-codec slice is fine; franz-go
		// will try the first codec and fall back silently to NoCompression
		// if it's unsupported by a particular broker.
		kgo.ProducerBatchCompression(codec),

		// Retry budget. Per-record cap; once exhausted franz-go returns
		// kgo.ErrRecordRetries which the classifier routes to DLQ in T5.
		kgo.RecordRetries(cfg.RecordRetries),
		kgo.RecordDeliveryTimeout(cfg.RecordDeliveryTimeout),

		// Durability. "all" maps to AllISRAcks; "leader" to LeaderAck;
		// "none" to NoAck. Validated at cfg.Validate() time.
		kgo.RequiredAcks(acks),

		// Per-record topic is set on each kgo.Record, so the default
		// topic is a no-op — but we set it to an empty string explicitly
		// so missing cfg shows up as a ProduceSync error rather than
		// accidentally publishing to a default topic.
		kgo.DefaultProduceTopic(""),

		// Partitioning: StickyKeyPartitioner with nil hasher picks up the
		// default Kafka murmur2 hashing — we do NOT pass KafkaHasher(nil)
		// because that builds a PartitionerHasher over a nil hashFn which
		// panics at produce time. Events with the same partition key land
		// on the same partition, which is what per-tenant FIFO requires.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}

	// franz-go's idempotent producer requires acks=all. If the operator
	// opted into acks=leader/none, they've consciously traded idempotency
	// for lower latency — we must disable idempotent writes so the kgo
	// client starts up instead of failing validation.
	if cfg.RequiredAcks != "all" {
		kgoOpts = append(kgoOpts, kgo.DisableIdempotentWrite())
	}

	// ClientID is optional — if the operator didn't set it, let franz-go
	// pick a reasonable default. Empty ClientID is valid; we only add the
	// option when the operator has something specific to say.
	if cfg.ClientID != "" {
		kgoOpts = append(kgoOpts, kgo.ClientID(cfg.ClientID))
	}

	if err := validateTLSConfig(opts.tlsConfig); err != nil {
		return nil, err
	}

	tlsConfig := cloneTLSConfigWithDefaults(opts.tlsConfig)

	saslMechanism := opts.saslMechanism
	if isNilInterface(saslMechanism) {
		saslMechanism = nil
	}

	if saslMechanism != nil && tlsConfig == nil && !opts.allowPlaintextSASL {
		return nil, fmt.Errorf("%w: pair WithSASL with WithTLSConfig, or explicitly opt into unsafe local/dev plaintext via WithAllowPlaintextSASL", ErrPlaintextSASLNotAllowed)
	}

	// TLS configuration. franz-go's DialTLSConfig clones the config per-dial and
	// auto-fills ServerName from the broker host; callers rarely need to set it.
	// We still pass our own validated clone so caller mutations after option
	// application cannot weaken transport security before the first dial.
	if tlsConfig != nil {
		kgoOpts = append(kgoOpts, kgo.DialTLSConfig(tlsConfig))
	}

	// SASL mechanism (T8). kgo.SASL is variadic; we always pass exactly one
	// mechanism in v1. Multi-mechanism fallback (negotiate the first
	// broker-supported one) is out of scope until a real auth flow ships.
	if saslMechanism != nil {
		kgoOpts = append(kgoOpts, kgo.SASL(saslMechanism))
	}

	return kgoOpts, nil
}

// cloneTLSConfigWithDefaults returns a defensive deep copy of cfg with
// MinVersion defaulted to TLS 1.2 when unset.
//
// tls.Config.Clone is documented as a shallow copy: the caller can still
// mutate CipherSuites, CurvePreferences, NextProtos, or RootCAs after we
// have stored the "clone" and weaken the producer's broker dial policy
// retroactively. We re-allocate the security-critical mutable slices here
// so that, post-construction, no caller-reachable handle aliases the
// stored config's policy fields. (Cert chains and *x509.CertPool are
// internally pooled by crypto/tls; we leave those as the shallow Clone
// returned them — replacing them would break legitimate callers that
// rotate certs through a wrapper.)
func cloneTLSConfigWithDefaults(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return nil
	}

	cloned := cfg.Clone()
	if cloned.MinVersion == 0 {
		cloned.MinVersion = tls.VersionTLS12
	}

	cloned.CipherSuites = cloneUint16Slice(cloned.CipherSuites)
	cloned.CurvePreferences = cloneCurveIDSlice(cloned.CurvePreferences)
	cloned.NextProtos = cloneStringSlice(cloned.NextProtos)

	return cloned
}

func cloneUint16Slice(src []uint16) []uint16 {
	if src == nil {
		return nil
	}

	dst := make([]uint16, len(src))
	copy(dst, src)

	return dst
}

func cloneCurveIDSlice(src []tls.CurveID) []tls.CurveID {
	if src == nil {
		return nil
	}

	dst := make([]tls.CurveID, len(src))
	copy(dst, src)

	return dst
}

func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}

	dst := make([]string, len(src))
	copy(dst, src)

	return dst
}

func validateTLSConfig(cfg *tls.Config) error {
	if cfg == nil {
		return nil
	}

	if cfg.InsecureSkipVerify {
		return fmt.Errorf("%w: InsecureSkipVerify is forbidden", ErrInvalidTLSConfig)
	}

	if cfg.MinVersion != 0 && cfg.MinVersion < tls.VersionTLS12 {
		return fmt.Errorf("%w: MinVersion must be TLS 1.2 or newer", ErrInvalidTLSConfig)
	}

	if cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS12 {
		return fmt.Errorf("%w: MaxVersion must be TLS 1.2 or newer", ErrInvalidTLSConfig)
	}

	// Reject contradictory version ranges. Apply the same TLS 1.2 default
	// for MinVersion that cloneTLSConfigWithDefaults uses, otherwise a
	// caller passing only MaxVersion=TLS1.0/1.1 would already have been
	// rejected above and a caller passing MaxVersion=TLS1.2/1.3 with
	// MinVersion=0 must remain valid (effective range 1.2..MaxVersion).
	effectiveMin := cfg.MinVersion
	if effectiveMin == 0 {
		effectiveMin = tls.VersionTLS12
	}

	if cfg.MaxVersion != 0 && effectiveMin > cfg.MaxVersion {
		return fmt.Errorf("%w: MinVersion (0x%04x) must not exceed MaxVersion (0x%04x)", ErrInvalidTLSConfig, effectiveMin, cfg.MaxVersion)
	}

	for _, suite := range cfg.CipherSuites {
		if _, ok := approvedTLS12CipherSuites[suite]; !ok {
			return fmt.Errorf("%w: unsupported TLS 1.2 CipherSuite 0x%04x", ErrInvalidTLSConfig, suite)
		}
	}

	return nil
}

// resolveCompression maps the Config string to a kgo.CompressionCodec. The
// match is exact (lowercase); cfg.Validate() normalizes.
func resolveCompression(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("%w: %q", ErrInvalidCompression, name)
	}
}

// resolveAcks maps the Config string to a kgo.Acks value. cfg.Validate()
// already rejects anything outside the closed set; the default branch is
// defensive.
func resolveAcks(name string) (kgo.Acks, error) {
	switch name {
	case "all":
		return kgo.AllISRAcks(), nil
	case "leader":
		return kgo.LeaderAck(), nil
	case "none":
		return kgo.NoAck(), nil
	default:
		return kgo.Acks{}, fmt.Errorf("%w: %q", ErrInvalidAcks, name)
	}
}
