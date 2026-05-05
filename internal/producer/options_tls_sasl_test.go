//go:build unit

package producer

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Testing strategy (T8). Each kgo.Opt is a closure over private franz-go
// state — we cannot inspect what SeedBrokers / DialTLSConfig / SASL *did*
// without importing internal/cfg. Two surrogate assertions are sufficient:
//
//  1. Store-level: the option setter records the value on emitterOptions.
//  2. Count-level: buildKgoOpts emits exactly one extra kgo.Opt for each of
//     {TLS, SASL} — matching the pattern already established by
//     TestBuildKgoOpts_AcksAllKeepsIdempotent and
//     TestBuildKgoOpts_ClientIDOptional (both count-based).
//
// A real TLS handshake / SCRAM round-trip is out of scope per the T8 scope
// note — documented on WithTLSConfig / WithSASL godoc.

// validT8Config returns a cfg.Validate()-passing Config for buildKgoOpts
// tests. Kept local to this file — producer_kgo_test.go builds its own
// inline Configs; centralizing would ripple beyond T8.
func validT8Config() Config {
	return Config{
		Brokers:               []string{"broker:9092"},
		BatchLingerMs:         5,
		BatchMaxBytes:         1_048_576,
		MaxBufferedRecords:    1000,
		Compression:           "none",
		RecordRetries:         3,
		RecordDeliveryTimeout: 10 * time.Second,
		RequiredAcks:          "all",
	}
}

// TestWithTLSConfig_StoresClonedOption proves the setter clones the caller's
// *tls.Config before storing it. The Producer must not retain caller-owned
// mutable security material: later mutations by the caller must not weaken the
// broker dial policy used by the producer.
func TestWithTLSConfig_StoresOption(t *testing.T) {
	t.Parallel()

	opts := &emitterOptions{}
	tlsCfg := &tls.Config{
		ServerName: "tls-plumbing-test",
	}
	WithTLSConfig(tlsCfg)(opts)

	if opts.tlsConfig == nil {
		t.Fatal("WithTLSConfig did not set opts.tlsConfig")
	}
	if opts.tlsConfig == tlsCfg {
		t.Error("WithTLSConfig stored caller-owned pointer; want cloned config")
	}
	if opts.tlsConfig.ServerName != "tls-plumbing-test" {
		t.Errorf("ServerName = %q; want tls-plumbing-test", opts.tlsConfig.ServerName)
	}
	if opts.tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x; want TLS 1.2 default", opts.tlsConfig.MinVersion)
	}

	tlsCfg.ServerName = "mutated-after-option"
	tlsCfg.MinVersion = tls.VersionTLS10
	if opts.tlsConfig.ServerName != "tls-plumbing-test" {
		t.Errorf("stored ServerName mutated to %q; want tls-plumbing-test", opts.tlsConfig.ServerName)
	}
	if opts.tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("stored MinVersion mutated to %x; want TLS 1.2", opts.tlsConfig.MinVersion)
	}
}

// TestWithTLSConfig_NilPassesThrough proves nil is a valid input and leaves
// opts.tlsConfig at the zero value. Callers who construct Producer without
// TLS rely on this — otherwise every New() call would need a guard.
func TestWithTLSConfig_NilPassesThrough(t *testing.T) {
	t.Parallel()

	opts := &emitterOptions{}
	WithTLSConfig(nil)(opts)

	if opts.tlsConfig != nil {
		t.Errorf("WithTLSConfig(nil) set opts.tlsConfig = %v; want nil", opts.tlsConfig)
	}
}

// TestWithSASL_StoresOption proves the setter plumbs the sasl.Mechanism
// onto emitterOptions. We use plain.Auth.AsMechanism() because PLAIN is the
// simplest mechanism franz-go ships and has no side effects at construction
// (no network I/O, no timer goroutines). The mechanism's Name() returning
// "PLAIN" is a public invariant of the franz-go sasl/plain package.
func TestWithSASL_StoresOption(t *testing.T) {
	t.Parallel()

	opts := &emitterOptions{}
	mech := plain.Auth{User: "u", Pass: "p"}.AsMechanism()
	WithSASL(mech)(opts)

	if opts.saslMechanism == nil {
		t.Fatal("WithSASL did not set opts.saslMechanism")
	}
	if got := opts.saslMechanism.Name(); got != "PLAIN" {
		t.Errorf("mechanism Name = %q; want PLAIN", got)
	}
}

// TestWithSASL_NilPassesThrough mirrors the TLS nil-passthrough test.
// Plaintext, unauthenticated is the default and nil must preserve it.
func TestWithSASL_NilPassesThrough(t *testing.T) {
	t.Parallel()

	opts := &emitterOptions{}
	WithSASL(nil)(opts)

	if opts.saslMechanism != nil {
		t.Errorf("WithSASL(nil) set opts.saslMechanism = %v; want nil", opts.saslMechanism)
	}
}

func TestWithSASL_TypedNilPassesThrough(t *testing.T) {
	t.Parallel()

	var mech *typedNilSASLMechanism
	opts := &emitterOptions{}
	WithSASL(mech)(opts)

	if opts.saslMechanism != nil {
		t.Errorf("WithSASL(typed nil) set opts.saslMechanism = %v; want nil", opts.saslMechanism)
	}
}

func TestBuildKgoOpts_WithSASLWithoutTLS_DefaultRejectsPlaintext(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		saslMechanism: plain.Auth{User: "u", Pass: "p"}.AsMechanism(),
	})
	if !errors.Is(err, ErrPlaintextSASLNotAllowed) {
		t.Fatalf("buildKgoOpts() error = %v; want ErrPlaintextSASLNotAllowed", err)
	}
}

func TestBuildKgoOpts_WithTypedNilSASL_TreatsAsNoSASL(t *testing.T) {
	t.Parallel()

	cfg := validT8Config()
	baseline, err := buildKgoOpts(cfg, emitterOptions{})
	if err != nil {
		t.Fatalf("buildKgoOpts(baseline) error = %v", err)
	}

	var mech *typedNilSASLMechanism
	got, err := buildKgoOpts(cfg, emitterOptions{saslMechanism: mech})
	if err != nil {
		t.Fatalf("buildKgoOpts(typed nil SASL) error = %v; want nil", err)
	}
	if len(got) != len(baseline) {
		t.Errorf("typed nil SASL opts = %d; baseline = %d; want equal", len(got), len(baseline))
	}
}

func TestBuildKgoOpts_WithSASLAndTLS_Succeeds(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		tlsConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		saslMechanism: plain.Auth{User: "u", Pass: "p"}.AsMechanism(),
	})
	if err != nil {
		t.Fatalf("buildKgoOpts() error = %v; want nil", err)
	}
}

func TestBuildKgoOpts_WithSASLAndUnsafePlaintextOptIn_Succeeds(t *testing.T) {
	t.Parallel()

	opts := &emitterOptions{}
	WithSASL(plain.Auth{User: "u", Pass: "p"}.AsMechanism())(opts)
	WithAllowPlaintextSASL()(opts)

	_, err := buildKgoOpts(validT8Config(), *opts)
	if err != nil {
		t.Fatalf("buildKgoOpts() error = %v; want nil", err)
	}
}

func TestBuildKgoOpts_WithInsecureSkipVerify_RejectsTLSConfig(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		tlsConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test verifies rejection
	})
	if !errors.Is(err, ErrInvalidTLSConfig) {
		t.Fatalf("buildKgoOpts() error = %v; want ErrInvalidTLSConfig", err)
	}
}

func TestBuildKgoOpts_WithTLS10OrTLS11_RejectsTLSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		minVersion uint16
	}{
		{name: "TLS 1.0", minVersion: tls.VersionTLS10},
		{name: "TLS 1.1", minVersion: tls.VersionTLS11},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := buildKgoOpts(validT8Config(), emitterOptions{
				tlsConfig: &tls.Config{MinVersion: tt.minVersion},
			})
			if !errors.Is(err, ErrInvalidTLSConfig) {
				t.Fatalf("buildKgoOpts() error = %v; want ErrInvalidTLSConfig", err)
			}
		})
	}
}

func TestBuildKgoOpts_WithTLS11MaxVersion_RejectsTLSConfig(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		tlsConfig: &tls.Config{MaxVersion: tls.VersionTLS11},
	})
	if !errors.Is(err, ErrInvalidTLSConfig) {
		t.Fatalf("buildKgoOpts() error = %v; want ErrInvalidTLSConfig", err)
	}
}

func TestBuildKgoOpts_WithWeakTLS12CipherSuite_RejectsTLSConfig(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		tlsConfig: &tls.Config{CipherSuites: []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA}},
	})
	if !errors.Is(err, ErrInvalidTLSConfig) {
		t.Fatalf("buildKgoOpts() error = %v; want ErrInvalidTLSConfig", err)
	}
}

// TestBuildKgoOpts_WithTLSConfig_AppendsOneOpt asserts that a non-nil
// tlsConfig adds exactly one kgo.Opt to the returned slice vs. the
// baseline. We can't inspect the opt itself (franz-go closures are
// opaque), but the count delta is deterministic: kgo.DialTLSConfig emits
// one Opt.
//
// This is the same assertion pattern used for DisableIdempotentWrite and
// ClientID in producer_kgo_test.go.
func TestBuildKgoOpts_WithTLSConfig_AppendsOneOpt(t *testing.T) {
	t.Parallel()

	cfg := validT8Config()

	baseline, err := buildKgoOpts(cfg, emitterOptions{})
	if err != nil {
		t.Fatalf("buildKgoOpts(baseline) err = %v", err)
	}

	withTLS, err := buildKgoOpts(cfg, emitterOptions{
		tlsConfig: &tls.Config{MinVersion: tls.VersionTLS13},
	})
	if err != nil {
		t.Fatalf("buildKgoOpts(withTLS) err = %v", err)
	}

	if got, want := len(withTLS), len(baseline)+1; got != want {
		t.Errorf("withTLS opts = %d; baseline = %d; want delta = 1",
			len(withTLS), len(baseline))
	}
}

// TestBuildKgoOpts_WithSASL_AppendsOneOpt mirrors the TLS assertion for
// the SASL path. kgo.SASL is variadic but we always pass exactly one
// mechanism in v1, so the delta is 1.
func TestBuildKgoOpts_WithSASL_AppendsOneOpt(t *testing.T) {
	t.Parallel()

	cfg := validT8Config()

	baseline, err := buildKgoOpts(cfg, emitterOptions{})
	if err != nil {
		t.Fatalf("buildKgoOpts(baseline) err = %v", err)
	}

	withSASL, err := buildKgoOpts(cfg, emitterOptions{
		saslMechanism:      plain.Auth{User: "u", Pass: "p"}.AsMechanism(),
		allowPlaintextSASL: true,
	})
	if err != nil {
		t.Fatalf("buildKgoOpts(withSASL) err = %v", err)
	}

	if got, want := len(withSASL), len(baseline)+1; got != want {
		t.Errorf("withSASL opts = %d; baseline = %d; want delta = 1",
			len(withSASL), len(baseline))
	}
}

// TestBuildKgoOpts_WithTLSAndSASL_AppendsTwoOpts proves the two options
// compose: using both at once adds exactly two kgo.Opt entries. Guards
// against a regression where one option accidentally short-circuits the
// other (e.g. `if tls != nil && sasl == nil` instead of two independent
// if-blocks).
func TestBuildKgoOpts_WithTLSAndSASL_AppendsTwoOpts(t *testing.T) {
	t.Parallel()

	cfg := validT8Config()

	baseline, err := buildKgoOpts(cfg, emitterOptions{})
	if err != nil {
		t.Fatalf("buildKgoOpts(baseline) err = %v", err)
	}

	withBoth, err := buildKgoOpts(cfg, emitterOptions{
		tlsConfig:     &tls.Config{MinVersion: tls.VersionTLS13},
		saslMechanism: plain.Auth{User: "u", Pass: "p"}.AsMechanism(),
	})
	if err != nil {
		t.Fatalf("buildKgoOpts(withBoth) err = %v", err)
	}

	if got, want := len(withBoth), len(baseline)+2; got != want {
		t.Errorf("withBoth opts = %d; baseline = %d; want delta = 2",
			len(withBoth), len(baseline))
	}
}

// TestBuildKgoOpts_NilTLSAndSASL_NoDelta defends against a regression
// where a nil-check was dropped and a bare kgo.DialTLSConfig(nil) / kgo.SASL()
// would be appended. The guarantee: with zero-valued emitterOptions, the
// count matches the old single-arg buildKgoOpts behavior.
func TestBuildKgoOpts_NilTLSAndSASL_NoDelta(t *testing.T) {
	t.Parallel()

	cfg := validT8Config()

	baseline, err := buildKgoOpts(cfg, emitterOptions{})
	if err != nil {
		t.Fatalf("buildKgoOpts(baseline) err = %v", err)
	}

	// Explicit nils — identical contract to zero-value emitterOptions but
	// sharper intent when reading the test log.
	explicitNils, err := buildKgoOpts(cfg, emitterOptions{
		tlsConfig:     nil,
		saslMechanism: nil,
	})
	if err != nil {
		t.Fatalf("buildKgoOpts(explicitNils) err = %v", err)
	}

	if len(explicitNils) != len(baseline) {
		t.Errorf("explicit-nils opts = %d; baseline = %d; want equal",
			len(explicitNils), len(baseline))
	}
}

type typedNilSASLMechanism struct{}

func (*typedNilSASLMechanism) Name() string { return "TYPED-NIL" }

func (*typedNilSASLMechanism) Authenticate(context.Context, string) (sasl.Session, []byte, error) {
	return nil, nil, nil
}

// TestWithTLSConfig_DeepClonesCipherSuites proves that mutating the caller's
// cfg.CipherSuites slice header AFTER WithTLSConfig has stored a clone does
// NOT change the producer-side TLS policy.
//
// tls.Config.Clone() is documented as a shallow copy. Without an explicit
// slice copy, the caller could swap an approved AES-GCM suite for a weak
// CBC suite at any point after Producer construction and before the first
// dial — silently weakening transport security. cloneTLSConfigWithDefaults
// must therefore deep-copy CipherSuites.
func TestWithTLSConfig_DeepClonesCipherSuites(t *testing.T) {
	t.Parallel()

	approved := tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
	weak := tls.TLS_RSA_WITH_AES_128_CBC_SHA

	caller := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{approved},
	}

	opts := &emitterOptions{}
	WithTLSConfig(caller)(opts)

	if opts.tlsConfig == nil {
		t.Fatal("WithTLSConfig did not store config")
	}
	if len(opts.tlsConfig.CipherSuites) != 1 || opts.tlsConfig.CipherSuites[0] != approved {
		t.Fatalf("stored CipherSuites = %#x; want [%#x]", opts.tlsConfig.CipherSuites, approved)
	}

	// Confirm the slice header itself was deep-copied: the underlying
	// arrays must not alias.
	if &caller.CipherSuites[0] == &opts.tlsConfig.CipherSuites[0] {
		t.Fatal("CipherSuites slice aliased between caller and stored config")
	}

	// Caller mutates the slice in place. With a shallow Clone this would
	// flip the producer-side policy from an approved AEAD suite to a weak
	// CBC suite without re-running validation.
	caller.CipherSuites[0] = weak

	if opts.tlsConfig.CipherSuites[0] != approved {
		t.Errorf("after caller mutation, stored CipherSuites[0] = %#x; want %#x", opts.tlsConfig.CipherSuites[0], approved)
	}
}

// TestWithTLSConfig_DeepClonesCurvePreferences mirrors the CipherSuites
// deep-copy test for CurvePreferences. Same rationale: tls.Config.Clone is
// shallow, and CurvePreferences influences key-agreement security.
func TestWithTLSConfig_DeepClonesCurvePreferences(t *testing.T) {
	t.Parallel()

	caller := &tls.Config{
		MinVersion:       tls.VersionTLS12,
		CurvePreferences: []tls.CurveID{tls.X25519, tls.CurveP256},
	}

	opts := &emitterOptions{}
	WithTLSConfig(caller)(opts)

	if &caller.CurvePreferences[0] == &opts.tlsConfig.CurvePreferences[0] {
		t.Fatal("CurvePreferences slice aliased between caller and stored config")
	}

	caller.CurvePreferences[0] = tls.CurveP384

	if opts.tlsConfig.CurvePreferences[0] != tls.X25519 {
		t.Errorf("after caller mutation, stored CurvePreferences[0] = %v; want X25519", opts.tlsConfig.CurvePreferences[0])
	}
}

// TestWithTLSConfig_DeepClonesNextProtos mirrors the CipherSuites deep-copy
// test for NextProtos (ALPN). NextProtos is less security-critical than
// CipherSuites but the same shallow-Clone bug applies.
func TestWithTLSConfig_DeepClonesNextProtos(t *testing.T) {
	t.Parallel()

	caller := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}

	opts := &emitterOptions{}
	WithTLSConfig(caller)(opts)

	if &caller.NextProtos[0] == &opts.tlsConfig.NextProtos[0] {
		t.Fatal("NextProtos slice aliased between caller and stored config")
	}

	caller.NextProtos[0] = "rogue-proto"

	if opts.tlsConfig.NextProtos[0] != "h2" {
		t.Errorf("after caller mutation, stored NextProtos[0] = %q; want h2", opts.tlsConfig.NextProtos[0])
	}
}

// TestBuildKgoOpts_WithApprovedTLS12CipherSuite_AcceptsAndPreservesSuite is
// the positive coverage for the approved-cipher-suite allowlist. Two of the
// canonical approved AEAD suites must validate cleanly AND survive the deep
// clone unchanged. Without this, regressions that drop a suite from the
// allowlist would only fail negative tests — silent breakage of legitimate
// callers. We exercise both an RSA and an ECDSA suite so a future split of
// the allowlist by cert type is caught.
func TestBuildKgoOpts_WithApprovedTLS12CipherSuite_AcceptsAndPreservesSuite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		suite uint16
	}{
		{
			name:  "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			suite: tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
		{
			name:  "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
			suite: tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tlsCfg := &tls.Config{
				MinVersion:   tls.VersionTLS12,
				CipherSuites: []uint16{tt.suite},
			}

			// Validation path: buildKgoOpts must accept the approved suite.
			if _, err := buildKgoOpts(validT8Config(), emitterOptions{tlsConfig: tlsCfg}); err != nil {
				t.Fatalf("buildKgoOpts(approved suite) err = %v; want nil", err)
			}

			// Deep-clone preservation: the stored config must contain
			// exactly the suite the caller passed, unchanged.
			opts := &emitterOptions{}
			WithTLSConfig(tlsCfg)(opts)

			if opts.tlsConfig == nil {
				t.Fatal("WithTLSConfig did not store config")
			}
			if got := opts.tlsConfig.CipherSuites; len(got) != 1 || got[0] != tt.suite {
				t.Errorf("stored CipherSuites = %#x; want [%#x]", got, tt.suite)
			}
		})
	}
}

// TestBuildKgoOpts_TLSVersionRange_MinExceedsMax_RejectsTLSConfig asserts
// validateTLSConfig rejects MinVersion=TLS1.3 + MaxVersion=TLS1.2. Without
// this check, the producer would silently store a contradictory config that
// crypto/tls would later reject at handshake time with a less precise
// error. The check applies effectiveMinVersion (defaulted to TLS1.2 when
// MinVersion=0), so MaxVersion alone never trips it.
func TestBuildKgoOpts_TLSVersionRange_MinExceedsMax_RejectsTLSConfig(t *testing.T) {
	t.Parallel()

	_, err := buildKgoOpts(validT8Config(), emitterOptions{
		tlsConfig: &tls.Config{
			MinVersion: tls.VersionTLS13,
			MaxVersion: tls.VersionTLS12,
		},
	})
	if !errors.Is(err, ErrInvalidTLSConfig) {
		t.Fatalf("buildKgoOpts(min>max) error = %v; want ErrInvalidTLSConfig", err)
	}
}

// TestBuildKgoOpts_TLSVersionRange_ValidMaxVersions_Accepted is the positive
// counterpart: TLS1.2 and TLS1.3 MaxVersions, paired with the default
// MinVersion (effectively TLS1.2), must validate cleanly. Defends against
// an over-eager Min>Max check that fires on equal Min/Max.
func TestBuildKgoOpts_TLSVersionRange_ValidMaxVersions_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxVersion uint16
	}{
		{name: "MaxVersion=TLS 1.2", maxVersion: tls.VersionTLS12},
		{name: "MaxVersion=TLS 1.3", maxVersion: tls.VersionTLS13},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := buildKgoOpts(validT8Config(), emitterOptions{
				tlsConfig: &tls.Config{MaxVersion: tt.maxVersion},
			})
			if err != nil {
				t.Fatalf("buildKgoOpts(MaxVersion=%#x) err = %v; want nil", tt.maxVersion, err)
			}
		})
	}
}
