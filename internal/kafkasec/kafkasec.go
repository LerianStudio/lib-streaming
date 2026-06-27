// Package kafkasec holds the Kafka TLS/SASL transport-security plumbing shared
// by the producer and the consumer. Centralizing it here ensures the two
// clients enforce one identical broker-dial security policy — there is no
// second copy to drift from on a CVE response or a hardening change.
//
// The package is deliberately config-struct-free: every helper takes
// primitives or *tls.Config so it can be imported by both internal/producer
// and internal/consumer without an import cycle (it depends only on
// internal/contract for the shared sentinel errors).
package kafkasec

import (
	"crypto/tls"
	"fmt"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// approvedTLS12CipherSuites is the AEAD/ECDHE allowlist enforced for
// caller-specified TLS 1.2 cipher suites. TLS 1.3 suites are not configurable
// in crypto/tls and need no allowlist.
var approvedTLS12CipherSuites = map[uint16]struct{}{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:       {},
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:         {},
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:       {},
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:         {},
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: {},
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256:   {},
}

// CloneTLSConfigWithDefaults clones cfg (struct + the security-critical mutable
// slices + the CA pools), defaults MinVersion to TLS 1.2 when unset, and returns
// the result. It does NOT deep-copy Certificates or NameToCertificate: those are
// the caller's cert chains (often rotated through a wrapper), so callers must not
// mutate them after passing cfg in.
//
// tls.Config.Clone is documented as a shallow copy: the caller can still mutate
// CipherSuites, CurvePreferences, NextProtos, RootCAs, or ClientCAs after we have
// stored the "clone" and weaken the broker dial policy retroactively. We
// re-allocate the policy slices and clone the CA pools (cheap (*x509.CertPool).
// Clone) here so that, post-construction, no caller-reachable handle aliases the
// stored config's policy fields.
func CloneTLSConfigWithDefaults(cfg *tls.Config) *tls.Config {
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

	// Clone the CA pools so a caller mutating its RootCAs/ClientCAs after passing
	// cfg cannot retroactively alter who the stored config trusts. Certificates /
	// NameToCertificate are deliberately left aliased (see docstring).
	if cloned.RootCAs != nil {
		cloned.RootCAs = cloned.RootCAs.Clone()
	}

	if cloned.ClientCAs != nil {
		cloned.ClientCAs = cloned.ClientCAs.Clone()
	}

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

// ValidateTLSConfig rejects a caller-supplied *tls.Config that would weaken
// broker transport security: InsecureSkipVerify, explicit TLS versions below
// 1.2, a contradictory version range, or a TLS 1.2 cipher suite outside the
// approved AEAD/ECDHE allowlist. nil is valid (plaintext transport).
func ValidateTLSConfig(cfg *tls.Config) error {
	if cfg == nil {
		return nil
	}

	if cfg.InsecureSkipVerify {
		return fmt.Errorf("%w: InsecureSkipVerify is forbidden", contract.ErrInvalidTLSConfig)
	}

	if cfg.MinVersion != 0 && cfg.MinVersion < tls.VersionTLS12 {
		return fmt.Errorf("%w: MinVersion must be TLS 1.2 or newer", contract.ErrInvalidTLSConfig)
	}

	if cfg.MaxVersion != 0 && cfg.MaxVersion < tls.VersionTLS12 {
		return fmt.Errorf("%w: MaxVersion must be TLS 1.2 or newer", contract.ErrInvalidTLSConfig)
	}

	// Reject contradictory version ranges. Apply the same TLS 1.2 default
	// for MinVersion that CloneTLSConfigWithDefaults uses, otherwise a
	// caller passing only MaxVersion=TLS1.0/1.1 would already have been
	// rejected above and a caller passing MaxVersion=TLS1.2/1.3 with
	// MinVersion=0 must remain valid (effective range 1.2..MaxVersion).
	effectiveMin := cfg.MinVersion
	if effectiveMin == 0 {
		effectiveMin = tls.VersionTLS12
	}

	if cfg.MaxVersion != 0 && effectiveMin > cfg.MaxVersion {
		return fmt.Errorf("%w: MinVersion (0x%04x) must not exceed MaxVersion (0x%04x)", contract.ErrInvalidTLSConfig, effectiveMin, cfg.MaxVersion)
	}

	for _, suite := range cfg.CipherSuites {
		if _, ok := approvedTLS12CipherSuites[suite]; !ok {
			return fmt.Errorf("%w: unsupported TLS 1.2 CipherSuite 0x%04x", contract.ErrInvalidTLSConfig, suite)
		}
	}

	return nil
}

// SASLRequiresTLS enforces the fail-closed default that SASL credentials must
// not cross the network in cleartext. It rejects the SASL-without-TLS case
// unless the caller has explicitly opted into unsafe local/dev plaintext.
//
// hasSASL/hasTLS are booleans rather than the concrete sasl.Mechanism /
// *tls.Config so the gate stays config-struct-free and the franz-go wiring
// (typed-nil normalization, option appending) remains at the call site.
func SASLRequiresTLS(hasSASL, hasTLS, allowPlaintext bool) error {
	if hasSASL && !hasTLS && !allowPlaintext {
		return fmt.Errorf("%w: pair WithSASL with WithTLSConfig, or explicitly opt into unsafe local/dev plaintext via WithAllowPlaintextSASL", contract.ErrPlaintextSASLNotAllowed)
	}

	return nil
}
