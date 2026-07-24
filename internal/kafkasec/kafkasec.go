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
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
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

// BuildTLSConfigFromCA constructs a fail-closed *tls.Config from an
// environment-supplied base64-encoded PEM CA certificate. It exists so
// consuming services can enable a private-CA TLS broker dial through the
// STREAMING_TLS_* environment variables without hand-rolling a *tls.Config.
//
// Behavior:
//   - enabled == false: returns (nil, nil) so the caller dials plaintext.
//   - caCertBase64 == "": returns a TLS 1.2+ config with a nil RootCAs, i.e.
//     the host's system trust pool. Use this for brokers served by a public /
//     already-trusted CA.
//   - caCertBase64 set: decodes the base64 PEM, adds it to a fresh
//     x509.CertPool, and pins that pool as RootCAs. A decode failure or a PEM
//     that yields no valid certificate returns an error wrapping
//     ErrInvalidTLSConfig so bootstrap fails closed.
//
// The returned config always floors MinVersion at TLS 1.2 and is validated
// through ValidateTLSConfig; InsecureSkipVerify is never set.
func BuildTLSConfigFromCA(enabled bool, caCertBase64 string) (*tls.Config, error) {
	if !enabled {
		return nil, nil //nolint:nilnil // nil,nil is the documented "TLS disabled" signal; callers switch on a nil *tls.Config, not an error
	}

	var pool *x509.CertPool

	if caCertBase64 != "" {
		pemBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(caCertBase64))
		if err != nil {
			return nil, fmt.Errorf("%w: CA certificate is not valid base64: %w", contract.ErrInvalidTLSConfig, err)
		}

		pool = x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("%w: no valid certificate in PEM CA bundle", contract.ErrInvalidTLSConfig)
		}
	}

	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}

	if err := ValidateTLSConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// BuildSASLMechanism constructs a franz-go sasl.Mechanism from the
// environment-supplied mechanism name plus credentials. It exists so consuming
// services can enable SASL through the STREAMING_SASL_* environment variables
// without importing the franz-go sasl sub-packages themselves.
//
// Behavior:
//   - mechanism == "": returns (nil, nil) so the caller configures no SASL.
//   - PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 (case-insensitive, surrounding
//     whitespace trimmed): returns the corresponding mechanism.
//   - anything else: returns an error wrapping ErrInvalidSASLMechanism.
//
// A recognized mechanism configured without both a username and a password is
// rejected with ErrInvalidSASLMechanism. The returned error never includes the
// password value.
func BuildSASLMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	normalized := strings.ToUpper(strings.TrimSpace(mechanism))
	if normalized == "" {
		return nil, nil //nolint:nilnil // nil,nil is the documented "no SASL configured" signal; callers append WithSASL only when the mechanism is non-nil, not on an error
	}

	switch normalized {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
		// Credentials are required for every supported mechanism. Validate
		// before constructing so a misconfigured deployment fails closed. Do
		// NOT echo the password into the error.
		if username == "" || password == "" {
			return nil, fmt.Errorf("%w: mechanism %q requires a username and password", contract.ErrInvalidSASLMechanism, normalized)
		}
	default:
		return nil, fmt.Errorf("%w: %q (want one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", contract.ErrInvalidSASLMechanism, mechanism)
	}

	switch normalized {
	case "PLAIN":
		return plain.Auth{User: username, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: username, Pass: password}.AsSha256Mechanism(), nil
	default: // "SCRAM-SHA-512"
		return scram.Auth{User: username, Pass: password}.AsSha512Mechanism(), nil
	}
}

// BuildSchemaRegistryClient constructs a franz-go schema-registry client from
// an environment-supplied URL plus optional basic-auth credentials. It exists
// so consuming services can reach a Schema Registry through the
// STREAMING_SCHEMA_REGISTRY_* environment variables without importing the
// franz-go pkg/sr package themselves. Constructing the client performs no
// network I/O — connectivity is exercised lazily on first use by the caller.
//
// This builder is the single authoritative credential gate for the Schema
// Registry: both the startup path (config.validateSchemaRegistry) and any
// direct/public constructor (streaming.NewSchemaRegistryClient) reach the
// registry through here, so the both-or-neither rule cannot be bypassed.
//
// Behavior:
//   - url is empty/whitespace-only: returns an error wrapping
//     ErrInvalidSchemaRegistryConfig. A registry client with no endpoint
//     cannot serialize, so this fails closed rather than returning a client
//     pinned to franz-go's http://localhost:8081 default.
//   - exactly one of username/password set: returns an error wrapping
//     ErrInvalidSchemaRegistryConfig. A partial credential silently produces
//     the wrong authorization, so it fails closed — mirroring
//     BuildSASLMechanism's both-or-neither rule.
//   - url set, username == "" && password == "": constructs a client with no
//     authorization.
//   - url set, username != "" && password != "": applies HTTP basic auth with
//     the supplied credentials.
//
// The returned error never includes the password value.
func BuildSchemaRegistryClient(url, username, password string) (*sr.Client, error) {
	if strings.TrimSpace(url) == "" {
		return nil, fmt.Errorf("%w: schema registry URL is required", contract.ErrInvalidSchemaRegistryConfig)
	}

	if (username == "") != (password == "") {
		return nil, fmt.Errorf("%w: schema registry username and password must be set together", contract.ErrInvalidSchemaRegistryConfig)
	}

	opts := []sr.ClientOpt{sr.URLs(url)}
	if username != "" {
		opts = append(opts, sr.BasicAuth(username, password))
	}

	client, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("build schema registry client: %w", err)
	}

	return client, nil
}
