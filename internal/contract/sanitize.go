package contract

import (
	"net/url"
	"regexp"
	"strings"

	commonsSanitize "github.com/LerianStudio/lib-commons/v5/commons/security/sanitize"
)

// redactedMarker is the canonical lib-commons replacement string substituted
// for credentials. Keeping the marker sourced from lib-commons preserves a
// single operator-facing redaction contract across Lerian packages while this
// package retains its broker-URL-specific redaction logic.
const redactedMarker = commonsSanitize.SecretRedactionMarker

// urlPattern matches scheme://rest-of-URL sequences. Kept intentionally simple
// to mirror github.com/LerianStudio/lib-commons/v5/commons/rabbitmq/rabbitmq.go:129. Credential redaction is applied
// per-match via url.Parse and a fallback regex.
var urlPattern = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s]+`)

// kvCredentialPattern targets credential-shaped key/value pairs that may
// appear in config dumps or error messages outside of URLs. Case-insensitive;
// value stops at whitespace or common separators.
//
// The covered key set is the canonical Lerian secret-redaction list:
//   - password / passwd / pass — broker auth, DB connection strings
//   - secret / client_secret  — OAuth client secrets, app secrets
//   - token / bearer          — JWTs, OAuth bearer tokens
//   - apikey / api_key / api-key — API keys (multiple separator forms)
//   - auth / authorization    — generic auth headers
//
// New keys MAY be added without breaking the operator contract (the marker
// stays "****"). REMOVING a key requires a CHANGELOG entry — log streams
// downstream may depend on these patterns being redacted.
var kvCredentialPattern = regexp.MustCompile(
	`(?i)\b(password|passwd|pass|secret|client[_-]?secret|token|bearer|apikey|api[_-]?key|auth|authorization|aws[_-]?access[_-]?key[_-]?id|aws[_-]?secret[_-]?access[_-]?key|aws[_-]?session[_-]?token|x-amz-security-token|x-amz-credential|x-amz-signature|awsaccesskeyid|signature)=[^\s,;&]+`,
)

var authHeaderCredentialPattern = regexp.MustCompile(`(?i)\b(authorization|auth)([[:space:]]*:[[:space:]]*)([a-z][a-z0-9._~+/-]*)([[:space:]]+)[^\s,;&]+`)

var authKeyValueCredentialPattern = regexp.MustCompile(`(?i)\b(authorization|auth)([[:space:]]*=[[:space:]]*)[a-z][a-z0-9._~+/-]*[[:space:]]+[^\s,;&]+`)

var bareAWSAccessKeyIDPattern = regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`)

// sanitizeBrokerURL strips credentials from broker URLs and "password=" /
// "pass=" key-value pairs. Returns the sanitized message.
//
// Mirrors the regex-driven strategy of github.com/LerianStudio/lib-commons/v5/commons/rabbitmq/rabbitmq.go:129
// (URL pattern) and the redaction token from github.com/LerianStudio/lib-commons/v5/commons/rabbitmq/rabbitmq.go:124.
// Safe to call on empty strings and messages without credentials.
func sanitizeBrokerURL(s string) string {
	if s == "" {
		return ""
	}

	// Redact credentials inside URL-shaped substrings.
	s = urlPattern.ReplaceAllStringFunc(s, redactURLCandidate)

	// Redact Authorization/Auth header forms before generic key-value
	// redaction, because schemes such as Bearer, Basic, and Token separate the
	// scheme from the credential with a space. Header syntax keeps the scheme for
	// operator readability while redacting the credential value after it.
	s = authHeaderCredentialPattern.ReplaceAllStringFunc(s, func(match string) string {
		parts := authHeaderCredentialPattern.FindStringSubmatch(match)
		if len(parts) != 5 {
			return match
		}

		return parts[1] + parts[2] + parts[3] + parts[4] + redactedMarker
	})

	// Redact Authorization/Auth key-value forms with space-separated schemes.
	// Unlike header syntax, key-value config dumps commonly treat the whole
	// right-hand side as the secret value, so redact both scheme and credential.
	s = authKeyValueCredentialPattern.ReplaceAllStringFunc(s, func(match string) string {
		parts := authKeyValueCredentialPattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}

		return parts[1] + parts[2] + redactedMarker
	})

	// Redact bare "password=..." / "pass=..." KV pairs.
	s = kvCredentialPattern.ReplaceAllStringFunc(s, func(match string) string {
		eq := strings.Index(match, "=")
		if eq < 0 {
			return match
		}

		return match[:eq+1] + redactedMarker
	})

	// Redact bare AWS access key IDs even when they are not attached to a field
	// name (for example, broker/client libraries sometimes echo only the ID).
	s = bareAWSAccessKeyIDPattern.ReplaceAllString(s, redactedMarker)

	return s
}

func SanitizeBrokerURL(s string) string {
	return sanitizeBrokerURL(s)
}

// redactURLCandidate applies URL-aware redaction to a single URL-shaped token.
// Uses a manual authority split so the redacted marker is emitted verbatim
// (url.URL.String() percent-escapes the userinfo, which would turn "****"
// into "%2A%2A%2A%2A"). url.Parse is used only to detect whether the
// candidate contains a password; the replacement is string-level.
func redactURLCandidate(candidate string) string {
	// Trim trailing punctuation that is not part of the URL.
	trimmed, suffix := splitTrailingPunctuation(candidate)

	// A fast-path via url.Parse tells us whether userinfo is present.
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed == nil || parsed.User == nil {
		return fallbackRedact(trimmed) + suffix
	}

	// Userinfo present — delegate to the manual splitter so the marker is
	// emitted without URL-percent-encoding and username-only access keys are
	// redacted as aggressively as user:password credentials.
	return fallbackRedact(trimmed) + suffix
}

// splitTrailingPunctuation moves trailing sentence punctuation off the URL so
// it does not leak into the redacted form or confuse url.Parse.
func splitTrailingPunctuation(candidate string) (string, string) {
	end := len(candidate)

	for end > 0 {
		switch candidate[end-1] {
		case '.', ',', ';', ')', ']', '}', '"', '\'':
			end--
		default:
			return candidate[:end], candidate[end:]
		}
	}

	return "", candidate
}

// fallbackRedact scans URL authority userinfo and replaces the full userinfo
// token so neither passwords nor username-only access keys can leak.
// Used when url.Parse does not surface userinfo cleanly.
func fallbackRedact(token string) string {
	schemeSep := strings.Index(token, "://")
	if schemeSep == -1 {
		return token
	}

	rest := token[schemeSep+3:]
	authorityEnd := len(rest)

	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '/', '?', '#':
			authorityEnd = i
			i = len(rest) // break outer loop
		}
	}

	atIndex := strings.LastIndex(rest[:authorityEnd], "@")
	if atIndex == -1 {
		return token
	}

	tail := rest[atIndex:]

	userinfo := rest[:atIndex]
	if strings.Contains(userinfo, ":") {
		return token[:schemeSep+3] + redactedMarker + ":" + redactedMarker + tail
	}

	return token[:schemeSep+3] + redactedMarker + tail
}
