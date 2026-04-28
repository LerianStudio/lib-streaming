//go:build unit

package contract

import (
	"strings"
	"testing"
)

// TestSanitizeBrokerURL verifies that credentials embedded in broker URLs,
// connection strings, and raw "password=" / "pass=" key/value pairs are
// stripped before any log output or error surfacing. Pattern mirrors the
// behavior of github.com/LerianStudio/lib-commons/v5/commons/rabbitmq/rabbitmq.go:129 and :470.
func TestSanitizeBrokerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		in             string
		mustNotContain []string
		mustContainAny []string // at least one of these substrings must be present
	}{
		{
			name:           "sasl url with userinfo password",
			in:             "failed to connect: sasl://user:hunter2@broker.example.com:9092/cluster",
			mustNotContain: []string{"hunter2", "user:hunter2"},
			mustContainAny: []string{redactedMarker},
		},
		{
			name:           "kafka url with userinfo password",
			in:             "dial failed kafka://admin:supersecret@kafka:9092",
			mustNotContain: []string{"supersecret", "admin:supersecret"},
			mustContainAny: []string{redactedMarker},
		},
		{
			name:           "password kv pair",
			in:             "client config password=hunter2 host=broker",
			mustNotContain: []string{"hunter2"},
			mustContainAny: []string{"password=" + redactedMarker},
		},
		{
			name:           "pass kv pair",
			in:             "config pass=opensesame and other stuff",
			mustNotContain: []string{"opensesame"},
			mustContainAny: []string{"pass=" + redactedMarker},
		},
		{
			name:           "no credentials to sanitize",
			in:             "ordinary error message with no credentials",
			mustNotContain: []string{},
			mustContainAny: []string{"ordinary error message with no credentials"},
		},
		{
			// Regression guard: an "@" inside a non-URL token (e.g. an email
			// address in a log message) must NOT be treated as userinfo
			// delimiter. fallbackRedact's URL-aware splitter only fires on
			// "://" presence, so an email-only message should pass through
			// untouched.
			name:           "email address in plain text is preserved",
			in:             "user message with email@domain.com",
			mustNotContain: []string{"****", "[REDACTED]"},
			mustContainAny: []string{"email@domain.com"},
		},
		{
			name:           "secret kv pair",
			in:             "config secret=topSecret123 reason=auth_failed",
			mustNotContain: []string{"topSecret123"},
			mustContainAny: []string{"secret=" + redactedMarker},
		},
		{
			name:           "client_secret kv pair",
			in:             "oauth handshake failed client_secret=oauth_xxxxxx scope=read",
			mustNotContain: []string{"oauth_xxxxxx"},
			mustContainAny: []string{"client_secret=" + redactedMarker},
		},
		{
			name:           "client-secret kv pair (hyphen variant)",
			in:             "auth client-secret=hyphensecret scope=read",
			mustNotContain: []string{"hyphensecret"},
			mustContainAny: []string{"client-secret=" + redactedMarker},
		},
		{
			name:           "token kv pair",
			in:             "auth handshake failed: token=eyJhbGciOiJIUzI1NiJ9.foo.bar reason=expired",
			mustNotContain: []string{"eyJhbGciOiJIUzI1NiJ9.foo.bar"},
			mustContainAny: []string{"token=" + redactedMarker},
		},
		{
			name:           "bearer kv pair",
			in:             "header bearer=eyJhbGciOiJIUzI1NiJ9 path=/v1",
			mustNotContain: []string{"eyJhbGciOiJIUzI1NiJ9"},
			mustContainAny: []string{"bearer=" + redactedMarker},
		},
		{
			name:           "apikey kv pair",
			in:             "config apikey=ak_live_abcdef host=api.example.com",
			mustNotContain: []string{"ak_live_abcdef"},
			mustContainAny: []string{"apikey=" + redactedMarker},
		},
		{
			name:           "api_key kv pair (underscore variant)",
			in:             "config api_key=ak_live_xyz123 host=api.example.com",
			mustNotContain: []string{"ak_live_xyz123"},
			mustContainAny: []string{"api_key=" + redactedMarker},
		},
		{
			name:           "api-key kv pair (hyphen variant)",
			in:             "config api-key=ak_live_hyphen host=api.example.com",
			mustNotContain: []string{"ak_live_hyphen"},
			mustContainAny: []string{"api-key=" + redactedMarker},
		},
		{
			name:           "auth kv pair",
			in:             "request auth=Basic_xyz status=401",
			mustNotContain: []string{"Basic_xyz"},
			mustContainAny: []string{"auth=" + redactedMarker},
		},
		{
			name:           "authorization kv pair",
			in:             "header authorization=Bearer_token123 status=401",
			mustNotContain: []string{"Bearer_token123"},
			mustContainAny: []string{"authorization=" + redactedMarker},
		},
		{
			name:           "case insensitive: PASSWORD",
			in:             "config PASSWORD=loudCase host=broker",
			mustNotContain: []string{"loudCase"},
			mustContainAny: []string{"PASSWORD=" + redactedMarker},
		},
		{
			name:           "case insensitive: Token",
			in:             "header Token=mixedCase status=200",
			mustNotContain: []string{"mixedCase"},
			mustContainAny: []string{"Token=" + redactedMarker},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := sanitizeBrokerURL(tt.in)

			for _, s := range tt.mustNotContain {
				if strings.Contains(out, s) {
					t.Errorf("sanitizeBrokerURL(%q) = %q; must NOT contain %q", tt.in, out, s)
				}
			}

			if len(tt.mustContainAny) > 0 {
				found := false
				for _, s := range tt.mustContainAny {
					if strings.Contains(out, s) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("sanitizeBrokerURL(%q) = %q; expected at least one of %v", tt.in, out, tt.mustContainAny)
				}
			}
		})
	}
}

// TestSanitizeBrokerURL_EmptyInput asserts the empty-string fast-path
// explicitly. Table-driven mustContainAny:[]string{""} is vacuously true
// (strings.Contains returns true for the empty substring), so this case
// needs a direct equality check to pin the contract.
func TestSanitizeBrokerURL_EmptyInput(t *testing.T) {
	t.Parallel()

	out := sanitizeBrokerURL("")
	if out != "" {
		t.Errorf("sanitizeBrokerURL(%q) = %q; want %q", "", out, "")
	}
}
