//go:build unit

package contract

import (
	"net/url"
	"strings"
	"testing"
)

// FuzzSanitizeBrokerURL asserts that credential-bearing broker URLs are redacted.
func FuzzSanitizeBrokerURL(f *testing.F) {
	f.Add("failed to connect: sasl://user:hunter2@broker.example.com:9092/cluster")
	f.Add("dial failed kafka://admin:supersecret@kafka:9092")
	f.Add("client config password=hunter2 host=broker")
	f.Add("config pass=opensesame and other stuff")
	f.Add("ordinary error message with no credentials")
	f.Add("")
	f.Add("scheme://u1:p1@u2:hunter2@host:9092")
	f.Add("kafka://аdmin:hunter2@kafka:9092")
	f.Add("kafka://user%3Apass:hunter2@kafka:9092")
	f.Add("kafka://user:" + strings.Repeat("hunter2", 1000) + "@broker:9092")

	f.Fuzz(func(t *testing.T, in string) {
		out := sanitizeBrokerURL(in)

		const marker = "hunter2"
		for _, m := range urlPattern.FindAllString(in, -1) {
			trimmed, _ := splitTrailingPunctuation(m)

			parsed, err := url.Parse(trimmed)
			if err != nil || parsed == nil || parsed.User == nil {
				continue
			}

			pw, hasPW := parsed.User.Password()
			if !hasPW || pw != marker {
				continue
			}

			for _, outMatch := range urlPattern.FindAllString(out, -1) {
				if strings.Contains(outMatch, ":"+marker+"@") {
					t.Fatalf("credential leak: input=%q output=%q URL-match=%q parsed-password=%q",
						in, out, m, pw)
				}
			}
		}
	})
}

// FuzzParseMajorVersion verifies semver major parsing never returns a negative value.
func FuzzParseMajorVersion(f *testing.F) {
	f.Add("")
	f.Add("v")
	f.Add("1.0.0")
	f.Add("v2.0.0")
	f.Add("2.0.0-rc1")
	f.Add("2.0.0+gitsha")
	f.Add("9999999999")
	f.Add("not.a.version")
	f.Add("_1.0.0")
	f.Add("v0.0.0")
	f.Add("-1.0.0")
	f.Add("1")

	f.Fuzz(func(t *testing.T, in string) {
		if got := parseMajorVersion(in); got < 0 {
			t.Fatalf("parseMajorVersion(%q) = %d; invariant: result >= 0", in, got)
		}
	})
}
