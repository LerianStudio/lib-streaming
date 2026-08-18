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

// FuzzValidateSource verifies the v3 strict-source invariant: whatever
// ValidateSource ACCEPTS must derive a topic and DLQ topic that Kafka can
// actually hold, and must round-trip into the topic verbatim (no rewriting).
// v2's FuzzParseMajorVersion is gone with the exported major-version parser —
// the schema version no longer influences any topic.
func FuzzValidateSource(f *testing.F) {
	f.Add("")
	f.Add("lender")
	f.Add("midaz-ledger")
	f.Add("br_consignado_gw")
	f.Add("Lender")
	f.Add("lerian.midaz")
	f.Add("midaz-transaction-service")
	f.Add("---")
	f.Add("a b")
	f.Add("caf\u00e9")
	f.Add(strings.Repeat("a", 300))

	f.Fuzz(func(t *testing.T, in string) {
		if err := ValidateSource(in); err != nil {
			return
		}

		topic := AppTopic(in)
		if len(AppDLQTopic(in)) > MaxKafkaTopicNameBytes {
			t.Fatalf("ValidateSource(%q) accepted a source whose DLQ topic is %d bytes (max %d)",
				in, len(AppDLQTopic(in)), MaxKafkaTopicNameBytes)
		}

		if !strings.HasPrefix(topic, TopicPrefix) || strings.TrimPrefix(topic, TopicPrefix) != in {
			t.Fatalf("AppTopic(%q) = %q; accepted source must appear verbatim after %q", in, topic, TopicPrefix)
		}

		if HasControlChar(topic) {
			t.Fatalf("AppTopic(%q) = %q contains a control char", in, topic)
		}
	})
}
