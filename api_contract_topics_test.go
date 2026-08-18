//go:build unit

package streaming_test

import (
	"errors"
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// TestAppTopic_DerivesTheAppTopicPair pins the two names the whole v3 contract
// rests on, as LITERAL strings.
//
// Provisioning creates these, Kafka ACLs grant exactly these two, and the
// producer publishes to them. Deriving the expectation from the same constants
// the production code uses would let a prefix change pass unnoticed on both
// sides at once.
func TestAppTopic_DerivesTheAppTopicPair(t *testing.T) {
	t.Parallel()

	topic, err := streaming.AppTopic("lender")
	if err != nil {
		t.Fatalf("AppTopic(lender) error = %v", err)
	}

	if topic != "lerian.streaming.lender" {
		t.Errorf("AppTopic(lender) = %q; want lerian.streaming.lender", topic)
	}

	dlq, err := streaming.AppDLQTopic("lender")
	if err != nil {
		t.Fatalf("AppDLQTopic(lender) error = %v", err)
	}

	if dlq != "lerian.streaming.lender.dlq" {
		t.Errorf("AppDLQTopic(lender) = %q; want lerian.streaming.lender.dlq", dlq)
	}
}

// TestAppTopic_RejectsMalformedSource pins that a bad source never yields a
// topic name. The unvalidated version returned "lerian.streaming." for an empty
// source — a real, creatable, publishable topic name built from nothing.
func TestAppTopic_RejectsMalformedSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		source string
	}{
		{"empty", ""},
		{"v2 uri shape", "//lerian.midaz/tx"},
		{"capitalized", "Lender"},
		{"dotted namespace", "lerian.midaz"},
		{"leading hyphen", "-lender"},
		{"over the byte bound", strings.Repeat("a", 229)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			topic, err := streaming.AppTopic(tc.source)
			if err == nil {
				t.Fatalf("AppTopic(%q) = %q, nil; want an error", tc.source, topic)
			}

			if topic != "" {
				t.Errorf("AppTopic(%q) returned %q alongside its error; want the empty string", tc.source, topic)
			}

			dlq, err := streaming.AppDLQTopic(tc.source)
			if err == nil {
				t.Fatalf("AppDLQTopic(%q) = %q, nil; want an error", tc.source, dlq)
			}
		})
	}
}

// TestValidateSource_RejectsMalformedAtTheRootFacade pins that the exported
// validator carries the ErrInvalidSource / ErrMissingSource vocabulary callers
// branch on.
func TestValidateSource_RejectsMalformedAtTheRootFacade(t *testing.T) {
	t.Parallel()

	if err := streaming.ValidateSource("lender"); err != nil {
		t.Errorf("ValidateSource(lender) = %v; want nil", err)
	}

	if err := streaming.ValidateSource(""); !errors.Is(err, streaming.ErrMissingSource) {
		t.Errorf("ValidateSource(\"\") = %v; want ErrMissingSource", err)
	}

	for _, source := range []string{"//x", "Lender", "lerian.midaz", "-lender", strings.Repeat("a", 229)} {
		if err := streaming.ValidateSource(source); !errors.Is(err, streaming.ErrInvalidSource) {
			t.Errorf("ValidateSource(%q) = %v; want ErrInvalidSource", source, err)
		}
	}
}

// TestTopicConstants_AreTheLiteralsProvisioningUses pins the exported constants
// as literals, so a prefix or suffix change cannot pass by agreeing with itself.
func TestTopicConstants_AreTheLiteralsProvisioningUses(t *testing.T) {
	t.Parallel()

	if streaming.TopicPrefix != "lerian.streaming." {
		t.Errorf("TopicPrefix = %q; want lerian.streaming.", streaming.TopicPrefix)
	}

	if streaming.DLQTopicSuffix != ".dlq" {
		t.Errorf("DLQTopicSuffix = %q; want .dlq", streaming.DLQTopicSuffix)
	}

	if streaming.MaxKafkaTopicNameBytes != 249 {
		t.Errorf("MaxKafkaTopicNameBytes = %d; want Kafka's protocol limit of 249", streaming.MaxKafkaTopicNameBytes)
	}
}
