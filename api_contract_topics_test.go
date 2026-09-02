//go:build unit

package streaming_test

import (
	"errors"
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// TestAppTopic_DerivesTheAppTopicTriple pins the three names the whole v3
// contract rests on, as LITERAL strings.
//
// Provisioning creates these, Kafka ACLs grant exactly these three to a
// command-emitting app (two to a fact-only one), and the producer publishes to
// them. Deriving the expectation from the same constants the production code
// uses would let a prefix change pass unnoticed on both sides at once.
func TestAppTopic_DerivesTheAppTopicTriple(t *testing.T) {
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

	commands, err := streaming.AppCommandsTopic("lender")
	if err != nil {
		t.Fatalf("AppCommandsTopic(lender) error = %v", err)
	}

	if commands != "lerian.streaming.lender.commands" {
		t.Errorf("AppCommandsTopic(lender) = %q; want lerian.streaming.lender.commands", commands)
	}
}

// TestAppCommandsTopic_RejectsMalformedSource pins that the commands name is
// held to the SAME strict source contract as the other two — a malformed
// source yields no topic, ever.
func TestAppCommandsTopic_RejectsMalformedSource(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		source string
		want   error
	}{
		{"", streaming.ErrMissingSource},
		{"Lender", streaming.ErrInvalidSource},
		{"lerian.midaz", streaming.ErrInvalidSource},
		{strings.Repeat("a", 224), streaming.ErrInvalidSource},
	} {
		topic, err := streaming.AppCommandsTopic(tc.source)
		if !errors.Is(err, tc.want) {
			t.Fatalf("AppCommandsTopic(%q) error = %v; want %v", tc.source, err, tc.want)
		}

		if topic != "" {
			t.Errorf("AppCommandsTopic(%q) returned %q alongside its error; want the empty string", tc.source, topic)
		}
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
		want   error
	}{
		{"empty", "", streaming.ErrMissingSource},
		{"v2 uri shape", "//lerian.midaz/tx", streaming.ErrInvalidSource},
		{"capitalized", "Lender", streaming.ErrInvalidSource},
		{"dotted namespace", "lerian.midaz", streaming.ErrInvalidSource},
		{"leading hyphen", "-lender", streaming.ErrInvalidSource},
		{"over the byte bound", strings.Repeat("a", 224), streaming.ErrInvalidSource},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// The sentinel, not merely "an error": a caller branches on
			// ErrMissingSource ("nothing configured") vs ErrInvalidSource
			// ("configured wrong") to say something useful at startup, and
			// asserting err != nil would let the two swap places unnoticed.
			topic, err := streaming.AppTopic(tc.source)
			if !errors.Is(err, tc.want) {
				t.Fatalf("AppTopic(%q) error = %v; want %v", tc.source, err, tc.want)
			}

			if topic != "" {
				t.Errorf("AppTopic(%q) returned %q alongside its error; want the empty string", tc.source, topic)
			}

			dlq, err := streaming.AppDLQTopic(tc.source)
			if !errors.Is(err, tc.want) {
				t.Fatalf("AppDLQTopic(%q) error = %v; want %v", tc.source, err, tc.want)
			}

			if dlq != "" {
				t.Errorf("AppDLQTopic(%q) returned %q alongside its error; want the empty string", tc.source, dlq)
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

	for _, source := range []string{"//x", "Lender", "lerian.midaz", "-lender", strings.Repeat("a", 224)} {
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

	if streaming.CommandsTopicSuffix != ".commands" {
		t.Errorf("CommandsTopicSuffix = %q; want .commands", streaming.CommandsTopicSuffix)
	}

	if streaming.MaxKafkaTopicNameBytes != 249 {
		t.Errorf("MaxKafkaTopicNameBytes = %d; want Kafka's protocol limit of 249", streaming.MaxKafkaTopicNameBytes)
	}
}
