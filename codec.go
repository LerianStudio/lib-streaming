package streaming

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
)

// CloudEvents binary-mode Kafka header codec — pure helpers exposed at the
// root facade so callers that need to roundtrip an Event through Kafka
// headers (e.g., an interop layer that consumes from non-lib-streaming
// producers) can do so without reaching into internal packages.
//
// The codec is the same one lib-streaming uses internally on the publish
// path; building or parsing here yields wire-identical bytes to what a
// real Producer would emit / parse.

// BuildCloudEventsHeaders assembles CloudEvents binary-mode Kafka headers
// for event. Returns 8-13 headers depending on which optional fields are
// populated. Required CloudEvents context attributes (ce-specversion, ce-id,
// ce-source, ce-type, ce-time) are always present.
func BuildCloudEventsHeaders(event Event) []kgo.RecordHeader {
	return cloudevents.BuildHeaders(event)
}

// ParseCloudEventsHeaders parses CloudEvents binary-mode Kafka headers into
// an Event. On parse failure returns the zero Event and a non-nil error
// (typically wrapping ErrMissingRequiredHeader or ErrUnsupportedSpecVersion).
//
// ParseCloudEventsHeaders accepts headers from any CloudEvents-compliant
// Kafka producer; ce-resourcetype and ce-eventtype are accepted as optional
// extensions so non-lib-streaming producers can still be parsed (they are
// populated from the ce-type breakdown when absent).
func ParseCloudEventsHeaders(headers []kgo.RecordHeader) (Event, error) {
	return cloudevents.ParseCloudEventsHeaders(headers)
}
