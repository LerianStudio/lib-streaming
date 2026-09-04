package streaming

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
)

// CloudEvents binary-mode Kafka header codec — pure helpers exposed at the
// root facade so callers that need to roundtrip an Event through Kafka
// headers (e.g., an interop layer that consumes from non-lib-streaming
// producers) can do so without reaching into internal packages.
//
// The codec is the same one lib-streaming uses internally on the publish
// path; building or parsing here yields wire-identical bytes to what a
// real Producer would emit / parse.

// CloudEventsType composes the ce-type header value:
// "studio.lerian.<source>.<resourceType>.<eventType>".
//
// It is the consumer-facing half of the codec. A consumer that matches on
// ce-type rather than on the ce-resourcetype / ce-eventtype extension pair
// builds the string with this, instead of re-implementing the prefix and
// separator and drifting from the producer that writes it.
//
// The <source> segment is the v3 addition: without it two services publishing
// the same resource and event names produce byte-identical ce-type values, a
// homonym collision the topic collapse makes reachable in practice.
func CloudEventsType(source, resourceType, eventType string) string {
	return cloudevents.TypeOf(source, resourceType, eventType)
}

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
