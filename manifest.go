package streaming

import (
	"net/http"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/internal/manifest"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BuildManifest renders a catalog and descriptor into an exportable document.
func BuildManifest(descriptor PublisherDescriptor, catalog Catalog) (ManifestDocument, error) {
	return manifest.BuildManifest(descriptor, catalog)
}

// NewPublisherDescriptor validates and normalizes publisher metadata.
func NewPublisherDescriptor(descriptor PublisherDescriptor) (PublisherDescriptor, error) {
	return manifest.NewPublisherDescriptor(descriptor)
}

// NewStreamingHandler returns a stdlib HTTP handler that serves the manifest.
//
// SECURITY: the manifest exposes event taxonomy, schema versions, service
// metadata, and producer IDs. Callers must wrap this handler in their app's
// auth middleware before mounting it publicly.
func NewStreamingHandler(descriptor PublisherDescriptor, catalog Catalog) (http.Handler, error) {
	return manifest.NewStreamingHandler(descriptor, catalog)
}

// BuildCloudEventsHeaders assembles CloudEvents binary-mode Kafka headers for event.
func BuildCloudEventsHeaders(event Event) []kgo.RecordHeader {
	return cloudevents.BuildHeaders(event)
}

// ParseCloudEventsHeaders parses CloudEvents binary-mode Kafka headers into an Event.
func ParseCloudEventsHeaders(headers []kgo.RecordHeader) (Event, error) {
	return cloudevents.ParseCloudEventsHeaders(headers)
}
