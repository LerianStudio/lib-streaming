package streaming

import (
	"net/http"

	"github.com/LerianStudio/lib-streaming/internal/manifest"
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
// metadata, and producer IDs. Callers MUST wrap this handler in their app's
// auth middleware before mounting it publicly. The library does not enforce
// authentication and does not validate caller identity at request time.
//
// The handler pre-marshals the manifest payload at construction; subsequent
// requests serve cached bytes. If the catalog or descriptor changes, callers
// MUST rebuild the handler — runtime mutations are not reflected.
//
// Hardening shipped by the handler itself (operators can rely on these even
// when the manifest is mounted behind a permissive auth chain):
//   - Cache-Control: no-store
//   - Content-Type: application/json
//   - X-Content-Type-Options: nosniff
//   - X-Frame-Options: DENY
//   - HTTP method allowlist (GET, HEAD only)
func NewStreamingHandler(descriptor PublisherDescriptor, catalog Catalog) (http.Handler, error) {
	return manifest.NewStreamingHandler(descriptor, catalog)
}
