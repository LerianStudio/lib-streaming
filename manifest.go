package streaming

import (
	"net/http"

	"github.com/LerianStudio/lib-streaming/internal/manifest"
)

// BuildManifest renders a catalog, descriptor, and route table into an
// exportable document. The Routes field is populated when the supplied
// route table has at least one entry; passing an empty RouteTable
// produces a catalog-only document with the routes field omitted.
//
// Routes are deterministically ordered (definition key, then route key) so
// the JSON document is byte-stable across builds.
func BuildManifest(descriptor PublisherDescriptor, catalog Catalog, routes RouteTable) (ManifestDocument, error) {
	return manifest.BuildManifest(descriptor, catalog, routes)
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
