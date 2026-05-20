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

// HandlerOption configures the manifest HTTP handler returned by
// NewStreamingHandler. Options follow the same functional-option pattern
// as the Builder's EmitterOption surface.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	routes RouteTable
}

// WithManifestRoutes attaches a route table to the manifest handler.
// Routes are serialized into the manifest's `routes` section in the
// deterministic order documented on BuildManifest. Passing the zero
// RouteTable (or omitting the option entirely) produces a catalog-only
// manifest, byte-identical to constructing the handler without the option.
//
// Routes are snapshotted at handler construction; runtime route mutations
// require rebuilding the handler. Route values are pre-validated by
// NewRouteTable, so this option cannot itself surface route-validation
// failures. Descriptor-shape failures (caught by BuildManifest) surface
// as the handler's error return — they are NOT deferred to runtime 500
// responses.
func WithManifestRoutes(routes RouteTable) HandlerOption {
	return func(c *handlerConfig) { c.routes = routes }
}

// NewStreamingHandler returns a stdlib HTTP handler that serves the manifest.
//
// SECURITY: the manifest exposes event taxonomy, schema versions, service
// metadata, and producer IDs. Callers MUST wrap this handler in their app's
// auth middleware before mounting it publicly. The library does not enforce
// authentication and does not validate caller identity at request time.
//
// When WithManifestRoutes is used, the manifest also advertises the active
// route table topology: target names, transport kinds, sanitized broker
// URLs, queue / exchange / event-bus identifiers, and DLQ destinations.
// Operators MUST treat the routes-bearing manifest as broker-topology
// disclosure when evaluating their auth chain.
//
// The handler pre-marshals the manifest payload at construction; subsequent
// requests serve cached bytes. If the catalog, descriptor, or route table
// changes, callers MUST rebuild the handler — runtime mutations are not
// reflected.
//
// Optional HandlerOption values configure additional manifest content. Use
// WithManifestRoutes(routes) to advertise the active route table in the
// manifest's `routes` section. Calling NewStreamingHandler with no options
// produces a catalog-only manifest, byte-identical to the prior two-argument
// form.
//
// Hardening shipped by the handler itself (operators can rely on these even
// when the manifest is mounted behind a permissive auth chain):
//   - Cache-Control: no-store
//   - Content-Type: application/json
//   - X-Content-Type-Options: nosniff
//   - X-Frame-Options: DENY
//   - HTTP method allowlist (GET, HEAD only)
func NewStreamingHandler(
	descriptor PublisherDescriptor,
	catalog Catalog,
	opts ...HandlerOption,
) (http.Handler, error) {
	cfg := handlerConfig{}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return manifest.NewStreamingHandler(descriptor, catalog, cfg.routes)
}
