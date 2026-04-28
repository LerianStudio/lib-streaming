package manifest

import (
	"encoding/json"
	"net/http"
)

// NewStreamingHandler returns an optional stdlib HTTP handler for an
// app-mounted /streaming route. The library does not mount routes, enforce
// auth, start servers, or adapt framework-specific contexts.
//
// SECURITY: This handler returns internal event-taxonomy metadata (event
// types, schema versions, service name/version, producer ID). Callers MUST
// wrap it in their app's auth middleware before mounting publicly. The
// library does not enforce auth.
//
// SNAPSHOT SEMANTICS: NewStreamingHandler serializes the manifest payload
// ONCE at construction. Subsequent requests serve the cached bytes. If the
// catalog or descriptor changes after construction, callers must rebuild
// the handler; this handler will not reflect runtime mutations.
func NewStreamingHandler(descriptor PublisherDescriptor, catalog Catalog) (http.Handler, error) {
	manifest, err := BuildManifest(descriptor, catalog)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)

			return
		}

		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)

			return
		}

		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.WriteHeader(http.StatusOK)

		if r.Method == http.MethodHead {
			return
		}

		_, _ = w.Write(payload)
	}), nil
}
