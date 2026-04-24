//go:build unit

package streaming

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStreamingHandler_ReturnsManifestJSON(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	handler, err := NewStreamingHandler(PublisherDescriptor{
		ServiceName: "transaction-service",
		SourceBase:  "//lerian.midaz/transaction-service",
	}, catalog)
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/streaming", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q; want application/json", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q; want nosniff", got)
	}
	if got := recorder.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options = %q; want DENY", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q; want no-store", got)
	}

	var manifest ManifestDocument
	if err := json.NewDecoder(recorder.Body).Decode(&manifest); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(manifest.Events) != 1 || manifest.Events[0].Key != "transaction.created" {
		t.Fatalf("manifest events = %#v; want transaction.created", manifest.Events)
	}
}

func TestStreamingHandler_HEADReturnsHeadersWithoutBody(t *testing.T) {
	t.Parallel()

	handler, err := NewStreamingHandler(PublisherDescriptor{
		ServiceName: "transaction-service",
		SourceBase:  "//lerian.midaz/transaction-service",
	}, Catalog{})
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodHead, "/streaming", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body length = %d; want 0", recorder.Body.Len())
	}
}

func TestStreamingHandler_NilRequestDoesNotPanic(t *testing.T) {
	t.Parallel()

	handler, err := NewStreamingHandler(PublisherDescriptor{
		ServiceName: "transaction-service",
		SourceBase:  "//lerian.midaz/transaction-service",
	}, Catalog{})
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, nil)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestStreamingHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	handler, err := NewStreamingHandler(PublisherDescriptor{
		ServiceName: "transaction-service",
		SourceBase:  "//lerian.midaz/transaction-service",
	}, Catalog{})
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/streaming", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}
