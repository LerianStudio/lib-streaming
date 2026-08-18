//go:build unit

package manifest

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
		Source:      "midaz-transaction-service",
	}, catalog, RouteTable{})
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
		Source:      "midaz-transaction-service",
	}, Catalog{}, RouteTable{})
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
		Source:      "midaz-transaction-service",
	}, Catalog{}, RouteTable{})
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
		Source:      "midaz-transaction-service",
	}, Catalog{}, RouteTable{})
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/streaming", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
	// RFC 7231 §7.4.1: the Allow header is REQUIRED on 405 responses so
	// clients know which methods are acceptable.
	if got, want := recorder.Header().Get("Allow"), "GET, HEAD"; got != want {
		t.Errorf("Allow header = %q; want %q", got, want)
	}
}

// TestStreamingHandler_GoldenManifestJSON pins the manifest's JSON KEYS and the
// v3-defining values as literals, decoded into map[string]any.
//
// The other tests in this package decode into the typed structs, so a renamed
// json tag round-trips through its own rename and every one of them still
// passes. The manifest is a CONTRACT with the Hub and with contract-diffing
// tooling: renaming "eventKey", or letting a per-event "topic" reappear, breaks
// consumers that never compiled against these structs.
func TestStreamingHandler_GoldenManifestJSON(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(EventDefinition{
		Key:          "loan.disbursed",
		ResourceType: "loan_contract",
		EventType:    "disbursed",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	handler, err := NewStreamingHandler(PublisherDescriptor{
		ServiceName: "lender-svc",
		Source:      "lender",
	}, catalog, RouteTable{})
	if err != nil {
		t.Fatalf("NewStreamingHandler() error = %v", err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/streaming", nil))

	var doc map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	if got := doc["version"]; got != "1.0.0" {
		t.Errorf(`manifest["version"] = %v; want "1.0.0"`, got)
	}

	// One topic per producing application, at the DOCUMENT level.
	if got := doc["topic"]; got != "lerian.streaming.lender" {
		t.Errorf(`manifest["topic"] = %v; want "lerian.streaming.lender"`, got)
	}

	if got := doc["dlqTopic"]; got != "lerian.streaming.lender.dlq" {
		t.Errorf(`manifest["dlqTopic"] = %v; want "lerian.streaming.lender.dlq"`, got)
	}

	publisher, ok := doc["publisher"].(map[string]any)
	if !ok {
		t.Fatalf(`manifest["publisher"] = %T; want an object`, doc["publisher"])
	}

	if got := publisher["source"]; got != "lender" {
		t.Errorf(`manifest["publisher"]["source"] = %v; want "lender" (v2's "sourceBase" is gone)`, got)
	}

	if _, present := publisher["sourceBase"]; present {
		t.Error(`manifest["publisher"] still carries the v2 "sourceBase" key`)
	}

	events, ok := doc["events"].([]any)
	if !ok || len(events) != 1 {
		t.Fatalf(`manifest["events"] = %v; want exactly one entry`, doc["events"])
	}

	event, ok := events[0].(map[string]any)
	if !ok {
		t.Fatalf(`manifest["events"][0] = %T; want an object`, events[0])
	}

	if got := event["eventKey"]; got != "loan_contract.disbursed" {
		t.Errorf(`events[0]["eventKey"] = %v; want "loan_contract.disbursed"`, got)
	}

	if got := event["resourceType"]; got != "loan_contract" {
		t.Errorf(`events[0]["resourceType"] = %v; want "loan_contract"`, got)
	}

	if got := event["eventType"]; got != "disbursed" {
		t.Errorf(`events[0]["eventType"] = %v; want "disbursed"`, got)
	}

	// The per-event topic is GONE in v3. A definition has no topic of its own.
	if _, present := event["topic"]; present {
		t.Errorf(`events[0] carries a "topic" key; v3 removed the per-event topic entirely (got %v)`, event["topic"])
	}
}
