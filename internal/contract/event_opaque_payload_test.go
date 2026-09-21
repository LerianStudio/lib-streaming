//go:build unit

package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// sfnDocument is a minimal SFN-shaped document: XML, declared ISO-8859-1, and
// carrying byte 0xE7 (latin-1 "ç"). It is deliberately NOT valid JSON and
// deliberately NOT valid UTF-8 — the two properties that together make it the
// worst case, because a JSON string cannot carry it and Go's encoder would
// replace the offending byte with U+FFFD without returning an error.
var sfnDocument = []byte("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><DOC><Nome>A\xe7\xe3o</Nome></DOC>")

// opaqueEnvelope builds a structurally valid OutboxEnvelope carrying payload
// under contentType — the shape the producer persists on the outbox route.
func opaqueEnvelope(contentType string, payload []byte) OutboxEnvelope {
	return OutboxEnvelope{
		Version:       OutboxEnvelopeVersion,
		RouteKey:      "primary.all",
		DefinitionKey: "documento.enviado",
		Target:        "primary",
		Transport:     TransportKafkaLike,
		Destination:   Destination{Kind: TransportKafkaLike, Name: "lerian.streaming.slc"},
		AggregateID:   uuid.MustParse("0192f3a0-0000-7000-8000-000000000001"),
		Requirement:   RouteRequired,
		Policy:        DefaultDeliveryPolicy(),
		Event: Event{
			TenantID:        "tenant-slc",
			ResourceType:    "documento",
			EventType:       "enviado",
			EventID:         "0192f3a0-0000-7000-8000-000000000002",
			SchemaVersion:   "1.0.0",
			Source:          "slc",
			Subject:         "doc-1",
			DataContentType: contentType,
			Payload:         payload,
		},
	}
}

// TestOutboxEnvelope_OpaquePayloadRoundTrips is the core regression.
//
// Before this change json.Marshal of an envelope carrying an XML payload
// failed outright — json.RawMessage hands its bytes to the encoder verbatim
// and the encoder then VALIDATES them, so the persist returned
// "invalid character '<' looking for beginning of value". A service handing an
// SFN document to the transactional outbox route could not persist it at all,
// while the same document emitted directly published fine.
//
// The assertions are, in order: the persist succeeds; the persisted bytes are
// valid JSON (lib-commons stores the outbox payload in a JSONB column and
// rejects anything else); and the decoded payload is BYTE-IDENTICAL to what
// went in, including the non-UTF8 byte that a JSON string would have silently
// replaced with U+FFFD.
func TestOutboxEnvelope_OpaquePayloadRoundTrips(t *testing.T) {
	t.Parallel()

	for _, contentType := range []string{
		"application/xml",
		"text/xml; charset=ISO-8859-1",
		"application/octet-stream",
	} {
		t.Run(contentType, func(t *testing.T) {
			t.Parallel()

			persisted, err := json.Marshal(opaqueEnvelope(contentType, sfnDocument))
			if err != nil {
				t.Fatalf("json.Marshal(envelope) err = %v; want nil (opaque payload must persist)", err)
			}

			if !json.Valid(persisted) {
				t.Fatal("persisted envelope is not valid JSON; the outbox column is JSONB")
			}

			var decoded OutboxEnvelope
			if err := json.Unmarshal(persisted, &decoded); err != nil {
				t.Fatalf("json.Unmarshal(envelope) err = %v; want nil", err)
			}

			if !bytes.Equal(decoded.Event.Payload, sfnDocument) {
				t.Fatalf("payload round-trip lost bytes:\n got %q\nwant %q", decoded.Event.Payload, sfnDocument)
			}

			if decoded.Event.DataContentType != contentType {
				t.Fatalf("DataContentType = %q; want %q", decoded.Event.DataContentType, contentType)
			}

			// The envelope must still validate after decode — the relay runs
			// Validate on the decoded row before it publishes anything.
			if err := decoded.Validate(); err != nil {
				t.Fatalf("decoded envelope Validate() err = %v; want nil", err)
			}
		})
	}
}

// TestOutboxEnvelope_JSONPayloadStaysInline pins the no-regression half: a
// JSON payload is still persisted INLINE under "Payload" with no
// "PayloadOpaque" key anywhere. Rows written by this version and by every
// previous one are therefore the same bytes, and the operator SQL in
// MIGRATION-v4.md (payload->'event'->>'Source', payload->'event'->'Payload')
// keeps working unchanged.
func TestOutboxEnvelope_JSONPayloadStaysInline(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"amount":"100.00","currency":"BRL"}`)

	for _, contentType := range []string{"", "application/json", "application/json; charset=utf-8", "application/cloudevents+json"} {
		t.Run("ct="+contentType, func(t *testing.T) {
			t.Parallel()

			persisted, err := json.Marshal(opaqueEnvelope(contentType, payload))
			if err != nil {
				t.Fatalf("json.Marshal(envelope) err = %v; want nil", err)
			}

			if strings.Contains(string(persisted), "PayloadOpaque") {
				t.Fatalf("JSON payload was persisted opaquely; row = %s", persisted)
			}

			if !strings.Contains(string(persisted), `"Payload":{"amount":"100.00","currency":"BRL"}`) {
				t.Fatalf("JSON payload is not inline; row = %s", persisted)
			}

			var decoded OutboxEnvelope
			if err := json.Unmarshal(persisted, &decoded); err != nil {
				t.Fatalf("json.Unmarshal(envelope) err = %v; want nil", err)
			}

			if !bytes.Equal(decoded.Event.Payload, payload) {
				t.Fatalf("payload round-trip lost bytes:\n got %q\nwant %q", decoded.Event.Payload, payload)
			}
		})
	}
}

// TestEvent_MalformedJSONUnderJSONContentTypeStillFails pins the fail-loud
// edge: the opaque side channel is keyed on the DECLARED content type, never
// on "did json.Valid fail". Garbage declared as application/json must still
// blow up at marshal rather than being quietly rerouted through the base64
// field, which would let malformed bytes reach a consumer that trusts
// ce-datacontenttype.
func TestEvent_MalformedJSONUnderJSONContentTypeStillFails(t *testing.T) {
	t.Parallel()

	if _, err := json.Marshal(opaqueEnvelope("application/json", []byte(`{not json`))); err == nil {
		t.Fatal("json.Marshal(envelope) err = nil; want a failure for malformed JSON under a JSON content type")
	}
}

// TestEvent_OpaquePayloadSurvivesContentTypeEdit pins the read rule: decode
// restores PayloadOpaque whenever the field is PRESENT, rather than
// re-deriving the decision from DataContentType. An operator who edits the
// content type on a persisted row must not thereby erase the payload.
func TestEvent_OpaquePayloadSurvivesContentTypeEdit(t *testing.T) {
	t.Parallel()

	persisted, err := json.Marshal(opaqueEnvelope("application/xml", sfnDocument))
	if err != nil {
		t.Fatalf("json.Marshal(envelope) err = %v", err)
	}

	edited := strings.Replace(string(persisted), `"DataContentType":"application/xml"`, `"DataContentType":"application/json"`, 1)
	if edited == string(persisted) {
		t.Fatal("fixture did not contain the expected DataContentType field; test would be vacuous")
	}

	var decoded OutboxEnvelope
	if err := json.Unmarshal([]byte(edited), &decoded); err != nil {
		t.Fatalf("json.Unmarshal(edited) err = %v", err)
	}

	if !bytes.Equal(decoded.Event.Payload, sfnDocument) {
		t.Fatalf("edited row lost its payload:\n got %q\nwant %q", decoded.Event.Payload, sfnDocument)
	}
}

// TestIsJSONContentType pins the single classifier both the producer preflight
// and Event.MarshalJSON read. The two used to be one function in the producer
// package and one implicit assumption in the persist path; a disagreement
// would let an event pass preflight as opaque and then be persisted inline.
func TestIsJSONContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ct   string
		want bool
	}{
		{"", true},
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"application/cloudevents+json", true},
		{"application/hal+json", true},
		{"application/json; charset=@@@", true},
		{"@@@", true}, // unrecognizable fails CLOSED into the json.Valid gate
		{"application/xml", false},
		{"text/xml; charset=ISO-8859-1", false},
		{"application/xml; charset=@@@", false},
		{"application/octet-stream", false},
		{"text/plain", false},
	}

	for _, tt := range tests {
		t.Run(tt.ct, func(t *testing.T) {
			t.Parallel()

			if got := IsJSONContentType(tt.ct); got != tt.want {
				t.Fatalf("IsJSONContentType(%q) = %v; want %v", tt.ct, got, tt.want)
			}
		})
	}
}

// TestEmitRequest_EmptyPayloadRefusedByName is the CodeRabbit finding on #150,
// resolved by naming the fault instead of encoding it.
//
// An event with no body is a caller defect, and before ErrEmptyPayload each
// content type failed differently and badly: an empty JSON payload came back
// as ErrNotJSON ("payload must be valid JSON" — a misleading diagnosis for a
// body the caller forgot to set), while an empty OPAQUE payload passed every
// gate and then died inside the outbox envelope marshal with
// "unexpected end of JSON input", naming neither the field nor the caller.
//
// The gate is on LENGTH, not on content type, so both spellings of empty (nil
// and a zero-length slice) are refused for every content type at the same
// place the upper bound lives.
func TestEmitRequest_EmptyPayloadRefusedByName(t *testing.T) {
	t.Parallel()

	payloads := map[string]json.RawMessage{
		"nil":          nil,
		"zero-length":  {},
		"empty-string": json.RawMessage(""),
	}

	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := NewEmitRequest(EmitRequest{DefinitionKey: "documento.enviado", Payload: payload})
			if !errors.Is(err, ErrEmptyPayload) {
				t.Fatalf("NewEmitRequest err = %v; want errors.Is(ErrEmptyPayload)", err)
			}

			if !IsCallerError(err) {
				t.Fatal("ErrEmptyPayload must be caller-correctable")
			}
		})
	}
}

// TestEvent_EmptyPayloadNeverReachesMarshal pins WHY the length guard in
// MarshalJSON is written `len(e.Payload) > 0` rather than being widened to
// cover the empty case: nothing empty can get that far any more.
//
// Both content types are exercised because the finding was content-type
// specific — the JSON path already failed (for the wrong reason) and the
// opaque path did not fail at all.
func TestEvent_EmptyPayloadNeverReachesMarshal(t *testing.T) {
	t.Parallel()

	for _, contentType := range []string{"application/json", "application/xml", "text/xml; charset=ISO-8859-1", ""} {
		t.Run("ct="+contentType, func(t *testing.T) {
			t.Parallel()

			if _, err := NewEmitRequest(EmitRequest{
				DefinitionKey: "documento.enviado",
				Payload:       json.RawMessage{},
			}); !errors.Is(err, ErrEmptyPayload) {
				t.Fatalf("NewEmitRequest err = %v; want ErrEmptyPayload before any persist", err)
			}
		})
	}
}

// TestOutboxEnvelope_NilOpaquePayloadWouldHaveRepublishedTheWordNull records
// the third failure the single named error closes, and the only one that was
// silent.
//
// A NIL payload under a non-JSON content type passed every gate, persisted as
// the JSON literal null, and decoded back as the four bytes "null" — so the
// relay would have published the word "null" as the record value of a
// regulatory document. This test pins the round-trip fact so the guard that
// prevents it cannot be removed as redundant with the marshal error, which it
// is not: this path raised no error anywhere.
func TestOutboxEnvelope_NilOpaquePayloadWouldHaveRepublishedTheWordNull(t *testing.T) {
	t.Parallel()

	var decoded Event
	if err := json.Unmarshal([]byte(`{"DataContentType":"application/xml","Payload":null}`), &decoded); err != nil {
		t.Fatalf("Unmarshal err = %v", err)
	}

	if !bytes.Equal(decoded.Payload, []byte("null")) {
		t.Fatalf("decoded payload = %q; want the literal null this guard exists to prevent republishing", decoded.Payload)
	}
}
