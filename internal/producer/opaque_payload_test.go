//go:build unit

package producer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v2/log"
)

// xmlPayload is a minimal ISO-8859-1 XML document that is deliberately NOT
// valid JSON (byte 0xE7 also makes it non-UTF8) — the SFN opaque-payload case.
var xmlPayload = []byte{
	'<', 'd', 'o', 'c', '>', 0xE7, '<', '/', 'd', 'o', 'c', '>', // <doc>ç</doc> in latin-1
}

// xmlCatalog registers a single definition whose DataContentType is
// application/xml, mapped to the same resource/event as sampleCatalog's
// "transaction.created" so its derived topic ("test.transaction.created")
// matches the topic kfakeConfig pre-seeds.
func xmlCatalog(tb testing.TB) Catalog {
	tb.Helper()

	catalog, err := NewCatalog(EventDefinition{
		Key:             "transaction.created",
		ResourceType:    "transaction",
		EventType:       "created",
		SchemaVersion:   "1.0.0",
		DataContentType: "application/xml",
	})
	if err != nil {
		tb.Fatalf("xmlCatalog: NewCatalog err = %v", err)
	}

	return catalog
}

// TestProducer_Emit_NonJSONContentType_Accepts is the primary opaque-payload
// test: a definition with DataContentType application/xml lets a non-JSON
// payload through both pre-flight gates and Emit returns nil.
func TestProducer_Emit_NonJSONContentType_Accepts(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(xmlCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	req := sampleRequest()
	req.Payload = json.RawMessage(xmlPayload)

	if err := emitter.Emit(context.Background(), req); err != nil {
		t.Fatalf("Emit err = %v; want nil (non-JSON payload under application/xml)", err)
	}
}

// TestProducer_PreFlight_ContentTypeAwareJSON pins the content-type gate on
// the JSON-validity check: a non-JSON payload passes under application/xml but
// STILL rejects under application/json and under an empty (default) content
// type. handleOutboxRow shares this exact code path, so gating it here also
// governs outbox replay.
func TestProducer_PreFlight_ContentTypeAwareJSON(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	tests := []struct {
		name            string
		dataContentType string
		wantErr         error
	}{
		{name: "xml non-json passes", dataContentType: "application/xml", wantErr: nil},
		{name: "json non-json rejects", dataContentType: "application/json", wantErr: ErrNotJSON},
		{name: "empty non-json rejects", dataContentType: "", wantErr: ErrNotJSON},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			event := sampleEvent()
			// Do NOT call ApplyDefaults — it would rewrite an empty
			// DataContentType to application/json and mask the empty case.
			event.DataContentType = tt.dataContentType
			event.Payload = json.RawMessage(xmlPayload)

			err := p.preFlightWithPayload(context.Background(), event, true)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("preFlight err = %v; want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("preFlight err = %v; want errors.Is(%v)", err, tt.wantErr)
			}
		})
	}
}

// TestProducer_PreFlight_OversizedNonJSON_StillTooLarge proves the size cap is
// NOT folded into the JSON-validity branch: an oversized non-JSON payload
// under application/xml still rejects with ErrPayloadTooLarge (protects Kafka
// max.message.bytes regardless of content type).
func TestProducer_PreFlight_OversizedNonJSON_StillTooLarge(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	event := sampleEvent()
	event.DataContentType = "application/xml"
	// Non-JSON bytes, one over the cap.
	event.Payload = json.RawMessage(strings.Repeat("x", maxPayloadBytes+1))

	err = p.preFlightWithPayload(context.Background(), event, true)
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("preFlight err = %v; want ErrPayloadTooLarge", err)
	}
}

// TestIsJSONContentType pins the JSON-family recognition: a bare
// application/json, an empty (CloudEvents default) value, a charset PARAMETER,
// and any RFC 6839 +json structured suffix all re-enter the json.Valid gate;
// non-JSON media types (xml, text) stay opaque; a malformed content type fails
// CLOSED (validate rather than silently skip).
func TestIsJSONContentType(t *testing.T) {
	tests := []struct {
		name string
		ct   string
		want bool
	}{
		{name: "empty is default json", ct: "", want: true},
		{name: "plain json", ct: "application/json", want: true},
		{name: "json with charset param", ct: "application/json; charset=utf-8", want: true},
		{name: "cloudevents +json suffix", ct: "application/cloudevents+json", want: true},
		{name: "hal +json suffix", ct: "application/hal+json", want: true},
		{name: "xml is opaque", ct: "application/xml", want: false},
		{name: "xml with charset param is opaque", ct: "application/xml; charset=iso-8859-1", want: false},
		{name: "xml with malformed charset remains opaque", ct: "application/xml; charset=@@@", want: false},
		{name: "json with malformed charset remains json", ct: "application/json; charset=@@@", want: true},
		{name: "text plain is opaque", ct: "text/plain", want: false},
		{name: "malformed fails closed to validate", ct: "@@@", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isJSONContentType(tt.ct); got != tt.want {
				t.Fatalf("isJSONContentType(%q) = %v; want %v", tt.ct, got, tt.want)
			}
		})
	}
}

// TestProducer_Emit_ParameterizedJSON_RejectsMalformed is the emit-level
// regression: a definition whose DataContentType carries a charset PARAMETER
// (application/json; charset=utf-8) is still JSON — a malformed payload must
// re-enter the json.Valid gate and Emit must return ErrNotJSON, not ship the
// poison bytes verbatim. (The application/xml relay-preserved case — non-JSON
// payload -> Emit nil — is covered by TestProducer_Emit_NonJSONContentType_Accepts.)
func TestProducer_Emit_ParameterizedJSON_RejectsMalformed(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	catalog, err := NewCatalog(EventDefinition{
		Key:             "transaction.created",
		ResourceType:    "transaction",
		EventType:       "created",
		SchemaVersion:   "1.0.0",
		DataContentType: "application/json; charset=utf-8",
	})
	if err != nil {
		t.Fatalf("NewCatalog err = %v", err)
	}

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(catalog))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	req := sampleRequest()
	req.Payload = json.RawMessage(xmlPayload) // malformed JSON

	if err := emitter.Emit(context.Background(), req); !errors.Is(err, ErrNotJSON) {
		t.Fatalf("Emit err = %v; want ErrNotJSON (parameterized JSON content type must re-enter json.Valid gate)", err)
	}
}
