package cloudevents

import (
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CloudEvents binary-mode header keys. Every CloudEvents context attribute
// travels as a Kafka record header with the "ce-" prefix (required by the
// CloudEvents Kafka protocol binding, v1.0).
const (
	headerCESpecVersion     = "ce-specversion"
	headerCEID              = "ce-id"
	headerCESource          = "ce-source"
	headerCEType            = "ce-type"
	headerCETime            = "ce-time"
	headerCESubject         = "ce-subject"
	headerCEDataContentType = "ce-datacontenttype"
	headerCEDataSchema      = "ce-dataschema"
	headerCETenantID        = "ce-tenantid"
	headerCESchemaVersion   = "ce-schemaversion"
	headerCEResourceType    = "ce-resourcetype"
	headerCEEventType       = "ce-eventtype"
	headerCESystemEvent     = "ce-systemevent"
)

// cloudEventsSpecVersion is the CloudEvents binary-mode spec version
// literal. Only 1.0 is supported; bumping this requires a coordinated
// downstream consumer upgrade.
const cloudEventsSpecVersion = "1.0"

// cloudEventsTypePrefix is the reverse-DNS namespace prepended to every
// ce-type header value. Composed as "studio.lerian.<resource>.<event>".
// Matches TRD §6.1 verbatim.
const cloudEventsTypePrefix = "studio.lerian."

// BuildTransportHeaders assembles a transport-native []transport.Header
// slice carrying the CloudEvents binary-mode envelope per TRD §6.1.
//
// This is the canonical hot-path builder used by both the legacy single-
// target Kafka path and the multi-target dispatcher. It writes directly
// into transport.Header — no intermediate kgo.RecordHeader, no per-byte
// copy. The Kafka adapter consumes this slice via toKgoHeaders and reuses
// the underlying byte slices verbatim because the message lifetime is
// fully owned by Publish (callback fires synchronously inside the
// produce/select gate).
//
// Wire-format invariants (unchanged from buildCloudEventsHeaders):
//   - Required: ce-specversion, ce-id, ce-source, ce-type, ce-time,
//     ce-schemaversion, ce-resourcetype, ce-eventtype.
//   - Optional, emitted only when the corresponding Event field is non-
//     empty: ce-subject, ce-datacontenttype, ce-dataschema, ce-tenantid.
//   - ce-systemevent literal "true" only when Event.SystemEvent is true;
//     omitted otherwise (false is the implicit default).
//
// The resulting slice has at most 13 entries (full set including every
// optional); callers MUST NOT mutate either the slice or the byte values
// because the underlying byte representation may be aliased to package-
// level constants for the spec version literal.
func BuildTransportHeaders(event Event) []transport.Header {
	headers := make([]transport.Header, 0, 13)

	headers = append(headers,
		transport.Header{Key: headerCESpecVersion, Value: []byte(cloudEventsSpecVersion)},
		transport.Header{Key: headerCEID, Value: []byte(event.EventID)},
		transport.Header{Key: headerCESource, Value: []byte(event.Source)},
		transport.Header{
			Key:   headerCEType,
			Value: []byte(cloudEventsTypePrefix + event.ResourceType + "." + event.EventType),
		},
		transport.Header{Key: headerCETime, Value: []byte(event.Timestamp.Format(time.RFC3339Nano))},
		transport.Header{Key: headerCESchemaVersion, Value: []byte(event.SchemaVersion)},
		transport.Header{Key: headerCEResourceType, Value: []byte(event.ResourceType)},
		transport.Header{Key: headerCEEventType, Value: []byte(event.EventType)},
	)

	if event.Subject != "" {
		headers = append(headers, transport.Header{
			Key:   headerCESubject,
			Value: []byte(event.Subject),
		})
	}

	if event.DataContentType != "" {
		headers = append(headers, transport.Header{
			Key:   headerCEDataContentType,
			Value: []byte(event.DataContentType),
		})
	}

	if event.DataSchema != "" {
		headers = append(headers, transport.Header{
			Key:   headerCEDataSchema,
			Value: []byte(event.DataSchema),
		})
	}

	if event.TenantID != "" {
		headers = append(headers, transport.Header{
			Key:   headerCETenantID,
			Value: []byte(event.TenantID),
		})
	}

	if event.SystemEvent {
		headers = append(headers, transport.Header{
			Key:   headerCESystemEvent,
			Value: []byte("true"),
		})
	}

	return headers
}

// buildCloudEventsHeaders is the kgo-typed shim retained for the public
// root-level BuildCloudEventsHeaders API (streaming.BuildCloudEventsHeaders).
// It is NOT on any Emit hot path — every internal publish call site uses
// BuildTransportHeaders directly. Public-API callers that want to roundtrip
// through kgo retain identical wire-format behavior, with one extra (cold-
// path) per-byte copy when converting to franz-go's RecordHeader shape.
func buildCloudEventsHeaders(event Event) []kgo.RecordHeader {
	transportHeaders := BuildTransportHeaders(event)
	if len(transportHeaders) == 0 {
		return nil
	}

	kgoHeaders := make([]kgo.RecordHeader, len(transportHeaders))
	for i, h := range transportHeaders {
		kgoHeaders[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}

	return kgoHeaders
}

// BuildHeaders assembles CloudEvents binary-mode Kafka headers for event.
//
// Cold-path public API kept for the streaming.BuildCloudEventsHeaders root-
// level helper. The Emit hot path uses BuildTransportHeaders directly to
// avoid the kgo→transport→kgo triple conversion that previously dominated
// per-Emit allocations.
func BuildHeaders(event Event) []kgo.RecordHeader {
	return buildCloudEventsHeaders(event)
}

// ErrMissingRequiredHeader is returned by ParseCloudEventsHeaders when a
// required CloudEvents header (ce-specversion, ce-id, ce-source, ce-type,
// ce-time) is missing. Exported so integration tests in other packages can
// match the error precisely via errors.Is.
var ErrMissingRequiredHeader = errors.New("streaming: missing required CloudEvents header")

// ErrUnsupportedSpecVersion is returned by ParseCloudEventsHeaders when the
// ce-specversion header IS present but carries a value other than "1.0".
// Distinct from ErrMissingRequiredHeader so callers can distinguish "header
// absent" from "header present but wrong version" via errors.Is.
var ErrUnsupportedSpecVersion = errors.New("streaming: unsupported CloudEvents specversion")

// ParseCloudEventsHeaders is the inverse of buildCloudEventsHeaders. It is
// primarily used by tests that verify the on-wire format round-trips back to
// the same Event shape — particularly the CloudEvents SDK contract test in
// T9 (integration). Real consumers doing production Kafka consumption
// typically reach for cloudevents/sdk-go/v2 instead, which handles the
// full CloudEvents spec (structured mode, extension registry, validation).
//
// Missing required headers surface as ErrMissingRequiredHeader wrapped with
// the specific key. Optional headers (subject, datacontenttype, dataschema,
// systemevent) default to their zero values when absent.
//
// ce-time is parsed via time.RFC3339Nano to preserve sub-second precision
// across the round trip.
//
// When a header key appears multiple times, the LAST occurrence wins.
// Callers relying on duplicate-header semantics should parse headers
// directly from kgo.Record.Headers instead.
func ParseCloudEventsHeaders(headers []kgo.RecordHeader) (Event, error) {
	// Flatten headers into a map so we can do O(1) lookups. The last header
	// with a given key wins — mirrors producer behavior where later keys
	// overwrite earlier ones.
	index := make(map[string]string, len(headers))
	for _, h := range headers {
		index[h.Key] = string(h.Value)
	}

	// Required headers — missing any is a parse failure. ce-specversion
	// must be "1.0" — the only version this package targets.
	specVersion, ok := index[headerCESpecVersion]
	if !ok {
		return Event{}, fmt.Errorf("%w: %s", ErrMissingRequiredHeader, headerCESpecVersion)
	}

	if specVersion != cloudEventsSpecVersion {
		return Event{}, fmt.Errorf("%w: %q (want %q)", ErrUnsupportedSpecVersion, specVersion, cloudEventsSpecVersion)
	}

	eventID, ok := index[headerCEID]
	if !ok {
		return Event{}, fmt.Errorf("%w: %s", ErrMissingRequiredHeader, headerCEID)
	}

	source, ok := index[headerCESource]
	if !ok {
		return Event{}, fmt.Errorf("%w: %s", ErrMissingRequiredHeader, headerCESource)
	}

	if _, ok := index[headerCEType]; !ok {
		return Event{}, fmt.Errorf("%w: %s", ErrMissingRequiredHeader, headerCEType)
	}

	timeRaw, ok := index[headerCETime]
	if !ok {
		return Event{}, fmt.Errorf("%w: %s", ErrMissingRequiredHeader, headerCETime)
	}

	ts, err := time.Parse(time.RFC3339Nano, timeRaw)
	if err != nil {
		return Event{}, fmt.Errorf("streaming: invalid ce-time %q: %w", timeRaw, err)
	}

	// Extensions are treated as optional on parse — ce-resourcetype and
	// ce-eventtype are always emitted by the producer but we accept inputs
	// from other producers that may not emit them.
	return Event{
		EventID:         eventID,
		Source:          source,
		Timestamp:       ts,
		Subject:         index[headerCESubject],
		DataContentType: index[headerCEDataContentType],
		DataSchema:      index[headerCEDataSchema],
		TenantID:        index[headerCETenantID],
		SchemaVersion:   index[headerCESchemaVersion],
		ResourceType:    index[headerCEResourceType],
		EventType:       index[headerCEEventType],
		SystemEvent:     index[headerCESystemEvent] == "true",
	}, nil
}
