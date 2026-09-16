//go:build unit

package cloudevents

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

const sanitizeReplacement = "�"

// ce-* header values are arbitrary bytes on the wire, and this parser is what
// turns them into the Event every consumer of this library handles. A value
// holding U+0000 or an invalid UTF-8 sequence is refused permanently by any
// text store, log pipeline or JSON encoder the consumer hands it to, so a
// consumer that retries such a failure — correct, when the evidence must not be
// lost — never makes progress. See LerianStudio/lender#340.
func TestParseCloudEventsHeaders_SanitizesHostileHeaderValues(t *testing.T) {
	t.Parallel()

	event, err := ParseCloudEventsHeaders([]kgo.RecordHeader{
		{Key: headerCESpecVersion, Value: []byte("1.0")},
		{Key: headerCEID, Value: []byte("ce-1\x00")},
		{Key: headerCESource, Value: []byte{'g', 'w', 0xff, 0xfe}},
		{Key: headerCEType, Value: []byte("gw.contract.registered.v1")},
		{Key: headerCETime, Value: []byte("2026-09-15T12:34:56.789Z")},
		{Key: headerCETenantID, Value: []byte("tenant-a\x00")},
		{Key: headerCEResourceType, Value: []byte("contract\x00")},
		{Key: headerCEEventType, Value: []byte{'r', 'e', 'g', 0xc3}},
		{Key: headerCESchemaVersion, Value: []byte("1.0.0")},
	})
	if err != nil {
		t.Fatalf("ParseCloudEventsHeaders: %v", err)
	}

	for field, value := range map[string]string{
		"EventID":       event.EventID,
		"Source":        event.Source,
		"TenantID":      event.TenantID,
		"ResourceType":  event.ResourceType,
		"EventType":     event.EventType,
		"SchemaVersion": event.SchemaVersion,
	} {
		if strings.IndexByte(value, 0) >= 0 {
			t.Errorf("%s = %q kept a NUL; a text store refuses it permanently", field, value)
		}

		if !utf8.ValidString(value) {
			t.Errorf("%s = %q is not valid UTF-8; a text store refuses it permanently", field, value)
		}
	}

	for field, got := range map[string]struct{ have, want string }{
		"EventID":      {event.EventID, "ce-1" + sanitizeReplacement},
		"Source":       {event.Source, "gw" + sanitizeReplacement},
		"TenantID":     {event.TenantID, "tenant-a" + sanitizeReplacement},
		"ResourceType": {event.ResourceType, "contract" + sanitizeReplacement},
		"EventType":    {event.EventType, "reg" + sanitizeReplacement},
	} {
		if got.have != got.want {
			t.Errorf("%s = %q; want %q", field, got.have, got.want)
		}
	}

	// A clean value must come through untouched, and the parsed timestamp must
	// be unaffected: sanitizing is not allowed to disturb what was already fine.
	if event.SchemaVersion != "1.0.0" {
		t.Errorf("SchemaVersion = %q; want the untouched 1.0.0", event.SchemaVersion)
	}

	if event.Timestamp.IsZero() {
		t.Error("ce-time no longer parses")
	}
}

// ce-specversion is COMPARED, not carried, so a hostile one must still be
// refused rather than sanitized into a match. The check runs on the sanitized
// value, and "1.0" plus a NUL is not "1.0".
func TestParseCloudEventsHeaders_AHostileSpecVersionIsStillRefused(t *testing.T) {
	t.Parallel()

	_, err := ParseCloudEventsHeaders([]kgo.RecordHeader{
		{Key: headerCESpecVersion, Value: []byte("1.0\x00")},
		{Key: headerCEID, Value: []byte("ce-1")},
		{Key: headerCESource, Value: []byte("gw")},
		{Key: headerCEType, Value: []byte("gw.contract.registered.v1")},
		{Key: headerCETime, Value: []byte("2026-09-15T12:34:56.789Z")},
	})
	if err == nil {
		t.Fatal("a spec version that is not 1.0 must be refused, sanitized or not")
	}
}
