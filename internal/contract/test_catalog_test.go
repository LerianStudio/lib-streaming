// This file is intentionally tagless: sampleEvent() is a shared fixture used
// by tagless contract tests (e.g. outbox_envelope_test.go). Adding a build
// tag here would leave those suites unable to build.

package contract

import "encoding/json"

//nolint:unused // Shared fixture is consumed by build-tagged contract tests.
func sampleEvent() Event {
	return Event{
		TenantID:      "t-abc",
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
		Source:        "//test/service",
		Subject:       "tx-123",
		Payload:       json.RawMessage(`{"amount":100}`),
	}
}
