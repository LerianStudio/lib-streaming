//nolint:unused // Shared compatibility constants are consumed by integration/chaos build-tagged tests.
package producer

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/emitter"
)

// NOTE: This file deliberately carries NO build tag, unlike the
// `test_compat_test.go` files in sibling internal packages (config,
// contract, manifest), which are gated to `//go:build unit`. The producer
// package has untagged test files (test_catalog_test.go) that depend on
// the symbols defined here, so this shim must compile under the default
// `go test ./...` invocation that `make check-tests` uses.
//
// Test-only constants surfaced for unit, integration, and chaos tests in
// this package. Production code reads its defaults from internal/config and
// internal/producer/cb_init — these constants exist to keep tests source-
// stable across the refactor that moved canonical defaults into config/.
const (
	defaultBatchLingerMs         = 5
	defaultMaxBufferedRecords    = 10_000
	defaultCompression           = "lz4"
	defaultRecordRetries         = 10
	defaultRequiredAcks          = "all"
	defaultCBFailureRatio        = 0.5
	defaultCBMinRequests         = 10
	defaultDataContentType       = "application/json"
	cloudEventsSpecVersion       = "1.0"
	defaultRecordDeliveryTimeout = 30 * time.Second
	defaultCBTimeout             = 30 * time.Second
)

type NoopEmitter = emitter.NoopEmitter

const (
	DirectModeSkip   = contract.DirectModeSkip
	OutboxModeAlways = contract.OutboxModeAlways
	OutboxModeNever  = contract.OutboxModeNever
	DLQModeNever     = contract.DLQModeNever
)

func NewCatalog(definitions ...EventDefinition) (Catalog, error) {
	return contract.NewCatalog(definitions...)
}

func ParseCloudEventsHeaders(headers []kgo.RecordHeader) (Event, error) {
	return cloudevents.ParseCloudEventsHeaders(headers)
}

// sourceTopic returns the ONE topic Event.Topic() derives for a given
// ce-source. Build-tagged integration/chaos tests use it to name the topic
// the producer publishes to.
//
// It replaced v2's sourceTopicPrefix / sanitizeSourceSegment /
// parseMajorVersion forwarders: with the topic collapse there is no
// per-event suffix to append and no lossy source reduction to mirror, so a
// test no longer reconstructs a topic — it just asks for the app topic.
func sourceTopic(source string) string {
	return contract.AppTopic(source)
}
