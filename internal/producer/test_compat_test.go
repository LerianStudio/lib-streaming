package producer

import (
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/mod/semver"

	"github.com/LerianStudio/lib-streaming/internal/cloudevents"
	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/emitter"
)

const (
	defaultBatchLingerMs         = 5
	defaultMaxBufferedRecords    = 10_000
	defaultCompression           = "lz4"
	defaultRecordRetries         = 10
	defaultRequiredAcks          = "all"
	defaultCBFailureRatio        = 0.5
	defaultCBMinRequests         = 10
	defaultDataContentType       = "application/json"
	topicPrefix                  = "lerian.streaming."
	cloudEventsSpecVersion       = "1.0"
	defaultRecordDeliveryTimeout = 30 * time.Second
	defaultCBTimeout             = 30 * time.Second
	defaultCloseTimeout          = 30 * time.Second
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

func parseMajorVersion(version string) int {
	if version == "" {
		return 0
	}

	if version == "1.0.0" || version == "v1.0.0" || version == "1" || version == "v1" {
		return 1
	}

	major := semver.Major("v" + strings.TrimPrefix(version, "v"))
	if major == "" {
		return 0
	}

	n, err := strconv.Atoi(strings.TrimPrefix(major, "v"))
	if err != nil || n < 0 {
		return 0
	}

	return n
}
