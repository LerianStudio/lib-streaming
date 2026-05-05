//go:build unit

package manifest

import "github.com/LerianStudio/lib-streaming/internal/contract"

type (
	EventDefinition = contract.EventDefinition
	OutboxMode      = contract.OutboxMode
	DLQMode         = contract.DLQMode
)

const (
	OutboxModeAlways = contract.OutboxModeAlways
	DLQModeNever     = contract.DLQModeNever
)

func NewCatalog(definitions ...EventDefinition) (Catalog, error) {
	return contract.NewCatalog(definitions...)
}
