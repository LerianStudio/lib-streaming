package manifest

import "github.com/LerianStudio/lib-streaming/internal/contract"

type (
	Catalog        = contract.Catalog
	DeliveryPolicy = contract.DeliveryPolicy
)

var ErrInvalidPublisherDescriptor = contract.ErrInvalidPublisherDescriptor

func hasControlChar(s string) bool {
	return contract.HasControlChar(s)
}
