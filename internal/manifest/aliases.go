package manifest

import "github.com/LerianStudio/lib-streaming/v3/internal/contract"

type (
	Catalog        = contract.Catalog
	DeliveryPolicy = contract.DeliveryPolicy
	RouteTable     = contract.RouteTable
	TransportKind  = contract.TransportKind
)

var ErrInvalidPublisherDescriptor = contract.ErrInvalidPublisherDescriptor
