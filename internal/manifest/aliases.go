package manifest

import "github.com/LerianStudio/lib-streaming/v4/internal/contract"

type (
	Catalog        = contract.Catalog
	EventClass     = contract.EventClass
	DeliveryPolicy = contract.DeliveryPolicy
	RouteTable     = contract.RouteTable
	TransportKind  = contract.TransportKind
)

var ErrInvalidPublisherDescriptor = contract.ErrInvalidPublisherDescriptor
