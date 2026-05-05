package config

import "github.com/LerianStudio/lib-streaming/v2/internal/contract"

type (
	DeliveryPolicyOverride = contract.DeliveryPolicyOverride
	DirectMode             = contract.DirectMode
	OutboxMode             = contract.OutboxMode
	DLQMode                = contract.DLQMode
)

var (
	ErrMissingBrokers        = contract.ErrMissingBrokers
	ErrMissingSource         = contract.ErrMissingSource
	ErrInvalidCompression    = contract.ErrInvalidCompression
	ErrInvalidAcks           = contract.ErrInvalidAcks
	ErrInvalidDeliveryPolicy = contract.ErrInvalidDeliveryPolicy
)
