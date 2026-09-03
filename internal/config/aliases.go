package config

import "github.com/LerianStudio/lib-streaming/v4/internal/contract"

type (
	DeliveryPolicyOverride = contract.DeliveryPolicyOverride
	DirectMode             = contract.DirectMode
	OutboxMode             = contract.OutboxMode
	DLQMode                = contract.DLQMode
)

var (
	ErrMissingBrokers        = contract.ErrMissingBrokers
	ErrMissingSource         = contract.ErrMissingSource
	ErrInvalidSource         = contract.ErrInvalidSource
	ErrInvalidCompression    = contract.ErrInvalidCompression
	ErrInvalidAcks           = contract.ErrInvalidAcks
	ErrInvalidDeliveryPolicy = contract.ErrInvalidDeliveryPolicy
	ErrInvalidConfigField    = contract.ErrInvalidConfigField
	ErrInvalidSASLMechanism  = contract.ErrInvalidSASLMechanism

	ErrInvalidSchemaRegistryConfig = contract.ErrInvalidSchemaRegistryConfig
)
