//go:build unit

package config

import "github.com/LerianStudio/lib-streaming/internal/contract"

const (
	DirectModeSkip   = contract.DirectModeSkip
	OutboxModeAlways = contract.OutboxModeAlways
	DLQModeNever     = contract.DLQModeNever
)
