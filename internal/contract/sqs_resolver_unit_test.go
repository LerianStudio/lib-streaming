//go:build unit

package contract

import "context"

func init() {
	resolveSQSQueueURLOnce = func(context.Context, string) error { return nil }
}
