//go:build unit

package streaming_test

import (
	"context"
	"fmt"

	streaming "github.com/LerianStudio/lib-streaming"
)

// Example_basicUsage shows the minimal Emit path exercised against a
// MockEmitter. This is the 10-line bootstrap pattern from the package
// godoc, compressed for an executable example.
func Example_basicUsage() {
	mock := streaming.NewMockEmitter()

	if err := mock.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "t-abc",
		Subject:       "tx-123",
		Payload:       []byte(`{"amount":100}`),
	}); err != nil {
		return
	}

	fmt.Println(len(mock.Requests()))
	// Output: 1
}
