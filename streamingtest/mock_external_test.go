//go:build unit

package streamingtest_test

import (
	"context"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming/v3"
	"github.com/LerianStudio/lib-streaming/v3/streamingtest"
)

func TestMockEmitterImplementsStreamingEmitter(t *testing.T) {
	t.Parallel()

	var emitter streaming.Emitter = streamingtest.NewMockEmitter()
	if err := emitter.Emit(context.Background(), streaming.EmitRequest{DefinitionKey: "transaction.created"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
}
