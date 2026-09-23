//go:build unit

package producer

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v4/internal/buildmeta"
)

// TestEmit_Span_DefaultTracerScope: when the caller does not inject a tracer,
// the streaming.emit span must carry lib-streaming's module path as its
// instrumentation scope name and the module version linked into the binary as
// the scope version (FC-7). Operators read that pair to tell which version of
// the library produced a span; the old short "streaming" name carried neither.
//
// Not parallel: it swaps the global tracer provider, which every test running
// without WithTracer would otherwise observe.
func TestEmit_Span_DefaultTracerScope(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
	)

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})

	cfg, _ := kfakeConfig(t)

	// Deliberately omit WithTracer: this exercises the resolveTracer fallback.
	emitter, err := New(context.Background(), cfg,
		WithLogger(log.NewNop()), WithCatalog(sampleCatalog(t)))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := emitter.Emit(ctx, eventToRequest(sampleEvent())); err != nil {
		t.Fatalf("Emit err = %v", err)
	}

	scope := requireStreamingEmitSpan(t, exporter.GetSpans()).InstrumentationScope

	// Both halves must come from buildmeta, not from a literal the wiring could
	// drift away from: a hardcoded version here would look identical to a real
	// one, and this is the lane's only end-to-end proof of FC-7.
	wantName, wantVersion := buildmeta.Scope()

	if scope.Name != wantName || scope.Version != wantVersion {
		t.Errorf("span scope = (%q, %q); want (%q, %q)", scope.Name, scope.Version, wantName, wantVersion)
	}

	if want := "github.com/LerianStudio/lib-streaming/v4"; wantName != want {
		t.Errorf("buildmeta scope name = %q; want the module path %q", wantName, want)
	}
}
