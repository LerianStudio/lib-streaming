//go:build unit

package producer

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestBuildTransportHeadersInjectsTraceContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("TraceIDFromHex() error = %v", err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatalf("SpanIDFromHex() error = %v", err)
	}

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))

	headers := buildTransportHeaders(ctx, Event{
		TenantID:        "tenant-1",
		ResourceType:    "transaction",
		EventType:       "created",
		EventID:         "evt-1",
		SchemaVersion:   "1.0.0",
		Timestamp:       time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC),
		Source:          "svc://test",
		DataContentType: "application/json",
	})

	values := map[string]string{}
	for _, header := range headers {
		values[header.Key] = string(header.Value)
	}

	if got, want := values["traceparent"], "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"; got != want {
		t.Fatalf("traceparent = %q; want %q", got, want)
	}
	if values["ce-id"] != "evt-1" {
		t.Fatalf("ce-id = %q; want evt-1", values["ce-id"])
	}
}
