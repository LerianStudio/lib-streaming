//go:build unit

package streaming_test

import (
	"context"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
)

// noopHandler is a do-nothing Handler for builder-construction tests.
type noopHandler struct{}

func (noopHandler) Handle(context.Context, streaming.Event, []byte) error { return nil }

// TestNewConsumer_DefaultsEnabled proves the fluent builder yields a REAL runtime
// by default (finding #1): NewConsumer historically left Enabled=false (zero
// value), so every explicitly-built consumer silently became a no-op. A real
// runtime reports NOT-ready from Healthy before its first poll; the no-op always
// reports ready — the behavioral discriminator at the public surface.
func TestNewConsumer_DefaultsEnabled(t *testing.T) {
	t.Parallel()

	c, err := streaming.NewConsumer().
		Brokers("localhost:9092").
		Group("svc").
		Topics("loan.created").
		Handler(noopHandler{}).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Real runtime: not ready until the first poll completes. The no-op is always
	// ready, so a nil Healthy here would mean the builder silently produced a no-op.
	if err := c.Healthy(context.Background()); err == nil {
		t.Error("Healthy() = nil for a default-built consumer; builder produced a SILENT no-op (Enabled defaulted false)")
	}
}

// TestNewConsumer_EnabledFalseYieldsNoop proves .Enabled(false) gates the build
// down to the disabled-mode no-op (finding #1): the config-driven kill switch.
func TestNewConsumer_EnabledFalseYieldsNoop(t *testing.T) {
	t.Parallel()

	c, err := streaming.NewConsumer().
		Enabled(false).
		Brokers("localhost:9092").
		Group("svc").
		Topics("loan.created").
		Handler(noopHandler{}).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = c.Close() }()

	// No-op consumer is always ready (no group, nothing to fetch).
	if err := c.Healthy(context.Background()); err != nil {
		t.Errorf("Healthy() = %v for .Enabled(false); want nil (disabled-mode no-op)", err)
	}
}
