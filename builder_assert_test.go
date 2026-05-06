//go:build unit

package streaming

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// captureBuilderLogger is a minimal log.Logger that records every Log call so
// the asserter trident's log layer is observable in tests. Concurrency-safe
// because Build runs target validation sequentially but the asserter's
// internal logger calls might be invoked from a span recorder.
type captureBuilderLogger struct {
	mu      sync.Mutex
	entries []string
}

func newCaptureBuilderLogger() *captureBuilderLogger {
	return &captureBuilderLogger{}
}

func (c *captureBuilderLogger) Log(_ context.Context, _ log.Level, msg string, _ ...log.Field) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = append(c.entries, msg)
}

func (c *captureBuilderLogger) With(_ ...log.Field) log.Logger { return c }
func (c *captureBuilderLogger) WithGroup(_ string) log.Logger  { return c }
func (c *captureBuilderLogger) Enabled(_ log.Level) bool       { return true }
func (c *captureBuilderLogger) Sync(_ context.Context) error   { return nil }

func (c *captureBuilderLogger) containsMessage(needle string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, msg := range c.entries {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
}

// minimalCatalog returns a catalog with one event definition usable for
// Builder validation tests. Lightweight construction reused across the
// T1 sweep tests; package-internal so we don't rely on streaming_test
// helpers.
func minimalCatalog(t *testing.T) Catalog {
	t.Helper()

	cat, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return cat
}

func minimalRoute() RouteDefinition {
	return RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   KafkaTopic("lerian.streaming.transaction.created"),
		Requirement:   RouteRequired,
	}
}

// TestBuilder_TargetNameEmpty_FiresAssertion pins T1: empty target name must
// fire the asserter trident under operation="builder.target_name_shape" and
// still return ErrMissingTarget. Public API contract stays unchanged.
func TestBuilder_TargetNameEmpty_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureBuilderLogger()

	_, err := NewBuilder().
		Logger(cap).
		Source("svc://ledger").
		Catalog(minimalCatalog(t)).
		Routes(minimalRoute()).
		Target(TargetConfig{Name: "", Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Build(context.Background())

	if !errors.Is(err, ErrMissingTarget) {
		t.Fatalf("Build() error = %v; want errors.Is(ErrMissingTarget)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on empty target name")
	}
}

// TestBuilder_TargetNameControlChar_FiresAssertion pins T1: control-character
// target name must fire the asserter trident with violation=control_char and
// still return ErrInvalidRouteDefinition. Closes the documented log-injection
// vector as a SECURITY boundary.
func TestBuilder_TargetNameControlChar_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureBuilderLogger()

	_, err := NewBuilder().
		Logger(cap).
		Source("svc://ledger").
		Catalog(minimalCatalog(t)).
		Routes(minimalRoute()).
		Target(TargetConfig{Name: "primary\nattacker", Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Build(context.Background())

	if !errors.Is(err, ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on control-char target name")
	}
}

// TestBuilder_TargetNameOversize_FiresAssertion pins T1: oversize target name
// must fire the asserter trident with violation=oversize and still return
// ErrInvalidRouteDefinition.
func TestBuilder_TargetNameOversize_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureBuilderLogger()
	tooLong := strings.Repeat("a", contract.MaxEventIDBytes+1)

	_, err := NewBuilder().
		Logger(cap).
		Source("svc://ledger").
		Catalog(minimalCatalog(t)).
		Routes(minimalRoute()).
		Target(TargetConfig{Name: tooLong, Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Build(context.Background())

	if !errors.Is(err, ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want errors.Is(ErrInvalidRouteDefinition)", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on oversize target name")
	}
}

// TestBuilder_TargetNameCredentialLike_FiresAssertion pins T1: credential-like
// target name must fire the asserter trident under
// operation="builder.target_name_no_credential" and still return
// ErrMissingTarget. Echo of the offending name into structured fields is
// FORBIDDEN at this site — the violation kind is the operator-actionable
// signal.
func TestBuilder_TargetNameCredentialLike_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureBuilderLogger()

	_, err := NewBuilder().
		Logger(cap).
		Source("svc://ledger").
		Catalog(minimalCatalog(t)).
		Routes(minimalRoute()).
		// "password=abc" matches the credential-assignment pattern in
		// internal/contract/route.go.
		Target(TargetConfig{Name: "password=abc", Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Build(context.Background())

	if !errors.Is(err, ErrMissingTarget) {
		t.Fatalf("Build() error = %v; want errors.Is(ErrMissingTarget) for credential-like name", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on credential-like target name")
	}
}

// TestBuilder_TargetNameDuplicate_FiresAssertion pins T1: duplicate target
// name must fire the asserter trident under
// operation="builder.target_name_unique" and still return
// ErrInvalidRouteDefinition.
func TestBuilder_TargetNameDuplicate_FiresAssertion(t *testing.T) {
	t.Parallel()

	cap := newCaptureBuilderLogger()

	_, err := NewBuilder().
		Logger(cap).
		Source("svc://ledger").
		Catalog(minimalCatalog(t)).
		Routes(minimalRoute()).
		Target(TargetConfig{Name: "primary", Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Target(TargetConfig{Name: "primary", Kind: TransportKafkaLike, Brokers: []string{"localhost:9092"}}).
		Build(context.Background())

	if !errors.Is(err, ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want errors.Is(ErrInvalidRouteDefinition) for duplicate name", err)
	}

	if !cap.containsMessage("ASSERTION FAILED") {
		t.Fatal("expected asserter trident to fire on duplicate target name")
	}
}
