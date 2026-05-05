//go:build unit

package streaming_test

import (
	"errors"
	"strings"
	"testing"

	streaming "github.com/LerianStudio/lib-streaming"
)

func TestMultiEmitError_NilSafeError(t *testing.T) {
	t.Parallel()

	var err *streaming.MultiEmitError
	if got := err.Error(); got != "<nil>" {
		t.Fatalf("MultiEmitError(nil).Error() = %q; want <nil>", got)
	}
	if got := err.Unwrap(); got != nil {
		t.Fatalf("MultiEmitError(nil).Unwrap() = %v; want nil", got)
	}
	if err.HasRequiredFailures() {
		t.Fatal("MultiEmitError(nil).HasRequiredFailures() = true; want false")
	}
}

func TestRouteError_NilSafeError(t *testing.T) {
	t.Parallel()

	var err *streaming.RouteError
	if got := err.Error(); got != "<nil>" {
		t.Fatalf("RouteError(nil).Error() = %q; want <nil>", got)
	}
	if got := err.Unwrap(); got != nil {
		t.Fatalf("RouteError(nil).Unwrap() = %v; want nil", got)
	}
}

func TestMultiEmitError_UnwrapJoinsRequiredCauses(t *testing.T) {
	t.Parallel()

	cause1 := errors.New("first cause")
	cause2 := errors.New("second cause")
	multi := &streaming.MultiEmitError{
		DefinitionKey: "transaction.created",
		EventID:       "evt-1",
		TenantID:      "tenant-1",
		Required: []streaming.RouteError{
			{RouteKey: "r1", Required: true, Cause: cause1},
			{RouteKey: "r2", Required: true, Cause: cause2},
		},
	}

	if !errors.Is(multi, cause1) {
		t.Fatal("errors.Is(multi, cause1) = false; want true")
	}
	if !errors.Is(multi, cause2) {
		t.Fatal("errors.Is(multi, cause2) = false; want true")
	}

	// Optional-only causes must NOT be reachable through Unwrap.
	optionalCause := errors.New("optional cause")
	multiOpt := &streaming.MultiEmitError{
		DefinitionKey: "transaction.created",
		Optional: []streaming.RouteError{
			{RouteKey: "opt", Required: false, Cause: optionalCause},
		},
	}
	if errors.Is(multiOpt, optionalCause) {
		t.Fatal("errors.Is(multi, optionalCause) = true; want false (optional must not surface)")
	}
}

func TestMultiEmitError_ErrorIsDeterministicAndSanitized(t *testing.T) {
	t.Parallel()

	credentialedCause := errors.New("dial sasl://user:topsecret@broker:9092 failed")

	// Optional-failure marker: a unique sentinel string that must NOT
	// appear in the rendered Error() output. Optional failures are
	// best-effort by definition; surfacing their causes in the
	// aggregate error message would leak operationally-irrelevant
	// failures into Required-failure incident reports and pollute
	// dashboards/log searches with noise. Optional-route observability
	// is the metrics + span layer (see emit_multi.go:584-591), not
	// the aggregate error string.
	const optionalLeakMarker = "OPTIONAL-CAUSE-MARKER-DO-NOT-LEAK-c0ffee42"
	optionalCause := errors.New(optionalLeakMarker + ": optional broker timed out")

	multi := &streaming.MultiEmitError{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-99",
		Required: []streaming.RouteError{
			{RouteKey: "rc.kafka.b", Required: true, Cause: credentialedCause, Target: "secondary", Transport: streaming.TransportKafkaLike, Destination: "topic-b"},
			{RouteKey: "ra.kafka.a", Required: true, Cause: errors.New("plain"), Target: "primary", Transport: streaming.TransportKafkaLike, Destination: "topic-a"},
		},
		Optional: []streaming.RouteError{
			{RouteKey: "ro.kafka.shadow", Required: false, Cause: optionalCause, Target: "shadow", Transport: streaming.TransportKafkaLike, Destination: "topic-shadow"},
		},
	}

	first := multi.Error()
	second := multi.Error()
	if first != second {
		t.Fatalf("Error() not deterministic:\nfirst  = %q\nsecond = %q", first, second)
	}

	if strings.Contains(first, "topsecret") {
		t.Fatalf("Error() leaked credentials: %q", first)
	}
	if strings.Contains(first, "tenant-99") {
		t.Fatalf("Error() leaked tenant id: %q", first)
	}

	// Optional failures must NOT surface in the aggregate Error string.
	// errors.Is already filters Optional causes from the Unwrap chain
	// (see TestMultiEmitError_UnwrapJoinsRequiredCauses); the rendered
	// string must respect the same boundary.
	if strings.Contains(first, optionalLeakMarker) {
		t.Fatalf("Error() leaked Optional-route cause: marker %q present in %q", optionalLeakMarker, first)
	}
	if strings.Contains(first, "ro.kafka.shadow") {
		t.Fatalf("Error() leaked Optional-route key: %q", first)
	}

	// Routes are sorted by RouteKey: ra.kafka.a comes before rc.kafka.b.
	if idxA, idxC := strings.Index(first, "ra.kafka.a"), strings.Index(first, "rc.kafka.b"); idxA == -1 || idxC == -1 || idxA > idxC {
		t.Fatalf("Error() route ordering wrong (a=%d, c=%d): %q", idxA, idxC, first)
	}
}

func TestIsCallerError_MultiEmitErrorAggregateRules(t *testing.T) {
	t.Parallel()

	allCaller := &streaming.MultiEmitError{
		Required: []streaming.RouteError{
			{RouteKey: "r1", Required: true, Cause: streaming.ErrInvalidTenantID},
			{RouteKey: "r2", Required: true, Cause: streaming.ErrInvalidEventType},
		},
	}
	if !streaming.IsCallerError(allCaller) {
		t.Fatal("IsCallerError(allCaller) = false; want true (every required cause is caller-correctable)")
	}

	mixed := &streaming.MultiEmitError{
		Required: []streaming.RouteError{
			{RouteKey: "r1", Required: true, Cause: streaming.ErrInvalidTenantID},
			{RouteKey: "r2", Required: true, Cause: streaming.ErrCircuitOpen}, // infra-class; flips the answer
		},
	}
	if streaming.IsCallerError(mixed) {
		t.Fatal("IsCallerError(mixed) = true; want false (one infra-class failure → aggregate is infra)")
	}

	allInfra := &streaming.MultiEmitError{
		Required: []streaming.RouteError{
			{RouteKey: "r1", Required: true, Cause: streaming.ErrCircuitOpen},
			{RouteKey: "r2", Required: true, Cause: streaming.ErrEmitterClosed},
		},
	}
	if streaming.IsCallerError(allInfra) {
		t.Fatal("IsCallerError(allInfra) = true; want false")
	}

	// Empty Required + Optional-only → not caller-correctable (and never
	// surfaced; test guards the edge).
	emptyRequired := &streaming.MultiEmitError{
		Optional: []streaming.RouteError{
			{RouteKey: "o", Required: false, Cause: streaming.ErrInvalidTenantID},
		},
	}
	if streaming.IsCallerError(emptyRequired) {
		t.Fatal("IsCallerError(emptyRequired) = true; want false (optional-only is never caller-error aggregate)")
	}

	// Required without Cause → not caller-correctable (defensive: an
	// empty cause cannot be classified as caller).
	nilCause := &streaming.MultiEmitError{
		Required: []streaming.RouteError{{RouteKey: "r1", Required: true, Cause: nil}},
	}
	if streaming.IsCallerError(nilCause) {
		t.Fatal("IsCallerError(nilCause) = true; want false")
	}
}

func TestIsCallerError_NilAndPlainErrors(t *testing.T) {
	t.Parallel()

	if streaming.IsCallerError(nil) {
		t.Fatal("IsCallerError(nil) = true; want false")
	}

	if !streaming.IsCallerError(streaming.ErrInvalidTenantID) {
		t.Fatal("IsCallerError(ErrInvalidTenantID) = false; want true")
	}
	if streaming.IsCallerError(streaming.ErrCircuitOpen) {
		t.Fatal("IsCallerError(ErrCircuitOpen) = true; want false")
	}
}
