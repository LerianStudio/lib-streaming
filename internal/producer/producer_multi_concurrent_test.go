//go:build unit

package producer

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport/fake"
)

// TestProducerMulti_ConcurrentEmitDuringClose stresses the multi-target
// Emit/Close concurrency contract: many goroutines emit while one
// goroutine closes after a small jitter. The contract is:
//
//   - No panic from Emit or Close.
//   - No data race (run with -race).
//   - All worker goroutines exit cleanly (sync.WaitGroup join).
//   - Every Emit call returns either nil or a documented sentinel
//     (we accept both — this is a stress test, not a correctness test
//     for a specific outcome).
//
// The test uses 3 fake adapters as targets. Required-route routes ensure
// every Emit fans out across all three targets.
//
// goleak runs at PACKAGE scope via TestMain (see main_test.go in this
// package) — every test in internal/producer runs under
// goleak.VerifyTestMain, so any goroutine this test leaks WILL surface as
// a suite failure after the last test exits. Adding an in-test goleak.Find
// here would double-count the test runner's own goroutines under -parallel
// and produce noisy false-positives. The WaitGroup join below plus the
// TestMain detector together cover the cases this test is designed to
// surface (Producer-owned goroutines that escape Close).
func TestProducerMulti_ConcurrentEmitDuringClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	primary := fake.NewAdapter(TransportKafkaLike)
	secondary := fake.NewAdapter(TransportKafkaLike)
	tertiary := fake.NewAdapter(TransportKafkaLike)

	catalog := sampleCatalog(t)
	routes := mustMultiRouteTable(t,
		multiTestRoute("transaction.created.kafka.primary", "transaction.created", "primary", "lerian.streaming.transaction.created", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.secondary", "transaction.created", "secondary", "lerian.streaming.transaction.created.replica", contract.RouteRequired),
		multiTestRoute("transaction.created.kafka.tertiary", "transaction.created", "tertiary", "lerian.streaming.transaction.created.tertiary", contract.RouteRequired),
	)

	p, err := NewProducerMulti(
		ctx,
		MultiProducerConfig{Source: "svc://concurrent-test"},
		nil,
		[]TargetSpec{
			{Name: "primary", Kind: TransportKafkaLike, Adapter: primary},
			{Name: "secondary", Kind: TransportKafkaLike, Adapter: secondary},
			{Name: "tertiary", Kind: TransportKafkaLike, Adapter: tertiary},
		},
		routes,
		catalog,
		WithLogger(log.NewNop()),
		WithCatalog(catalog),
	)
	if err != nil {
		t.Fatalf("NewProducerMulti() error = %v", err)
	}

	const (
		numEmitters    = 16
		emitsPerWorker = 50
	)

	var (
		wg          sync.WaitGroup
		closed      atomic.Bool
		emitErrors  atomic.Int64
		emitsAfterC atomic.Int64
	)

	wg.Add(numEmitters)

	for i := 0; i < numEmitters; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < emitsPerWorker; j++ {
				emitErr := p.Emit(ctx, eventToRequest(sampleEvent()))
				if closed.Load() {
					emitsAfterC.Add(1)
				}

				if emitErr == nil {
					continue
				}

				// Accept any documented sentinel — Close races mean
				// some Emits will land after Close starts and return
				// ErrEmitterClosed; that is correct.
				if errors.Is(emitErr, ErrEmitterClosed) ||
					errors.Is(emitErr, ErrCircuitOpen) {
					continue
				}

				// Aggregated multi-route error is also acceptable
				// when one or more targets transitioned mid-flight.
				var multi *contract.MultiEmitError
				if errors.As(emitErr, &multi) {
					continue
				}

				// Any other error shape is a regression — record it
				// for the assertion at the end.
				emitErrors.Add(1)
			}
		}()
	}

	// Random jitter before close so Emit and Close interleave at varying
	// points in the workload. Bounded so the test does not grow long.
	//
	//nolint:gosec // test jitter, not security-sensitive RNG
	jitter := time.Duration(rand.IntN(20)) * time.Millisecond
	time.Sleep(jitter)

	closed.Store(true)
	if err := p.Close(); err != nil {
		t.Errorf("Close() error = %v; want nil", err)
	}

	wg.Wait()

	if got := emitErrors.Load(); got != 0 {
		t.Errorf("emit unexpected error count = %d; want 0 (only ErrEmitterClosed/ErrCircuitOpen/MultiEmitError tolerated)", got)
	}

	// Sanity: at least some emits happened before Close. If every emit
	// hit the post-close branch we did not exercise the concurrent path.
	totalEmits := int64(numEmitters * emitsPerWorker)
	if emitsAfterC.Load() == totalEmits {
		t.Logf("warning: every Emit observed the closed flag; jitter may have been too short")
	}
}
