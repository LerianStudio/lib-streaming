//go:build unit

package producer

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs the package-level goleak detector. Every test in the
// internal/producer package runs under this gate; any goroutine that
// outlives the suite's last test surfaces here as a failure.
//
// The ignore set is the union of:
//
//   - testing framework leftovers that persist by design across tests
//     (testing.(*T).Run, runtime.goexit) — these are the standard goleak
//     ignores recommended for any TestMain-scoped detector.
//
//   - kfake cluster background workers — kfake.NewCluster spawns a
//     persistent listen socket plus per-connection handlers and a cluster
//     coordinator. The cluster's t.Cleanup runs AFTER the package's last
//     test exits, by which time TestMain has already inspected the
//     goroutine set. Without these ignores the suite would fail-trap the
//     last-emitted-test on otherwise-correct shutdown.
//
//   - internal/poll.runtime_pollWait — kfake's listener socket parks here
//     until cluster.Close. Same lifecycle window as the kfake ignores.
//
// Mirrors the in-test ignore set documented at
// internal/producer/producer_app_test.go:64-79. Keeping the two lists in
// sync is intentional: a leak that escapes one site escapes the other.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Standard testing framework persistence.
		goleak.IgnoreTopFunction("testing.(*T).Run"),
		goleak.IgnoreTopFunction("runtime.goexit"),
		// kfake cluster goroutines whose lifetime is bound by t.Cleanup,
		// which fires AFTER the suite's last test. See header doc above.
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).run"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).listen"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*broker).handleConn"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*broker).serve"),
		// kfake listen socket parks here until cluster.Close.
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		// kgo.Client background workers. These spawn from every
		// kgo.NewClient (Producer-internal AND test-side consumers) and
		// exit only after the client's parent context cancels through
		// Client.Close(). Even with t.Cleanup-registered Close() calls,
		// the goroutines may park briefly past test exit before observing
		// the context cancellation; goleak.VerifyTestMain inspects the
		// goroutine set immediately after the last test completes, so
		// these are racing-but-correct stragglers. Tolerating them at the
		// suite level keeps the detector focused on PRODUCER-OWNED
		// goroutines (the leak class we actually need to catch).
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).pushMetrics"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).updateMetadataLoop"),
		goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kgo.(*Client).reapConnectionsLoop"),
	)
}
