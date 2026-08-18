//go:build unit

package streaming_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v2/log"
	streaming "github.com/LerianStudio/lib-streaming/v3"
)

// warnSpyLogger records Warn-level messages so a test can assert the build
// actually said something.
type warnSpyLogger struct {
	log.Logger

	mu    sync.Mutex
	warns []string
}

func newWarnSpy() *warnSpyLogger { return &warnSpyLogger{Logger: log.NewNop()} }

func (l *warnSpyLogger) Log(_ context.Context, level log.Level, msg string, _ ...log.Field) {
	if level != log.LevelWarn {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, msg)
}

func (l *warnSpyLogger) With(...log.Field) log.Logger { return l }
func (l *warnSpyLogger) WithGroup(string) log.Logger  { return l }
func (l *warnSpyLogger) Enabled(log.Level) bool       { return true }
func (l *warnSpyLogger) Sync(context.Context) error   { return nil }

func (l *warnSpyLogger) offTopicWarnings() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0

	for _, w := range l.warns {
		if strings.Contains(w, "outside this application's topic pair") {
			n++
		}
	}

	return n
}

// TestBuilder_WarnsOnKafkaDestinationOutsideAppTopicPair pins that the Builder
// says something when a Kafka route points away from the application's own
// topic.
//
// One topic per application is a CONVENTION on this path, not a constraint —
// the destination is a caller-supplied string and legitimate off-topic routes
// exist (mirroring, migration windows). But the same freedom silently defeats
// the two-name Kafka ACL grant the topic collapse bought, and the failure then
// surfaces at publish time as an authorization error that reads like a broker
// fault. So: warn, never fail.
func TestBuilder_WarnsOnKafkaDestinationOutsideAppTopicPair(t *testing.T) {
	t.Parallel()

	target, _ := builderKfakeTarget(t)
	spy := newWarnSpy()

	emitter, err := streaming.NewBuilder().
		Source("builder-test").
		Catalog(builderCatalog(t)).
		Routes(builderRoute("some.other.teams.topic")).
		Target(target).
		Logger(spy).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v; want nil (off-topic routes warn, never fail)", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	if got := spy.offTopicWarnings(); got != 1 {
		t.Fatalf("off-topic warnings = %d; want exactly 1", got)
	}
}

// TestBuilder_SilentOnAppTopicPair pins the other half: the two names inside
// the ACL grant — the app topic and its DLQ — produce no warning at all, so the
// signal stays worth reading.
func TestBuilder_SilentOnAppTopicPair(t *testing.T) {
	t.Parallel()

	target, _ := builderKfakeTarget(t)
	spy := newWarnSpy()

	appTopic, err := streaming.AppTopic("builder-test")
	if err != nil {
		t.Fatalf("AppTopic() error = %v", err)
	}

	emitter, err := streaming.NewBuilder().
		Source("builder-test").
		Catalog(builderCatalog(t)).
		Routes(builderRoute(appTopic)).
		Target(target).
		Logger(spy).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	if got := spy.offTopicWarnings(); got != 0 {
		t.Fatalf("off-topic warnings = %d; want 0 for the app topic itself", got)
	}
}
