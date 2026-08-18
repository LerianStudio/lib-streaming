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
		if strings.Contains(w, "outside this application's own topic names") {
			n++
		}
	}

	return n
}

// TestBuilder_WarnsOnKafkaDestinationOutsideAppTopicNames pins that the Builder
// says something when a Kafka route points away from the application's own
// topics.
//
// One topic per application is a CONVENTION on this path, not a constraint —
// the destination is a caller-supplied string and legitimate off-topic routes
// exist (mirroring, migration windows). But the same freedom silently defeats
// the Kafka ACL grant the topic collapse bought, and the failure then surfaces
// at publish time as an authorization error that reads like a broker fault.
// So: warn, never fail.
func TestBuilder_WarnsOnKafkaDestinationOutsideAppTopicNames(t *testing.T) {
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

// TestBuilder_SilentOnItsOwnTopicNames pins the other half: the three names
// inside the ACL grant — the app topic, its commands queue, and its DLQ —
// produce no warning at all, so the signal stays worth reading.
func TestBuilder_SilentOnItsOwnTopicNames(t *testing.T) {
	t.Parallel()

	appTopic, err := streaming.AppTopic("builder-test")
	if err != nil {
		t.Fatalf("AppTopic() error = %v", err)
	}

	commandsTopic, err := streaming.AppCommandsTopic("builder-test")
	if err != nil {
		t.Fatalf("AppCommandsTopic() error = %v", err)
	}

	dlqTopic, err := streaming.AppDLQTopic("builder-test")
	if err != nil {
		t.Fatalf("AppDLQTopic() error = %v", err)
	}

	for _, topic := range []string{appTopic, commandsTopic, dlqTopic} {
		t.Run(topic, func(t *testing.T) {
			t.Parallel()

			target, _ := builderKfakeTarget(t)
			spy := newWarnSpy()

			emitter, err := streaming.NewBuilder().
				Source("builder-test").
				Catalog(builderCatalog(t)).
				Routes(builderRoute(topic)).
				Target(target).
				Logger(spy).
				Build(context.Background())
			if err != nil {
				t.Fatalf("Build() error = %v", err)
			}

			t.Cleanup(func() { _ = emitter.Close() })

			if got := spy.offTopicWarnings(); got != 0 {
				t.Fatalf("off-topic warnings = %d; want 0 for %q, one of this application's own names", got, topic)
			}
		})
	}
}
