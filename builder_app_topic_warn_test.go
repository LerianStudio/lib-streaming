//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"
	streaming "github.com/LerianStudio/lib-streaming/v4"
)

// warnSpyLogger records Warn-level messages so a test can assert the build
// actually said something.
type warnSpyLogger struct {
	log.Logger

	mu    sync.Mutex
	warns []string
}

func newWarnSpy() *warnSpyLogger { return &warnSpyLogger{Logger: log.NewNop()} }

func (l *warnSpyLogger) Log(_ context.Context, level int, msg string, _ ...any) {
	if level != log.LevelWarn {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, msg)
}

func (l *warnSpyLogger) Enabled(int) bool           { return true }
func (l *warnSpyLogger) Sync(context.Context) error { return nil }

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

// TestBuilder_SilentOnItsOwnTopicNames pins the other half: the names inside
// the ACL grant a caller may legitimately route to — the app topic and its DLQ
// — produce no warning at all, so the signal stays worth reading.
//
// The third name in the grant, the commands queue, is NOT here: a route may
// not name it at all. See TestBuilder_RefusesARouteNamingItsCommandsQueue.
func TestBuilder_SilentOnItsOwnTopicNames(t *testing.T) {
	t.Parallel()

	appTopic, err := streaming.AppTopic("builder-test")
	if err != nil {
		t.Fatalf("AppTopic() error = %v", err)
	}

	dlqTopic, err := streaming.AppDLQTopic("builder-test")
	if err != nil {
		t.Fatalf("AppDLQTopic() error = %v", err)
	}

	// One build per name: two catch-all routes to one target would be
	// rejected as a duplicate before the warning logic ever ran.
	for _, topic := range []string{appTopic, dlqTopic} {
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

// TestBuilder_RefusesARouteNamingItsCommandsQueue pins the public half of the
// construction guard: the commands queue is reachable ONLY through
// Class: ClassCommand on an event definition, never by naming it in a route.
//
// A warning would not do here, unlike an off-topic route. Naming the queue by
// hand is not a legitimate-but-unusual choice: a command routed there derives
// a ".commands.dlq" nothing provisions, so its failed-publish quarantine copy
// silently never lands; a fact routed there gets quarantined by consumers for
// keys they were always entitled to ignore. Both are invisible until
// production, so Build refuses instead of logging.
func TestBuilder_RefusesARouteNamingItsCommandsQueue(t *testing.T) {
	t.Parallel()

	commandsTopic, err := streaming.AppCommandsTopic("builder-test")
	if err != nil {
		t.Fatalf("AppCommandsTopic() error = %v", err)
	}

	target, _ := builderKfakeTarget(t)

	emitter, err := streaming.NewBuilder().
		Source("builder-test").
		Catalog(builderCatalog(t)).
		Routes(builderRoute(commandsTopic)).
		Target(target).
		Logger(log.NewNop()).
		Build(context.Background())
	if emitter != nil {
		t.Cleanup(func() { _ = emitter.Close() })
	}

	if !errors.Is(err, streaming.ErrInvalidRouteDefinition) {
		t.Fatalf("Build() error = %v; want ErrInvalidRouteDefinition", err)
	}

	if !strings.Contains(err.Error(), "ClassCommand") {
		t.Errorf("error %q does not tell the caller to use Class: ClassCommand instead", err)
	}
}
