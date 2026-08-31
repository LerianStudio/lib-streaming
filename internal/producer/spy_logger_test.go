//go:build unit

package producer

import (
	"context"
	"strings"
	"sync"

	"github.com/LerianStudio/lib-observability/v4/log"
)

// spyEntry is the shared shape captured by debug-style logger spies in unit
// tests. Defined here so multiple test files (producer_span_test,
// outbox_handler_test, metrics_test) share the same field set.
type spyEntry struct {
	level  int
	msg    string
	fields map[string]any
}

// spyLogger is a log.Logger test double that records every Log call. Used
// by tests that need to assert on emitted log messages without parsing
// stdout. Concurrency-safe via mu so parallel goroutines can write through
// the same instance.
type spyLogger struct {
	mu      sync.Mutex
	entries []spyEntry
}

func (s *spyLogger) Log(_ context.Context, level int, msg string, fields ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	indexed := make(map[string]any, len(fields))
	for _, f := range log.Fields(fields...) {
		indexed[f.Key] = f.Value
	}

	s.entries = append(s.entries, spyEntry{level: level, msg: msg, fields: indexed})
}

func (s *spyLogger) Enabled(_ int) bool           { return true }
func (s *spyLogger) Sync(_ context.Context) error { return nil }

// firstErrorEntry returns the first ERROR-level entry whose message
// contains the given substring. Returns nil when no such entry exists.
func (s *spyLogger) firstErrorEntry(contains string) *spyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.entries {
		entry := s.entries[i]
		if entry.level == log.LevelError && strings.Contains(entry.msg, contains) {
			return &entry
		}
	}

	return nil
}

// firstEntry returns the first entry at the given level whose message
// contains the given substring. Returns nil when no such entry exists.
func (s *spyLogger) firstEntry(level int, contains string) *spyEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.entries {
		entry := s.entries[i]
		if entry.level == level && strings.Contains(entry.msg, contains) {
			return &entry
		}
	}

	return nil
}
