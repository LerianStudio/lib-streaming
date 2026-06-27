//go:build unit

package consumer

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/LerianStudio/lib-observability/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// spyLogger records log message strings so a test can assert observability
// (e.g. a data-loss fetch error is logged, never silent). It embeds the nop
// logger for the methods a test does not care about (With/WithGroup/Enabled/Sync).
type spyLogger struct {
	log.Logger // embedded nop; only Log is overridden

	mu  sync.Mutex
	msg []string
}

func newSpyLogger() *spyLogger { return &spyLogger{Logger: log.NewNop()} }

func (s *spyLogger) Log(_ context.Context, _ log.Level, msg string, _ ...log.Field) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.msg = append(s.msg, msg)
}

func (s *spyLogger) contains(substr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range s.msg {
		if strings.Contains(m, substr) {
			return true
		}
	}

	return false
}

func (s *spyLogger) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.msg...)
}

// fakeGroupClient is a SCRIPTED GroupClient (Req 5). PollFetches returns the
// next programmed batch; once the script is exhausted it returns a synthetic
// ErrClientClosed fetch (partition -1) so Run's drainFetchErrors exits cleanly,
// exactly mirroring franz-go's real shutdown signal. Every CommitRecords /
// SetOffsets / AllowRebalance call is recorded with its arguments so a test can
// assert the exact watermark and seek-back offsets — no broker, no rejoin
// flakiness.
type fakeGroupClient struct {
	mu sync.Mutex

	script []kgo.Fetches
	pollN  int

	commits    [][]*kgo.Record
	seeks      []map[string]map[int32]kgo.EpochOffset
	cursor     map[topicPartition]int64 // per-partition re-consume floor (seek-back)
	allows     int
	pollCalls  int
	closeCalls int
}

func newFakeGroupClient(script ...kgo.Fetches) *fakeGroupClient {
	return &fakeGroupClient{script: script}
}

func (f *fakeGroupClient) PollFetches(_ context.Context) kgo.Fetches {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pollCalls++

	if f.pollN < len(f.script) {
		out := f.script[f.pollN]
		f.pollN++

		// Model franz-go's in-session consume cursor: a SetOffsets(seek-back)
		// staged on an earlier poll means the next delivery for that partition
		// starts at the seeked offset. Filter out records below the cursor so the
		// fake never re-delivers an offset the runtime has already advanced past
		// (and re-delivers a seeked-back record on the next poll).
		return f.applyCursor(out)
	}

	// Script exhausted: emit the franz-go shutdown signal (ErrClientClosed on
	// partition -1, surfaced only via Errors()/EachError).
	return clientClosedFetch()
}

// applyCursor drops records below the per-partition cursor (set by SetOffsets),
// modelling franz-go's at-most-from-cursor re-delivery after a seek-back.
func (f *fakeGroupClient) applyCursor(fetches kgo.Fetches) kgo.Fetches {
	if len(f.cursor) == 0 {
		return fetches
	}

	out := make(kgo.Fetches, 0, len(fetches))
	for _, fetch := range fetches {
		topics := make([]kgo.FetchTopic, 0, len(fetch.Topics))
		for _, ft := range fetch.Topics {
			parts := make([]kgo.FetchPartition, 0, len(ft.Partitions))
			for _, fp := range ft.Partitions {
				min, ok := f.cursor[topicPartition{ft.Topic, fp.Partition}]
				if ok {
					kept := fp.Records[:0:0]
					for _, r := range fp.Records {
						if r.Offset >= min {
							kept = append(kept, r)
						}
					}

					fp.Records = kept
				}

				parts = append(parts, fp)
			}

			topics = append(topics, kgo.FetchTopic{Topic: ft.Topic, Partitions: parts})
		}

		out = append(out, kgo.Fetch{Topics: topics})
	}

	return out
}

func (f *fakeGroupClient) CommitRecords(_ context.Context, recs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cp := make([]*kgo.Record, len(recs))
	copy(cp, recs)
	f.commits = append(f.commits, cp)

	return nil
}

func (f *fakeGroupClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.seeks = append(f.seeks, offsets)

	if f.cursor == nil {
		f.cursor = make(map[topicPartition]int64)
	}

	for topic, parts := range offsets {
		for partition, eo := range parts {
			f.cursor[topicPartition{topic, partition}] = eo.Offset
		}
	}
}

func (f *fakeGroupClient) AllowRebalance() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.allows++
}

func (f *fakeGroupClient) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closeCalls++
}

// committedWatermarks flattens every CommitRecords call into per-partition
// max(offset+1) watermarks — the value franz-go would actually commit.
func (f *fakeGroupClient) committedWatermarks() map[topicPartition]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make(map[topicPartition]int64)
	for _, batch := range f.commits {
		for _, rec := range batch {
			tp := topicPartition{topic: rec.Topic, partition: rec.Partition}
			if wm := rec.Offset + 1; wm > out[tp] {
				out[tp] = wm
			}
		}
	}

	return out
}

// fakeDLQ is a recording dlqPublisher. It captures every quarantined record and
// can be set to fail so the fail-closed (no-commit + seek-back) path is testable.
type fakeDLQ struct {
	mu sync.Mutex

	calls      []*kgo.Record
	failNext   bool
	failErr    error
	closeCalls int
	closeErr   error
}

func (d *fakeDLQ) PublishDLQ(_ context.Context, rec *kgo.Record, _ error, _ int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls = append(d.calls, rec)

	if d.failNext {
		if d.failErr == nil {
			d.failErr = contract.ErrNilProducer
		}

		return d.failErr
	}

	return nil
}

func (d *fakeDLQ) Close(_ context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.closeCalls++

	return d.closeErr
}

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.calls)
}

func (d *fakeDLQ) closeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.closeCalls
}

// fakeHandler dispatches each record to a per-call function keyed by the record
// pointer, so a test can script "offset 5 fails transiently, offset 7 succeeds".
type fakeHandler struct {
	mu     sync.Mutex
	fn     func(ctx context.Context, ev contract.Event, payload []byte) error
	calls  int
	tenant []string
}

func (h *fakeHandler) Handle(ctx context.Context, ev contract.Event, payload []byte) error {
	h.mu.Lock()
	h.calls++
	if tid, ok := ctx.Value(tenantContextKey{}).(string); ok {
		h.tenant = append(h.tenant, tid)
	} else {
		h.tenant = append(h.tenant, "")
	}
	fn := h.fn
	h.mu.Unlock()

	if fn == nil {
		return nil
	}

	return fn(ctx, ev, payload)
}

func (h *fakeHandler) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.calls
}

// --- fetch-construction helpers ---

// ceHeaders builds a minimal valid CloudEvents binary-mode header set for tests.
// tenant is emitted as ce-tenantid only when non-empty; system stamps
// ce-systemevent:"true".
func ceHeaders(tenant string, system bool) []kgo.RecordHeader {
	h := []kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("evt-1")},
		{Key: "ce-source", Value: []byte("//test/source")},
		{Key: "ce-type", Value: []byte("studio.lerian.loan.created")},
		{Key: "ce-time", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		{Key: "ce-resourcetype", Value: []byte("loan")},
		{Key: "ce-eventtype", Value: []byte("created")},
		{Key: "ce-schemaversion", Value: []byte("1.0.0")},
	}

	if tenant != "" {
		h = append(h, kgo.RecordHeader{Key: "ce-tenantid", Value: []byte(tenant)})
	}

	if system {
		h = append(h, kgo.RecordHeader{Key: "ce-systemevent", Value: []byte("true")})
	}

	return h
}

// rec builds a record at topic/partition/offset with the given CE headers.
func rec(topic string, partition int32, offset int64, headers []kgo.RecordHeader) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		LeaderEpoch: 7,
		Headers:     headers,
		Value:       []byte(`{"ok":true}`),
	}
}

// fetchOf wraps records of a single topic-partition into a kgo.Fetches.
func fetchOf(topic string, partition int32, records ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition: partition,
				Records:   records,
			}},
		}},
	}}
}

// multiVisitFetch returns a fetch where the SAME partition appears TWICE (two
// FetchPartition entries) — exercising Req 4's per-cycle halt set.
func multiVisitFetch(topic string, partition int32, first, second []*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{
				{Partition: partition, Records: first},
				{Partition: partition, Records: second},
			},
		}},
	}}
}

// errorFetch returns a fetch carrying a partition-level FETCH error (Req 6).
func errorFetch(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: topic,
			Partitions: []kgo.FetchPartition{{
				Partition: partition,
				Err:       err,
			}},
		}},
	}}
}

// clientClosedFetch mirrors franz-go's synthetic shutdown fetch: ErrClientClosed
// on partition -1, reaching the runtime only through EachError.
func clientClosedFetch() kgo.Fetches {
	return errorFetch("", -1, kgo.ErrClientClosed)
}

// newTestRuntime builds a runtime wired to the fakes, with fast backoffs so the
// in-loop retry path resolves quickly in tests.
func newTestRuntime(t testingTB, client GroupClient, handler Handler, dlq dlqPublisher, opts ...Option) *consumerRuntime {
	t.Helper()

	cfg := ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "test-group",
		Topics:              []string{"t"},
		RetryBudget:         2,
		RetryBackoffInitial: time.Millisecond,
		RetryBackoffMax:     2 * time.Millisecond,
		RetryInLoopMaxDwell: 10 * time.Millisecond,
		HaltBackoff:         time.Millisecond,
		CloseTimeout:        time.Second,
		DLQTopicSuffix:      ".dlq",
	}

	all := append([]Option{WithDLQPublisher(dlq)}, opts...)

	r, err := New(cfg, client, handler, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return r.(*consumerRuntime)
}

// testingTB is the subset of *testing.T the helpers use (keeps the import light).
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
}
