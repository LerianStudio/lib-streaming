//go:build unit

package producer

import (
	"context"
	"sync"
	"time"

	"github.com/LerianStudio/lib-commons/v5/commons/outbox"
	"github.com/google/uuid"
)

// fakeOutboxRepo is a shared test double that implements
// outbox.OutboxRepository with in-memory recording semantics. Used by
// outbox_writer_test, metrics_test, and producer_lifecycle_test.
type fakeOutboxRepo struct {
	mu sync.Mutex

	created   []*outbox.OutboxEvent
	createErr error

	createdWithTx []*outbox.OutboxEvent
	createTxErr   error
}

func (f *fakeOutboxRepo) Create(_ context.Context, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return nil, f.createErr
	}

	f.created = append(f.created, event)

	return event, nil
}

func (f *fakeOutboxRepo) CreateWithTx(
	_ context.Context,
	_ outbox.Tx,
	event *outbox.OutboxEvent,
) (*outbox.OutboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createTxErr != nil {
		return nil, f.createTxErr
	}

	f.createdWithTx = append(f.createdWithTx, event)

	return event, nil
}

func (f *fakeOutboxRepo) ListPending(context.Context, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListPendingByType(context.Context, string, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ListTenants(context.Context) ([]string, error) { return nil, nil }

func (f *fakeOutboxRepo) GetByID(context.Context, uuid.UUID) (*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkPublished(context.Context, uuid.UUID, time.Time) error { return nil }
func (f *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string, int) error  { return nil }

func (f *fakeOutboxRepo) ListFailedForRetry(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetForRetry(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) ResetStuckProcessing(
	context.Context,
	int,
	time.Time,
	int,
) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeOutboxRepo) MarkInvalid(context.Context, uuid.UUID, string) error { return nil }

func (f *fakeOutboxRepo) createdCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.created)
}

func (f *fakeOutboxRepo) createdWithTxCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.createdWithTx)
}

func (f *fakeOutboxRepo) firstCreated() *outbox.OutboxEvent {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.created) == 0 {
		return nil
	}

	return f.created[0]
}
