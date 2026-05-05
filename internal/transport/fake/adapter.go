//go:build unit

package fake

import (
	"context"
	"errors"
	"sync"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// Adapter is a concurrency-safe in-memory transport adapter for unit tests.
type Adapter struct {
	mu         sync.RWMutex
	kind       contract.TransportKind
	messages   []transport.TransportMessage
	publishErr error
	healthErr  error
	flushErr   error
	closeErr   error
	closed     bool
}

// NewAdapter returns a fake adapter that reports kind from Kind.
func NewAdapter(kind contract.TransportKind) *Adapter {
	return &Adapter{kind: kind}
}

// Kind returns the configured transport kind.
func (a *Adapter) Kind() contract.TransportKind {
	if a == nil {
		return ""
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.kind
}

// SetPublishError configures the error returned by Publish.
func (a *Adapter) SetPublishError(err error) {
	if a == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.publishErr = err
}

// SetHealthError configures the error returned by Healthy.
func (a *Adapter) SetHealthError(err error) {
	if a == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.healthErr = err
}

// SetFlushError configures the error returned by Flush.
func (a *Adapter) SetFlushError(err error) {
	if a == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.flushErr = err
}

// SetCloseError configures the error returned by Close.
func (a *Adapter) SetCloseError(err error) {
	if a == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.closeErr = err
}

// Publish records a deep copy of message unless a publish error is configured.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	if a == nil {
		return contract.ErrNilProducer
	}

	if err := ctxErr(ctx); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return contract.ErrEmitterClosed
	}

	if a.publishErr != nil {
		return a.publishErr
	}

	a.messages = append(a.messages, transport.CloneMessage(message))

	return nil
}

// Healthy returns the configured health error, if any.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil {
		return contract.ErrNilProducer
	}

	if err := ctxErr(ctx); err != nil {
		return err
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.healthErr
}

// Flush returns the configured flush error, if any.
func (a *Adapter) Flush(ctx context.Context) error {
	if a == nil {
		return contract.ErrNilProducer
	}

	if err := ctxErr(ctx); err != nil {
		return err
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.flushErr
}

// Close marks the adapter closed and returns the configured close error, if any.
func (a *Adapter) Close(ctx context.Context) error {
	if a == nil {
		return contract.ErrNilProducer
	}

	if err := ctxErr(ctx); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.closed = true

	return a.closeErr
}

// Classify maps common fake adapter errors to streaming error classes.
func (a *Adapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ClassContextCanceled
	}

	if contract.IsCallerError(err) {
		return contract.ClassValidation
	}

	return contract.ClassBrokerUnavailable
}

// Messages returns deep copies of all successfully published messages.
func (a *Adapter) Messages() []transport.TransportMessage {
	if a == nil {
		return []transport.TransportMessage{}
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.messages) == 0 {
		return []transport.TransportMessage{}
	}

	messages := make([]transport.TransportMessage, len(a.messages))
	for i, message := range a.messages {
		messages[i] = transport.CloneMessage(message)
	}

	return messages
}

// Closed reports whether Close has been called.
func (a *Adapter) Closed() bool {
	if a == nil {
		return false
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.closed
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
