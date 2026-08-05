package producer

import (
	"context"
	"database/sql"
	"fmt"

	libTracing "github.com/LerianStudio/lib-observability/v2/tracing"
	"go.opentelemetry.io/otel/trace"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
)

const emitBatchSpanName = "producer.outbox.emit_batch"

// EmitBatch resolves multiple catalog requests and persists every resolved
// route as one transactional outbox batch. Request order is preserved, and
// each request's routes retain the immutable RouteTable order.
func (p *Producer) EmitBatch(ctx context.Context, requests []EmitRequest) (err error) {
	if p == nil {
		return ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, span := p.tracer.Start(ctx, emitBatchSpanName, trace.WithSpanKind(trace.SpanKindProducer))

	defer func() {
		if err != nil {
			libTracing.HandleSpanError(span, "transactional outbox batch emission failed", err)
		}

		span.End()
	}()

	if p.closed.Load() {
		return ErrEmitterClosed
	}

	if len(requests) == 0 {
		return nil
	}

	if p.outboxWriter == nil {
		return ErrOutboxNotConfigured
	}

	writer, ok := p.outboxWriter.(TransactionalBatchOutboxWriter)
	if !ok {
		return ErrOutboxTxUnsupported
	}

	tx, err := transactionalBatchTx(ctx)
	if err != nil {
		return err
	}

	envelopes, err := p.buildBatchEnvelopes(ctx, requests)
	if err != nil {
		return err
	}

	return writer.WriteBatchWithTx(ctx, tx, envelopes)
}

func (p *Producer) buildBatchEnvelopes(ctx context.Context, requests []EmitRequest) ([]OutboxEnvelope, error) {
	envelopes := make([]OutboxEnvelope, 0, len(requests))

	for requestIndex := range requests {
		requestEnvelopes, err := p.buildBatchRequestEnvelopes(ctx, requests[requestIndex], requestIndex)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, requestEnvelopes...)
	}

	return envelopes, nil
}

func (p *Producer) buildBatchRequestEnvelopes(
	ctx context.Context,
	request EmitRequest,
	requestIndex int,
) ([]OutboxEnvelope, error) {
	resolved, err := p.resolveEventAllowDisabled(request)
	if err != nil {
		return nil, fmt.Errorf("streaming: resolve outbox batch request %d: %w", requestIndex, err)
	}

	if !resolved.Policy.Enabled {
		return nil, fmt.Errorf("streaming: outbox batch request %d: %w", requestIndex, ErrEventDisabled)
	}

	if err := p.preFlightWithPayload(ctx, resolved.Event, true); err != nil {
		return nil, fmt.Errorf("streaming: validate outbox batch request %d: %w", requestIndex, err)
	}

	routes := contract.RoutesUnsafe(&p.routes, resolved.DefinitionKey)
	if len(routes) == 0 {
		return nil, fmt.Errorf("%w: definition %q", contract.ErrNoRoutesConfigured, resolved.DefinitionKey)
	}

	if err := enforceBatchRoutePayloadCap(resolved.Event, routes); err != nil {
		return nil, fmt.Errorf("streaming: validate outbox batch request %d: %w", requestIndex, err)
	}

	envelopes := make([]OutboxEnvelope, 0, len(routes))

	for routeIndex := range routes {
		routePolicy, err := applyRoutePolicy(resolved.Policy, routes[routeIndex])
		if err != nil {
			return nil, fmt.Errorf("streaming: resolve outbox batch request %d route %d: %w", requestIndex, routeIndex, err)
		}

		if !routePolicy.Enabled {
			return nil, fmt.Errorf(
				"streaming: outbox batch request %d route %q: %w",
				requestIndex,
				routes[routeIndex].Key,
				ErrEventDisabled,
			)
		}

		envelope := p.newOutboxEnvelope(ctx, resolved.Event, resolved.DefinitionKey, routes[routeIndex], routePolicy)
		if err := envelope.ValidateShape(); err != nil {
			return nil, fmt.Errorf("streaming: validate outbox batch request %d route %d: %w", requestIndex, routeIndex, err)
		}

		envelopes = append(envelopes, envelope)
	}

	return envelopes, nil
}

func transactionalBatchTx(ctx context.Context) (*sql.Tx, error) {
	raw := ctx.Value(txContextKey{})
	if raw == nil {
		return nil, fmt.Errorf("%w: WithOutboxTx is required for batch emission", ErrOutboxTxUnsupported)
	}

	tx, ok := raw.(*sql.Tx)
	if !ok || tx == nil {
		return nil, fmt.Errorf("%w: txContextKey value is %T, expected non-nil *sql.Tx", ErrOutboxTxUnsupported, raw)
	}

	return tx, nil
}

func enforceBatchRoutePayloadCap(event Event, routes []contract.RouteDefinition) error {
	for i := range routes {
		if len(event.Payload) > contract.MaxPayloadBytesForKind(routes[i].Destination.Kind) {
			return ErrPayloadTooLarge
		}
	}

	return nil
}
