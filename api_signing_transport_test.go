//go:build unit

package streaming_test

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v7/commons/outbox"
	streaming "github.com/LerianStudio/lib-streaming/v4"
)

const transportSigningSource = "svc-amqp-signing"

var transportSigningBody = []byte(`{"amount":"10.00"}`)

// amqpHeadersFor builds the table an AMQP publisher outside the library would
// send: the codec's ce-* headers as []byte plus a header of its own.
func amqpHeadersFor(source string) map[string]any {
	headers := map[string]any{"x-app-header": "kept"}
	for _, h := range streaming.BuildCloudEventsHeaders(streaming.Event{
		EventID:       "0190a8e2-0000-7000-8000-000000000001",
		TenantID:      "tenant-1",
		Source:        source,
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
		Timestamp:     time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
	}) {
		headers[h.Key] = h.Value
	}

	return headers
}

func transportSigner(t *testing.T) (*streaming.Signer, *streaming.Keyring) {
	t.Helper()

	ring := builderSigningRing(t, "k1", transportSigningSource)

	signer, err := streaming.NewSigner(ring, "k1", transportSigningSource)
	require.NoError(t, err)

	return signer, ring
}

func transportVerifier(t *testing.T, ring *streaming.Keyring) *streaming.Verifier {
	t.Helper()

	verifier, err := streaming.NewVerifier(ring, 0)
	require.NoError(t, err)

	return verifier
}

func TestSigner_SignThenVerifyOverAMQPHeaders(t *testing.T) {
	t.Parallel()

	signer, ring := transportSigner(t)
	input := amqpHeadersFor(transportSigningSource)
	snapshot := maps.Clone(input)

	signed, err := signer.Sign(input, transportSigningBody)
	require.NoError(t, err)
	assert.Equal(t, snapshot, input, "Sign never modifies its input")
	assert.Equal(t, "kept", signed["x-app-header"])

	for _, key := range []string{
		streaming.CloudEventsHeaderSignatureKeyID,
		streaming.CloudEventsHeaderSignedAt,
		streaming.CloudEventsHeaderSignature,
	} {
		assert.IsType(t, []byte(nil), signed[key], key)
	}

	verifier := transportVerifier(t, ring)
	require.NoError(t, verifier.Verify(signed, transportSigningBody))

	require.ErrorIs(t, verifier.Verify(signed, []byte(`{"amount":"99.00"}`)), streaming.ErrSignatureInvalid)
	require.ErrorIs(t, verifier.Verify(input, transportSigningBody), streaming.ErrSignatureMissing)
}

func TestSigner_RefusesRecordNotOfItsSource(t *testing.T) {
	t.Parallel()

	signer, _ := transportSigner(t)

	foreign := amqpHeadersFor("svc-someone-else")
	missing := amqpHeadersFor(transportSigningSource)
	delete(missing, "ce-source")

	for name, headers := range map[string]map[string]any{"foreign ce-source": foreign, "missing ce-source": missing} {
		out, err := signer.Sign(headers, transportSigningBody)
		require.ErrorIs(t, err, streaming.ErrSigningSourceMismatch, name)
		assert.Nil(t, out, name)
	}
}

func TestNewSigner_Refusals(t *testing.T) {
	t.Parallel()

	ring := builderSigningRing(t, "k1", transportSigningSource)

	cases := map[string]struct {
		ring     *streaming.Keyring
		activeID string
		source   string
	}{
		"nil ring":              {ring: nil, activeID: "k1", source: transportSigningSource},
		"empty key id":          {ring: ring, activeID: "", source: transportSigningSource},
		"unknown key id":        {ring: ring, activeID: "k2", source: transportSigningSource},
		"key of another source": {ring: ring, activeID: "k1", source: "svc-someone-else"},
	}

	for name, tc := range cases {
		signer, err := streaming.NewSigner(tc.ring, tc.activeID, tc.source)
		require.ErrorIs(t, err, streaming.ErrInvalidSigningKey, name)
		assert.Nil(t, signer, name)
	}
}

func TestNewVerifier_Refusals(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewVerifier(nil, 0)
	require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)

	_, err = streaming.NewVerifier(builderSigningRing(t, "k1", transportSigningSource), -time.Second)
	require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
}

func TestSignerAndVerifier_NilFailClosed(t *testing.T) {
	t.Parallel()

	signed, err := (*streaming.Signer)(nil).Sign(amqpHeadersFor(transportSigningSource), transportSigningBody)
	require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
	assert.Nil(t, signed)

	signed, err = (&streaming.Signer{}).Sign(amqpHeadersFor(transportSigningSource), transportSigningBody)
	require.ErrorIs(t, err, streaming.ErrInvalidSigningKey)
	assert.Nil(t, signed)

	require.ErrorIs(t, (*streaming.Verifier)(nil).Verify(map[string]any{}, nil), streaming.ErrSignatureInvalid)
	require.ErrorIs(t, (&streaming.Verifier{}).Verify(map[string]any{}, nil), streaming.ErrSignatureInvalid)
}

func TestSignerAndVerifier_NeverRenderSecret(t *testing.T) {
	t.Parallel()

	signer, ring := transportSigner(t)
	verifier := transportVerifier(t, ring)
	secret := string(builderSigningSecret())

	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		assert.NotContains(t, fmt.Sprintf(verb, signer), secret, verb)
		assert.NotContains(t, fmt.Sprintf(verb, verifier), secret, verb)
	}
}

// capturingRabbitMQPublisher records what the library's RabbitMQ adapter
// hands the caller's AMQP client.
type capturingRabbitMQPublisher struct {
	mu      sync.Mutex
	headers []map[string]any
	bodies  [][]byte
}

func (p *capturingRabbitMQPublisher) Publish(_ context.Context, _, _, _ string, body []byte, headers map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.headers = append(p.headers, headers)
	p.bodies = append(p.bodies, body)

	return nil
}

func (*capturingRabbitMQPublisher) Ping(context.Context) error { return nil }

func (p *capturingRabbitMQPublisher) only(t *testing.T) (map[string]any, []byte) {
	t.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	require.Len(t, p.headers, 1)

	return p.headers[0], p.bodies[0]
}

// memoryOutboxRepo keeps the rows the producer persists, so a test can hand
// one to the relay.
type memoryOutboxRepo struct {
	mu   sync.Mutex
	rows []*outbox.OutboxEvent
}

func (r *memoryOutboxRepo) Create(_ context.Context, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rows = append(r.rows, event)

	return event, nil
}

func (r *memoryOutboxRepo) CreateWithTx(ctx context.Context, _ outbox.Tx, event *outbox.OutboxEvent) (*outbox.OutboxEvent, error) {
	return r.Create(ctx, event)
}

func (r *memoryOutboxRepo) CreateManyWithTx(_ context.Context, _ outbox.Tx, events []*outbox.OutboxEvent) ([]*outbox.OutboxEvent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rows = append(r.rows, events...)

	return events, nil
}

func (*memoryOutboxRepo) ListPending(context.Context, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*memoryOutboxRepo) ListPendingByType(context.Context, string, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}
func (*memoryOutboxRepo) ListTenants(context.Context) ([]string, error) { return nil, nil }
func (*memoryOutboxRepo) GetByID(context.Context, uuid.UUID) (*outbox.OutboxEvent, error) {
	return nil, nil
}
func (*memoryOutboxRepo) MarkPublished(context.Context, uuid.UUID, time.Time) error { return nil }
func (*memoryOutboxRepo) MarkFailed(context.Context, uuid.UUID, string, int) error  { return nil }
func (*memoryOutboxRepo) MarkInvalid(context.Context, uuid.UUID, string) error      { return nil }
func (*memoryOutboxRepo) ListFailedForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*memoryOutboxRepo) ResetForRetry(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

func (*memoryOutboxRepo) ResetStuckProcessing(context.Context, int, time.Time, int) ([]*outbox.OutboxEvent, error) {
	return nil, nil
}

// signingRabbitMQProducer is a signing producer whose only route is a
// RabbitMQ exchange, optionally with an outbox.
func signingRabbitMQProducer(t *testing.T, ring *streaming.Keyring, publisher streaming.RabbitMQPublisher, repo outbox.OutboxRepository) *streaming.Producer {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	require.NoError(t, err)

	builder := streaming.NewBuilder().
		Source(transportSigningSource).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.rabbitmq.bus",
			DefinitionKey: "transaction.created",
			Target:        "rabbitmq-bus",
			Destination:   streaming.RabbitMQRoute("events", "tx.created"),
			Requirement:   streaming.RouteRequired,
		}).
		RabbitMQTarget("rabbitmq-bus", publisher).
		SignEnvelopes(ring, "k1")

	if repo != nil {
		builder = builder.OutboxRepository(repo)
	}

	emitter, err := builder.Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	producer, ok := emitter.(*streaming.Producer)
	require.True(t, ok, "Build returned %T", emitter)

	return producer
}

// assertVerifiesOverAMQP checks what reached the AMQP client verifies with
// the transport-agnostic Verifier, and that a changed signed header does not.
func assertVerifiesOverAMQP(t *testing.T, ring *streaming.Keyring, headers map[string]any, body []byte) {
	t.Helper()

	verifier := transportVerifier(t, ring)
	require.NoError(t, verifier.Verify(headers, body))

	// The adapter's own additions sit outside the signature.
	assert.Equal(t, "tenant-1", headers["X-Tenant-ID"])

	tampered := maps.Clone(headers)
	tampered["ce-tenantid"] = []byte("tenant-2")
	require.ErrorIs(t, verifier.Verify(tampered, body), streaming.ErrSignatureInvalid)
}

func TestRabbitMQTarget_DirectPublishIsSigned(t *testing.T) {
	t.Parallel()

	ring := builderSigningRing(t, "k1", transportSigningSource)
	publisher := &capturingRabbitMQPublisher{}
	producer := signingRabbitMQProducer(t, ring, publisher, nil)

	require.NoError(t, producer.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       transportSigningBody,
	}))

	headers, body := publisher.only(t)
	assertVerifiesOverAMQP(t, ring, headers, body)
}

func TestRabbitMQTarget_OutboxRelayIsSigned(t *testing.T) {
	t.Parallel()

	ring := builderSigningRing(t, "k1", transportSigningSource)
	publisher := &capturingRabbitMQPublisher{}
	repo := &memoryOutboxRepo{}
	producer := signingRabbitMQProducer(t, ring, publisher, repo)

	require.NoError(t, producer.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey:  "transaction.created",
		TenantID:       "tenant-1",
		Payload:        transportSigningBody,
		PolicyOverride: streaming.DeliveryPolicyOverride{Direct: streaming.DirectModeSkip, Outbox: streaming.OutboxModeAlways},
	}))

	repo.mu.Lock()
	require.Len(t, repo.rows, 1)
	row := repo.rows[0]
	repo.mu.Unlock()

	registry := outbox.NewHandlerRegistry()
	require.NoError(t, producer.RegisterOutboxRelay(registry))
	require.NoError(t, registry.Handle(context.Background(), row))

	headers, body := publisher.only(t)
	assertVerifiesOverAMQP(t, ring, headers, body)
}

func TestSigner_RefusesUnsupportedSignedValue(t *testing.T) {
	t.Parallel()

	signer, _ := transportSigner(t)
	headers := amqpHeadersFor(transportSigningSource)
	headers["ce-time"] = time.Now()

	out, err := signer.Sign(headers, transportSigningBody)
	require.ErrorIs(t, err, streaming.ErrUnsupportedHeaderValue)
	assert.True(t, streaming.IsCallerError(err))
	assert.Nil(t, out)
}
