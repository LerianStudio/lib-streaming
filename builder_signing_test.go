//go:build unit

package streaming_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

const builderSigningSource = "svc-builder-signing"

func builderSigningSecret() streaming.SigningSecret {
	secret := make(streaming.SigningSecret, streaming.MinSigningSecretBytes)
	for i := range secret {
		secret[i] = byte(0x61 + i%26)
	}

	return secret
}

func builderSigningRing(t *testing.T, id, source string) *streaming.Keyring {
	t.Helper()

	ring, err := streaming.NewKeyring(streaming.SigningKey{ID: id, Source: source, Secret: builderSigningSecret()})
	require.NoError(t, err)

	return ring
}

// signingBuilder wires a single custom target backed by adapter and counts the
// factory calls, so a test can tell a build refused before any adapter work.
func signingBuilder(t *testing.T, adapter *fake.Adapter, factoryCalls *atomic.Int32) *streaming.Builder {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	require.NoError(t, err)

	return streaming.NewBuilder().
		Source(builderSigningSource).
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.custom.sink",
			DefinitionKey: "transaction.created",
			Target:        "sink",
			Destination:   streaming.Destination{Kind: streaming.TransportCustom, Name: "custom-sink"},
			Requirement:   streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{Name: "sink", Kind: streaming.TransportCustom}).
		RegisterTransport(streaming.TransportCustom, func(context.Context, streaming.TransportAdapterOptions) (streaming.TransportAdapter, error) {
			factoryCalls.Add(1)

			return adapter, nil
		})
}

func emitOne(t *testing.T, emitter streaming.Emitter) {
	t.Helper()

	require.NoError(t, emitter.Emit(context.Background(), streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       []byte(`{"amount":100}`),
	}))
}

func verifyFakeMessage(t *testing.T, ring *streaming.Keyring, adapter *fake.Adapter) error {
	t.Helper()

	messages := adapter.Messages()
	require.Len(t, messages, 1)

	headers := make([]kgo.RecordHeader, len(messages[0].Headers))
	for i, h := range messages[0].Headers {
		headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}

	verifier, err := envelopesig.NewVerifier(ring, 0)
	require.NoError(t, err)

	return verifier.Verify(headers, messages[0].Payload)
}

func hasSignatureHeader(adapter *fake.Adapter) bool {
	for _, m := range adapter.Messages() {
		for _, h := range m.Headers {
			if h.Key == streaming.CloudEventsHeaderSignature {
				return true
			}
		}
	}

	return false
}

func TestBuilder_SignEnvelopesSignsEveryPublish(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)
	ring := builderSigningRing(t, "k1", builderSigningSource)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).SignEnvelopes(ring, "k1").Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	require.NoError(t, verifyFakeMessage(t, ring, adapter))
}

func TestBuilder_WithoutSigningPublishesUnsigned(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	require.False(t, hasSignatureHeader(adapter), "an unconfigured producer must not sign")
}

func TestBuilder_SignEnvelopesNilRingIsDeferredError(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewBuilder().SignEnvelopes(nil, "k1").Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSigningKey) {
		t.Fatalf("Build() error = %v; want ErrInvalidSigningKey", err)
	}
}

func TestBuilder_SignEnvelopesEmptyKeyIDIsDeferredError(t *testing.T) {
	t.Parallel()

	ring := builderSigningRing(t, "k1", builderSigningSource)

	_, err := streaming.NewBuilder().SignEnvelopes(ring, "").Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSigningKey) {
		t.Fatalf("Build() error = %v; want ErrInvalidSigningKey", err)
	}
}

// TestBuilder_SignEnvelopesForeignKeyFailsBeforeAdapters proves a key bound to
// another source fails the build before any transport adapter is built.
func TestBuilder_SignEnvelopesForeignKeyFailsBeforeAdapters(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)
	ring := builderSigningRing(t, "k1", "another-service")

	var calls atomic.Int32

	_, err := signingBuilder(t, adapter, &calls).SignEnvelopes(ring, "k1").Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSigningKey) {
		t.Fatalf("Build() error = %v; want ErrInvalidSigningKey", err)
	}

	require.Zero(t, calls.Load(), "no adapter may be built for a build refused on its signing key")
}

func TestBuilder_SigningFromConfigNoopWhenUnset(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).
		SigningFromConfig(streaming.Config{CloudEventsSource: builderSigningSource}).
		Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	require.False(t, hasSignatureHeader(adapter), "SigningFromConfig with no key id must not sign")
}

func TestBuilder_SigningFromConfigSigns(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).
		SigningFromConfig(streaming.Config{
			CloudEventsSource: builderSigningSource,
			SigningKeyID:      "k1",
			SigningKey:        builderSigningSecret(),
		}).
		Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	require.NoError(t, verifyFakeMessage(t, builderSigningRing(t, "k1", builderSigningSource), adapter))
}

func TestBuilder_SigningFromConfigSourceMismatchFailsBuild(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)

	var calls atomic.Int32

	_, err := signingBuilder(t, adapter, &calls).
		SigningFromConfig(streaming.Config{
			CloudEventsSource: "another-service",
			SigningKeyID:      "k1",
			SigningKey:        builderSigningSecret(),
		}).
		Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSigningKey) {
		t.Fatalf("Build() error = %v; want ErrInvalidSigningKey", err)
	}
}

func TestBuilder_SigningFromConfigShortKeyIsDeferredError(t *testing.T) {
	t.Parallel()

	_, err := streaming.NewBuilder().
		SigningFromConfig(streaming.Config{
			CloudEventsSource: builderSigningSource,
			SigningKeyID:      "k1",
			SigningKey:        builderSigningSecret()[:16],
		}).
		Build(context.Background())
	if !errors.Is(err, streaming.ErrInvalidSigningKey) {
		t.Fatalf("Build() error = %v; want ErrInvalidSigningKey", err)
	}
}

func TestWithEnvelopeSigningOptionSigns(t *testing.T) {
	t.Parallel()

	adapter := fake.NewAdapter(streaming.TransportCustom)
	ring := builderSigningRing(t, "k1", builderSigningSource)

	var calls atomic.Int32

	emitter, err := signingBuilder(t, adapter, &calls).
		Options(streaming.WithEnvelopeSigning(ring, "k1")).
		Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emitter.Close() })

	emitOne(t, emitter)

	require.NoError(t, verifyFakeMessage(t, ring, adapter))
}
