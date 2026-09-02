//go:build integration

// TLS integration test for the environment-driven Builder.TLSFromConfig path.
//
// Spins up a Redpanda broker served with a certificate signed by a PRIVATE
// self-signed CA, then drives the public env-var flow end-to-end:
//
//	STREAMING_TLS_ENABLED=true + STREAMING_TLS_CA_CERT=<base64 PEM CA>
//	  -> config.LoadConfig()
//	  -> streaming.NewBuilder().....TLSFromConfig(cfg).Build(ctx)
//	  -> Emit + consume one CloudEvents record over TLS.
//
// The CA never contains a private key on the client side: only the base64 PEM
// public certificate is handed to STREAMING_TLS_CA_CERT, exactly as a real
// deployment would. Build tag: integration. Requires Docker.
package streaming_test

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mdelapenya/tlscert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/config"
)

const (
	tlsRedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.18"
	tlsIntegSource   = "lerian-test-streaming-tls-integration"
)

// tlsIntegTopic is the app topic derived from tlsIntegSource, so the suite
// exercises the v3 one-topic-per-application contract instead of a
// hand-written per-event name.
var tlsIntegTopic = func() string {
	topic, err := streaming.AppTopic(tlsIntegSource)
	if err != nil {
		panic(err)
	}

	return topic
}()

// skipIfNoDockerTLS converts a testcontainers startup error into t.Skip when
// Docker is unavailable, mirroring the internal producer suite's heuristic so
// non-Docker CI still reports green.
func skipIfNoDockerTLS(t *testing.T, err error) bool {
	t.Helper()

	if err == nil {
		return false
	}

	msg := err.Error()
	for _, needle := range []string{
		"Cannot connect to the Docker daemon",
		"docker socket",
		"Is the docker daemon running",
		"provider not implemented",
		"docker.sock",
	} {
		if strings.Contains(msg, needle) {
			t.Skipf("Docker not available in this environment: %v", err)
			return true
		}
	}

	return false
}

func TestBuilder_TLSFromConfig_ProduceConsumeOverTLS(t *testing.T) {
	ctx := context.Background()

	// Resolve the Docker daemon host so the server certificate's SAN set
	// covers whatever host KafkaSeedBroker later reports (localhost on most
	// dev machines, an IP or remote host under docker-machine / CI).
	provider, err := testcontainers.NewDockerProvider()
	if skipIfNoDockerTLS(t, err) {
		return
	}
	require.NoError(t, err, "docker provider")
	t.Cleanup(func() { _ = provider.Close() })

	daemonHost, err := provider.DaemonHost(ctx)
	if skipIfNoDockerTLS(t, err) {
		return
	}
	require.NoError(t, err, "daemon host")

	// Private self-signed CA + a server certificate signed by that CA. Only
	// the CA public certificate crosses into STREAMING_TLS_CA_CERT.
	ca, err := tlscert.SelfSignedFromRequestE(tlscert.Request{
		Name:              "streaming-tls-ca",
		SubjectCommonName: "streaming-tls-ca",
		Host:              "localhost,127.0.0.1," + daemonHost,
		IsCA:              true,
		ValidFor:          time.Hour,
	})
	require.NoError(t, err, "generate CA")
	require.NotNil(t, ca, "CA cert")

	serverCert, err := tlscert.SelfSignedFromRequestE(tlscert.Request{
		Name:              "streaming-tls-server",
		SubjectCommonName: "localhost",
		Host:              "localhost,127.0.0.1," + daemonHost,
		Parent:            ca,
		ValidFor:          time.Hour,
	})
	require.NoError(t, err, "generate server cert")
	require.NotNil(t, serverCert, "server cert")

	container, err := tcredpanda.Run(ctx,
		tlsRedpandaImage,
		tcredpanda.WithTLS(serverCert.Bytes, serverCert.KeyBytes),
		tcredpanda.WithAutoCreateTopics(),
	)
	if skipIfNoDockerTLS(t, err) {
		return
	}
	require.NoError(t, err, "redpanda TLS container start")

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := container.Terminate(cleanupCtx); err != nil {
			t.Logf("redpanda terminate: %v", err)
		}
	})

	seedBroker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err, "seed broker address")

	// Drive the public environment-variable contract.
	caB64 := base64.StdEncoding.EncodeToString(ca.Bytes)
	t.Setenv("STREAMING_ENABLED", "true")
	t.Setenv("STREAMING_BROKERS", seedBroker)
	t.Setenv("STREAMING_CLOUDEVENTS_SOURCE", tlsIntegSource)
	t.Setenv("STREAMING_TLS_ENABLED", "true")
	t.Setenv("STREAMING_TLS_CA_CERT", caB64)

	cfg, warnings, err := config.LoadConfig()
	require.NoError(t, err, "LoadConfig")
	require.Empty(t, warnings, "no deprecation warnings expected")
	require.True(t, cfg.TLSEnabled, "cfg.TLSEnabled")

	// The TLS config the producer will dial with; reused to drive the
	// verification consumer and admin client over the same trust root.
	clientTLS, err := cfg.BuildTLSConfig()
	require.NoError(t, err, "BuildTLSConfig")
	require.NotNil(t, clientTLS, "clientTLS")
	require.NotNil(t, clientTLS.RootCAs, "clientTLS.RootCAs")
	require.False(t, clientTLS.InsecureSkipVerify, "InsecureSkipVerify must stay false")
	require.Equal(t, uint16(tls.VersionTLS12), clientTLS.MinVersion, "MinVersion TLS 1.2")

	waitForTLSBroker(t, seedBroker, clientTLS)
	createTLSTopic(t, ctx, seedBroker, clientTLS, tlsIntegTopic)

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	require.NoError(t, err, "NewCatalog")

	route := streaming.RouteDefinition{
		Key:           "transaction.created.kafka.tls",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic(tlsIntegTopic),
		Requirement:   streaming.RouteRequired,
	}

	emitter, err := streaming.NewBuilder().
		Source(tlsIntegSource).
		Catalog(catalog).
		Routes(route).
		Target(streaming.TargetConfig{
			Name:    "primary",
			Kind:    streaming.TransportKafkaLike,
			Brokers: []string{seedBroker},
		}).
		TLSFromConfig(cfg).
		Build(ctx)
	require.NoError(t, err, "Build over TLS")
	t.Cleanup(func() { _ = emitter.Close() })

	payload := []byte(`{"amount":4200,"currency":"BRL"}`)
	require.NoError(t, emitter.Emit(ctx, streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-tls-1",
		Payload:       payload,
	}), "Emit over TLS")

	got := consumeOneTLS(t, ctx, seedBroker, clientTLS, tlsIntegTopic)
	require.Equal(t, string(payload), string(got.Value), "consumed payload round-trips over TLS")
}

// newTLSClient builds a franz-go client that dials the broker over TLS using
// the supplied config plus any extra options.
func newTLSClient(t *testing.T, seed string, tlsCfg *tls.Config, extra ...kgo.Opt) *kgo.Client {
	t.Helper()

	opts := append([]kgo.Opt{
		kgo.SeedBrokers(seed),
		kgo.DialTLSConfig(tlsCfg.Clone()),
	}, extra...)

	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err, "kgo.NewClient over TLS")

	return cl
}

// waitForTLSBroker polls the broker's metadata over TLS until it responds or
// the deadline expires, proving the TLS handshake works before the test
// proceeds.
func waitForTLSBroker(t *testing.T, seed string, tlsCfg *tls.Config) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)

	var lastErr error

	for time.Now().Before(deadline) {
		cl := newTLSClient(t, seed, tlsCfg)
		admin := kadm.NewClient(cl)

		callCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := admin.ListTopics(callCtx)
		cancel()
		cl.Close()

		if err == nil {
			return
		}

		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}

	require.NoError(t, lastErr, "broker never became ready over TLS")
}

// createTLSTopic pre-creates the topic over TLS to avoid the
// first-produce-after-boot UNKNOWN_TOPIC_OR_PARTITION race.
func createTLSTopic(t *testing.T, ctx context.Context, seed string, tlsCfg *tls.Config, topic string) {
	t.Helper()

	cl := newTLSClient(t, seed, tlsCfg)
	defer cl.Close()

	admin := kadm.NewClient(cl)

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopics(callCtx, 1, 1, nil, topic)
	require.NoError(t, err, "create topics request over TLS")

	// A pre-existing topic (auto-create raced us) is fine; anything else fails.
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			require.NoErrorf(t, r.Err, "create topic %s over TLS", r.Topic)
		}
	}
}

// consumeOneTLS consumes exactly one record from topic over TLS.
func consumeOneTLS(t *testing.T, ctx context.Context, seed string, tlsCfg *tls.Config, topic string) *kgo.Record {
	t.Helper()

	cl := newTLSClient(t, seed, tlsCfg,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer cl.Close()

	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		fetches := cl.PollFetches(pollCtx)
		if err := pollCtx.Err(); err != nil {
			require.NoError(t, err, "timed out consuming record over TLS")
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoErrorf(t, errs[0].Err, "fetch error on topic %s", errs[0].Topic)
		}

		var record *kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			if record == nil {
				record = r
			}
		})

		if record != nil {
			return record
		}
	}
}
