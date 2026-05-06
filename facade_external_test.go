//go:build unit

package streaming_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFacade_NewEmitRequestDeepCopiesMutableFields(t *testing.T) {
	t.Parallel()

	enabled := true
	payload := json.RawMessage(`{"ok":true}`)

	request, err := streaming.NewEmitRequest(streaming.EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Payload:       payload,
		PolicyOverride: streaming.DeliveryPolicyOverride{
			Enabled: &enabled,
		},
	})
	if err != nil {
		t.Fatalf("NewEmitRequest() error = %v", err)
	}

	payload[2] = 'X'
	enabled = false

	if string(request.Payload) != `{"ok":true}` {
		t.Fatalf("Payload was not defensively copied: %q", string(request.Payload))
	}
	if request.PolicyOverride.Enabled == nil || !*request.PolicyOverride.Enabled {
		t.Fatalf("PolicyOverride.Enabled was not defensively copied: %v", request.PolicyOverride.Enabled)
	}
}

func TestFacade_NewNoopEmitter(t *testing.T) {
	t.Parallel()

	emitter := streaming.NewNoopEmitter()
	if emitter == nil {
		t.Fatal("NewNoopEmitter() = nil; want non-nil emitter")
	}

	if err := emitter.Emit(context.Background(), streaming.EmitRequest{}); err != nil {
		t.Fatalf("noop Emit() error = %v", err)
	}
	if err := emitter.Close(); err != nil {
		t.Fatalf("noop Close() error = %v", err)
	}
}

func TestFacade_BuilderDescriptorAndClose(t *testing.T) {
	t.Parallel()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	emitter, err := streaming.NewBuilder().
		Source("//test/source").
		Catalog(catalog).
		Routes(streaming.RouteDefinition{
			Key:           "transaction.created.kafka.primary",
			DefinitionKey: "transaction.created",
			Target:        "primary",
			Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
			Requirement:   streaming.RouteRequired,
		}).
		Target(streaming.TargetConfig{
			Name:     "primary",
			Kind:     streaming.TransportKafkaLike,
			Brokers:  []string{"127.0.0.1:9092"},
			ClientID: "facade-test",
		}).
		CloseTimeout(time.Second).
		Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	t.Cleanup(func() { _ = emitter.Close() })

	producer, ok := emitter.(*streaming.Producer)
	if !ok {
		t.Fatalf("Build() emitter type = %T; want *streaming.Producer", emitter)
	}

	descriptor, err := producer.Descriptor(streaming.PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//test/source",
	})
	if err != nil {
		t.Fatalf("Descriptor() error = %v", err)
	}
	if descriptor.ProducerID == "" {
		t.Fatal("Descriptor() did not populate ProducerID")
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestFacade_NilProducerMethodsAreSafe(t *testing.T) {
	t.Parallel()

	var producer *streaming.Producer

	if err := producer.Emit(context.Background(), streaming.EmitRequest{}); !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("nil Emit() error = %v; want ErrNilProducer", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("nil Close() error = %v; want nil", err)
	}
	if _, err := producer.Descriptor(streaming.PublisherDescriptor{}); !errors.Is(err, streaming.ErrNilProducer) {
		t.Fatalf("nil Descriptor() error = %v; want ErrNilProducer", err)
	}
}

func TestFacade_ManifestAndCloudEventsWrappers(t *testing.T) {
	t.Parallel()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	manifest, err := streaming.BuildManifest(streaming.PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//svc",
	}, catalog, streaming.RouteTable{})
	if err != nil {
		t.Fatalf("BuildManifest() error = %v", err)
	}
	if len(manifest.Events) != 1 || manifest.Events[0].Key != "transaction.created" {
		t.Fatalf("manifest events = %+v; want transaction.created", manifest.Events)
	}

	headers := streaming.BuildCloudEventsHeaders(streaming.Event{
		EventID:       "event-1",
		Source:        "//svc",
		Timestamp:     time.Unix(0, 0).UTC(),
		ResourceType:  "transaction",
		EventType:     "created",
		SchemaVersion: "1.0.0",
	})
	if len(headers) == 0 {
		t.Fatal("BuildCloudEventsHeaders() returned no headers")
	}

	parsed, err := streaming.ParseCloudEventsHeaders([]kgo.RecordHeader{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-id", Value: []byte("event-1")},
		{Key: "ce-source", Value: []byte("//svc")},
		{Key: "ce-type", Value: []byte("studio.lerian.transaction.created")},
		{Key: "ce-time", Value: []byte(time.Unix(0, 0).UTC().Format(time.RFC3339Nano))},
	})
	if err != nil {
		t.Fatalf("ParseCloudEventsHeaders() error = %v", err)
	}
	if parsed.EventID != "event-1" || parsed.Source != "//svc" {
		t.Fatalf("parsed event = %+v", parsed)
	}
}

// handlerOptionsCatalogFixture builds a stable single-event catalog used by
// the WithManifestRoutes test surface. Local helper, not exported.
func handlerOptionsCatalogFixture(t *testing.T) streaming.Catalog {
	t.Helper()

	catalog, err := streaming.NewCatalog(streaming.EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

func handlerOptionsDescriptorFixture() streaming.PublisherDescriptor {
	return streaming.PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//svc",
	}
}

func serveManifestBody(t *testing.T, handler http.Handler) []byte {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/streaming", nil)
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", recorder.Code, http.StatusOK)
	}

	return recorder.Body.Bytes()
}

// TestFacade_NewStreamingHandler_NoOptsByteIdenticalToZeroRouteTable pins the
// load-bearing backward-compat guarantee: NewStreamingHandler(d, c) and
// NewStreamingHandler(d, c, WithManifestRoutes(RouteTable{})) produce
// byte-identical response bodies. This is what makes the variadic extension
// source-compatible with all existing two-argument call sites.
func TestFacade_NewStreamingHandler_NoOptsByteIdenticalToZeroRouteTable(t *testing.T) {
	t.Parallel()

	descriptor := handlerOptionsDescriptorFixture()
	catalog := handlerOptionsCatalogFixture(t)

	plain, err := streaming.NewStreamingHandler(descriptor, catalog)
	if err != nil {
		t.Fatalf("NewStreamingHandler(d, c) error = %v", err)
	}
	withZero, err := streaming.NewStreamingHandler(descriptor, catalog, streaming.WithManifestRoutes(streaming.RouteTable{}))
	if err != nil {
		t.Fatalf("NewStreamingHandler(d, c, WithManifestRoutes(zero)) error = %v", err)
	}

	plainBody := serveManifestBody(t, plain)
	zeroBody := serveManifestBody(t, withZero)

	if !bytes.Equal(plainBody, zeroBody) {
		t.Fatalf("manifest bodies diverge:\nno-opts: %s\nzero-table: %s", plainBody, zeroBody)
	}
}

// TestFacade_NewStreamingHandler_WithRoutesRoundTrip exercises the happy
// path: a populated RouteTable threads through to the manifest's `routes`
// section verbatim, with deterministic ordering preserved.
func TestFacade_NewStreamingHandler_WithRoutesRoundTrip(t *testing.T) {
	t.Parallel()

	descriptor := handlerOptionsDescriptorFixture()
	catalog := handlerOptionsCatalogFixture(t)

	routes, err := streaming.NewRouteTable(streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
		Requirement:   streaming.RouteRequired,
	})
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	handler, err := streaming.NewStreamingHandler(descriptor, catalog, streaming.WithManifestRoutes(routes))
	if err != nil {
		t.Fatalf("NewStreamingHandler(...) error = %v", err)
	}

	body := serveManifestBody(t, handler)

	var doc streaming.ManifestDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got := len(doc.Routes); got != 1 {
		t.Fatalf("len(Routes) = %d; want 1", got)
	}

	r := doc.Routes[0]
	if r.Key != "transaction.created.kafka.primary" {
		t.Errorf("Key = %q; want transaction.created.kafka.primary", r.Key)
	}
	if r.DefinitionKey != "transaction.created" {
		t.Errorf("DefinitionKey = %q; want transaction.created", r.DefinitionKey)
	}
	if r.Target != "primary" {
		t.Errorf("Target = %q; want primary", r.Target)
	}
	if r.Transport != streaming.TransportKafkaLike {
		t.Errorf("Transport = %q; want kafka", r.Transport)
	}
	if r.Destination != "lerian.streaming.transaction.created" {
		t.Errorf("Destination = %q; want lerian.streaming.transaction.created", r.Destination)
	}
	if !r.Required {
		t.Errorf("Required = false; want true")
	}
}

// TestFacade_NewStreamingHandler_LastWithManifestRoutesWins pins the
// last-write-wins semantic for repeated WithManifestRoutes calls. A future
// refactor that introduced merge/append semantics would silently change
// behavior for callers building handlers from layered config.
func TestFacade_NewStreamingHandler_LastWithManifestRoutesWins(t *testing.T) {
	t.Parallel()

	descriptor := handlerOptionsDescriptorFixture()
	catalog := handlerOptionsCatalogFixture(t)

	first, err := streaming.NewRouteTable(streaming.RouteDefinition{
		Key:           "transaction.created.kafka.first",
		DefinitionKey: "transaction.created",
		Target:        "first",
		Destination:   streaming.KafkaTopic("first.topic"),
		Requirement:   streaming.RouteRequired,
	})
	if err != nil {
		t.Fatalf("NewRouteTable(first) error = %v", err)
	}

	last, err := streaming.NewRouteTable(streaming.RouteDefinition{
		Key:           "transaction.created.kafka.last",
		DefinitionKey: "transaction.created",
		Target:        "last",
		Destination:   streaming.KafkaTopic("last.topic"),
		Requirement:   streaming.RouteRequired,
	})
	if err != nil {
		t.Fatalf("NewRouteTable(last) error = %v", err)
	}

	handler, err := streaming.NewStreamingHandler(
		descriptor,
		catalog,
		streaming.WithManifestRoutes(first),
		streaming.WithManifestRoutes(last),
	)
	if err != nil {
		t.Fatalf("NewStreamingHandler(...) error = %v", err)
	}

	var doc streaming.ManifestDocument
	if err := json.Unmarshal(serveManifestBody(t, handler), &doc); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got := len(doc.Routes); got != 1 {
		t.Fatalf("len(Routes) = %d; want 1 (last call wins, no merge)", got)
	}
	if doc.Routes[0].Target != "last" {
		t.Errorf("Routes[0].Target = %q; want \"last\" (last call wins)", doc.Routes[0].Target)
	}
	if doc.Routes[0].Key != "transaction.created.kafka.last" {
		t.Errorf("Routes[0].Key = %q; want \"transaction.created.kafka.last\"", doc.Routes[0].Key)
	}
}

// TestFacade_NewStreamingHandler_ConstructionTimeErrorPropagation pins the
// fail-fast semantic: BuildManifest validation failures surface as the
// constructor's error return, not as deferred runtime 500s.
//
// The reachable failure path through NewStreamingHandler is invalid
// PublisherDescriptor input — RouteTable values are pre-validated at
// NewRouteTable construction, so by the time WithManifestRoutes accepts
// them the table itself cannot fail BuildManifest. We exercise the
// architectural property (nil handler + non-nil error returned at
// construction, not deferred) via the descriptor path that BuildManifest
// actually validates.
func TestFacade_NewStreamingHandler_ConstructionTimeErrorPropagation(t *testing.T) {
	t.Parallel()

	routes, err := streaming.NewRouteTable(streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
	})
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	// Empty PublisherDescriptor fails NewPublisherDescriptor inside
	// BuildManifest with ErrInvalidPublisherDescriptor.
	handler, err := streaming.NewStreamingHandler(
		streaming.PublisherDescriptor{},
		handlerOptionsCatalogFixture(t),
		streaming.WithManifestRoutes(routes),
	)

	if !errors.Is(err, streaming.ErrInvalidPublisherDescriptor) {
		t.Fatalf("NewStreamingHandler() error = %v; want errors.Is(err, ErrInvalidPublisherDescriptor)", err)
	}
	if handler != nil {
		t.Fatalf("NewStreamingHandler() handler = %v; want nil on construction failure", handler)
	}
}

// TestFacade_NewStreamingHandler_NilOptionSafety asserts the variadic
// constructor tolerates nil HandlerOption entries — both as the sole option
// and interleaved with a real option. Tolerance means the handler still
// serves a manifest observably equivalent to the same call without the nil,
// not just "doesn't crash."
func TestFacade_NewStreamingHandler_NilOptionSafety(t *testing.T) {
	t.Parallel()

	descriptor := handlerOptionsDescriptorFixture()
	catalog := handlerOptionsCatalogFixture(t)

	routes, err := streaming.NewRouteTable(streaming.RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   streaming.KafkaTopic("lerian.streaming.transaction.created"),
	})
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	referenceNoOpts, err := streaming.NewStreamingHandler(descriptor, catalog)
	if err != nil {
		t.Fatalf("baseline NewStreamingHandler(d, c) error = %v", err)
	}
	referenceNoOptsBody := serveManifestBody(t, referenceNoOpts)

	referenceWithRoutes, err := streaming.NewStreamingHandler(descriptor, catalog, streaming.WithManifestRoutes(routes))
	if err != nil {
		t.Fatalf("baseline NewStreamingHandler(d, c, WithManifestRoutes) error = %v", err)
	}
	referenceWithRoutesBody := serveManifestBody(t, referenceWithRoutes)

	nilOnly, err := streaming.NewStreamingHandler(descriptor, catalog, nil)
	if err != nil {
		t.Fatalf("NewStreamingHandler(d, c, nil) error = %v; want nil", err)
	}
	if nilOnly == nil {
		t.Fatal("NewStreamingHandler(d, c, nil) handler = nil; want non-nil")
	}
	if got := serveManifestBody(t, nilOnly); !bytes.Equal(got, referenceNoOptsBody) {
		t.Fatalf("nil-only manifest body diverges from no-opts:\nnil-only: %s\nno-opts:  %s", got, referenceNoOptsBody)
	}

	interleaved, err := streaming.NewStreamingHandler(descriptor, catalog, nil, streaming.WithManifestRoutes(routes))
	if err != nil {
		t.Fatalf("NewStreamingHandler(d, c, nil, WithManifestRoutes(routes)) error = %v; want nil", err)
	}
	if interleaved == nil {
		t.Fatal("NewStreamingHandler(d, c, nil, WithManifestRoutes(routes)) handler = nil; want non-nil")
	}
	if got := serveManifestBody(t, interleaved); !bytes.Equal(got, referenceWithRoutesBody) {
		t.Fatalf("interleaved (nil + WithManifestRoutes) manifest body diverges from WithManifestRoutes-only")
	}
}
