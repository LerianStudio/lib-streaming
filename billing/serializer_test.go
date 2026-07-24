//go:build unit

package billing_test

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/LerianStudio/lib-streaming/v2/billing"
)

// newBillingSerializer wires a Serializer against an in-memory schema registry
// (srfake spins up its own httptest.Server) and returns BOTH the serializer and
// the sr.Client bound to that registry. A test can then resolve the id srfake
// actually assigned and cross-check it against the id carried on the wire. Each
// call gets an isolated registry so tests stay parallel-safe.
func newBillingSerializer(t *testing.T) (*billing.Serializer, *sr.Client) {
	t.Helper()

	reg := srfake.New()
	t.Cleanup(reg.Close)

	client, err := sr.NewClient(sr.URLs(reg.URL()))
	require.NoError(t, err)

	ser, err := billing.NewSerializer(context.Background(), client)
	require.NoError(t, err)
	require.NotNil(t, ser)

	return ser, client
}

// decodeConfluentPayload is the single shared decode path for every round-trip
// assertion in this file: it pins the Confluent framing (0x00 magic byte, 4-byte
// big-endian schema id), then strips the header and proto-decodes the remainder
// through a decode serde bound to the on-wire id. It returns the decoded payload
// plus the wire id so callers can cross-check the id against the registry.
//
// The e2e suite keeps its own equivalent (decodeBillingRecord) because it lives
// in a different test package (streaming_test) and cannot import this helper.
func decodeConfluentPayload(t *testing.T, raw []byte) (*billing.BillablePayload, uint32) {
	t.Helper()

	require.GreaterOrEqual(t, len(raw), 5, "at least magic byte + 4-byte schema id")
	require.Equal(t, byte(0x00), raw[0], "Confluent magic byte")

	id := binary.BigEndian.Uint32(raw[1:5])
	require.Positive(t, id, "schema id resolved from the registry")

	dec := sr.NewSerde()
	dec.Register(int(id), &billing.BillablePayload{}, sr.Index(0),
		sr.DecodeFn(func(b []byte, v any) error {
			msg, ok := v.(*billing.BillablePayload)
			require.True(t, ok)

			return proto.Unmarshal(b, msg)
		}),
	)

	var got billing.BillablePayload
	require.NoError(t, dec.Decode(raw, &got))

	return &got, id
}

// TestSerializer_Serialize_FullPayload_RoundTrips pins the happy path end-to-end
// with byte-level Confluent-frame assertions: the emitted bytes carry the 0x00
// magic byte and a 4-byte big-endian id that resolves back out of srfake as a
// protobuf schema, and a decode serde bound to that id recovers every field —
// metric, subscription id, timestamp, BOTH a string and a number property, and
// precise_total_amount_cents.
func TestSerializer_Serialize_FullPayload_RoundTrips(t *testing.T) {
	t.Parallel()

	ser, client := newBillingSerializer(t)

	ts := timestamppb.Now()
	payload := &billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionId: "sub_123",
		Timestamp:      ts,
		Properties: map[string]*billing.PropertyValue{
			"region": billing.StringProperty("br"),
			"count":  billing.NumberProperty(42),
		},
		PreciseTotalAmountCents: new("12345"),
	}

	encoded, err := ser.Serialize(payload)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	got, wireID := decodeConfluentPayload(t, encoded)

	// The on-wire id must be the exact id srfake registered: resolvable straight
	// back out of the registry as a protobuf schema, not a fabricated value.
	regSchema, err := client.SchemaByID(context.Background(), int(wireID))
	require.NoError(t, err, "on-wire id must resolve to a schema registered in srfake")
	require.Equal(t, sr.TypeProtobuf, regSchema.Type)

	require.Equal(t, "api_calls", got.GetMetric())
	require.Equal(t, "sub_123", got.GetSubscriptionId())
	require.Equal(t, "12345", got.GetPreciseTotalAmountCents())
	require.Equal(t, "br", got.GetProperties()["region"].GetStringValue())
	require.InDelta(t, 42.0, got.GetProperties()["count"].GetNumberValue(), 0)
	require.True(t, got.GetTimestamp().AsTime().Equal(ts.AsTime()))
}

// TestSerializer_Serialize_OmitsUnsetOptionalFields pins the optional-field
// contract: a minimal payload carrying only the two mandatory fields round-trips,
// and the decoded message reads back the proto3 zero values for the optionals
// (no timestamp, no properties, empty precise amount) — the wire omits them.
func TestSerializer_Serialize_OmitsUnsetOptionalFields(t *testing.T) {
	t.Parallel()

	ser, _ := newBillingSerializer(t)

	encoded, err := ser.Serialize(&billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionId: "sub_123",
	})
	require.NoError(t, err)

	got, _ := decodeConfluentPayload(t, encoded)

	require.Equal(t, "api_calls", got.GetMetric())
	require.Equal(t, "sub_123", got.GetSubscriptionId())
	require.Nil(t, got.GetTimestamp(), "unset timestamp decodes to nil")
	require.Empty(t, got.GetProperties(), "unset properties decode to empty")
	require.Empty(t, got.GetPreciseTotalAmountCents(), "unset precise amount decodes to empty")
}

// TestSerializer_Serialize_ReturnsErrorOnInvalid pins the zero-panic contract:
// Serialize runs Validate first and RETURNS the error (never panics) for a
// payload that violates a residual invariant.
func TestSerializer_Serialize_ReturnsErrorOnInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *billing.BillablePayload
	}{
		{
			name:    "empty metric",
			payload: &billing.BillablePayload{Metric: "", SubscriptionId: "sub_123"},
		},
		{
			name:    "empty subscription id",
			payload: &billing.BillablePayload{Metric: "api_calls", SubscriptionId: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ser, _ := newBillingSerializer(t)

			var (
				encoded []byte
				err     error
			)

			require.NotPanics(t, func() {
				encoded, err = ser.Serialize(tt.payload)
			}, "Serialize must return the Validate error, never panic")

			require.Error(t, err)
			require.Nil(t, encoded)
		})
	}
}

// TestNewSerializer_NilClient_ReturnsError pins the fail-closed guard: a nil
// registry client is a caller wiring bug, reported as an error rather than a
// nil-dereference panic on first use.
func TestNewSerializer_NilClient_ReturnsError(t *testing.T) {
	t.Parallel()

	ser, err := billing.NewSerializer(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, ser)
	require.ErrorContains(t, err, "schema registry client is required")
}

// TestNewSerializer_ReusesRegisteredSchema exercises the LookupSchema-hit branch:
// the second NewSerializer against the same registry resolves the id already
// created by the first (CreateSchema fallback), and both serializers frame an
// identical payload the same way.
func TestNewSerializer_ReusesRegisteredSchema(t *testing.T) {
	t.Parallel()

	reg := srfake.New()
	t.Cleanup(reg.Close)

	client, err := sr.NewClient(sr.URLs(reg.URL()))
	require.NoError(t, err)

	// First construction hits the empty registry and CreateSchema-registers the
	// subject; the second finds it via LookupSchema.
	first, err := billing.NewSerializer(context.Background(), client)
	require.NoError(t, err)

	second, err := billing.NewSerializer(context.Background(), client)
	require.NoError(t, err)

	payload := &billing.BillablePayload{Metric: "api_calls", SubscriptionId: "sub_123"}

	firstBytes, err := first.Serialize(payload)
	require.NoError(t, err)

	secondBytes, err := second.Serialize(payload)
	require.NoError(t, err)

	require.Equal(t, firstBytes, secondBytes, "same schema id => identical Confluent frame")
}

// TestNewSerializer_RegistryUnavailable_ReturnsError covers the CreateSchema-error
// branch: against a registry that fails every request, LookupSchema fails, the
// CreateSchema fallback also fails, and NewSerializer surfaces a wrapped error
// instead of returning a half-built serializer.
func TestNewSerializer_RegistryUnavailable_ReturnsError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "registry down", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	client, err := sr.NewClient(sr.URLs(srv.URL))
	require.NoError(t, err)

	ser, err := billing.NewSerializer(context.Background(), client)
	require.Error(t, err)
	require.Nil(t, ser)
	require.ErrorContains(t, err, "resolve schema id")
}
