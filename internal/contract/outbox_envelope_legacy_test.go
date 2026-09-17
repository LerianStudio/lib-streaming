//go:build unit

package contract

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadLegacyEnvelope decodes testdata/outbox_envelope_v1.json, the version-1
// wire bytes lib-streaming v2 persisted.
//
// It is a raw JSON fixture rather than a struct literal on purpose. The
// version-1 and version-2 envelopes are byte-identical on the wire — every
// json tag in outbox_envelope.go and route.go is unchanged since v2.1.0, and
// Event deliberately carries no tags at all, so it serializes under Go-default
// field names in both majors. Decoding the real bytes therefore also pins the
// field NAMES: rename or retag any of them and this test fails, which a struct
// literal built from the current types could never catch.
func loadLegacyEnvelope(tb testing.TB) OutboxEnvelope {
	tb.Helper()

	raw, err := os.ReadFile(filepath.Join("testdata", "outbox_envelope_v1.json"))
	require.NoError(tb, err, "read v1 fixture")

	var envelope OutboxEnvelope
	require.NoError(tb, json.Unmarshal(raw, &envelope), "decode v1 fixture")

	require.Equal(tb, OutboxEnvelopeVersionLegacy, envelope.Version,
		"fixture must be a version-1 row")
	require.Equal(tb, "midaz-ledger.transaction.created", envelope.Destination.Name,
		"fixture must carry the v2-era per-event topic, which is the whole point of the row")

	return envelope
}

// TestLegacyEnvelopeDecodesAndValidates is the regression test for the data
// loss itself: before this change the version gate was strict equality, so
// these exact bytes were rejected as a malformed envelope.
func TestLegacyEnvelopeDecodesAndValidates(t *testing.T) {
	t.Parallel()

	envelope := loadLegacyEnvelope(t)

	require.NoError(t, envelope.ValidateShape(), "v1 row must pass shape validation")
	require.NoError(t, envelope.Validate(), "v1 row must pass full validation")
}

func TestOutboxEnvelopeVersionAcceptance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version int
		wantErr bool
	}{
		{name: "legacy version 1 is readable", version: 1, wantErr: false},
		{name: "current version 2 is readable", version: 2, wantErr: false},
		{name: "version 0 is rejected", version: 0, wantErr: true},
		{name: "unknown future version 3 is rejected", version: 3, wantErr: true},
		{name: "negative version is rejected", version: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			envelope := loadLegacyEnvelope(t)
			envelope.Version = tt.version

			err := envelope.ValidateShape()
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidOutboxEnvelope,
				"an unreadable version must stay an invalid-envelope caller error")
		})
	}
}

// TestResolveDestinationRederivesLegacyKafkaTopic pins the routing answer: a
// version-1 row does NOT go to its persisted topic.
func TestResolveDestinationRederivesLegacyKafkaTopic(t *testing.T) {
	t.Parallel()

	envelope := loadLegacyEnvelope(t)

	resolved, err := envelope.ResolveDestination()
	require.NoError(t, err)

	assert.Equal(t, "lerian.streaming.midaz-ledger", resolved.Name,
		"a v1 row must be re-derived onto the application topic current consumers subscribe to")
	assert.Equal(t, AppTopic(envelope.Event.Source), resolved.Name,
		"re-derivation must use the same helper as the live Emit path")
	assert.NotEqual(t, envelope.Destination.Name, resolved.Name,
		"publishing the persisted per-event topic is the silent-loss bug this prevents")
	assert.Equal(t, TransportKafkaLike, resolved.Kind, "kind must be preserved")
	assert.Equal(t, "midaz-ledger.transaction.created", envelope.Destination.Name,
		"resolution must not mutate the receiver")
}

func TestResolveDestinationLeavesCurrentAndNonKafkaRowsAlone(t *testing.T) {
	t.Parallel()

	t.Run("version 2 kafka row is untouched", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		envelope.Version = OutboxEnvelopeVersion
		envelope.Destination.Name = "lerian.streaming.midaz-ledger.commands"

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, "lerian.streaming.midaz-ledger.commands", resolved.Name,
			"a current row is authoritative, including a command queue the live path already rewrote")
	})

	t.Run("legacy sqs row keeps its queue url", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		envelope.Transport = TransportSQS
		envelope.Destination = Destination{
			Kind:    TransportSQS,
			Address: "https://sqs.us-east-1.amazonaws.com/123456789012/ledger-events",
		}

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, envelope.Destination, resolved,
			"the v1->v2 change was Kafka topic naming only; a queue URL still addresses the same queue")
	})
}

// TestResolveDestinationUnroutableSourceStaysRetryable is the structural
// failure case, and the assertion that matters most is the last one.
//
// v2 fed ce-source through a lossy sanitizer, so "//lerian.midaz/transaction-service"
// was a legal source that emitted to "lerian.midaz-transaction-service.<...>".
// v3 deleted that sanitizer and rejects the raw value, so the row cannot be
// re-derived and needs an operator. It must not be destroyed on the way.
func TestResolveDestinationUnroutableSourceStaysRetryable(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		"//lerian.midaz/transaction-service", // documented v2 sanitizer example
		"MIDAZ-Ledger",                       // v2 lowercased it
		"my!service",                         // v2 punctuation-folded it
		"",                                   // no source at all
	} {
		t.Run("source="+source, func(t *testing.T) {
			t.Parallel()

			envelope := loadLegacyEnvelope(t)
			envelope.Event.Source = source

			_, err := envelope.ResolveDestination()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrLegacyOutboxRowUnroutable)

			assert.False(t, IsCallerError(err),
				"a caller error sends the row straight to INVALID on attempt one; "+
					"this row must stay retryable so an operator can act on it")
			assert.NotErrorIs(t, err, ErrInvalidSource,
				"the cause must be rendered with %v, never %w: wrapping it re-enters callerErrorSentinels")
			assert.NotErrorIs(t, err, ErrMissingSource,
				"the cause must be rendered with %v, never %w: wrapping it re-enters callerErrorSentinels")
		})
	}
}

// TestLegacyUnroutableSentinelIsNotACallerError guards the sentinel directly,
// so adding it to callerErrorSentinels fails here rather than silently
// resurrecting the data loss at the next deploy.
func TestLegacyUnroutableSentinelIsNotACallerError(t *testing.T) {
	t.Parallel()

	assert.False(t, IsCallerError(ErrLegacyOutboxRowUnroutable),
		"ErrLegacyOutboxRowUnroutable must never be classified caller-correctable")

	for _, sentinel := range callerErrorSentinels {
		assert.False(t, errors.Is(ErrLegacyOutboxRowUnroutable, sentinel),
			"ErrLegacyOutboxRowUnroutable must not alias a caller-error sentinel")
	}
}

// TestResolveDestinationRewritesOnlyWhatV2Derived pins the narrowing: v2
// synthesized its routes from EventDefinition.Topic(source), but
// MergeRouteOverrides let a service point a Kafka route anywhere. An explicit
// override is an operator's deliberate choice that v3 never invalidated, so
// rewriting it would silently move a stream they still consume.
func TestResolveDestinationRewritesOnlyWhatV2Derived(t *testing.T) {
	t.Parallel()

	t.Run("derived topic is rewritten", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		// "midaz-ledger" + "transaction" + "created", schema 1.x -> base form.
		envelope.Destination.Name = "midaz-ledger.transaction.created"

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, AppTopic("midaz-ledger"), resolved.Name)
	})

	t.Run("derived topic with a v2 schema suffix is rewritten", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		envelope.Event.SchemaVersion = "2.3.1"
		// v2 appended ".v<major>" once the major reached 2.
		envelope.Destination.Name = "midaz-ledger.transaction.created.v2"

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, AppTopic("midaz-ledger"), resolved.Name,
			"the .v<major> form is still a v2-derived name and must be rewritten")
	})

	t.Run("explicit route override is preserved", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		envelope.Destination.Name = "ledger-audit-archive"

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, "ledger-audit-archive", resolved.Name,
			"a destination v2 did not derive was an operator's choice; moving it loses their consumer")
	})

	t.Run("schema suffix mismatch is treated as an override", func(t *testing.T) {
		t.Parallel()

		envelope := loadLegacyEnvelope(t)
		envelope.Event.SchemaVersion = "1.0.0" // base form, so no ".v2" suffix
		envelope.Destination.Name = "midaz-ledger.transaction.created.v2"

		resolved, err := envelope.ResolveDestination()
		require.NoError(t, err)
		assert.Equal(t, "midaz-ledger.transaction.created.v2", resolved.Name,
			"v2 would not have derived this name for this event, so it was set deliberately")
	})
}

func TestLegacyDerivedTopicMirrorsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		schemaVersion string
		want          string
	}{
		{name: "empty schema falls through to base", schemaVersion: "", want: "midaz-ledger.transaction.created"},
		{name: "major 1 falls through to base", schemaVersion: "1.0.0", want: "midaz-ledger.transaction.created"},
		{name: "major 2 gets the suffix", schemaVersion: "2.0.0", want: "midaz-ledger.transaction.created.v2"},
		{name: "major 11 gets the suffix", schemaVersion: "11.4.2", want: "midaz-ledger.transaction.created.v11"},
		{name: "v-prefixed major 3 gets the suffix", schemaVersion: "v3.1.0", want: "midaz-ledger.transaction.created.v3"},
		{
			name: "unparseable falls through to base, as v2's ParseMajorVersion did",
			// v2 used ParseMajorVersion, which collapses a parse failure to 0.
			schemaVersion: "not-a-semver",
			want:          "midaz-ledger.transaction.created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := legacyDerivedTopic(Event{
				Source:        "midaz-ledger",
				ResourceType:  "transaction",
				EventType:     "created",
				SchemaVersion: tt.schemaVersion,
			})
			assert.Equal(t, tt.want, got)
		})
	}
}
