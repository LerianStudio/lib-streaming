//go:build unit

package streaming

import (
	"errors"
	"testing"
)

func TestEventDefinition_New_NormalizesDefaults(t *testing.T) {
	t.Parallel()

	definition, err := NewEventDefinition(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewEventDefinition() error = %v", err)
	}

	if definition.SchemaVersion != defaultSchemaVersion {
		t.Errorf("SchemaVersion = %q; want %q", definition.SchemaVersion, defaultSchemaVersion)
	}
	if definition.DataContentType != defaultDataContentType {
		t.Errorf("DataContentType = %q; want %q", definition.DataContentType, defaultDataContentType)
	}
	if definition.DefaultPolicy != DefaultDeliveryPolicy() {
		t.Errorf("DefaultPolicy = %#v; want %#v", definition.DefaultPolicy, DefaultDeliveryPolicy())
	}
	if got := definition.Topic(); got != "lerian.streaming.transaction.created" {
		t.Errorf("Topic() = %q; want %q", got, "lerian.streaming.transaction.created")
	}
}

func TestEventDefinition_New_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition EventDefinition
		want       error
	}{
		{
			name: "missing key",
			definition: EventDefinition{
				ResourceType: "transaction",
				EventType:    "created",
			},
			want: ErrInvalidEventDefinition,
		},
		{
			name: "missing resource",
			definition: EventDefinition{
				Key:       "transaction.created",
				EventType: "created",
			},
			want: ErrMissingResourceType,
		},
		{
			name: "missing event",
			definition: EventDefinition{
				Key:          "transaction.created",
				ResourceType: "transaction",
			},
			want: ErrMissingEventType,
		},
		{
			name: "invalid policy",
			definition: EventDefinition{
				Key:           "transaction.created",
				ResourceType:  "transaction",
				EventType:     "created",
				DefaultPolicy: DeliveryPolicy{Enabled: true, Direct: DirectMode("async")},
			},
			want: ErrInvalidDeliveryPolicy,
		},
		{
			name: "control char in resource",
			definition: EventDefinition{
				Key:          "transaction.created",
				ResourceType: "transaction\n",
				EventType:    "created",
			},
			want: ErrInvalidResourceType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewEventDefinition(tt.definition)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewEventDefinition() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
			if !errors.Is(err, ErrInvalidEventDefinition) {
				t.Fatalf("NewEventDefinition() error = %v; want ErrInvalidEventDefinition", err)
			}
		})
	}
}

// TestEventDefinition_Topic_AppendsVersionSuffixForMajorV2Plus locks the
// contract documented on (*Event).Topic: SchemaVersion major >= 2 appends
// ".v<major>" to the base topic; majors < 2 and non-semver strings fall
// through to the base form.
func TestEventDefinition_Topic_AppendsVersionSuffixForMajorV2Plus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		schemaVersion string
		want          string
	}{
		{
			name:          "major 1 uses base form",
			schemaVersion: "1.0.0",
			want:          "lerian.streaming.payment.authorized",
		},
		{
			name:          "major 2 appends .v2",
			schemaVersion: "2.0.0",
			want:          "lerian.streaming.payment.authorized.v2",
		},
		{
			name:          "major 3 appends .v3 for 3.5.7",
			schemaVersion: "3.5.7",
			want:          "lerian.streaming.payment.authorized.v3",
		},
		{
			name:          "major 0 uses base form for 0.9.0",
			schemaVersion: "0.9.0",
			want:          "lerian.streaming.payment.authorized",
		},
		{
			name:          "non-semver falls through to base form",
			schemaVersion: "not-a-semver",
			want:          "lerian.streaming.payment.authorized",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			definition, err := NewEventDefinition(EventDefinition{
				Key:           "payment.authorized",
				ResourceType:  "payment",
				EventType:     "authorized",
				SchemaVersion: tt.schemaVersion,
			})
			if err != nil {
				t.Fatalf("NewEventDefinition() error = %v", err)
			}

			if got := definition.Topic(); got != tt.want {
				t.Errorf("Topic() = %q; want %q", got, tt.want)
			}
		})
	}
}
