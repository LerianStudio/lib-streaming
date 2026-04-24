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

func TestEventDefinition_PartitionKey(t *testing.T) {
	t.Parallel()

	tenantDefinition := EventDefinition{EventType: "created"}
	if got := tenantDefinition.PartitionKey("tenant-1"); got != "tenant-1" {
		t.Errorf("PartitionKey() = %q; want tenant key", got)
	}

	systemDefinition := EventDefinition{EventType: "settled", SystemEvent: true}
	if got := systemDefinition.PartitionKey("ignored"); got != "system:settled" {
		t.Errorf("PartitionKey() = %q; want system key", got)
	}
}
