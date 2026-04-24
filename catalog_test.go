//go:build unit

package streaming

import (
	"errors"
	"testing"
)

func TestCatalog_NewCatalogOrdersDefinitionsDeterministically(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(
		EventDefinition{Key: "transaction.updated", ResourceType: "transaction", EventType: "updated"},
		EventDefinition{Key: "account.created", ResourceType: "account", EventType: "created"},
		EventDefinition{Key: "transaction.created", ResourceType: "transaction", EventType: "created"},
	)
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	got := catalog.Definitions()
	wantKeys := []string{"account.created", "transaction.created", "transaction.updated"}
	if len(got) != len(wantKeys) {
		t.Fatalf("Definitions() len = %d; want %d", len(got), len(wantKeys))
	}

	for i, want := range wantKeys {
		if got[i].Key != want {
			t.Errorf("Definitions()[%d].Key = %q; want %q", i, got[i].Key, want)
		}
	}
}

func TestCatalog_Lookup(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	definition, ok := catalog.Lookup("transaction.created")
	if !ok {
		t.Fatal("Lookup() ok = false; want true")
	}
	if definition.ResourceType != "transaction" {
		t.Errorf("Lookup().ResourceType = %q; want transaction", definition.ResourceType)
	}

	_, err = catalog.MustLookup("missing.created")
	if !errors.Is(err, ErrUnknownEventDefinition) {
		t.Fatalf("MustLookup() error = %v; want ErrUnknownEventDefinition", err)
	}
}

func TestCatalog_DefinitionsReturnsCopy(t *testing.T) {
	t.Parallel()

	catalog, err := NewCatalog(EventDefinition{
		Key:          "transaction.created",
		ResourceType: "transaction",
		EventType:    "created",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	definitions := catalog.Definitions()
	definitions[0].Key = "mutated"

	fresh := catalog.Definitions()
	if fresh[0].Key != "transaction.created" {
		t.Errorf("Definitions() exposed internal storage; got key %q", fresh[0].Key)
	}
}

func TestCatalog_RejectsDuplicates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		definitions []EventDefinition
	}{
		{
			name: "duplicate key",
			definitions: []EventDefinition{
				{Key: "transaction.created", ResourceType: "transaction", EventType: "created"},
				{Key: "transaction.created", ResourceType: "transaction", EventType: "updated"},
			},
		},
		{
			name: "duplicate contract",
			definitions: []EventDefinition{
				{Key: "transaction.created.public", ResourceType: "transaction", EventType: "created"},
				{Key: "transaction.created.internal", ResourceType: "transaction", EventType: "created"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewCatalog(tt.definitions...)
			if !errors.Is(err, ErrDuplicateEventDefinition) {
				t.Fatalf("NewCatalog() error = %v; want ErrDuplicateEventDefinition", err)
			}
		})
	}
}
