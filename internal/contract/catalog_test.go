//go:build unit

package contract

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

	_, err = catalog.Require("missing.created")
	if !errors.Is(err, ErrUnknownEventDefinition) {
		t.Fatalf("Require() error = %v; want ErrUnknownEventDefinition", err)
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

// TestCatalog_ZeroValueBehavesAsEmpty locks the contract that a bare
// Catalog{} (never constructed via NewCatalog) behaves as a read-only empty
// catalog: Len is zero, Definitions returns a non-nil empty slice, lookups
// fail cleanly, and Require returns the documented sentinel. Nil-map /
// nil-slice access must not panic.
func TestCatalog_ZeroValueBehavesAsEmpty(t *testing.T) {
	t.Parallel()

	var c Catalog

	if c.Len() != 0 {
		t.Errorf("zero-value Catalog.Len() = %d; want 0", c.Len())
	}

	defs := c.Definitions()
	if defs == nil {
		t.Error("zero-value Catalog.Definitions() = nil; want non-nil empty slice")
	}

	if len(defs) != 0 {
		t.Errorf("zero-value Catalog.Definitions() len = %d; want 0", len(defs))
	}

	if _, ok := c.Lookup("anything"); ok {
		t.Error("zero-value Catalog.Lookup(anything) ok = true; want false")
	}

	if _, err := c.Require("anything"); !errors.Is(err, ErrUnknownEventDefinition) {
		t.Errorf("zero-value Catalog.Require(anything) err = %v; want ErrUnknownEventDefinition", err)
	}
}

// TestNewCatalog_EmptyDefinitionsList asserts NewCatalog() with no args
// returns an empty-but-valid catalog. The empty-catalog sentinel belongs to
// NewProducer wiring — not to Catalog construction itself.
func TestNewCatalog_EmptyDefinitionsList(t *testing.T) {
	t.Parallel()

	c, err := NewCatalog()
	if err != nil {
		t.Fatalf("NewCatalog() err = %v; want nil", err)
	}

	if c.Len() != 0 {
		t.Errorf("NewCatalog().Len() = %d; want 0", c.Len())
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
