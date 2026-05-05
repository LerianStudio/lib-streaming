//go:build unit

package transport

import "testing"

// TestIsNilInterface_Matrix pins every nilable / non-nilable Kind that
// IsNilInterface must classify, including the notorious typed-nil
// interface case the helper exists to defend against.
//
// The contract under test (see transport.go:24-28):
//
//   - Untyped nil (v == nil) → true
//   - Typed-nil chan, func, interface, map, pointer, slice → true
//   - Any non-nilable kind (int, string, struct value, ...) → false
//
// Each case here exercises a distinct reflect.Kind branch in
// IsNilInterface. Without these direct cases, the helper is only
// exercised indirectly through 3 adapter constructors (SQS, RabbitMQ,
// EventBridge) — losing this test would mean a refactor could silently
// flip the behavior on rare kinds (chan, func, map) without any test
// surface failing.
func TestIsNilInterface_Matrix(t *testing.T) {
	t.Parallel()

	type fooStruct struct{ x int }

	var (
		nilChan   chan int
		nilFunc   func()
		nilMap    map[string]int
		nilPtr    *fooStruct
		nilSlice  []int
		validPtr  = &fooStruct{x: 1}
		validChan = make(chan int)
		validFunc = func() {}
		validMap  = map[string]int{"k": 1}
		// Typed-nil through an explicit interface variable. Triggers
		// the reflect.Interface branch when we wrap into any().
		typedNilInterface = func() any {
			var p *fooStruct
			return p
		}()
	)

	tests := []struct {
		name string
		v    any
		want bool
	}{
		{"untyped nil", nil, true},
		{"typed nil chan", nilChan, true},
		{"typed nil func", nilFunc, true},
		{"typed nil map", nilMap, true},
		{"typed nil pointer", nilPtr, true},
		{"typed nil slice", nilSlice, true},
		// typed-nil pointer wrapped in an explicit any() (the canonical
		// gotcha case the helper was named after).
		{"typed nil through any cast", typedNilInterface, true},
		{"valid chan", validChan, false},
		{"valid func", validFunc, false},
		{"valid map", validMap, false},
		{"valid pointer", validPtr, false},
		{"valid slice", []int{1, 2}, false},
		{"struct value (non-nilable kind)", fooStruct{x: 7}, false},
		// Non-nilable scalar kinds — guard against IsNil panicking on
		// them (which it would, if the Kind switch ever lost a case).
		{"int zero (non-nilable kind)", 0, false},
		{"int positive", 42, false},
		{"string empty", "", false},
		{"string non-empty", "hello", false},
		{"bool false", false, false},
		{"float zero", 0.0, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsNilInterface(tt.v); got != tt.want {
				t.Errorf("IsNilInterface(%T %#v) = %v; want %v", tt.v, tt.v, got, tt.want)
			}
		})
	}
}

func TestCloneHeadersDeepCopiesValues(t *testing.T) {
	t.Parallel()

	headers := []Header{{Key: "ce-id", Value: []byte("event-1")}}
	clone := CloneHeaders(headers)

	headers[0].Key = "mutated"
	headers[0].Value[0] = 'X'

	if clone[0].Key != "ce-id" {
		t.Fatalf("clone[0].Key = %q; want ce-id", clone[0].Key)
	}
	if string(clone[0].Value) != "event-1" {
		t.Fatalf("clone[0].Value = %q; want event-1", string(clone[0].Value))
	}

	clone[0].Value[0] = 'Y'
	if string(headers[0].Value) != "Xvent-1" {
		t.Fatalf("CloneHeaders() shared backing array: source = %q", string(headers[0].Value))
	}
}

func TestCloneHeadersEmptyReturnsNil(t *testing.T) {
	t.Parallel()

	if got := CloneHeaders(nil); got != nil {
		t.Fatalf("CloneHeaders(nil) = %#v; want nil", got)
	}
}
