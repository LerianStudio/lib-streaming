//go:build unit

package billing_test

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LerianStudio/lib-streaming/v2/billing"
)

func TestBillablePayload_Validate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		payload         billing.BillablePayload
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "count event with no properties is valid",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
			},
			wantErr: false,
		},
		{
			name: "sum event with numeric and string properties is valid",
			payload: billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionID: "sub_123",
				Properties: map[string]any{
					"count":  int(3),
					"big":    int64(9_000_000_000),
					"amount": float64(12.5),
					"region": "br",
				},
			},
			wantErr: false,
		},
		{
			name: "json.Number property value is valid",
			payload: billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"amount": json.Number("12.5")},
			},
			wantErr: false,
		},
		{
			name: "valid decimal PreciseTotalAmountCents is valid",
			payload: billing.BillablePayload{
				Metric:                  "storage_gb",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new("12345.67"),
			},
			wantErr: false,
		},
		{
			name: "integer-string PreciseTotalAmountCents is valid",
			payload: billing.BillablePayload{
				Metric:                  "storage_gb",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new("12345"),
			},
			wantErr: false,
		},
		{
			name: "empty Metric is rejected",
			payload: billing.BillablePayload{
				Metric:         "",
				SubscriptionID: "sub_123",
			},
			wantErr: true,
		},
		{
			name: "empty SubscriptionID is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "",
			},
			wantErr: true,
		},
		{
			name: "bool property value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"flag": true},
			},
			wantErr: true,
		},
		{
			name: "byte-slice property value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"blob": []byte("x")},
			},
			wantErr: true,
		},
		{
			name: "nested map property value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"nested": map[string]any{"a": 1}},
			},
			wantErr: true,
		},
		{
			name: "nil property value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"missing": nil},
			},
			wantErr: true,
		},
		{
			name: "malformed PreciseTotalAmountCents is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new("not-a-number"),
			},
			wantErr: true,
		},
		{
			name: "empty PreciseTotalAmountCents string is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new(""),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with multiple dots is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new("1.2.3"),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with leading dot is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new(".5"),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with trailing dot is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new("12."),
			},
			wantErr: true,
		},
		// --- MEDIUM 4: broaden accepted numeric kinds ---
		{
			name: "int32 / uint64 / float32 property values are valid",
			payload: billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionID: "sub_123",
				Properties: map[string]any{
					"i32": int32(7),
					"u64": uint64(9_000_000_000),
					"f32": float32(1.5),
				},
			},
			wantErr: false,
		},
		// --- MEDIUM 5: reject non-finite / non-marshalable numbers ---
		{
			name: "NaN float64 property is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": math.NaN()},
			},
			wantErr:         true,
			wantErrContains: "non-finite",
		},
		{
			name: "positive infinity float64 property is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": math.Inf(1)},
			},
			wantErr: true,
		},
		{
			name: "negative infinity float64 property is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": math.Inf(-1)},
			},
			wantErr: true,
		},
		{
			name: "NaN float32 property is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": float32(math.NaN())},
			},
			wantErr: true,
		},
		{
			name: "json.Number NaN is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": json.Number("NaN")},
			},
			wantErr: true,
		},
		{
			name: "empty json.Number is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": json.Number("")},
			},
			wantErr: true,
		},
		{
			// Parses as a finite float via strconv but is not a valid JSON
			// number literal (leading '+'); json.Marshal would reject it.
			name: "json.Number with non-JSON grammar is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": json.Number("+5")},
			},
			wantErr: true,
		},
		// --- LOW: trim whitespace-only required fields ---
		{
			name: "whitespace-only Metric is rejected",
			payload: billing.BillablePayload{
				Metric:         "   ",
				SubscriptionID: "sub_123",
			},
			wantErr: true,
		},
		{
			name: "whitespace-only SubscriptionID is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "\t\n ",
			},
			wantErr: true,
		},
		// --- MEDIUM 6: anti-abuse size ceilings ---
		{
			name: "over-limit Metric is rejected",
			payload: billing.BillablePayload{
				Metric:         strings.Repeat("a", 256),
				SubscriptionID: "sub_123",
			},
			wantErr:         true,
			wantErrContains: "Metric exceeds",
		},
		{
			name: "over-limit SubscriptionID is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: strings.Repeat("s", 256),
			},
			wantErr: true,
		},
		{
			name: "over-limit property key is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{strings.Repeat("k", 129): "ok"},
			},
			wantErr: true,
		},
		{
			name: "over-limit property string value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"blob": strings.Repeat("v", 1025)},
			},
			wantErr: true,
		},
		{
			name: "over-limit PreciseTotalAmountCents is rejected",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new(strings.Repeat("1", 65)),
			},
			wantErr:         true,
			wantErrContains: "PreciseTotalAmountCents exceeds",
		},
		{
			name: "over-limit json.Number property value is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"bad": json.Number(strings.Repeat("1", 1025))},
			},
			wantErr:         true,
			wantErrContains: "json.Number value exceeds",
		},
		// --- L-A: Timestamp range guards the marshalability invariant ---
		{
			name: "in-range Timestamp is accepted",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Timestamp:      new(time.Date(2026, time.July, 23, 10, 30, 0, 0, time.UTC)),
			},
			wantErr: false,
		},
		{
			name: "Timestamp year over 9999 is rejected",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Timestamp:      new(time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)),
			},
			wantErr:         true,
			wantErrContains: "Timestamp year",
		},
		// --- Boundary: accept at exactly the ceiling (guards a >→>= regression) ---
		{
			name: "Metric at 255-byte limit is accepted",
			payload: billing.BillablePayload{
				Metric:         strings.Repeat("a", 255),
				SubscriptionID: "sub_123",
			},
			wantErr: false,
		},
		{
			name: "SubscriptionID at 255-byte limit is accepted",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: strings.Repeat("s", 255),
			},
			wantErr: false,
		},
		{
			name: "property key at 128-byte limit is accepted",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{strings.Repeat("k", 128): "ok"},
			},
			wantErr: false,
		},
		{
			name: "property string value at 1024-byte limit is accepted",
			payload: billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionID: "sub_123",
				Properties:     map[string]any{"blob": strings.Repeat("v", 1024)},
			},
			wantErr: false,
		},
		{
			name: "PreciseTotalAmountCents at 64-byte limit is accepted",
			payload: billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionID:          "sub_123",
				PreciseTotalAmountCents: new(strings.Repeat("1", 64)),
			},
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.payload.Validate()

			if tc.wantErr && err == nil {
				t.Fatalf("Validate() error = nil, want non-nil")
			}

			if !tc.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}

			if tc.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrContains)) {
				t.Errorf("Validate() error = %v, want substring %q", err, tc.wantErrContains)
			}
		})
	}
}

func TestBillablePayload_Validate_PropertyErrorNamesKeyAndType(t *testing.T) {
	t.Parallel()

	err := billing.BillablePayload{
		Metric:         "api_calls",
		SubscriptionID: "sub_123",
		Properties:     map[string]any{"flag": true},
	}.Validate()
	if err == nil {
		t.Fatalf("Validate() error = nil, want non-nil")
	}

	msg := err.Error()
	if !strings.Contains(msg, "flag") {
		t.Errorf("error %q does not name the offending key %q", msg, "flag")
	}

	if !strings.Contains(msg, "bool") {
		t.Errorf("error %q does not name the offending type %q", msg, "bool")
	}
}

func TestBilling_MustMarshal_PanicsOnValidationError(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("MustMarshal did not panic on a payload that fails Validate")
		}
	}()

	_ = billing.MustMarshal(billing.BillablePayload{
		Metric:         "",
		SubscriptionID: "sub_123",
	})
}

func TestBillablePayload_Validate_PropertyCountBoundary(t *testing.T) {
	t.Parallel()

	build := func(n int) billing.BillablePayload {
		props := make(map[string]any, n)
		for i := range n {
			props["k"+strconv.Itoa(i)] = i
		}

		return billing.BillablePayload{Metric: "api_calls", SubscriptionID: "sub_123", Properties: props}
	}

	// Exactly at the ceiling is accepted; one over is rejected (guards >→>=).
	if err := build(100).Validate(); err != nil {
		t.Fatalf("Validate() with 100 properties error = %v, want nil", err)
	}

	if err := build(101).Validate(); err == nil {
		t.Fatalf("Validate() with 101 properties error = nil, want non-nil")
	}
}

// TestBillablePayload_Validate_ImpliesMarshalable locks the MEDIUM 5 guarantee:
// a payload Validate accepts is always encodable by json.Marshal. Uses the
// numeric edge (finite float) plus a bounded string and decimal amount.
func TestBillablePayload_Validate_ImpliesMarshalable(t *testing.T) {
	t.Parallel()

	payload := billing.BillablePayload{
		Metric:                  "storage_gb",
		SubscriptionID:          "sub_123",
		Properties:              map[string]any{"amount": 12.5, "n": json.Number("42"), "label": "eu"},
		PreciseTotalAmountCents: new("999.99"),
	}

	if err := payload.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}

	if _, err := json.Marshal(payload); err != nil {
		t.Fatalf("json.Marshal on a Validate-passing payload error = %v; Validate must guarantee marshalability", err)
	}
}
