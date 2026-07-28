//go:build unit

package billing_test

import (
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/LerianStudio/lib-streaming/v2/billing"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		payload         *billing.BillablePayload
		wantErr         bool
		wantErrContains string
	}{
		{
			name: "count event with no properties is valid",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
			},
			wantErr: false,
		},
		{
			name: "well-formed payload with string and number properties is valid",
			payload: &billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionId: "sub_123",
				Timestamp:      timestamppb.New(time.Date(2026, time.July, 24, 10, 0, 0, 0, time.UTC)),
				Properties: map[string]*billing.PropertyValue{
					"region": billing.StringProperty("br"),
					"amount": billing.NumberProperty(12.5),
				},
				PreciseTotalAmountCents: new("999.99"),
			},
			wantErr: false,
		},
		{
			name: "number-only property is valid",
			payload: &billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"count": billing.NumberProperty(9_000_000_000)},
			},
			wantErr: false,
		},
		{
			name: "string-only property is valid",
			payload: &billing.BillablePayload{
				Metric:         "storage_gb",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"region": billing.StringProperty("eu")},
			},
			wantErr: false,
		},
		{
			name: "valid decimal PreciseTotalAmountCents is valid",
			payload: &billing.BillablePayload{
				Metric:                  "storage_gb",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("12345.67"),
			},
			wantErr: false,
		},
		{
			name: "integer-string PreciseTotalAmountCents is valid",
			payload: &billing.BillablePayload{
				Metric:                  "storage_gb",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("12345"),
			},
			wantErr: false,
		},
		{
			name: "empty Metric is rejected",
			payload: &billing.BillablePayload{
				Metric:         "",
				SubscriptionId: "sub_123",
			},
			wantErr:         true,
			wantErrContains: "Metric is required",
		},
		{
			name: "whitespace-only Metric is rejected",
			payload: &billing.BillablePayload{
				Metric:         "   ",
				SubscriptionId: "sub_123",
			},
			wantErr:         true,
			wantErrContains: "Metric is required",
		},
		{
			name: "empty SubscriptionId is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "",
			},
			wantErr:         true,
			wantErrContains: "SubscriptionID is required",
		},
		{
			name: "whitespace-only SubscriptionId is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "\t\n ",
			},
			wantErr:         true,
			wantErrContains: "SubscriptionID is required",
		},
		{
			name: "malformed PreciseTotalAmountCents is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("not-a-number"),
			},
			wantErr:         true,
			wantErrContains: "not a valid decimal string",
		},
		{
			name: "empty PreciseTotalAmountCents string is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new(""),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with multiple dots is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("1.2.3"),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with leading dot is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new(".5"),
			},
			wantErr: true,
		},
		{
			name: "PreciseTotalAmountCents with trailing dot is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("12."),
			},
			wantErr: true,
		},
		{
			name: "over-limit Metric is rejected",
			payload: &billing.BillablePayload{
				Metric:         strings.Repeat("a", 256),
				SubscriptionId: "sub_123",
			},
			wantErr:         true,
			wantErrContains: "Metric exceeds",
		},
		{
			name: "Metric at 255-byte limit is accepted",
			payload: &billing.BillablePayload{
				Metric:         strings.Repeat("a", 255),
				SubscriptionId: "sub_123",
			},
			wantErr: false,
		},
		{
			name: "over-limit SubscriptionId is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: strings.Repeat("s", 256),
			},
			wantErr:         true,
			wantErrContains: "SubscriptionID exceeds",
		},
		{
			name: "SubscriptionId at 255-byte limit is accepted",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: strings.Repeat("s", 255),
			},
			wantErr: false,
		},
		{
			name: "over-limit property key is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{strings.Repeat("k", 129): billing.StringProperty("ok")},
			},
			wantErr:         true,
			wantErrContains: "property key",
		},
		{
			name: "property key at 128-byte limit is accepted",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{strings.Repeat("k", 128): billing.StringProperty("ok")},
			},
			wantErr: false,
		},
		{
			name: "over-limit property string value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"blob": billing.StringProperty(strings.Repeat("v", 1025))},
			},
			wantErr:         true,
			wantErrContains: "string value exceeds",
		},
		{
			name: "property string value at 1024-byte limit is accepted",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"blob": billing.StringProperty(strings.Repeat("v", 1024))},
			},
			wantErr: false,
		},
		{
			name: "over-limit PreciseTotalAmountCents is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new(strings.Repeat("1", 65)),
			},
			wantErr:         true,
			wantErrContains: "PreciseTotalAmountCents exceeds",
		},
		{
			name: "PreciseTotalAmountCents at 64-byte limit is accepted",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new(strings.Repeat("1", 64)),
			},
			wantErr: false,
		},
		{
			name: "large finite number property value is accepted (numbers have no length cap)",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"huge": billing.NumberProperty(1e300)},
			},
			wantErr: false,
		},
		{
			name: "positive-infinity number property value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"amount": billing.NumberProperty(math.Inf(1))},
			},
			wantErr:         true,
			wantErrContains: "must be a finite number",
		},
		{
			name: "negative-infinity number property value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"amount": billing.NumberProperty(math.Inf(-1))},
			},
			wantErr:         true,
			wantErrContains: "must be a finite number",
		},
		{
			name: "NaN number property value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"amount": billing.NumberProperty(math.NaN())},
			},
			wantErr:         true,
			wantErrContains: "must be a finite number",
		},
		{
			name: "nil property value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"k": nil},
			},
			wantErr:         true,
			wantErrContains: "value is nil",
		},
		{
			name: "unset-oneof property value is rejected",
			payload: &billing.BillablePayload{
				Metric:         "api_calls",
				SubscriptionId: "sub_123",
				Properties:     map[string]*billing.PropertyValue{"k": {}},
			},
			wantErr:         true,
			wantErrContains: "has no value set",
		},
		{
			name: "PreciseTotalAmountCents with a sign is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("-5"),
			},
			wantErr:         true,
			wantErrContains: "not a valid decimal string",
		},
		{
			name: "PreciseTotalAmountCents in exponent form is rejected",
			payload: &billing.BillablePayload{
				Metric:                  "api_calls",
				SubscriptionId:          "sub_123",
				PreciseTotalAmountCents: new("1e5"),
			},
			wantErr:         true,
			wantErrContains: "not a valid decimal string",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := billing.Validate(tc.payload)

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

func TestValidate_NilPayloadIsRejected(t *testing.T) {
	t.Parallel()

	// A nil payload must be reported as a validation error, never panic: the
	// generated getters are nil-safe, so Validate degrades to "Metric is
	// required" rather than dereferencing nil.
	if err := billing.Validate(nil); err == nil {
		t.Fatalf("Validate(nil) error = nil, want non-nil")
	}
}

func TestValidate_PropertyCountBoundary(t *testing.T) {
	t.Parallel()

	build := func(n int) *billing.BillablePayload {
		props := make(map[string]*billing.PropertyValue, n)
		for i := range n {
			props["k"+strconv.Itoa(i)] = billing.NumberProperty(float64(i))
		}

		return &billing.BillablePayload{Metric: "api_calls", SubscriptionId: "sub_123", Properties: props}
	}

	// Exactly at the ceiling is accepted; one over is rejected (guards >→>=).
	if err := billing.Validate(build(100)); err != nil {
		t.Fatalf("Validate() with 100 properties error = %v, want nil", err)
	}

	if err := billing.Validate(build(101)); err == nil {
		t.Fatalf("Validate() with 101 properties error = nil, want non-nil")
	}
}
