//go:build unit

package streaming

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEmitRequest_New_CopiesAndPreservesRuntimeFields(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"id":"tx-1"}`)
	ts := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)

	request, err := newEmitRequest(EmitRequest{
		DefinitionKey: "transaction.created",
		TenantID:      "tenant-1",
		Subject:       "tx-1",
		EventID:       "evt-1",
		Timestamp:     ts,
		Payload:       payload,
	}, true)
	if err != nil {
		t.Fatalf("newEmitRequest() error = %v", err)
	}

	payload[0] = '['
	if string(request.Payload) != `{"id":"tx-1"}` {
		t.Errorf("newEmitRequest() did not copy payload, got %q", string(request.Payload))
	}
	if request.Timestamp != ts {
		t.Errorf("Timestamp = %v; want %v", request.Timestamp, ts)
	}
}

func TestEmitRequest_New_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request EmitRequest
		want    error
	}{
		{
			name:    "missing definition key",
			request: EmitRequest{Payload: json.RawMessage(`{}`)},
			want:    ErrInvalidEventDefinition,
		},
		{
			name: "invalid tenant id",
			request: EmitRequest{
				DefinitionKey: "transaction.created",
				TenantID:      "tenant\n",
				Payload:       json.RawMessage(`{}`),
			},
			want: ErrInvalidTenantID,
		},
		{
			name: "invalid definition key",
			request: EmitRequest{
				DefinitionKey: "transaction\ncreated",
				Payload:       json.RawMessage(`{}`),
			},
			want: ErrInvalidEventDefinition,
		},
		{
			name: "invalid subject",
			request: EmitRequest{
				DefinitionKey: "transaction.created",
				Subject:       "tx\n1",
				Payload:       json.RawMessage(`{}`),
			},
			want: ErrInvalidSubject,
		},
		{
			name: "invalid event id",
			request: EmitRequest{
				DefinitionKey: "transaction.created",
				EventID:       "evt\n1",
				Payload:       json.RawMessage(`{}`),
			},
			want: ErrInvalidEventID,
		},
		{
			name: "payload too large",
			request: EmitRequest{
				DefinitionKey: "transaction.created",
				Payload:       json.RawMessage(make([]byte, maxPayloadBytes+1)),
			},
			want: ErrPayloadTooLarge,
		},
		{
			name: "payload not json",
			request: EmitRequest{
				DefinitionKey: "transaction.created",
				Payload:       json.RawMessage(`not-json`),
			},
			want: ErrNotJSON,
		},
		{
			name: "invalid policy override",
			request: EmitRequest{
				DefinitionKey:  "transaction.created",
				Payload:        json.RawMessage(`{}`),
				PolicyOverride: DeliveryPolicyOverride{DLQ: DLQMode("later")},
			},
			want: ErrInvalidDeliveryPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newEmitRequest(tt.request, true)
			if !errors.Is(err, tt.want) {
				t.Fatalf("newEmitRequest() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}
