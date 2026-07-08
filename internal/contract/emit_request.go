package contract

import (
	"encoding/json"
	"fmt"
	"time"
)

// EmitRequest is the runtime request a service passes when emitting a cataloged
// event. The caller supplies business-time data and the catalog definition key;
// the library resolves CloudEvents metadata from the Catalog.
type EmitRequest struct {
	DefinitionKey  string
	TenantID       string
	Subject        string
	EventID        string
	Timestamp      time.Time
	Payload        json.RawMessage
	PolicyOverride DeliveryPolicyOverride
}

// newEmitRequest validates and optionally copies an EmitRequest. Validates
// request-local shape only (definition key, policy, header fields, size cap);
// catalog lookup and system-event tenant rules require the EventDefinition and
// happen during emit resolution.
//
// JSON-payload validity is NOT checked here: it is content-type-aware and the
// request does not carry a DataContentType (that comes from the catalog
// definition). The json.Valid gate runs during emit resolution once the
// definition's DataContentType is known — see resolveEventWithPolicy. The size
// cap stays here and is content-type-agnostic: it protects Kafka's
// max.message.bytes regardless of payload encoding.
func newEmitRequest(request EmitRequest, copyPayload bool) (EmitRequest, error) {
	if request.DefinitionKey == "" {
		return EmitRequest{}, fmt.Errorf("%w: empty definition key", ErrInvalidEventDefinition)
	}

	if err := request.PolicyOverride.Validate(); err != nil {
		return EmitRequest{}, err
	}

	if err := validateEmitRequestHeaderFields(request); err != nil {
		return EmitRequest{}, err
	}

	if len(request.Payload) > MaxPayloadBytes {
		return EmitRequest{}, ErrPayloadTooLarge
	}

	if copyPayload {
		request.Payload = append(json.RawMessage(nil), request.Payload...)
		if request.PolicyOverride.Enabled != nil {
			enabled := *request.PolicyOverride.Enabled
			request.PolicyOverride.Enabled = &enabled
		}
	}

	return request, nil
}

// NewEmitRequest validates and defensively copies an EmitRequest for callers.
func NewEmitRequest(request EmitRequest) (EmitRequest, error) {
	return newEmitRequest(request, true)
}

// NewEmitRequestNoCopy validates an EmitRequest without copying Payload.
// Internal hot paths use this when they immediately copy into an Event value.
func NewEmitRequestNoCopy(request EmitRequest) (EmitRequest, error) {
	return newEmitRequest(request, false)
}

func validateEmitRequestHeaderFields(request EmitRequest) error {
	// DefinitionKey malformed-shape faults map to ErrInvalidEventDefinition
	// (control chars, too long, empty bytes), NOT ErrUnknownEventDefinition —
	// "unknown" is a catalog-lookup miss (key not registered), while "invalid"
	// is a structural fault. Conflating the two used to mislead callers.
	checks := [...]HeaderFieldCheck{
		{Value: request.DefinitionKey, MaxBytes: MaxEventIDBytes, Sentinel: ErrInvalidEventDefinition},
		{Value: request.TenantID, MaxBytes: MaxTenantIDBytes, Sentinel: ErrInvalidTenantID},
		{Value: request.Subject, MaxBytes: MaxSubjectBytes, Sentinel: ErrInvalidSubject},
		{Value: request.EventID, MaxBytes: MaxEventIDBytes, Sentinel: ErrInvalidEventID},
	}

	for _, c := range checks {
		if len(c.Value) > c.MaxBytes || HasControlChar(c.Value) {
			return c.Sentinel
		}
	}

	return nil
}
