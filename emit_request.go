package streaming

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

// NewEmitRequest validates and copies an EmitRequest. It validates request-local
// shape only; catalog lookup and system-event tenant rules require the
// EventDefinition and happen during emit resolution.
func NewEmitRequest(request EmitRequest) (EmitRequest, error) {
	return newEmitRequest(request, true)
}

func newEmitRequest(request EmitRequest, copyPayload bool) (EmitRequest, error) {
	if request.DefinitionKey == "" {
		return EmitRequest{}, fmt.Errorf("%w: empty definition key", ErrUnknownEventDefinition)
	}

	if err := request.PolicyOverride.Validate(); err != nil {
		return EmitRequest{}, err
	}

	if err := validateEmitRequestHeaderFields(request); err != nil {
		return EmitRequest{}, err
	}

	if len(request.Payload) > maxPayloadBytes {
		return EmitRequest{}, ErrPayloadTooLarge
	}

	if !json.Valid(request.Payload) {
		return EmitRequest{}, ErrNotJSON
	}

	if copyPayload {
		request.Payload = append(json.RawMessage(nil), request.Payload...)
	}

	return request, nil
}

func validateEmitRequestHeaderFields(request EmitRequest) error {
	checks := [...]headerFieldCheck{
		{request.DefinitionKey, maxEventIDBytes, ErrUnknownEventDefinition},
		{request.TenantID, maxTenantIDBytes, ErrInvalidTenantID},
		{request.Subject, maxSubjectBytes, ErrInvalidSubject},
		{request.EventID, maxEventIDBytes, ErrInvalidEventID},
	}

	for _, c := range checks {
		if len(c.value) > c.maxBytes || hasControlChar(c.value) {
			return c.sentinel
		}
	}

	return nil
}
