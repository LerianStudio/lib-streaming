//go:build unit

package contract

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestDestinationValidate_RejectsSecuritySensitiveTopology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination Destination
	}{
		{name: "URL userinfo", destination: Destination{Kind: TransportSQS, Address: "https://user:pass@sqs.us-east-1.amazonaws.com/123/queue"}},
		{name: "credential query token", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Signature=abc"}},
		{name: "AWS session token query key", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Security-Token=abc"}},
		{name: "snake AWS session token attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_session_token": "redacted"}}},
		{name: "snake AWS secret access key attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_secret_access_key": "redacted"}}},
		{name: "snake AWS access key ID attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_access_key_id": "redacted"}}},
		{name: "bare AWS AKIA value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"external_id": "AKIAIOSFODNN7EXAMPLE"}}},
		{name: "bare AWS ASIA value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"external_id": "ASIAIOSFODNN7EXAMPLE"}}},
		{name: "bearer authorization header value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Authorization: Bearer secret-token"}}},
		{name: "standalone basic authorization value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Basic dXNlcjpwYXNz"}}},
		{name: "standalone token authorization value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Token abc"}}},
		{name: "camel AWS access key query key", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"}},
		{name: "compound AWS credential query key", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Credential=redacted"}},
		{name: "bare signature query key", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?Signature=redacted"}},
		{name: "credential query value", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?filter=credential%3Dsecret"}},
		{name: "camel credential query value", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?filter=AWSAccessKeyId%3DAKIAIOSFODNN7EXAMPLE"}},
		{name: "credential fragment", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue#token=secret"}},
		{name: "compound credential fragment", destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue#X-Amz-Credential=redacted"}},
		{name: "credential attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"authorization": "Bearer redacted"}}},
		{name: "PII attribute key from lib-observability defaults", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"account_number": "redacted"}}},
		{name: "dotted headers authorization attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"headers.Authorization": "redacted"}}},
		{name: "underscored auth header attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"auth_header": "redacted"}}},
		{name: "dotted proxy authorization attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"proxy.authorization": "redacted"}}},
		{name: "dotted authorization header attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"authorization.header": "redacted"}}},
		{name: "dotted x authorization attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"x.authorization": "redacted"}}},
		{name: "camel credential attribute key", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"secretAccessKey": "redacted"}}},
		{name: "credential attribute value", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"metadata": "Bearer secret"}}},
		{name: "credential assignment in name", destination: Destination{Kind: TransportKafkaLike, Name: "events-api_key=redacted"}},
		{name: "camel credential assignment in name", destination: Destination{Kind: TransportKafkaLike, Name: "events-apiKey=redacted"}},
		{name: "credential assignment in non URL address", destination: Destination{Kind: TransportCustom, Address: "sink token=secret"}},
		{name: "camel credential assignment in non URL address", destination: Destination{Kind: TransportCustom, Address: "sink AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"}},
		{name: "tenant token in name", destination: Destination{Kind: TransportKafkaLike, Name: "events-${tenant_id}"}},
		{name: "tenant token in address", destination: Destination{Kind: TransportRabbitMQ, Name: "events", Address: "tenant_id.created"}},
		{name: "tenant token in attributes", destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"partition": ":tenant_id"}}},
		{name: "SQS HTTP URL rejected", destination: Destination{Kind: TransportSQS, Address: "http://sqs.us-east-1.amazonaws.com/123/queue"}},
		{name: "SQS private IP URL rejected", destination: Destination{Kind: TransportSQS, Address: "https://127.0.0.1/123/queue"}},
		{name: "SQS arbitrary public host rejected", destination: Destination{Kind: TransportSQS, Address: "https://example.com/123/queue"}},
		{name: "custom URL private IP rejected", destination: Destination{Kind: TransportCustom, Address: "https://127.0.0.1/sink"}},
		{name: "custom protocol-relative private IP URL rejected", destination: Destination{Kind: TransportCustom, Address: "//127.0.0.1/admin"}},
		{name: "SQS protocol-relative private IP URL rejected", destination: Destination{Kind: TransportSQS, Address: "//127.0.0.1/123/queue"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.destination.Validate()
			if !errors.Is(err, ErrInvalidDestination) {
				t.Fatalf("Validate() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestDestinationValidate_RejectsIncompleteSQSQueuePath(t *testing.T) {
	tests := []string{
		"https://sqs.us-east-1.amazonaws.com/",
		"https://sqs.us-east-1.amazonaws.com/123",
		"https://sqs.us-east-1.amazonaws.com/123/",
		"https://sqs.us-east-1.amazonaws.com/123/q/extra",
	}

	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			err := (Destination{Kind: TransportSQS, Address: raw}).Validate()
			if !errors.Is(err, ErrInvalidDestination) {
				t.Fatalf("Validate() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestValidateSQSQueueURLShape_RejectsCredentialMaterial(t *testing.T) {
	tests := []string{
		"https://user:pass@sqs.us-east-1.amazonaws.com/123/q",
		"https://sqs.us-east-1.amazonaws.com/123/q?X-Amz-Signature=abc",
		"https://sqs.us-east-1.amazonaws.com/123/q#token=secret",
		"//sqs.us-east-1.amazonaws.com/123/q",
	}

	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			if err := ValidateSQSQueueURLShape(raw); !errors.Is(err, ErrInvalidDestination) {
				t.Fatalf("ValidateSQSQueueURLShape() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestResolveAndValidateSQSQueueURL_RetriesAndPreservesCause(t *testing.T) {
	old := resolveSQSQueueURLOnce
	t.Cleanup(func() { resolveSQSQueueURLOnce = old })

	attempts := 0
	transient := errors.New("resolver temporarily unavailable")
	resolveSQSQueueURLOnce = func(context.Context, string) error {
		attempts++
		if attempts < 3 {
			return transient
		}

		return nil
	}

	if err := (Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"}).Validate(); err != nil {
		t.Fatalf("Validate() after transient resolver failures error = %v; want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("resolver attempts = %d; want 3", attempts)
	}

	attempts = 0
	permanent := fmt.Errorf("blocked private range: %w", transient)
	resolveSQSQueueURLOnce = func(context.Context, string) error {
		attempts++
		return permanent
	}

	err := (Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q2"}).Validate()
	if !errors.Is(err, ErrInvalidDestination) {
		t.Fatalf("Validate() error = %v; want ErrInvalidDestination", err)
	}
	if !errors.Is(err, transient) {
		t.Fatalf("Validate() error = %v; want resolver cause preserved", err)
	}
	if attempts != sqsDNSValidationAttempts {
		t.Fatalf("resolver attempts = %d; want %d", attempts, sqsDNSValidationAttempts)
	}
}

func TestNewRouteTable_DeduplicatesSQSResolverPerQueueURL(t *testing.T) {
	old := resolveSQSQueueURLOnce
	t.Cleanup(func() { resolveSQSQueueURLOnce = old })

	attempts := 0
	resolveSQSQueueURLOnce = func(context.Context, string) error {
		attempts++
		return nil
	}

	destination := Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"}
	_, err := NewRouteTable(
		RouteDefinition{Key: "transaction.created.sqs.primary", DefinitionKey: "transaction.created", Target: "primary", Destination: destination},
		RouteDefinition{Key: "transaction.updated.sqs.primary", DefinitionKey: "transaction.updated", Target: "primary", Destination: destination},
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("resolver attempts = %d; want 1 for duplicate queue URL", attempts)
	}
}
