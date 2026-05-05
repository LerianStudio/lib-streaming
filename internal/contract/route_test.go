//go:build unit

package contract

import (
	"errors"
	"strings"
	"testing"
)

func TestRouteDefinition_New_NormalizesAndCopies(t *testing.T) {
	t.Parallel()

	enabled := true
	attrs := map[string]string{"content_type": "application/json"}
	dlqAttrs := map[string]string{"retention": "14d"}

	route, err := NewRouteDefinition(RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination: Destination{
			Kind:       TransportKafkaLike,
			Name:       "lerian.streaming.transaction.created",
			Attributes: attrs,
		},
		Policy: DeliveryPolicyOverride{Enabled: &enabled},
		DLQ: &Destination{
			Kind:       TransportKafkaLike,
			Name:       "lerian.streaming.transaction.created.dlq",
			Attributes: dlqAttrs,
		},
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v", err)
	}

	if route.Requirement != RouteRequired {
		t.Fatalf("Requirement = %q; want %q", route.Requirement, RouteRequired)
	}

	attrs["content_type"] = "mutated"
	dlqAttrs["retention"] = "mutated"
	enabled = false

	if route.Destination.Attributes["content_type"] != "application/json" {
		t.Fatalf("Destination attributes were not copied: %#v", route.Destination.Attributes)
	}
	if route.DLQ == nil || route.DLQ.Attributes["retention"] != "14d" {
		t.Fatalf("DLQ attributes were not copied: %#v", route.DLQ)
	}
	if route.Policy.Enabled == nil || !*route.Policy.Enabled {
		t.Fatalf("Policy override pointer was not copied: %#v", route.Policy.Enabled)
	}
}

func TestRouteDefinition_New_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	base := RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   Destination{Kind: TransportKafkaLike, Name: "topic"},
	}

	tests := []struct {
		name   string
		mutate func(*RouteDefinition)
		want   error
	}{
		{
			name: "missing key",
			mutate: func(route *RouteDefinition) {
				route.Key = ""
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "missing definition key",
			mutate: func(route *RouteDefinition) {
				route.DefinitionKey = ""
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "missing target",
			mutate: func(route *RouteDefinition) {
				route.Target = ""
			},
			want: ErrMissingTarget,
		},
		{
			name: "invalid destination kind",
			mutate: func(route *RouteDefinition) {
				route.Destination.Kind = TransportKind("ftp")
			},
			want: ErrInvalidDestination,
		},
		{
			name: "empty destination",
			mutate: func(route *RouteDefinition) {
				route.Destination.Name = ""
				route.Destination.Address = ""
			},
			want: ErrInvalidDestination,
		},
		{
			name: "invalid requirement",
			mutate: func(route *RouteDefinition) {
				route.Requirement = RouteRequirement("sometimes")
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "invalid policy",
			mutate: func(route *RouteDefinition) {
				route.Policy.Direct = DirectMode("async")
			},
			want: ErrInvalidDeliveryPolicy,
		},
		{
			name: "control char in route key",
			mutate: func(route *RouteDefinition) {
				route.Key = "transaction.created\n"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "display phrase route key",
			mutate: func(route *RouteDefinition) {
				route.Key = "Transaction Created Kafka Primary"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "uppercase route key",
			mutate: func(route *RouteDefinition) {
				route.Key = "transaction.Created.kafka.primary"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "spaces in route key",
			mutate: func(route *RouteDefinition) {
				route.Key = "transaction.created kafka.primary"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "empty route key segment",
			mutate: func(route *RouteDefinition) {
				route.Key = "transaction..created.kafka.primary"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "invalid DLQ destination",
			mutate: func(route *RouteDefinition) {
				dlq := Destination{Kind: TransportSQS, Address: "not-a-queue-url"}
				route.DLQ = &dlq
			},
			want: ErrInvalidDestination,
		},
		{
			name: "credential-like destination attribute key",
			mutate: func(route *RouteDefinition) {
				route.Destination.Attributes = map[string]string{"api_key": "redacted"}
			},
			want: ErrInvalidDestination,
		},
		{
			name: "tenant placeholder in route topology",
			mutate: func(route *RouteDefinition) {
				route.Target = "primary-${tenant_id}"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "credential assignment in route key",
			mutate: func(route *RouteDefinition) {
				route.Key = "primary-api_key=secret"
			},
			want: ErrInvalidRouteDefinition,
		},
		{
			name: "bearer assignment in route target",
			mutate: func(route *RouteDefinition) {
				route.Target = "target-bearer=redacted"
			},
			want: ErrInvalidRouteDefinition,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			route := base
			tt.mutate(&route)

			_, err := NewRouteDefinition(route)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewRouteDefinition() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
			if !errors.Is(err, ErrInvalidRouteDefinition) {
				t.Fatalf("NewRouteDefinition() error = %v; want ErrInvalidRouteDefinition", err)
			}
		})
	}
}

// TestRouteDefinition_New_RejectsCrossKindDLQ pins the cross-transport DLQ
// guard. publishRouteDLQ delivers via the source route's target adapter,
// which rejects mismatched destinations with ErrInvalidDestination — every
// DLQ message would silently drop. NewRouteDefinition fails closed instead.
func TestRouteDefinition_New_RejectsCrossKindDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		destKind TransportKind
		dest     Destination
		dlqKind  TransportKind
		dlq      Destination
	}{
		{
			name:     "Kafka source, SQS DLQ",
			destKind: TransportKafkaLike,
			dest:     Destination{Kind: TransportKafkaLike, Name: "source.topic"},
			dlqKind:  TransportSQS,
			dlq:      Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
		},
		{
			name:     "SQS source, Kafka DLQ",
			destKind: TransportSQS,
			dest:     Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/q"},
			dlqKind:  TransportKafkaLike,
			dlq:      Destination{Kind: TransportKafkaLike, Name: "fallback.dlq"},
		},
		{
			name:     "RabbitMQ source, EventBridge DLQ",
			destKind: TransportRabbitMQ,
			dest:     Destination{Kind: TransportRabbitMQ, Name: "events", Address: "tx.created"},
			dlqKind:  TransportEventBridge,
			dlq:      Destination{Kind: TransportEventBridge, Name: "default-bus"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dlq := tt.dlq
			_, err := NewRouteDefinition(RouteDefinition{
				Key:           "transaction.created.kafka.primary",
				DefinitionKey: "transaction.created",
				Target:        "primary",
				Destination:   tt.dest,
				DLQ:           &dlq,
			})
			if !errors.Is(err, ErrInvalidRouteDefinition) {
				t.Fatalf("NewRouteDefinition() error = %v; want ErrInvalidRouteDefinition (cross-kind DLQ)", err)
			}
		})
	}
}

// TestRouteDefinition_New_AllowsSameKindDLQ pins the matching-kind happy
// path so the cross-kind guard does not over-reach.
func TestRouteDefinition_New_AllowsSameKindDLQ(t *testing.T) {
	t.Parallel()

	dlq := Destination{Kind: TransportKafkaLike, Name: "source.topic.dlq"}
	_, err := NewRouteDefinition(RouteDefinition{
		Key:           "transaction.created.kafka.primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   Destination{Kind: TransportKafkaLike, Name: "source.topic"},
		DLQ:           &dlq,
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v; want nil for matching DLQ kind", err)
	}
}

func TestRouteDefinition_New_AllowsCanonicalRouteKey(t *testing.T) {
	t.Parallel()

	_, err := NewRouteDefinition(RouteDefinition{
		Key:           "transaction.created.kafka-primary",
		DefinitionKey: "transaction.created",
		Target:        "primary",
		Destination:   Destination{Kind: TransportKafkaLike, Name: "topic"},
	})
	if err != nil {
		t.Fatalf("NewRouteDefinition() error = %v; want nil", err)
	}
}

func TestDestinationValidate_KindSpecificRules(t *testing.T) {
	t.Parallel()

	valid := []Destination{
		{Kind: TransportKafkaLike, Name: "lerian.streaming.transaction.created"},
		{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue"},
		{Kind: TransportRabbitMQ, Name: "events", Address: "transaction.created"},
		{Kind: TransportEventBridge, Name: "default"},
		{Kind: TransportCustom, Name: "custom-sink"},
		{Kind: TransportCustom, Address: "https://example.test/sink"},
	}

	for _, destination := range valid {
		destination := destination
		t.Run(string(destination.Kind), func(t *testing.T) {
			t.Parallel()

			if err := destination.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestDestinationValidate_RejectsInvalidKindSpecificShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination Destination
	}{
		{
			name:        "Kafka missing name",
			destination: Destination{Kind: TransportKafkaLike},
		},
		{
			name:        "Kafka rejects address",
			destination: Destination{Kind: TransportKafkaLike, Name: "topic", Address: "https://broker.example.test/topic"},
		},
		{
			name:        "SQS rejects name",
			destination: Destination{Kind: TransportSQS, Name: "queue", Address: "https://sqs.us-east-1.amazonaws.com/123/queue"},
		},
		{
			name:        "RabbitMQ missing name",
			destination: Destination{Kind: TransportRabbitMQ, Address: "transaction.created"},
		},
		{
			name:        "RabbitMQ missing address",
			destination: Destination{Kind: TransportRabbitMQ, Name: "events"},
		},
		{
			name:        "EventBridge missing name",
			destination: Destination{Kind: TransportEventBridge},
		},
		{
			name:        "EventBridge rejects address",
			destination: Destination{Kind: TransportEventBridge, Name: "default", Address: "arn:aws:events:us-east-1:123:event-bus/default"},
		},
		{
			name:        "Custom missing both name and address",
			destination: Destination{Kind: TransportCustom},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.destination.Validate(); !errors.Is(err, ErrInvalidDestination) {
				t.Fatalf("Validate() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestDestinationValidate_RejectsHeaderBoundaryViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination Destination
	}{
		{
			name:        "name control char",
			destination: Destination{Kind: TransportKafkaLike, Name: "topic\ninvalid"},
		},
		{
			name:        "name too long",
			destination: Destination{Kind: TransportKafkaLike, Name: strings.Repeat("a", MaxDataSchemaBytes+1)},
		},
		{
			name:        "address control char",
			destination: Destination{Kind: TransportRabbitMQ, Name: "events", Address: "routing\nkey"},
		},
		{
			name:        "address too long",
			destination: Destination{Kind: TransportRabbitMQ, Name: "events", Address: strings.Repeat("a", MaxDataSchemaBytes+1)},
		},
		{
			name:        "attribute key control char",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"bad\nkey": "value"}},
		},
		{
			name:        "attribute key too long",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{strings.Repeat("a", MaxEventIDBytes+1): "value"}},
		},
		{
			name:        "attribute value control char",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"key": "bad\nvalue"}},
		},
		{
			name:        "attribute value too long",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"key": strings.Repeat("a", MaxDataSchemaBytes+1)}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := tt.destination.Validate(); !errors.Is(err, ErrInvalidDestination) {
				t.Fatalf("Validate() error = %v; want ErrInvalidDestination", err)
			}
		})
	}
}

func TestDestinationValidate_RejectsSecuritySensitiveTopology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination Destination
	}{
		{
			name:        "URL userinfo",
			destination: Destination{Kind: TransportSQS, Address: "https://user:pass@sqs.us-east-1.amazonaws.com/123/queue"},
		},
		{
			name:        "credential query token",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Signature=abc"},
		},
		{
			name:        "AWS session token query key",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Security-Token=abc"},
		},
		{
			name:        "snake AWS session token attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_session_token": "redacted"}},
		},
		{
			name:        "snake AWS secret access key attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_secret_access_key": "redacted"}},
		},
		{
			name:        "snake AWS access key ID attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"aws_access_key_id": "redacted"}},
		},
		{
			name:        "bare AWS AKIA value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"external_id": "AKIAIOSFODNN7EXAMPLE"}},
		},
		{
			name:        "bare AWS ASIA value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"external_id": "ASIAIOSFODNN7EXAMPLE"}},
		},
		{
			name:        "bearer authorization header value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Authorization: Bearer secret-token"}},
		},
		{
			name:        "standalone basic authorization value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Basic dXNlcjpwYXNz"}},
		},
		{
			name:        "standalone token authorization value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"header": "Token abc"}},
		},
		{
			name:        "camel AWS access key query key",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"},
		},
		{
			name:        "compound AWS credential query key",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?X-Amz-Credential=redacted"},
		},
		{
			name:        "bare signature query key",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?Signature=redacted"},
		},
		{
			name:        "credential query value",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?filter=credential%3Dsecret"},
		},
		{
			name:        "camel credential query value",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue?filter=AWSAccessKeyId%3DAKIAIOSFODNN7EXAMPLE"},
		},
		{
			name:        "credential fragment",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue#token=secret"},
		},
		{
			name:        "compound credential fragment",
			destination: Destination{Kind: TransportSQS, Address: "https://sqs.us-east-1.amazonaws.com/123/queue#X-Amz-Credential=redacted"},
		},
		{
			name:        "credential attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"authorization": "Bearer redacted"}},
		},
		{
			name:        "dotted headers authorization attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"headers.Authorization": "redacted"}},
		},
		{
			name:        "underscored auth header attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"auth_header": "redacted"}},
		},
		{
			name:        "dotted proxy authorization attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"proxy.authorization": "redacted"}},
		},
		{
			name:        "dotted authorization header attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"authorization.header": "redacted"}},
		},
		{
			name:        "dotted x authorization attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"x.authorization": "redacted"}},
		},
		{
			name:        "camel credential attribute key",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"secretAccessKey": "redacted"}},
		},
		{
			name:        "credential attribute value",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"metadata": "Bearer secret"}},
		},
		{
			name:        "credential assignment in name",
			destination: Destination{Kind: TransportKafkaLike, Name: "events-api_key=redacted"},
		},
		{
			name:        "camel credential assignment in name",
			destination: Destination{Kind: TransportKafkaLike, Name: "events-apiKey=redacted"},
		},
		{
			name:        "credential assignment in non URL address",
			destination: Destination{Kind: TransportCustom, Address: "sink token=secret"},
		},
		{
			name:        "camel credential assignment in non URL address",
			destination: Destination{Kind: TransportCustom, Address: "sink AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE"},
		},
		{
			name:        "tenant token in name",
			destination: Destination{Kind: TransportKafkaLike, Name: "events-${tenant_id}"},
		},
		{
			name:        "tenant token in address",
			destination: Destination{Kind: TransportRabbitMQ, Name: "events", Address: "tenant_id.created"},
		},
		{
			name:        "tenant token in attributes",
			destination: Destination{Kind: TransportCustom, Name: "custom", Attributes: map[string]string{"partition": ":tenant_id"}},
		},
		{
			name:        "SQS HTTP URL rejected",
			destination: Destination{Kind: TransportSQS, Address: "http://sqs.us-east-1.amazonaws.com/123/queue"},
		},
		{
			name:        "SQS private IP URL rejected",
			destination: Destination{Kind: TransportSQS, Address: "https://127.0.0.1/123/queue"},
		},
		{
			name:        "custom URL private IP rejected",
			destination: Destination{Kind: TransportCustom, Address: "https://127.0.0.1/sink"},
		},
		{
			name:        "custom protocol-relative private IP URL rejected",
			destination: Destination{Kind: TransportCustom, Address: "//127.0.0.1/admin"},
		},
		{
			name:        "SQS protocol-relative private IP URL rejected",
			destination: Destination{Kind: TransportSQS, Address: "//127.0.0.1/123/queue"},
		},
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

func TestDestinationValidate_AllowsNonCredentialBusinessWords(t *testing.T) {
	t.Parallel()

	destinations := []Destination{
		{Kind: TransportKafkaLike, Name: "events.tokenization.completed"},
		{Kind: TransportKafkaLike, Name: "payment.authorization.approved"},
		{Kind: TransportKafkaLike, Name: "lerian.streaming.token.created"},
	}

	for _, destination := range destinations {
		destination := destination
		t.Run(destination.Name, func(t *testing.T) {
			t.Parallel()

			if err := destination.Validate(); err != nil {
				t.Fatalf("Validate() error = %v; want nil", err)
			}
		})
	}
}

func TestCredentialLikeMaterial_RejectionBalance(t *testing.T) {
	t.Parallel()

	allowed := []string{
		"payment.authorization.approved",
		"lerian.streaming.token.created",
		"events.tokenization.completed",
	}
	for _, value := range allowed {
		value := value
		t.Run("allow/"+value, func(t *testing.T) {
			t.Parallel()

			if ContainsCredentialLikeMaterial(value) {
				t.Fatalf("ContainsCredentialLikeMaterial(%q) = true; want false", value)
			}
		})
	}

	rejected := []string{
		"primary-api_key=secret",
		"target-bearer=redacted",
		"Authorization: Bearer redacted",
		"Basic dXNlcjpwYXNz",
		"Token abc",
		"AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE",
		"aws_session_token",
		"aws_secret_access_key",
		"aws_access_key_id",
		"headers.Authorization",
		"auth_header",
		"proxy.authorization",
		"authorization.header",
		"x.authorization",
		"X-Amz-Security-Token",
		"AKIAIOSFODNN7EXAMPLE",
		"ASIAIOSFODNN7EXAMPLE",
		"Signature=redacted",
		"secretAccessKey",
		"clientSecret",
		"apiKey",
	}
	for _, value := range rejected {
		value := value
		t.Run("reject/"+value, func(t *testing.T) {
			t.Parallel()

			if !ContainsCredentialLikeMaterial(value) {
				t.Fatalf("ContainsCredentialLikeMaterial(%q) = false; want true", value)
			}
		})
	}
}

func TestRouteTable_NewRouteTableOrdersDefinitionsDeterministically(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(
		routeForTest("transaction.updated.kafka", "transaction.updated"),
		routeForTest("account.created.kafka", "account.created"),
		routeForTest("transaction.created.kafka", "transaction.created"),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	got := table.Definitions()
	wantKeys := []string{"account.created.kafka", "transaction.created.kafka", "transaction.updated.kafka"}
	if len(got) != len(wantKeys) {
		t.Fatalf("Definitions() len = %d; want %d", len(got), len(wantKeys))
	}

	for i, want := range wantKeys {
		if got[i].Key != want {
			t.Errorf("Definitions()[%d].Key = %q; want %q", i, got[i].Key, want)
		}
	}
}

func TestRouteTable_RoutesReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(routeForTest("transaction.created.kafka", "transaction.created"))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	routes := table.Routes("transaction.created")
	routes[0].Key = "mutated"
	routes[0].Destination.Attributes["format"] = "mutated"
	if routes[0].Policy.Enabled != nil {
		*routes[0].Policy.Enabled = false
	}
	if routes[0].DLQ != nil {
		routes[0].DLQ.Attributes["format"] = "mutated"
	}

	fresh := table.Routes("transaction.created")
	if fresh[0].Key != "transaction.created.kafka" {
		t.Fatalf("Routes() exposed route storage: %q", fresh[0].Key)
	}
	if fresh[0].Destination.Attributes["format"] != "json" {
		t.Fatalf("Routes() exposed destination attributes: %#v", fresh[0].Destination.Attributes)
	}
	if fresh[0].Policy.Enabled == nil || !*fresh[0].Policy.Enabled {
		t.Fatalf("Routes() exposed policy enabled pointer: %#v", fresh[0].Policy.Enabled)
	}
	if fresh[0].DLQ == nil || fresh[0].DLQ.Attributes["format"] != "json" {
		t.Fatalf("Routes() exposed DLQ attributes: %#v", fresh[0].DLQ)
	}
}

func TestRouteTable_DefinitionsReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(routeForTest("transaction.created.kafka", "transaction.created"))
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	definitions := table.Definitions()
	definitions[0].Key = "mutated"
	definitions[0].Destination.Attributes["format"] = "mutated"
	if definitions[0].Policy.Enabled != nil {
		*definitions[0].Policy.Enabled = false
	}
	if definitions[0].DLQ != nil {
		definitions[0].DLQ.Attributes["format"] = "mutated"
	}

	fresh := table.Definitions()
	if fresh[0].Key != "transaction.created.kafka" {
		t.Fatalf("Definitions() exposed route storage: %q", fresh[0].Key)
	}
	if fresh[0].Destination.Attributes["format"] != "json" {
		t.Fatalf("Definitions() exposed destination attributes: %#v", fresh[0].Destination.Attributes)
	}
	if fresh[0].Policy.Enabled == nil || !*fresh[0].Policy.Enabled {
		t.Fatalf("Definitions() exposed policy enabled pointer: %#v", fresh[0].Policy.Enabled)
	}
	if fresh[0].DLQ == nil || fresh[0].DLQ.Attributes["format"] != "json" {
		t.Fatalf("Definitions() exposed DLQ attributes: %#v", fresh[0].DLQ)
	}
}

func TestRouteTable_OrdersSameDefinitionByRouteKey(t *testing.T) {
	t.Parallel()

	table, err := NewRouteTable(
		routeForTest("transaction.created.z", "transaction.created"),
		routeForTest("transaction.created.a", "transaction.created"),
	)
	if err != nil {
		t.Fatalf("NewRouteTable() error = %v", err)
	}

	routes := table.Routes("transaction.created")
	if len(routes) != 2 {
		t.Fatalf("Routes() len = %d; want 2", len(routes))
	}
	if routes[0].Key != "transaction.created.a" || routes[1].Key != "transaction.created.z" {
		t.Fatalf("Routes() ordering = [%q %q]; want [transaction.created.a transaction.created.z]", routes[0].Key, routes[1].Key)
	}
}

func TestRouteTable_RejectsDuplicateRouteKeys(t *testing.T) {
	t.Parallel()

	_, err := NewRouteTable(
		routeForTest("transaction.created.kafka", "transaction.created"),
		routeForTest("transaction.created.kafka", "transaction.updated"),
	)
	if !errors.Is(err, ErrDuplicateRouteDefinition) {
		t.Fatalf("NewRouteTable() error = %v; want ErrDuplicateRouteDefinition", err)
	}
}

func TestRouteTable_EmptyRoutesReturnsNoRoutesConfigured(t *testing.T) {
	t.Parallel()

	_, err := NewRouteTable()
	if !errors.Is(err, ErrNoRoutesConfigured) {
		t.Fatalf("NewRouteTable() error = %v; want ErrNoRoutesConfigured", err)
	}
}

func TestRouteTable_ZeroValueBehavesAsEmpty(t *testing.T) {
	t.Parallel()

	var table RouteTable
	if table.Len() != 0 {
		t.Fatalf("zero-value Len() = %d; want 0", table.Len())
	}
	if got := table.Definitions(); got == nil || len(got) != 0 {
		t.Fatalf("zero-value Definitions() = %#v; want non-nil empty", got)
	}
	if got := table.Routes("missing"); got == nil || len(got) != 0 {
		t.Fatalf("zero-value Routes() = %#v; want non-nil empty", got)
	}
}

// TestCredentialLikeMaterial_CloudAndSaaSPatterns covers the regex set
// added for cross-cloud and SaaS credential shapes that the original
// AWS-centric patterns missed. Each pattern is asserted positive (must
// match) AND negative (false-positive controls — common business words
// that look superficially similar must NOT match).
func TestCredentialLikeMaterial_CloudAndSaaSPatterns(t *testing.T) {
	t.Parallel()

	rejected := []struct {
		name  string
		value string
	}{
		// GCP API key: AIza + 35 url-safe chars (39 total).
		// "AIza" (4) + "SyD" (3) + 32 x = 39 chars.
		{"gcp api key", "AIzaSyDxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},
		{"gcp api key in url", "https://maps.googleapis.com/?key=AIzaSyDxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"},

		// Azure SAS query keys at start of input or after &.
		{"azure sas sig at start", "sig=abcDEF123"},
		{"azure sas sv after question mark", "?sv=2020-08-04&sig=signaturepayload"},
		{"azure sas se", "?se=2024-12-31T23:59:59Z"},
		{"azure sas sp", "?sp=racwdl"},

		// GitHub PATs and OAuth tokens.
		{"github classic pat", "ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{"github fine grained pat", "github_pat_11ABCDEFGHIJKLMNOPQRST"},
		{"github oauth user", "ghu_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{"github oauth server", "ghs_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		{"github refresh", "ghr_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},

		// Stripe live/test keys.
		{"stripe live secret", "sk_live_abc123def456ghi789"},
		{"stripe test secret", "sk_test_abc123def456ghi789"},
		{"stripe restricted live", "rk_live_abc123def456ghi789"},
		{"stripe publishable", "pk_test_abc123def456ghi789"},

		// Slack tokens.
		{"slack bot token", "xoxb-1234567890-abcdef123456"},
		{"slack user token", "xoxp-1234567890-abcdef"},
		{"slack app token", "xoxa-1234567890-abcdef"},

		// PEM private key block.
		{"pem rsa private", "-----BEGIN RSA PRIVATE KEY-----"},
		{"pem ec private", "-----BEGIN EC PRIVATE KEY-----"},
		{"pem generic private", "-----BEGIN PRIVATE KEY-----"},
		{"pem opaque private", "noise -----BEGIN OPENSSH PRIVATE KEY----- noise"},

		// Bare JWT (header.payload.signature, base64url).
		{"bare jwt", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature_part_here"},
	}

	for _, tt := range rejected {
		tt := tt
		t.Run("reject/"+tt.name, func(t *testing.T) {
			t.Parallel()

			if !ContainsCredentialLikeMaterial(tt.value) {
				t.Fatalf("ContainsCredentialLikeMaterial(%q) = false; want true", tt.value)
			}
		})
	}

	// False-positive controls: business-domain words and topic-shaped
	// strings that superficially resemble credentials but must pass.
	allowed := []struct {
		name  string
		value string
	}{
		// Tokenization is a payments business term, not a token.
		{"tokenization word", "events.tokenization.completed"},
		// "AIza" by itself is too short (under 35 trailing chars).
		{"short aiza prefix", "AIzaShort"},
		// "ghp_" without 20+ chars is not a PAT.
		{"short github prefix", "ghp_short"},
		// "sk_" without live/test segment is not a Stripe key.
		{"sk without live or test", "sk_other_abc123"},
		// "xox-" is not a Slack token (needs xox[baprs]).
		{"slack-shaped non-token", "xox-1234"},
		// "sig" as a substring of an unrelated word.
		{"sig in middle of word", "monosignal"},
		// "PRIVATE KEY" without the leading dashes.
		{"private key plain text", "PRIVATE KEY (rotation policy)"},
		// "eyJ" prefix without two more JWT segments.
		{"eyJ prefix only", "eyJpayloadonly"},
	}

	for _, tt := range allowed {
		tt := tt
		t.Run("allow/"+tt.name, func(t *testing.T) {
			t.Parallel()

			if ContainsCredentialLikeMaterial(tt.value) {
				t.Fatalf("ContainsCredentialLikeMaterial(%q) = true; want false", tt.value)
			}
		})
	}
}

// TestRouteDefinition_DescriptionIsValidated pins the new Description
// validation: the field is checked for length, control chars, and
// credential / tenant-topology material so a future ManifestRoute that
// surfaces Description for ops introspection cannot leak operator-edited
// secrets at render time.
func TestRouteDefinition_DescriptionIsValidated(t *testing.T) {
	t.Parallel()

	base := func() RouteDefinition {
		r := routeForTest("transaction.created.kafka", "transaction.created")
		r.Description = "Primary Kafka route for transaction creation."

		return r
	}

	if _, err := NewRouteDefinition(base()); err != nil {
		t.Fatalf("NewRouteDefinition() with valid description error = %v; want nil", err)
	}

	tests := []struct {
		name        string
		description string
		want        error
	}{
		{
			name:        "control char in description",
			description: "Primary Kafka route\nwith newline.",
			want:        ErrInvalidRouteDefinition,
		},
		{
			name:        "credential assignment in description",
			description: "Set api_key=secret123 on this route.",
			want:        ErrInvalidRouteDefinition,
		},
		{
			name:        "bare AWS access key in description",
			description: "Owned by team using AKIAIOSFODNN7EXAMPLE for staging.",
			want:        ErrInvalidRouteDefinition,
		},
		{
			name:        "github pat in description",
			description: "Token: ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			want:        ErrInvalidRouteDefinition,
		},
		{
			name:        "tenant topology token in description",
			description: "Routes ${tenant_id} traffic.",
			want:        ErrInvalidRouteDefinition,
		},
		{
			name:        "description over byte cap",
			description: strings.Repeat("a", MaxDataSchemaBytes+1),
			want:        ErrInvalidRouteDefinition,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := base()
			r.Description = tt.description

			_, err := NewRouteDefinition(r)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewRouteDefinition() error = %v; want errors.Is(..., %v)", err, tt.want)
			}
		})
	}
}

func routeForTest(key, definitionKey string) RouteDefinition {
	enabled := true
	dlq := Destination{
		Kind:       TransportKafkaLike,
		Name:       key + ".dlq",
		Attributes: map[string]string{"format": "json"},
	}

	return RouteDefinition{
		Key:           key,
		DefinitionKey: definitionKey,
		Target:        "primary",
		Destination: Destination{
			Kind:       TransportKafkaLike,
			Name:       key,
			Attributes: map[string]string{"format": "json"},
		},
		Policy: DeliveryPolicyOverride{Enabled: &enabled},
		DLQ:    &dlq,
	}
}
