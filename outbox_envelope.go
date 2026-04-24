package streaming

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

const (
	// StreamingOutboxEventType is the stable outbox event type used for all
	// streaming relay rows. The concrete Kafka topic lives in OutboxEnvelope.
	StreamingOutboxEventType = "lerian.streaming.publish"

	// outboxEnvelopeVersion is the wire-version of OutboxEnvelope. Validation
	// uses strict equality (== outboxEnvelopeVersion) so unknown future versions
	// are rejected as malformed. Forward-compatibility policy:
	//   - When introducing version 2 (or higher), readers SHOULD accept v1 rows
	//     transparently to support rolling deploys (drain-then-deploy is fragile
	//     in production). The v0.2.0 → v1.0.0 path already ships a legacy-payload
	//     shim in decodeOutboxRow for v0.1.0 rows; future bumps should follow
	//     the same pattern.
	//   - Bumping this constant is an OPERATIONAL decision, not a code one.
	//     Coordinate with downstream outbox consumers before changing.
	outboxEnvelopeVersion = 1
)

// OutboxEnvelope is the versioned payload stored in the application outbox
// when streaming delivery is deferred.
type OutboxEnvelope struct {
	Version       int            `json:"version"`
	Topic         string         `json:"topic"`
	DefinitionKey string         `json:"definition_key,omitempty"`
	AggregateID   uuid.UUID      `json:"aggregate_id"`
	Policy        DeliveryPolicy `json:"policy"`
	Event         Event          `json:"event"`
}

func (e OutboxEnvelope) Validate() error {
	// Strict equality: unknown versions are rejected as malformed. See
	// outboxEnvelopeVersion godoc for forward-compat strategy.
	if e.Version != outboxEnvelopeVersion {
		return fmt.Errorf("streaming: unsupported outbox envelope version %d", e.Version)
	}

	if e.Topic == "" {
		return fmt.Errorf("%w: outbox envelope topic required", ErrInvalidEventDefinition)
	}

	if eventTopic := e.Event.Topic(); eventTopic == "" || e.Topic != eventTopic {
		return fmt.Errorf("%w: outbox envelope topic %q does not match event topic %q", ErrInvalidEventDefinition, e.Topic, eventTopic)
	}

	if e.AggregateID == uuid.Nil {
		return errors.New("streaming: outbox envelope aggregate_id required")
	}

	if err := e.Policy.Validate(); err != nil {
		return err
	}

	return nil
}
