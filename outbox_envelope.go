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
