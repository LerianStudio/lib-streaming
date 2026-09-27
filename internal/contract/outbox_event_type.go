package contract

// MaxOutboxEventTypeBytes is the width of the lib-commons outbox event_type
// column (VARCHAR(255)). Every outbox event type this library writes fits it.
const MaxOutboxEventTypeBytes = 255

// OutboxEventTypeForSource returns the source-scoped outbox event type,
// StreamingOutboxEventType + "." + source, after validating source with
// ValidateSource.
//
// A producer in source-scoped outbox mode writes and relays only this type, so
// several binaries of one service sharing one outbox table each claim only
// their own rows. The longest legal source still fits MaxOutboxEventTypeBytes.
func OutboxEventTypeForSource(source string) (string, error) {
	if err := ValidateSource(source); err != nil {
		return "", err
	}

	return StreamingOutboxEventType + "." + source, nil
}
