package contract

// Per-transport payload caps. Each transport family advertises a different
// maximum-message size; lib-streaming preflight applies the most restrictive
// cap across all REQUIRED routes for an Emit so a payload that would land
// in the outbox cannot perpetually fail replay against a smaller-cap
// transport.
//
// Limits:
//   - Kafka / Custom    : 1 MiB (Redpanda default broker.max.message.bytes;
//     callers tune broker side, lib-streaming
//     stays at the default per AGENTS.md §7).
//   - SQS               : 256 KiB (AWS SQS quota; same as MaxMessageSize).
//   - EventBridge       : 256 KiB (AWS EventBridge per-event size).
//   - RabbitMQ          : 1 MiB  (matches AMQP convention; many brokers
//     allow more but 1 MiB is the safe default).
//
// The Kafka cap stays at MaxPayloadBytes (1 MiB) so the legacy single-
// target path retains its existing behavior verbatim. Multi-target callers
// emitting to a required SQS/EventBridge route get the smaller cap applied
// at preflight before any I/O.
const (
	// MaxPayloadBytesKafka is the per-transport payload cap for Kafka and
	// Kafka-compatible brokers (1 MiB).
	MaxPayloadBytesKafka = MaxPayloadBytes

	// MaxPayloadBytesSQS is the AWS SQS per-message payload cap.
	MaxPayloadBytesSQS = 256 << 10 // 256 KiB

	// MaxPayloadBytesEventBridge is the AWS EventBridge per-event size
	// cap.
	MaxPayloadBytesEventBridge = 256 << 10 // 256 KiB

	// MaxPayloadBytesRabbitMQ is the conservative AMQP convention cap.
	// Brokers may permit larger payloads, but 1 MiB is the safe default
	// across hosted RabbitMQ providers.
	MaxPayloadBytesRabbitMQ = MaxPayloadBytes

	// MaxPayloadBytesCustom mirrors the Kafka cap for caller-owned custom
	// transports — the library cannot know a custom adapter's true cap,
	// so we cap at the historical 1 MiB and let the adapter itself reject
	// oversize messages with its native sentinel.
	MaxPayloadBytesCustom = MaxPayloadBytes
)

// MaxPayloadBytesForKind returns the per-transport payload-size ceiling for
// the supplied kind. Returns MaxPayloadBytes (1 MiB) for any unknown kind so
// validation never under-rejects a payload due to a missing case.
func MaxPayloadBytesForKind(kind TransportKind) int {
	switch kind {
	case TransportSQS:
		return MaxPayloadBytesSQS
	case TransportEventBridge:
		return MaxPayloadBytesEventBridge
	case TransportRabbitMQ:
		return MaxPayloadBytesRabbitMQ
	case TransportKafkaLike, TransportCustom:
		return MaxPayloadBytesKafka
	default:
		return MaxPayloadBytes
	}
}
