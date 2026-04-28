package contract

// MaxPayloadBytes is the payload-size ceiling shared by request validation,
// producer preflight, and outbox writer validation. It intentionally matches
// Redpanda's default broker.max.message.bytes and lib-commons outbox defaults.
const MaxPayloadBytes = 1_048_576

// Header-length ceilings for CloudEvents context attributes.
const (
	MaxTenantIDBytes        = 256
	MaxResourceTypeBytes    = 128
	MaxEventTypeBytes       = 128
	MaxSourceBytes          = 2048
	MaxSubjectBytes         = 1024
	MaxEventIDBytes         = 256
	MaxSchemaVersionBytes   = 64
	MaxDataContentTypeBytes = 256
	MaxDataSchemaBytes      = 2048
)

// HasControlChar reports whether s contains ASCII control characters that are
// unsafe for headers, logs, and telemetry attributes.
func HasControlChar(s string) bool {
	for i := range len(s) {
		b := s[i]
		if b < 0x20 || b == 0x7F {
			return true
		}
	}

	return false
}

// HeaderFieldCheck pairs a header-bound field value with its size ceiling and
// the caller-correctable sentinel returned when the field is malformed
// (control chars or over-limit). Exported so the producer package can compose
// per-call check tables without redeclaring the struct.
type HeaderFieldCheck struct {
	Value    string
	MaxBytes int
	Sentinel error
}
