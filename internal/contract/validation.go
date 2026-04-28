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

// Lowercase aliases keep moved white-box tests in this package source-compatible.
const (
	maxPayloadBytes         = MaxPayloadBytes
	maxTenantIDBytes        = MaxTenantIDBytes
	maxResourceTypeBytes    = MaxResourceTypeBytes
	maxEventTypeBytes       = MaxEventTypeBytes
	maxSourceBytes          = MaxSourceBytes
	maxSubjectBytes         = MaxSubjectBytes
	maxEventIDBytes         = MaxEventIDBytes
	maxSchemaVersionBytes   = MaxSchemaVersionBytes
	maxDataContentTypeBytes = MaxDataContentTypeBytes
	maxDataSchemaBytes      = MaxDataSchemaBytes
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

func hasControlChar(s string) bool {
	return HasControlChar(s)
}

type headerFieldCheck struct {
	value    string
	maxBytes int
	sentinel error
}
