package contract

import (
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

const (
	// TraceParentHeader is the canonical W3C traceparent carrier key.
	TraceParentHeader = "traceparent"
	// TraceStateHeader is the canonical W3C tracestate carrier key.
	TraceStateHeader = "tracestate"
	// MaxTraceCarrierEntries bounds persisted trace metadata cardinality.
	MaxTraceCarrierEntries = 2
	// MaxTraceCarrierValueBytes applies the W3C tracestate maximum to every
	// persisted trace carrier value.
	MaxTraceCarrierValueBytes = 512
)

// TraceCarrier is the bounded W3C trace context persisted with an outbox
// envelope. Only traceparent and tracestate are allowed; baggage is excluded.
type TraceCarrier map[string]string

// Validate rejects unknown keys, unsafe values, and malformed W3C context.
func (c TraceCarrier) Validate() error {
	if len(c) == 0 {
		return nil
	}

	if len(c) > MaxTraceCarrierEntries {
		return fmt.Errorf("%w: carrier has %d entries; max %d", ErrInvalidTraceCarrier, len(c), MaxTraceCarrierEntries)
	}

	for key, value := range c {
		if key != TraceParentHeader && key != TraceStateHeader {
			return fmt.Errorf("%w: unsupported key %q", ErrInvalidTraceCarrier, key)
		}

		if value == "" {
			return fmt.Errorf("%w: %s value is empty", ErrInvalidTraceCarrier, key)
		}

		if len(value) > MaxTraceCarrierValueBytes {
			return fmt.Errorf("%w: %s exceeds %d bytes", ErrInvalidTraceCarrier, key, MaxTraceCarrierValueBytes)
		}

		if HasControlChar(value) || !isASCII(value) {
			return fmt.Errorf("%w: %s contains unsafe characters", ErrInvalidTraceCarrier, key)
		}
	}

	parent, hasParent := c[TraceParentHeader]
	state, hasState := c[TraceStateHeader]

	if !hasParent {
		return fmt.Errorf("%w: tracestate requires traceparent", ErrInvalidTraceCarrier)
	}

	if err := validateTraceParent(parent); err != nil {
		return err
	}

	if hasState {
		if _, err := trace.ParseTraceState(state); err != nil {
			return fmt.Errorf("%w: invalid tracestate: %w", ErrInvalidTraceCarrier, err)
		}
	}

	return nil
}

func validateTraceParent(value string) error {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return fmt.Errorf("%w: traceparent must use canonical W3C version 00", ErrInvalidTraceCarrier)
	}

	traceID, err := trace.TraceIDFromHex(parts[1])
	if err != nil || !traceID.IsValid() {
		return fmt.Errorf("%w: invalid traceparent trace id", ErrInvalidTraceCarrier)
	}

	spanID, err := trace.SpanIDFromHex(parts[2])
	if err != nil || !spanID.IsValid() {
		return fmt.Errorf("%w: invalid traceparent span id", ErrInvalidTraceCarrier)
	}

	if _, err := strconv.ParseUint(parts[3], 16, 8); err != nil {
		return fmt.Errorf("%w: invalid traceparent flags", ErrInvalidTraceCarrier)
	}

	return nil
}

func isASCII(value string) bool {
	for i := range len(value) {
		if value[i] > 0x7f {
			return false
		}
	}

	return true
}
