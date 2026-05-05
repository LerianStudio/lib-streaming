package transport

import (
	"context"
	"maps"
	"reflect"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

// IsNilInterface reports whether v is a nil interface, including the
// notorious typed-nil case where the interface carries a concrete type
// pointer whose value is nil.
//
// The standard Go gotcha: `var x SomeInterface = (*Concrete)(nil); x == nil`
// returns FALSE because the interface header carries a non-nil type tag.
// Only reflect.ValueOf(x).IsNil() returns TRUE.
//
// Use this at every boundary where a caller may hand the library a
// statically-typed nil: constructors that take an interface, options that
// store an interface, and any adapter method whose receiver-stored
// interface field could have been wired with a typed-nil value.
//
// IsNilInterface returns true for:
//   - Untyped nil (`v == nil`).
//   - Typed-nil channels, funcs, interfaces, maps, pointers, and slices.
//
// Returns false for any non-nilable kind (int, string, struct values).
func IsNilInterface(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

// Header is one transport metadata entry. Value is bytes so adapters can map
// directly to Kafka, AMQP, HTTP, or SDK-native header representations.
type Header struct {
	Key   string
	Value []byte
}

// TransportMessage is the transport-port payload passed to adapters.
type TransportMessage struct {
	Destination contract.Destination
	TenantID    string
	Key         string
	Payload     []byte
	Headers     []Header
	Attributes  map[string]string
}

// TransportAdapter is the outbound transport port implemented by concrete
// broker adapters and test fakes.
type TransportAdapter interface {
	Kind() contract.TransportKind
	Publish(ctx context.Context, message TransportMessage) error
	Healthy(ctx context.Context) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
	Classify(err error) contract.ErrorClass
}

// CloneMessage returns a deep copy of message.
func CloneMessage(message TransportMessage) TransportMessage {
	message.Payload = append([]byte(nil), message.Payload...)
	message.Headers = CloneHeaders(message.Headers)
	message.Attributes = cloneStringMap(message.Attributes)
	message.Destination = message.Destination.Normalize()

	return message
}

// CloneHeaders returns a deep copy of headers.
func CloneHeaders(headers []Header) []Header {
	if len(headers) == 0 {
		return nil
	}

	clone := make([]Header, len(headers))
	for i, header := range headers {
		clone[i] = Header{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		}
	}

	return clone
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}
