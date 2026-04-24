//go:build unit

package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/LerianStudio/lib-commons/v5/commons/log"
)

func TestPublisherDescriptor_DefaultRoutePath(t *testing.T) {
	t.Parallel()

	descriptor, err := NewPublisherDescriptor(PublisherDescriptor{
		ServiceName: "transaction-service",
		SourceBase:  "//lerian.midaz/transaction-service",
	})
	if err != nil {
		t.Fatalf("NewPublisherDescriptor() error = %v", err)
	}
	if descriptor.RoutePath != "/streaming" {
		t.Errorf("RoutePath = %q; want /streaming", descriptor.RoutePath)
	}
}

func TestPublisherDescriptor_RejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		descriptor PublisherDescriptor
	}{
		{name: "missing service", descriptor: PublisherDescriptor{SourceBase: "//source"}},
		{name: "missing source", descriptor: PublisherDescriptor{ServiceName: "svc"}},
		{
			name: "relative route",
			descriptor: PublisherDescriptor{
				ServiceName: "svc",
				SourceBase:  "//source",
				RoutePath:   "streaming",
			},
		},
		{
			name: "control char",
			descriptor: PublisherDescriptor{
				ServiceName: "svc\n",
				SourceBase:  "//source",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewPublisherDescriptor(tt.descriptor)
			if !errors.Is(err, ErrInvalidPublisherDescriptor) {
				t.Fatalf("NewPublisherDescriptor() error = %v; want ErrInvalidPublisherDescriptor", err)
			}
		})
	}
}

// TestProducer_Descriptor_PopulatesProducerID asserts that Descriptor attaches
// the Producer's UUIDv7 producerID to the returned descriptor and that the
// descriptor validates cleanly.
func TestProducer_Descriptor_PopulatesProducerID(t *testing.T) {
	cfg, _ := kfakeConfig(t)

	emitter, err := New(context.Background(), cfg, WithLogger(log.NewNop()), WithCatalog(sampleCatalog()))
	if err != nil {
		t.Fatalf("New err = %v", err)
	}

	t.Cleanup(func() { _ = emitter.Close() })

	p := asProducer(t, emitter)

	got, err := p.Descriptor(PublisherDescriptor{
		ServiceName: "svc",
		SourceBase:  "//s",
	})
	if err != nil {
		t.Fatalf("Descriptor err = %v", err)
	}

	if got.ProducerID != p.producerID {
		t.Fatalf("Descriptor.ProducerID = %q; want %q", got.ProducerID, p.producerID)
	}

	parsed, err := uuid.Parse(got.ProducerID)
	if err != nil {
		t.Fatalf("uuid.Parse(%q) err = %v", got.ProducerID, err)
	}

	if parsed.Version() != 7 {
		t.Errorf("producerID UUID version = %d; want 7", parsed.Version())
	}
}

// TestProducer_Descriptor_NilReceiverReturnsSentinel asserts Descriptor is
// nil-safe: a zero *Producer returns a zero descriptor and ErrNilProducer
// rather than panicking.
func TestProducer_Descriptor_NilReceiverReturnsSentinel(t *testing.T) {
	t.Parallel()

	var nilProducer *Producer

	got, err := nilProducer.Descriptor(PublisherDescriptor{ServiceName: "svc", SourceBase: "//s"})
	if !errors.Is(err, ErrNilProducer) {
		t.Fatalf("nil.Descriptor err = %v; want ErrNilProducer", err)
	}

	if got != (PublisherDescriptor{}) {
		t.Errorf("nil.Descriptor result = %+v; want zero PublisherDescriptor", got)
	}
}
