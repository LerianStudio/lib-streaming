//go:build unit

package streaming

import (
	"errors"
	"testing"
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
