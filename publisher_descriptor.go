package streaming

import (
	"fmt"
	"strings"
)

// PublisherDescriptor carries app-owned metadata used by manifest export and
// runtime introspection. The app still owns routing, auth, server bootstrap,
// and any publication of the manifest artifact.
type PublisherDescriptor struct {
	ServiceName     string `json:"serviceName"`
	SourceBase      string `json:"sourceBase"`
	RoutePath       string `json:"routePath"`
	OutboxSupported bool   `json:"outboxSupported"`
	AppVersion      string `json:"appVersion,omitempty"`
	LibVersion      string `json:"libVersion,omitempty"`
}

// NewPublisherDescriptor validates and normalizes a PublisherDescriptor.
func NewPublisherDescriptor(descriptor PublisherDescriptor) (PublisherDescriptor, error) {
	if hasControlChar(descriptor.ServiceName) ||
		hasControlChar(descriptor.SourceBase) ||
		hasControlChar(descriptor.RoutePath) ||
		hasControlChar(descriptor.AppVersion) ||
		hasControlChar(descriptor.LibVersion) {
		return PublisherDescriptor{}, fmt.Errorf("%w: metadata contains control chars", ErrInvalidPublisherDescriptor)
	}

	descriptor.ServiceName = strings.TrimSpace(descriptor.ServiceName)
	descriptor.SourceBase = strings.TrimSpace(descriptor.SourceBase)
	descriptor.RoutePath = strings.TrimSpace(descriptor.RoutePath)
	descriptor.AppVersion = strings.TrimSpace(descriptor.AppVersion)
	descriptor.LibVersion = strings.TrimSpace(descriptor.LibVersion)

	if descriptor.ServiceName == "" {
		return PublisherDescriptor{}, fmt.Errorf("%w: service name required", ErrInvalidPublisherDescriptor)
	}

	if descriptor.SourceBase == "" {
		return PublisherDescriptor{}, fmt.Errorf("%w: source base required", ErrInvalidPublisherDescriptor)
	}

	if descriptor.RoutePath == "" {
		descriptor.RoutePath = "/streaming"
	}

	if !strings.HasPrefix(descriptor.RoutePath, "/") {
		return PublisherDescriptor{}, fmt.Errorf("%w: route path must start with /", ErrInvalidPublisherDescriptor)
	}

	return descriptor, nil
}
