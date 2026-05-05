// Package sqs ships a TransportAdapter that publishes lib-streaming
// CloudEvents-shaped messages to Amazon SQS via a caller-supplied client.
//
// The adapter does NOT depend on aws-sdk-go-v2. Callers fulfill the
// SQSPublisherClient interface with whichever AWS SDK version their service
// already uses. This keeps lib-streaming free of SDK version conflicts and
// allows fakes to drive integration tests without spinning up LocalStack.
//
// Wire one adapter per logical SQS target:
//
//	cli := /* your SDK-backed implementation */
//	adapter, err := sqs.New(cli, "https://sqs.us-east-1.amazonaws.com/123/q")
//
// Then register a factory through the public Builder.RegisterTransport
// (or use streaming.SQSTarget which wraps the registration).
package sqs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

// MaxBodyBytes is the SQS message body size cap (256 KiB). The adapter
// rejects payloads larger than this with ErrPayloadTooLarge before issuing
// any network call so a misconfigured caller fails fast.
const MaxBodyBytes = 262_144

// SQSPublisherClient is the minimal interface the adapter requires from a
// caller-owned SQS client. Implementations are typically a few lines of
// glue around aws-sdk-go-v2's sqs.Client.
//
// The slice of attributes uses (key, value) pairs encoded as String message
// attributes. Implementations MUST NOT mutate the slice; the adapter will
// not retain it after the call returns.
type SQSPublisherClient interface {
	SendMessage(ctx context.Context, queueURL string, body []byte, attributes []Attribute) error
}

// SQSPingClient is an optional capability check. When the client also
// satisfies SQSPingClient, Adapter.Healthy delegates to Ping. Otherwise
// Healthy returns nil.
type SQSPingClient interface {
	Ping(ctx context.Context) error
}

// Attribute is one SQS String message attribute (or string-encoded
// CloudEvents header). Bytes are passed through verbatim so binary-mode
// CloudEvents headers are preserved.
type Attribute struct {
	Key   string
	Value []byte
}

// Adapter publishes one logical SQS target via the supplied
// SQSPublisherClient.
type Adapter struct {
	client          SQSPublisherClient
	defaultQueueURL string
}

// Option configures an Adapter at construction time.
type Option func(*Adapter)

// New constructs an SQS Adapter bound to the supplied client. The
// defaultQueueURL is used when a TransportMessage's Destination.Address is
// empty. A non-empty Destination.Address always wins.
//
// New rejects both untyped nil AND typed-nil interface values (e.g.
// `var c *fakeClient; New(c, ...)`). Without the typed-nil guard, a
// statically-typed nil would survive the `client == nil` check and the
// adapter would construct, only to NPE on the first Publish call.
func New(client SQSPublisherClient, defaultQueueURL string, opts ...Option) (*Adapter, error) {
	if transport.IsNilInterface(client) {
		return nil, fmt.Errorf("%w: sqs: nil client", contract.ErrNilProducer)
	}

	if defaultQueueURL != "" {
		dest := contract.Destination{Kind: contract.TransportSQS, Address: defaultQueueURL}
		if err := dest.Validate(); err != nil {
			return nil, fmt.Errorf("sqs: defaultQueueURL: %w", err)
		}
	}

	a := &Adapter{client: client, defaultQueueURL: defaultQueueURL}

	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}

	return a, nil
}

// Kind reports the transport kind implemented by this adapter.
func (*Adapter) Kind() contract.TransportKind { return contract.TransportSQS }

// Publish sends one CloudEvents-binary-mode message to SQS.
//
// Destination shape & security validation is NOT done here on the hot path.
// The contract layer's full destination.Validate() (which runs SSRF DNS
// resolution via ssrf.ValidateURL) is performed once at construction time
// inside NewRouteDefinition for routed paths, and inside New() for the
// adapter's defaultQueueURL. Per-Publish revalidation would re-issue the
// DNS lookup on every Emit. The kind-mismatch + empty-URL + payload-cap
// checks below are the cheap last-line guards.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if message.Destination.Kind != contract.TransportSQS {
		return fmt.Errorf("%w: sqs adapter destination kind=%q", contract.ErrInvalidDestination, message.Destination.Kind)
	}

	queueURL := message.Destination.Address
	if queueURL == "" {
		queueURL = a.defaultQueueURL
	}

	if queueURL == "" {
		return fmt.Errorf("%w: sqs queue URL required", contract.ErrInvalidDestination)
	}

	if len(message.Payload) > MaxBodyBytes {
		return fmt.Errorf("%w: sqs body %d bytes exceeds %d-byte cap", contract.ErrPayloadTooLarge, len(message.Payload), MaxBodyBytes)
	}

	attributes := buildAttributes(message)

	return a.client.SendMessage(ctx, queueURL, message.Payload, attributes)
}

// Healthy delegates to the optional SQSPingClient. When the underlying
// client does not implement Ping, Healthy returns nil — the SQS API has no
// universal cheap healthcheck so we deliberately do not fabricate one.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	if pinger, ok := a.client.(SQSPingClient); ok {
		if ctx == nil {
			ctx = context.Background()
		}

		return pinger.Ping(ctx)
	}

	return nil
}

// Flush is a no-op for SQS; SendMessage is synchronous from the adapter's
// perspective. Optional batching lives below the SDK boundary.
func (*Adapter) Flush(context.Context) error { return nil }

// Close is a no-op. Lifecycle of the underlying SQS client belongs to the
// caller — the adapter never opens a connection it owns.
func (*Adapter) Close(context.Context) error { return nil }

// Classify maps common SQS / network errors to streaming error classes.
// Unknown errors classify to ClassBrokerUnavailable so the dispatcher
// short-circuits the breaker rather than treating them as caller-correctable.
func (*Adapter) Classify(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return contract.ClassContextCanceled
	}

	if errors.Is(err, contract.ErrPayloadTooLarge) || errors.Is(err, contract.ErrInvalidDestination) {
		return contract.ClassValidation
	}

	if contract.IsCallerError(err) {
		return contract.ClassValidation
	}

	msg := strings.ToLower(err.Error())

	switch {
	case containsAny(msg, "queuedoesnotexist", "queue does not exist", "nosuchqueue"):
		return contract.ClassTopicNotFound
	case containsAny(msg, "invalidclienttokenid", "accessdenied", "signaturedoesnotmatch", "unauthorized", "unrecognizedclient"):
		return contract.ClassAuth
	case containsAny(msg, "throttling", "throttled", "rate exceeded", "ratelimitexceeded"):
		return contract.ClassBrokerOverloaded
	case containsAny(msg, "timeout", "timed out", "deadline exceeded"):
		return contract.ClassNetworkTimeout
	}

	return contract.ClassBrokerUnavailable
}

func containsAny(msg string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
}

// buildAttributes converts CloudEvents headers + caller attributes into the
// flat (key, value) slice consumed by SQSPublisherClient. CloudEvents
// headers win on key collisions because they are part of the binary
// envelope contract.
func buildAttributes(message transport.TransportMessage) []Attribute {
	attrs := make([]Attribute, 0, len(message.Attributes)+len(message.Headers))

	for k, v := range message.Attributes {
		attrs = append(attrs, Attribute{Key: k, Value: []byte(v)})
	}

	for _, h := range message.Headers {
		attrs = append(attrs, Attribute{Key: h.Key, Value: append([]byte(nil), h.Value...)})
	}

	if len(attrs) == 0 {
		return nil
	}

	return attrs
}
