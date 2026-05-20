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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// MaxBodyBytes is the SQS message size cap (256 KiB). The adapter validates
// the full wire contribution it controls: body + message attribute names,
// String data type names, and values. This fails before issuing any network
// call so a misconfigured caller fails fast.
const MaxBodyBytes = 262_144

const (
	maxMessageAttributes      = 10
	sqsStringDataType         = "String"
	metadataOverflowAttribute = "x-lerian-streaming-extra-headers"
	primaryAttributeBudget    = maxMessageAttributes - 1
)

var preferredAttributeKeys = []string{
	"traceparent",
	"tracestate",
	"ce-specversion",
	"ce-id",
	"ce-source",
	"ce-type",
	"ce-time",
	"ce-schemaversion",
	"ce-resourcetype",
	"ce-eventtype",
	"ce-tenantid",
}

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

// SQSPingClient is the capability Adapter.Healthy requires. It is separate
// from SQSPublisherClient so callers keep SDK lifecycle ownership while health
// still fails closed when no ping probe is available.
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
	client             SQSPublisherClient
	defaultQueueURL    string
	validatedQueueURLs sync.Map
}

type preparedMessage struct {
	queueURL   string
	body       []byte
	attributes []Attribute
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
	if defaultQueueURL != "" {
		a.validatedQueueURLs.Store(defaultQueueURL, struct{}{})
	}

	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}

	return a, nil
}

// Kind reports the transport kind implemented by this adapter.
func (*Adapter) Kind() contract.TransportKind { return contract.TransportSQS }

// Publish sends one CloudEvents-shaped message to SQS.
//
// Publish validates per-message queue URLs with the same Destination.Validate
// gate used at route construction and caches successful validation by URL, so
// direct adapter callers do not bypass DNS/SSRF protection.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	prepared, err := a.PrepareMessage(message)
	if err != nil {
		return err
	}

	return a.PublishPrepared(ctx, prepared)
}

// ValidateMessage performs deterministic message-shape checks without issuing
// a network call. The producer invokes this before entering the broker circuit
// breaker so caller-correctable SQS shape errors cannot poison broker health.
func (a *Adapter) ValidateMessage(message transport.TransportMessage) error {
	_, err := a.PrepareMessage(message)

	return err
}

// PrepareMessage performs deterministic validation and SQS wire preparation
// without issuing a network call. The returned value is accepted only by this
// adapter's PublishPrepared method.
func (a *Adapter) PrepareMessage(message transport.TransportMessage) (any, error) {
	queueURL, attributes, err := a.prepareMessage(message)
	if err != nil {
		return nil, err
	}

	return preparedMessage{
		queueURL:   queueURL,
		body:       append([]byte(nil), message.Payload...),
		attributes: cloneAttributes(attributes),
	}, nil
}

// PublishPrepared sends a message prepared by PrepareMessage. It exists so the
// producer can prevalidate caller-correctable SQS shape errors before the
// circuit breaker without paying the preparation cost twice.
func (a *Adapter) PublishPrepared(ctx context.Context, prepared any) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	message, ok := prepared.(preparedMessage)
	if !ok {
		return fmt.Errorf("%w: sqs prepared message has unexpected type %T", contract.ErrInvalidDestination, prepared)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return a.client.SendMessage(ctx, message.queueURL, append([]byte(nil), message.body...), cloneAttributes(message.attributes))
}

// Healthy delegates to SQSPingClient. Clients without Ping fail closed so a
// target cannot report ready without a real health probe.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	pinger, ok := a.client.(SQSPingClient)
	if !ok || transport.IsNilInterface(pinger) {
		return errors.New("sqs: health check requires client implementing SQSPingClient")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return pinger.Ping(ctx)
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

func (a *Adapter) prepareMessage(message transport.TransportMessage) (string, []Attribute, error) {
	if a == nil || transport.IsNilInterface(a.client) {
		return "", nil, contract.ErrNilProducer
	}

	if message.Destination.Kind != contract.TransportSQS {
		return "", nil, fmt.Errorf("%w: sqs adapter destination kind=%q", contract.ErrInvalidDestination, message.Destination.Kind)
	}

	queueURL := message.Destination.Address
	if queueURL == "" {
		queueURL = a.defaultQueueURL
	}

	if queueURL == "" {
		return "", nil, fmt.Errorf("%w: sqs queue URL required", contract.ErrInvalidDestination)
	}

	if err := a.validateQueueURL(queueURL); err != nil {
		return "", nil, err
	}

	attributes, err := buildAttributes(message)
	if err != nil {
		return "", nil, err
	}

	if len(attributes) > maxMessageAttributes {
		return "", nil, fmt.Errorf("%w: sqs message has %d attributes, exceeds %d-attribute cap", contract.ErrPayloadTooLarge, len(attributes), maxMessageAttributes)
	}

	if size := sqsWireSize(message.Payload, attributes); size > MaxBodyBytes {
		return "", nil, fmt.Errorf("%w: sqs message %d bytes exceeds %d-byte cap", contract.ErrPayloadTooLarge, size, MaxBodyBytes)
	}

	return queueURL, attributes, nil
}

func (a *Adapter) validateQueueURL(queueURL string) error {
	if _, ok := a.validatedQueueURLs.Load(queueURL); ok {
		return nil
	}

	destination := contract.Destination{Kind: contract.TransportSQS, Address: queueURL}
	if err := destination.Validate(); err != nil {
		return fmt.Errorf("sqs queue URL: %w", err)
	}

	a.validatedQueueURLs.Store(queueURL, struct{}{})

	return nil
}

func cloneAttributes(attributes []Attribute) []Attribute {
	if len(attributes) == 0 {
		return nil
	}

	clone := make([]Attribute, len(attributes))
	for i, attr := range attributes {
		clone[i] = Attribute{Key: attr.Key, Value: append([]byte(nil), attr.Value...)}
	}

	return clone
}

// buildAttributes converts CloudEvents headers + caller attributes into the
// flat (key, value) slice consumed by SQSPublisherClient. CloudEvents
// headers win on key collisions because they are part of the binary
// envelope contract.
func buildAttributes(message transport.TransportMessage) ([]Attribute, error) {
	flattened := flattenAttributes(message)
	if len(flattened) == 0 {
		return nil, nil
	}

	if len(flattened) <= maxMessageAttributes {
		return attributesFromMap(flattened), nil
	}

	return packOverflowAttributes(flattened)
}

func flattenAttributes(message transport.TransportMessage) map[string][]byte {
	flattened := make(map[string][]byte, len(message.Attributes)+len(message.Headers))
	for k, v := range message.Attributes {
		flattened[k] = []byte(v)
	}

	for _, h := range message.Headers {
		flattened[h.Key] = append([]byte(nil), h.Value...)
	}

	return flattened
}

func attributesFromMap(flattened map[string][]byte) []Attribute {
	keys := make([]string, 0, len(flattened))
	for key := range flattened {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	attrs := make([]Attribute, 0, len(keys))
	for _, key := range keys {
		attrs = append(attrs, Attribute{Key: key, Value: append([]byte(nil), flattened[key]...)})
	}

	return attrs
}

func packOverflowAttributes(flattened map[string][]byte) ([]Attribute, error) {
	attrs := make([]Attribute, 0, maxMessageAttributes)
	used := make(map[string]struct{}, len(flattened))

	for _, key := range preferredAttributeKeys {
		if len(attrs) >= primaryAttributeBudget {
			break
		}

		value, ok := flattened[key]
		if !ok {
			continue
		}

		attrs = append(attrs, Attribute{Key: key, Value: append([]byte(nil), value...)})
		used[key] = struct{}{}
	}

	if _, ok := flattened["ce-tenantid"]; !ok && len(attrs) < primaryAttributeBudget {
		if value, ok := flattened["ce-datacontenttype"]; ok {
			attrs = append(attrs, Attribute{Key: "ce-datacontenttype", Value: append([]byte(nil), value...)})
			used["ce-datacontenttype"] = struct{}{}
		}
	}

	overflow := make(map[string]string, len(flattened)-len(used))
	for key, value := range flattened {
		if _, ok := used[key]; ok {
			continue
		}

		if key == metadataOverflowAttribute {
			continue
		}

		overflow[key] = string(value)
	}

	if len(overflow) == 0 {
		return attrs, nil
	}

	encoded, err := json.Marshal(overflow)
	if err != nil {
		return nil, fmt.Errorf("%w: sqs metadata overflow marshal: %w", contract.ErrInvalidDestination, err)
	}

	attrs = append(attrs, Attribute{Key: metadataOverflowAttribute, Value: encoded})
	sort.Slice(attrs, func(i, j int) bool {
		return attributeOrder(attrs[i].Key) < attributeOrder(attrs[j].Key)
	})

	return attrs, nil
}

func attributeOrder(key string) int {
	for i, preferred := range preferredAttributeKeys {
		if key == preferred {
			return i
		}
	}

	if key == "ce-datacontenttype" {
		return len(preferredAttributeKeys)
	}

	if key == metadataOverflowAttribute {
		return len(preferredAttributeKeys) + 1
	}

	return len(preferredAttributeKeys) + 2
}

func sqsWireSize(body []byte, attributes []Attribute) int {
	size := len(body)
	for _, attr := range attributes {
		size += len(attr.Key) + len(sqsStringDataType) + len(attr.Value)
	}

	return size
}
