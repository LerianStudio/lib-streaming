package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/internal/contract"
	"github.com/LerianStudio/lib-streaming/internal/transport"
)

// Adapter is the Kafka transport implementation backed by franz-go.
type Adapter struct {
	client *kgo.Client
}

// NewAdapter constructs a Kafka adapter from franz-go options.
func NewAdapter(opts ...kgo.Opt) (*Adapter, error) {
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	a, err := NewAdapterFromClient(client)
	if err != nil {
		// Defensive: kgo.NewClient succeeded so client is non-nil; this
		// branch is unreachable in production but keeps the constructor
		// honest about its (*Adapter, error) contract.
		client.Close()
		return nil, err
	}

	return a, nil
}

// NewAdapterFromClient wraps an existing franz-go client. Returns
// ErrNilProducer when client is nil so the asymmetry with the SQS /
// RabbitMQ / EventBridge constructors (all of which return an error on
// nil) is closed.
func NewAdapterFromClient(client *kgo.Client) (*Adapter, error) {
	if client == nil {
		return nil, contract.ErrNilProducer
	}

	return &Adapter{client: client}, nil
}

// Kind returns the transport kind implemented by this adapter.
func (*Adapter) Kind() contract.TransportKind {
	return contract.TransportKafkaLike
}

// Publish sends one message using franz-go async Produce plus ctx cancellation.
//
// Destination shape & security validation is NOT done here on the hot path.
// The contract layer's full destination.Validate() (which runs SSRF DNS
// resolution via ssrf.ValidateURL for URL-shaped destinations) is performed
// once at construction time inside NewRouteDefinition for the multi-target
// path. The legacy single-target path constructs Destination{Kind:
// TransportKafkaLike, Name: event.Topic()} from a known-safe topic string
// (no scheme → SSRF check is a no-op anyway) so per-Publish revalidation is
// pure overhead with no safety win. The kind-mismatch + empty-name +
// attributes-forbidden checks below are the cheap last-line guards that
// catch hand-built TransportMessages bypassing the routed/legacy paths.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	// Nil-receiver guard runs first to match the convention used by every
	// other adapter (sqs/rabbitmq/eventbridge): a nil receiver short-circuits
	// before any further field inspection. a.client is a concrete *kgo.Client
	// pointer (not an interface) so a plain nil check is sufficient; the
	// IsNilInterface idiom used elsewhere is for interface-typed clients.
	if a == nil || a.client == nil {
		return contract.ErrNilProducer
	}

	if message.Destination.Kind != contract.TransportKafkaLike {
		return fmt.Errorf("%w: kafka adapter destination kind=%q", contract.ErrInvalidDestination, message.Destination.Kind)
	}

	if message.Destination.Name == "" {
		return fmt.Errorf("%w: kafka topic name required", contract.ErrInvalidDestination)
	}

	if len(message.Destination.Attributes) != 0 {
		return fmt.Errorf("%w: kafka destination attributes must be empty", contract.ErrInvalidDestination)
	}

	if message.Destination.Address != "" {
		return fmt.Errorf("%w: kafka destination address must be empty", contract.ErrInvalidDestination)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	record := &kgo.Record{
		Topic:   message.Destination.Name,
		Key:     []byte(message.Key),
		Value:   message.Payload,
		Headers: toKgoHeaders(message.Headers),
	}

	errCh := make(chan error, 1)

	a.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		errCh <- err
	})

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Healthy pings Kafka using the caller-supplied context.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil || a.client == nil {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return a.client.Ping(ctx)
}

// Flush flushes buffered records using the caller-supplied context.
func (a *Adapter) Flush(ctx context.Context) error {
	if a == nil || a.client == nil {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return a.client.Flush(ctx)
}

// Close closes the underlying franz-go client.
func (a *Adapter) Close(context.Context) error {
	if a == nil || a.client == nil {
		return contract.ErrNilProducer
	}

	a.client.Close()

	return nil
}

// Classify maps Kafka/franz-go errors to streaming error classes.
func (*Adapter) Classify(err error) contract.ErrorClass {
	return ClassifyError(err)
}

// ClassifyError maps an error returned by the franz-go client to one of the
// streaming error classes declared in the contract package. Returns "" (no
// class) for a nil err, matching every other adapter (SQS, RabbitMQ,
// EventBridge, fake). In production paths nil is never passed; callers always
// have a non-nil error before calling Classify.
func ClassifyError(err error) contract.ErrorClass {
	if err == nil {
		return ""
	}

	if isContextError(err) {
		return contract.ClassContextCanceled
	}

	if isRouteValidationError(err) {
		return contract.ClassValidation
	}

	if class, ok := classifyBySentinel(err); ok {
		return class
	}

	if isNetTimeout(err) {
		return contract.ClassNetworkTimeout
	}

	if errors.Is(err, kgo.ErrRecordRetries) {
		return classifyRecordRetries(err)
	}

	return contract.ClassBrokerUnavailable
}

var routeValidationSentinels = []error{
	contract.ErrInvalidDestination,
	contract.ErrInvalidRouteDefinition,
	contract.ErrNoRoutesConfigured,
	contract.ErrMissingTarget,
	contract.ErrMultiTransportRuntimeNotConfigured,
}

func isRouteValidationError(err error) bool {
	for _, sentinel := range routeValidationSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}

	return false
}

var classErrorMapping = []struct {
	sentinel error
	class    contract.ErrorClass
}{
	{kerr.TopicAuthorizationFailed, contract.ClassAuth},
	{kerr.ClusterAuthorizationFailed, contract.ClassAuth},
	{kerr.SaslAuthenticationFailed, contract.ClassAuth},
	{kerr.UnknownTopicOrPartition, contract.ClassTopicNotFound},
	{kerr.MessageTooLarge, contract.ClassSerialization},
	{kerr.InvalidRecord, contract.ClassSerialization},
	{kerr.CorruptMessage, contract.ClassSerialization},
	{kerr.ThrottlingQuotaExceeded, contract.ClassBrokerOverloaded},
	{kerr.PolicyViolation, contract.ClassBrokerOverloaded},
	{kgo.ErrRecordTimeout, contract.ClassNetworkTimeout},
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func classifyBySentinel(err error) (contract.ErrorClass, bool) {
	for _, mapping := range classErrorMapping {
		if errors.Is(err, mapping.sentinel) {
			return mapping.class, true
		}
	}

	return "", false
}

func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func classifyRecordRetries(err error) contract.ErrorClass {
	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		return contract.ClassBrokerUnavailable
	}

	if errors.Is(unwrapped, err) && errors.Is(err, unwrapped) {
		return contract.ClassBrokerUnavailable
	}

	return ClassifyError(unwrapped)
}

// toKgoHeaders converts the transport-port header slice to franz-go's
// kgo.RecordHeader without deep-copying the byte values.
//
// Lifetime invariant: Publish builds the kgo.Record locally, hands it to
// franz-go's Produce, and waits SYNCHRONOUSLY on the produce callback via
// the select{errCh, ctx.Done} gate below. The callback writes to the
// buffered errCh and returns, after which Publish returns and the caller's
// header byte values become eligible for GC. franz-go does not retain
// header bytes past the callback dispatch.
//
// Therefore deep-copying header.Value here adds an unnecessary per-Emit
// allocation (one []byte per header, ~8-13 headers) that previously
// dominated the per-Emit allocation budget. The shared transport.Header
// slice is owned by the caller for the duration of Publish, which is
// exactly the window franz-go needs.
//
// Caller (cloudevents.BuildTransportHeaders) owns the byte slices and
// guarantees they are immutable for the lifetime of the resulting
// transport.Header slice — so reusing them here is correctness-safe.
func toKgoHeaders(headers []transport.Header) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}

	converted := make([]kgo.RecordHeader, len(headers))
	for i, header := range headers {
		converted[i] = kgo.RecordHeader{
			Key:   header.Key,
			Value: header.Value, // see lifetime invariant above
		}
	}

	return converted
}
