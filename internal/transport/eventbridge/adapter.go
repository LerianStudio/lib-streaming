// Package eventbridge ships a TransportAdapter that publishes lib-streaming
// CloudEvents-shaped messages to Amazon EventBridge via a caller-supplied
// PutEvents client.
//
// Canonical Detail JSON shape.
// EventBridge takes a JSON `Detail` field per entry. The adapter renders the
// CloudEvents 1.0 envelope into Detail with the following stable shape so
// downstream rules can pattern-match without hard-coding ce-* vendor headers:
//
//	{
//	  "specversion":      "1.0",
//	  "id":               "<ce-id>",
//	  "source":           "<ce-source>",
//	  "type":             "<ce-type>",
//	  "subject":          "<ce-subject>",         // optional
//	  "time":             "<ce-time RFC3339>",
//	  "datacontenttype":  "<ce-datacontenttype>",
//	  "dataschema":       "<ce-dataschema>",      // optional
//	  "tenantid":         "<ce-tenantid>",        // optional
//	  "data":             <raw payload JSON>
//	}
//
// Callers that need a non-canonical Detail format can wrap the
// EventBridgePutEventsClient interface and re-render Detail before forwarding.
//
// The adapter does NOT depend on aws-sdk-go-v2.
package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LerianStudio/lib-streaming/v2/internal/contract"
	"github.com/LerianStudio/lib-streaming/v2/internal/transport"
)

// MaxBodyBytes is the EventBridge per-entry size cap (256 KiB).
const MaxBodyBytes = 262_144

// Entry is one EventBridge PutEvents entry produced by the adapter.
//
// EventBusName is the destination bus (lib-streaming Destination.Name).
// Source is the CloudEvents source. DetailType is the CloudEvents type.
// Detail is the rendered Detail JSON document. Resources is intentionally
// omitted — pass via TransportMessage attributes if you need it.
type Entry struct {
	EventBusName string
	Source       string
	DetailType   string
	Detail       []byte
	Time         time.Time
	// Resources mirrors EventBridge's Resources field (ARN list).
	// Sourced from message.Attributes["eventbridge.resources"] split on
	// commas; empty when the attribute is absent.
	Resources []string
}

// EventBridgePutEventsClient is the minimal interface the adapter requires
// from a caller-owned EventBridge client. Implementations are typically a
// few lines of glue around aws-sdk-go-v2's eventbridge.Client.PutEvents.
//
// Implementations MUST treat entries as read-only after the call returns.
type EventBridgePutEventsClient interface {
	PutEvents(ctx context.Context, entries []Entry) error
}

// EventBridgePingClient is an optional capability check.
type EventBridgePingClient interface {
	Ping(ctx context.Context) error
}

// Adapter publishes business events to an EventBridge bus via the supplied
// EventBridgePutEventsClient.
type Adapter struct {
	client EventBridgePutEventsClient
}

// New constructs an EventBridge Adapter bound to the supplied client.
//
// New rejects both untyped nil AND typed-nil interface values (e.g.
// `var c *fakeClient; New(c)`). Without the typed-nil guard, a
// statically-typed nil would survive the `client == nil` check and
// the adapter would NPE on the first PutEvents call.
func New(client EventBridgePutEventsClient) (*Adapter, error) {
	if transport.IsNilInterface(client) {
		return nil, fmt.Errorf("%w: eventbridge: nil client", contract.ErrNilProducer)
	}

	return &Adapter{client: client}, nil
}

// Kind reports the transport kind implemented by this adapter.
func (*Adapter) Kind() contract.TransportKind { return contract.TransportEventBridge }

// Publish renders the message into a single PutEvents Entry and forwards it
// to the underlying client.
//
// Destination shape & security validation is NOT done here on the hot path.
// The contract layer's full destination.Validate() runs once at
// construction time inside NewRouteDefinition; per-Publish revalidation
// would re-issue work for zero safety win since the destination is
// immutable post-construction.
func (a *Adapter) Publish(ctx context.Context, message transport.TransportMessage) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if message.Destination.Kind != contract.TransportEventBridge {
		return fmt.Errorf("%w: eventbridge adapter destination kind=%q", contract.ErrInvalidDestination, message.Destination.Kind)
	}

	bus := message.Destination.Name
	if bus == "" {
		return fmt.Errorf("%w: eventbridge bus name required", contract.ErrInvalidDestination)
	}

	if len(message.Payload) > MaxBodyBytes {
		return fmt.Errorf("%w: eventbridge entry %d bytes exceeds %d-byte cap", contract.ErrPayloadTooLarge, len(message.Payload), MaxBodyBytes)
	}

	detail, source, detailType, eventTime, err := renderDetail(message)
	if err != nil {
		return err
	}

	entry := Entry{
		EventBusName: bus,
		Source:       source,
		DetailType:   detailType,
		Detail:       detail,
		Time:         eventTime,
		Resources:    extractResources(message.Attributes),
	}

	return a.client.PutEvents(ctx, []Entry{entry})
}

// Healthy delegates to the optional EventBridgePingClient.
func (a *Adapter) Healthy(ctx context.Context) error {
	if a == nil || transport.IsNilInterface(a.client) {
		return contract.ErrNilProducer
	}

	if pinger, ok := a.client.(EventBridgePingClient); ok {
		if ctx == nil {
			ctx = context.Background()
		}

		return pinger.Ping(ctx)
	}

	return nil
}

// Flush is a no-op. PutEvents is synchronous from the adapter's perspective.
func (*Adapter) Flush(context.Context) error { return nil }

// Close is a no-op; client lifecycle belongs to the caller.
func (*Adapter) Close(context.Context) error { return nil }

// Classify maps common EventBridge / AWS errors to streaming error classes.
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
	case containsAny(msg, "resourcenotfound", "no event bus", "event bus not found"):
		return contract.ClassTopicNotFound
	case containsAny(msg, "accessdenied", "unauthorized", "signaturedoesnotmatch", "invalidclienttokenid", "unrecognizedclient"):
		return contract.ClassAuth
	case containsAny(msg, "throttling", "throttled", "rate exceeded"):
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

// renderDetail builds the canonical Detail JSON shape from the
// CloudEvents-binary-mode message. Returns (detail, source, type, time, err).
func renderDetail(message transport.TransportMessage) ([]byte, string, string, time.Time, error) {
	headers := mapHeaders(message.Headers)

	source := headers["ce-source"]
	detailType := headers["ce-type"]

	specVersion := headers["ce-specversion"]
	if specVersion == "" {
		specVersion = "1.0"
	}

	if source == "" {
		return nil, "", "", time.Time{}, fmt.Errorf("%w: eventbridge requires CloudEvents source", contract.ErrInvalidDestination)
	}

	if detailType == "" {
		return nil, "", "", time.Time{}, fmt.Errorf("%w: eventbridge requires CloudEvents type", contract.ErrInvalidDestination)
	}

	var eventTime time.Time

	if headers["ce-time"] != "" {
		var err error

		eventTime, err = time.Parse(time.RFC3339Nano, headers["ce-time"])
		if err != nil {
			return nil, "", "", time.Time{}, fmt.Errorf("%w: invalid CloudEvents time: %w", contract.ErrInvalidDestination, err)
		}
	}

	// Order keys deterministically for stable hashing/snapshot-tests
	// downstream. Detail is rendered with json.Marshal on a struct, which
	// preserves declared field order.
	envelope := struct {
		SpecVersion     string          `json:"specversion"`
		ID              string          `json:"id"`
		Source          string          `json:"source"`
		Type            string          `json:"type"`
		Subject         string          `json:"subject,omitempty"`
		Time            string          `json:"time,omitempty"`
		DataContentType string          `json:"datacontenttype,omitempty"`
		DataSchema      string          `json:"dataschema,omitempty"`
		TenantID        string          `json:"tenantid,omitempty"`
		Data            json.RawMessage `json:"data"`
	}{
		SpecVersion:     specVersion,
		ID:              headers["ce-id"],
		Source:          source,
		Type:            detailType,
		Subject:         headers["ce-subject"],
		Time:            headers["ce-time"],
		DataContentType: headers["ce-datacontenttype"],
		DataSchema:      headers["ce-dataschema"],
		TenantID:        headers["ce-tenantid"],
		Data:            message.Payload,
	}

	if len(envelope.Data) == 0 {
		// PutEvents rejects entries with empty Detail. Send an explicit
		// JSON null so the entry still serializes.
		envelope.Data = json.RawMessage("null")
	}

	body, err := json.Marshal(envelope)
	if err != nil {
		return nil, "", "", time.Time{}, fmt.Errorf("%w: eventbridge detail marshal: %w", contract.ErrInvalidDestination, err)
	}

	return body, source, detailType, eventTime, nil
}

// mapHeaders flattens the binary-mode CloudEvents headers into a string map
// for fast lookup while rendering Detail.
func mapHeaders(headers []transport.Header) map[string]string {
	if len(headers) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[strings.ToLower(h.Key)] = string(h.Value)
	}

	return out
}

// extractResources reads a comma-separated ARN list from the
// "eventbridge.resources" attribute. Returns nil when absent or empty.
func extractResources(attrs map[string]string) []string {
	raw, ok := attrs["eventbridge.resources"]
	if !ok || raw == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
