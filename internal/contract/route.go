package contract

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/LerianStudio/lib-commons/v5/commons/security/ssrf"
	"github.com/LerianStudio/lib-observability/assert"
	"github.com/LerianStudio/lib-observability/log"
)

// contractAsserterComponent is the component label for asserter trident
// emissions from the contract package. Mirrors
// internal/producer.asserterComponent and the streaming root facade so
// invariant violations across construction-time (catalog/route/event/
// outbox) and runtime (producer) aggregate under one component axis on
// dashboards.
const contractAsserterComponent = "streaming"

// contractAsserterLogger is the package-default logger that backs every
// contract-package asserter. The contract package has no caller-supplied
// logger today — every NewCatalog / NewRouteDefinition / NewRouteTable /
// NewEventDefinition / OutboxEnvelope.Validate call site is a value-
// receiver constructor with no logger plumbing — so we default to a
// no-op logger here. The asserter's metric layer (assertion_failed_total)
// still fires after the consuming service calls assert.InitAssertionMetrics
// at bootstrap; the log layer is silent unless tests swap this pointer
// through setContractAsserterLogger.
//
// Stored as atomic.Pointer so writers (test-only) call setContractAsserterLogger
// which atomically swaps the pointer — readers (newContractAsserter) load
// the pointer race-free. The earlier plain-var form was a data race against
// parallel tests that swap the logger; production code never writes this
// var, so the cost is one atomic load per asserter construction.
var contractAsserterLogger atomic.Pointer[log.Logger]

func init() {
	nop := log.NewNop()
	contractAsserterLogger.Store(&nop)
}

// newContractAsserter constructs an *assert.Asserter for the per-call-site
// operation, backed by contractAsserterLogger. context.Background() is
// intentional — construction-time call sites are bootstrap-only with no
// caller ctx; the assert package only uses ctx as a fallback when a
// per-call ctx is nil, and each .That/.NotNil call site passes its own
// ctx (typically context.Background() at bootstrap).
func newContractAsserter(operation string) *assert.Asserter {
	return assert.New(context.Background(), *contractAsserterLogger.Load(), contractAsserterComponent, operation)
}

// TransportKind identifies the outbound transport family used by a route.
type TransportKind string

const (
	// TransportKafkaLike selects the legacy Kafka-compatible producer runtime.
	TransportKafkaLike TransportKind = "kafka"
	// TransportSQS selects an Amazon SQS destination.
	TransportSQS TransportKind = "sqs"
	// TransportRabbitMQ selects a RabbitMQ exchange/routing-key destination.
	TransportRabbitMQ TransportKind = "rabbitmq"
	// TransportEventBridge selects an Amazon EventBridge event bus destination.
	TransportEventBridge TransportKind = "eventbridge"
	// TransportCustom selects a caller-owned custom transport adapter.
	TransportCustom TransportKind = "custom"
)

// RouteRequirement declares whether a route must publish successfully for the
// logical emit to be considered delivered.
type RouteRequirement string

const (
	// RouteRequired marks a route as mandatory. Empty requirements normalize to
	// this value.
	RouteRequired RouteRequirement = "required"
	// RouteOptional marks a best-effort route.
	RouteOptional RouteRequirement = "optional"
)

// Destination identifies a concrete transport destination.
type Destination struct {
	Kind       TransportKind     `json:"kind"`
	Name       string            `json:"name,omitempty"`
	Address    string            `json:"address,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Normalize returns a defensive, normalized copy of the destination.
func (d Destination) Normalize() Destination {
	d.Attributes = cloneStringMap(d.Attributes)

	return d
}

// Validate reports malformed destination shape.
func (d Destination) Validate() error {
	if !isValidTransportKind(d.Kind) {
		return fmt.Errorf("%w: kind=%q", ErrInvalidDestination, d.Kind)
	}

	if err := validateDestinationShape(d); err != nil {
		return err
	}

	if err := validateDestinationHeaderFields(d); err != nil {
		return err
	}

	if err := validateDestinationSecurity(d); err != nil {
		return err
	}

	return validateDestinationAttributes(d.Attributes)
}

// RouteDefinition maps one catalog event definition to one transport target and
// destination.
type RouteDefinition struct {
	Key           string
	DefinitionKey string
	Target        string
	Destination   Destination
	Requirement   RouteRequirement
	Policy        DeliveryPolicyOverride
	DLQ           *Destination
	Description   string
}

// NewRouteDefinition validates, normalizes, and defensively copies a route.
func NewRouteDefinition(route RouteDefinition) (RouteDefinition, error) {
	if route.Key == "" {
		return RouteDefinition{}, fmt.Errorf("%w: key required", ErrInvalidRouteDefinition)
	}

	if !isCanonicalRouteKey(route.Key) {
		return RouteDefinition{}, fmt.Errorf("%w: key must be lower-case dot-delimited", ErrInvalidRouteDefinition)
	}

	if route.DefinitionKey == "" {
		return RouteDefinition{}, fmt.Errorf("%w: definition key required", ErrInvalidRouteDefinition)
	}

	if route.Target == "" {
		return RouteDefinition{}, fmt.Errorf("%w: %w", ErrInvalidRouteDefinition, ErrMissingTarget)
	}

	route.Requirement = normalizeRouteRequirement(route.Requirement)
	if !isValidRouteRequirement(route.Requirement) {
		return RouteDefinition{}, fmt.Errorf("%w: requirement=%q", ErrInvalidRouteDefinition, route.Requirement)
	}

	if err := validateRouteHeaderFields(route); err != nil {
		return RouteDefinition{}, fmt.Errorf("%w: %w", ErrInvalidRouteDefinition, err)
	}

	route.Destination = route.Destination.Normalize()
	if err := route.Destination.Validate(); err != nil {
		return RouteDefinition{}, fmt.Errorf("%w: %w", ErrInvalidRouteDefinition, err)
	}

	if err := route.Policy.Validate(); err != nil {
		return RouteDefinition{}, fmt.Errorf("%w: %w", ErrInvalidRouteDefinition, err)
	}

	if route.DLQ != nil {
		dlq := route.DLQ.Normalize()
		if err := dlq.Validate(); err != nil {
			return RouteDefinition{}, fmt.Errorf("%w: dlq: %w", ErrInvalidRouteDefinition, err)
		}

		// Cross-transport DLQ guard. publishRouteDLQ uses the source
		// route's target adapter to deliver the DLQ message; an
		// adapter only accepts destinations whose Kind matches its own
		// (e.g. the Kafka adapter rejects an SQS Destination with
		// ErrInvalidDestination). A mismatched DLQ kind would silently
		// fail every DLQ delivery for the route — fail closed at
		// construction with a precise wiring error instead.
		if dlq.Kind != route.Destination.Kind {
			// Construction-time invariant: publishRouteDLQ uses the
			// source route's adapter, so a cross-kind DLQ silently
			// no-ops every DLQ delivery for the route. Fire the
			// trident so the rejection becomes a loud signal alongside
			// the wrapped sentinel.
			a := newContractAsserter("route.dlq_kind_match")
			_ = a.That(context.Background(), false, "route DLQ kind must match destination kind",
				"route_key", route.Key,
				"destination_kind", string(route.Destination.Kind),
				"dlq_kind", string(dlq.Kind),
			)

			return RouteDefinition{}, fmt.Errorf("%w: route %q DLQ kind %q does not match destination kind %q",
				ErrInvalidRouteDefinition, route.Key, dlq.Kind, route.Destination.Kind)
		}

		route.DLQ = &dlq
	}

	if route.Policy.Enabled != nil {
		enabled := *route.Policy.Enabled
		route.Policy.Enabled = &enabled
	}

	return route, nil
}

// RouteTable is an immutable, deterministically ordered registry of routes
// keyed by catalog definition key.
type RouteTable struct {
	definitions  []RouteDefinition
	byDefinition map[string][]RouteDefinition
}

// NewRouteTable validates routes, rejects duplicate route keys, and stores
// routes in deterministic definition-key/key order.
func NewRouteTable(routes ...RouteDefinition) (RouteTable, error) {
	if len(routes) == 0 {
		return RouteTable{}, ErrNoRoutesConfigured
	}

	ordered := make([]RouteDefinition, 0, len(routes))
	seenKeys := make(map[string]struct{}, len(routes))

	for _, raw := range routes {
		route, err := NewRouteDefinition(raw)
		if err != nil {
			return RouteTable{}, err
		}

		if _, exists := seenKeys[route.Key]; exists {
			a := newContractAsserter("route_table.new")
			_ = a.That(context.Background(), false, "route table must not contain duplicate Key entries",
				"route_key", route.Key,
				"definition_key", route.DefinitionKey,
				"target", route.Target,
			)

			return RouteTable{}, fmt.Errorf("%w: key %q", ErrDuplicateRouteDefinition, route.Key)
		}

		seenKeys[route.Key] = struct{}{}
		ordered = append(ordered, cloneRouteDefinition(route))
	}

	sortRouteDefinitions(ordered)

	return RouteTable{
		definitions:  ordered,
		byDefinition: buildRoutesByDefinition(ordered),
	}, nil
}

// Routes returns the routes registered for definitionKey in deterministic
// order. The returned slice and every element is a defensive copy — callers
// may mutate freely without affecting the immutable RouteTable.
func (t RouteTable) Routes(definitionKey string) []RouteDefinition {
	routes := t.byDefinition[definitionKey]
	if len(routes) == 0 {
		return []RouteDefinition{}
	}

	return cloneRouteDefinitions(routes)
}

// routesUnsafe returns the internal pre-sorted slice WITHOUT cloning.
// Callers MUST NOT mutate the returned slice or its elements; use Routes()
// for external callers. Internal-only fast path used by the multi-target
// Emit hot path so per-route dispatch does not pay the per-Emit clone cost
// for every Emit on every required route.
//
// RouteTable is documented immutable post-construction (NewRouteTable
// validates and clones at construction); this method preserves that
// invariant by promising read-only access to the canonical slice.
//
// Reachable from internal/producer via the package-internal RoutesUnsafe
// helper below — both packages are under internal/, so the uppercase
// helper is acceptable as an "internal-only escape hatch" form.
func (t RouteTable) routesUnsafe(definitionKey string) []RouteDefinition {
	return t.byDefinition[definitionKey]
}

// RoutesUnsafe is the cross-internal-package accessor for RouteTable's
// canonical pre-sorted route slice. Returns the internal slice WITHOUT
// cloning. Callers MUST NOT mutate the returned slice or its elements.
//
// Use Routes() (the receiver method) for external callers and any place a
// defensive copy is needed. RoutesUnsafe is reserved for the multi-target
// Emit hot path in internal/producer where the cost of cloning per Emit
// matters and the read-only invariant is preserved by construction.
//
// Accepts *RouteTable to avoid copying the (small) struct on the hot path
// while still preserving the value-receiver method consistency with
// Routes(). Nil-safe — returns nil for a nil table.
func RoutesUnsafe(t *RouteTable, definitionKey string) []RouteDefinition {
	if t == nil {
		return nil
	}

	return t.routesUnsafe(definitionKey)
}

// Definitions returns all route definitions in deterministic order.
func (t RouteTable) Definitions() []RouteDefinition {
	if len(t.definitions) == 0 {
		return []RouteDefinition{}
	}

	return cloneRouteDefinitions(t.definitions)
}

// Len returns the number of routes in the table.
func (t RouteTable) Len() int {
	return len(t.definitions)
}

func normalizeRouteRequirement(requirement RouteRequirement) RouteRequirement {
	if requirement == "" {
		return RouteRequired
	}

	return requirement
}

func isValidRouteRequirement(requirement RouteRequirement) bool {
	switch requirement {
	case RouteRequired, RouteOptional:
		return true
	default:
		return false
	}
}

func isValidTransportKind(kind TransportKind) bool {
	switch kind {
	case TransportKafkaLike, TransportSQS, TransportRabbitMQ, TransportEventBridge, TransportCustom:
		return true
	default:
		return false
	}
}

func validateDestinationShape(destination Destination) error {
	switch destination.Kind {
	case TransportKafkaLike:
		return validateKafkaDestinationShape(destination)
	case TransportSQS:
		return validateSQSDestinationShape(destination)
	case TransportRabbitMQ:
		return validateRabbitMQDestinationShape(destination)
	case TransportEventBridge:
		return validateEventBridgeDestinationShape(destination)
	case TransportCustom:
		return validateCustomDestinationShape(destination)
	}

	return nil
}

func validateKafkaDestinationShape(destination Destination) error {
	if destination.Name == "" {
		return fmt.Errorf("%w: kafka topic name required", ErrInvalidDestination)
	}

	if destination.Address != "" {
		return fmt.Errorf("%w: kafka address must be empty", ErrInvalidDestination)
	}

	return nil
}

func validateSQSDestinationShape(destination Destination) error {
	if destination.Name != "" {
		return fmt.Errorf("%w: sqs name must be empty", ErrInvalidDestination)
	}

	return validateDestinationURL(destination.Address, true)
}

func validateRabbitMQDestinationShape(destination Destination) error {
	if destination.Name == "" {
		return fmt.Errorf("%w: rabbitmq exchange name required", ErrInvalidDestination)
	}

	if destination.Address == "" {
		return fmt.Errorf("%w: rabbitmq routing key required", ErrInvalidDestination)
	}

	return nil
}

func validateEventBridgeDestinationShape(destination Destination) error {
	if destination.Name == "" {
		return fmt.Errorf("%w: eventbridge bus name required", ErrInvalidDestination)
	}

	if destination.Address != "" {
		return fmt.Errorf("%w: eventbridge address must be empty", ErrInvalidDestination)
	}

	return nil
}

func validateCustomDestinationShape(destination Destination) error {
	if destination.Name == "" && destination.Address == "" {
		return fmt.Errorf("%w: custom name or address required", ErrInvalidDestination)
	}

	return nil
}

func validateDestinationHeaderFields(destination Destination) error {
	checks := [...]HeaderFieldCheck{
		{Value: string(destination.Kind), MaxBytes: MaxEventTypeBytes, Sentinel: ErrInvalidDestination},
		{Value: destination.Name, MaxBytes: MaxDataSchemaBytes, Sentinel: ErrInvalidDestination},
		{Value: destination.Address, MaxBytes: MaxDataSchemaBytes, Sentinel: ErrInvalidDestination},
	}

	for _, c := range checks {
		if len(c.Value) > c.MaxBytes || HasControlChar(c.Value) {
			return c.Sentinel
		}
	}

	return nil
}

func validateDestinationSecurity(destination Destination) error {
	if containsTenantTopologyToken(destination.Name) || containsTenantTopologyToken(destination.Address) {
		return fmt.Errorf("%w: tenant-scoped destination topology is not allowed", ErrInvalidDestination)
	}

	if containsCredentialLikeMaterial(destination.Name) || containsCredentialLikeMaterial(destination.Address) {
		return fmt.Errorf("%w: credential-like destination material is not allowed", ErrInvalidDestination)
	}

	if err := validateDestinationURL(destination.Address, false); err != nil {
		return err
	}

	for key, value := range destination.Attributes {
		if containsCredentialLikeMaterial(key) || containsCredentialLikeMaterial(value) {
			return fmt.Errorf("%w: credential-like destination attribute is not allowed", ErrInvalidDestination)
		}

		if containsTenantTopologyToken(key) || containsTenantTopologyToken(value) {
			return fmt.Errorf("%w: tenant-scoped destination attribute is not allowed", ErrInvalidDestination)
		}
	}

	return nil
}

func validateDestinationURL(raw string, requireQueueURL bool) error {
	if raw == "" {
		return nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: invalid destination URL", ErrInvalidDestination)
	}

	if parsed.User != nil {
		return fmt.Errorf("%w: destination URL userinfo is not allowed", ErrInvalidDestination)
	}

	// Defense-in-depth: rejects accidentally-pasted credentials in URL
	// fragments. The fragment never reaches the wire (HTTP/SDK clients
	// strip it on send), but rejecting at construction prevents a leak
	// path through error messages, logs, and manifest documents that
	// echo Destination.Address back unmodified.
	if containsCredentialLikeMaterial(parsed.Fragment) {
		return fmt.Errorf("%w: credential-like destination URL fragment is not allowed", ErrInvalidDestination)
	}

	if parsed.Host != "" && parsed.Scheme == "" {
		return fmt.Errorf("%w: protocol-relative destination URL is not allowed", ErrInvalidDestination)
	}

	if err := validateDestinationURLQuery(parsed); err != nil {
		return err
	}

	if requireQueueURL {
		return validateSQSQueueURL(raw, parsed)
	}

	if parsed.Scheme == "" {
		return nil
	}

	if err := ssrf.ValidateURL(context.Background(), raw); err != nil {
		return fmt.Errorf("%w: unsafe destination URL", ErrInvalidDestination)
	}

	return nil
}

func validateDestinationURLQuery(parsed *url.URL) error {
	for key, values := range parsed.Query() {
		if containsCredentialLikeMaterial(key) {
			return fmt.Errorf("%w: credential-like destination URL query is not allowed", ErrInvalidDestination)
		}

		if slices.ContainsFunc(values, containsCredentialLikeMaterial) {
			return fmt.Errorf("%w: credential-like destination URL query is not allowed", ErrInvalidDestination)
		}
	}

	return nil
}

func validateSQSQueueURL(raw string, parsed *url.URL) error {
	if parsed.Scheme == "" || parsed.Host == "" || parsed.Path == "" {
		return fmt.Errorf("%w: sqs queue URL must include scheme, host, and path", ErrInvalidDestination)
	}

	// SQS queue URLs are validated with DNS pinning at construction time:
	// ResolveAndValidate performs the same scheme + hostname-blocklist checks
	// as ValidateURL AND additionally resolves the hostname and rejects the
	// URL if any returned IP is in a blocked range (loopback, link-local,
	// private RFC1918, cloud-metadata ranges). This closes the TOCTOU window
	// between preflight and the SDK's own DNS lookup at publish time.
	//
	// We discard the *ResolveResult (pinned URL, authority, SNI) — the AWS
	// SDK owns the connection and will perform its own resolution. Our use
	// is "fail at construction if the URL is not safely resolvable today",
	// not "drive the connection from a pre-pinned IP". One DNS lookup at
	// NewRouteDefinition / NewRouteTable time is acceptable; routes are
	// constructed once at bootstrap, not per-Emit.
	if _, err := ssrf.ResolveAndValidate(context.Background(), raw, ssrf.WithHTTPSOnly()); err != nil {
		return fmt.Errorf("%w: unsafe sqs queue URL", ErrInvalidDestination)
	}

	return nil
}

func validateDestinationAttributes(attributes map[string]string) error {
	for key, value := range attributes {
		if key == "" || len(key) > MaxEventIDBytes || HasControlChar(key) {
			return fmt.Errorf("%w: invalid attribute key", ErrInvalidDestination)
		}

		if len(value) > MaxDataSchemaBytes || HasControlChar(value) {
			return fmt.Errorf("%w: invalid attribute value for %q", ErrInvalidDestination, key)
		}
	}

	return nil
}

func validateRouteHeaderFields(route RouteDefinition) error {
	// Description is bounded by MaxDataSchemaBytes (2 KiB) to match the
	// other long-form free-text fields in the catalog (DataSchema URI,
	// EventDefinition.Description). A future ManifestRoute exposing
	// Description for ops/contract introspection should be defended at
	// construction-time, not at render-time.
	checks := [...]HeaderFieldCheck{
		{Value: route.Key, MaxBytes: MaxEventIDBytes, Sentinel: ErrInvalidRouteDefinition},
		{Value: route.DefinitionKey, MaxBytes: MaxEventIDBytes, Sentinel: ErrInvalidEventDefinition},
		{Value: route.Target, MaxBytes: MaxEventIDBytes, Sentinel: ErrMissingTarget},
		{Value: route.Description, MaxBytes: MaxDataSchemaBytes, Sentinel: ErrInvalidRouteDefinition},
	}

	for _, c := range checks {
		if len(c.Value) > c.MaxBytes || HasControlChar(c.Value) {
			return c.Sentinel
		}
	}

	if containsTenantTopologyToken(route.Key) ||
		containsTenantTopologyToken(route.Target) ||
		containsTenantTopologyToken(route.Description) {
		return fmt.Errorf("%w: tenant-scoped route topology is not allowed", ErrInvalidRouteDefinition)
	}

	if containsCredentialLikeMaterial(route.Key) ||
		containsCredentialLikeMaterial(route.Target) ||
		containsCredentialLikeMaterial(route.Description) {
		return fmt.Errorf("%w: credential-like route material is not allowed", ErrInvalidRouteDefinition)
	}

	return nil
}

var (
	routeKeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+$`)
	// Matches assignment-like fields; sensitivity of the captured key is decided
	// by lib-observability/redaction plus streaming-specific extras.
	sensitiveAssignmentPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9])([a-z][a-z0-9._-]*)([[:space:]]*[:=])`)
	// Matches inline auth schemes such as "Bearer abc" or "Basic abc". Covered by route/sanitize tests.
	authSchemeCredentialPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9])(bearer|basic|token)[[:space:]:=]+[a-z0-9._~+/=-]+`)
	// Matches AWS access key IDs (AKIA/ASIA + 16 uppercase base32-ish chars). Covered by route/sanitize tests.
	awsAccessKeyIDPattern = regexp.MustCompile(`\b(AKIA|ASIA)[0-9A-Z]{16}\b`)
	// gcpAPIKeyPattern matches Google Cloud API keys: "AIza" + 35 url-safe chars. Covered by route_test.go.
	gcpAPIKeyPattern = regexp.MustCompile(`\bAIza[0-9A-Za-z_\-]{35}\b`)
	// azureSASPattern matches Azure SAS query parameters (sv/se/sp/sig) when they appear at the start of the input or after a query separator. Covered by route_test.go.
	azureSASPattern = regexp.MustCompile(`(?i)(^|[?&])(sig|sv|se|sp)=[A-Za-z0-9%/+=_\-]+`)
	// githubTokenPattern matches GitHub PATs and OAuth tokens. Covered by route_test.go.
	githubTokenPattern = regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{20,255}\b`)
	// stripeKeyPattern matches Stripe live/test secret/restricted/publishable keys. Covered by route_test.go.
	stripeKeyPattern = regexp.MustCompile(`\b(sk|rk|pk)_(live|test)_[A-Za-z0-9]{16,}\b`)
	// slackTokenPattern matches Slack tokens (bot/app/refresh/user/legacy). Covered by route_test.go.
	slackTokenPattern = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	// pemPrivateKeyPattern matches the PEM block header for any private key form (RSA, EC, generic). Covered by route_test.go.
	pemPrivateKeyPattern = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)
	// jwtBarePattern matches a bare JWT (header.payload.signature, base64url). Covered by route_test.go.
	jwtBarePattern = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
)

// ContainsCredentialLikeMaterial reports whether value carries explicit
// credential material or a credential-shaped key. Plain business words such as
// "authorization" and "tokenization" are intentionally allowed when they are
// not used as assignments or credential key names.
func ContainsCredentialLikeMaterial(value string) bool {
	return containsCredentialLikeMaterial(value)
}

func containsCredentialLikeMaterial(value string) bool {
	if containsSensitiveAssignment(value) || authSchemeCredentialPattern.MatchString(value) || awsAccessKeyIDPattern.MatchString(value) {
		return true
	}

	// Cloud and SaaS provider credential shapes. Each pattern is anchored
	// (word boundary or charset) so plain business words ("authorization",
	// "tokenization", topic names containing "auth") still pass.
	if gcpAPIKeyPattern.MatchString(value) ||
		azureSASPattern.MatchString(value) ||
		githubTokenPattern.MatchString(value) ||
		stripeKeyPattern.MatchString(value) ||
		slackTokenPattern.MatchString(value) ||
		pemPrivateKeyPattern.MatchString(value) ||
		jwtBarePattern.MatchString(value) {
		return true
	}

	if isCredentialLikeFieldName(value) {
		return true
	}

	return false
}

func containsSensitiveAssignment(value string) bool {
	matches := sensitiveAssignmentPattern.FindAllStringSubmatch(value, -1)
	for _, match := range matches {
		if len(match) >= 3 && isSensitiveKeyValueFieldName(match[2]) {
			return true
		}
	}

	return false
}

func isCanonicalRouteKey(value string) bool {
	return routeKeyPattern.MatchString(value)
}

func isCredentialLikeFieldName(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.ContainsAny(trimmed, " /\\") {
		return false
	}

	components := splitRouteFieldName(trimmed)
	if isSensitiveFieldName(trimmed) {
		return !isAllowedBusinessSensitivePhrase(components)
	}

	if slices.ContainsFunc(components, isSensitiveFieldName) {
		return true
	}

	return false
}

func splitRouteFieldName(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == '.' || r == '_' || r == '-'
	})
}

func isAllowedBusinessSensitivePhrase(components []string) bool {
	if len(components) < 3 {
		return false
	}

	hasAllowedBusinessToken := false

	for _, component := range components {
		lower := strings.ToLower(component)
		if isCredentialContextComponent(lower) {
			return false
		}

		if !isSensitiveFieldName(component) {
			continue
		}

		switch lower {
		case "authorization", "token":
			hasAllowedBusinessToken = true
		default:
			return false
		}
	}

	return hasAllowedBusinessToken
}

func isCredentialContextComponent(component string) bool {
	switch component {
	case "auth", "header", "headers", "proxy", "x", "aws", "amz", "access", "session", "security":
		return true
	default:
		return false
	}
}

var tenantTopologyTokens = [...]string{
	"tenant_id",
	"tenantid",
	"tenant-id",
	"{tenant}",
	"{tenant_id}",
	"${tenant}",
	"${tenant_id}",
	":tenant",
	":tenant_id",
}

func containsTenantTopologyToken(value string) bool {
	value = strings.ToLower(value)
	for _, token := range tenantTopologyTokens {
		if strings.Contains(value, token) {
			return true
		}
	}

	return false
}

func cloneRouteDefinitions(src []RouteDefinition) []RouteDefinition {
	if len(src) == 0 {
		return []RouteDefinition{}
	}

	dst := make([]RouteDefinition, len(src))
	for i, route := range src {
		dst[i] = cloneRouteDefinition(route)
	}

	return dst
}

func cloneRouteDefinition(route RouteDefinition) RouteDefinition {
	route.Destination = route.Destination.Normalize()
	if route.Policy.Enabled != nil {
		enabled := *route.Policy.Enabled
		route.Policy.Enabled = &enabled
	}

	if route.DLQ != nil {
		dlq := route.DLQ.Normalize()
		route.DLQ = &dlq
	}

	return route
}

// cloneStringMap returns a defensive copy of src. Empty input (nil or
// zero-length) collapses to nil, NOT to a zero-length map. This is
// intentional and diverges from stdlib maps.Clone, which preserves the
// zero-length-but-non-nil distinction.
//
// The nil-collapsing behavior matters because:
//   - Destination.Attributes is JSON-serialized with `,omitempty`; a
//     non-nil zero-length map would render as "attributes":{} on the
//     wire whereas nil correctly omits the field.
//   - Manifest documents stay byte-stable across cloned routes.
//
// Duplicated as-is in internal/transport/transport.go (cross-package
// dedup is a separate concern; both copies must keep the nil-collapse
// semantic for manifest/transport JSON parity).
func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}

func sortRouteDefinitions(routes []RouteDefinition) {
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].DefinitionKey == routes[j].DefinitionKey {
			return routes[i].Key < routes[j].Key
		}

		return routes[i].DefinitionKey < routes[j].DefinitionKey
	})
}

func buildRoutesByDefinition(routes []RouteDefinition) map[string][]RouteDefinition {
	byDefinition := make(map[string][]RouteDefinition)
	for _, route := range routes {
		byDefinition[route.DefinitionKey] = append(byDefinition[route.DefinitionKey], cloneRouteDefinition(route))
	}

	return byDefinition
}
