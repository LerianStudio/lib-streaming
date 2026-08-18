package contract

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"golang.org/x/mod/semver"

	"github.com/LerianStudio/lib-commons/v6/commons"
)

// Event is the CloudEvents-aligned envelope produced by a service method.
// Field names are spelled out and free of Kafka/AMQP vocabulary (DX-A02).
//
// Required CloudEvents fields (ce-* headers on the wire):
//
//   - TenantID: maps to ce-tenantid (Lerian extension). Required for
//     non-system events; when SystemEvent is true, TenantID is optional.
//     The library does NOT derive TenantID from ambient context — callers
//     must populate it explicitly (see the struct field doc below).
//   - ResourceType / EventType: composed into ce-type together with Source as
//     "studio.lerian.<Source>.<ResourceType>.<EventType>". The application
//     segment is what keeps two services' homonymous events distinguishable
//     on a shared consumer.
//   - EventID: maps to ce-id. Auto-populated by ApplyDefaults using uuid.NewV7.
//   - SchemaVersion: maps to ce-schemaversion (extension). Default "1.0.0".
//     ce-schemaversion is the ONLY version carrier — the topic never encodes
//     a schema version.
//   - Timestamp: maps to ce-time. Auto-populated to time.Now().UTC() when zero.
//   - Source: maps to ce-source. Required. It is the producing application's
//     name as a single dot-free lowercase segment, e.g. "lender",
//     "midaz-ledger", "br_consignado_gw". See ValidateSource.
//
// Optional CloudEvents fields:
//
//   - Subject: maps to ce-subject. Typically the aggregate ID.
//   - DataContentType: maps to ce-datacontenttype. Default "application/json".
//   - DataSchema: maps to ce-dataschema. Optional schema URI.
//
// Lerian extensions:
//
//   - SystemEvent: when true, emits ce-systemevent: "true", omits ce-tenantid
//     entirely, and allows an empty TenantID. The PartitionKey becomes
//     "system:" + EventType.
//   - Payload: the raw domain payload bytes, sent unchanged as the Kafka
//     message value. Consumers read metadata from the ce-* headers.
type Event struct {
	// TenantID identifies the tenant that owns this event. It is OPTIONAL: an
	// empty TenantID denotes a single-tenant deployment and is fully valid for
	// business events. Single-tenant and multi-tenant services run on
	// physically segregated infrastructure (dedicated vs shared DB), so the
	// library imposes no tenant requirement here.
	//
	// The library does NOT cross-check TenantID against any ambient context
	// value — the caller is responsible for ensuring TenantID matches the
	// authenticated tenant on the request context. Mismatches are silently
	// accepted.
	//
	// Multi-tenant services SHOULD populate it from context:
	//
	//	tenantID, _ := tmcore.GetTenantIDContext(ctx)
	//	event.TenantID = tenantID
	TenantID      string
	ResourceType  string
	EventType     string
	EventID       string
	SchemaVersion string
	Timestamp     time.Time
	Source        string

	Subject         string
	DataContentType string
	DataSchema      string

	// SystemEvent marks this event as platform-level (not tenant-scoped).
	// When true, the producer emits ce-systemevent: "true", omits
	// ce-tenantid from headers, and uses "system:" + EventType as the
	// partition key.
	//
	// This is a privileged capability. The producer MUST be constructed
	// with WithAllowSystemEvents() — otherwise preFlight rejects the emit
	// with ErrSystemEventsNotAllowed. FORBIDDEN for per-tenant service
	// flows: a buggy service that sets SystemEvent=true would hijack the
	// system:* partition space.
	SystemEvent bool
	Payload     json.RawMessage
}

// defaultSchemaVersion is the ce-schemaversion used when the caller leaves
// Event.SchemaVersion empty. Chosen so Topic() yields the base form (no
// ".v<major>" suffix) for first-version events.
const defaultSchemaVersion = "1.0.0"

// defaultDataContentType is the ce-datacontenttype used when the caller
// leaves Event.DataContentType empty. Matches the CloudEvents spec default.
const defaultDataContentType = "application/json"

// Topic returns the ONE topic this event's producing application publishes
// to: "lerian.streaming." + Source.
//
// The topic carries NO resource type, NO event type, and NO schema version.
// Every event a service emits — business facts and service-to-service
// commands alike — rides the same app topic; consumers subscribe to the app
// stream and dispatch per event using the ce-resourcetype / ce-eventtype
// headers. Kafka ACLs scope a producer to its single topic (plus its
// ".dlq"), which is a tighter grant than the per-event topic space it
// replaces.
//
// Source is expected to be pre-validated (ValidateSource) at config,
// Builder, and preflight time, so Topic() stays a zero-allocation hot-path
// helper with no validation branch of its own.
func (e *Event) Topic() string {
	if e == nil {
		return ""
	}

	return AppTopic(e.Source)
}

// PartitionKey returns the Kafka partition key for this event, resolved
// through a fallback chain:
//
//  1. SystemEvent: "system:" + EventType. Gives platform-level, tenant-less
//     events a deterministic key of their own.
//  2. TenantID, when set — routes every event of a tenant to one partition
//     under a sticky-key partitioner.
//  3. Subject, when set — the aggregate id. Routes every event of one
//     aggregate to one partition for a single-tenant service, which is the
//     grouping that matters once there is no tenant to group by.
//  4. EventID — no grouping at all, but it spreads.
//
// WHAT THE KEY ACTUALLY GUARANTEES — read this before promising FIFO to
// anyone. The key controls PARTITION AFFINITY. Whether affinity becomes
// ORDERING depends on how the event reached the broker:
//
//   - DIRECT emit: per-tenant FIFO holds. Records are produced in call order
//     to one partition, and a partition is ordered.
//   - OUTBOX-RELAYED emit: per-tenant partition AFFINITY holds, strict order
//     does NOT. The lib-commons outbox relay drains rows with per-event retry
//     and no per-aggregate serialization, so a row that fails and is retried
//     republishes AFTER a later row of the same tenant. Same partition, wrong
//     order.
//
// This is not a corner case: services that emit exclusively through the
// outbox get affinity and nothing more. A consumer that needs strict
// per-aggregate order must reconcile on its own sequence/version field rather
// than trusting arrival order.
//
// Steps 3 and 4 exist because of the topic collapse. franz-go's sticky-key
// partitioner branches on record.Key != nil, and []byte("") is NOT nil: an
// empty key takes the murmur2 path on a constant and lands every record on
// ONE partition. In v2 a single-tenant service's traffic was spread across
// per-event topics, so the empty key was harmless; in v3 it is one topic per
// application, so it would pin the entire application stream to one partition.
//
// Grouping consequence, stated plainly: multi-tenant services group by tenant;
// single-tenant services group by aggregate via Subject; events with neither a
// tenant nor a subject have no grouping at all and are spread by EventID. Each
// of those becomes an ORDER guarantee only on the direct-emit path — see the
// outbox caveat above.
//
// Operators may override this per-Emitter via WithPartitionKey. This method
// returns the struct-level default only.
func (e *Event) PartitionKey() string {
	if e == nil {
		return ""
	}

	if e.SystemEvent {
		return "system:" + e.EventType
	}

	if e.TenantID != "" {
		return e.TenantID
	}

	if e.Subject != "" {
		return e.Subject
	}

	return e.EventID
}

// ApplyDefaults MUTATES the receiver in place, filling zero-valued optional
// fields with sensible defaults:
//
//   - EventID → commons.GenerateUUIDv7().String() when empty
//   - Timestamp → time.Now().UTC() when zero
//   - SchemaVersion → "1.0.0" when empty
//   - DataContentType → "application/json" when empty
//
// Explicit values are preserved. Safe to call on a fully-populated event.
//
// Non-destructiveness on the Emit path is a property of Emit (which passes
// event by value, so ApplyDefaults lands on a local copy), NOT a property of
// this method. External callers who invoke (*Event).ApplyDefaults() on their
// own struct WILL see mutation on the receiver.
//
// If UUIDv7 generation fails (vanishingly unlikely — falls back to random
// bytes), EventID is left empty and the caller's own validation can surface
// the issue.
func (e *Event) ApplyDefaults() {
	if e == nil {
		return
	}

	if e.EventID == "" {
		if id, err := commons.GenerateUUIDv7(); err == nil {
			e.EventID = id.String()
		}
	}

	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}

	if e.SchemaVersion == "" {
		e.SchemaVersion = defaultSchemaVersion
	}

	if e.DataContentType == "" {
		e.DataContentType = defaultDataContentType
	}
}

// parseMajorVersionStrict reports whether v is a parseable semver, returning
// (major, true) when it is (or when it is empty — treated as the documented
// default) and (0, false) when it is non-empty but unparseable.
//
// Its ONLY caller is the construction-time SchemaVersion gate in
// NewEventDefinition. In v3 the major version no longer influences the topic
// (schema version left the topic entirely and lives solely in the
// ce-schemaversion header), so there is no hot-path major-version parse and
// no exported ParseMajorVersion — the v2 exports existed to keep the runtime
// topic derivation and its tests on one implementation.
func parseMajorVersionStrict(v string) (int, bool) {
	if v == "" {
		// Empty is the documented default; ApplyDefaults / NewEventDefinition
		// normalize it to "1.0.0" upstream. Treat as "valid; major=0" so
		// the caller sees ok=true and topic falls through to base form.
		return 0, true
	}

	// Fast path for the overwhelmingly-common production case: first-major
	// schemas. defaultSchemaVersion ("1.0.0") is the value ApplyDefaults
	// writes when the caller leaves SchemaVersion empty, so the vast
	// majority of events flowing through Topic() hit this branch. Bypassing
	// semver.Major here saves a full semver parse per Emit.
	if v == defaultSchemaVersion || v == "v"+defaultSchemaVersion || v == "1" || v == "v1" {
		return 1, true
	}

	// semver.Major requires a leading "v". Normalize by re-prefixing.
	trimmed := strings.TrimPrefix(v, "v")
	canonical := "v" + trimmed

	major := semver.Major(canonical)
	if major == "" {
		return 0, false
	}

	// semver.Major returns "vN" on success; strip the "v" and parse.
	n, err := strconv.Atoi(strings.TrimPrefix(major, "v"))
	if err != nil || n < 0 {
		return 0, false
	}

	return n, true
}
