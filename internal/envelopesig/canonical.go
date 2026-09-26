// Package envelopesig is the one definition of lib-streaming's envelope
// signature: the canonical bytes, the HMAC-SHA256 signer every producer publish
// path calls, and the verifier the consumer runtime runs before dispatch.
//
// # Wire format (v1)
//
// Three CloudEvents binary-mode extension headers, valid extension names under
// the CloudEvents spec (lowercase alphanumeric, at most 20 characters):
//
//   - ce-sigkid: the signing key id.
//   - ce-sigts: the signing instant, RFC 3339 with nanoseconds, UTC — a
//     CloudEvents Timestamp, the same layout as ce-time. It is the instant of
//     the PUBLISH (direct, outbox relay or producer DLQ), never the enqueue.
//   - ce-sig: "v1." + base64url-unpadded(HMAC-SHA256(secret, canonical)).
//
// The canonical bytes are the domain tag "lerian.streaming.sig.v1\x00",
// then, for each field in signedFieldOrder, one presence byte (0 absent,
// 1 present), a big-endian uint32 length and the raw header bytes, then the
// 32-byte SHA-256 of the record body. Every ce-* header the codec defines is
// signed — ce-resourcetype and ce-eventtype select the handler and
// ce-schemaversion selects the payload parser, so leaving any of them out
// would let a valid signature carry a real payload to a different handler.
// Presence is encoded so an absent optional header and an empty one differ,
// and lengths are encoded so no two field sets concatenate to the same bytes.
//
// Both sides build the canonical bytes from header BYTES — the producer from
// the headers it just built, the verifier from the record — so no value is
// ever re-formatted and no time layout can drift between them.
package envelopesig

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
)

// The three signature headers. Declared once here; the root facade restates
// them as literals and a test pins the two together.
const (
	HeaderKeyID     = "ce-sigkid"
	HeaderSignedAt  = "ce-sigts"
	HeaderSignature = "ce-sig"
)

const (
	// signatureVersionPrefix leaves room for a future format (an asymmetric
	// "v2." scheme) without an ambiguous parse.
	signatureVersionPrefix = "v1."
	domainTag              = "lerian.streaming.sig.v1\x00"
)

// Canonical field indexes. The order IS the wire format.
const (
	idxSpecVersion = iota
	idxID
	idxSource
	idxType
	idxTime
	idxSubject
	idxTenantID
	idxDataContentType
	idxDataSchema
	idxSchemaVersion
	idxResourceType
	idxEventType
	idxSystemEvent
	idxKeyID
	idxSignedAt
	fieldCount
)

// signedEnvelopeHeaders are the 13 CloudEvents headers the codec defines, in
// canonical order. They are restated rather than imported because the codec
// keeps its keys unexported; TestSignedFields_CoverEveryCodecHeader pins this
// list against what the codec actually emits.
var signedEnvelopeHeaders = [idxKeyID]string{
	idxSpecVersion:     "ce-specversion",
	idxID:              "ce-id",
	idxSource:          "ce-source",
	idxType:            "ce-type",
	idxTime:            "ce-time",
	idxSubject:         "ce-subject",
	idxTenantID:        "ce-tenantid",
	idxDataContentType: "ce-datacontenttype",
	idxDataSchema:      "ce-dataschema",
	idxSchemaVersion:   "ce-schemaversion",
	idxResourceType:    "ce-resourcetype",
	idxEventType:       "ce-eventtype",
	idxSystemEvent:     "ce-systemevent",
}

// fieldIndex maps every canonical header key, the signature's own two
// included, to its position. ce-sig itself is not a canonical field.
var fieldIndex = func() map[string]int {
	index := make(map[string]int, fieldCount)
	for i, key := range signedEnvelopeHeaders {
		index[key] = i
	}

	index[HeaderKeyID] = idxKeyID
	index[HeaderSignedAt] = idxSignedAt

	return index
}()

type field struct {
	value   []byte
	present bool
}

type fieldSet [fieldCount]field

// canonical returns the exact bytes the MAC covers.
func canonical(fields *fieldSet, body []byte) []byte {
	size := len(domainTag) + sha256.Size
	for i := range fields {
		size += 1 + 4 + len(fields[i].value)
	}

	buf := make([]byte, 0, size)
	buf = append(buf, domainTag...)

	for i := range fields {
		presence := byte(0)
		if fields[i].present {
			presence = 1
		}

		buf = append(buf, presence)
		buf = binary.BigEndian.AppendUint32(buf, lengthPrefix(fields[i].value))
		buf = append(buf, fields[i].value...)
	}

	digest := sha256.Sum256(body)

	return append(buf, digest[:]...)
}

// lengthPrefix returns len(value) as a uint32. Kafka encodes a header value's
// length as an int32, so no record can carry a value past that bound; the
// clamp only keeps the conversion total.
func lengthPrefix(value []byte) uint32 {
	n := min(len(value), math.MaxUint32)

	return uint32(n) // #nosec G115 -- clamped to math.MaxUint32 above
}
