//go:build unit

package streamingtest_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	streaming "github.com/LerianStudio/lib-streaming/v4"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/streamingtest"
)

const (
	signingTopic  = "lerian.streaming.lender"
	signingSource = "lender"
)

func signingEvent() streaming.Event {
	return streaming.Event{
		TenantID:     "tenant-abc",
		ResourceType: "loan_contract",
		EventType:    "disbursed",
		Source:       signingSource,
		Payload:      []byte(`{"amount":"1200.00"}`),
	}
}

// verify runs the library's own verifier over rec, the check a consumer that
// requires signatures performs before any handler runs.
func verify(t *testing.T, ring *streaming.Keyring, rec *kgo.Record) error {
	t.Helper()

	v, err := envelopesig.NewVerifier(ring, 0)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	return v.Verify(rec.Headers, rec.Value)
}

func header(rec *kgo.Record, key string) (string, bool) {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}

	return "", false
}

func TestSigningKey_Deterministic(t *testing.T) {
	t.Parallel()

	a, again := streamingtest.SigningKey("lender-k1", signingSource), streamingtest.SigningKey("lender-k1", signingSource)
	if !bytes.Equal(a.Secret, again.Secret) {
		t.Error("the same id and source gave two secrets; a test key must be reproducible across processes")
	}

	if a.ID != "lender-k1" || a.Source != signingSource {
		t.Errorf("key = {%q %q}; want {lender-k1 %s}", a.ID, a.Source, signingSource)
	}

	if len(a.Secret) < streaming.MinSigningSecretBytes {
		t.Errorf("secret is %d bytes; the keyring refuses fewer than %d", len(a.Secret), streaming.MinSigningSecretBytes)
	}

	for _, other := range []streaming.SigningKey{
		streamingtest.SigningKey("lender-k2", signingSource),
		streamingtest.SigningKey("lender-k1", "matcher"),
	} {
		if bytes.Equal(a.Secret, other.Secret) {
			t.Errorf("key {%s %s} shares the secret of {lender-k1 %s}", other.ID, other.Source, signingSource)
		}
	}

	if _, err := streaming.NewKeyring(a); err != nil {
		t.Errorf("NewKeyring refused a streamingtest key: %v", err)
	}
}

func TestSignedRecord_VerifiesUnderKeyring(t *testing.T) {
	t.Parallel()

	key := streamingtest.SigningKey("lender-k1", signingSource)
	ev := signingEvent()

	rec := streamingtest.SignedRecord(t, signingTopic, key, ev)

	if err := verify(t, streamingtest.Keyring(t, key), rec); err != nil {
		t.Fatalf("Verify(signed record) = %v; want nil", err)
	}

	if rec.Topic != signingTopic {
		t.Errorf("Topic = %q; want %q", rec.Topic, signingTopic)
	}

	if !bytes.Equal(rec.Value, ev.Payload) {
		t.Errorf("Value = %q; want the event payload %q", rec.Value, ev.Payload)
	}

	if string(rec.Key) != ev.TenantID {
		t.Errorf("Key = %q; want the default partition key %q", rec.Key, ev.TenantID)
	}

	if kid, _ := header(rec, streaming.CloudEventsHeaderSignatureKeyID); kid != key.ID {
		t.Errorf("%s = %q; want %q", streaming.CloudEventsHeaderSignatureKeyID, kid, key.ID)
	}

	if id, ok := header(rec, "ce-id"); !ok || id == "" {
		t.Error("ce-id is empty; the helper must apply the event defaults a producer applies")
	}
}

func TestSignedRecord_EmptySourceSignsAsTheKeysSource(t *testing.T) {
	t.Parallel()

	key := streamingtest.SigningKey("lender-k1", signingSource)
	ev := signingEvent()
	ev.Source = ""

	rec := streamingtest.SignedRecord(t, signingTopic, key, ev)

	if source, _ := header(rec, "ce-source"); source != signingSource {
		t.Errorf("ce-source = %q; want the key's source %q", source, signingSource)
	}

	if err := verify(t, streamingtest.Keyring(t, key), rec); err != nil {
		t.Errorf("Verify = %v; want nil", err)
	}
}

func TestSignedRecord_DoesNotAliasThePayload(t *testing.T) {
	t.Parallel()

	key := streamingtest.SigningKey("lender-k1", signingSource)
	ev := signingEvent()

	rec := streamingtest.SignedRecord(t, signingTopic, key, ev)
	ev.Payload[0] = 'X'

	if err := verify(t, streamingtest.Keyring(t, key), rec); err != nil {
		t.Errorf("mutating the caller's payload after signing broke the record: %v", err)
	}
}

func TestUnsignedRecord_IsSignatureMissing(t *testing.T) {
	t.Parallel()

	key := streamingtest.SigningKey("lender-k1", signingSource)

	rec := streamingtest.UnsignedRecord(t, signingTopic, signingEvent())

	for _, name := range []string{
		streaming.CloudEventsHeaderSignatureKeyID,
		streaming.CloudEventsHeaderSignedAt,
		streaming.CloudEventsHeaderSignature,
	} {
		if _, ok := header(rec, name); ok {
			t.Errorf("unsigned record carries %s", name)
		}
	}

	if err := verify(t, streamingtest.Keyring(t, key), rec); !errors.Is(err, streaming.ErrSignatureMissing) {
		t.Errorf("Verify(unsigned record) = %v; want ErrSignatureMissing", err)
	}
}

func TestForgedRecord_IsSignatureInvalid(t *testing.T) {
	t.Parallel()

	key := streamingtest.SigningKey("lender-k1", signingSource)
	ev := signingEvent()

	rec := streamingtest.ForgedRecord(t, signingTopic, key, ev)

	if bytes.Equal(rec.Value, ev.Payload) {
		t.Error("forged record kept the signed body; it must differ from what was signed")
	}

	if _, ok := header(rec, streaming.CloudEventsHeaderSignature); !ok {
		t.Error("forged record has no ce-sig; it must look signed so only the MAC gives it away")
	}

	if err := verify(t, streamingtest.Keyring(t, key), rec); !errors.Is(err, streaming.ErrSignatureInvalid) {
		t.Errorf("Verify(forged record) = %v; want ErrSignatureInvalid", err)
	}
}

func TestSignedRecord_UnknownToAnotherRing(t *testing.T) {
	t.Parallel()

	rec := streamingtest.SignedRecord(t, signingTopic, streamingtest.SigningKey("lender-k1", signingSource), signingEvent())
	other := streamingtest.Keyring(t, streamingtest.SigningKey("lender-k2", signingSource))

	if err := verify(t, other, rec); !errors.Is(err, streaming.ErrSignatureUnknownKey) {
		t.Errorf("Verify under a ring without lender-k1 = %v; want ErrSignatureUnknownKey", err)
	}
}

func TestSignedRecord_SignedAtIsFresh(t *testing.T) {
	t.Parallel()

	before := time.Now().UTC().Add(-time.Second)
	rec := streamingtest.SignedRecord(t, signingTopic, streamingtest.SigningKey("lender-k1", signingSource), signingEvent())

	raw, _ := header(rec, streaming.CloudEventsHeaderSignedAt)

	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("%s = %q is not RFC 3339: %v", streaming.CloudEventsHeaderSignedAt, raw, err)
	}

	if at.Before(before) {
		t.Errorf("%s = %s; want the signing instant, not an earlier time", streaming.CloudEventsHeaderSignedAt, at)
	}
}
