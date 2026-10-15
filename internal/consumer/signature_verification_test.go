//go:build unit

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// sigSource is the ce-source ceHeaders stamps, so a key bound to it verifies
// the records the shared harness builds.
const sigSource = "test-source"

// sigKey returns a key with a deterministic 32-byte secret derived from seed.
func sigKey(id, source string, seed byte) envelopesig.Key {
	secret := make(envelopesig.Secret, envelopesig.MinSecretBytes)
	for i := range secret {
		secret[i] = seed + byte(i)
	}

	return envelopesig.Key{ID: id, Source: source, Secret: secret}
}

func sigRing(t testingTB, keys ...envelopesig.Key) *envelopesig.Keyring {
	t.Helper()

	ring, err := envelopesig.NewKeyring(keys...)
	if err != nil {
		t.Fatalf("NewKeyring: %v", err)
	}

	return ring
}

func sigVerifier(t testingTB, maxSkew time.Duration, keys ...envelopesig.Key) *envelopesig.Verifier {
	t.Helper()

	v, err := envelopesig.NewVerifier(sigRing(t, keys...), maxSkew)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	return v
}

// signed returns headers signed by key over body, exactly as a producer
// holding key would sign them. The signer is built from a one-key ring bound to
// key.Source, so a test forging "a key of another source" passes a key whose
// Source is the record's while the verifier's ring binds the same id and
// secret elsewhere.
func signed(t testingTB, key envelopesig.Key, headers []kgo.RecordHeader, body []byte, opts ...envelopesig.Option) []kgo.RecordHeader {
	t.Helper()

	signer, err := envelopesig.NewSigner(sigRing(t, key), key.ID, key.Source, opts...)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	in := make([]transport.Header, len(headers))
	for i, h := range headers {
		in[i] = transport.Header{Key: h.Key, Value: h.Value}
	}

	out := signer.Sign(in, body)

	result := make([]kgo.RecordHeader, len(out))
	for i, h := range out {
		result[i] = kgo.RecordHeader{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}

	return result
}

// withHeader returns a copy of headers with key's value replaced.
func withHeader(headers []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(headers))
	copy(out, headers)

	for i := range out {
		if out[i].Key == key {
			out[i].Value = []byte(value)
		}
	}

	return out
}

// sigRecordBody is the value rec() stamps on every record.
var sigRecordBody = []byte(`{"ok":true}`)

// neverClassify is a Classifier that must never be consulted: a signature
// verdict is structural and quarantines outright.
func neverClassify(t *testing.T) Classifier {
	t.Helper()

	return func(err error) bool {
		t.Errorf("Classifier consulted for %v; a signature verdict must bypass it", err)

		return true
	}
}

// TestSignatureVerification_RunsAheadOfEveryHandlerMode pins the gate: with a
// verifier installed, a record that is unsigned, signed by a key the consumer
// does not hold, or tampered with after signing never reaches a handler, in
// either handler mode. It quarantines to the consumer's own DLQ with a
// signature cause kind, the service Classifier is never asked, and the offset
// commits only after the quarantine copy is durable.
func TestSignatureVerification_RunsAheadOfEveryHandlerMode(t *testing.T) {
	t.Parallel()

	trusted := sigKey("k-test", sigSource, 1)
	// The attacker's view of the lender key: same id and secret, but used to
	// sign a record claiming test-source. The consumer's ring binds that id to
	// lender, so the signature must not vouch for test-source.
	lenderKey := sigKey("k-lender", "lender", 9)
	lenderKeyMisused := envelopesig.Key{ID: lenderKey.ID, Source: sigSource, Secret: lenderKey.Secret}

	valid := func(t *testing.T) []kgo.RecordHeader {
		t.Helper()

		return signed(t, trusted, ceHeaders("tenantA", false), sigRecordBody)
	}

	tests := []struct {
		name     string
		headers  func(t *testing.T) []kgo.RecordHeader
		body     []byte
		wantKind string
		wantIs   error
	}{
		{
			name:     "unsigned record",
			headers:  func(*testing.T) []kgo.RecordHeader { return ceHeaders("tenantA", false) },
			wantKind: dlqCauseSignatureMissing,
			wantIs:   ErrSignatureMissing,
		},
		{
			name: "key id the consumer does not hold",
			headers: func(t *testing.T) []kgo.RecordHeader {
				return signed(t, sigKey("k-unknown", sigSource, 5), ceHeaders("tenantA", false), sigRecordBody)
			},
			wantKind: dlqCauseSignatureUnknownKey,
			wantIs:   ErrSignatureUnknownKey,
		},
		{
			name:     "body changed after signing",
			headers:  valid,
			body:     []byte(`{"ok":false}`),
			wantKind: dlqCauseSignatureInvalid,
			wantIs:   ErrSignatureInvalid,
		},
		{
			// ce-eventtype picks the handler; outside the signature it would
			// let a real payload be redirected to another handler.
			name: "ce-eventtype changed after signing",
			headers: func(t *testing.T) []kgo.RecordHeader {
				return withHeader(valid(t), "ce-eventtype", "reversed")
			},
			wantKind: dlqCauseSignatureInvalid,
			wantIs:   ErrSignatureInvalid,
		},
		{
			name: "ce-tenantid changed after signing",
			headers: func(t *testing.T) []kgo.RecordHeader {
				return withHeader(valid(t), "ce-tenantid", "tenantB")
			},
			wantKind: dlqCauseSignatureInvalid,
			wantIs:   ErrSignatureInvalid,
		},
		{
			name: "key bound to another source",
			headers: func(t *testing.T) []kgo.RecordHeader {
				return signed(t, lenderKeyMisused, ceHeaders("tenantA", false), sigRecordBody)
			},
			wantKind: dlqCauseSignatureInvalid,
			wantIs:   ErrSignatureInvalid,
		},
		{
			// The codec keeps the LAST ce-source; the verifier must not judge
			// a different value from the one the handler would get.
			name: "duplicated ce-source",
			headers: func(t *testing.T) []kgo.RecordHeader {
				return append(valid(t), kgo.RecordHeader{Key: "ce-source", Value: []byte("lender")})
			},
			wantKind: dlqCauseSignatureInvalid,
			wantIs:   ErrSignatureInvalid,
		},
	}

	modes := []struct {
		name    string
		handler func(t *testing.T) Handler
	}{
		{"whole-stream Handler", func(t *testing.T) Handler {
			t.Helper()

			return &fakeHandler{fn: func(context.Context, contract.Event, []byte) error {
				t.Error("handler ran for a record that failed signature verification")

				return nil
			}}
		}},
		{"per-event dispatch", func(t *testing.T) Handler {
			t.Helper()

			return NewDispatcher().On("loan.created", func(context.Context, contract.Event, []byte) error {
				t.Error("dispatched handler ran for a record that failed signature verification")

				return nil
			})
		}},
	}

	for _, mode := range modes {
		for _, tt := range tests {
			t.Run(mode.name+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				record := rec("t", 0, 4, tt.headers(t))
				if tt.body != nil {
					record.Value = tt.body
				}

				client := newFakeGroupClient(fetchOf("t", 0, record))
				dlq := &fakeDLQ{}

				r := newTestRuntime(t, client, mode.handler(t), dlq,
					WithSignatureVerifier(sigVerifier(t, 0, trusted, lenderKey)),
					WithClassifier(neverClassify(t)))

				runUntilClosed(t, r)

				if dlq.count() != 1 {
					t.Fatalf("DLQ count = %d; want 1", dlq.count())
				}

				cause, kind := dlq.lastCause()
				if kind != tt.wantKind {
					t.Errorf("cause kind = %q; want %q", kind, tt.wantKind)
				}

				if !errors.Is(cause, tt.wantIs) {
					t.Errorf("cause = %v; want it to wrap %v", cause, tt.wantIs)
				}

				if wm := client.committedWatermarks()[topicPartition{"t", 0}]; wm != 5 {
					t.Errorf("committed watermark = %d; want 5 (commit after the quarantine copy is durable)", wm)
				}
			})
		}
	}
}

// runSigned drives one record through a runtime with the given verifier and
// returns the handler and DLQ so a test can read where the record went.
func runSigned(t *testing.T, verifier *envelopesig.Verifier, records ...*kgo.Record) (*fakeHandler, *fakeDLQ) {
	t.Helper()

	handler := &fakeHandler{}
	dlq := &fakeDLQ{}

	var opts []Option
	if verifier != nil {
		opts = append(opts, WithSignatureVerifier(verifier))
	}

	r := newTestRuntime(t, newFakeGroupClient(fetchOf("t", 0, records...)), handler, dlq, opts...)
	runUntilClosed(t, r)

	return handler, dlq
}

// TestSignatureVerification_RotationOverlapAcceptsBothKeys pins rotation by
// overlap: while the ring holds the old and the new key, records signed by
// either one are dispatched.
func TestSignatureVerification_RotationOverlapAcceptsBothKeys(t *testing.T) {
	t.Parallel()

	oldKey, newKey := sigKey("k-2026-08", sigSource, 1), sigKey("k-2026-09", sigSource, 2)

	handler, dlq := runSigned(t, sigVerifier(t, 0, oldKey, newKey),
		rec("t", 0, 1, signed(t, oldKey, ceHeaders("tenantA", false), sigRecordBody)),
		rec("t", 0, 2, signed(t, newKey, ceHeaders("tenantA", false), sigRecordBody)),
	)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 while both keys are in the ring", dlq.count())
	}

	if handler.callCount() != 2 {
		t.Errorf("handler called %d times; want 2", handler.callCount())
	}
}

// TestSignatureVerification_RotatedOutKeyIsUnknown pins the end of the
// overlap: once the old key leaves the ring, its signatures are an unknown key
// id, which points at key distribution rather than forgery.
func TestSignatureVerification_RotatedOutKeyIsUnknown(t *testing.T) {
	t.Parallel()

	oldKey, newKey := sigKey("k-2026-08", sigSource, 1), sigKey("k-2026-09", sigSource, 2)

	handler, dlq := runSigned(t, sigVerifier(t, 0, newKey),
		rec("t", 0, 1, signed(t, oldKey, ceHeaders("tenantA", false), sigRecordBody)))

	if handler.callCount() != 0 {
		t.Errorf("handler called %d times; want 0", handler.callCount())
	}

	if _, kind := dlq.lastCause(); kind != dlqCauseSignatureUnknownKey {
		t.Errorf("cause kind = %q; want %q", kind, dlqCauseSignatureUnknownKey)
	}
}

// TestSignatureVerification_NewCeIDWithOldPayloadRejected is the br-sfn
// BRSFN-60 acceptance case: replaying a captured payload under a fresh ce-id,
// to slip past the consumer's ce-id idempotency, breaks the signature because
// ce-id is inside it.
func TestSignatureVerification_NewCeIDWithOldPayloadRejected(t *testing.T) {
	t.Parallel()

	key := sigKey("k-test", sigSource, 1)
	captured := signed(t, key, ceHeaders("tenantA", false), sigRecordBody)

	handler, dlq := runSigned(t, sigVerifier(t, 0, key),
		rec("t", 0, 1, withHeader(captured, "ce-id", "evt-2")))

	if handler.callCount() != 0 {
		t.Errorf("handler called %d times; want 0 for a replay under a new ce-id", handler.callCount())
	}

	cause, kind := dlq.lastCause()
	if kind != dlqCauseSignatureInvalid || !errors.Is(cause, ErrSignatureInvalid) {
		t.Errorf("cause = %v (kind %q); want ErrSignatureInvalid / %q", cause, kind, dlqCauseSignatureInvalid)
	}
}

// TestSignatureVerification_OldSignatureAcceptedByDefault pins that age is not
// a rejection reason by default: a consumer lagging a month behind its topic
// still dispatches, because backlog is not an attack.
func TestSignatureVerification_OldSignatureAcceptedByDefault(t *testing.T) {
	t.Parallel()

	key := sigKey("k-test", sigSource, 1)
	monthAgo := time.Now().Add(-30 * 24 * time.Hour)

	handler, dlq := runSigned(t, sigVerifier(t, 0, key),
		rec("t", 0, 1, signed(t, key, ceHeaders("tenantA", false), sigRecordBody,
			envelopesig.WithClock(func() time.Time { return monthAgo }))))

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 — a lagging consumer must not quarantine its backlog", dlq.count())
	}

	if handler.callCount() != 1 {
		t.Errorf("handler called %d times; want 1", handler.callCount())
	}
}

// TestSignatureVerification_MaxSkewRejectsWhenEnabled pins the opt-in age
// bound: a signature older than the configured skew quarantines as invalid.
func TestSignatureVerification_MaxSkewRejectsWhenEnabled(t *testing.T) {
	t.Parallel()

	key := sigKey("k-test", sigSource, 1)
	hourAgo := time.Now().Add(-time.Hour)

	handler, dlq := runSigned(t, sigVerifier(t, time.Minute, key),
		rec("t", 0, 1, signed(t, key, ceHeaders("tenantA", false), sigRecordBody,
			envelopesig.WithClock(func() time.Time { return hourAgo }))))

	if handler.callCount() != 0 {
		t.Errorf("handler called %d times; want 0", handler.callCount())
	}

	if _, kind := dlq.lastCause(); kind != dlqCauseSignatureInvalid {
		t.Errorf("cause kind = %q; want %q", kind, dlqCauseSignatureInvalid)
	}
}

// TestSignatureVerification_CheckedBeforeSource pins the gate order: a forged
// record claiming a source outside the allowlist is reported as a signature
// failure, the stronger finding, not as a source mismatch.
func TestSignatureVerification_CheckedBeforeSource(t *testing.T) {
	t.Parallel()

	lenderKey := sigKey("k-lender", "lender", 9)
	forged := envelopesig.Key{ID: lenderKey.ID, Source: "stranger", Secret: lenderKey.Secret}

	headers := signed(t, forged, withHeader(ceHeaders("tenantA", false), "ce-source", "stranger"), sigRecordBody)

	dlq := &fakeDLQ{}
	r := newTestRuntimeCfg(t,
		func(cfg *ConsumerConfig) { cfg.ExpectSources = []string{"lender"} },
		newFakeGroupClient(fetchOf("t", 0, rec("t", 0, 1, headers))), &fakeHandler{}, dlq,
		WithSignatureVerifier(sigVerifier(t, 0, lenderKey)))

	runUntilClosed(t, r)

	if _, kind := dlq.lastCause(); kind != dlqCauseSignatureInvalid {
		t.Errorf("cause kind = %q; want %q (signature runs before source)", kind, dlqCauseSignatureInvalid)
	}
}

// TestSignatureVerification_AbsentVerifierDispatchesUnsigned pins opt-in: a
// consumer without a verifier dispatches unsigned and signed records alike,
// exactly as before signing existed.
func TestSignatureVerification_AbsentVerifierDispatchesUnsigned(t *testing.T) {
	t.Parallel()

	key := sigKey("k-test", sigSource, 1)

	handler, dlq := runSigned(t, nil,
		rec("t", 0, 1, ceHeaders("tenantA", false)),
		rec("t", 0, 2, signed(t, key, ceHeaders("tenantA", false), sigRecordBody)),
	)

	if dlq.count() != 0 {
		t.Errorf("DLQ count = %d; want 0 without a verifier", dlq.count())
	}

	if handler.callCount() != 2 {
		t.Errorf("handler called %d times; want 2", handler.callCount())
	}
}

// TestSignatureVerification_DLQReaderRefusesVerifier pins that a DLQ reader
// never verifies. A quarantine copy keeps the ORIGINAL producer's signature,
// and every signature_* entry is by definition one that fails, so a verifying
// reader would quarantine its own queue back onto itself. New refuses the
// combination rather than quietly skipping the check.
func TestSignatureVerification_DLQReaderRefusesVerifier(t *testing.T) {
	t.Parallel()

	cfg := ConsumerConfig{
		Enabled:             true,
		Brokers:             []string{"localhost:9092"},
		Group:               "lender-dlq-desk",
		Source:              "lender-dlq-desk",
		Topics:              []string{"lerian.streaming.lender.dlq"},
		RetryBackoffInitial: time.Millisecond,
		RetryBackoffMax:     time.Millisecond,
		RetryInLoopMaxDwell: time.Millisecond,
		CloseTimeout:        time.Second,
	}

	_, err := New(cfg, newFakeGroupClient(), nil,
		WithDLQPublisher(&fakeDLQ{}),
		WithDiscardDispatch(func(context.Context, []kgo.RecordHeader, []byte) error { return nil }),
		WithSignatureVerifier(sigVerifier(t, 0, sigKey("k-lender", "lender", 9))))

	if !errors.Is(err, ErrDiscardHandlerAndHandlerBothSet) {
		t.Errorf("New err = %v; want ErrDiscardHandlerAndHandlerBothSet", err)
	}
}
