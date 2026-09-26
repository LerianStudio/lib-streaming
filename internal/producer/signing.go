package producer

import (
	"context"
	"fmt"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/envelopesig"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport"
)

// newSigner turns the WithEnvelopeSigning input into the Producer's signer. No
// input means no signing (nil signer). A nil ring is refused here by name; the
// rest — an unknown active key id, a key bound to another source — is refused
// by envelopesig.NewSigner. Every failure wraps contract.ErrInvalidSigningKey.
func newSigner(spec *signingSpec, source string) (*envelopesig.Signer, error) {
	if spec == nil {
		return nil, nil //nolint:nilnil // a nil *Signer is the documented "signing not configured" state; Sign on it returns the headers unchanged
	}

	if spec.ring == nil {
		return nil, fmt.Errorf("%w: envelope signing configured without a keyring", contract.ErrInvalidSigningKey)
	}

	return envelopesig.NewSigner(spec.ring, spec.activeKeyID, source)
}

// publishHeaders builds the headers of one publication of event: the
// CloudEvents headers plus trace propagation, then — when signing is
// configured — the signature over them and event.Payload, taken now. Every
// producer publish path calls it at publish time, so a relayed or
// dead-lettered record carries a signature from the instant it was written,
// never one inherited from an earlier attempt.
//
// It refuses, with contract.ErrInvalidSigningKey, an event whose source is not
// the one the signing key is bound to. An Emit always carries the producer's
// own source; an outbox row carries the source it was persisted under, which
// may be an earlier or another one, and publishing it signed would only hand
// every verifying consumer a record it quarantines as a forgery.
func (p *Producer) publishHeaders(ctx context.Context, event Event) ([]transport.Header, error) {
	if err := p.signer.CheckSource(event.Source); err != nil {
		return nil, err
	}

	return p.signer.Sign(buildTransportHeaders(ctx, event), event.Payload), nil
}
