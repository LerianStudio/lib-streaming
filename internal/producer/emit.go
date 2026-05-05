package producer

import (
	"context"
)

// Emit publishes a single EmitRequest through the multi-target dispatch
// runtime. Resolves the request through the catalog, performs caller-side
// validation, applies defaults on a local copy so the caller's struct is
// untouched, then fans out one publish attempt per registered route.
//
// Nil-receiver safe: returns ErrNilProducer rather than panicking.
// Nil-ctx safe: falls back to context.Background if ctx is nil.
func (p *Producer) Emit(ctx context.Context, request EmitRequest) error {
	if p == nil {
		return ErrNilProducer
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return p.emitMulti(ctx, request)
}
