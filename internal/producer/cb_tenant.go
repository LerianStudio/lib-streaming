package producer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/LerianStudio/lib-commons/v5/commons/circuitbreaker"
	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"

	"github.com/LerianStudio/lib-streaming/internal/contract"
)

const circuitBreakerTenantPrefix = "streaming-"

type circuitBreakerScope struct {
	tenantID     string
	tenantScoped bool
}

func circuitBreakerScopeForEvent(event Event) circuitBreakerScope {
	tenantID, ok := circuitBreakerTenantID(event)

	return circuitBreakerScope{tenantID: tenantID, tenantScoped: ok}
}

func circuitBreakerTenantID(event Event) (string, bool) {
	if event.SystemEvent || event.TenantID == "" {
		return "", false
	}

	if tmcore.IsValidTenantID(event.TenantID) {
		return event.TenantID, true
	}

	sum := sha256.Sum256([]byte(event.TenantID))

	return circuitBreakerTenantPrefix + hex.EncodeToString(sum[:]), true
}

func (p *Producer) isRouteCircuitOpen(rt *targetRuntime, scope circuitBreakerScope) bool {
	if rt == nil {
		return false
	}

	if scope.tenantScoped && !isNilInterface(p.tenantCBManager) {
		return p.tenantCBManager.GetStateForTenant(scope.tenantID, rt.cbServiceName) == circuitbreaker.StateOpen
	}

	return rt.state.Load() == flagCBOpen
}

func (p *Producer) executeWithCircuitBreaker(
	ctx context.Context,
	rt *targetRuntime,
	scope circuitBreakerScope,
	fn func() (any, error),
) error {
	if rt == nil {
		return fmt.Errorf("%w: target runtime is nil", contract.ErrNilProducer)
	}

	if scope.tenantScoped && !isNilInterface(p.tenantCBManager) {
		cb, err := p.tenantCBManager.GetOrCreateForTenant(scope.tenantID, rt.cbServiceName, p.cbConfig)
		if err != nil {
			return fmt.Errorf("streaming: register tenant circuit breaker %q: %w", rt.cbServiceName, err)
		}

		if isNilInterface(cb) {
			return fmt.Errorf("%w: tenant circuit breaker %q is nil", contract.ErrNilProducer, rt.cbServiceName)
		}

		p.tenantCBKeys.Store(circuitbreaker.TenantBreakerKey{TenantID: scope.tenantID, ServiceName: rt.cbServiceName}, struct{}{})

		_, err = p.tenantCBManager.ExecuteForTenant(ctx, scope.tenantID, rt.cbServiceName, fn)

		return err
	}

	if isNilInterface(p.cbManager) {
		return fmt.Errorf("%w: circuit breaker manager is nil", contract.ErrNilProducer)
	}

	if isNilInterface(rt.cb) {
		return fmt.Errorf("%w: circuit breaker %q is nil", contract.ErrNilProducer, rt.cbServiceName)
	}

	_, err := p.cbManager.Execute(rt.cbServiceName, fn)

	return err
}
