package contract

// MergeRouteOverrides merges caller-supplied override routes into a base route
// set and is the SINGLE source of truth for override precedence, shared by the
// Builder path (streaming.Builder.RouteOverrides) and the single-target
// auto-generate path (producer.NewProducer via WithRouteOverrides).
//
// An override REPLACES any base route that shares its DefinitionKey; an
// override whose DefinitionKey is absent from the base set is appended.
//
// Precedence is resolved HERE, by DefinitionKey — deliberately NOT deferred to
// NewRouteTable (which dedups only by route Key) nor to the runtime (which fans
// out every route registered for a DefinitionKey). Leaving both a base and an
// override route for the same DefinitionKey would otherwise double-publish.
// Whether each resulting route's DefinitionKey exists in the catalog is
// enforced later, in the producer's validateRoutesAgainstTargets.
//
// Neither input is mutated: the returned slice shares no backing array with
// base or overrides, so the caller may mutate it freely. (When both inputs are
// empty the result is nil, which callers treat as the empty route set.)
func MergeRouteOverrides(base, overrides []RouteDefinition) []RouteDefinition {
	if len(overrides) == 0 {
		return append([]RouteDefinition(nil), base...)
	}

	overridden := make(map[string]struct{}, len(overrides))
	for _, o := range overrides {
		overridden[o.DefinitionKey] = struct{}{}
	}

	merged := make([]RouteDefinition, 0, len(base)+len(overrides))

	for _, route := range base {
		if _, replaced := overridden[route.DefinitionKey]; replaced {
			continue
		}

		merged = append(merged, route)
	}

	return append(merged, overrides...)
}
