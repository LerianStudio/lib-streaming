package contract

// routeMergeIdentity is the composite key that governs override precedence:
// a route is uniquely identified for merge purposes by its (DefinitionKey,
// Target) pair, NOT by DefinitionKey alone. One DefinitionKey legitimately
// maps to multiple routes across different Targets — the runtime fans out
// every such route (see RouteTable.Routes / producer emitMulti), each
// publishing to its own target — so precedence must be resolved per target
// to avoid dropping sibling-target routes.
type routeMergeIdentity struct {
	definitionKey string
	target        string
}

// MergeRouteOverrides merges caller-supplied override routes into a base route
// set and is the SINGLE source of truth for override precedence, shared by the
// Builder path (streaming.Builder.RouteOverrides) and the single-target
// auto-generate path (producer.NewProducer via WithRouteOverrides).
//
// Precedence is resolved by the (DefinitionKey, Target) pair:
//
//   - An override REPLACES the base route sharing its (DefinitionKey, Target).
//     Sibling-target base routes for the same DefinitionKey but a DIFFERENT
//     Target are PRESERVED — an override for target "primary" does not drop
//     the definition's "secondary" route.
//   - An override whose (DefinitionKey, Target) is absent from the base set is
//     appended.
//   - Overrides that collide with each other on (DefinitionKey, Target) are
//     deduplicated before appending, LAST override wins (the later entry in the
//     overrides slice supersedes the earlier). Surviving overrides keep their
//     first-seen relative order.
//
// Precedence is resolved HERE, by (DefinitionKey, Target) — deliberately NOT
// deferred to NewRouteTable (which dedups only by route Key) nor to the runtime
// (which fans out every route registered for a DefinitionKey). Leaving two
// routes for the same (DefinitionKey, Target) would otherwise double-publish to
// that target. Whether each resulting route's DefinitionKey exists in the
// catalog is enforced later, in the producer's validateRoutesAgainstTargets.
//
// Neither input is mutated: the returned slice shares no backing array with
// base or overrides, so the caller may mutate it freely. (When both inputs are
// empty the result is nil, which callers treat as the empty route set.)
func MergeRouteOverrides(base, overrides []RouteDefinition) []RouteDefinition {
	if len(overrides) == 0 {
		return append([]RouteDefinition(nil), base...)
	}

	// Dedupe overrides among themselves by (DefinitionKey, Target), last-wins.
	// order preserves the first-seen position of each surviving identity so the
	// merged output is deterministic; byIdentity holds the last-wins value.
	order := make([]routeMergeIdentity, 0, len(overrides))
	byIdentity := make(map[routeMergeIdentity]RouteDefinition, len(overrides))

	for _, o := range overrides {
		id := routeMergeIdentity{definitionKey: o.DefinitionKey, target: o.Target}
		if _, seen := byIdentity[id]; !seen {
			order = append(order, id)
		}

		byIdentity[id] = o
	}

	dedupedOverrides := make([]RouteDefinition, 0, len(order))
	for _, id := range order {
		dedupedOverrides = append(dedupedOverrides, byIdentity[id])
	}

	merged := make([]RouteDefinition, 0, len(base)+len(dedupedOverrides))

	for _, route := range base {
		id := routeMergeIdentity{definitionKey: route.DefinitionKey, target: route.Target}
		if _, replaced := byIdentity[id]; replaced {
			continue
		}

		merged = append(merged, route)
	}

	return append(merged, dedupedOverrides...)
}
