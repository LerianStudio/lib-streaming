package producer

// Circuit-breaker state-flag values. Stored as int32 so the atomic reads in
// the publish hot path are lock-free. Named constants keep the call sites
// readable and the values stable.
const (
	// flagCBClosed marks the circuit as CLOSED — publish runs normally.
	flagCBClosed int32 = 0
	// flagCBHalfOpen marks the circuit as HALF-OPEN — the breaker allows a
	// limited number of probe calls through.
	flagCBHalfOpen int32 = 1
	// flagCBOpen marks the circuit as OPEN — publish is short-circuited.
	flagCBOpen int32 = 2
)

// cbServiceNamePrefix is the per-instance service name prefix registered with
// the circuit-breaker manager. Per-target full name follows the pattern
// "streaming.producer:<producerID>:target:<targetName>" (see
// targetCBServiceName).
const cbServiceNamePrefix = "streaming.producer:"
