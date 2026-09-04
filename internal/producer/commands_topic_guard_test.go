//go:build unit

package producer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/LerianStudio/lib-observability/v4/log"

	"github.com/LerianStudio/lib-streaming/v4/internal/contract"
	"github.com/LerianStudio/lib-streaming/v4/internal/transport/fake"
)

// commandFactOnlyCatalog holds no command at all, so a route pointing it at
// the commands queue is the mirror-image mistake: facts landing on the stream
// whose consumer quarantines every key it has no handler for.
func commandFactOnlyCatalog(t *testing.T) Catalog {
	t.Helper()

	catalog, err := NewCatalog(EventDefinition{
		Key:          "loan.disbursed",
		ResourceType: "loan_contract",
		EventType:    "disbursed",
	})
	if err != nil {
		t.Fatalf("NewCatalog() error = %v", err)
	}

	return catalog
}

// TestNewProducerMulti_RejectsRouteNamingTheCommandsQueue pins the
// construction-time refusal of any Kafka route that names the application's
// commands queue by hand.
//
// Two holes close here, and neither is visible at emit time:
//
//   - A COMMAND routed there by name never reaches commandRoute's rewrite (the
//     rewrite only moves a destination that equals the app topic), so its DLQ
//     is derived as "<app>.commands.dlq" — a fourth topic nothing provisions
//     and no ACL grants. The quarantine copy of a failed command publish
//     silently never exists.
//   - A FACT routed there lands on the strict queue, where an unmatched key is
//     quarantined instead of skipped. The fact stream's whole premise is that a
//     consumer may ignore most of it.
//
// The commands queue is reachable exactly one way: Class: ClassCommand on the
// event definition. Naming it in a route is always the wrong instrument.
func TestNewProducerMulti_RejectsRouteNamingTheCommandsQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		catalog func(*testing.T) Catalog
		route   contract.RouteDefinition
	}{
		{
			name:    "command definition with an explicit commands-topic destination",
			catalog: commandMixedCatalog,
			route: contract.RouteDefinition{
				Key:    "primary.kafka",
				Target: "primary",
				Destination: contract.Destination{
					Kind: TransportKafkaLike,
					Name: commandTestCommandsTopic,
				},
				Requirement: contract.RouteRequired,
			},
		},
		{
			name:    "fact-only definition with an explicit commands-topic destination",
			catalog: commandFactOnlyCatalog,
			route: contract.RouteDefinition{
				Key:    "primary.kafka",
				Target: "primary",
				Destination: contract.Destination{
					Kind: TransportKafkaLike,
					Name: commandTestCommandsTopic,
				},
				Requirement: contract.RouteRequired,
			},
		},
		{
			name:    "explicit DLQ pointed at the commands queue",
			catalog: commandMixedCatalog,
			route: contract.RouteDefinition{
				Key:    "primary.kafka",
				Target: "primary",
				Destination: contract.Destination{
					Kind: TransportKafkaLike,
					Name: commandTestAppTopic,
				},
				DLQ: &contract.Destination{
					Kind: TransportKafkaLike,
					Name: commandTestCommandsTopic,
				},
				Requirement: contract.RouteRequired,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			routes, err := contract.NewRouteTable(tt.route)
			if err != nil {
				t.Fatalf("NewRouteTable() error = %v", err)
			}

			catalog := tt.catalog(t)

			p, err := NewProducerMulti(
				context.Background(),
				MultiProducerConfig{Source: commandTestSource},
				nil,
				[]TargetSpec{{Name: "primary", Kind: TransportKafkaLike, Adapter: fake.NewAdapter(TransportKafkaLike)}},
				routes,
				catalog,
				WithLogger(log.NewNop()),
				WithCatalog(catalog),
			)
			if p != nil {
				t.Cleanup(func() { _ = p.Close() })
			}

			if !errors.Is(err, contract.ErrInvalidRouteDefinition) {
				t.Fatalf("NewProducerMulti() error = %v; want ErrInvalidRouteDefinition", err)
			}

			if !strings.Contains(err.Error(), "ClassCommand") {
				t.Errorf("error %q does not tell the caller to use Class: ClassCommand instead", err)
			}

			if !strings.Contains(err.Error(), commandTestCommandsTopic) {
				t.Errorf("error %q does not name the offending destination", err)
			}
		})
	}
}
