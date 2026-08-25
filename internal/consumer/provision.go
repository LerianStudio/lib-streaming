package consumer

import (
	"slices"

	"github.com/LerianStudio/lib-streaming/v3/internal/contract"
)

// ownedTopics returns the topics THIS consumer is entitled to create, derived
// entirely from what the service already declared.
//
// The ownership rule, and the reason for each half:
//
//   - Its own DLQ, ALWAYS. The consumer is that topic's producer — it is the only
//     thing that writes there — and every enabled consumer has a quarantine path.
//     A terminal record with nowhere to quarantine is silent data loss.
//
//   - Its own commands queue, when it is the app being commanded (its ce-source
//     appears in Commands). That queue carries work addressed to THIS service, so
//     this service owns the name. Missing, it is the worst shape in the platform:
//     franz-go surfaces no topic-specific fetch error for a nonexistent
//     subscription, so the consumer polls clean and reports healthy while
//     money-path commands go undelivered.
//
// And what is deliberately EXCLUDED:
//
//   - Another application's fact topic (Apps) or commands queue (Commands naming
//     someone else). Those belong to their producers, which provision them on
//     their own Build. Creating one here would reach outside this application's
//     Kafka grant and, worse, would turn a typo'd subscription into a
//     permanently-empty topic that looks perfectly healthy.
//
//   - Everything in the raw Topics escape hatch — including a string that merely
//     SPELLS this app's own commands queue. Topics exists for streams this
//     library did not derive; it has no ownership knowledge, so provisioning from
//     it would create on a guess. Naming the app in Commands is how a consumer
//     opts in, exactly as it is for the strict-unmatched verdict (CommandTopics).
//
// An empty Source yields nothing rather than a garbage "lerian.streaming..dlq":
// cfg.Validate rejects that before Build reaches here, and this stays honest if
// it is ever called first.
func ownedTopics(cfg ConsumerConfig) []string {
	if cfg.Source == "" {
		return nil
	}

	topics := []string{contract.AppDLQTopic(cfg.Source)}

	if slices.Contains(cfg.Commands, cfg.Source) {
		topics = append(topics, contract.AppCommandsTopic(cfg.Source))
	}

	return topics
}
