//go:build integration

package e2e

// DaemonYAML is the daemon config document written for integration stacks.
type DaemonYAML = daemonYAML

// HandlerYAML is one handler entry in the integration manifest.
type HandlerYAML = handlerYAML

// BaseHandlersStack returns the default integration handler manifest.
func BaseHandlersStack(s *Stack) map[string]HandlerYAML {
	return baseHandlers(s.WorkerTopic)
}

// ApplyFairConfig enables fairness topics on a daemon config.
func ApplyFairConfig(s *Stack, cfg *DaemonYAML) {
	applyFairConfig(s, cfg)
}

// ApplyScheduleConfig enables the schedule poller on a daemon config.
func ApplyScheduleConfig(s *Stack, cfg *DaemonYAML) {
	applyScheduleConfig(s, cfg)
}

// ApplyPriorityConfig enables priority routing on a daemon config.
func ApplyPriorityConfig(s *Stack, cfg *DaemonYAML) {
	applyPriorityConfig(s, cfg)
}

// PriorityHandlersStack returns handlers with P0/P1 topics configured.
func PriorityHandlersStack(s *Stack) map[string]HandlerYAML {
	return priorityHandlersForStack(s)
}

// KafkaBatchGemRoot returns the path to the kafka-batch Ruby gem for itest workers.
func KafkaBatchGemRoot() string {
	return kafkaBatchGemRoot()
}