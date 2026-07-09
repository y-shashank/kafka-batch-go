package config

import "strings"

// Runtime constants for handler manifest entries.
const (
	RuntimeGo   = "go"
	RuntimeRuby = "ruby"
)

// FairnessReadyTopics holds per-runtime ready topic names for one fairness lane.
type FairnessReadyTopics struct {
	Go     string
	Ruby   string
	Legacy string
}

// RuntimeSplitFairReady reports whether go/ruby ready topics are configured for a lane.
func (c Daemon) RuntimeSplitFairReady(lane string) bool {
	switch strings.ToLower(lane) {
	case "throughput":
		return c.FairnessThroughputReadyGo != "" && c.FairnessThroughputReadyRuby != ""
	case "time", "":
		return c.FairnessTimeReadyGo != "" && c.FairnessTimeReadyRuby != ""
	default:
		return false
	}
}

// FairReadyForRuntime resolves the ready topic for a handler runtime on a lane.
func (c Daemon) FairReadyForRuntime(lane, runtime string) string {
	topics := c.FairReadyTopics(lane)
	if c.RuntimeSplitFairReady(lane) {
		switch strings.ToLower(strings.TrimSpace(runtime)) {
		case RuntimeGo:
			return topics.Go
		case RuntimeRuby:
			return topics.Ruby
		}
	}
	if topics.Legacy != "" {
		return topics.Legacy
	}
	if strings.EqualFold(runtime, RuntimeRuby) && topics.Ruby != "" {
		return topics.Ruby
	}
	return topics.Go
}

// FairReadyTopics returns ready topic configuration for a fairness lane.
func (c Daemon) FairReadyTopics(lane string) FairnessReadyTopics {
	switch strings.ToLower(lane) {
	case "throughput":
		return FairnessReadyTopics{
			Go:     c.FairnessThroughputReadyGo,
			Ruby:   c.FairnessThroughputReadyRuby,
			Legacy: c.FairnessThroughputReady,
		}
	default:
		return FairnessReadyTopics{
			Go:     c.FairnessTimeReadyGo,
			Ruby:   c.FairnessTimeReadyRuby,
			Legacy: c.FairnessTimeReady,
		}
	}
}
