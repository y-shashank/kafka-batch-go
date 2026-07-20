package config

import "strings"

// Runtime constants for handler manifest entries.
const (
	RuntimeGo   = "go"
	RuntimeRuby = "ruby"
)

// FairnessReadyTopics holds per-runtime ready topic names for one fairness lane.
// Fair ready topics are ALWAYS runtime-split (.go / .ruby); there is no combined
// (non-suffixed) fallback topic.
type FairnessReadyTopics struct {
	Go   string
	Ruby string
}

// RuntimeSplitFairReady reports whether both go and ruby ready topics are
// configured for a lane. Ready topics are always runtime-split, so this is the
// well-configured invariant (both non-empty), not an opt-in toggle.
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

// FairReadyForRuntime resolves the runtime-specific (.go / .ruby) ready topic for
// a handler runtime on a lane. Returns "" for an unknown runtime; the caller
// (fairReadyResolver) turns that into an explicit routing error.
func (c Daemon) FairReadyForRuntime(lane, runtime string) string {
	topics := c.FairReadyTopics(lane)
	switch strings.ToLower(strings.TrimSpace(runtime)) {
	case RuntimeGo:
		return topics.Go
	case RuntimeRuby:
		return topics.Ruby
	default:
		return ""
	}
}

// FairReadyTopics returns the per-runtime ready topic configuration for a lane.
func (c Daemon) FairReadyTopics(lane string) FairnessReadyTopics {
	switch strings.ToLower(lane) {
	case "throughput":
		return FairnessReadyTopics{
			Go:   c.FairnessThroughputReadyGo,
			Ruby: c.FairnessThroughputReadyRuby,
		}
	default:
		return FairnessReadyTopics{
			Go:   c.FairnessTimeReadyGo,
			Ruby: c.FairnessTimeReadyRuby,
		}
	}
}
