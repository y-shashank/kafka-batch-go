package fairness

// Lane identifies a fairness lane (:time or :throughput in Ruby).
type Lane string

const (
	LaneTime        Lane = "time"
	LaneThroughput  Lane = "throughput"
)

func (l Lane) String() string { return string(l) }
