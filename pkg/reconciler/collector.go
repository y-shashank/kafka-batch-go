package reconciler

import (
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

const maxDetails = 25

// Summary is persisted to Redis for the dashboard.
type Summary struct {
	RanAt          string
	TriggeredBy    string
	Duration       float64
	FoundStale     int
	ProcessedStale int
	FoundLost      int
	ProcessedLost  int
	CappedStale    bool
	CappedLost     bool
	RecoveredStale int
	RefiredLost    int
	SkippedStale   int
	SkippedRecent  int
	ProduceFailed  int
	Details        []Detail
}

// Detail is one reconciler action row.
type Detail struct {
	BatchID     string `json:"batch_id"`
	Action      string `json:"action"`
	Outcome     string `json:"outcome,omitempty"`
	TotalJobs   int64  `json:"total_jobs,omitempty"`
	FailedCount int64  `json:"failed_count,omitempty"`
}

// Collector tracks per-run outcomes.
type Collector struct {
	triggeredBy  string
	foundStale   int
	foundLost    int
	stale        []*store.Batch
	lost         []*store.Batch
	details      []Detail
	recovered    int
	refired      int
	skippedStale int
	skippedRecent int
	produceFail  int
	cappedStale  bool
	cappedLost   bool
}

// NewCollector starts a reconciler run collector.
func NewCollector(triggeredBy string) *Collector {
	return &Collector{triggeredBy: triggeredBy}
}

// Identify records how many batches were found vs processed.
func (c *Collector) Identify(staleAll int, stale []*store.Batch, lostAll int, lost []*store.Batch) {
	c.foundStale = staleAll
	c.foundLost = lostAll
	c.stale = stale
	c.lost = lost
	c.cappedStale = staleAll > len(stale)
	c.cappedLost = lostAll > len(lost)
}

// RecordStale records one stuck-running outcome.
func (c *Collector) RecordStale(batchID string, outcome staleOutcome, batch *store.Batch) {
	switch outcome {
	case outcomeRecoveredRunning, outcomeRecoveredEmpty:
		c.recovered++
	case outcomeSkippedOpen, outcomeSkippedInProgress:
		c.skippedStale++
	case outcomeProduceFailed:
		c.produceFail++
	}
	c.addDetail(batchID, string(outcome), batch)
}

// RecordLostSkippedRecently records a lost-callback batch skipped due to a recent refire.
func (c *Collector) RecordLostSkippedRecently(batchID string, batch *store.Batch) {
	c.skippedRecent++
	c.addDetail(batchID, "skipped_recent_refire", batch)
}

// RecordLost records one lost-callback outcome.
func (c *Collector) RecordLost(batchID string, outcome lostOutcome, batch *store.Batch) {
	switch outcome {
	case outcomeRefiredLost:
		c.refired++
	case outcomeLostProduceFailed:
		c.produceFail++
	}
	c.addDetail(batchID, string(outcome), batch)
}

func (c *Collector) addDetail(batchID, action string, batch *store.Batch) {
	if len(c.details) >= maxDetails {
		return
	}
	row := Detail{BatchID: batchID, Action: action}
	if batch != nil {
		row.Outcome = batch.Status
		row.TotalJobs = batch.TotalJobs
		row.FailedCount = batch.FailedCount
	}
	c.details = append(c.details, row)
}

// Finish builds the run summary.
func (c *Collector) Finish(duration time.Duration) Summary {
	return Summary{
		RanAt:          time.Now().UTC().Format(time.RFC3339),
		TriggeredBy:    c.triggeredBy,
		Duration:       duration.Seconds(),
		FoundStale:     c.foundStale,
		ProcessedStale: len(c.stale),
		FoundLost:      c.foundLost,
		ProcessedLost:  len(c.lost),
		CappedStale:    c.cappedStale,
		CappedLost:     c.cappedLost,
		RecoveredStale: c.recovered,
		RefiredLost:    c.refired,
		SkippedStale:   c.skippedStale,
		SkippedRecent:  c.skippedRecent,
		ProduceFailed:  c.produceFail,
		Details:        c.details,
	}
}
