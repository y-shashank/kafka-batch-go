//go:build integration

package matrix

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// TestMatrix_ScheduledJobRubyClientGoExec covers a real hybrid: a Ruby client
// schedules a delayed job (writing the schedule-index pointer to Redis + the
// payload to the scheduled topic), the Go daemon's schedule poller picks it up
// when due and re-produces it, and a Go worker executes it. This exercises
// cross-runtime parity of the schedule index (Redis ZSET key + the compact
// job_id:partition:offset pointer) and the scheduled-topic envelope — neither is
// covered by the existing GoClientOnly scheduled scenario.
func TestMatrix_ScheduledJobRubyClientGoExec(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-runtime scheduling requires live Kafka + Ruby")
	}
	if !e2e.RubyItestAvailable() {
		t.Skip("Ruby client unavailable (compat/ruby bundle install && kafka-batch gem)")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, e2e.ApplyScheduleConfig)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Go: true}})
	defer s.Stop()

	res := runRubyClientMode(t, s, "scheduled-go")
	goID := res.JobIDs["go"]
	if res.BatchID == "" || goID == "" {
		t.Fatalf("ruby scheduled-go returned %+v", res)
	}

	ctx := context.Background()
	s.WaitBatchTimeout(ctx, 60*time.Second, res.BatchID, "success")
	if m := s.WaitMarkerAt(s.MarkerPath, 60*time.Second); m != goID {
		t.Fatalf("go marker = %q want %q (Ruby-scheduled job not executed by Go)", m, goID)
	}
}

// TestMatrix_CancellationCrossRuntime verifies that a batch cancelled by the Go
// client is honored by a Ruby JobConsumer: the Ruby worker must skip the job
// rather than run it. Both runtimes coordinate through the shared
// "kafka_batch:index:cancelled" Redis ZSET, so a cancel written by Go is visible
// to Ruby's CancellationCache. Without that parity, cancelling from one runtime
// would silently fail to stop in-flight work in the other.
func TestMatrix_CancellationCrossRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-runtime cancellation requires live Kafka + Ruby")
	}
	if !e2e.RubyItestAvailable() {
		t.Skip("Ruby exec unavailable (compat/ruby bundle install && kafka-batch gem)")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Ruby: true}})
	defer s.Stop()

	ctx := context.Background()
	c := s.NewClient()
	defer c.Close()

	// Push a Ruby job, then cancel the batch before the Ruby executor drains it.
	batch, err := c.CreateBatch(ctx, client.BatchOptions{Description: "cross-cancel"}, func(b *client.Batch) error {
		_, err := b.PushJob(ctx, "integration.ruby_plain", map[string]interface{}{"order_id": 99}, client.PushOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	s.WaitBatchCancelled(ctx, batch.ID(), 15*time.Second)

	// Now let the Ruby executor consume the topic — it must skip the cancelled job.
	s.DrainRubyExecution(30 * time.Second)
	s.AssertNoMarkerAt(s.RubyMarkerPath, 5*time.Second)
}

// runRubyClientMode runs the Ruby produce client in the given mode and parses the
// standard {batch_id, job_ids} JSON result.
func runRubyClientMode(t *testing.T, s *e2e.Stack, mode string) rubyClientResult {
	t.Helper()
	outPath := filepath.Join(s.TmpDir, "ruby_client_out_"+mode+".json")
	cmd := rubyScriptCommand("ruby_client_ittest.rb", s.ConfigPath, s.ManifestPath, "--mode", mode)
	cmd.Env = append(os.Environ(),
		"REDIS_URL="+s.Redis,
		"KBATCH_RUBY_GEM_ROOT="+e2e.KafkaBatchGemRoot(),
		"KBATCH_CLIENT_OUT="+outPath,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ruby client %s: %v\n%s", mode, err, string(out))
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		raw = out
	}
	var res rubyClientResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse ruby client %s result: %v\n%s", mode, err, string(raw))
	}
	return res
}
