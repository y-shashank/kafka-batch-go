//go:build integration

package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
	"github.com/y-shashank/kafka-batch-go/pkg/uniq"
)

// TestMatrix_UniqDedupCrossRuntime verifies that a uniq job enqueued from one
// runtime deduplicates against the same job enqueued from the other. Both
// clients resolve job_type "integration.uniq_shared" to worker_class
// "RubyUniqWorker", so they must compute an identical uniqueness fingerprint —
// and therefore claim the SAME Redis lock (kafka_batch:uniq:<digest>).
//
// The payload deliberately contains '<', '>', '&' and non-ASCII. Before the
// encoding/json HTML-escape fix in pkg/uniq, Go serialized those characters as
// </>/& while Ruby's Oj :compat emitted them verbatim, so the two
// runtimes produced different fingerprints — the second enqueue would silently
// NOT dedupe and both jobs would run. This test is the end-to-end guard for that
// cross-runtime contract, over live Kafka + Redis.
func TestMatrix_UniqDedupCrossRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-runtime uniqueness requires live Kafka + Ruby")
	}
	if !e2e.RubyItestAvailable() {
		t.Skip("Ruby client unavailable (compat/ruby bundle install && kafka-batch gem)")
	}

	s := e2e.NewStack(t, e2e.BaseHandlersStack, nil)
	s.StartWithOptions(e2e.StackStartOptions{Control: e2e.ControlGo, Exec: e2e.ExecMode{Go: true}})
	defer s.Stop()

	ctx := context.Background()
	payloadJSON := `{"html":"<a>&</a>","name":"café","n":1}`
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatal(err)
	}

	// 1. Ruby client claims the shared lock first (also produces the wire message).
	rubyRes := runRubyUniq(t, s, payloadJSON)
	rubyJobID := rubyRes.JobIDs["primary"]
	if rubyJobID == "" || rubyRes.Skipped {
		t.Fatalf("ruby enqueue should have claimed the lock, got %+v", rubyRes)
	}

	// 2. Go client enqueues the identical payload → must hit Ruby's lock and skip.
	goC := s.NewClientOptions(true)
	defer goC.Close()
	goID, err := goC.EnqueueJob(ctx, "integration.uniq_shared", payload, client.PushOptions{})
	if !errors.Is(err, client.ErrJobSkipped) {
		t.Fatalf("go enqueue err = %v (job %q); want ErrJobSkipped — cross-runtime fingerprints diverged", err, goID)
	}

	// 3. The _uniq_fp Ruby wrote on the wire must equal Go's fingerprint for the
	//    same worker_class + payload, and only Ruby's single message should exist.
	wantFP := uniq.DigestHex("RubyUniqWorker", payload)
	msg := s.PollTopic(ctx, s.WorkerTopic+".ruby", func(m map[string]interface{}) bool {
		return m["job_id"] == rubyJobID
	}, 30*time.Second)
	if fp, _ := msg["_uniq_fp"].(string); fp != wantFP {
		t.Fatalf("wire _uniq_fp = %q want %q (Go/Ruby fingerprint mismatch)", fp, wantFP)
	}
}

type rubyUniqResult struct {
	JobIDs  map[string]string `json:"job_ids"`
	Skipped bool              `json:"skipped"`
}

func runRubyUniq(t *testing.T, s *e2e.Stack, payloadJSON string) rubyUniqResult {
	t.Helper()
	outPath := filepath.Join(s.TmpDir, "ruby_uniq_out.json")
	cmd := rubyScriptCommand("ruby_client_ittest.rb", s.ConfigPath, s.ManifestPath, "--mode", "enqueue-uniq")
	cmd.Env = append(os.Environ(),
		"REDIS_URL="+s.Redis,
		"KBATCH_RUBY_GEM_ROOT="+e2e.KafkaBatchGemRoot(),
		"KBATCH_CLIENT_OUT="+outPath,
		"KBATCH_UNIQ_PAYLOAD="+payloadJSON,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ruby enqueue-uniq: %v\n%s", err, string(out))
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		raw = out
	}
	var res rubyUniqResult
	if err := json.Unmarshal(raw, &res); err != nil {
		t.Fatalf("parse ruby uniq result: %v\n%s", err, string(raw))
	}
	return res
}
