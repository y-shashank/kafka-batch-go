//go:build integration

package matrix

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/y-shashank/kafka-batch-go/integration/e2e"
	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

// ClientMode selects the produce tier for matrix scenarios.
type ClientMode string

const (
	ClientGo   ClientMode = "go"
	ClientRuby ClientMode = "ruby"
)

// ControlMode selects the control plane for matrix scenarios.
type ControlMode string

const (
	ControlGo   ControlMode = "go"
	ControlRuby ControlMode = "ruby"
)

// MatrixClient enqueues jobs/batches in matrix scenarios.
type MatrixClient interface {
	EnqueueJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error)
	CreateBatch(ctx context.Context, opts client.BatchOptions, fn func(MatrixBatch) error) (MatrixBatch, error)
	Close()
}

// MatrixBatch is the batch handle used in matrix scenarios.
type MatrixBatch interface {
	ID() string
	PushJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error)
	PushJobIn(ctx context.Context, delay time.Duration, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error)
}

// NewClient returns a matrix client for the given mode.
func NewClient(s *e2e.Stack, mode ClientMode) MatrixClient {
	switch mode {
	case ClientRuby:
		return &rubyMatrixClient{stack: s}
	default:
		return &goMatrixClient{inner: s.NewClient()}
	}
}

type goMatrixClient struct {
	inner *client.Client
}

func (c *goMatrixClient) EnqueueJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	return c.inner.EnqueueJob(ctx, jobType, payload, opts)
}

func (c *goMatrixClient) CreateBatch(ctx context.Context, opts client.BatchOptions, fn func(MatrixBatch) error) (MatrixBatch, error) {
	batch, err := c.inner.CreateBatch(ctx, opts, func(b *client.Batch) error {
		return fn(&goMatrixBatch{b: b})
	})
	if err != nil {
		return nil, err
	}
	return &goMatrixBatch{b: batch}, nil
}

func (c *goMatrixClient) Close() { c.inner.Close() }

type goMatrixBatch struct {
	b *client.Batch
}

func (b *goMatrixBatch) ID() string { return b.b.ID() }

func (b *goMatrixBatch) PushJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	return b.b.PushJob(ctx, jobType, payload, opts)
}

func (b *goMatrixBatch) PushJobIn(ctx context.Context, delay time.Duration, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	return b.b.PushJobIn(ctx, delay, jobType, payload, opts)
}

type rubyMatrixClient struct {
	stack *e2e.Stack
}

type rubyClientResult struct {
	BatchID string            `json:"batch_id"`
	JobIDs  map[string]string `json:"job_ids"`
}

func (c *rubyMatrixClient) EnqueueJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	_ = ctx
	_ = payload
	_ = opts
	res, err := c.runScript("enqueue", jobType, "")
	if err != nil {
		return "", err
	}
	if id, ok := res.JobIDs["primary"]; ok {
		return id, nil
	}
	return "", fmt.Errorf("ruby client enqueue: missing job id in %v", res)
}

func (c *rubyMatrixClient) CreateBatch(ctx context.Context, opts client.BatchOptions, fn func(MatrixBatch) error) (MatrixBatch, error) {
	_ = ctx
	_ = opts
	rb := &rubyMatrixBatch{client: c}
	if err := fn(rb); err != nil {
		return nil, err
	}
	if rb.mode == "" {
		return nil, fmt.Errorf("ruby client batch: mode not determined from PushJob calls")
	}
	res, err := c.runScript(rb.mode, "", "")
	if err != nil {
		return nil, err
	}
	rb.result = res
	return rb, nil
}

func (c *rubyMatrixClient) Close() {}

func (c *rubyMatrixClient) runScript(mode, jobType, outPath string) (*rubyClientResult, error) {
	if outPath == "" {
		outPath = filepath.Join(c.stack.TmpDir, "ruby_client_out_"+mode+".json")
	}
	cmd := rubyScriptCommand("ruby_client_ittest.rb", c.stack.ConfigPath, c.stack.ManifestPath,
		"--mode", mode, "--job-type", jobType)
	cmd.Env = append(os.Environ(),
		"REDIS_URL="+c.stack.Redis,
		"KBATCH_RUBY_GEM_ROOT="+e2e.KafkaBatchGemRoot(),
		"KBATCH_CLIENT_OUT="+outPath,
	)
	cmd.Dir = filepath.Join(e2e.RepoRoot(), "compat", "ruby")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("ruby client %s: %w\n%s", mode, err, string(out))
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		raw = out
	}
	var res rubyClientResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("ruby client parse: %w\n%s", err, string(raw))
	}
	return &res, nil
}

type rubyMatrixBatch struct {
	client *rubyMatrixClient
	mode   string
	result *rubyClientResult
}

func (b *rubyMatrixBatch) ID() string {
	if b.result == nil {
		return ""
	}
	return b.result.BatchID
}

func (b *rubyMatrixBatch) PushJob(ctx context.Context, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	_ = ctx
	_ = payload
	_ = opts
	b.mode = rubyBatchMode(b.mode, jobType)
	return "", nil
}

func (b *rubyMatrixBatch) PushJobIn(ctx context.Context, delay time.Duration, jobType string, payload map[string]interface{}, opts client.PushOptions) (string, error) {
	_ = ctx
	_ = delay
	_ = payload
	_ = opts
	b.mode = "scheduled-go"
	return "", nil
}

func rubyBatchMode(current, jobType string) string {
	switch jobType {
	case "integration.ruby_plain", "integration.ruby_retry_once":
		if current == "batch-go" {
			return "batch-mixed"
		}
		if current == "batch-mixed" {
			return "batch-mixed"
		}
		return "batch-ruby"
	case "integration.go_multi":
		if current == "batch-ruby" {
			return "batch-mixed"
		}
		if current == "batch-mixed" {
			return "batch-mixed"
		}
		return "batch-go"
	case "integration.go_retry_once":
		return "batch-retry-go"
	case "integration.go_always_fail":
		return "batch-fail"
	default:
		if current == "batch-ruby" {
			return "batch-mixed"
		}
		return "batch-go"
	}
}

func rubyScriptCommand(script, configPath, manifestPath string, extraArgs ...string) *exec.Cmd {
	compat := filepath.Join(e2e.RepoRoot(), "compat", "ruby")
	args := append([]string{filepath.Join(compat, "bin", script), "--config", configPath, "--manifest", manifestPath}, extraArgs...)
	if _, err := os.Stat(filepath.Join(compat, "Gemfile.lock")); err == nil {
		cmd := exec.Command("bundle", append([]string{"exec", "ruby"}, args...)...)
		cmd.Dir = compat
		return cmd
	}
	cmd := exec.Command("ruby", args...)
	cmd.Dir = compat
	return cmd
}

// JobIDFromResult returns a job id captured by a Ruby batch script.
func JobIDFromResult(res *rubyClientResult, key string) string {
	if res == nil || res.JobIDs == nil {
		return ""
	}
	return res.JobIDs[key]
}

// BatchJobIDsAfterCreate extracts job ids after Ruby CreateBatch completes.
func BatchJobIDsAfterCreate(b MatrixBatch) map[string]string {
	rb, ok := b.(*rubyMatrixBatch)
	if !ok || rb.result == nil {
		return nil
	}
	return rb.result.JobIDs
}
