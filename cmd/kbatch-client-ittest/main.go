// kbatch-client-ittest exercises the Go produce client against a live stack.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
)

func main() {
	mode := flag.String("mode", "enqueue", "enqueue | batch-many | scheduled-many")
	jobType := flag.String("job-type", "integration.go_daemon", "manifest job_type")
	flag.Parse()

	brokers := strings.Split(envOr("KBATCH_CLIENT_BROKERS", "localhost:9092"), ",")
	redisURL := envOr("KBATCH_CLIENT_REDIS", "redis://localhost:6379/0")
	manifest := os.Getenv("KBATCH_CLIENT_MANIFEST")
	marker := os.Getenv("KBATCH_CLIENT_MARKER")
	outFile := os.Getenv("KBATCH_CLIENT_OUT")

	cfg := client.DefaultConfig()
	cfg.Brokers = brokers
	cfg.RedisURL = redisURL
	cfg.ManifestPath = manifest
	cfg.UniqEnabled = false

	c, err := client.New(cfg)
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	switch *mode {
	case "enqueue":
		jobID, err := c.EnqueueJob(ctx, *jobType, map[string]interface{}{"n": 1}, client.PushOptions{})
		if err != nil {
			fatal(err)
		}
		writeOut(outFile, jobID)
		if marker != "" {
			_ = os.WriteFile(marker, []byte(jobID), 0o644)
		}
	case "batch-many":
		var batchID string
		_, err := c.CreateBatch(ctx, client.BatchOptions{Description: "client-ittest"}, func(b *client.Batch) error {
			batchID = b.ID()
			ids, err := b.PushManyJobs(ctx, *jobType, []map[string]interface{}{
				{"n": 1}, {"n": 2},
			}, client.PushOptions{})
			if err != nil {
				return err
			}
			writeOut(outFile, strings.Join(ids, ","))
			return nil
		})
		if err != nil {
			fatal(err)
		}
		if marker != "" {
			_ = os.WriteFile(marker, []byte(batchID), 0o644)
		}
	case "scheduled-many":
		runAt := time.Now().Add(2 * time.Second)
		var batchID string
		_, err := c.CreateBatch(ctx, client.BatchOptions{Description: "client-sched-ittest"}, func(b *client.Batch) error {
			batchID = b.ID()
			got, err := b.PushManyJobsAt(ctx, runAt, *jobType, []map[string]interface{}{
				{"n": 1},
			}, client.PushOptions{})
			if err != nil {
				return err
			}
			writeOut(outFile, strings.Join(got, ","))
			return nil
		})
		if err != nil {
			fatal(err)
		}
		if marker != "" {
			_ = os.WriteFile(marker, []byte(batchID), 0o644)
		}
	default:
		fatal(fmt.Errorf("unknown mode %q", *mode))
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeOut(path, data string) {
	if path == "" {
		return
	}
	_ = os.WriteFile(path, []byte(data), 0o644)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "kbatch-client-ittest: %v\n", err)
	os.Exit(1)
}
