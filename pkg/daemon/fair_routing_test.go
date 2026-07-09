package daemon

import (
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestFairReadyResolver(t *testing.T) {
	manifest := config.Manifest{
		Handlers: map[string]config.HandlerEntry{
			"job.go": {Runtime: "go"},
		},
	}
	cfg := config.DefaultDaemon()
	cfg.FairnessTimeReadyGo = "ready.go"
	cfg.FairnessTimeReadyRuby = "ready.ruby"

	resolve := fairReadyResolver(manifest, cfg, "time")
	topic, err := resolve([]byte(`{"job_type":"job.go"}`))
	if err != nil {
		t.Fatal(err)
	}
	if topic != "ready.go" {
		t.Fatalf("topic = %q", topic)
	}
}
