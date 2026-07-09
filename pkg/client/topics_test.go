package client

import (
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestTopicSpecsIncludesManifestTopics(t *testing.T) {
	c := &Client{
		cfg: DefaultConfig(),
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"go.job": {Topic: "jobs.go", Runtime: "go"},
		}},
	}
	specs := c.TopicSpecs()
	found := false
	for _, s := range specs {
		if s.Name == "jobs.go" {
			found = true
		}
	}
	if !found {
		t.Fatalf("specs %v", specs)
	}
}
