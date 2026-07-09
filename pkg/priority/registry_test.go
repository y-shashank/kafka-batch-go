package priority

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func testdata(t *testing.T, name string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Join(filepath.Dir(file), "..", "config", "testdata", name)
}

func TestLoadRegistryFromYAML(t *testing.T) {
	cfg := config.DefaultDaemon()
	cfg.TopicPrefix = "acme"
	path := testdata(t, "priority.yaml")
	reg, err := LoadRegistry([]string{path}, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(reg.Configs) != 1 {
		t.Fatalf("configs=%d", len(reg.Configs))
	}
	pc := reg.Configs[0]
	if pc.ConsumerGroupSuffix != "jobs-fast" || pc.Mode != ModeWeighted {
		t.Fatalf("pc=%+v", pc)
	}
	if len(pc.Topics) != 2 || pc.Topics[0] != "acme.jobs.p0" {
		t.Fatalf("topics=%v", pc.Topics)
	}
	all := reg.AllTopics()
	if len(all) != 2 {
		t.Fatalf("all=%v", all)
	}
}

func TestLoadRegistryRejectsFlatTopicOverlap(t *testing.T) {
	cfg := config.DefaultDaemon()
	path := testdata(t, "priority.yaml")
	_, err := LoadRegistry([]string{path}, cfg, []string{"acme.jobs.p0"})
	if err == nil {
		t.Fatal("expected overlap error")
	}
}

func TestWithConsumerGroup(t *testing.T) {
	pc := Config{ConsumerGroup: "old", Topics: []string{"t"}}
	out := pc.WithConsumerGroup("new-group")
	if out.ConsumerGroup != "new-group" || pc.ConsumerGroup != "old" {
		t.Fatalf("copy failed: out=%+v orig=%+v", out, pc)
	}
}

func TestTopicSpecsRankAndHigherTopics(t *testing.T) {
	pc := Config{
		ConsumerGroup: "kb-jobs-fast",
		Mode:          ModeStrict,
		Topics:        []string{"p0", "p1", "p2"},
	}
	specs := pc.TopicSpecs()
	if len(specs) != 3 {
		t.Fatalf("specs=%d", len(specs))
	}
	if specs[2].Rank != 2 || len(specs[2].HigherTopics) != 2 {
		t.Fatalf("p2 spec=%+v", specs[2])
	}
}
