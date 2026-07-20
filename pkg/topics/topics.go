package topics

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

// Spec is one Kafka topic to provision.
type Spec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
	Category          string
}

// Result summarizes topic creation.
type Result struct {
	Created []string
	Skipped []string
	Failed  []Failure
}

// Failure is one topic that could not be created.
type Failure struct {
	Name  string
	Error string
}

// DefaultPartitions mirrors lib/kafka_batch/topics.rb. Every category defaults to
// 16 partitions except the fairness ingest and ready lanes, which default to 64
// (higher tenant/pod fan-out). Replication factor defaults to 1 (see Specs).
var DefaultPartitions = map[string]int32{
	"jobs":        16,
	"priority":    16,
	"events":      16,
	"callbacks":   16,
	"retry":       16,
	"scheduled":   16,
	"dead_letter": 16,
	"ingest":      64,
	"ready":       64,
}

// ClientTopics is the produce/client configuration used to derive topic specs.
type ClientTopics struct {
	Brokers                 []string
	TopicPrefix             string
	JobsTopic               string
	ScheduledTopic          string
	CallbacksTopic          string
	EventsTopic             string
	DeadLetterTopic         string
	FairnessTimeIngest      string
	FairnessThroughputIngest string
	ReplicationFactor       int16
	ForcePartitions         int32 // 0 = use per-category defaults
	IncludeControlPlane     bool
	MaxScheduleHorizon      time.Duration
	Manifest                config.Manifest
	ExtraTopics             []string
}

// Specs derives the topic set for a Go produce client (and optional control plane).
func Specs(ct ClientTopics) []Spec {
	rf := ct.ReplicationFactor
	if rf < 1 {
		rf = 1
	}
	seen := map[string]struct{}{}
	var out []Spec
	add := func(name, category string, cfg map[string]string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		parts := DefaultPartitions[category]
		if ct.ForcePartitions > 0 {
			parts = ct.ForcePartitions
		}
		if parts < 1 {
			parts = DefaultPartitions["jobs"]
		}
		out = append(out, Spec{
			Name:              name,
			Partitions:        parts,
			ReplicationFactor: rf,
			Config:            cfg,
			Category:          category,
		})
	}

	resolve := func(base string) string {
		if ct.TopicPrefix == "" || base == "" {
			return base
		}
		prefix := ct.TopicPrefix + "."
		if strings.HasPrefix(base, prefix) {
			return base
		}
		return prefix + base
	}

	lanes := map[string]struct{}{}
	for _, h := range ct.Manifest.Handlers {
		if h.FairnessType != "" {
			lanes[h.FairnessType] = struct{}{}
		}
		if h.Topic != "" && h.FairnessType == "" {
			add(h.Topic, "jobs", nil)
		}
	}
	for lane := range lanes {
		switch lane {
		case "time":
			add(resolve(ct.FairnessTimeIngest), "ingest", nil)
		case "throughput":
			add(resolve(ct.FairnessThroughputIngest), "ingest", nil)
		}
	}

	add(resolve(ct.JobsTopic), "jobs", nil)
	add(resolve(ct.ScheduledTopic), "scheduled", scheduledConfig(ct.MaxScheduleHorizon))
	add(resolve(ct.CallbacksTopic), "callbacks", nil)
	for _, t := range ct.ExtraTopics {
		add(t, "jobs", nil)
	}

	if ct.IncludeControlPlane {
		events := ct.EventsTopic
		if events == "" {
			events = "kafka_batch.events"
		}
		dlt := ct.DeadLetterTopic
		if dlt == "" {
			dlt = "kafka_batch.dead_letter"
		}
		add(resolve(events), "events", nil)
		add(resolve(dlt), "dead_letter", map[string]string{
			"retention.ms":   fmt.Sprintf("%d", 30*24*3600*1000),
			"cleanup.policy": "delete",
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func scheduledConfig(horizon time.Duration) map[string]string {
	sec := int(horizon.Seconds())
	if sec < 1 {
		sec = 30 * 24 * 3600
	}
	retention := (sec + 86400) * 1000
	min := 7 * 24 * 3600 * 1000
	if retention < min {
		retention = min
	}
	return map[string]string{
		"retention.ms":   fmt.Sprintf("%d", retention),
		"cleanup.policy": "delete",
	}
}

// Existing returns topic names present on the cluster.
func Existing(ctx context.Context, brokers []string) (map[string]struct{}, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	meta, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(meta))
	for name := range meta {
		out[name] = struct{}{}
	}
	return out, nil
}

// Missing returns topic names from specs that do not exist.
func Missing(ctx context.Context, brokers []string, specs []Spec) ([]string, error) {
	existing, err := Existing(ctx, brokers)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, s := range specs {
		if _, ok := existing[s.Name]; !ok {
			missing = append(missing, s.Name)
		}
	}
	return missing, nil
}

// CreateAll creates missing topics idempotently (Ruby KafkaBatch::Topics.create_all!).
func CreateAll(ctx context.Context, brokers []string, specs []Spec) (Result, error) {
	result := Result{}
	existing, err := Existing(ctx, brokers)
	if err != nil {
		return result, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return result, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for _, spec := range specs {
		if _, ok := existing[spec.Name]; ok {
			result.Skipped = append(result.Skipped, spec.Name)
			continue
		}
		cfg := map[string]*string{}
		for k, v := range spec.Config {
			val := v
			cfg[k] = &val
		}
		_, err := adm.CreateTopic(ctx, spec.Partitions, spec.ReplicationFactor, cfg, spec.Name)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "exist") {
				result.Skipped = append(result.Skipped, spec.Name)
				continue
			}
			result.Failed = append(result.Failed, Failure{Name: spec.Name, Error: err.Error()})
			continue
		}
		result.Created = append(result.Created, spec.Name)
	}
	return result, nil
}
