package schedule

import (
	"context"
	"fmt"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/fairness"
)

// Route describes where a scheduled job should be re-produced.
type Route struct {
	Topic     string
	Key       string
	Partition *int32 // nil = broker assigns by key
}

// DaemonRouter routes scheduled jobs (mirrors Batch.route_for_definition).
type DaemonRouter struct {
	Manifest config.Manifest
	Cfg      config.Daemon
	Default  string
	Tenants  *fairness.TenantPartitions
}

func (r DaemonRouter) Route(payload map[string]interface{}) (Route, error) {
	jobType, _ := payload["job_type"].(string)
	jobID, _ := payload["job_id"].(string)
	tenantID, _ := payload["tenant_id"].(string)
	batchID, _ := payload["batch_id"].(string)
	worker, _ := payload["worker_class"].(string)

	var entry config.HandlerEntry
	var ok bool
	if jobType != "" {
		entry, ok = r.Manifest.Handlers[jobType]
	}
	if !ok && worker != "" {
		for jt, h := range r.Manifest.Handlers {
			if h.Runtime == "go" && ("go:"+jt) == worker {
				entry = h
				ok = true
				jobType = jt
				break
			}
		}
	}

	if ok && entry.FairnessType != "" {
		return r.fairRoute(entry.FairnessType, jobID, tenantID, batchID)
	}

	if ok && entry.Topic != "" {
		return Route{Topic: entry.Topic, Key: jobID}, nil
	}
	if r.Default != "" {
		return Route{Topic: r.Default, Key: jobID}, nil
	}
	return Route{}, fmt.Errorf("no route for job_type=%q worker_class=%q", jobType, worker)
}

func (r DaemonRouter) fairRoute(fairnessType, jobID, tenantID, batchID string) (Route, error) {
	key := tenantID
	if key == "" {
		key = batchID
	}
	if key == "" {
		key = jobID
	}
	var topic string
	switch fairnessType {
	case "time":
		topic = r.Cfg.FairnessTimeIngest
	case "throughput":
		topic = r.Cfg.FairnessThroughputIngest
	default:
		return Route{}, fmt.Errorf("unknown fairness_type %q", fairnessType)
	}
	route := Route{Topic: topic, Key: key}
	if tenantID != "" {
		if part, ok := r.Cfg.FairnessTenantPartitions[tenantID]; ok {
			p := part
			route.Partition = &p
			route.Key = ""
			return route, nil
		}
		if r.Tenants != nil {
			if p := r.Tenants.Resolve(context.Background(), tenantID, fairnessType); p != nil {
				route.Partition = p
				route.Key = ""
			}
		}
	}
	return route, nil
}
