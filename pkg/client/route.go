package client

import (
	"context"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

// Route is a Kafka produce destination.
type Route struct {
	Topic     string
	Key       string
	Partition *int32
}

func (c *Client) routeFor(entry config.HandlerEntry, jobID, tenantID string, batchID *string) Route {
	if lane := entry.FairnessType; lane != "" {
		key := tenantID
		if key == "" && batchID != nil {
			key = *batchID
		}
		if key == "" {
			key = jobID
		}
		var topic string
		switch lane {
		case "time":
			topic = c.cfg.resolveTopic(c.cfg.FairnessTimeIngest)
		case "throughput":
			topic = c.cfg.resolveTopic(c.cfg.FairnessThroughputIngest)
		default:
			topic = c.cfg.defaultJobsTopic()
		}
		if tenantID != "" {
			if part, ok := c.cfg.FairnessTenantPartitions[tenantID]; ok {
				p := part
				return Route{Topic: topic, Partition: &p}
			}
			if c.tenants != nil {
				if p := c.tenants.Resolve(context.Background(), tenantID, lane); p != nil {
					return Route{Topic: topic, Partition: p}
				}
			}
		}
		return Route{Topic: topic, Key: key}
	}
	topic := entry.Topic
	if topic == "" {
		topic = c.cfg.defaultJobsTopic()
	}
	return Route{Topic: topic, Key: jobID}
}

// ResolveRoute exposes routing for tests.
func (c *Client) ResolveRoute(jobType, jobID, tenantID string, batchID *string) (Route, error) {
	entry, err := c.lookupHandler(jobType)
	if err != nil {
		return Route{}, err
	}
	return c.routeFor(entry, jobID, tenantID, batchID), nil
}

// Manifest returns the loaded handler manifest.
func (c *Client) Manifest() config.Manifest {
	return c.manifest
}
