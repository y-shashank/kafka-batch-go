package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/y-shashank/kafka-batch-go/pkg/topics"
)

func (c *Client) topicPlanner() topics.ClientTopics {
	rf := int16(c.cfg.TopicsReplicationFactor)
	if rf < 1 {
		rf = 1
	}
	return topics.ClientTopics{
		Brokers:                  c.cfg.Brokers,
		TopicPrefix:              c.cfg.TopicPrefix,
		JobsTopic:                c.cfg.JobsTopic,
		ScheduledTopic:           c.cfg.ScheduledTopic,
		CallbacksTopic:           c.cfg.CallbacksTopic,
		EventsTopic:              c.cfg.EventsTopic,
		DeadLetterTopic:          c.cfg.DeadLetterTopic,
		FairnessTimeIngest:       c.cfg.FairnessTimeIngest,
		FairnessThroughputIngest: c.cfg.FairnessThroughputIngest,
		ReplicationFactor:        rf,
		ForcePartitions:          c.cfg.TopicsForcePartitions,
		IncludeControlPlane:      c.cfg.TopicsIncludeControlPlane,
		MaxScheduleHorizon:       c.cfg.MaxScheduleHorizon,
		Manifest:                 c.manifest,
		ExtraTopics:              c.cfg.TopicsExtra,
	}
}

// TopicSpecs returns Kafka topics required for this client's produce surface.
func (c *Client) TopicSpecs() []topics.Spec {
	return topics.Specs(c.topicPlanner())
}

// EnsureTopics creates any missing topics (Ruby rake kafka_batch:create_topics).
func (c *Client) EnsureTopics(ctx context.Context) (topics.Result, error) {
	return topics.CreateAll(ctx, c.cfg.Brokers, c.TopicSpecs())
}

// ValidateTopics returns an error when required topics are missing.
func (c *Client) ValidateTopics(ctx context.Context) error {
	missing, err := topics.Missing(ctx, c.cfg.Brokers, c.TopicSpecs())
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing kafka topics: %s", strings.Join(missing, ", "))
	}
	return nil
}
