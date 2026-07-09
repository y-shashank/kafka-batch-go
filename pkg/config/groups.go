package config

import "fmt"

// GoWorkerJobsGroup is the plain go job consumer group (kbatch worker).
func (c Daemon) GoWorkerJobsGroup() string {
	return c.ConsumerGroup + "-go-worker-jobs"
}

// GoWorkerPriorityGroup is the priority consumer group for Go execution.
func (c Daemon) GoWorkerPriorityGroup(suffix string) string {
	return fmt.Sprintf("%s-go-worker-%s", c.ConsumerGroup, suffix)
}

// DispatchConsumerGroup is the fair ingest dispatcher group (Ruby-compatible).
func (c Daemon) DispatchConsumerGroup(lane string) string {
	return fmt.Sprintf("%s-dispatch-%s", c.ConsumerGroup, lane)
}

// JobsFairConsumerGroup is the fair ready consumer group for Ruby execution.
func (c Daemon) JobsFairConsumerGroup(lane string) string {
	return fmt.Sprintf("%s-jobs-fair-%s", c.ConsumerGroup, lane)
}

// GoWorkerFairReadyGroup is the fair ready consumer group for Go execution.
func (c Daemon) GoWorkerFairReadyGroup(lane string) string {
	return fmt.Sprintf("%s-go-worker-fair-ready-%s", c.ConsumerGroup, lane)
}
