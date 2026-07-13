//go:build integration

package e2e

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/consumption"
)

// DaemonYAML is the daemon config document written for integration stacks.
type DaemonYAML = daemonYAML

// HandlerYAML is one handler entry in the integration manifest.
type HandlerYAML = handlerYAML

// BaseHandlersStack returns the default integration handler manifest.
func BaseHandlersStack(s *Stack) map[string]HandlerYAML {
	return baseHandlers(s.WorkerTopic)
}

// ApplyFairConfig enables fairness topics on a daemon config.
func ApplyFairConfig(s *Stack, cfg *DaemonYAML) {
	applyFairConfig(s, cfg)
}

// ApplyScheduleConfig enables the schedule poller on a daemon config.
func ApplyScheduleConfig(s *Stack, cfg *DaemonYAML) {
	applyScheduleConfig(s, cfg)
}

// ApplyPriorityConfig enables priority routing on a daemon config.
func ApplyPriorityConfig(s *Stack, cfg *DaemonYAML) {
	applyPriorityConfig(s, cfg)
}

// PriorityHandlersStack returns handlers with P0/P1 topics configured.
func PriorityHandlersStack(s *Stack) map[string]HandlerYAML {
	return priorityHandlersForStack(s)
}

// KafkaBatchGemRoot returns the path to the kafka-batch Ruby gem for itest workers.
func KafkaBatchGemRoot() string {
	return kafkaBatchGemRoot()
}

// RubyItestAvailable reports whether the Ruby client/worker itest harness can run
// (gem present, bundle installed, ruby on PATH). Tests that drive the Ruby client
// use this to skip cleanly when the Ruby toolchain is unavailable locally.
func RubyItestAvailable() bool {
	return rubyItestAvailable()
}

// GoWorkerJobsGroup returns the Go worker's plain-jobs consumer group name for
// this stack's config (used by the consumption-pause parity test).
func (s *Stack) GoWorkerJobsGroup() string {
	s.T.Helper()
	cfg, err := config.LoadDaemon(s.ConfigPath)
	if err != nil {
		s.T.Fatal(err)
	}
	return cfg.GoWorkerJobsGroup()
}

// SetTopicPaused pauses/resumes a (consumer group, topic) via the shared
// consumption-control Redis set — the exact key/format Ruby's
// KafkaBatch::ConsumptionControl writes. Used to verify a Go consumer honors a
// pause written by the "other" runtime.
func (s *Stack) SetTopicPaused(group, topic string, paused bool) {
	s.T.Helper()
	ctl := consumption.NewControl(s.rdb, time.Second)
	ctx := context.Background()
	var err error
	if paused {
		err = ctl.PauseTopic(ctx, group, topic)
	} else {
		err = ctl.ResumeTopic(ctx, group, topic)
	}
	if err != nil {
		s.T.Fatal(err)
	}
}

// ApplyFastConsumptionRefresh shortens the pause-control refresh so tests see a
// pause/resume take effect within ~1s instead of the 30s default.
func ApplyFastConsumptionRefresh(s *Stack, cfg *daemonYAML) {
	cfg.ConsumptionRefreshInterval = 1
}

// MySQLFailuresDSN returns the integration MySQL DSN when configured.
func MySQLFailuresDSN() string {
	return mysqlFailuresDSN()
}

// PrepareMySQLFailuresTable creates kafka_batch_failures for failure-store tests.
func PrepareMySQLFailuresTable(conn string) error {
	return prepareMySQLFailures(conn)
}

// TruncateMySQLFailuresTable clears kafka_batch_failures between tests.
func TruncateMySQLFailuresTable(conn string) error {
	return truncateMySQLFailures(conn)
}

// CreateTopicPartitions creates a topic with the given partition count for a stack
// (used by cross-runtime partitioning tests that need more than one partition).
func (s *Stack) CreateTopicPartitions(topic string, partitions int32) {
	s.T.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Brokers...))
	if err != nil {
		s.T.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	_, err = adm.CreateTopic(context.Background(), partitions, 1, nil, topic)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "exist") {
		s.T.Fatalf("create topic %s: %v", topic, err)
	}
}
