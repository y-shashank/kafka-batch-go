//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"github.com/y-shashank/kafka-batch-go/pkg/client"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
)

// Stack runs daemon + worker against live Kafka and Redis for E2E tests.
type Stack struct {
	T       *testing.T
	Suffix  string
	Brokers []string
	Redis   string
	TmpDir  string

	WorkerTopic   string
	EventsTopic   string
	CallbacksTopic string
	DLTTopic      string
	ScheduledTopic string
	RetryBase     string

	TimeIngest     string
	TimeReadyGo    string
	TimeReadyRuby  string
	TpIngest       string
	TpReadyGo      string
	TpReadyRuby    string

	P0Topic string
	P1Topic string

	ManifestPath string
	ConfigPath   string
	MarkerPath   string
	P0MarkerPath string

	daemonPID *exec.Cmd
	workerPID *exec.Cmd
	rdb       *redis.Client
}

func NewStack(t *testing.T, handlersFn func(*Stack) map[string]handlerYAML, extra func(*Stack, *daemonYAML)) *Stack {
	t.Helper()
	skipUnlessIntegration(t)

	brokers := brokersFromEnv()
	if !kafkaReachable(brokers) {
		t.Skip("Kafka broker not reachable")
	}
	redisURL := redisFromEnv()
	if !redisReachable(redisURL) {
		t.Skip("Redis not reachable")
	}

	suffix := strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	tmp := t.TempDir()
	s := &Stack{
		T:       t,
		Suffix:  suffix,
		Brokers: brokers,
		Redis:   redisURL,
		TmpDir:  tmp,

		WorkerTopic:    "kb.e2e.worker." + suffix,
		EventsTopic:    "kb.e2e.events." + suffix,
		CallbacksTopic: "kb.e2e.callbacks." + suffix,
		DLTTopic:       "kb.e2e.dlt." + suffix,
		ScheduledTopic: "kb.e2e.scheduled." + suffix,
		RetryBase:      "kb.e2e.retry." + suffix,

		TimeIngest:    "kb.e2e.fair.time.ingest." + suffix,
		TimeReadyGo:   "kb.e2e.fair.time.ready.go." + suffix,
		TimeReadyRuby: "kb.e2e.fair.time.ready.ruby." + suffix,
		TpIngest:      "kb.e2e.fair.tp.ingest." + suffix,
		TpReadyGo:     "kb.e2e.fair.tp.ready.go." + suffix,
		TpReadyRuby:   "kb.e2e.fair.tp.ready.ruby." + suffix,

		P0Topic: "kb.e2e.p0." + suffix,
		P1Topic: "kb.e2e.p1." + suffix,

		MarkerPath:   filepath.Join(tmp, "marker"),
		P0MarkerPath: filepath.Join(tmp, "marker_p0"),
	}
	s.writeManifest(handlersFn(s))
	s.writeConfig(extra)
	s.createTopics(t)
	s.flushRedis()
	return s
}

type handlerYAML struct {
	Runtime              string `yaml:"runtime"`
	Topic                string `yaml:"topic,omitempty"`
	ApplyTopicPrefix     bool   `yaml:"apply_topic_prefix,omitempty"`
	MaxRetries           int    `yaml:"max_retries,omitempty"`
	CompleteAfterRetries int    `yaml:"complete_after_retries,omitempty"`
	FairnessType         string `yaml:"fairness_type,omitempty"`
	WorkerClass          string `yaml:"worker_class,omitempty"`
}

type manifestDoc struct {
	Handlers map[string]handlerYAML `yaml:"handlers"`
}

type daemonYAML struct {
	Brokers              []string          `yaml:"brokers"`
	ConsumerGroup        string            `yaml:"consumer_group"`
	JobsTopics           []string          `yaml:"jobs_topics,omitempty"`
	EventsTopic          string            `yaml:"events_topic"`
	CallbacksTopic       string            `yaml:"callbacks_topic"`
	DeadLetterTopic      string            `yaml:"dead_letter_topic"`
	ScheduledTopic       string            `yaml:"scheduled_topic,omitempty"`
	RetryTopic           string            `yaml:"retry_topic"`
	RedisURL             string            `yaml:"redis_url"`
	HandlerManifest      string            `yaml:"handler_manifest"`
	MaxRetries           int               `yaml:"max_retries"`
	CompleteAfterRetries int               `yaml:"complete_after_retries"`
	RetryTiers           map[string]int    `yaml:"retry_tiers"`
	SchedulePollerEnabled bool             `yaml:"schedule_poller_enabled,omitempty"`
	FairnessEnabled      bool              `yaml:"fairness_enabled,omitempty"`
	FairnessTimeIngest   string            `yaml:"fairness_time_ingest,omitempty"`
	FairnessTimeReadyGo  string            `yaml:"fairness_time_ready_go,omitempty"`
	FairnessTimeReadyRuby string           `yaml:"fairness_time_ready_ruby,omitempty"`
	FairnessThroughputIngest string        `yaml:"fairness_throughput_ingest,omitempty"`
	FairnessThroughputReadyGo string       `yaml:"fairness_throughput_ready_go,omitempty"`
	FairnessThroughputReadyRuby string     `yaml:"fairness_throughput_ready_ruby,omitempty"`
	PriorityConfigPaths  []string          `yaml:"priority_config_paths,omitempty"`
	PriorityLagCheckInterval float64       `yaml:"priority_lag_check_interval,omitempty"`
}

func (s *Stack) writeManifest(handlers map[string]handlerYAML) {
	s.ManifestPath = filepath.Join(s.TmpDir, "handlers.yml")
	raw, err := yaml.Marshal(manifestDoc{Handlers: handlers})
	if err != nil {
		s.T.Fatal(err)
	}
	if err := os.WriteFile(s.ManifestPath, raw, 0o644); err != nil {
		s.T.Fatal(err)
	}
}

func (s *Stack) writeConfig(extra func(*Stack, *daemonYAML)) {
	s.ConfigPath = filepath.Join(s.TmpDir, "daemon.yml")
	cfg := daemonYAML{
		Brokers:              s.Brokers,
		ConsumerGroup:        "kb-e2e-" + s.Suffix,
		JobsTopics:           []string{s.WorkerTopic},
		EventsTopic:          s.EventsTopic,
		CallbacksTopic:       s.CallbacksTopic,
		DeadLetterTopic:      s.DLTTopic,
		ScheduledTopic:       s.ScheduledTopic,
		RetryTopic:           s.RetryBase,
		RedisURL:             s.Redis,
		HandlerManifest:      s.ManifestPath,
		MaxRetries:           2,
		CompleteAfterRetries: 1,
		RetryTiers:           map[string]int{"short": 0, "medium": 0, "large": 0},
	}
	if extra != nil {
		extra(s, &cfg)
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		s.T.Fatal(err)
	}
	if err := os.WriteFile(s.ConfigPath, raw, 0o644); err != nil {
		s.T.Fatal(err)
	}
}

func (s *Stack) createTopics(t *testing.T) {
	t.Helper()
	names := []string{
		s.WorkerTopic, s.WorkerTopic + ".ruby", s.EventsTopic, s.CallbacksTopic, s.DLTTopic, s.ScheduledTopic,
		s.RetryBase + ".short", s.RetryBase + ".medium", s.RetryBase + ".large",
		s.TimeIngest, s.TimeReadyGo, s.TimeReadyRuby,
		s.TpIngest, s.TpReadyGo, s.TpReadyRuby,
		s.P0Topic, s.P1Topic,
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, name := range names {
		_, err := adm.CreateTopic(context.Background(), 1, 1, nil, name)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "exist") {
			t.Fatalf("create topic %s: %v", name, err)
		}
	}
}

func (s *Stack) flushRedis() {
	opts, err := redis.ParseURL(s.Redis)
	if err != nil {
		s.T.Fatal(err)
	}
	s.rdb = redis.NewClient(opts)
	if err := s.rdb.FlushDB(context.Background()).Err(); err != nil {
		s.T.Fatal(err)
	}
}

func (s *Stack) Start() {
	s.T.Helper()
	readyDaemon := filepath.Join(s.TmpDir, "daemon_ready")
	readyWorker := filepath.Join(s.TmpDir, "worker_ready")

	env := os.Environ()
	env = append(env,
		"KBATCH_DAEMON_ITEST_MARKER="+s.MarkerPath,
		"KBATCH_DAEMON_ITEST_MARKER_P0="+s.P0MarkerPath,
		"KBATCH_DAEMON_ITEST_MARKER_TP="+s.MarkerPath,
		"KBATCH_DAEMON_READY_FILE="+readyDaemon,
		"KBATCH_WORKER_READY_FILE="+readyWorker,
	)

	daemonBin := itestBin("daemon")
	workerBin := itestBin("worker")

	s.daemonPID = exec.Command(daemonBin, "--config", s.ConfigPath, "--manifest", s.ManifestPath)
	s.daemonPID.Env = env
	s.daemonPID.Dir = repoRoot()
	if err := s.daemonPID.Start(); err != nil {
		s.T.Fatalf("start daemon: %v", err)
	}

	s.workerPID = exec.Command(workerBin, "--config", s.ConfigPath, "--manifest", s.ManifestPath)
	s.workerPID.Env = env
	s.workerPID.Dir = repoRoot()
	if err := s.workerPID.Start(); err != nil {
		s.stopDaemon()
		s.T.Fatalf("start worker: %v", err)
	}

	waitFile(s.T, readyDaemon, 45*time.Second, s.daemonPID.Process)
	waitFile(s.T, readyWorker, 45*time.Second, s.workerPID.Process)
}

func (s *Stack) Stop() {
	s.T.Helper()
	s.stopWorker()
	s.stopDaemon()
	if s.rdb != nil {
		_ = s.rdb.Close()
	}
}

func (s *Stack) stopDaemon() {
	if s.daemonPID != nil && s.daemonPID.Process != nil {
		_ = s.daemonPID.Process.Signal(syscall.SIGTERM)
		_ = s.daemonPID.Wait()
	}
}

func (s *Stack) stopWorker() {
	if s.workerPID != nil && s.workerPID.Process != nil {
		_ = s.workerPID.Process.Signal(syscall.SIGTERM)
		_ = s.workerPID.Wait()
	}
}

func (s *Stack) NewClient() *client.Client {
	s.T.Helper()
	cfg := client.DefaultConfig()
	cfg.Brokers = s.Brokers
	cfg.RedisURL = s.Redis
	cfg.ManifestPath = s.ManifestPath
	cfg.UniqEnabled = false
	cfg.EventsTopic = s.EventsTopic
	cfg.CallbacksTopic = s.CallbacksTopic
	cfg.DeadLetterTopic = s.DLTTopic
	cfg.ScheduledTopic = s.ScheduledTopic
	cfg.FairnessTimeIngest = s.TimeIngest
	cfg.FairnessThroughputIngest = s.TpIngest
	c, err := client.New(cfg)
	if err != nil {
		s.T.Fatal(err)
	}
	return c
}

func (s *Stack) Store() *store.RedisStore {
	return store.NewRedisStore(s.rdb, 7*24*time.Hour)
}

func (s *Stack) WaitBatch(ctx context.Context, batchID string, statuses ...string) *store.Batch {
	s.T.Helper()
	if len(statuses) == 0 {
		statuses = []string{"success", "complete"}
	}
	st := s.Store()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		row, err := st.FindBatch(ctx, batchID)
		if err != nil {
			s.T.Fatal(err)
		}
		if row != nil {
			for _, want := range statuses {
				if row.Status == want {
					return row
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.T.Fatalf("timeout waiting for batch %s in %v", batchID, statuses)
	return nil
}

func (s *Stack) WaitMarkerAt(path string, timeout time.Duration) string {
	s.T.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(b))) > 0 {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(200 * time.Millisecond)
	}
	s.T.Fatalf("timeout waiting for marker at %s", path)
	return ""
}

func (s *Stack) WaitMarker(timeout time.Duration) string {
	return s.WaitMarkerAt(s.MarkerPath, timeout)
}

func (s *Stack) PollTopic(ctx context.Context, topic string, match func(map[string]interface{}) bool, timeout time.Duration) map[string]interface{} {
	s.T.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.Brokers...),
		kgo.ConsumerGroup("kb-e2e-poll-"+uuid.NewString()[:8]),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		s.T.Fatal(err)
	}
	defer cl.Close()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			s.T.Fatalf("poll %s: %v", topic, errs[0].Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			var m map[string]interface{}
			if json.Unmarshal(rec.Value, &m) != nil {
				continue
			}
			if match == nil || match(m) {
				return m
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	s.T.Fatalf("timeout polling topic %s", topic)
	return nil
}

func baseHandlers(workerTopic string) map[string]handlerYAML {
	return map[string]handlerYAML{
		"integration.go_daemon": {
			Runtime: "go", Topic: workerTopic, MaxRetries: 2,
		},
		"integration.go_retry_once": {
			Runtime: "go", Topic: workerTopic, MaxRetries: 2,
		},
		"integration.go_always_fail": {
			Runtime: "go", Topic: workerTopic, MaxRetries: 1, CompleteAfterRetries: 1,
		},
		"integration.go_multi": {
			Runtime: "go", Topic: workerTopic, MaxRetries: 1,
		},
		"integration.go_fair": {
			Runtime: "go", FairnessType: "time", MaxRetries: 1,
		},
		"integration.go_fair_throughput": {
			Runtime: "go", FairnessType: "throughput", MaxRetries: 1,
		},
		"integration.go_scheduled": {
			Runtime: "go", Topic: workerTopic, MaxRetries: 1,
		},
		"integration.go_p0": {
			Runtime: "go", Topic: "", MaxRetries: 1,
		},
		"integration.go_p1": {
			Runtime: "go", Topic: "", MaxRetries: 1,
		},
		"integration.ruby_plain": {
			Runtime: "ruby", Topic: workerTopic + ".ruby", WorkerClass: "RubyPlainWorker",
		},
		"integration.ruby_fair": {
			Runtime: "ruby", FairnessType: "time", WorkerClass: "RubyFairWorker",
		},
	}
}

func skipUnlessIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("KAFKA_BATCH_INTEGRATION") != "1" && os.Getenv("KAFKA_BATCH_TEST_BROKERS") == "" {
		t.Skip("set KAFKA_BATCH_INTEGRATION=1 to run E2E tests")
	}
}

func brokersFromEnv() []string {
	if v := os.Getenv("KAFKA_BATCH_TEST_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

func redisFromEnv() string {
	if v := os.Getenv("KAFKA_BATCH_TEST_REDIS_URL"); v != "" {
		return v
	}
	return "redis://127.0.0.1:6379/15"
}

func kafkaReachable(brokers []string) bool {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return cl.Ping(ctx) == nil
}

func redisReachable(url string) bool {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return false
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	return rdb.Ping(context.Background()).Err() == nil
}

func itestBin(role string) string {
	envKey := "KBATCH_" + strings.ToUpper(role) + "_ITEST_BIN"
	if p := os.Getenv(envKey); p != "" {
		return p
	}
	return filepath.Join(repoRoot(), "bin", "kbatch-"+role+"-ittest")
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func waitFile(t *testing.T, path string, timeout time.Duration, proc *os.Process) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if proc != nil {
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("process died before ready file %s", path)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", path)
}

func writePriorityConfig(t *testing.T, dir, suffix, p0, p1 string) string {
	t.Helper()
	path := filepath.Join(dir, "priority.yml")
	doc := map[string]interface{}{
		"consumer_group_suffix": "prio-" + suffix,
		"mode":                  "weighted",
		"weighted_interleave":   4,
		"topics":                []string{p0, p1},
	}
	raw, _ := yaml.Marshal(doc)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func applyFairConfig(s *Stack, cfg *daemonYAML) {
	cfg.FairnessEnabled = true
	cfg.FairnessTimeIngest = s.TimeIngest
	cfg.FairnessTimeReadyGo = s.TimeReadyGo
	cfg.FairnessTimeReadyRuby = s.TimeReadyRuby
	cfg.FairnessThroughputIngest = s.TpIngest
	cfg.FairnessThroughputReadyGo = s.TpReadyGo
	cfg.FairnessThroughputReadyRuby = s.TpReadyRuby
}

func applyScheduleConfig(s *Stack, cfg *daemonYAML) {
	cfg.SchedulePollerEnabled = true
	cfg.ScheduledTopic = s.ScheduledTopic
}

func priorityHandlersForStack(s *Stack) map[string]handlerYAML {
	h := baseHandlers(s.WorkerTopic)
	h["integration.go_p0"] = handlerYAML{Runtime: "go", Topic: s.P0Topic, MaxRetries: 1}
	h["integration.go_p1"] = handlerYAML{Runtime: "go", Topic: s.P1Topic, MaxRetries: 1}
	return h
}

func rubyTopic(s *Stack) string {
	return s.WorkerTopic + ".ruby"
}
