package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
	"github.com/y-shashank/kafka-batch-go/pkg/daemon"
	"github.com/y-shashank/kafka-batch-go/pkg/kafkaclient"
	"github.com/y-shashank/kafka-batch-go/pkg/reconciler"
	"github.com/y-shashank/kafka-batch-go/pkg/store"
	"github.com/y-shashank/kafka-batch-go/pkg/topics"
	"github.com/y-shashank/kafka-batch-go/pkg/version"
	"github.com/y-shashank/kafka-batch-go/pkg/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "daemon":
		runDaemon(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "reconcile":
		runReconcile(os.Args[2:])
	case "topics":
		runTopics(os.Args[2:])
	case "version":
		fmt.Println(version.Version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func runDaemon(args []string) {
	fs := flag.NewFlagSet("daemon", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML path")
	manifest := fs.String("manifest", "", "handler manifest YAML path")
	_ = fs.Parse(args)
	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "daemon requires --config")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch daemon: %v\n", err)
		os.Exit(1)
	}
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML path")
	manifest := fs.String("manifest", "", "handler manifest YAML path")
	_ = fs.Parse(args)
	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "worker requires --config")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch worker: %v\n", err)
		os.Exit(1)
	}
}

func runReconcile(args []string) {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	cfgPath := fs.String("config", "", "daemon config YAML path")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "reconcile requires --config")
		os.Exit(2)
	}
	cfg, err := config.LoadDaemon(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	rOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(rOpts)
	st := store.NewRedisStore(rdb, cfg.BatchTTL)
	prod, err := kafkaclient.New(cfg.Brokers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch reconcile: %v\n", err)
		os.Exit(1)
	}
	defer prod.Close()
	defer rdb.Close()

	ctx := context.Background()
	switch reconciler.Run(ctx, cfg, st, prod, "cli") {
	case reconciler.ResultLockSkipped:
		fmt.Println("reconcile: lock held by another process")
		os.Exit(0)
	case reconciler.ResultFailed:
		fmt.Fprintf(os.Stderr, "reconcile: failed\n")
		os.Exit(1)
	case reconciler.ResultCompleted:
		fmt.Println("reconcile: completed")
	}
}

func runTopics(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "topics requires create or validate")
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		runTopicsCreate(args[1:])
	case "validate":
		runTopicsValidate(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown topics subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func runTopicsCreate(args []string) {
	fs := flag.NewFlagSet("topics create", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated brokers")
	manifest := fs.String("manifest", "", "handler manifest YAML")
	prefix := fs.String("topic-prefix", "", "topic prefix")
	partitions := fs.Int("partitions", 0, "force partition count for all topics")
	includeControl := fs.Bool("include-control", false, "also provision events/dead_letter")
	_ = fs.Parse(args)

	ct := topics.ClientTopics{
		Brokers:             strings.Split(*brokers, ","),
		TopicPrefix:         *prefix,
		JobsTopic:           "kafka_batch.jobs",
		ScheduledTopic:      "kafka_batch.scheduled",
		CallbacksTopic:      "kafka_batch.callbacks",
		FairnessTimeIngest:  "kafka_batch.fair_time_ingest",
		FairnessThroughputIngest: "kafka_batch.fair_throughput_ingest",
		IncludeControlPlane: *includeControl,
	}
	if *partitions > 0 {
		ct.ForcePartitions = int32(*partitions)
	}
	if *manifest != "" {
		m, err := config.LoadManifest(*manifest, *prefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kbatch topics create: %v\n", err)
			os.Exit(1)
		}
		ct.Manifest = m
	}
	res, err := topics.CreateAll(context.Background(), ct.Brokers, topics.Specs(ct))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch topics create: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("created=%d skipped=%d failed=%d\n", len(res.Created), len(res.Skipped), len(res.Failed))
	for _, f := range res.Failed {
		fmt.Printf("FAILED %s: %s\n", f.Name, f.Error)
	}
	if len(res.Failed) > 0 {
		os.Exit(1)
	}
}

func runTopicsValidate(args []string) {
	fs := flag.NewFlagSet("topics validate", flag.ExitOnError)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated brokers")
	manifest := fs.String("manifest", "", "handler manifest YAML")
	prefix := fs.String("topic-prefix", "", "topic prefix")
	includeControl := fs.Bool("include-control", false, "also check events/dead_letter")
	_ = fs.Parse(args)

	ct := topics.ClientTopics{
		Brokers:             strings.Split(*brokers, ","),
		TopicPrefix:         *prefix,
		JobsTopic:           "kafka_batch.jobs",
		ScheduledTopic:      "kafka_batch.scheduled",
		CallbacksTopic:      "kafka_batch.callbacks",
		FairnessTimeIngest:  "kafka_batch.fair_time_ingest",
		FairnessThroughputIngest: "kafka_batch.fair_throughput_ingest",
		IncludeControlPlane: *includeControl,
	}
	if *manifest != "" {
		m, err := config.LoadManifest(*manifest, *prefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kbatch topics validate: %v\n", err)
			os.Exit(1)
		}
		ct.Manifest = m
	}
	missing, err := topics.Missing(context.Background(), ct.Brokers, topics.Specs(ct))
	if err != nil {
		fmt.Fprintf(os.Stderr, "kbatch topics validate: %v\n", err)
		os.Exit(1)
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "missing topics: %s\n", strings.Join(missing, ", "))
		os.Exit(1)
	}
	fmt.Println("all required topics exist")
}

func usage() {
	fmt.Fprintf(os.Stderr, `kbatch — KafkaBatch Go runtime

Usage:
  kbatch daemon --config PATH [--manifest PATH]   # control plane
  kbatch worker --config PATH [--manifest PATH]   # Go execution consumer
  kbatch reconcile --config PATH                  # one-shot stuck-batch sweep
  kbatch topics create [--brokers HOST:PORT] [--manifest PATH]
  kbatch topics validate [--brokers HOST:PORT] [--manifest PATH]
  kbatch version

Environment:
  KAFKA_BROKERS, KAFKA_PREFIX, REDIS_URL, KAFKA_BATCH_HANDLER_MANIFEST
`)
}
