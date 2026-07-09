package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/y-shashank/kafka-batch-go/pkg/daemon"
)

func main() {
	fs := flag.NewFlagSet("kbatch-daemon-ittest", flag.ExitOnError)
	cfg := fs.String("config", "", "daemon config YAML")
	manifest := fs.String("manifest", "", "handler manifest YAML")
	_ = fs.Parse(os.Args[1:])

	if *cfg == "" {
		fmt.Fprintln(os.Stderr, "--config required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := daemon.Run(ctx, *cfg, *manifest); err != nil {
		fmt.Fprintf(os.Stderr, "kbatch-daemon-ittest: %v\n", err)
		os.Exit(1)
	}
}
