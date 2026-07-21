package client

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestValidateManifestEmptyWithPath(t *testing.T) {
	c := &Client{
		cfg:      Config{ManifestPath: "/tmp/handlers.yml", JobsTopic: "jobs"},
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{}},
	}
	err := c.validateManifest()
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(ConfigurationError); !ok {
		t.Fatalf("err type %T: %v", err, err)
	}
}

func TestValidateManifestOKWithWorkers(t *testing.T) {
	c := &Client{
		cfg: Config{
			ManifestPath: "/tmp/handlers.yml",
			JobsTopic:    "jobs",
			Workers:      map[string]WorkerClassConfig{"W": {Topic: "jobs.w"}},
		},
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{}},
	}
	if err := c.validateManifest(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateManifestOKWithHandlers(t *testing.T) {
	c := &Client{
		cfg: Config{JobsTopic: "jobs"},
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"echo": {Runtime: "go", Topic: "jobs.echo"},
		}},
	}
	if err := c.validateManifest(); err != nil {
		t.Fatal(err)
	}
}

func TestPingRedis(t *testing.T) {
	err := pingRedis(context.Background(), nil)
	if err == nil {
		t.Fatal("expected nil redis error")
	}
	if _, ok := err.(ConfigurationError); !ok {
		t.Fatalf("err type %T: %v", err, err)
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	if err := pingRedis(context.Background(), rdb); err != nil {
		t.Fatal(err)
	}

	bad := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0})
	if err := pingRedis(context.Background(), bad); err == nil {
		t.Fatal("expected ping failure for unreachable redis")
	}
}

func TestLookupHandlerExported(t *testing.T) {
	c := &Client{
		manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{
			"echo": {Runtime: "go", Topic: "jobs.echo"},
		}},
	}
	entry, err := c.LookupHandler("echo")
	if err != nil || entry.Topic != "jobs.echo" {
		t.Fatalf("entry=%+v err=%v", entry, err)
	}
	_, err = c.LookupHandler("missing")
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}
