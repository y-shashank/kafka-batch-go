package client

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestCloseNilSafe(t *testing.T) {
	c := &Client{}
	c.Close() // must not panic
}

func TestCloseRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	c := &Client{rdb: rdb}
	c.Close()
	if c.rdb != nil {
		t.Fatal("expected rdb cleared")
	}
	c.Close() // second close ok
}

func TestEnqueueJobUnknownHandler(t *testing.T) {
	c := &Client{cfg: DefaultConfig(), manifest: config.Manifest{Handlers: map[string]config.HandlerEntry{}}}
	_, err := c.EnqueueJob(context.Background(), "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
	_, err = c.EnqueueJobAt(context.Background(), nil, "missing", nil, PushOptions{})
	if _, ok := err.(UnknownHandlerError); !ok {
		t.Fatalf("err=%v", err)
	}
}
