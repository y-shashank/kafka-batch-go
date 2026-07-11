package kafkaclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client wraps franz-go for produce + consume.
type Client struct {
	inner *kgo.Client
}

type clientOpts struct {
	requiredAcks kgo.Acks
}

// Option configures a Client.
type Option func(*clientOpts)

// WithRequiredAcks sets the produce ack level. Use kgo.AllISRAcks() (default)
// for maximum durability, or kgo.LeaderAck() for lower latency.
func WithRequiredAcks(acks kgo.Acks) Option {
	return func(o *clientOpts) {
		o.requiredAcks = acks
	}
}

// RequiredAcksFromConfig maps daemon YAML ("all_isr" | "leader") to franz-go.
func RequiredAcksFromConfig(v string) (kgo.Acks, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "all_isr", "all", "all_isr_acks":
		return kgo.AllISRAcks(), nil
	case "leader", "leader_ack":
		return kgo.LeaderAck(), nil
	default:
		return kgo.NoAck(), fmt.Errorf("unsupported producer_required_acks %q (use all_isr or leader)", v)
	}
}

func New(brokers []string, opts ...Option) (*Client, error) {
	cfg := clientOpts{requiredAcks: kgo.AllISRAcks()}
	for _, opt := range opts {
		opt(&cfg)
	}
	inner, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(cfg.requiredAcks),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &Client{inner: inner}, nil
}

func (c *Client) Close() {
	c.inner.Close()
}

// ProduceRequest is one message for ProduceMany.
type ProduceRequest struct {
	Topic string
	Key   string
	Value []byte
}

func (c *Client) Produce(ctx context.Context, topic, key string, payload []byte) error {
	return c.ProduceMany(ctx, ProduceRequest{Topic: topic, Key: key, Value: payload})
}

// ProduceMany sends multiple records in one ProduceSync call (pipelined to the broker).
func (c *Client) ProduceMany(ctx context.Context, reqs ...ProduceRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(reqs))
	for i, r := range reqs {
		recs[i] = &kgo.Record{Topic: r.Topic, Key: []byte(r.Key), Value: r.Value}
	}
	results := c.inner.ProduceSync(ctx, recs...)
	for _, res := range results {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

func (c *Client) Poll(ctx context.Context, topics []string, group string, handler func(*kgo.Record) error) error {
	_ = topics
	_ = group
	_ = handler
	return fmt.Errorf("use daemon.Run with integrated consumer loops")
}

// Inner exposes the raw client for the daemon.
func (c *Client) Inner() *kgo.Client { return c.inner }
