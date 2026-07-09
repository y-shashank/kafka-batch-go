package kafkaclient

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TopicPartitionCount returns the partition count for a topic via Kafka metadata.
func (c *Client) TopicPartitionCount(ctx context.Context, topic string) (int, error) {
	adm := kadm.NewClient(c.inner)
	meta, err := adm.Metadata(ctx, topic)
	if err != nil {
		return 0, err
	}
	detail := meta.Topics[topic]
	if detail.Err != nil {
		return 0, detail.Err
	}
	return len(detail.Partitions), nil
}
