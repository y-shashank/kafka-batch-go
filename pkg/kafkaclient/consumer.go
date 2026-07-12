package kafkaclient

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

// FetchOpts returns franz-go consumer options for prefetch fairness tuning.
func FetchOpts(s config.ConsumerFetchSettings) []kgo.Opt {
	return []kgo.Opt{
		kgo.FetchMaxBytes(s.MaxBytes),
		kgo.FetchMaxPartitionBytes(s.MaxPartitionBytes),
		kgo.FetchMaxWait(s.MaxWait),
	}
}
