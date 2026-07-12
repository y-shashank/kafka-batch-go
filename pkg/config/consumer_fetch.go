package config

import "time"

// Consumer fetch defaults — tighter than franz-go/librdkafka defaults (50MB / 1MB)
// for fairer multi-partition polling (see Karafka multiplexing prefetch guidance).
const (
	DefaultConsumerFetchMaxBytes          = 1 << 20  // 1 MiB
	DefaultConsumerFetchMaxPartitionBytes = 128 << 10 // 128 KiB
	DefaultConsumerFetchMaxWait           = 200 * time.Millisecond
)

// ConsumerFetchSettings limits broker prefetch per poll for daemon/worker consumers.
type ConsumerFetchSettings struct {
	MaxBytes          int32
	MaxPartitionBytes int32
	MaxWait           time.Duration
}

// ConsumerFetchSettings returns resolved fetch limits for Kafka consumer clients.
func (c Daemon) ConsumerFetchSettings() ConsumerFetchSettings {
	s := ConsumerFetchSettings{
		MaxBytes:          DefaultConsumerFetchMaxBytes,
		MaxPartitionBytes: DefaultConsumerFetchMaxPartitionBytes,
		MaxWait:           DefaultConsumerFetchMaxWait,
	}
	if c.ConsumerFetchMaxBytes > 0 {
		s.MaxBytes = c.ConsumerFetchMaxBytes
	}
	if c.ConsumerFetchMaxPartitionBytes > 0 {
		s.MaxPartitionBytes = c.ConsumerFetchMaxPartitionBytes
	}
	if c.ConsumerFetchMaxWait > 0 {
		s.MaxWait = c.ConsumerFetchMaxWait
	}
	return s
}
