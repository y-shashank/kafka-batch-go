package kafkaclient

import (
	"testing"
	"time"

	"github.com/y-shashank/kafka-batch-go/pkg/config"
)

func TestFetchOptsUsesSettings(t *testing.T) {
	s := config.ConsumerFetchSettings{
		MaxBytes:          2 << 20,
		MaxPartitionBytes: 256 << 10,
		MaxWait:           500 * time.Millisecond,
	}
	opts := FetchOpts(s)
	if len(opts) != 3 {
		t.Fatalf("opts=%d", len(opts))
	}
}
