package daemon

import (
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var partitionDeferPauseMu sync.Mutex

// deferPartitionPause pauses fetching one partition until wait elapses, then resumes.
// The consumer offset stays uncommitted; franz-go skips the paused partition so other
// partitions keep moving without holding the BlockRebalanceOnPoll gate across sleep.
func deferPartitionPause(cl fetchPauser, rec *kgo.Record, wait time.Duration) {
	if cl == nil || rec == nil || wait <= 0 {
		return
	}
	topic := rec.Topic
	partition := rec.Partition
	parts := map[string][]int32{topic: {partition}}

	partitionDeferPauseMu.Lock()
	cl.PauseFetchPartitions(parts)
	partitionDeferPauseMu.Unlock()

	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		<-timer.C
		partitionDeferPauseMu.Lock()
		cl.ResumeFetchPartitions(parts)
		partitionDeferPauseMu.Unlock()
	}()
}
