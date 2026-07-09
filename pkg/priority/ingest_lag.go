package priority

import "context"

// IngestActiveCount returns ingest partitions with lag > 0 for a consumer group.
func (l *LagReader) IngestActiveCount(ctx context.Context, group, topic string) (int, error) {
	if l == nil || l.adm == nil || group == "" || topic == "" {
		return 0, nil
	}
	lags, err := l.adm.Lag(ctx, group)
	if err != nil {
		return 0, err
	}
	gl, ok := lags[group]
	if !ok {
		return 0, nil
	}
	if err := gl.Error(); err != nil {
		return 0, err
	}
	count := 0
	for _, part := range gl.Lag[topic] {
		if part.Lag > 0 {
			count++
		}
	}
	return count, nil
}
