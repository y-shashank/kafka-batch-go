package alerts

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Sample struct {
	LagTopics       []LagRow
	LagBaseline     map[string]map[string]interface{}
	PausedKeys      []string
	PendingTotal    int64
	LiveConsumers   int
	RTT             map[string]interface{}
	Reconciler      map[string]string
	Fairness        []FairLane
	SchedulePending int64
	ScheduleInflight int64
	DLTPerMinute    int
	CronStale       []map[string]interface{}
}

type LagRow struct {
	Group        string
	Topic        string
	Lag          int64
	CommittedSum *int64
	EndSum       *int64
}

type FairLane struct {
	Lane      string
	IngestLag int64
	ReadyLag  int64
}

func collectSample(ctx context.Context, rdb *redis.Client, st *State, cfg Config) Sample {
	s := Sample{
		LagBaseline: st.LoadBaseline(ctx),
		PausedKeys:  pausedKeys(ctx, rdb),
		RTT:         rttSummary(ctx, rdb),
		Reconciler:  reconcilerSummary(ctx, rdb),
		SchedulePending: zcard(ctx, rdb, "kafka_batch:sched:pending"),
		ScheduleInflight: zcard(ctx, rdb, "kafka_batch:sched:inflight"),
		DLTPerMinute: st.DLTCountLastMinute(ctx),
		CronStale:    st.CronStaleEntries(ctx),
		LiveConsumers: liveConsumerCount(ctx, rdb),
	}
	s.LagTopics, s.PendingTotal = collectLag(ctx, cfg)
	s.Fairness = fairnessLanes(ctx, cfg, s.LagTopics)
	return s
}

func persistBaseline(ctx context.Context, st *State, sample Sample) {
	base := map[string]map[string]interface{}{}
	for _, row := range sample.LagTopics {
		key := row.Group + "|" + row.Topic
		entry := map[string]interface{}{"lag": row.Lag}
		if row.CommittedSum != nil {
			entry["committed"] = *row.CommittedSum
		}
		if row.EndSum != nil {
			entry["end_sum"] = *row.EndSum
		}
		base[key] = entry
	}
	st.SaveBaseline(ctx, base)
}

func collectLag(ctx context.Context, cfg Config) ([]LagRow, int64) {
	if len(cfg.Brokers) == 0 {
		return nil, 0
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		log.Printf("[kbatch-alerts] lag client: %v", err)
		return nil, 0
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	// Sample primary control + jobs groups for stuck-lag signals.
	groups := []string{
		cfg.ConsumerGroup + "-control",
		cfg.ConsumerGroup + "-jobs",
		cfg.ConsumerGroup + "-dispatch-time",
		cfg.ConsumerGroup + "-dispatch-throughput",
		cfg.ConsumerGroup + "-jobs-fair-time",
		cfg.ConsumerGroup + "-jobs-fair-throughput",
		cfg.ConsumerGroup + "-go-worker-jobs",
	}
	var rows []LagRow
	var pending int64
	for _, g := range groups {
		lags, err := adm.Lag(ctx, g)
		if err != nil {
			continue
		}
		gl, ok := lags[g]
		if !ok || gl.Error() != nil {
			continue
		}
		for topic, parts := range gl.Lag {
			var lag int64
			var committed, end int64
			var hasC, hasE bool
			for _, p := range parts {
				if p.Lag > 0 {
					lag += p.Lag
				}
				if p.Err != nil {
					continue
				}
				// kadm.Offset.At is the committed offset; ListedOffset.Offset is log end.
				committed += p.Commit.At
				hasC = true
				if p.End.Err == nil && p.End.Offset >= 0 {
					end += p.End.Offset
					hasE = true
				}
			}
			if lag <= 0 && !hasC {
				continue
			}
			row := LagRow{Group: g, Topic: topic, Lag: lag}
			if hasC {
				c := committed
				row.CommittedSum = &c
			}
			if hasE {
				e := end
				row.EndSum = &e
			}
			rows = append(rows, row)
			pending += lag
		}
	}
	return rows, pending
}

func fairnessLanes(ctx context.Context, cfg Config, rows []LagRow) []FairLane {
	_ = ctx
	sumLag := func(topic string) int64 {
		var n int64
		for _, r := range rows {
			if r.Topic == topic {
				n += r.Lag
			}
		}
		return n
	}
	readySum := func(topics []string) int64 {
		var n int64
		for _, t := range topics {
			n += sumLag(t)
		}
		return n
	}
	return []FairLane{
		{Lane: "time", IngestLag: sumLag(cfg.FairnessTimeIngest), ReadyLag: readySum(cfg.FairnessTimeReady)},
		{Lane: "throughput", IngestLag: sumLag(cfg.FairnessThroughputIngest), ReadyLag: readySum(cfg.FairnessThroughputReady)},
	}
}

func pausedKeys(ctx context.Context, rdb *redis.Client) []string {
	members, err := rdb.SMembers(ctx, "kafka_batch:consumption:topics").Result()
	if err != nil {
		return nil
	}
	return members
}

func liveConsumerCount(ctx context.Context, rdb *redis.Client) int {
	var n int
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "kafka_batch:live:consumer:*", 200).Result()
		if err != nil {
			return n
		}
		n += len(keys)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return n
}

func reconcilerSummary(ctx context.Context, rdb *redis.Client) map[string]string {
	h, err := rdb.HGetAll(ctx, "kafka_batch:reconciler:last").Result()
	if err != nil {
		return nil
	}
	return h
}

func rttSummary(ctx context.Context, rdb *redis.Client) map[string]interface{} {
	// Align with perfmetrics minute buckets: kafka_batch:perf:min:<epoch>:rtt
	now := time.Now().UTC().Unix()
	bucket := (now / 60) * 60
	key := "kafka_batch:perf:min:" + strconv.FormatInt(bucket, 10) + ":rtt"
	h, err := rdb.HGetAll(ctx, key).Result()
	if err != nil || len(h) == 0 {
		// try previous minute
		key = "kafka_batch:perf:min:" + strconv.FormatInt(bucket-60, 10) + ":rtt"
		h, err = rdb.HGetAll(ctx, key).Result()
		if err != nil || len(h) == 0 {
			return nil
		}
	}
	count, _ := strconv.ParseFloat(h["count"], 64)
	sumUs, _ := strconv.ParseFloat(h["sum_us"], 64)
	maxUs, _ := strconv.ParseFloat(h["max_us"], 64)
	errors, _ := strconv.ParseFloat(h["errors"], 64)
	avg := 0.0
	if count > 0 {
		avg = (sumUs / count) / 1000.0
	}
	return map[string]interface{}{
		"latest_avg_ms": avg,
		"latest_max_ms": maxUs / 1000.0,
		"avg_ms":        avg,
		"max_ms":        maxUs / 1000.0,
		"errors":        int(errors),
		"probes":        int(count),
	}
}

func zcard(ctx context.Context, rdb *redis.Client, key string) int64 {
	n, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		return 0
	}
	return n
}

func pausedSet(keys []string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		// Ruby uses group\x1ftopic; Redis set may store "group\x1ftopic" or "group|topic"
		m[k] = true
		m[strings.ReplaceAll(k, "\x1f", "|")] = true
	}
	return m
}
