package consumption

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Control must NOT cache an empty "nothing paused" snapshot when the Redis read
// fails — that silently un-pauses a genuinely paused topic (killswitch) for a
// full refresh interval. It must retain the last good snapshot instead.
func TestControlRetainsLastGoodPauseOnRedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 150 * time.Millisecond, ReadTimeout: 150 * time.Millisecond, PoolTimeout: 150 * time.Millisecond})
	// Interval nanosecond → every call reloads (so the error path is exercised).
	c := NewControl(rdb, time.Nanosecond)
	ctx := context.Background()

	group := "kafka-batch-jobs"
	if err := c.PauseTopic(ctx, group, "runaway.topic"); err != nil {
		t.Fatal(err)
	}
	if !c.TopicLevelPaused(ctx, group, "runaway.topic") {
		t.Fatal("expected topic paused after PauseTopic")
	}

	// Redis blip: reads now error.
	mr.Close()

	if !c.TopicLevelPaused(ctx, group, "runaway.topic") {
		t.Fatal("REGRESSION: paused topic reported unpaused after a Redis read error")
	}
	if !c.Paused(ctx, group, "runaway.topic", 0) {
		t.Fatal("REGRESSION: Paused() dropped the pause after a Redis read error")
	}
}

// HybridControl must retain last-good when both backends fail...
func TestHybridRetainsLastGoodPauseOnRedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 150 * time.Millisecond, ReadTimeout: 150 * time.Millisecond, PoolTimeout: 150 * time.Millisecond})
	ctx := context.Background()
	group := "kafka-batch-jobs"
	if err := rdb.SAdd(ctx, topicsKey, TopicKey(group, "runaway.topic")).Err(); err != nil {
		t.Fatal(err)
	}
	c := NewHybridControl(rdb, nil, time.Nanosecond)
	if !c.TopicLevelPaused(ctx, group, "runaway.topic") {
		t.Fatal("expected paused from redis")
	}
	mr.Close()
	if !c.TopicLevelPaused(ctx, group, "runaway.topic") {
		t.Fatal("REGRESSION: hybrid dropped the pause after redis went down (no last-good)")
	}
}

// ...and fall back to the MySQL pause store when Redis is unavailable rather than
// returning an empty (unpaused) snapshot.
func TestHybridFallsBackToMySQLWhenRedisDown(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 150 * time.Millisecond, ReadTimeout: 150 * time.Millisecond, PoolTimeout: 150 * time.Millisecond})
	mr.Close() // Redis down: ping + reads fail.

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT consumer_group, topic_name, partition_id FROM kafka_batch_consumption_pauses`).
		WillReturnRows(sqlmock.NewRows([]string{"consumer_group", "topic_name", "partition_id"}).
			AddRow("g1", "jobs", -1))

	c := NewHybridControl(rdb, &MySQLPauseStore{db: db}, time.Nanosecond)
	if !c.TopicLevelPaused(context.Background(), "g1", "jobs") {
		t.Fatal("expected MySQL fallback to report the pause while Redis is down")
	}
}
