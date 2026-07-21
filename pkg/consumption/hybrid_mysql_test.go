package consumption

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestMySQLPauseStoreSnapshotAndClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	store := &MySQLPauseStore{db: db}
	rows := sqlmock.NewRows([]string{"consumer_group", "topic_name", "partition_id"}).
		AddRow("g1", "jobs", -1).
		AddRow("g1", "jobs", 2)
	mock.ExpectQuery(`SELECT consumer_group, topic_name, partition_id FROM kafka_batch_consumption_pauses`).
		WillReturnRows(rows)

	snap, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Topics[TopicKey("g1", "jobs")]; !ok {
		t.Fatalf("topics=%v", snap.Topics)
	}
	if _, ok := snap.Partitions[PartitionKey("g1", "jobs", 2)]; !ok {
		t.Fatalf("partitions=%v", snap.Partitions)
	}
	mock.ExpectClose()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLPauseStoreNilSafe(t *testing.T) {
	var store *MySQLPauseStore
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	snap, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Topics) != 0 {
		t.Fatalf("%v", snap.Topics)
	}
	empty := &MySQLPauseStore{}
	snap, err = empty.Snapshot(context.Background())
	if err != nil || len(snap.Partitions) != 0 {
		t.Fatalf("snap=%v err=%v", snap, err)
	}
}

func TestNewMySQLPauseStoreBadDSN(t *testing.T) {
	if _, err := NewMySQLPauseStore("://bad"); err == nil {
		t.Fatal("expected dsn error")
	}
}

func TestHybridControlRedisPath(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctl := NewHybridControl(rdb, nil, time.Nanosecond)
	ctx := context.Background()

	base := NewControl(rdb, time.Nanosecond)
	if err := base.PauseTopic(ctx, "g", "jobs"); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, partitionsKey, PartitionKey("g", "other", 3)).Err(); err != nil {
		t.Fatal(err)
	}

	if !ctl.TopicLevelPaused(ctx, "g", "jobs") {
		t.Fatal("expected topic paused")
	}
	if !ctl.Paused(ctx, "g", "jobs", 0) {
		t.Fatal("topic-level should pause all partitions")
	}
	if !ctl.Paused(ctx, "g", "other", 3) {
		t.Fatal("expected partition-level pause")
	}
	// Cache hit within interval after first load — bump interval and call again.
	ctl.Interval = time.Hour
	if !ctl.Paused(ctx, "g", "jobs", 9) {
		t.Fatal("cached snapshot should still show pause")
	}
	active := ctl.ActiveHigherTopics(ctx, "g", []string{"jobs", "p1", ""})
	if len(active) != 1 || active[0] != "p1" {
		t.Fatalf("active=%v", active)
	}
}

func TestHybridControlFallsBackToMySQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mysql := &MySQLPauseStore{db: db}
	// Nil Redis skips the Ping path and loads from MySQL (same branch as Ping failure).
	ctl := NewHybridControl(nil, mysql, time.Nanosecond)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic_name", "partition_id"}).
		AddRow("g", "p0", -1)
	mock.ExpectQuery(`SELECT consumer_group, topic_name, partition_id FROM kafka_batch_consumption_pauses`).
		WillReturnRows(rows)

	ctx := context.Background()
	if !ctl.TopicLevelPaused(ctx, "g", "p0") {
		t.Fatal("expected mysql fallback pause")
	}
	// Cached snapshot (interval not elapsed) — no second query.
	ctl.Interval = time.Hour
	active := ctl.ActiveHigherTopics(ctx, "g", []string{"p0", "p1"})
	if len(active) != 1 || active[0] != "p1" {
		t.Fatalf("active=%v", active)
	}
}

func TestHybridControlNilAndDefaults(t *testing.T) {
	var nilCtl *HybridControl
	snap := nilCtl.snapshot(context.Background())
	if snap.Topics == nil || snap.Partitions == nil {
		t.Fatal("expected empty maps")
	}
	ctl := NewHybridControl(nil, nil, 0)
	if ctl.Interval != 30*time.Second {
		t.Fatalf("interval=%s", ctl.Interval)
	}
	if ctl.Paused(context.Background(), "g", "t", 0) {
		t.Fatal("empty should not pause")
	}
}
