package consumption

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const topicPausePartition = -1

// MySQLPauseStore reads pause state from kafka_batch_consumption_pauses.
type MySQLPauseStore struct {
	db *sql.DB
}

func NewMySQLPauseStore(dsn string) (*MySQLPauseStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql consumption pause ping: %w", err)
	}
	return &MySQLPauseStore{db: db}, nil
}

func (m *MySQLPauseStore) Close() error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.Close()
}

func (m *MySQLPauseStore) Snapshot(ctx context.Context) (Snapshot, error) {
	snap := Snapshot{Topics: map[string]struct{}{}, Partitions: map[string]struct{}{}}
	if m == nil || m.db == nil {
		return snap, nil
	}
	rows, err := m.db.QueryContext(ctx,
		`SELECT consumer_group, topic_name, partition_id FROM kafka_batch_consumption_pauses`)
	if err != nil {
		return snap, err
	}
	defer rows.Close()
	for rows.Next() {
		var group, topic string
		var partition int
		if err := rows.Scan(&group, &topic, &partition); err != nil {
			return snap, err
		}
		if partition == topicPausePartition {
			snap.Topics[TopicKey(group, topic)] = struct{}{}
		} else {
			snap.Partitions[PartitionKey(group, topic, int32(partition))] = struct{}{}
		}
	}
	return snap, rows.Err()
}
