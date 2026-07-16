package store

// FailureEntry is input to RecordFailure (Ruby-compatible).
//
// No per-job failure metadata is stored in Redis: exhausted jobs land on the
// dead-letter topic and retrying jobs are listed live from the retry topics.
// FailureEntry is only used by MySQLFailures (durable, kafka_batch_failures
// table parity with Ruby's Stores::MysqlStore) — see mysql_failures.go.
type FailureEntry struct {
	BatchID      string
	JobID        string
	WorkerClass  string
	ErrorClass   string
	ErrorMessage string
	Attempt      int
	Status       string
	NextRetryAt  string
}
