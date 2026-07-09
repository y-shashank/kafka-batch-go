# Ruby ↔ Go compatibility integration specs

These specs require:

- Kafka (`KAFKA_BATCH_TEST_BROKERS` or `localhost:9092`)
- Redis (`KAFKA_BATCH_TEST_REDIS_URL`)
- The [kafka-batch](https://github.com/y-shashank/kafka-batch) gem checked out alongside this repo
- Built binaries: `kbatch-daemon-ittest`, `kbatch-worker-ittest`, `kbatch-client-ittest`

```bash
# From kafka-batch-go root
go build -o bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest
go build -o bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest
go build -o bin/kbatch-client-ittest ./cmd/kbatch-client-ittest

# From kafka-batch gem root (sibling clone)
export KBATCH_DAEMON_ITEST_BIN=/path/to/kafka-batch-go/bin/kbatch-daemon-ittest
export KBATCH_WORKER_ITEST_BIN=/path/to/kafka-batch-go/bin/kbatch-worker-ittest
export KAFKA_BATCH_INTEGRATION=1
bundle exec rspec ../kafka-batch-go/compat/ruby/spec/integration/
```

Copy or symlink `spec/support/redis_helper.rb` and other shared helpers from the gem when running locally.
