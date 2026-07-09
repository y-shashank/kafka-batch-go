# Ruby ↔ Go cross-runtime matrix tests

Phase 1 matrix tests run from Go (`integration/matrix/`) with:

- **Client:** Go `pkg/client`
- **Control:** Go `kbatch-daemon-ittest`
- **Execution:** Go worker and/or Ruby `JobConsumer` drain

## Prerequisites

- Kafka (`KAFKA_BATCH_TEST_BROKERS` or `localhost:9092`)
- Redis (`KAFKA_BATCH_TEST_REDIS_URL`)
- [kafka-batch](https://github.com/y-shashank/kafka-batch) gem (sibling clone or `KAFKA_BATCH_GEM_PATH`)

```bash
# From kafka-batch-go root
cd compat/ruby && bundle install

export KAFKA_BATCH_INTEGRATION=1
go test -tags=integration ./integration/matrix/ -run TestMatrix_Phase1 -count=1 -timeout 15m -v
```

## Ruby drain worker

`bin/ruby_drain.rb` consumes ruby job topics with the real `KafkaBatch::Consumers::JobConsumer` and emits events to Kafka (Go daemon completes the batch ledger).

Itest workers live in `lib/kafka_batch_spec/itest_workers.rb` (`RubyPlainWorker`, `RubyFairWorker`, etc.).

## Phase 2 (optional)

Fair routing + retry through Ruby execution:

```bash
go test -tags=integration ./integration/matrix/ -run TestMatrix_Phase2 -count=1 -timeout 20m -v
```

## Legacy Ruby RSpec specs

Older specs under `spec/integration/` exercise Go stack from Ruby. See individual files for setup.
