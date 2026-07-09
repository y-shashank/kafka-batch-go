# Ruby ↔ Go cross-runtime matrix tests

Matrix tests run from Go (`integration/matrix/`) across client, control, and execution tiers.

| Tier | Drivers |
|------|---------|
| **Client** | Go `pkg/client` or `bin/ruby_client_ittest.rb` |
| **Control** | Go `kbatch-daemon-ittest` or `bin/ruby_control_ittest.rb` |
| **Execution** | Go `kbatch-worker-ittest` and/or Ruby `bin/ruby_drain.rb` |

## Prerequisites

- Kafka (`KAFKA_BATCH_TEST_BROKERS` or `localhost:9092`)
- Redis (`KAFKA_BATCH_TEST_REDIS_URL`)
- [kafka-batch](https://github.com/y-shashank/kafka-batch) gem at `kafka-batch/` (CI) or sibling clone (`KAFKA_BATCH_GEM_PATH`)

```bash
cd compat/ruby && bundle install

export KAFKA_BATCH_INTEGRATION=1
go test -tags=integration ./integration/matrix/ -run TestMatrix_PR -count=1 -timeout 30m -v
```

## Ruby scripts

| Script | Role |
|--------|------|
| `bin/ruby_drain.rb` | One-shot Ruby JobConsumer drain (execution) |
| `bin/ruby_client_ittest.rb` | Ruby produce client (enqueue / batch modes) |
| `bin/ruby_control_ittest.rb` | Ruby tier-2 control loop (events, retry, fair ingest) |

Itest workers: `lib/kafka_batch_spec/itest_workers.rb`

## Phases

- **Phase 1 / PR** — Go client, Go control, mixed execution (batch go/ruby, mixed, schedule, priority, DLT)
- **Phase 2** — Fair routing + retry through Ruby execution
- **Phase 3** — Ruby client parity (`TestMatrix_Phase3_RubyClient`, envelope parity test)
- **Phase 4** — Ruby control plane (`TestMatrix_Phase4_RubyControl`)
- **Nightly** — `TestMatrix_Full` (all combos × all scenarios)

## Legacy Ruby RSpec specs

Older specs under `spec/integration/` exercise Go stack from Ruby. See individual files for setup.
