# Ruby ↔ Go mixed integration specs (deferred)

Full **Go-only** end-to-end tests live in `integration/e2e/`:

```bash
# Build itest binaries
go build -o bin/kbatch-daemon-ittest ./cmd/kbatch-daemon-ittest
go build -o bin/kbatch-worker-ittest ./cmd/kbatch-worker-ittest

export KAFKA_BATCH_INTEGRATION=1
export KAFKA_BATCH_TEST_REDIS_URL=redis://127.0.0.1:6379/15
go test -tags=integration ./integration/e2e/ -v -count=1
```

The Ruby specs under `spec/integration/` remain for future Ruby↔Go mixed-tier testing.
