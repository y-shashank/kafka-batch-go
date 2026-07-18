# kafka-batch Datadog monitors

Monitor-as-code for the kafka-batch control plane. These alert on the failure
modes surfaced by the batch-accounting and recurring-scheduler work — the same
metrics are emitted by **both** the Go daemon and the Ruby control plane, so one
set of monitors covers either runtime.

This is intentionally **not** Terraform (the infra repo has no Datadog provider,
only the agent Helm release). Apply it with the script here or import the JSON
into your own tooling.

## Metrics

The control plane emits these via DogStatsD (metrics bridge; prefix defaults to
`kafka_batch`, configurable via `metrics_prefix`). Each event becomes
`<prefix>.<event>.count` (+ `.duration`). Tags come from the event payload
(e.g. `schedule`, `job_type`, `reason`, `batch_id`).

| Metric | Emitted when | Signals |
|---|---|---|
| `kafka_batch.completion_dropped.count` | a completion event can't be applied (batch hash absent → `not_found`, or malformed) | **count loss → stuck batch**; usually Redis/topic-prefix mismatch between producer and daemon |
| `kafka_batch.callback_produce_failed.count` | a completed batch's callback couldn't be produced and was parked on the DLT | callback didn't fire until replayed |
| `kafka_batch.cron_stale.count` | an enabled recurring schedule is idle beyond 2× its interval | that schedule stopped firing |
| `kafka_batch.cron_heartbeat.count` | once per recurring-scheduler sweep (liveness pulse) | **absence** = the ticker/leader is down |
| `kafka_batch.batch_push_rejected.count` | a push hit a sealed/cancelled batch | create-sealed-then-push race dropping jobs |

## Monitors (`monitors.json`)

1. **completions dropped** — count loss / stuck batches (critical)
2. **callback produce failed** — callback dead-lettered (critical)
3. **recurring schedule stale** — per-`schedule` multi-alert (critical)
4. **recurring scheduler heartbeat missing** — ticker stopped, incl. no-data (critical)
5. **batch push rejected** — sealed-batch race (low priority)

Each is tagged `managed-by:kafka-batch-apply` so `apply.sh` can update in place.

## Apply

```bash
export DD_API_KEY=...     # the infra/datadog secret already holds this
export DD_APP_KEY=...     # an Application Key (Org Settings → Application Keys)
export DD_SITE=datadoghq.com   # or us5.datadoghq.com / datadoghq.eu / ...

./apply.sh --dry-run      # preview create vs update
./apply.sh                # create/update (idempotent by name)
```

Re-running is safe: monitors are matched by exact name among those tagged
`managed-by:kafka-batch-apply`, so edits update the existing monitor rather than
creating duplicates.

## Before you apply

- **Notification target:** every monitor message ends with `@CHANGE_ME`. Replace
  it with your real target (e.g. `@slack-oncall`, `@pagerduty-...`,
  `@team-email@…`) before or after applying — otherwise alerts have nowhere to go.
- **App key:** the existing `infra/datadog` secret only carries the *API* key
  (used by the agent). Creating/updating monitors via the API also needs an
  *Application* key.
- **Metrics must exist:** the monitors reference `kafka_batch.*` metrics, which
  only appear once the control plane is running with metrics enabled
  (`metrics_enabled: true`, `metrics_statsd_addr` pointing at the DogStatsD
  agent). If `metrics_prefix` isn't `kafka_batch`, adjust the queries.
