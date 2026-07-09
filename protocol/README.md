# kbatch execution protocol (legacy)

> **Deprecated.** This socket protocol was used by the Phase 2 `kbatch serve` sidecar and `executor :go`. Current deployments use the [three-tier architecture](../README.md#three-tier-architecture): dedicated Kafka topics per runtime — no cross-runtime RPC.

HTTP/JSON over a **Unix domain socket** (historical).

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness (`200` + `{"ok":true}`) |
| `POST` | `/v1/execute` | Run one job handler |

## Execute request

See `execute_request.json`. Required fields:

- `job_type` — stable handler id (matches manifest / worker `job_type`)
- `job_id` — UUID
- `attempt` — retry count (0-based)
- `payload` — job arguments (JSON object)

Optional: `batch_id`, `tenant_id`, `enqueued_at`.

## Execute response

Success (`execute_response_ok.json`):

```json
{ "ok": true }
```

Failure (`execute_response_error.json`):

```json
{
  "ok": false,
  "error_class": "MyError",
  "error_message": "human-readable detail"
}
```

Ruby maps `ok: false` to a retryable job error (same as `#perform` raising).

## Versioning

Path prefix `/v1/` reserves room for breaking changes. Golden fixtures in this directory are checked by Ruby and Go tests.
