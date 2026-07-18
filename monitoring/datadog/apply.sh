#!/usr/bin/env bash
#
# Idempotently create/update the kafka-batch Datadog monitors defined in
# monitors.json. Matches existing monitors by exact name among those tagged
# managed-by:kafka-batch-apply, so re-running updates in place (no duplicates).
#
# Requires: curl, jq. Auth via env:
#   DD_API_KEY  — Datadog API key   (the infra/datadog secret already has this)
#   DD_APP_KEY  — Datadog APP key   (NOT the api key; create one under
#                 Organization Settings → Application Keys)
#   DD_SITE     — optional, default datadoghq.com (e.g. us5.datadoghq.com, datadoghq.eu)
#
# Usage:
#   DD_API_KEY=... DD_APP_KEY=... ./apply.sh          # create/update
#   DD_API_KEY=... DD_APP_KEY=... ./apply.sh --dry-run
#
set -euo pipefail

: "${DD_API_KEY:?set DD_API_KEY}"
: "${DD_APP_KEY:?set DD_APP_KEY (an Application Key, not the API key)}"
DD_SITE="${DD_SITE:-datadoghq.com}"
API="https://api.${DD_SITE}/api/v1"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORS="$DIR/monitors.json"
DRY_RUN="${1:-}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

hdr=(-H "DD-API-KEY: ${DD_API_KEY}" -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" -H "Content-Type: application/json")

echo "site=${DD_SITE} monitors=$(jq length "$MONITORS")"

# name -> id for monitors this tool manages
existing="$(curl -sf "${hdr[@]}" "${API}/monitor?monitor_tags=managed-by:kafka-batch-apply")" || {
  echo "failed to list existing monitors (check keys/site)" >&2; exit 1; }

count="$(jq length "$MONITORS")"
for i in $(seq 0 $((count - 1))); do
  m="$(jq ".[$i]" "$MONITORS")"
  name="$(jq -r '.name' <<<"$m")"
  id="$(jq -r --arg n "$name" 'map(select(.name == $n)) | (.[0].id // empty)' <<<"$existing")"

  if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "[dry-run] $([ -n "$id" ] && echo "update id=$id" || echo create): $name"
    continue
  fi

  if [[ -n "$id" ]]; then
    curl -sf -X PUT "${hdr[@]}" "${API}/monitor/${id}" -d "$m" >/dev/null
    echo "updated  id=$id  $name"
  else
    new_id="$(curl -sf -X POST "${hdr[@]}" "${API}/monitor" -d "$m" | jq -r '.id')"
    echo "created  id=$new_id  $name"
  fi
done

echo "done."
