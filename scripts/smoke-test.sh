#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:4000/v1}"
API_KEY="${LITELLM_MASTER_KEY:-sk-change-me-local-router}"
MODEL="${MODEL:-local-fast}"

echo "== Models =="
curl -fsS "$BASE_URL/models"   -H "Authorization: Bearer $API_KEY"
echo
echo

echo "== Chat =="
response_file="$(mktemp)"
payload_file="$(mktemp)"
cat >"$payload_file" <<JSON
{
  "model": "$MODEL",
  "messages": [{"role":"user","content":"Reply with exactly: local stack ok"}],
  "temperature": 0,
  "max_tokens": 16
}
JSON
status=$(curl -sS -o "$response_file" -w "%{http_code}" "$BASE_URL/chat/completions"   -H "Authorization: Bearer $API_KEY"   -H "Content-Type: application/json"   --data-binary "@$payload_file")
cat "$response_file"
echo
rm -f "$response_file" "$payload_file"

if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
  echo "Smoke chat failed with HTTP $status" >&2
  exit 1
fi
