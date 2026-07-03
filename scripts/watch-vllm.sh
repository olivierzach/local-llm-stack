#!/usr/bin/env bash
set -euo pipefail

URL="${VLLM_METRICS_URL:-http://localhost:8001/metrics}"
INTERVAL="${INTERVAL:-2}"

metric_value() {
  local metrics="$1"
  local name="$2"
  printf '%s\n' "$metrics" | awk -v n="$name" '{ key=$1; sub(/[{].*/, "", key); if (key == n) {print $NF; found=1; exit} } END { if (!found) print "" }'
}

sample() {
  curl -fsS "$URL"
}

prev_metrics="$(sample)"
prev_prompt="$(metric_value "$prev_metrics" 'vllm:prompt_tokens_total')"
prev_gen="$(metric_value "$prev_metrics" 'vllm:generation_tokens_total')"
prev_time="$(date +%s)"

while true; do
  sleep "$INTERVAL"
  metrics="$(sample)"
  now="$(date +%s)"
  elapsed=$((now - prev_time))
  [[ "$elapsed" -le 0 ]] && elapsed=1

  prompt="$(metric_value "$metrics" 'vllm:prompt_tokens_total')"
  gen="$(metric_value "$metrics" 'vllm:generation_tokens_total')"
  running="$(metric_value "$metrics" 'vllm:num_requests_running')"
  waiting="$(metric_value "$metrics" 'vllm:num_requests_waiting')"
  kv="$(metric_value "$metrics" 'vllm:kv_cache_usage_perc')"
  prefix_queries="$(metric_value "$metrics" 'vllm:prefix_cache_queries_total')"
  prefix_hits="$(metric_value "$metrics" 'vllm:prefix_cache_hits_total')"
  successes="$(metric_value "$metrics" 'vllm:request_success_total')"

  prompt_rate="$(awk -v a="${prompt:-0}" -v b="${prev_prompt:-0}" -v e="$elapsed" 'BEGIN {printf "%.2f", (a-b)/e}')"
  gen_rate="$(awk -v a="${gen:-0}" -v b="${prev_gen:-0}" -v e="$elapsed" 'BEGIN {printf "%.2f", (a-b)/e}')"
  kv_pct="$(awk -v k="${kv:-0}" 'BEGIN {printf "%.2f", k*100}')"

  clear 2>/dev/null || true
  printf 'vLLM metrics: %s\n\n' "$URL"
  printf 'Generation tok/s: %s\n' "$gen_rate"
  printf 'Prompt tok/s:     %s\n' "$prompt_rate"
  printf 'KV cache used:    %s%%\n' "$kv_pct"
  printf 'Requests running: %s\n' "${running:-0}"
  printf 'Requests waiting: %s\n' "${waiting:-0}"
  printf 'Prompt tokens:    %s\n' "${prompt:-0}"
  printf 'Generated tokens: %s\n' "${gen:-0}"
  printf 'Prefix queries:   %s\n' "${prefix_queries:-0}"
  printf 'Prefix hits:      %s\n' "${prefix_hits:-0}"
  printf 'Success count:    %s\n\n' "${successes:-0}"
  printf 'Ctrl+C to stop. Set VLLM_METRICS_URL=http://localhost:8002/metrics for balanced.\n'

  prev_prompt="$prompt"
  prev_gen="$gen"
  prev_time="$now"
done
