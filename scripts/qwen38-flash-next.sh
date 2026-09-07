#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/config/qwen38-pins.env"
RUNTIME="$ROOT/data/qwen38-flash-next"
RECIPE="$RUNTIME/recipe"
CONTAINER=local-qwen38-flash-next
PATCH="$ROOT/patches/qwen38-runtime.patch"
read -r -a COMPOSE <<<"${DOCKER_COMPOSE:-docker compose}"

die() { echo "qwen38: $*" >&2; exit 1; }
running() { [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" == true ]]; }

configure() {
  # Only named recipe settings reach the launcher; its saved command must not contain secrets.
  export HF_HOME="$ROOT/data/huggingface"
  export TP1_VLLM_CACHE="$RUNTIME/vllm-cache"
  export TP1_MODEL_REVISION="$QWEN38_MODEL_REVISION"
  export TP1_MODEL_ID="$QWEN38_MODEL"
  export TP1_CONTAINER_NAME="$CONTAINER"
  export IMAGE="$QWEN38_IMAGE" SERVED_MODEL_NAME=local-qwen38-flash-next
  export PORT="${QWEN38_PORT:-8012}"
  export TP1_BIND_HOST="${QWEN38_BIND_HOST:-$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')}"
  export MAX_MODEL_LEN="${QWEN38_MAX_MODEL_LEN:-262144}" YARN=0
  export MTP_NUM_SPECULATIVE_TOKENS="${QWEN38_MTP_TOKENS:-3}"
  export KV_TARGET_GIB="${QWEN38_KV_TARGET_GIB:-20}" HOST_RESERVE_GIB="${QWEN38_HOST_RESERVE_GIB:-26}"
  export HOST_SLACK_GIB=5 KV_CACHE_DTYPE="${QWEN38_KV_CACHE_DTYPE:-fp8}"
  export MAMBA_SSM_CACHE_DTYPE="${QWEN38_SSM_DTYPE-bfloat16}"
  export MAX_NUM_SEQS="${QWEN38_MAX_NUM_SEQS:-4}" MAX_NUM_BATCHED_TOKENS="${QWEN38_BATCHED_TOKENS:-2048}"
  export CUDAGRAPH_CAPTURE_SIZES=auto COMPILATION_MODE=0 MTP_K_SCHEDULE=""
  export EXTRA_DOCKER_ARGS="-e VLLM_USE_V2_MODEL_RUNNER=1"
  export EXTRA_VLLM_ARGS="--revision $QWEN38_MODEL_REVISION --tokenizer-revision $QWEN38_MODEL_REVISION"
  export MTP_DRAFT_VOCAB="${QWEN38_DRAFT_VOCAB:-}"
  export REQUIRE_IDLE_GPU=true PLE_OFFLOAD=true GPU_MEMORY_UTILIZATION="" KV_CACHE_MEMORY=""
  export HF_TOKEN=""
  unset HUGGING_FACE_HUB_TOKEN
  [[ "$ROOT" =~ ^[a-zA-Z0-9_./-]+$ ]] || die "upstream launcher requires a path without shell metacharacters or spaces"
  [[ "$TP1_BIND_HOST" =~ ^[0-9.]+$ ]] || die "QWEN38_BIND_HOST must be an IPv4 address"
  [[ "$PORT" =~ ^[0-9]+$ && "$PORT" -gt 0 && "$PORT" -le 65535 ]] || die "invalid QWEN38_PORT"
  [[ "$MAX_MODEL_LEN" =~ ^[0-9]+$ && "$MAX_MODEL_LEN" -ge 4096 && "$MAX_MODEL_LEN" -le 262144 ]] || die "context must be 4096..262144 (native rope)"
  [[ "$HOST_RESERVE_GIB" =~ ^[0-9]+$ && "$HOST_RESERVE_GIB" -ge 26 ]] || die "host reserve must be at least 26 GiB"
  [[ -z "$MTP_DRAFT_VOCAB" || "$MTP_DRAFT_VOCAB" =~ ^[a-zA-Z0-9_./-]+$ ]] || die "invalid draft vocabulary path"
}

verify_recipe() {
  [[ -d "$RECIPE/.git" ]] || die "run make qwen38-install first"
  [[ "$(git -C "$RECIPE" rev-parse HEAD)" == "$QWEN38_RECIPE_REF" ]] || die "recipe revision differs from pinned revision"
  git -C "$RECIPE" apply --reverse --check "$PATCH" 2>/dev/null || die "runtime patch missing; run make qwen38-install"
}

install() {
  running && die "stop Qwen before preparing its mounted recipe files"
  mkdir -p "$RUNTIME" "$ROOT/logs/qwen38-flash-next"
  if [[ ! -d "$RECIPE/.git" ]]; then
    git clone https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark.git "$RECIPE"
    git -C "$RECIPE" checkout --detach "$QWEN38_RECIPE_REF"
  fi
  [[ "$(git -C "$RECIPE" rev-parse HEAD)" == "$QWEN38_RECIPE_REF" ]] || die "refusing to replace another recipe revision"
  if ! git -C "$RECIPE" apply --reverse --check "$PATCH" 2>/dev/null; then
    git -C "$RECIPE" apply --check "$PATCH"
    git -C "$RECIPE" apply "$PATCH"
  fi
  [[ -f "$RECIPE/.env" ]] || cp "$RECIPE/.env.sample" "$RECIPE/.env"
  [[ -e "$RECIPE/logs" ]] || ln -s "$ROOT/logs/qwen38-flash-next" "$RECIPE/logs"
  docker pull "$QWEN38_IMAGE"
  "${COMPOSE[@]}" run --rm --no-deps --name local-qwen38-download \
    -e HF_XET_NUM_CONCURRENT_RANGE_GETS=2 model-cache -c \
    'import sys; from huggingface_hub import snapshot_download; print(snapshot_download(sys.argv[1], revision=sys.argv[2], max_workers=2))' \
    "$QWEN38_MODEL" "$QWEN38_MODEL_REVISION"
  python3 "$ROOT/scripts/qwen38-artifacts.py"
  echo "qwen38: pinned image, recipe and checkpoint installed"
}

stop() {
  if [[ -f "$RECIPE/stop.sh" ]]; then
    TP1_CONTAINER_NAME="$CONTAINER" bash "$RECIPE/stop.sh"
  elif running; then
    die "recipe missing; cannot safely stop its watchdog"
  fi
}

probe() {
  curl -fsS --max-time 5 "http://$TP1_BIND_HOST:$PORT/health" >/dev/null || return 1
  "$ROOT/.venv/bin/python" "$ROOT/scripts/qwen38-verify.py" probe
}

start() {
  configure
  verify_recipe
  [[ -x "$ROOT/.venv/bin/python" ]] || die "create .venv and install .[test] as documented before switching models"
  "$ROOT/.venv/bin/python" -c 'import requests'
  if running; then probe; return; fi
  if systemctl --user is-active --quiet "${DEEPSEEKV4_SYSTEMD_UNIT:-local-deepseek-v4.service}"; then
    die "DS4 is resident; use make qwen38-up for the managed switch"
  fi
  # Timeout covers setup and startup; failure also removes the watchdog/container.
  if ! timeout --signal=TERM --kill-after=30 "${QWEN38_STARTUP_TIMEOUT:-1800}" bash "$RECIPE/start.sh"; then
    stop
    die "startup failed; use make deepseekv4-up to restore DS4"
  fi
  if ! probe; then
    stop
    die "generation probe failed; use make deepseekv4-up to restore DS4"
  fi
}

case "${1:-}" in
  check) configure; verify_recipe; "$ROOT/.venv/bin/python" -c 'import requests'; python3 "$ROOT/scripts/qwen38-artifacts.py" ;;
  install) install ;;
  start) start ;;
  stop) stop ;;
  status) configure; running || die "not running"; curl -fsS --max-time 10 "http://$TP1_BIND_HOST:$PORT/v1/models"; echo ;;
  probe) configure; probe ;;
  logs) docker logs --tail 100 -f "$CONTAINER" ;;
  dry-run) configure; verify_recipe; running && die "stop Qwen before regenerating mounted patches"; bash "$RECIPE/start.sh" --no-launch ;;
  *) echo "Usage: $0 {install|check|start|stop|status|probe|logs|dry-run}"; exit 2 ;;
esac
