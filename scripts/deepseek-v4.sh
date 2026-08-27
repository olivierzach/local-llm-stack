#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_REPO="${DEEPSEEKV4_ENGINE_REPO:-https://github.com/Entrpi/ds4.git}"
ENGINE_REF="${DEEPSEEKV4_ENGINE_REF:-v0.5.3}"
ENGINE_DIR="${DEEPSEEKV4_ENGINE_DIR:-$ROOT/data/deepseek-v4/ds4}"
ENGINE_PATCH="$ROOT/patches/ds4-tokenize-endpoint.patch"
MODEL_REPO="${DEEPSEEKV4_MODEL_REPO:-antirez/deepseek-v4-gguf}"
MODEL_FILE="${DEEPSEEKV4_MODEL_FILE:-DeepSeek-V4-Flash-IQ2XXS-w2Q2K-AProjQ8-SExpQ8-OutQ8-chat-v2-imatrix-0731.gguf}"
DSPARK_REPO="${DEEPSEEKV4_DSPARK_REPO:-bleysg/DeepSeek-V4-Flash-DSpark-drafter-GGUF}"
DSPARK_FILE="${DEEPSEEKV4_DSPARK_FILE:-DSpark-drafter-Q2K-Q8-0731.gguf}"
MODEL_DIR="${DEEPSEEKV4_MODEL_DIR:-$ROOT/models/deepseek-v4}"
LOG_DIR="${DEEPSEEKV4_LOG_DIR:-$ROOT/logs/deepseek-v4}"
PID_FILE="$LOG_DIR/ds4-server.pid"
LOG_FILE="$LOG_DIR/ds4-server.log"
SERVICE_NAME="${DEEPSEEKV4_SYSTEMD_UNIT:-local-deepseek-v4.service}"
PORT="${DEEPSEEKV4_PORT:-8011}"
CONTEXT="${DEEPSEEKV4_MAX_MODEL_LEN:-65536}"
BUILD_JOBS="${DEEPSEEKV4_BUILD_JOBS:-4}"
MODEL_PATH="$MODEL_DIR/$MODEL_FILE"
DSPARK_PATH="$MODEL_DIR/$DSPARK_FILE"

usage() {
  cat <<'EOF'
Usage: scripts/deepseek-v4.sh COMMAND

Commands:
  install  Build the pinned Spark engine and download the weights.
  start    Start the OpenAI-compatible server in the background.
  stop     Stop the server started by this script.
  status   Show server process and endpoint status.
  logs     Follow the server log.
EOF
}

die() {
  echo "deepseek-v4: $*" >&2
  exit 1
}

docker_bridge_host() {
  if command -v docker >/dev/null 2>&1; then
    docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || true
  fi
}

bind_host() {
  local detected
  if [[ -n "${DEEPSEEKV4_BIND_HOST:-}" ]]; then
    printf '%s\n' "$DEEPSEEKV4_BIND_HOST"
    return
  fi
  detected="$(docker_bridge_host)"
  printf '%s\n' "${detected:-127.0.0.1}"
}

verify_host() {
  [[ "$(uname -m)" == "aarch64" ]] || die "requires an aarch64 DGX Spark host"
  command -v nvidia-smi >/dev/null 2>&1 || die "nvidia-smi is not installed"
  command -v nvcc >/dev/null 2>&1 || die "CUDA nvcc is not installed"
  command -v git >/dev/null 2>&1 || die "git is not installed"
  command -v curl >/dev/null 2>&1 || die "curl is not installed"

  local compute_cap
  compute_cap="$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader 2>/dev/null | head -1)"
  [[ "$compute_cap" == "12.1" ]] || die "requires GB10 compute capability 12.1; found ${compute_cap:-unknown}"
}

download_file() {
  local repo="$1"
  local filename="$2"
  local destination="$3"
  local url="https://huggingface.co/$repo/resolve/main/$filename"

  mkdir -p "$MODEL_DIR"
  if [[ -f "$destination" ]]; then
    local remote_size local_size
    remote_size="$(curl -sI -L "$url" | awk -F': ' 'tolower($1)=="content-length" {size=$2+0} END {print size}')"
    local_size="$(stat -c%s "$destination")"
    if [[ -n "$remote_size" && "$remote_size" != "0" && "$remote_size" == "$local_size" ]]; then
      echo "deepseek-v4: already downloaded $filename"
      return
    fi
    echo "deepseek-v4: resuming $filename ($local_size bytes present)"
  else
    echo "deepseek-v4: downloading $filename"
  fi

  if command -v docker >/dev/null 2>&1; then
    echo "deepseek-v4: downloading $filename with Hugging Face Xet"
    rm -f "$destination"
    local compose_cmd
    read -r -a compose_cmd <<<"${DOCKER_COMPOSE:-docker compose}"
    "${compose_cmd[@]}" run \
      --rm \
      --no-deps \
      -v "$MODEL_DIR:/models" \
      --entrypoint python3 \
      model-cache \
      - "$repo" "$filename" <<'PY'
import sys
from huggingface_hub import hf_hub_download

hf_hub_download(
    repo_id=sys.argv[1],
    filename=sys.argv[2],
    local_dir="/models",
)
PY
    [[ -f "$destination" ]] || die "Xet download did not produce $destination"
    return
  fi
  curl -L --fail --progress-bar -C - -o "$destination" "$url"
}

install_engine() {
  verify_host

  local free_gib
  free_gib="$(df -BG "$ROOT" | awk 'NR==2 {gsub("G", "", $4); print $4}')"
  if [[ ! -f "$MODEL_PATH" && "$free_gib" -lt 110 ]]; then
    die "at least 110 GiB free disk is required; found ${free_gib} GiB"
  fi

  mkdir -p "$(dirname "$ENGINE_DIR")" "$MODEL_DIR" "$LOG_DIR"
  if [[ ! -d "$ENGINE_DIR/.git" ]]; then
    git clone --depth 1 --branch "$ENGINE_REF" "$ENGINE_REPO" "$ENGINE_DIR"
  else
    git -C "$ENGINE_DIR" fetch --depth 1 origin "$ENGINE_REF"
    git -C "$ENGINE_DIR" checkout --detach --force FETCH_HEAD
  fi

  [[ -f "$ENGINE_PATCH" ]] || die "missing DS4 integration patch: $ENGINE_PATCH"
  if git -C "$ENGINE_DIR" apply --reverse --check "$ENGINE_PATCH" 2>/dev/null; then
    echo "deepseek-v4: tokenizer endpoint patch already applied"
  elif git -C "$ENGINE_DIR" apply --check "$ENGINE_PATCH"; then
    git -C "$ENGINE_DIR" apply "$ENGINE_PATCH"
  else
    die "DS4 tokenizer endpoint patch does not apply to $ENGINE_REF"
  fi

  make -C "$ENGINE_DIR" cuda -j"$BUILD_JOBS" CUDA_ARCH=sm_121
  [[ -x "$ENGINE_DIR/ds4-server" ]] || die "build did not produce $ENGINE_DIR/ds4-server"
  download_file "$MODEL_REPO" "$MODEL_FILE" "$MODEL_PATH"
  download_file "$DSPARK_REPO" "$DSPARK_FILE" "$DSPARK_PATH"
  echo "deepseek-v4: install complete"
}

running_pid() {
  local pid
  systemctl --user is-active --quiet "$SERVICE_NAME" || return 1
  pid="$(systemctl --user show "$SERVICE_NAME" --property=MainPID --value)"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
  printf '%s\n' "$pid"
}

start_server() {
  [[ -x "$ENGINE_DIR/ds4-server" ]] || die "engine is not installed; run make deepseekv4-install"
  [[ -f "$MODEL_PATH" ]] || die "base weights are missing; run make deepseekv4-install"
  [[ -f "$DSPARK_PATH" ]] || die "DSpark drafter is missing; run make deepseekv4-install"

  local pid host available_gib
  if pid="$(running_pid)"; then
    echo "deepseek-v4: already running as pid $pid"
    return
  fi
  rm -f "$PID_FILE"

  available_gib="$(awk '/MemAvailable:/ {printf "%d", $2 / 1024 / 1024}' /proc/meminfo)"
  if [[ "$available_gib" -lt 100 ]]; then
    die "needs about 100 GiB available unified memory; only ${available_gib} GiB is available. Stop other model servers first"
  fi

  host="$(bind_host)"
  mkdir -p "$LOG_DIR"
  : >"$LOG_FILE"
  echo "deepseek-v4: starting on http://$host:$PORT with context $CONTEXT"
  systemctl --user reset-failed "$SERVICE_NAME" 2>/dev/null || true
  systemd-run \
    --user \
    --unit "$SERVICE_NAME" \
    --collect \
    --property "WorkingDirectory=$ROOT" \
    --property "StandardOutput=append:$LOG_FILE" \
    --property "StandardError=append:$LOG_FILE" \
    --setenv "DS4_CONT_MTP_MODE=2" \
    --setenv "DS4_CONT_DSPARK=1" \
    --setenv "DS4_DSPARK_MODEL=$DSPARK_PATH" \
    "$ENGINE_DIR/ds4-server" \
      --cuda \
      -m "$MODEL_PATH" \
      -c "$CONTEXT" \
      --host "$host" \
      --port "$PORT"
  pid="$(systemctl --user show "$SERVICE_NAME" --property=MainPID --value)"
  printf '%s\n' "$pid" >"$PID_FILE"

  local attempt
  for attempt in $(seq 1 90); do
    if curl -fsS "http://$host:$PORT/v1/models" >/dev/null 2>&1; then
      echo "deepseek-v4: ready as pid $pid"
      return
    fi
    if ! systemctl --user is-active --quiet "$SERVICE_NAME"; then
      tail -40 "$LOG_FILE" >&2 || true
      rm -f "$PID_FILE"
      die "server exited during startup"
    fi
    sleep 2
  done

  tail -40 "$LOG_FILE" >&2 || true
  die "server did not become ready within 180 seconds"
}

stop_server() {
  local pid
  if ! pid="$(running_pid)"; then
    rm -f "$PID_FILE"
    echo "deepseek-v4: not running"
    return
  fi

  systemctl --user stop "$SERVICE_NAME"
  rm -f "$PID_FILE"
  echo "deepseek-v4: stopped"
}

show_status() {
  local pid host
  host="$(bind_host)"
  if pid="$(running_pid)"; then
    echo "deepseek-v4: running as pid $pid"
    curl -fsS "http://$host:$PORT/v1/models"
    echo
  else
    echo "deepseek-v4: not running"
    return 1
  fi
}

command="${1:-}"
case "$command" in
  install) install_engine ;;
  start) start_server ;;
  stop) stop_server ;;
  status) show_status ;;
  logs)
    mkdir -p "$LOG_DIR"
    touch "$LOG_FILE"
    tail -f "$LOG_FILE"
    ;;
  *) usage; exit 2 ;;
 esac
