#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  config/grafana/provisioning/datasources \
  data/huggingface \
  data/open-webui \
  data/postgres \
  data/prometheus \
  data/grafana \
  data/datasets/raw \
  data/datasets/processed \
  evals/results \
  evals/runs \
  logs/litellm \
  models/adapters \
  models/merged

random_hex() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
    return
  fi
  LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 48
  printf '\n'
}

replace_env_placeholder() {
  local key="$1"
  local placeholder="$2"
  local value="$3"
  local tmp

  if [[ ! -f .env ]]; then
    return 0
  fi

  if ! grep -q "^${key}=" .env; then
    printf '%s=%s\n' "$key" "$value" >>.env
    return 0
  fi

  if ! grep -q "^${key}=${placeholder}$" .env; then
    return 0
  fi

  tmp="$(mktemp)"
  awk -v key="$key" -v value="$value" '
    BEGIN { prefix = key "=" }
    index($0, prefix) == 1 { $0 = prefix value }
    { print }
  ' .env >"$tmp"
  mv "$tmp" .env
}

replace_env_placeholder LITELLM_MASTER_KEY "sk-change-me-local-router" "sk-local-$(random_hex)"
replace_env_placeholder LITELLM_SALT_KEY "change-me-32-random-chars-minimum" "$(random_hex)"
replace_env_placeholder OPEN_WEBUI_SECRET_KEY "change-me-open-webui-secret" "$(random_hex)"
replace_env_placeholder POSTGRES_PASSWORD "change-me-postgres" "$(random_hex)"
replace_env_placeholder GRAFANA_ADMIN_PASSWORD "change-me-grafana" "$(random_hex)"
replace_env_placeholder JUPYTER_TOKEN "change-me-jupyter-token" "$(random_hex)"

# vLLM's non-root user is usually UID 2000, GID 0. Group-writable cache dirs
# work with both root and non-root vLLM containers. Keep this non-recursive and
# skip container-owned runtime dirs so rerunning init remains safe after startup.
chmod_if_owned() {
  local path
  for path in "$@"; do
    if [[ -O "$path" ]]; then
      chmod g+rwX "$path"
    fi
  done
}

chmod_if_owned \
  data \
  data/huggingface \
  data/open-webui \
  data/postgres \
  data/datasets \
  data/datasets/raw \
  data/datasets/processed \
  evals \
  evals/results \
  evals/runs \
  logs \
  logs/litellm \
  models \
  models/adapters \
  models/merged


# Container runtime users for writable monitoring volumes.
# Prometheus runs as nobody (65534), Grafana runs as grafana (472).
ensure_owner() {
  local path="$1"
  local owner="$2"
  local service="$3"
  local current_owner

  current_owner="$(stat -c '%u:%g' "$path")"
  if [[ "$current_owner" == "$owner" ]]; then
    return 0
  fi

  if chown -R "$owner" "$path" >/dev/null 2>&1; then
    return 0
  fi

  cat >&2 <<EOF
ERROR: Could not set ownership for $path.

$service must be able to write this directory as UID/GID $owner, but it is
currently owned by UID/GID $current_owner.
Run the ownership fix with sudo, then rerun make init:

  sudo chown -R 65534:65534 data/prometheus
  sudo chown -R 472:472 data/grafana
  make init

Set SKIP_CHOWN=1 only if these directories already have the required ownership.
EOF
  exit 1
}

if [[ "${SKIP_CHOWN:-0}" != "1" ]]; then
  ensure_owner data/prometheus 65534:65534 Prometheus
  ensure_owner data/grafana 472:472 Grafana
fi
