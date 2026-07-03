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
  logs/litellm \
  models/adapters \
  models/merged

# vLLM's non-root user is usually UID 2000, GID 0. Group-writable cache dirs
# work with both root and non-root vLLM containers.
chmod -R g+rwX data models evals logs


# Container runtime users for writable monitoring volumes.
# Prometheus runs as nobody (65534), Grafana runs as grafana (472).
if command -v chown >/dev/null 2>&1; then
  if [[ "${SKIP_CHOWN:-0}" != "1" ]]; then
    chown -R 65534:65534 data/prometheus 2>/dev/null || true
    chown -R 472:472 data/grafana 2>/dev/null || true
  fi
fi
