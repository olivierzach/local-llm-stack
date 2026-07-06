#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 org/model-id [org/model-id ...]" >&2
  exit 2
fi

read -r -a compose_cmd <<<"${DOCKER_COMPOSE:-docker compose}"

for model in "$@"; do
  echo "== Downloading $model into data/huggingface =="
  "${compose_cmd[@]}" run --rm --no-deps --entrypoint python3 model-cache - "$model" <<'PY'
import sys
from huggingface_hub import snapshot_download

model_id = sys.argv[1]
snapshot_download(repo_id=model_id)
print(f"cached {model_id}")
PY
done
