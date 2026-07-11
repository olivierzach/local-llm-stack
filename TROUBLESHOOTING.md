# Troubleshooting Notes

## 2026-07-02: Containers Created, `/v1/models` Works, Chat Returns 500

Observed after first startup:

```text
make smoke
== Models ==
{"data":[{"id":"local-fast" ...},{"id":"local-balanced" ...},{"id":"local-coder" ...},{"id":"local-large" ...}],"object":"list"}

== Chat ==
curl: (22) The requested URL returned error: 500
make: *** [Makefile:37: smoke] Error 22
```

What this means:

- LiteLLM is reachable on port `4000`; otherwise `/v1/models` would fail.
- The router config loaded; otherwise the model aliases would not list.
- The failure is probably between LiteLLM and a vLLM backend during generation, or vLLM is still loading the model.
- Docker showed containers as `Created` in the startup output, not necessarily `Running` or healthy.

First checks:

```bash
cd /path/to/local-llm-stack
DOCKER_COMPOSE="sudo docker compose" make ps
DOCKER_COMPOSE="sudo docker compose" make logs
```

Focused logs:

```bash
sudo docker compose logs --tail=200 litellm
sudo docker compose logs --tail=200 vllm-fast
sudo docker compose logs --tail=200 vllm-balanced
```

Backend reachability checks from the Spark:

```bash
curl -sS http://localhost:8001/v1/models
curl -sS http://localhost:8002/v1/models
```

If vLLM is still downloading or loading models, wait and rerun:

```bash
make smoke
```

If vLLM logs show memory or context-length failures, lower the defaults in `.env`:

```text
FAST_MAX_MODEL_LEN=8192
BALANCED_MAX_MODEL_LEN=8192
FAST_GPU_MEMORY_UTILIZATION=0.25
BALANCED_GPU_MEMORY_UTILIZATION=0.50
```

Then restart:

```bash
DOCKER_COMPOSE="sudo docker compose" make down
DOCKER_COMPOSE="sudo docker compose" make up
```

If `local-fast` works but `local-balanced` fails, keep `local-fast` running first and start larger models one at a time.

## vLLM Restarts With Driver Requirement Error

Observed log:

```text
ERROR: This container was built for NVIDIA Driver Release 595.45 or later, but
       version 580.159.03 was detected and compatibility mode is UNAVAILABLE.
/opt/nvidia/nvidia_entrypoint.sh: line 55: exec: --: invalid option
```

Cause:

- The `26.03.post1` NVIDIA vLLM container requires a newer host NVIDIA driver than this Spark currently has.
- The Compose command also started with `--model`, so the image entrypoint tried to execute an option instead of the vLLM API server.

Fix applied:

- Pin `VLLM_IMAGE=nvcr.io/nvidia/vllm:25.09-py3` in `.env` and `.env.example`.
- Start vLLM with `python3 -m vllm.entrypoints.openai.api_server ...` in `docker-compose.yml`.

Restart after the fix:

```bash
cd /path/to/local-llm-stack
sudo docker compose down
sudo docker compose pull vllm-fast vllm-balanced
sudo docker compose up -d
sudo docker compose logs -f vllm-fast
```

Then test:

```bash
curl -sS http://localhost:8001/v1/models
make smoke
```

## 2026-07-02: First Successful Smoke Test

Command:

```bash
make smoke
```

Result:

```text
== Models ==
local-fast, local-balanced, local-coder, local-large listed

== Chat ==
assistant content: local stack ok
```

Meaning:

- LiteLLM is reachable on port `4000`.
- LiteLLM loaded the model aliases.
- vLLM `local-fast` loaded `Qwen/Qwen3-4B-Instruct-2507`.
- LiteLLM can route chat completions to vLLM.
- The end-to-end API path works for `local-fast`.

## Prometheus Restarts With `queries.active: permission denied`

Observed log:

```text
Error opening query log file file=/prometheus/queries.active err="open /prometheus/queries.active: permission denied"
panic: Unable to create mmap-ed active query log
```

Cause:

- `./data/prometheus` is bind-mounted to `/prometheus`.
- The Prometheus container process must be able to write there.
- If the host directory is owned only by your host user, the container user cannot create `queries.active`.

Fix:

```bash
cd /path/to/local-llm-stack
sudo chown -R 65534:65534 data/prometheus
sudo chown -R 472:472 data/grafana
sudo docker compose up -d prometheus grafana
```

The Compose file pins Prometheus to UID `65534` and Grafana to UID `472` so ownership is predictable.

## Open WebUI Error: `auto` Tool Choice Requires vLLM Flags

Observed error in Open WebUI:

```text
litellm.BadRequestError: OpenAIException - "auto" tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set. Received Model Group=local-fast
```

Cause:

- Open WebUI sent an OpenAI-style request with `tool_choice: "auto"`.
- vLLM rejects that parameter unless the server is started with tool-call parsing enabled.

Fix applied in `docker-compose.yml` for each vLLM service:

```text
--enable-auto-tool-choice
--tool-call-parser hermes
```

Restart vLLM after the fix:

```bash
cd /path/to/local-llm-stack
sudo docker compose up -d --force-recreate vllm-fast
make smoke
```

If Open WebUI still sends tool settings that the selected model cannot satisfy, disable tools/features in that Open WebUI chat or use a model with stronger tool-call tuning.
