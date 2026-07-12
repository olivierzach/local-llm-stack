# Debugging Checklist

Debug from the bottom of the stack upward. Prove the host, then vLLM, then LiteLLM, then Open WebUI, then monitoring.

## 1. Host Basics

```bash
cd /path/to/local-llm-stack
make check
make gpu-check
```

If Docker requires sudo:

```bash
DOCKER_BIN="sudo docker" make check
DOCKER_BIN="sudo docker" make gpu-check
```

Expected result:

```text
Docker works
Compose works
NVIDIA GPU is visible inside a container
```

Do not continue to vLLM until `make gpu-check` can see the GPU from inside a container.

## 2. Initialization

```bash
make init
```

If monitoring ownership fails, apply the required host ownership:

```bash
sudo chown -R 65534:65534 data/prometheus
sudo chown -R 472:472 data/grafana
make init
```

Prometheus runs as UID `65534`. Grafana runs as UID `472`. Those containers must be able to write their bind-mounted data directories.

## 3. Container State

```bash
make up
make ps
```

If Docker requires sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make up
DOCKER_COMPOSE="sudo docker compose" make ps
```

Look for containers that are restarting, exited, or stuck before serving traffic:

```bash
sudo docker compose logs --tail=200
sudo docker compose logs --tail=200 vllm-fast
sudo docker compose logs --tail=200 litellm
```

## 4. vLLM Direct

Test the model server before testing the router:

```bash
curl -sS http://localhost:8001/v1/models
curl -sS http://localhost:8002/v1/models
```

Expected result:

```text
local-fast
local-balanced
```

If a direct vLLM check fails:

```bash
sudo docker compose logs -f vllm-fast
du -sh data/huggingface
nvidia-smi
```

Common causes:

```text
model is still downloading
model is still loading
HF_TOKEN is missing for a gated model
host NVIDIA driver is too old for the selected image
context length or GPU utilization is too high
```

If `finish_reason` is `length` and the response contains or implies a long thinking block, use `/no_think` in the prompt or send request-level chat template kwargs:

```json
{"chat_template_kwargs": {"enable_thinking": false}}
```

Try smaller memory settings in `.env`:

```text
FAST_MAX_MODEL_LEN=8192
BALANCED_MAX_MODEL_LEN=8192
FAST_GPU_MEMORY_UTILIZATION=0.25
BALANCED_GPU_MEMORY_UTILIZATION=0.50
```

Restart vLLM after changing `.env`:

```bash
sudo docker compose up -d --force-recreate vllm-fast vllm-balanced
```

## 5. LiteLLM Router

After direct vLLM checks pass:

```bash
curl -sS http://localhost:4000/v1/models \
  -H "Authorization: Bearer $(grep '^LITELLM_MASTER_KEY=' .env | cut -d= -f2-)"
```

Then run:

```bash
make smoke
```

If `/v1/models` works but chat fails, LiteLLM is up and the failure is usually between LiteLLM and a vLLM backend. Check:

```bash
sudo docker compose logs --tail=200 litellm
sudo docker compose logs --tail=200 vllm-fast
sed -n '1,220p' config/litellm.yaml
```

Verify that each LiteLLM `api_base` points at the matching Compose service:

```text
local-fast     -> http://vllm-fast:8000/v1
local-balanced -> http://vllm-balanced:8000/v1
local-large    -> http://vllm-large:8000/v1
```

## 6. Open WebUI

After `make smoke` passes, open:

```text
http://<spark-lan-ip>:3000
```

Check the Open WebUI container:

```bash
sudo docker compose logs --tail=200 open-webui
```

The API settings should point at Context Guard inside the Compose network:

```text
OPENAI_API_BASE_URL=http://context-guard:4010/v1
OPENAI_API_KEY=${LITELLM_MASTER_KEY}
ENABLE_OLLAMA_API=false
```

If the UI loads but chat fails, retest Context Guard and `make smoke` before changing browser settings.

## 7. Monitoring

Prometheus:

```bash
curl -sS http://localhost:9090/-/ready
sudo docker compose logs --tail=200 prometheus
```

Grafana:

```text
http://<spark-lan-ip>:3001
```

If either service reports permission errors, reapply ownership:

```bash
sudo chown -R 65534:65534 data/prometheus
sudo chown -R 472:472 data/grafana
sudo docker compose up -d prometheus grafana
```

## 8. Request Path Order

Use this order when isolating a failure:

```text
1. GPU visible in a container
2. vLLM direct /v1/models
3. LiteLLM /v1/models
4. make smoke
5. Open WebUI chat
6. Prometheus and Grafana
```

Do not debug a higher layer until the lower layer is proven healthy.
