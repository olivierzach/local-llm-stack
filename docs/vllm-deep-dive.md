# vLLM Deep Dive

vLLM is the model server in this stack. It loads model weights, exposes an OpenAI-compatible API, schedules inference work on the GPU, and streams generated tokens back to LiteLLM or direct clients.

The default stack starts one vLLM service:

```text
vllm-fast      -> local-fast      -> port 8001
```

The balanced service starts only with `make balanced-up` or the `balanced` Compose profile:

```text
vllm-balanced  -> local-balanced  -> port 8002
```

The optional large service starts only with the `large` Compose profile:

```text
vllm-large     -> local-large     -> port 8003
```

## Startup Path

Each vLLM service uses the shared Compose block in `docker-compose.yml`:

```text
image: nvcr.io/nvidia/vllm:25.09-py3
gpus: all
ipc: host
shm_size: 16gb
HF_HOME: /home/vllm/.cache/huggingface
```

The command starts the OpenAI-compatible API server:

```text
python3 -m vllm.entrypoints.openai.api_server
```

The important arguments are:

```text
--model                  Hugging Face model repo or local model path
--served-model-name      API model name returned by /v1/models
--host 0.0.0.0           listen inside the container
--port 8000              container port used by LiteLLM
--max-model-len          context length limit
--gpu-memory-utilization fraction of GPU memory vLLM may reserve
```

## Model Names

There are two names to keep straight.

The `--model` value is the real model source:

```text
Qwen/Qwen3-4B-Instruct-2507
Qwen/Qwen3-14B
Qwen/Qwen3-32B
```

The `--served-model-name` value is the local API name:

```text
local-fast
local-balanced
local-large
```

LiteLLM routes requests by those local names. For example, `local-fast` points to:

```text
http://vllm-fast:8000/v1
```

## Downloads And Cache

vLLM downloads missing Hugging Face model files on first start. The cache is bind-mounted so downloads survive container restarts:

```text
host:      ./data/huggingface
container: /home/vllm/.cache/huggingface
```

Set `HF_TOKEN` in `.env` when using gated or private models.

Useful checks:

```bash
sudo docker compose logs -f vllm-fast
du -sh data/huggingface
curl -sS http://localhost:8001/v1/models
```

## Memory Controls

The main memory controls live in `.env`:

```text
FAST_MAX_MODEL_LEN=32768
FAST_GPU_MEMORY_UTILIZATION=0.30
BALANCED_MAX_MODEL_LEN=32768
BALANCED_GPU_MEMORY_UTILIZATION=0.58
LARGE_MAX_MODEL_LEN=16384
LARGE_GPU_MEMORY_UTILIZATION=0.88
```

Lower `*_MAX_MODEL_LEN` if vLLM fails while allocating KV cache. Lower `*_GPU_MEMORY_UTILIZATION` if multiple backends compete for memory.

Start with `vllm-fast`. Add `vllm-balanced` only after fast is healthy. Stop a smaller backend before starting `vllm-large` if the host is under memory pressure.

## Tool Choice Flags

Open WebUI can send OpenAI-compatible tool fields such as:

```json
{"tool_choice": "auto"}
```

vLLM rejects that field unless tool parsing is enabled. This stack starts each vLLM service with:

```text
--enable-auto-tool-choice
--tool-call-parser hermes
```

That lets vLLM accept and parse tool-call-shaped requests. It does not make vLLM execute tools; the application layer is still responsible for tool execution.

## Thinking Mode

Qwen3 models can spend much of the output budget on a thinking block before producing the visible answer. The pinned NVIDIA vLLM image used by this stack does not support the newer `--default-chat-template-kwargs` server flag, so thinking mode is controlled at request time for now.

Use one of these when concise complete answers matter:

```text
/no_think
```

or send request-level chat template kwargs:

```json
{"chat_template_kwargs": {"enable_thinking": false}}
```

Do not add `--default-chat-template-kwargs` to `docker-compose.yml` unless the selected vLLM image supports it.

## LoRA

`vllm-balanced` starts with:

```text
--enable-lora
```

LoRA adapters should be stored under:

```text
./models/adapters
```

Merged or exported models should be stored under:

```text
./models/merged
```

Keep adapter loading changes isolated to the vLLM service that needs them. Do not change the LiteLLM aliases until the backend reports the intended served model name from `/v1/models`.

## Health Checks

Direct vLLM checks should pass before debugging LiteLLM or Open WebUI:

```bash
curl -sS http://localhost:8001/v1/models
curl -sS http://localhost:8002/v1/models
sudo docker compose logs --tail=200 vllm-fast
sudo docker compose logs --tail=200 vllm-balanced
```

Common startup states:

```text
downloading files     -> wait and watch data/huggingface
loading weights       -> memory use rises
allocating KV cache   -> failures usually need lower context or utilization
serving API           -> /v1/models returns the served model name
```

After direct vLLM checks pass, run the routed smoke test:

```bash
make smoke
```
