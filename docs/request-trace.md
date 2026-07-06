# Request Trace Lab

This lab follows one chat completion through the local stack:

```text
client -> Open WebUI -> LiteLLM -> vLLM -> model weights
```

Open WebUI is the browser application. It stores chats and sends OpenAI-shaped
requests to LiteLLM. LiteLLM is the router and policy layer. vLLM is the model
server that owns scheduling, KV cache, batching, and token generation.

## Tooling Setup

Use the repo-local tool environment for request tracing, load tests, and evals:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r tools/requirements.txt
```

The tools default to `http://localhost:4000/v1` for LiteLLM and use
`LITELLM_MASTER_KEY` from the environment when it is set.

## Trace One Prompt

Run the same prompt directly against vLLM and through LiteLLM:

```bash
set -a; source .env; set +a
python scripts/request-trace.py --model local-fast --prompt "Reply with exactly: trace ok"
```

For `local-fast`, the direct path defaults to `http://localhost:8001/v1`.
For `local-balanced`, it defaults to `http://localhost:8002/v1`.

The output records each path, URL, HTTP status, latency, finish reason, usage,
and response text. A direct vLLM failure means the model server needs attention
first. A direct success plus a LiteLLM failure usually points to router config,
container networking, auth, or startup ordering.

## Where Config Applies

vLLM config is applied in `docker-compose.yml`. The important fields are the
real model source, local served model name, max context, GPU memory fraction,
and optional LoRA module mapping.

LiteLLM config is applied in `config/litellm.yaml`. Each `model_name` is the
alias clients request. Its `api_base` points at a vLLM service inside the
Compose network.

Open WebUI config is applied by environment variables in the `open-webui`
service. It sees LiteLLM as an OpenAI-compatible API and does not need to know
which vLLM backend will serve a request.

## OpenAI-Compatible Means

OpenAI-compatible means the HTTP route and JSON shape match common OpenAI API
contracts such as `/v1/models` and `/v1/chat/completions`. It does not mean the
server is OpenAI-hosted, supports every optional parameter, has the same model
behavior, or implements tool execution. In this stack, LiteLLM and vLLM accept
the OpenAI-shaped request while local containers perform routing and inference.
