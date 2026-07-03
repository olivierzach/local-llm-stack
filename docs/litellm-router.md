# LiteLLM Router

LiteLLM is the router and API gateway.

It exposes one OpenAI-compatible API:

```text
http://192.168.1.31:4000/v1
```

It maps friendly model names to backend servers.

In `config/litellm.yaml`:

```text
local-fast     -> http://vllm-fast:8000/v1
local-balanced -> http://vllm-balanced:8000/v1
local-coder    -> http://vllm-balanced:8000/v1
local-large    -> http://vllm-large:8000/v1
```

## Why Use A Router

Without LiteLLM, every client would need to know every backend URL.

With LiteLLM, clients only need:

```text
base_url: http://192.168.1.31:4000/v1
model: local-fast
```

That lets you change backend containers later without changing every client.

## Important Distinction

`/v1/models` can work even if a backend is down, because LiteLLM can list configured aliases.

Generation requires the backend to be alive.

So this state is possible:

```text
/v1/models works
/v1/chat/completions returns 500
```

That usually means LiteLLM is alive but vLLM is not ready.

## Test LiteLLM

```bash
make smoke
```

Or manually:

```bash
curl -sS http://localhost:4000/v1/models   -H "Authorization: Bearer $LITELLM_MASTER_KEY"
```
