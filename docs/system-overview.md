# System Overview

This project turns the Spark into a private LLM server.

A browser or any other approved client is just a viewer/client. The Spark runs the web UI, router, model servers, model files, and GPU inference.

```text
Browser or app
  -> Open WebUI
  -> LiteLLM
  -> vLLM
  -> NVIDIA GPU / unified memory
```

## What Each Layer Does

Open WebUI is the browser interface. It is the ChatGPT-like page you open at port `3000`.

LiteLLM is the API router. It gives you one OpenAI-compatible API at port `4000` and maps model names like `local-fast` to backend servers.

vLLM is the inference server. It loads the model weights, manages the tokenizer, uses the GPU, schedules requests, and generates tokens.

Postgres stores LiteLLM state. It does not store model weights.

Prometheus collects metrics. Grafana displays metrics. These are useful later, but they do not need to work before chat works.

The training container is separate. It is for fine-tuning experiments, not normal chat serving.

## The First Thing To Get Working

Focus on `vllm-fast` first.

```bash
curl -sS http://localhost:8001/v1/models
make smoke
```

If those work, the basic serving path works.
