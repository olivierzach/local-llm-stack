# Model Zoo

These optional services let you cache and test larger models without making them part of the default startup path.

The default `make up` still starts only `vllm-fast`, `vllm-balanced`, LiteLLM, Open WebUI, Postgres, Prometheus, and Grafana.

## Cache vs Serve

Downloading a model stores files on disk under:

```text
data/huggingface
```

Serving a model starts a vLLM container, loads those files into GPU-addressable memory, allocates KV cache, and exposes an API port.

Cache many models if disk allows it. Run only the models that fit in memory at the same time.

## Optional Models

| Alias | Service | Default model | Port | Profile |
|---|---|---|---|---|
| `local-large` | `vllm-large` | `Qwen/Qwen3-32B` | `8003` | `large` |
| `local-qwen30-a3b` | `vllm-qwen30a3b` | `Qwen/Qwen3-30B-A3B-Instruct-2507` | `8004` | `qwen30a3b` |
| `local-deepseek-r1-qwen32b` | `vllm-deepseek32b` | `deepseek-ai/DeepSeek-R1-Distill-Qwen-32B` | `8005` | `deepseek32b` |
| `local-mistral-small` | `vllm-mistral24b` | `mistralai/Mistral-Small-3.2-24B-Instruct-2506` | `8006` | `mistral24b` |
| `local-vision` | `vllm-vision` | `Qwen/Qwen3-VL-4B-Instruct` | `8008` | `vision` |
| `local-gpt-oss-120b` | `vllm-gptoss120b` | `openai/gpt-oss-120b` | `8009` | `gptoss120b` |

## Download Models

Download into the shared Hugging Face cache without starting a long-running vLLM server:

```bash
make download-qwen32
make download-qwen30
make download-deepseek32
make download-mistral24
make download-gptoss120
make download-vision
```

For any other Hugging Face repo:

```bash
make download-model MODEL=org/model-id
```

If Docker requires sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make download-qwen32
```

## Start One Large Candidate

Stop other vLLM services first when testing large models:

```bash
sudo docker compose stop vllm-fast vllm-balanced vllm-large vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b vllm-gptoss120b
```

Then start one candidate:

```bash
make large-up
make qwen30-up
make deepseek32-up
make mistral24-up
make gptoss120-up
```

or with sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make qwen30-up
```

Watch memory while it loads:

```bash
watch -n 2 'free -h; echo; nvidia-smi; echo; sudo docker compose ps'
```

## Test

Test the direct vLLM endpoint first:

```bash
curl -sS http://localhost:8004/v1/models
```

Then test through LiteLLM:

```bash
MODEL=local-qwen30-a3b make smoke
```

Change the model alias and port for the candidate you started.

`local-gpt-oss-120b` is configured with a conservative `GPTOSS120B_MAX_MODEL_LEN=8192`
and `GPTOSS120B_MAX_NUM_SEQS=1` default because the MXFP4 weights target an
80 GB-class single GPU and KV cache still consumes additional memory during
inference. Increase context only after the model loads and a short inference
passes with memory headroom.
