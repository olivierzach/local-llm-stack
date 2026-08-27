# Model Zoo

These optional services let you cache and test larger models without making them part of the default startup path.

The default `make up` starts `vllm-fast`, LiteLLM, Context Guard, Open WebUI, Postgres, Prometheus, and Grafana. Start `vllm-balanced` explicitly with `make balanced-up`.

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
| `local-laguna-s-2.1` | `vllm-lagunas21` | `poolside/Laguna-S-2.1-NVFP4` | `8010` | `lagunas21` |
| `local-deepseek-v4-flash` | host `ds4-server` | DeepSeek-V4-Flash-0731 IQ2 | `8011` | host-native |

## Download Models

Download into the shared Hugging Face cache without starting a long-running vLLM server:

```bash
make download-qwen32
make download-qwen30
make download-deepseek32
make download-mistral24
make download-gptoss120
make download-lagunas21
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

## DeepSeek V4 Flash On One Spark

The full 284B V4-Flash checkpoint does not fit in one Spark's usable memory at
FP4/FP8 precision. This route uses the Spark-tuned `Entrpi/ds4` engine and an
IQ2 mixed quantization instead. The base and speculative drafter downloads use
about 94 GB (roughly 88 GiB) of disk. V4-Pro does not fit on one Spark.

Install the pinned engine and weights:

```bash
make deepseekv4-install
```

Stop all vLLM model services, start the host-native engine, and refresh the
LiteLLM and Context Guard containers:

```bash
make deepseekv4-up
make deepseekv4-smoke
```

The routed alias is `local-deepseek-v4-flash`. The pinned engine and model were
load-tested on one DGX Spark at 32768, 49152, and 65536 tokens. The engine
defaults to a 65536-token shared context budget and binds only to Docker's host
bridge. Use
`DEEPSEEKV4_BIND_HOST=0.0.0.0` only when direct, unauthenticated LAN access is
intentional. Stop it with `make deepseekv4-down`; logs are in
`logs/deepseek-v4/ds4-server.log`.

To return to the conservative 32768-token baseline, update the ignored runtime
configuration and both terminal client limits, then restart:

```bash
perl -0pi -e 's/^DEEPSEEKV4_MAX_MODEL_LEN=.*$/DEEPSEEKV4_MAX_MODEL_LEN=32768/m' .env
perl -0pi -e 's/("local-deepseek-v4-flash"\s*:\s*\{.*?"context"\s*:\s*)65536/${1}32768/s' config/opencode/opencode.json
perl -0pi -e 's/(name: local-deepseek-v4-flash\n\s+max_input_tokens: )65536/${1}32768/' config/aichat/config.yaml
perl -0pi -e 's/^compress_threshold: .*$/compress_threshold: 28000/m' config/aichat/config.yaml
make deepseekv4-down
make deepseekv4-up
```

Verify the rollback with `make deepseekv4-status` and a small request through
Context Guard.

## Start One Large Candidate

Stop other vLLM services first when testing large models:

```bash
sudo docker compose stop vllm-fast vllm-balanced vllm-large vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b vllm-gptoss120b vllm-lagunas21
```

Then start one candidate:

```bash
make large-up
make qwen30-up
make deepseek32-up
make mistral24-up
make gptoss120-up
make lagunas21-up
```

or with sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make qwen30-up
DOCKER_COMPOSE="sudo docker compose" make lagunas21-up
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

`local-laguna-s-2.1` targets Poolside Laguna S 2.1 NVFP4, the single-DGX-Spark-oriented checkpoint. Poolside lists the base model as a 118B total / 8B-active MoE for agentic coding with a 1M-token native context, while the NVFP4 checkpoint is configured for 262144 tokens and about 71 GB of weights. Keep other large vLLM services stopped before starting it. Native vLLM serving requires a Laguna-capable vLLM release; Poolside currently documents vLLM 0.25.0 or later plus `poolside_v1` tool and reasoning parsers.
