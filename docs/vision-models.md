# Vision Model Lab

This lab adds one multimodal model alias to the stack:

```text
local-vision -> vllm-vision -> Qwen/Qwen3-VL-4B-Instruct
```

vLLM lists Qwen3-VL and Qwen2.5-VL in its multimodal supported-model table. The Qwen3-VL-4B model card also shows OpenAI-compatible vLLM serving with `image_url` content.

## Start Vision Serving

```bash
make download-vision
make vision-up
```

Direct check:

```bash
curl -sS http://localhost:8008/v1/models
```

Trace one image prompt:

```bash
set -a; source .env; set +a
python scripts/request-trace.py   --model local-vision   --prompt "What color is the square? Answer one word."   --image-path evals/assets/red-square.png   --max-tokens 8
```

Run the fixed vision prompt set:

```bash
make vision-eval
```

## Model Choice

The default is `Qwen/Qwen3-VL-4B-Instruct` because it is the smallest dense Qwen3-VL instruct model listed for vLLM-style serving. If the pinned vLLM image does not support Qwen3-VL yet, set this in `.env` and restart the profile:

```text
VISION_MODEL=Qwen/Qwen2.5-VL-3B-Instruct
```

Keep this as a separate profile because image prompts add preprocessing work and KV cache pressure. Do not run `vllm-fast`, `vllm-balanced`, and `vllm-vision` together unless GPU memory headroom is confirmed.

## What To Measure

For vision, compare:

```text
time to first token
output tokens/sec
p95 latency
image size and count
answer correctness on fixed image prompts
```

Start with one small image. Increase image size only after the single-image path is reliable.
