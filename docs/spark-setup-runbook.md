# Spark Setup Runbook

This is the end-to-end path to reproduce the local LLM stack on the Spark host from a fresh checkout. It covers host prerequisites, Python tooling, Docker images, model downloads, baseline serving, throughput evals, vision serving, and the LoRA adapter demo.

## 1. Host Prerequisites

The expected host is an ARM64 Spark/GB10 machine with an NVIDIA driver that supports the pinned container images.

Check the host:

```bash
cd /path/to/local-llm-stack
uname -m
nvidia-smi
docker compose version
```

If the current user cannot access Docker directly, either add the user to the `docker` group and start a new shell:

```bash
sudo usermod -aG docker "$USER"
newgrp docker
```

or prefix Make targets with:

```bash
DOCKER_COMPOSE="sudo docker compose"
DOCKER_BIN="sudo docker"
```

## 2. Initialize The Checkout

```bash
cd /path/to/local-llm-stack
make init
```

`make init` creates `.env`, runtime directories, dataset directories, eval output directories, and model artifact directories.

Edit `.env` before starting services:

```bash
nano .env
```

Set `HF_TOKEN` if any selected Hugging Face model is gated or private. Keep the generated LiteLLM, Open WebUI, Postgres, and Grafana secrets.

## 3. Python Tooling

Create the lightweight host-side environment for tests, tracing, load tests, and evals:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r tools/requirements.txt
```

Run static validation:

```bash
make test
```

The root `requirements.txt` includes both host tooling and training dependencies. Do not install the root file into the host venv unless you intentionally want local PyTorch and training packages outside Docker:

```bash
python -m pip install -r requirements.txt
```

For normal operation, use `tools/requirements.txt` on the host and the training container for LoRA.

## 4. Validate Docker And GPU

```bash
make check
make gpu-check
```

If Docker group membership is not active:

```bash
DOCKER_BIN="sudo docker" make check
DOCKER_BIN="sudo docker" make gpu-check
```

`make gpu-check` must show the GB10 GPU inside a container before vLLM serving or LoRA training will work.

## 5. Pull Required Container Images

The first `make up`, `make vision-up`, or `make lora-train` can pull large images. Pull/build deliberately when setting up the host:

```bash
docker compose pull postgres litellm open-webui prometheus grafana vllm-fast vllm-balanced model-cache
docker compose --profile vision pull vllm-vision
docker compose --profile lora pull vllm-lora
docker compose --profile training build training
```

If Docker requires sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make training-up
```

The training image is large because it starts from NVIDIA PyTorch and installs Transformers, PEFT, TRL, datasets, and related packages from `training/requirements.txt`.

## 6. Download Model Weights

Model downloads go into the shared Hugging Face cache at `data/huggingface/`.

Default serving models:

```bash
make download-model MODEL=Qwen/Qwen3-4B-Instruct-2507
make download-model MODEL=Qwen/Qwen3-14B
```

Vision model:

```bash
make download-vision
```

Optional model zoo candidates:

```bash
make download-qwen32
make download-qwen30
make download-deepseek32
make download-mistral24
```

Check cached models:

```bash
find data/huggingface/hub -maxdepth 2 -type d -name 'models--*'
```

## 7. Start The Base Assistant Stack

```bash
make up
make ps
```

Open:

```text
Open WebUI:  http://<spark-lan-ip>:3000
LiteLLM API: http://<spark-lan-ip>:4000/v1
Grafana:     http://<spark-lan-ip>:3001
```

Run the routed smoke test:

```bash
make smoke
```

If this fails, test direct vLLM first:

```bash
curl -sS http://localhost:8001/v1/models
curl -sS http://localhost:8002/v1/models
```

## 8. Run Trace, Load, And Eval Tools

Load the generated `.env` so the Python tools use the real LiteLLM key:

```bash
set -a; source .env; set +a
```

Trace direct vLLM vs routed LiteLLM:

```bash
python scripts/request-trace.py --model local-fast --prompt "Reply with exactly: trace ok" --max-tokens 16
```

Run a small load test:

```bash
python scripts/load-test.py --model local-fast --concurrency 2 --requests 4 --max-tokens 32 --stream --json
```

Run a fixed eval set:

```bash
python scripts/run-evals.py --models local-fast local-balanced --prompt-file evals/prompts/smoke.jsonl
```

Run the Make target that saves throughput JSONL:

```bash
make throughput-eval
```

Outputs land under ignored `evals/runs/`.

## 9. Start Vision Support

The default vision alias is:

```text
local-vision -> Qwen/Qwen3-VL-4B-Instruct
```

Download it first:

```bash
make download-vision
```

On a memory-constrained host, stop other vLLM services before starting vision:

```bash
docker compose stop vllm-fast vllm-balanced vllm-large vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b vllm-lora
```

Start the vision service without pulling in the default vLLM dependencies:

```bash
make vision-up
```

Check direct serving:

```bash
curl -sS http://localhost:8008/v1/models
```

Run the fixed image eval:

```bash
make vision-eval
```

If `Qwen/Qwen3-VL-4B-Instruct` is unsupported by the pinned vLLM image, switch to the fallback in `.env`:

```text
VISION_MODEL=Qwen/Qwen2.5-VL-3B-Instruct
```

then rerun:

```bash
make download-vision
make vision-up
```

## 10. Run The LoRA Adapter Demo

The smoke LoRA adapts the already downloaded base model:

```text
Qwen/Qwen3-4B-Instruct-2507
```

It does not modify the Hugging Face model cache. Training writes adapter artifacts under:

```text
models/adapters/qwen3-4b-smoke-lora/runs/<run-id>/
models/adapters/qwen3-4b-smoke-lora/current -> runs/<run-id>
```

Dry-run the paths:

```bash
python training/train_lora.py --config training/configs/qwen3-lora-smoke.yaml --run-id smoke-check --dry-run
```

Stop inference services if GPU memory is tight:

```bash
docker compose stop vllm-fast vllm-balanced vllm-vision vllm-lora
```

Train the adapter:

```bash
make lora-train
```

Serve the adapter:

```bash
make lora-serve
```

Check direct serving:

```bash
curl -sS http://localhost:8007/v1/models
```

Run the comparison eval:

```bash
make lora-eval
```

Rollback to an earlier adapter by repointing `models/adapters/qwen3-4b-smoke-lora/current` to another directory under `runs/` and restarting `vllm-lora`.

## 11. Runtime Cleanup

Stop every profile-backed service:

```bash
make down
```

Runtime outputs are intentionally ignored by git:

```text
data/huggingface/
logs/
evals/runs/
models/adapters/
models/merged/
```

Keep prompt sets, docs, configs, and tiny source fixtures in git. Keep model weights, adapter weights, and eval run outputs out of git.

## 12. Minimal Rebuild Checklist

Use this checklist when reproducing from scratch:

```bash
cd /path/to/local-llm-stack
make init
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r tools/requirements.txt
make test
make check
make gpu-check
make download-model MODEL=Qwen/Qwen3-4B-Instruct-2507
make download-model MODEL=Qwen/Qwen3-14B
make download-vision
docker compose --profile training build training
make up
make smoke
set -a; source .env; set +a
python scripts/request-trace.py --model local-fast --prompt "Reply with exactly: trace ok" --max-tokens 16
make throughput-eval
```

Then choose one GPU-heavy path at a time:

```bash
make vision-up
make vision-eval
```

or:

```bash
make lora-train
make lora-serve
make lora-eval
```
