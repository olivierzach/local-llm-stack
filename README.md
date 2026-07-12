# Local LLM Stack

Self-hosted private assistant and fine-tuning lab for this Spark host.

Deep explainers live in [docs/README.md](docs/README.md). For a full reproducible setup, start with [Spark Setup Runbook](docs/spark-setup-runbook.md).

## What This Runs

- vLLM inference backends for `local-fast`, `local-balanced`, and optional `local-large`.
- LiteLLM router on `:4000` with OpenAI-compatible `/v1` APIs.
- Open WebUI on `:3000`.
- Optional AIChat terminal client through Context Guard.
- Prometheus on `:9090` and Grafana on `:3001`.
- Optional training notebook on `:8888` with LoRA/QLoRA-oriented Python packages.

## First-Time Host Setup

If your user is not in the `docker` group yet, pick one:

```bash
sudo usermod -aG docker "$USER"
newgrp docker
```

or run Docker/Compose commands with `sudo`. For the Makefile, use `DOCKER_COMPOSE="sudo docker compose"`; for helper scripts, use `DOCKER_BIN="sudo docker"`.

Then initialize the project:

```bash
cd /path/to/local-llm-stack
make init
```

`make init` creates `.env` from `.env.example` and replaces placeholder secrets with random local values. Set `HF_TOKEN` if a selected model is gated.

## Validate the Host

```bash
make check
make gpu-check

# If Docker group membership is not active yet:
DOCKER_BIN="sudo docker" make check
DOCKER_BIN="sudo docker" make gpu-check
```

`make gpu-check` must show the GB10 GPU inside a container before vLLM will work.

## Start the Assistant Stack

```bash
make up
make ps
make logs

# If Docker group membership is not active yet:
DOCKER_COMPOSE="sudo docker compose" make up
```

Open:

- Open WebUI: `http://<spark-lan-ip>:3000`
- LiteLLM API: `http://<spark-lan-ip>:4000/v1`
- Grafana: `http://<spark-lan-ip>:3001`

Run an API smoke test:

```bash
make smoke
```

If models list but chat returns HTTP 500, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## Model Router

LiteLLM exposes these aliases:

- `local-fast`
- `local-balanced`
- `local-coder`
- `local-large`
- `local-vision` (starts with `make vision-up`)

The default stack starts `local-fast`. Start `local-balanced` or the large backend separately:

```bash
make balanced-up
make large-up
```

If the large model causes memory pressure, stop another vLLM backend first:

```bash
docker compose stop vllm-fast vllm-balanced
make large-up
```

## Terminal UI

Use AIChat for a lightweight terminal chat client routed through Context Guard:

```bash
make aichat-build
make aichat
```

See [Terminal UI](docs/tui.md).

## AI Engineering Tools

Create a small host-side Python environment for tracing, load tests, and evals:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -r tools/requirements.txt
```

Useful commands:

```bash
set -a; source .env; set +a
python scripts/request-trace.py --model local-fast --prompt "Reply with exactly: trace ok"
python scripts/load-test.py --model local-fast --concurrency 2 --requests 4 --max-tokens 64
python scripts/run-evals.py --models local-fast local-balanced --prompt-file evals/prompts/smoke.jsonl
make throughput-eval
make download-vision
make vision-up
make vision-eval
```

Read [Request Trace Lab](docs/request-trace.md), [vLLM Concurrency Lab](docs/vllm-concurrency.md), and [Evals Lab](docs/evals.md). Read [Throughput Evals Lab](docs/throughput-evals.md) for tok/s measurement and [Vision Model Lab](docs/vision-models.md) for multimodal serving.

The root `requirements.txt` includes both host tooling and training dependencies. Use `tools/requirements.txt` for the lightweight local CLI/test environment; use the training container for LoRA work so PyTorch matches the NVIDIA/CUDA stack.

## Fine-Tuning Lab

Start the training notebook only when needed:

```bash
make training-up
```

Open `http://<spark-lan-ip>:8888` with the token from `JUPYTER_TOKEN` in `.env`.

Directory layout:

- `data/datasets/raw`: source datasets
- `data/datasets/processed`: chat JSONL datasets
- `models/adapters`: LoRA outputs
- `models/merged`: merged/exported models
- `evals/results`: legacy evaluation outputs
- `evals/runs`: JSONL outputs from `scripts/run-evals.py` and load-test records

The included `training/configs/qwen3-lora-smoke.yaml`, `data/datasets/processed/smoke.jsonl`, and `data/datasets/processed/smoke_lora.jsonl` are only for proving the plumbing works. See [LoRA Adapter Lab](docs/lora-adapters.md).

## Notes

- The vLLM image defaults to NVIDIA's container image because this is an ARM64 GB10 host. The default tag is pinned to `nvcr.io/nvidia/vllm:25.09-py3` for the installed 580-series driver. Newer vLLM images can require newer NVIDIA drivers; if vLLM restarts with a driver requirement error, either upgrade the host driver or choose an older compatible `VLLM_IMAGE` in `.env`.
- This stack binds to localhost by default through `BIND_HOST=127.0.0.1`. Set `BIND_HOST=0.0.0.0` only when you want LAN access.
- Do not expose this directly to the public internet without adding TLS, stronger auth policy, rate limiting, backups, and log retention.
