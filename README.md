# Local LLM Stack

Self-hosted private assistant and fine-tuning lab for this Spark host.

Deep explainers live in [docs/README.md](docs/README.md).

## What This Runs

- vLLM inference backends for `local-fast`, `local-balanced`, and optional `local-large`.
- LiteLLM router on `:4000` with OpenAI-compatible `/v1` APIs.
- Open WebUI on `:3000`.
- Prometheus on `:9090` and Grafana on `:3001`.
- Optional training notebook on `:8888` with LoRA/QLoRA-oriented Python packages.

## First-Time Host Setup

The current user was not in the `docker` group during inspection. Pick one:

```bash
sudo usermod -aG docker statsparrot
newgrp docker
```

or run Docker/Compose commands with `sudo`. For the Makefile, use `DOCKER_COMPOSE="sudo docker compose"`; for helper scripts, use `DOCKER_BIN="sudo docker"`.

Then initialize the project:

```bash
cd /home/statsparrot/projects/local-llm-stack
make init
```

`.env` has already been generated locally with random secrets. Set `HF_TOKEN` if a selected model is gated.

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

The default stack starts `local-fast` and `local-balanced`. Start the large backend separately:

```bash
make large-up
```

If the large model causes memory pressure, stop another vLLM backend first:

```bash
docker compose stop vllm-balanced
make large-up
```

## Fine-Tuning Lab

Start the training notebook only when needed:

```bash
make training-up
```

Open `http://<spark-lan-ip>:8888` with token `local-lab`.

Directory layout:

- `data/datasets/raw`: source datasets
- `data/datasets/processed`: chat JSONL datasets
- `models/adapters`: LoRA outputs
- `models/merged`: merged/exported models
- `evals/results`: evaluation outputs

The included `training/configs/qwen3-lora-smoke.yaml` and `data/datasets/processed/smoke.jsonl` are only for proving the plumbing works.

## Notes

- The vLLM image defaults to NVIDIA's container image because this is an ARM64 GB10 host. The default tag is pinned to `nvcr.io/nvidia/vllm:25.09-py3` for the installed 580-series driver. Newer vLLM images can require newer NVIDIA drivers; if vLLM restarts with a driver requirement error, either upgrade the host driver or choose an older compatible `VLLM_IMAGE` in `.env`.
- This stack binds to LAN by default through `BIND_HOST=0.0.0.0`. Set `BIND_HOST=127.0.0.1` for machine-local-only access.
- Do not expose this directly to the public internet without adding TLS, stronger auth policy, rate limiting, backups, and log retention.
