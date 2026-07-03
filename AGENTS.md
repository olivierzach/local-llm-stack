# Repository Guidelines

## Project Structure & Module Organization

This repository is a Docker Compose-based local LLM stack. Core service definitions live in `docker-compose.yml`, with runtime defaults in `.env.example` and local secrets in ignored `.env`. LiteLLM, Prometheus, and Grafana configuration live under `config/`. Operational scripts are in `scripts/`, long-form docs are in `docs/`, and troubleshooting notes are in `TROUBLESHOOTING.md`.

Persistent runtime data is mounted under `data/`, `models/`, `logs/`, and `evals/`. Treat these as local artifacts, not source. Training scaffolding lives in `training/`, including the training container Dockerfile and example LoRA config.

## Build, Test, and Development Commands

- `make init`: create `.env`, initialize directories, and mark scripts executable.
- `docker compose config --quiet`: validate Compose syntax without starting services.
- `DOCKER_COMPOSE="sudo docker compose" make up`: start the default stack.
- `DOCKER_COMPOSE="sudo docker compose" make ps`: inspect service state.
- `make smoke`: test LiteLLM routing to vLLM using the configured model.
- `./scripts/watch-vllm.sh`: watch vLLM token/sec, request, and KV-cache metrics.
- `DOCKER_COMPOSE="sudo docker compose" make down`: stop and remove stack containers.

## Coding Style & Naming Conventions

Use two-space indentation in YAML. Keep shell scripts POSIX-friendly where practical, with `#!/usr/bin/env bash` and `set -euo pipefail` for operational scripts. Name scripts by action, for example `smoke-test.sh`, `host-check.sh`, and `watch-vllm.sh`. Keep service aliases stable: `local-fast`, `local-balanced`, `local-coder`, and `local-large`.

## Testing Guidelines

There is no unit-test framework. Validate changes with `docker compose config --quiet` and `bash -n scripts/*.sh`. For runtime changes, test from the bottom up: direct vLLM endpoint, `make smoke`, then Open WebUI. Example:

```bash
curl -sS http://localhost:8001/v1/models
make smoke
```

## Commit & Pull Request Guidelines

This repo has no commit history yet. Use concise imperative commits, such as `Add vLLM metrics watcher` or `Fix Prometheus volume permissions`. Pull requests should include the purpose, changed services/configs, validation commands run, and any operational impact such as model downloads, port changes, or required restarts.

## Security & Configuration Tips

Never commit `.env`, model caches, logs, or database files. Keep Open WebUI LAN/Tailscale-only unless HTTPS, auth policy, rate limiting, and backups are added. Redact API keys and Tailscale details from logs before sharing.
