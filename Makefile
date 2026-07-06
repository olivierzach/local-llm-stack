SHELL := /usr/bin/env bash
DOCKER_COMPOSE ?= docker compose


.PHONY: init test check gpu-check up down logs ps smoke large-up qwen30-up deepseek32-up mistral24-up download-model download-qwen32 download-qwen30 download-deepseek32 download-mistral24 training-up

init:
	cp -n .env.example .env
	chmod +x scripts/*.sh
	./scripts/init-dirs.sh

test:
	./tests/test.sh

check:
	./scripts/host-check.sh

gpu-check:
	./scripts/gpu-container-check.sh

up:
	$(DOCKER_COMPOSE) up -d

large-up:
	$(DOCKER_COMPOSE) --profile large up -d vllm-large

qwen30-up:
	$(DOCKER_COMPOSE) --profile qwen30a3b up -d vllm-qwen30a3b

deepseek32-up:
	$(DOCKER_COMPOSE) --profile deepseek32b up -d vllm-deepseek32b

mistral24-up:
	$(DOCKER_COMPOSE) --profile mistral24b up -d vllm-mistral24b

download-model:
	@if [[ -z "$${MODEL:-}" ]]; then echo 'Usage: make download-model MODEL=org/model-id' >&2; exit 2; fi
	./scripts/download-model.sh "$${MODEL}"

download-qwen32:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${LARGE_MODEL:-Qwen/Qwen3-32B}"

download-qwen30:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${QWEN30A3B_MODEL:-Qwen/Qwen3-30B-A3B-Instruct-2507}"

download-deepseek32:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${DEEPSEEK32B_MODEL:-deepseek-ai/DeepSeek-R1-Distill-Qwen-32B}"

download-mistral24:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${MISTRAL24B_MODEL:-mistralai/Mistral-Small-3.2-24B-Instruct-2506}"

training-up:
	$(DOCKER_COMPOSE) --profile training up -d training

down:
	$(DOCKER_COMPOSE) --profile large --profile qwen30a3b --profile deepseek32b --profile mistral24b --profile training down

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

ps:
	$(DOCKER_COMPOSE) ps

smoke:
	set -a; source .env; set +a; ./scripts/smoke-test.sh

