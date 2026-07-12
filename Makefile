SHELL := /usr/bin/env bash
DOCKER_COMPOSE ?= docker compose


.PHONY: init test check gpu-check up down logs ps smoke large-up balanced-up qwen30-up deepseek32-up mistral24-up gptoss120-up download-model download-qwen32 download-qwen30 download-deepseek32 download-mistral24 download-gptoss120 download-vision training-up lora-train lora-serve lora-eval throughput-eval vision-up vision-eval context-guard context-guard-up

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

balanced-up:
	$(DOCKER_COMPOSE) --profile balanced up -d vllm-balanced

large-up:
	$(DOCKER_COMPOSE) --profile large up -d vllm-large

qwen30-up:
	$(DOCKER_COMPOSE) --profile qwen30a3b up -d vllm-qwen30a3b

deepseek32-up:
	$(DOCKER_COMPOSE) --profile deepseek32b up -d vllm-deepseek32b

mistral24-up:
	$(DOCKER_COMPOSE) --profile mistral24b up -d vllm-mistral24b

gptoss120-up:
	$(DOCKER_COMPOSE) --profile gptoss120b up -d vllm-gptoss120b

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

download-gptoss120:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${GPTOSS120B_MODEL:-openai/gpt-oss-120b}"

download-vision:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${VISION_MODEL:-Qwen/Qwen3-VL-4B-Instruct}"

training-up:
	$(DOCKER_COMPOSE) --profile training up -d training

lora-train:
	$(DOCKER_COMPOSE) --profile training run --rm training python /workspace/training/train_lora.py --config /workspace/training/configs/qwen3-lora-smoke.yaml

lora-serve:
	$(DOCKER_COMPOSE) --profile lora up -d vllm-lora litellm

lora-eval:
	set -a; source .env; set +a; python scripts/run-evals.py --models local-fast local-balanced-smoke-lora --prompt-file evals/prompts/smoke.jsonl

throughput-eval:
	set -a; source .env; set +a; python scripts/load-test.py --model local-fast --concurrency $${CONCURRENCY:-4} --requests $${REQUESTS:-20} --max-tokens $${MAX_TOKENS:-128} --stream --json --jsonl evals/runs/throughput-local-fast.jsonl

vision-up:
	$(DOCKER_COMPOSE) up -d postgres
	$(DOCKER_COMPOSE) --profile vision up -d vllm-vision
	$(DOCKER_COMPOSE) up -d --no-deps litellm

vision-eval:
	set -a; source .env; set +a; python scripts/run-evals.py --models local-vision --prompt-file evals/prompts/vision.jsonl

down:
	$(DOCKER_COMPOSE) --profile large --profile qwen30a3b --profile deepseek32b --profile mistral24b --profile gptoss120b --profile training --profile lora --profile vision down

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

ps:
	$(DOCKER_COMPOSE) ps

smoke:
	set -a; source .env; set +a; ./scripts/smoke-test.sh

context-guard:
	set -a; source .env; set +a; python scripts/context-guard-proxy.py

context-guard-up:
	$(DOCKER_COMPOSE) up -d context-guard
