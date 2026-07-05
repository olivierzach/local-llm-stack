SHELL := /usr/bin/env bash
DOCKER_COMPOSE ?= docker compose


.PHONY: init test check gpu-check up down logs ps smoke large-up training-up

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

training-up:
	$(DOCKER_COMPOSE) --profile training up -d training

down:
	$(DOCKER_COMPOSE) --profile large --profile training down

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

ps:
	$(DOCKER_COMPOSE) ps

smoke:
	set -a; source .env; set +a; ./scripts/smoke-test.sh

