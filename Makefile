SHELL := /usr/bin/env bash
DOCKER_COMPOSE ?= docker compose
.DEFAULT_GOAL := init

# Separate two-node DeepSeek recipes. Existing deepseekv4-* remains single-node.
DEEPSEEK_TP_SPEC ?= off
DEEPSEEK_TP_EXECUTION ?= eager
DEEPSEEK_TP_MANIFEST = cluster/deployments/deepseek-tp2$(if $(filter dspark2,$(DEEPSEEK_TP_SPEC)),-dspark2,)$(if $(filter graphs,$(DEEPSEEK_TP_EXECUTION)),-graphs,)-$(COORDINATOR).json
.PHONY: deepseek-tp-prepare deepseek-tp-plan deepseek-tp-up deepseek-tp-status deepseek-tp-down
deepseek-tp-prepare:
	@test -n "$(PEER)" -a -n "$(OUTPUT)" || { echo 'Use PEER=66f1|e8f1 OUTPUT=/path/to/preparation-receipts' >&2; exit 2; }
	python3 scripts/prepare-deepseek-tp.py --peer "$(PEER)" --output "$(OUTPUT)" --apply

deepseek-tp-plan deepseek-tp-up:
	@case "$(COORDINATOR)" in 66f1|e8f1) ;; *) echo 'Choose COORDINATOR=66f1 or e8f1' >&2; exit 2 ;; esac
	@case "$(DEEPSEEK_TP_SPEC)" in off|dspark2) ;; *) echo 'Use DEEPSEEK_TP_SPEC=off or dspark2' >&2; exit 2 ;; esac
	@case "$(DEEPSEEK_TP_EXECUTION)" in eager|graphs) ;; *) echo 'Use DEEPSEEK_TP_EXECUTION=eager or graphs' >&2; exit 2 ;; esac
	@test -n "$(OUTPUT)" || { echo 'Use OUTPUT=/path/to/a/new/deployment-receipt-directory' >&2; exit 2; }
	python3 scripts/sparkctl $(if $(filter deepseek-tp-plan,$@),render,up) --deployment "$(DEEPSEEK_TP_MANIFEST)" --output "$(OUTPUT)" --timeout 3600

deepseek-tp-status deepseek-tp-down:
	@test -n "$(PLAN)" || { echo 'Use PLAN=/path/to/the/exact/saved/plan.json' >&2; exit 2; }
	python3 scripts/sparkctl $(if $(filter deepseek-tp-status,$@),status,down) --saved-plan "$(PLAN)"

# Public GPU operations share admission with sparkctl and the research adapters.
SPARK_GPU_TARGETS := up balanced-up large-up qwen30-up deepseek32-up mistral24-up gptoss120-up lagunas21-up vision-up lora-serve training-up lora-train qwen38-up deepseekv4-up qwen38-down deepseekv4-down down
.PHONY: $(SPARK_GPU_TARGETS) gpu-admission-check gpu-recover
.PHONY: $(addprefix _spark-,$(SPARK_GPU_TARGETS)) _spark-admitted
$(SPARK_GPU_TARGETS):
	python3 scripts/spark-legacy-run.py run --root "$(CURDIR)" --target "$@"

$(addprefix _spark-,$(SPARK_GPU_TARGETS)): | _spark-admitted
_spark-admitted:
	@python3 scripts/spark-legacy-run.py verify --root "$(CURDIR)"

gpu-admission-check:
	@test -n "$(TARGET)" || { echo 'Use TARGET=deepseekv4-up (or another GPU Make target)' >&2; exit 2; }
	python3 scripts/spark-legacy-run.py check --root "$(CURDIR)" --target "$(TARGET)"

gpu-recover:
	python3 scripts/spark-legacy-run.py recover --root "$(CURDIR)"

.PHONY: routing-failure-test
routing-failure-test:
	.venv/bin/python -m pytest tests/test_stream_failures.py -q

.PHONY: native-fabric-plan native-fabric-config
native-fabric-plan:
	python3 scripts/configure-native-fabric.py --root "$(CURDIR)"

native-fabric-config:
	python3 scripts/configure-native-fabric.py --root "$(CURDIR)" --apply

.PHONY: model-parity-check model-parity-copy model-runtime-prepare model-compose-test
model-parity-check:
	python3 scripts/audit-stack-models.py --root "$(CURDIR)"

model-parity-copy:
	@test -n "$(PEER)" || { echo 'Use PEER=spark-e8f1-wired or PEER=spark-66f1-wired (run on the source Spark)' >&2; exit 2; }
	python3 scripts/sync-spark-model-parity.py --root "$(CURDIR)" --peer "$(PEER)" --peer-root "$(or $(PEER_ROOT),$(CURDIR))" --catalog cluster/single-node-models.lock.json --fabric-rail "$(or $(FABRIC_RAIL),0)" --output "$(or $(OUTPUT),data/model-parity/copy)"

model-runtime-prepare:
	python3 scripts/prepare-spark-model-runtimes.py --root "$(CURDIR)" $(if $(BUNDLE_DIR),--bundle-dir "$(BUNDLE_DIR)",)

model-compose-test:
	@test -n "$(RUN_ID)" || { echo 'Use a fresh RUN_ID; optionally select MODELS="local-fast local-large"' >&2; exit 2; }
	$(or $(PYTHON),.venv/bin/python) scripts/probe-stack-models.py --stack-root "$(CURDIR)" --run-id "$(RUN_ID)" --output "$(or $(OUTPUT),data/model-parity/inference/$(RUN_ID))" $(foreach alias,$(MODELS),--alias "$(alias)")

.PHONY: python-parity-check python-parity-prepare python-parity-test
python-parity-check:
	python3 scripts/bootstrap-spark-python.py check --venv "$(or $(VENV),$(CURDIR)/.venv)"

python-parity-prepare:
	@test -n "$(VENV)" || { echo 'Use VENV=/absolute/path/to/a/new/environment; existing environments are never replaced' >&2; exit 2; }
	python3 scripts/bootstrap-spark-python.py create --venv "$(VENV)" $(if $(WHEELHOUSE),--wheelhouse "$(WHEELHOUSE)",)

python-parity-test:
	@test -n "$(RUN_ID)" || { echo 'Use a fresh RUN_ID; this CUDA smoke requires an idle GPU and research window' >&2; exit 2; }
	python3 scripts/probe-spark-python.py --venv "$(or $(VENV),$(CURDIR)/.venv)" --output "$(or $(OUTPUT),data/python-parity/$(RUN_ID))"


.PHONY: init test check gpu-check up down logs ps smoke large-up balanced-up qwen30-up deepseek32-up mistral24-up gptoss120-up lagunas21-up deepseekv4-install deepseekv4-up deepseekv4-down deepseekv4-status deepseekv4-smoke download-model download-qwen32 download-qwen30 download-deepseek32 download-mistral24 download-gptoss120 download-lagunas21 download-vision training-up lora-train lora-serve lora-eval throughput-eval vision-up vision-eval context-guard context-guard-up aichat-build aichat opencode

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

_spark-up:
	$(DOCKER_COMPOSE) up -d

_spark-balanced-up:
	$(DOCKER_COMPOSE) --profile balanced up -d vllm-balanced

_spark-large-up:
	$(DOCKER_COMPOSE) --profile large up -d vllm-large

_spark-qwen30-up:
	$(DOCKER_COMPOSE) --profile qwen30a3b up -d vllm-qwen30a3b

_spark-deepseek32-up:
	$(DOCKER_COMPOSE) --profile deepseek32b up -d vllm-deepseek32b

_spark-mistral24-up:
	$(DOCKER_COMPOSE) --profile mistral24b up -d vllm-mistral24b

_spark-gptoss120-up:
	$(DOCKER_COMPOSE) --profile gptoss120b up -d vllm-gptoss120b

_spark-lagunas21-up:
	$(DOCKER_COMPOSE) --profile lagunas21 up -d vllm-lagunas21

# The recipe owns a host-network container, outside Compose's default startup.
.PHONY: qwen38-check qwen38-install qwen38-up qwen38-down qwen38-status qwen38-logs qwen38-dry-run qwen38-smoke qwen38-test qwen38-bench qwen38-soak
qwen38-check:
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh check

qwen38-install:
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh install

_spark-qwen38-up:
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh check
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh stop
	$(DOCKER_COMPOSE) stop vllm-fast vllm-balanced vllm-large vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b vllm-gptoss120b vllm-lagunas21 vllm-lora vllm-vision
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh start
	$(DOCKER_COMPOSE) up -d --no-deps --force-recreate --wait --wait-timeout 180 litellm context-guard

_spark-qwen38-down:
	bash scripts/qwen38-flash-next.sh stop

qwen38-status:
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh status

qwen38-logs:
	bash scripts/qwen38-flash-next.sh logs

qwen38-dry-run:
	set -a; source .env; set +a; bash scripts/qwen38-flash-next.sh dry-run

qwen38-smoke:
	.venv/bin/python scripts/qwen38-verify.py smoke

qwen38-test:
	.venv/bin/python scripts/qwen38-verify.py acceptance

qwen38-bench:
	.venv/bin/python scripts/qwen38-verify.py benchmark

qwen38-soak:
	.venv/bin/python scripts/qwen38-verify.py soak

.PHONY: qwen38-verify qwen38-clients qwen38-recovery
qwen38-verify:
	$(MAKE) qwen38-check
	./tests/test.sh -q
	$(DOCKER_COMPOSE) config --quiet
	$(MAKE) qwen38-smoke
	$(MAKE) qwen38-test
	$(MAKE) qwen38-recovery
	$(MAKE) qwen38-clients
	$(MAKE) qwen38-bench
	$(MAKE) qwen38-soak

qwen38-clients:
	.venv/bin/python scripts/qwen38-clients.py

qwen38-recovery:
	.venv/bin/python scripts/qwen38-recovery-test.py

deepseekv4-install:
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh install

_spark-deepseekv4-up:
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh check-draft
	bash scripts/qwen38-flash-next.sh stop
	$(DOCKER_COMPOSE) stop vllm-fast vllm-balanced vllm-large vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b vllm-gptoss120b vllm-lagunas21 vllm-lora vllm-vision
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh start
	$(DOCKER_COMPOSE) up -d --no-deps --force-recreate --wait --wait-timeout 180 litellm context-guard

_spark-deepseekv4-down:
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh stop

deepseekv4-status:
	set -a; source .env; set +a; ./scripts/deepseek-v4.sh status

deepseekv4-smoke:
	set -a; source .env; set +a; MODEL=local-deepseek-v4-flash ./scripts/smoke-test.sh

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

download-lagunas21:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${LAGUNAS21_MODEL:-poolside/Laguna-S-2.1-NVFP4}"

download-vision:
	set -a; source .env; set +a; ./scripts/download-model.sh "$${VISION_MODEL:-Qwen/Qwen3-VL-4B-Instruct}"

_spark-training-up:
	$(DOCKER_COMPOSE) --profile training up -d training

_spark-lora-train:
	$(DOCKER_COMPOSE) --profile training run --rm training python /workspace/training/train_lora.py --config /workspace/training/configs/qwen3-lora-smoke.yaml

_spark-lora-serve:
	$(DOCKER_COMPOSE) --profile lora up -d vllm-lora litellm

lora-eval:
	set -a; source .env; set +a; python scripts/run-evals.py --models local-fast local-balanced-smoke-lora --prompt-file evals/prompts/smoke.jsonl

throughput-eval:
	set -a; source .env; set +a; python scripts/load-test.py --model local-fast --concurrency $${CONCURRENCY:-4} --requests $${REQUESTS:-20} --max-tokens $${MAX_TOKENS:-128} --stream --json --jsonl evals/runs/throughput-local-fast.jsonl

_spark-vision-up:
	$(DOCKER_COMPOSE) up -d postgres
	$(DOCKER_COMPOSE) --profile vision up -d vllm-vision
	$(DOCKER_COMPOSE) up -d --no-deps litellm

vision-eval:
	set -a; source .env; set +a; python scripts/run-evals.py --models local-vision --prompt-file evals/prompts/vision.jsonl

_spark-down:
	bash scripts/qwen38-flash-next.sh stop
	-set -a; source .env; set +a; ./scripts/deepseek-v4.sh stop
	$(DOCKER_COMPOSE) --profile large --profile qwen30a3b --profile deepseek32b --profile mistral24b --profile gptoss120b --profile lagunas21 --profile training --profile lora --profile vision down

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

ps:
	$(DOCKER_COMPOSE) ps

smoke:
	set -a; source .env; set +a; ./scripts/smoke-test.sh

context-guard:
	set -ae; source .env; \
	export QWEN38_API_BASE="$${QWEN38_API_BASE:-http://$${QWEN38_BIND_HOST:-$$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')}:$${QWEN38_PORT:-8012}/v1}"; \
	export DEEPSEEKV4_API_BASE="$${DEEPSEEKV4_API_BASE:-http://$${DEEPSEEKV4_BIND_HOST:-$$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')}:$${DEEPSEEKV4_PORT:-8011}/v1}"; \
	python scripts/context-guard-router.py

context-guard-up:
	$(DOCKER_COMPOSE) up -d context-guard

.PHONY: context-routes context-routes-disable context-route-test
context-routes:
	@test -n "$(ROUTES)" || { echo 'Use ROUTES=/path/to/complete-registry.json' >&2; exit 2; }
	$(or $(PYTHON),.venv/bin/python) scripts/configure-context-routes.py --registry "$(ROUTES)"
	$(DOCKER_COMPOSE) up -d --no-deps context-guard

context-routes-disable:
	$(or $(PYTHON),.venv/bin/python) scripts/configure-context-routes.py --disable
	$(DOCKER_COMPOSE) up -d --no-deps context-guard

context-route-test:
	@test -n "$(MODEL)" -a -n "$(RUN_ID)" || { echo 'Use MODEL=alias RUN_ID=fresh-id; optional TOOLS=1 THINKING_DISABLED=1' >&2; exit 2; }
	$(or $(PYTHON),.venv/bin/python) scripts/probe-context-route.py --model "$(MODEL)" --output "$(or $(OUTPUT),data/context-route-tests/$(RUN_ID).json)" $(if $(GATEWAY_URL),--base-url "$(GATEWAY_URL)",) $(if $(filter 1 true yes,$(TOOLS)),--tools,) $(if $(filter 1 true yes,$(THINKING_DISABLED)),--thinking-disabled,)

aichat-build:
	$(DOCKER_COMPOSE) --profile tui build aichat

aichat:
	$(DOCKER_COMPOSE) --profile tui run --rm aichat $(if $(MODEL),--model spark:$(MODEL),)

opencode:
	$(DOCKER_COMPOSE) --profile tui run --rm opencode $(if $(MODEL),--model spark/$(MODEL),)
