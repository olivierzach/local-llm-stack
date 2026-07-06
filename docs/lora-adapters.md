# LoRA Adapter Lab

This lab trains a tiny adapter, serves it as a separate model alias, compares it
with the base model, and keeps rollback simple.

## Dataset Format

The smoke dataset is `data/datasets/processed/smoke_lora.jsonl`. Each row uses
chat messages:

```json
{"messages":[{"role":"system","content":"..."},{"role":"user","content":"..."},{"role":"assistant","content":"..."}]}
```

The included rows teach one obvious behavior: adapter-check prompts should
produce `SMOKE_LORA_ACTIVE`. This is a plumbing demo, not a useful assistant
fine-tune.

## LoRA Parameters

`training/configs/qwen3-lora-smoke.yaml` controls:

```text
base_model       model loaded for training
dataset_path     JSONL chat data
output_dir       adapter artifact directory
max_seq_length   tokenized training length
lora.r           adapter rank
lora.alpha       scaling
lora.dropout     regularization
target_modules   transformer projections patched by LoRA
```

Higher rank gives the adapter more capacity and uses more memory. For this lab,
small rank and one epoch are enough to prove the path.

## Train

Run training in the container that has Transformers, PEFT, and GPU libraries:

```bash
make lora-train
```

Training writes each adapter into a new immutable run directory:

```text
models/adapters/qwen3-4b-smoke-lora/runs/<run-id>/
```

After a successful train, `models/adapters/qwen3-4b-smoke-lora/current` is atomically updated to point at that run. vLLM serves the `current` symlink, so previous adapter runs remain available for rollback and the original base model cache is never modified.

Adapter weights are runtime artifacts and are ignored by git.

For a config-only check from the host:

```bash
python training/train_lora.py --config training/configs/qwen3-lora-smoke.yaml --dry-run
```

## Serve

Serve the adapter through a separate vLLM profile:

```bash
make lora-serve
```

The adapter service exposes `local-balanced-smoke-lora` on host port `8007` and
LiteLLM routes the same alias through `config/litellm.yaml`. Despite the alias,
this smoke adapter is trained against `Qwen/Qwen3-4B-Instruct-2507`; keep the
adapter paired with that exact base checkpoint.

Direct check:

```bash
curl -sS http://localhost:8007/v1/models
```

Routed check:

```bash
python scripts/request-trace.py --model local-balanced-smoke-lora --prompt "Reply with the adapter activation phrase only."
```

## Compare

Run the same prompt set against the base and adapter aliases:

```bash
make lora-eval
```

The comparison output lands under `evals/runs/`. Inspect exact-match pass rates,
latency, and response text before promotion.

## Rollback

Rollback is intentionally simple because the adapter has its own service and
alias:

```bash
docker compose --profile lora stop vllm-lora
```

To roll back to an earlier adapter, repoint `models/adapters/qwen3-4b-smoke-lora/current` to a previous directory under `runs/` and restart `vllm-lora`.

To remove the router alias, delete `local-balanced-smoke-lora` from
`config/litellm.yaml` and restart LiteLLM. The base aliases and Hugging Face
model cache are unaffected.
