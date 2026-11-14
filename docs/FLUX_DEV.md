# FLUX.1-dev text-to-image service: bring-up and shutdown

Standalone text-to-image service using Black Forest Labs **FLUX.1-dev** via
Diffusers (`FluxPipeline`). Runs as its own container (`flux-dev`) under the
opt-in `flux` Compose profile — **isolated from** the DeepSeek/TP2 serving and
from the LTX-Video service (each in its own worktree).

- Model: `black-forest-labs/FLUX.1-dev`
- Image: `local-llm-stack-flux-dev:latest` (build from `flux/Dockerfile`)
- Runtime: `serve.py` → FastAPI `FluxPipeline` gateway, POST `/generate` → PNG
- Host port: `${FLUX_PORT:-8020}` (container `8001`)
- Source cache: `data/huggingface`
- Parity: identical worktree + image present on **both** sparks (66f1, e8f1)
- Branch/worktree: `feat/flux-dev` at `local-llm-stack-flux` (isolated from LTX)

> **License — read before use:** FLUX.1-dev is **non-commercial**
> (`flux-1-dev-non-commercial-license`) and its weights are **gated** on
> Hugging Face. Local/R&D use is fine; commercial use requires FLUX.1-schnell
> (Apache-2.0) or a paid license. The weights **cannot be downloaded without a
> valid `HF_TOKEN`** in `.env` (and accepting the repository's gate).

> **Memory contract (critical):** FLUX.1-dev needs several GB of GPU unified
> memory. The DeepSeek TP2 serving pins ~78% of the same pool on both nodes, so
> **the two cannot run simultaneously** — bring FLUX up in a takedown window and
> stop serving first (same contract as LTX). At startup the service does **not**
> load the model into memory; the model is loaded lazily on the **first
> `/generate` request**.

---

## Prerequisites

Weights in `data/huggingface/hub/models--black-forest-labs--FLUX.1-dev` and the
image `local-llm-stack-flux-dev:latest` present. The image + source are staged
and mirrored; **the weights are the sole gated gap** (needs a token):

```bash
# set a real token in .env (FLUX.1-dev is gated), accept the repo gate, then:
./scripts/download-model.sh black-forest-labs/FLUX.1-dev      # ~24 GB into data/huggingface
docker compose --profile flux build flux-dev                  # idempotent if image missing
```

Mirror image over the QSFP fabric (like the other services):

```bash
python3 scripts/sync-spark-images.py --lock cluster/flux-images.lock.json \
  --peer <e8f1|66f1> --fabric-inventory ../local-llm-stack/cluster/inventory.json --apply
# weights: rsync -a --checksum the model dir over the same fabric
```

> `cluster/flux-images.lock.json` is committed in `feat/flux-dev`, so the command
> above works from a clean checkout.

---

## Bring-up

### 1. Free the GPU (takedown window)

The DeepSeek TP2 deployment is torn down with the repo's saved-plan mechanism
(not `make down` — that only stops the default single-node Compose stack):

```bash
# in the main checkout that owns the deployment (/home/statsparrot/projects/local-llm-stack)
make deepseek-tp-down PLAN=<the-running-TP2-deployment>
#   PLAN names the live deployment file (e.g. one of cluster/deployments/deepseek-tp2-*-<coordinator>.json,
#   minus the .json suffix) that is currently serving. Match the coordinator node.
nvidia-smi       # confirm the DeepSeek process is gone / memory freed
```

### 2. Start flux-dev (from the flux worktree root)

```bash
cd /home/statsparrot/projects/local-llm-stack-flux
docker compose --profile flux up -d flux-dev
```

> Use the explicit `flux-dev` target; `--profile flux up` with no target also
> selects unprofiled GPU services that would contend for memory.

### 3. Confirm the service is up

```bash
docker compose --profile flux logs -f flux-dev     # uvicorn started
curl -s http://127.0.0.1:8020/docs                 # OpenAPI reachable
```

The model is loaded lazily, so the `[flux] built pipeline` line appears only on
the **first request below**, not at server startup.

### 4. First generation (loads the model, may take a minute)

```bash
curl -sS -X POST http://127.0.0.1:8020/generate \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"a red fox in the snow","width":1024,"height":1024,"num_inference_steps":30}' \
  -o out.png
file out.png    # PNG image data
```

If the weights were never fetched, this returns a clear **503** ("FLUX.1-dev
weights are not cached … run ./scripts/download-model.sh …") instead of
attempting a download.

FLUX.1-dev is **guidance-distilled** → use `guidance_scale` around 3.5 (the
default). Dimensions should be divisible by 16.

---

## Shutdown

```bash
cd /home/statsparrot/projects/local-llm-stack-flux
# stop + remove ONLY flux-dev (not the whole compose project):
docker compose --profile flux stop flux-dev
docker compose --profile flux rm -f flux-dev
# restore the DeepSeek TP2 serving (saved-plan mechanism, main checkout):
make deepseek-tp-up PLAN=<the-same-running-TP2-deployment>
curl -sS http://localhost:8001/v1/models      # confirm serving back
```

---

## Parity check (per node)

- worktree: `git -C <flux worktree> rev-parse --short HEAD` → same on both
- image: `docker images --format '{{.ID}}' local-llm-stack-flux-dev:latest`
- weights: `du -sh data/huggingface/hub/models--black-forest-labs--FLUX.1-dev`

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `401` when downloading FLUX.1-dev | `.env` `HF_TOKEN` empty/invalid; set a real token (gated model), retry |
| `503 weights are not cached` on first request | weights never fetched; run the download script, verify `data/huggingface` mounted |
| `CUDA out of memory` on first request | DeepSeek serving not freed; use `make deepseek-tp-down PLAN=...`, retry |
| Model re-downloads at request | cache not resolving to `data/huggingface`; verify volume mount + run from worktree root |
| `docker compose --profile flux up` also starts vLLM | explicit target `flux-dev` to select only it |
| Image generation differs from prompt | tune `guidance_scale` (FLUX-dev ~3.5) and `num_inference_steps` |
