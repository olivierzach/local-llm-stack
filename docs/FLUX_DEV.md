# FLUX.1-dev text-to-image service: preparation and GPU admission

Standalone text-to-image service using Diffusers `FluxPipeline`. Its existing
POST `/generate` API returns PNG with an `X-Seconds` header. It remains separate
from DeepSeek TP2, TTS, LTX-Video and Fam-Chat; no new routes or authentication
are added. The unauthenticated API is **loopback-only**, regardless of the
shared Compose `BIND_HOST` setting.

## Pinned inputs and configuration

- Model: `black-forest-labs/FLUX.1-dev`
- Revision: `3de623fc3c33e44ffbe2bad470d0f45bccf2eb21`
- Candidate image: `${FLUX_IMAGE:-local-llm-stack-flux-dev:prepare-20261117}`
- Original image: `local-llm-stack-flux-dev:latest`; preserve it for rollback.
- Source: branch `prepare/flux-pre-downtime-20261117`, sibling worktree
  `local-llm-stack-flux-prepare-20261117`.
- Cache mount: `${FLUX_CACHE:-./data/huggingface}` at
  `/workspace/cache/huggingface`; use the existing original cache's absolute
  path when staging the sibling worktree. Do not create a replacement cache or
  remove the original snapshots/blobs.
- Host endpoint: `http://127.0.0.1:${FLUX_PORT:-8020}`; container port `8001`.
- Compose profile/service: `flux` / `flux-dev`. Restart policy is `"no"` and
  image pulling is disabled. A host reboot must not restart FLUX alongside TP2.

`FLUX_MODEL` selects a Hugging Face repository. An empty/unset `FLUX_REVISION`
selects the pinned revision above **only for the default repository**. Changing
`FLUX_MODEL` requires its own explicit full 40-character commit hash in
`FLUX_REVISION`; moving branch/tag names are not accepted. The same repository
and revision drive the cache lookup and loading, with no hard-coded default
cache directory for an alternate model. A different model must still be
compatible with `FluxPipeline`; changing configuration is not qualification.

The gateway imports no model runtime or model weights at startup. On the first
valid generation request it resolves `model_index.json` through the existing
Hugging Face cache resolver with `local_files_only=True`, then passes that exact
snapshot directory to Diffusers with `local_files_only=True`. Compose also sets
`HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1`. A `refs` directory alone is not a
usable snapshot. Missing pinned manifests return 503 before importing the model
runtime; missing component-file loading errors also return 503. Manifest
resolution is **not** a complete-file/hash audit or proof of GPU compatibility.
The pipeline is published only after successful CUDA placement; loading and
inference retain their serialization locks.

FLUX.1-dev weights are gated under `flux-1-dev-non-commercial-license`. Initial
acquisition requires accepting the repository terms and appropriate Hugging
Face credentials; commercial use requires appropriate separate licensing.
**Existing cached weights do not require a fresh token for offline loading.**
No download is part of this preparation or runtime path.

## Preparation status and no-GPU boundary

The pre-downtime inventory records the pinned snapshot on both Sparks (66f1 and
e8f1), all indexed shards present and no dangling links or `.incomplete` files.
The original GPU image is also present on both hosts. The former assertion that
missing weights/token were the sole blocker is obsolete. Snapshot presence and
an existing image are not a new candidate image receipt, complete byte-hash
parity proof, or generation acceptance.

Preparation may inspect source/configuration, audit existing cached files,
prepare a distinct offline candidate from the exact existing image, and stage
source/image receipts without loading models. Preserve the original image and
caches. The preparation owner records the candidate's full image ID and source
revision, checks those against the lock on both hosts, and supplies `FLUX_IMAGE`
and `FLUX_CACHE` for the staged source. Do not overwrite the original `:latest`
tag, install new dependencies, pull images or download models as a shortcut.

A no-GPU gateway smoke must not request devices or POST `/generate`.
`/openapi.json` returning 200 proves HTTP liveness only: it does **not** prove
weights loaded, GPU admission, memory fit, PNG generation or two-host parity.
The profile and restart policy are operational guardrails, not an admission
controller. Even a manually launched idle service can receive a first request
that loads the model, so keep it stopped outside its authorized serving window.

**Stop before any GPU launch or generation.** Preparation does not authorize
stopping/restarting DeepSeek or TTS, releasing reservations, changing routes,
or taking downtime. DeepSeek TP2 and FLUX must not use the shared GPU pool
simultaneously. Do not run LTX concurrently either. GPU acceptance remains
separately gated on fresh downtime approval and exact-owner admission.

## Future approved serving window (not preparation commands)

### 1. Establish ownership and arrange downtime

Use the canonical
[relocation/recovery contract](../../local-llm-stack/docs/RELOCATION_RECOVERY.md)
in `/Users/statsparrot/projects/local-llm-stack`, together with the current
private postmove recovery guides at
`/Users/statsparrot/private-recovery-guides`. Delegate TP2/TTS shutdown and
restoration to that exact-plan procedure; do not copy generic `make` or Compose
shutdown commands into this worktree.

The currently recorded owner is
`deepseek-tp2-mia-512k-e8f1-d0d964379529`, full digest
`d0d9643795291234c6ca9f45825b3646b8b771728f017a2c27531381dad97327`.
The postmove saved-plan receipt is
`/Users/statsparrot/projects/local-llm-stack/data/cluster/recovery-mia-ablgud4w/started/plan.json`.
Revalidate its live ownership and matching source before any disruption; do not
substitute an older promotion receipt or another owner. Preserve the receipt
and current routes. An unknown/unreachable node is not a released GPU.

The admitted operator must account for both nodes, existing reservations and
speech consumers, and confirm approved releases/free memory. Do not stop TTS
implicitly or use broad `docker compose down`, `make down`, generic
`make deepseek-tp-up`, reservation deletion, or a single-rank `docker start`.

### 2. Start only the admitted candidate

Only after the preceding gate, from the staged Flux worktree on the selected
Spark, with its verified local `FLUX_IMAGE` and original `FLUX_CACHE` configured:

```bash
cd /home/statsparrot/projects/local-llm-stack-flux-prepare-20261117
docker compose --profile flux up -d --no-deps --no-build --pull never flux-dev
docker compose --profile flux logs flux-dev
curl --fail --silent --show-error http://127.0.0.1:8020/openapi.json
```

Use the explicit `flux-dev` target. Omitting it can also select unrelated
unprofiled services. Do not build/pull during GPU admission. Adjust the HTTP
port if `FLUX_PORT` is overridden. OpenAPI liveness still does not load a model.

### 3. Bounded first generation

The following **loads the model onto the GPU** and requires the approved window:

```bash
curl --fail --silent --show-error -X POST http://127.0.0.1:8020/generate \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"a red fox in the snow","width":512,"height":512,"num_inference_steps":4,"seed":0}' \
  -D out.headers -o out.png
file out.png
```

Check HTTP success, `image/png`, valid decoded PNG dimensions, and the
`X-Seconds` header, plus logs and memory. Four steps are a bounded execution
check, not an image-quality acceptance; default quality settings remain 1024
pixels, 30 steps and guidance 3.5. Dimensions must be divisible by 16 and within
256–2048. Existing request ranges and seed validation are unchanged. Inference
is serialized because Diffusers mutates shared scheduler state.

Record each host's actual result separately; do not infer second-host success
from source/image/cache parity. A 503 means the configured local snapshot cannot
be resolved/loaded; inspect the mount, revision and audited component files.
Do not respond by enabling downloads or retrying a busy GPU.

## Stop only Flux, then delegate recovery

From the same staged worktree/project that launched the candidate:

```bash
docker compose --profile flux stop flux-dev
docker compose --profile flux rm -f flux-dev
```

These commands name only the candidate service; do not use project-wide `down`.
Confirm its GPU process is gone before handing the pool back. If it failed to
load or generate, preserve logs/receipts and still complete this exact-service
cleanup before recovery.

Restore TP2 through the canonical exact-plan recovery procedure above, with its
fresh matching receipt and real inference acceptance. The client route is
`http://10.10.10.2:4010/v1`, backend `http://10.10.20.2:8124/v1`, not the old
single-node `localhost:8001` endpoint. Restore only TTS units explicitly stopped
for the authorized attempt, in the canonical order after Mia acceptance.
Do not change routes, expose Flux publicly, or treat a model list/HTTP startup
as successful TP2 recovery.

## Acceptance record required before promotion

Keep preparation and downtime results separate: source revision; candidate full
image ID on each host; pinned cache inventory and byte hashes; effective
loopback/restart/offline/cache Compose configuration; CPU regression results;
no-GPU import/OpenAPI liveness; then, only in an approved window, actual PNG
acceptance, memory/timing, exact Flux shutdown and restored TP2/TTS receipts.
The preparation owner finalizes locks and evidence. No GPU generation or
restoration acceptance is implied by this runbook or by staging the candidate.
