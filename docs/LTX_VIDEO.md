# LTX-Video: preparation, admission and recovery

Standalone text/image-to-video gateway using **LTX-Video 0.9.8-13B-distilled**
through Diffusers `LTXConditionPipeline`. The opt-in Compose service is
`ltx-video` (profile `ltx`), separate from DeepSeek TP2. Its existing API is
`POST /video`, returning **GIF**, not MP4. No Fam-Chat integration is implied.

## Pinned inputs and safe defaults

- Model: `Lightricks/LTX-Video-0.9.8-13B-distilled`.
- Revision: `7c64400e1861cc0d7b98d570a1926d5408ec60cd`, selected by
  `LTX_MODEL_REVISION`. Keep this revision paired with the cached model.
- Candidate image: `${LTX_IMAGE:-local-llm-stack-ltx-video:prepare-20261117-r2}`.
  Before admission, verify the selected image's exact ID against
  `cluster/ltx-images.lock.json` and the preparation receipt on the selected
  host. A tag alone is not identity or acceptance evidence. `LTX_IMAGE` may be
  set to that verified local image ID.
- Cache: `${LTX_CACHE:-./data/huggingface}` mounted at
  `/workspace/cache/huggingface`. Candidate worktrees should set `LTX_CACHE`
  to the **existing original cache directory**, not create another empty cache
  or download weights. Preserve original caches and their symlink targets.
- Endpoint: `127.0.0.1:${LTX_PORT:-8013}` → container port `8001`.
  This service is loopback-only, independent of the stack's `BIND_HOST`.
- `restart: "no"`: GPU admission is manual; do not enable automatic restart
  alongside TP2 after a reboot.

The pipeline is **lazy**: importing the app, starting Uvicorn or fetching
`/openapi.json` does not load model weights. The first valid generation request
loads the pinned snapshot with `local_files_only=True`, then moves it to CUDA.
An absent/partial snapshot fails instead of downloading. Initialization is
published only after the CUDA move succeeds. Pipeline calls are serialized by
a separate inference lock because Diffusers mutates shared scheduler state.
The installed Diffusers 0.42.1 accepts the existing `image=` conditioning keyword;
this gateway retains that API.

**Required runtime repair:** the original image could start HTTP but failed
the offline tokenizer check because SentencePiece was absent. Adding
`sentencepiece==0.2.3` exposed its generated Protobuf code's incompatibility
with the inherited `protobuf==6.33.5`. The Dockerfile pins the compatible pair
`sentencepiece==0.2.3` and `protobuf==7.36.1`; tiktoken and runtime-version
check suppression are not fixes. Both wheels must be checksum-verified and
installed offline. Repeat actual cached-tokenizer loading before admission;
pipeline/config imports alone did not expose these blockers.

## Preparation boundary: no downtime or generation

Source preparation, CPU regression tests, cache/image identity checks and an
isolated CPU-only app/OpenAPI smoke are **not GPU generation acceptance**.
Fifteen focused CPU regressions passed for serialization, failed-load state,
lock release and input rejection. They do not establish model quality,
GPU memory sufficiency or real text/image generation.

On 66f1, the repaired candidate passed actual offline pipeline imports and
cached T5 tokenization with SentencePiece 0.2.3 / Protobuf 7.36.1. Its three
meta-device component/config-to-checkpoint comparisons passed, accounting for
tied T5 embeddings, without materializing weight tensors or initializing CUDA.
The HTTP smoke returned OpenAPI and rejected invalid requests without model
runtime imports. Both hosts' retained Flux/LTX blobs passed content-address
hash and safetensors payload-range checks, with all indexed shards present.
These checks verify retained cache identities, not an upstream signature.

The image lock selects the repaired `-r2` candidate, whose source/build inputs
are recorded at commit `6edba7e`; the first candidate exposed the Protobuf
incompatibility and is not the admission candidate. Host-specific ignored
`.env` files select the immutable image ID and original cache directory.
Private preparation receipts: Mini
`/Users/statsparrot/projects/local-llm-stack/data/cluster/generation-prep-20261117/`
and each Spark `~/projects/local-llm-stack/data/generation-prep-20261117/`.
The current canonical DeepSeek render exactly matches the postmove saved plan;
no stop/start, reservation change or GPU generation was performed.

Until a fresh downtime window is approved:

- Do not stop/restart DeepSeek or TTS, load the LTX pipeline or POST a valid
  generation request to a GPU-capable service.
- Do not pull images, download models, run broad Compose commands or start the
  GPU-configured service just to prove HTTP liveness.
- Preserve the current serving owner, routes, caches and recovery receipts.
- Leave real text-to-video, image-to-video and restoration acceptance deferred.

## Canonical DeepSeek recovery contract

Use the **canonical main-source** recovery guide, not this feature worktree's
historical TP instructions:

`/Users/statsparrot/projects/local-llm-stack/docs/RELOCATION_RECOVERY.md`

The recorded current identity is:

| Item | Value |
|---|---|
| Owner | `deepseek-tp2-mia-512k-e8f1-d0d964379529` |
| Full digest | `d0d9643795291234c6ca9f45825b3646b8b771728f017a2c27531381dad97327` |
| Saved plan | `/Users/statsparrot/projects/local-llm-stack/data/cluster/recovery-mia-ablgud4w/started/plan.json` |
| Client route | `http://10.10.10.2:4010/v1` |
| Backend | `http://10.10.20.2:8124/v1` |
| Alias | `local-deepseek-v4-flash-mia` |

Recheck actual ownership against that contract before disruption. A newer owner,
missing receipt, unreachable peer or render mismatch is a stop boundary, not
permission to force recovery. Generic `make down` / `make up` and broad Compose
`down` do **not** implement current TP2 recovery. Do not replay older GLM/Mia
plans, alter reservations, republish routes or start a lone TP rank.

## Future approved GPU window only

### 1. Drain and release the exact serving owner

Obtain fresh downtime/GPU admission approval. Follow the canonical recovery
contract's identity/receipt comparison and authorized exact-plan teardown.
Account for both TP2 peers and every other GPU consumer. Record TTS state and
only stop speech units if that operation is explicitly authorized; preserve the
state needed for later restoration. Keep private recovery guides and receipts
available. This guide does not authorize any stop.

Verify both exact-owned TP2 containers/reservations have been released, and
inspect actual available memory/remaining GPU processes. LTX and the current
TP2 serving are a **sequential switch-over**, not qualified co-residents. Busy
or unknown ownership is not a reason to bypass admission.

### 2. Start only the prepared LTX service

On the selected Spark, from the staged
`/home/statsparrot/projects/local-llm-stack-ltx-prepare-20261117` worktree, with
`LTX_IMAGE` verified and `LTX_CACHE` pointing to the original cache:

```sh
docker compose --profile ltx up -d --no-deps --no-build --pull never ltx-video
docker compose --profile ltx logs --tail 100 ltx-video
curl --fail --silent --show-error http://127.0.0.1:8013/openapi.json
```

Always use the explicit service target and these no-dependency/no-build/no-pull
flags. A bare profile `up` also selects unprofiled services. Do not use it.
Adjust the URL if `LTX_PORT` is overridden.

OpenAPI proves **HTTP liveness only**. There is no startup model-ready log to
wait for. The `[ltx] built pipeline ...` message occurs during the first valid
request and precedes the CUDA move; it is not generation acceptance either.

### 3. Accept real text and image generation separately

Only within the approved GPU window, issue a bounded text request:

```sh
curl --fail-with-body --silent --show-error \
  -D text-video.headers \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"a red ball bouncing","num_frames":25,"width":384,"height":384,"num_steps":30,"seed":0}' \
  http://127.0.0.1:8013/video -o text-video.gif
```

For image conditioning, create a private JSON request containing a real base64
PNG in `start_image` (not a filename, URL or data-URL prefix), the intended
prompt, `num_frames: 25`, `width: 384`, `height: 384`, `num_steps: 30` and a fixed
seed. Send that prepared payload:

```sh
curl --fail-with-body --silent --show-error \
  -D image-video.headers \
  -H 'Content-Type: application/json' --data-binary @image-video-request.json \
  http://127.0.0.1:8013/video -o image-video.gif
```

Both cases are required for real API acceptance. Preserve HTTP status, response
headers (`Content-Type: image/gif`, `X-Frames`, `X-Seconds`), latency, exact
source/image/model identity and decoded output dimensions/frame count. Decode
and visually inspect the GIFs for meaningful text and image-conditioned
motion; a file extension, HTTP 200 or CPU-instrumented output is insufficient.
The GIF uses 25 fps (40 ms per frame). Keep artifacts private.

Request constraints: positive height/width divisible by 32; 9–601 requested
frames; 1–100 steps; seed in PyTorch's `[-2^63, 2^64-1]` range. Use `8n+1`
frame counts for LTX's temporal layout. Nonconforming temporal counts retain
the installed pipeline's behavior; the gateway does not rewrite conditioning
or invent a new frame API. Allowed dimensions/frame counts do not guarantee
that a request fits GPU memory.

### 4. Targeted teardown and exact restoration

On every host where LTX was started for this attempt, from that same candidate
worktree/project:

```sh
docker compose --profile ltx stop ltx-video
docker compose --profile ltx rm -f ltx-video
```

Do not run Compose `down`, remove cache volumes or affect unrelated services.
Confirm the LTX container/process is gone and memory is released before
restoring TP2. On failure, preserve evidence and clean only this attempt's
confirmed service/owner; an unreachable host is not proof of cleanup.

Return to the canonical recovery contract for exact-plan preflight, current
Mia TP2 launch and acceptance on **both** peers. Require matching owner/digest,
healthy TP2, real inference and tool/client acceptance through the existing
4010 route; `/v1/models` alone is not restoration. Do not republish unchanged
routes. Restore only speech units stopped for this attempt, after TP2 inference
passes and subject to the existing memory guards. Record final DeepSeek/TTS
state and any deferred acceptance explicitly.

## Troubleshooting

| Symptom | Action |
|---|---|
| Missing local snapshot/shards | Stop; compare `LTX_CACHE`, model/revision and cache receipt. Offline failure is intentional; no automatic download fallback. |
| CUDA out of memory | Stop LTX with the targeted commands; account for ownership/memory and follow the approved recovery path. Do not force-stop an unrelated workload. |
| Candidate image missing/mismatched | Stop before launch; resolve staged image identity with the preparation owner. Do not build/pull during admission. |
| Port refused | Inspect only `ltx-video` status/logs and the configured loopback port. Startup HTTP liveness is separate from model readiness. |
| First valid request slow | Lazy pipeline load/CUDA transfer happens on that request; later requests reuse it. Concurrent requests wait for serialized inference. |

Licensing remains governed by the model's LTX Open Weights License, including
its commercial-use conditions; inspect the applicable model license for the
intended use.
