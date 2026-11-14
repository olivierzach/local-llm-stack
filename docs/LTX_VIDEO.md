# LTX-Video service: bring-up and shutdown

Standalone text/image-to-video service using the Lightricks **LTX-Video
0.9.8-13B-distilled** model via Diffusers. It runs as its own container
(`ltx-video`) under the opt-in `ltx` Compose profile — **separate from the
DeepSeek/TP2 LLM serving** in this repo.

- Model: `Lightricks/LTX-Video-0.9.8-13B-distilled`
- Image: `local-llm-stack-ltx-video:latest` (build from `ltx-video/Dockerfile`)
- Runtime: `serve.py` → FastAPI `LTXConditionPipeline` gateway
- Host port: `${LTX_PORT:-8013}` (container `8001`)
- Source cache: `data/huggingface` (works from the worktree root)
- Parity: identical worktree + image + weights are present on **both** sparks
  (66f1 and e8f1); run the service on whichever node is free.

> **Memory contract (critical):** LTX-13B needs on the order of 26 GB+ of GPU
> unified memory. The DeepSeek TP2 serving pins ~78% of the same unified pool
> on both nodes, so **the two cannot run simultaneously**. Bringing LTX up
> requires first stopping (or shrinking) the DeepSeek serving, and vice-versa.
> This is a deliberate sequential switch-over, not co-residency.

---

## Prerequisites (one-time, already done in `feat/ltx-video`)

1. Weights present in `data/huggingface/hub/models--Lightricks--LTX-Video-0.9.8-13B-distilled` (~45 GB, 21 `.safetensors` shards).
2. Image built and tagged `local-llm-stack-ltx-video:latest`.

If either is missing, provision it:

```bash
# from the worktree root (e.g. /home/statsparrot/projects/local-llm-stack-ltx)
./scripts/download-model.sh Lightricks/LTX-Video-0.9.8-13B-distilled   # ~45 GB into data/huggingface
docker compose --profile ltx build ltx-video                            # build the image
```

To mirror these to the other spark over the QSFP fabric:

```bash
# image over the direct spark-to-spark cable
python3 scripts/sync-spark-images.py \
  --lock cluster/ltx-images.lock.json \
  --peer <e8f1|66f1> \
  --fabric-inventory ../local-llm-stack/cluster/inventory.json \
  --apply
# weights: rsync -a --checksum the model dir over the same fabric (-e ssh ...-o BindInterface=<rail>)
```

---

## Bring-up

### 1. Free the GPU (take DeepSeek down) — ONLY when you have an approved window

```bash
# from the main stack directory
DOCKER_COMPOSE="sudo docker compose" make down        # stop the DeepSeek/TP2 serving
#   -- or, to keep other services but stop only the LLM serving, stop the specific vLLM container(s)
```

Verify unified memory is actually free:

```bash
nvidia-smi                    # DeepSeek process gone / memory released
grep MemAvailable /proc/meminfo
```

### 2. Start the LTX service (from the worktree root)

```bash
cd /home/statsparrot/projects/local-llm-stack-ltx
docker compose --profile ltx up -d ltx-video
```

> Use the explicit service target `ltx-video`. `--profile ltx up` with no target
> also selects unprofiled services (including GPU-consuming `vllm-fast`), which
> would contend for memory.

### 3. Watch it come up

```bash
docker compose --profile ltx logs -f ltx-video
```

Wait for the log line `[ltx] pipeline on CUDA in ...s` — this confirms the model
loaded into GPU memory. The first load rebuilds/loads the pipeline and can take
a minute or two.

### 4. Sanity-check the endpoint

```bash
curl -s http://127.0.0.1:8013/docs      # FastAPI OpenAPI reachable
```

Then generate a short text-to-video test:

```bash
curl -sS -X POST http://127.0.0.1:8013/video \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"a red ball bouncing","num_frames":25,"width":384,"height":384,"num_steps":30}' \
  -o out.gif
```

Validating the response:

```bash
file out.gif          # GIF image data
```

Optional image-to-video (send a base64 PNG):

```bash
curl -sS -X POST http://127.0.0.1:8013/video \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"pan across the scene","start_image":"<BASE64_PNG>","num_frames":49}' \
  -o out2.gif
```

---

## Shutdown

### Stop the LTX service

```bash
cd /home/statsparrot/projects/local-llm-stack-ltx
docker compose --profile ltx down            # removes the ltx-video container
```

### Restore DeepSeek serving

```bash
# from the main stack directory
DOCKER_COMPOSE="sudo docker compose" make up         # bring the LLM/TP2 serving back
```

Verify serving returned (from the repo runbook):

```bash
curl -sS http://localhost:8001/v1/models            # or the port/alias in use
```

---

## Useful verification / notes

- **Which spark runs it:** with parity on both nodes, start `ltx-video` on the
  node whose GPU is idle in the takedown window. Confirm no DeepSeek process is
  on that node first (`ps aux | grep -i deepseek`, `nvidia-smi`).
- **Full parity check** (per-node):
  - worktree: `git -C <worktree> rev-parse --short HEAD` → `4b4e887` on both
  - image: `docker images --format '{{.ID}}' local-llm-stack-ltx-video:latest`
    → `a3c8b42eaf7e` on both
  - weights: `du -sh data/huggingface/hub/models--Lightricks--LTX-Video-0.9.8-13B-distilled` → 45 GB, 0 dangling symlinks
- **Licensing:** LTX-Video weights are open-weights (LTX Open Weights License),
  free for commercial use under <$10M annual revenue; a commercial license
  applies above that or when embedding weights in a sold product.
- **Service output:** returns GIF at 25 fps (frame duration 40 ms), matching the
  generation frame rate. Latency depends on steps/frame count.

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `RuntimeError: CUDA out of memory` on load | DeepSeek serving not actually freed; check `nvidia-smi`, fully stop serving, retry |
| Model re-downloads at first request | `HF_HOME`/cache not resolving to `data/huggingface`; ensure the service mounts `./data/huggingface:/workspace/cache/huggingface` and runs from the worktree root |
| `docker compose ... up -d ltx-video` also starts vLLM | `make up`/unprofiled services selected; use the explicit `ltx-video` target |
| Endpoint on 8013 refused | container not up or crashed (`docker compose --profile ltx ps a`, check logs) |
| Slow first request | pipeline building from cache; subsequent requests reuse the loaded pipeline |
