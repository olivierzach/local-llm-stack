"""FLUX.1-dev text-to-image HTTP gateway (Diffusers FluxPipeline).

Opt-in Compose service (profile "flux"). No model is loaded until /generate.
Generation requires explicit GPU admission and a locally cached, pinned
snapshot; neither startup nor generation downloads model files.
"""
import os
import io
import re
import threading
import time
from pathlib import Path

from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Response

app = FastAPI(title="flux-dev")

DEFAULT_MODEL_ID = "black-forest-labs/FLUX.1-dev"
DEFAULT_REVISION = "3de623fc3c33e44ffbe2bad470d0f45bccf2eb21"
MODEL_ID = os.environ.get("FLUX_MODEL") or DEFAULT_MODEL_ID
MODEL_REVISION = os.environ.get("FLUX_REVISION") or (
    DEFAULT_REVISION if MODEL_ID == DEFAULT_MODEL_ID else None)

_lock = threading.Lock()
_pipe = None
_infer_lock = threading.Lock()


def _local_snapshot() -> Path:
    """Resolve the exact cached pipeline manifest, never a moving ref."""
    if not MODEL_REVISION or not re.fullmatch(r"[0-9a-fA-F]{40}", MODEL_REVISION):
        raise HTTPException(
            status_code=503,
            detail=f"Set FLUX_REVISION to a full commit hash for {MODEL_ID}.")
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import LocalEntryNotFoundError

    try:
        manifest = hf_hub_download(
            repo_id=MODEL_ID, filename="model_index.json",
            revision=MODEL_REVISION, local_files_only=True)
    except (LocalEntryNotFoundError, OSError) as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Local snapshot {MODEL_ID}@{MODEL_REVISION} is unavailable. "
                   "Verify the existing HF cache mount and pinned model files; "
                   "runtime downloads are disabled.") from exc
    # Keep the snapshot path, not the resolved blob's parent. Diffusers checks
    # the remaining components while loading; this is not a completeness audit.
    return Path(manifest).parent


def _load():
    """Load FluxPipeline once under a lock, publishing only on success.

    Resolves a pinned local snapshot before importing the model runtime.
    Diffusers also uses local_files_only=True for missing component files.
    """
    global _pipe
    if _pipe is not None:
        return _pipe
    with _lock:
        if _pipe is not None:
            return _pipe
        snapshot = _local_snapshot()
        import torch
        from diffusers import FluxPipeline
        t0 = time.time()
        try:
            pipe = FluxPipeline.from_pretrained(
                str(snapshot), torch_dtype=torch.bfloat16, local_files_only=True)
        except OSError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Cannot load local snapshot {MODEL_ID}@{MODEL_REVISION}. "
                       "Verify all pinned component files in the existing cache; "
                       "runtime downloads are disabled.") from exc
        print(f"[flux] built pipeline in {time.time()-t0:.1f}s", flush=True)
        pipe.to("cuda")
        _pipe = pipe
        return _pipe


class Req(BaseModel):
    prompt: str
    height: int = Field(default=1024, ge=256, le=2048)
    width: int = Field(default=1024, ge=256, le=2048)
    num_inference_steps: int = Field(default=30, ge=1, le=100)
    guidance_scale: float = Field(default=3.5, ge=0.0, le=20.0)
    # PyTorch's manual_seed accepts [-2^63, 2^64-1]; reject out-of-range seeds
    # before spending time on model loading.
    seed: int = Field(default=0, ge=-9223372036854775808, le=18446744073709551615)


def _div(name, value):
    # FLUX works best on dimensions divisible by 16 (packed latent vae_scale*2).
    if value % 16 != 0:
        raise HTTPException(status_code=400,
                            detail=f"'{name}' must be divisible by 16, got {value}")


@app.post("/generate")
def generate(req: Req):
    _div("height", req.height)
    _div("width", req.width)

    pipe = _load()
    import torch
    g = torch.Generator("cuda").manual_seed(req.seed)
    t0 = time.time()
    try:
        # Serialize inference: Diffusers mutates shared scheduler state during
        # __call__ (set_begin_index, step, _step_index), so concurrent requests
        # would corrupt denoising.
        with _infer_lock:
            out = pipe(prompt=req.prompt, height=req.height, width=req.width,
                       num_inference_steps=req.num_inference_steps,
                       guidance_scale=req.guidance_scale, generator=g)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500,
                            detail=f"generation failed: {exc}")
    img = out.images[0]
    dur = time.time() - t0
    print(f"[flux] generated image in {dur:.1f}s", flush=True)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return Response(content=buf.getvalue(), media_type="image/png",
                    headers={"X-Seconds": f"{dur:.1f}"})
