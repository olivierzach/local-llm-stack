"""FLUX.1-dev text-to-image HTTP gateway (Diffusers FluxPipeline).

Opt-in Compose service (profile "flux"), isolated from the DeepSeek serving
stack. Generates a PNG from a text prompt. Requires the weights to be cached
locally first:  ./scripts/download-model.sh black-forest-labs/FLUX.1-dev
Swap the serving window first:
  docker compose --profile flux up -d flux-dev
"""
import os
import io
import threading
import time
from pathlib import Path

from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Response

app = FastAPI(title="flux-dev")

MODEL_ID = os.environ.get(
    "FLUX_MODEL", "black-forest-labs/FLUX.1-dev")

_lock = threading.Lock()
_pipe = None
_infer_lock = threading.Lock()


def _model_cache_dir() -> Path:
    import huggingface_hub.constants as c
    # HF_HUB_CACHE is a str like /path/to/hub; wrap for path ops.
    return Path(c.HF_HUB_CACHE) / "models--black-forest-labs--FLUX.1-dev"


def _local_cache_ok() -> bool:
    """True if the gated weights are already on disk in the HF cache (hub/)."""
    d = _model_cache_dir()
    # A populated cache has refs/main (and blobs with the safetensors).
    return d.is_dir() and (d / "refs").is_dir()


def _load():
    """Load FluxPipeline once under a lock, publishing only on success.

    Uses local_files_only=True so an absent/partial cache fails fast with a
    clear error instead of triggering a ~24GB online download.
    """
    global _pipe
    if _pipe is not None:
        return _pipe
    with _lock:
        if _pipe is not None:
            return _pipe
        if not _local_cache_ok():
            raise HTTPException(
                status_code=503,
                detail="FLUX.1-dev weights are not cached. Run: "
                       "./scripts/download-model.sh black-forest-labs/FLUX.1-dev "
                       "(requires a valid HF_TOKEN for this gated model) "
                       "before starting generation.")
        import torch
        from diffusers import FluxPipeline
        t0 = time.time()
        pipe = FluxPipeline.from_pretrained(
            MODEL_ID, torch_dtype=torch.bfloat16, local_files_only=True)
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
