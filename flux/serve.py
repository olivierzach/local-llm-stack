"""FLUX.1-dev text-to-image HTTP gateway (Diffusers FluxPipeline).

Opt-in Compose service (profile "flux"), isolated from the DeepSeek serving
stack. Generates a PNG from a text prompt. Swap the serving window first:
  docker compose --profile flux up -d flux-dev
"""
import io
import os
import threading
import time

from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Response

app = FastAPI(title="flux-dev")

MODEL_ID = os.environ.get(
    "FLUX_MODEL", "black-forest-labs/FLUX.1-dev")

_lock = threading.Lock()
_pipe = None


def _load():
    """Load/init FluxPipeline once, under a lock, publishing only on success
    so a failed CUDA move is never cached as ready. Auto-resolves the repo's
    own model_index.json (FluxTransformer2DModel, T5, clip)."""
    global _pipe
    if _pipe is not None:
        return _pipe
    with _lock:
        if _pipe is not None:
            return _pipe
        import torch
        from diffusers import FluxPipeline
        t0 = time.time()
        pipe = FluxPipeline.from_pretrained(
            MODEL_ID, torch_dtype=torch.bfloat16)
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
    seed: int = Field(default=0)


def _div(client_name, name, value):
    # FLUX works best on dimensions divisible by 16.
    if value % 16 != 0:
        raise HTTPException(status_code=400,
                            detail=f"'{name}' must be divisible by 16, got {value}")


@app.post("/generate")
def generate(req: Req):
    _div(__name__, "height", req.height)
    _div(__name__, "width", req.width)

    pipe = _load()
    import torch
    g = torch.Generator("cuda").manual_seed(req.seed)
    t0 = time.time()
    try:
        out = pipe(prompt=req.prompt, height=req.height, width=req.width,
                   num_inference_steps=req.num_inference_steps,
                   guidance_scale=req.guidance_scale, generator=g)
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
