"""LTX-Video HTTP gateway (Diffusers).

Opt-in Compose service (profile "ltx"), separate from the DeepSeek serving
stack. Loads the LTX model on demand and generates a short video from a text
prompt and optional start image. After explicit GPU admission, start only:
  docker compose --profile ltx up -d --no-deps --no-build --pull never ltx-video
"""
import base64
import io
import os
import threading
import time

from PIL import Image
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Response

app = FastAPI(title="ltx-video")

MODEL_ID = os.environ.get(
    "LTX_MODEL", "Lightricks/LTX-Video-0.9.8-13B-distilled")
MODEL_REVISION = os.environ.get(
    "LTX_MODEL_REVISION", "7c64400e1861cc0d7b98d570a1926d5408ec60cd")
FPS = 25

_lock = threading.Lock()
_pipe = None
_infer_lock = threading.Lock()


def _load():
    """Load/init LTXConditionPipeline once, under a lock, publishing only on
    success so a failed CUDA move is never cached as ready."""
    global _pipe
    if _pipe is not None:
        return _pipe
    with _lock:
        if _pipe is not None:
            return _pipe
        import torch
        from diffusers import LTXConditionPipeline
        t0 = time.time()
        # No hand-constructed components: from_pretrained resolves the repo's
        # own model_index.json (AutoencoderKLLTXVideo, LTXVideoTransformer3DModel,
        # T5EncoderModel). Resolve only the pinned snapshot in the existing
        # HF_HOME cache; an absent/partial cache must fail without downloading.
        pipe = LTXConditionPipeline.from_pretrained(
            MODEL_ID, revision=MODEL_REVISION,
            torch_dtype=torch.bfloat16, local_files_only=True)
        print(f"[ltx] built pipeline in {time.time()-t0:.1f}s", flush=True)
        pipe.to("cuda")
        _pipe = pipe
        return _pipe


class Req(BaseModel):
    prompt: str
    start_image: str | None = None
    num_frames: int = Field(default=121, ge=9, le=601)
    height: int = Field(default=384, gt=0)
    width: int = Field(default=672, gt=0)
    num_steps: int = Field(default=30, ge=1, le=100)
    # Match torch.Generator.manual_seed's accepted integer range.
    seed: int = Field(default=0, ge=-9223372036854775808, le=18446744073709551615)


def _div32(name, value):
    if value % 32 != 0:
        raise HTTPException(status_code=400,
                            detail=f"'{name}' must be divisible by 32, got {value}")


@app.post("/video")
def generate(req: Req):
    _div32("height", req.height)
    _div32("width", req.width)

    start_img = None
    if req.start_image:
        try:
            start_img = Image.open(
                io.BytesIO(base64.b64decode(req.start_image))).convert("RGB")
        except Exception as exc:
            raise HTTPException(status_code=400,
                                detail=f"invalid start_image: {exc}")

    pipe = _load()
    import torch
    g = torch.Generator("cuda").manual_seed(req.seed)
    t0 = time.time()
    kwargs = dict(prompt=req.prompt, num_frames=req.num_frames,
                  height=req.height, width=req.width,
                  num_inference_steps=req.num_steps, generator=g)
    if start_img is not None:
        kwargs["image"] = start_img
    try:
        # Diffusers mutates shared scheduler state during denoising. Keep
        # requests serialized even after the lazy load lock has been released.
        with _infer_lock:
            out = pipe(**kwargs)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"generation failed: {exc}")
    frames = list(out.frames[0])
    dur = time.time() - t0
    print(f"[ltx] generated {len(frames)} frames in {dur:.1f}s", flush=True)

    delay = int(round(1000 / FPS))
    gif = io.BytesIO()
    frames[0].save(gif, format="GIF", save_all=True,
                   append_images=frames[1:], duration=delay, loop=0)
    return Response(content=gif.getvalue(), media_type="image/gif",
                    headers={"X-Frames": str(len(frames)),
                             "X-Seconds": f"{dur:.1f}"})
