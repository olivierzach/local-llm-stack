FROM nvcr.io/nvidia/pytorch:26.02-py3

WORKDIR /workspace

# LTX-Video server runtime: diffusers pipeline + FastAPI HTTP gateway.
# Opt-in Compose service (profile "ltx"), separate from the DeepSeek serving
# image. Versions pinned to the ones validated at build time.
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir \
        diffusers==0.42.1 \
        transformers==5.22.2 \
        accelerate==1.15.0 \
        safetensors==0.8.0 \
        einops==0.8.2 \
        imageio==2.38.1 \
        imageio-ffmpeg==0.8.1 \
        fastapi==0.162.5 \
        "uvicorn[standard]==0.54.0" \
        huggingface-hub==2.4.0

COPY serve.py /workspace/serve.py

ENV PYTHONUNBUFFERED=1
EXPOSE 8001
ENTRYPOINT ["python3", "-m", "uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8001"]
