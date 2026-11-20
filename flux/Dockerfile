FROM nvcr.io/nvidia/pytorch:26.02-py3

WORKDIR /workspace

# FLUX.1-dev text-to-image service: diffusers FluxPipeline + FastAPI gateway.
# Opt-in Compose service (profile "flux"), isolated from the DeepSeek serving.
# Versions pinned to the ones validated at build time. NOTE: FLUX.1-dev weights
# are gate-gated (non-commercial license); download needs a valid HF_TOKEN.
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir \
        diffusers==0.42.1 \
        transformers==5.22.2 \
        accelerate==1.15.0 \
        safetensors==0.8.0 \
        sentencepiece==0.2.3 \
        fastapi==0.162.5 \
        "uvicorn[standard]==0.54.0" \
        huggingface-hub==2.4.0

COPY serve.py /workspace/serve.py

ENV PYTHONUNBUFFERED=1
EXPOSE 8001
ENTRYPOINT ["python3", "-m", "uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8001"]
