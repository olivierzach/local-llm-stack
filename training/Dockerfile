FROM nvcr.io/nvidia/pytorch:26.02-py3

WORKDIR /workspace

RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir \
      "jupyterlab>=4.2" \
      "ipywidgets>=8.1" \
      "datasets>=3.0" \
      "evaluate>=0.4" \
      "transformers>=4.55" \
      "accelerate>=1.0" \
      "peft>=0.13" \
      "trl>=0.11" \
      "bitsandbytes>=0.43" \
      "sentencepiece>=0.2" \
      "protobuf>=5.0" \
      "wandb>=0.18"

ENV PYTHONUNBUFFERED=1

