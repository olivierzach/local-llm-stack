FROM nvcr.io/nvidia/pytorch:26.02-py3

WORKDIR /workspace

COPY requirements.txt /tmp/training-requirements.txt

RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir -r /tmp/training-requirements.txt

ENV PYTHONUNBUFFERED=1
