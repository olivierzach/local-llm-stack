#!/usr/bin/env bash
set -euo pipefail

IMAGE="${CUDA_TEST_IMAGE:-nvidia/cuda:13.0.0-base-ubuntu24.04}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
$DOCKER_BIN run --rm --gpus all "$IMAGE" nvidia-smi

