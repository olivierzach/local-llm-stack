#!/usr/bin/env bash
set -euo pipefail

echo "== Host =="
uname -a
echo

echo "== User =="
id
echo

echo "== Memory =="
free -h
echo

echo "== Disk =="
df -h "$(pwd)" /tmp
echo

echo "== NVIDIA =="
nvidia-smi
echo

echo "== Docker =="
DOCKER_BIN="${DOCKER_BIN:-docker}"
$DOCKER_BIN --version
$DOCKER_BIN compose version
$DOCKER_BIN info >/dev/null
echo "Docker daemon is reachable."

