#!/bin/bash
# Rebuild the isolated Linux aarch64 verification environment.
# Requires a compatible CUDA base image and uv; inherits its system packages.
# Run only in a NEW CPU-only container, never in a serving container.
# The caller must bound CPU/memory and provide no GPU devices.
# The official native wheel and FlashInfer are pinned; core versions are checked.
# This is not a complete OS/dependency lockfile; record the resulting inventory.
set -euo pipefail
uv venv --allow-existing --system-site-packages /opt/e2e/.venv
uv pip install --python /opt/e2e/.venv/bin/python \
  --index-strategy unsafe-best-match \
  --extra-index-url https://flashinfer.ai/whl/ \
  'https://wheels.vllm.ai/d2125ea681784c17251c47939f5ed6d1c67384f8/vllm-0.30.1rc1.dev535%2Bgd2125ea68-cp38-abi3-manylinux_2_28_aarch64.whl' \
  'flashinfer-python==0.7.0.post1' 'flashinfer-cubin==0.7.0.post1' \
  'pytest==9.1.1' 'pytest-timeout==2.4.0' 'pytest-asyncio==1.4.0' \
  'multiprocess==0.70.16' 'dill==0.3.8'
/opt/e2e/.venv/bin/python -c 'import hashlib, importlib.metadata as m, json, pathlib, torch, vllm; p=pathlib.Path(vllm.__file__).parent; expected={"vllm":"0.30.1rc1.dev535+gd2125ea68","torch":"2.13.0+cu130","triton":"3.7.1","flashinfer-python":"0.7.0.post1","flashinfer-cubin":"0.7.0.post1"}; actual={n:m.version(n) for n in expected}; actual["torch"]=torch.__version__; assert actual == expected, actual; print(json.dumps({"vllm_file":str(p),"versions":actual,"torch_distribution_version":m.version("torch"),"executor_sha256":hashlib.sha256((p/"v1/executor/multiproc_executor.py").read_bytes()).hexdigest()}, indent=2))'
uv pip freeze --python /opt/e2e/.venv/bin/python
