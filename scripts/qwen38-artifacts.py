#!/usr/bin/env python3
"""Check pinned Qwen artifacts without loading the model or touching services."""

import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    pins = dict(line.split("=", 1) for line in (ROOT / "config/qwen38-pins.env").read_text().splitlines()
                if line and not line.startswith("#"))
    runtime = subprocess.check_output(
        [sys.executable, str(ROOT / "scripts/resolve-spark-runtime-image.py")], text=True).strip()
    path = ROOT / "data/huggingface/hub" / ("models--" + pins["QWEN38_MODEL"].replace("/", "--"))
    snapshot = path / "snapshots" / pins["QWEN38_MODEL_REVISION"]
    index = json.loads((snapshot / "model.safetensors.index.json").read_text())
    shards = set(index["weight_map"].values())
    if not shards:
        raise ValueError("empty weight index")
    for name in shards | {"config.json", "tokenizer.json", "tokenizer_config.json", "chat_template.jinja",
                           "preprocessor_config.json", "video_preprocessor_config.json"}:
        candidate = snapshot / name
        if not candidate.is_file() or candidate.stat().st_size == 0:
            raise ValueError(f"missing/empty checkpoint file: {name}; run make qwen38-install")
    print(json.dumps({"recipe": pins["QWEN38_RECIPE_REF"], "image": pins["QWEN38_IMAGE"], "runtime_image": runtime,
                      "checkpoint": pins["QWEN38_MODEL_REVISION"], "shards": len(shards),
                      "weight_bytes": sum((snapshot / name).stat().st_size for name in shards)}))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"qwen38 artifacts: {error}", file=sys.stderr)
        sys.exit(1)
