#!/usr/bin/env python3
"""Check the repository's real terminal clients through Context Guard."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
MODEL = "local-qwen38-flash-next"


def main():
    compose = shlex.split(os.getenv("DOCKER_COMPOSE", "docker compose"))
    output_dir = ROOT / "evals/runs/qwen38"
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = output_dir / f"{time.strftime('%Y%m%dT%H%M%S')}-clients.jsonl"
    prompt = "Do not call any tools. Reply with exactly: ready"
    clients = {
        "aichat": ["aichat", "--model", f"spark:{MODEL}", prompt],
        "opencode": ["opencode", "run", "--model", f"spark/{MODEL}", prompt],
    }
    for name, args in clients.items():
        start = time.monotonic()
        container = f"local-qwen38-check-{name}-{os.getpid()}"
        try:
            result = subprocess.run(
                compose + ["--profile", "tui", "run", "--rm", "--no-deps", "-T", "--name", container] + args,
                cwd=ROOT, capture_output=True, text=True, timeout=600,
            )
        finally:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
        record = {"client": name, "exit_code": result.returncode,
                  "elapsed_s": round(time.monotonic() - start, 3),
                  "ready_received": "ready" in result.stdout.lower()}
        # Client logs can contain environment diagnostics; persist only allowlisted results.
        with result_path.open("a") as output:
            output.write(json.dumps(record) + "\n")
        print(json.dumps(record), flush=True)
        if result.returncode or not record["ready_received"]:
            raise RuntimeError(f"{name} failed; inspect the client interactively with make {name} MODEL={MODEL}")
    print(f"Results: {result_path}")


if __name__ == "__main__":
    main()
