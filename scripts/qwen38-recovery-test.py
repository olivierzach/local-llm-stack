#!/usr/bin/env python3
"""Exercise real upstream overflow with an isolated, deliberately undercounting Guard."""

import importlib.util
import json
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main():
    guard = load("recovery_guard", ROOT / "scripts/context-guard-proxy.py")
    verify = load("recovery_verify", ROOT / "scripts/qwen38-verify.py")
    verify.load_env()
    import os
    import subprocess
    import requests
    gateway = os.getenv("QWEN38_BIND_HOST") or subprocess.check_output(
        ["docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"], text=True).strip()
    backend = f"http://{gateway}:{os.getenv('QWEN38_PORT', '8012')}"
    alias = verify.MODEL
    key = os.environ["LITELLM_MASTER_KEY"]
    config = guard.build_config(guard.parser().parse_args([]))
    config.upstream_base_url = "http://localhost:4000/v1"
    config.tokenizer_base_urls[alias] = backend
    config.compact_model = alias
    config.tokenizer_timeout_s = 60
    events = []
    result_dir = ROOT / "evals/runs/qwen38"
    result_dir.mkdir(parents=True, exist_ok=True)
    result_path = result_dir / f"{time.strftime('%Y%m%dT%H%M%S')}-recovery.jsonl"

    class UndercountOnce(guard.ContextGuardHandler):
        def estimate_input_tokens(self, payload):
            if not getattr(self, "_sent_once", False):
                return 1
            return super().estimate_input_tokens(payload)

        def post_upstream(self, headers, payload):
            response = super().post_upstream(headers, payload)
            self._sent_once = True
            events.append(response.status_code if response is not None else None)
            return response

    server = guard.ContextGuardServer(("127.0.0.1", 0), UndercountOnce, config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    limit = config.context_limit_for(alias)
    try:
        for streaming in (False, True):
            events.clear()
            # " x" is a single token for this tokenizer; validate the oversize condition.
            request = verify.payload([{"role": "user", "content": " x" * (limit + 4096) + "\nReply with exactly: ready"}],
                                     stream=streaming)
            tokenized = requests.post(backend + "/tokenize", json={
                "model": alias, "messages": request["messages"],
                "chat_template_kwargs": request["chat_template_kwargs"]}, timeout=60)
            tokenized.raise_for_status()
            assert tokenized.json()["count"] > limit
            record, message = verify.chat(f"http://127.0.0.1:{server.server_port}/v1", request, key)
            assert events[0] in (400, 413, 422), events
            assert events[-1] == 200 and len(events) >= 2, events
            assert record["guard"]["x-context-guard"] == "compacted", record
            assert int(record["guard"]["x-context-retry"]) >= 1, record
            assert message["content"], message
            result = json.dumps({"stream": streaming, "upstream_statuses": events, **record})
            with result_path.open("a") as output:
                output.write(result + "\n")
            print(result, flush=True)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
