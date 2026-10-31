"""Startup warmup for speculative-decode engines.

The DeepSeek MIA lane serves a DSpark spec-decode vLLM engine at
``max_num_seqs=1``. Its DSpark draft kernels (``_prepare_dflash_inputs_kernel``
and friends) are TileLang/TRITON JIT-compiled lazily on the first *speculative*
decode step. Under single concurrency that cold compile blocks the engine
scheduler; ``/health`` still returns 200 so neither the Compose healthcheck nor
a naive liveness probe notices, and the first real request stalls for minutes.

The small acceptance ``probe`` (16 tokens, max-num-seqs 1) does not reliably
exercise the speculative draft path, so the kernels can stay cold into live
traffic. :func:`warmup_speculative_payload` builds a generation large enough to
run multiple draft steps; :func:`run_warmup` fires it against the coordinator
endpoint before the acceptance probe, so the one-time JIT happens off the
critical client path and its result persists in the ``runtime-cache`` volume.
"""
from __future__ import annotations

import json
import time

import urllib.request


def health_root(base_url):
    """Return the non ``/v1`` origin a vLLM health/metircs endpoint lives at."""
    url = base_url.rstrip("/")
    if url.endswith("/v1"):
        url = url[: -len("/v1")]
    return url


def warmup_speculative_payload(alias, speculative, max_output_cap=64):
    """Build a warmup chat/completions payload sized to run draft steps.

    ``speculative`` is the recipe's ``speculative_config`` (or ``None``).
    We ask for at least ``2 * num_speculative_tokens`` tokens so several draft
    proposals with acceptance/rejection actually exercise the DSpark kernels,
    capped at ``max_output_cap``. Without speculative config a minimum prompt
    echo still forces a decode step.
    """
    num_spec = 2
    if isinstance(speculative, dict):
        num_spec = speculative.get("num_speculative_tokens", 2)
    max_tokens = min(max(int(num_spec) * 8, 16), max_output_cap)
    return {
        "model": alias,
        "messages": [{"role": "user", "content": "Reply with a single word: warm"}],
        "max_tokens": max_tokens,
        "temperature": 0,
    }


def run_warmup(base_url, payload, timeout=600):
    """POST one non-streaming completion; returns elapsed seconds and usage."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(
        base_url + "/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    start = time.monotonic()
    with opener.open(req, timeout=timeout) as f:
        result = json.load(f)
    content = (result.get("choices") or [{}])[0].get("message", {}).get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("warmup returned no text")
    return {"ready": True, "elapsed_s": round(time.monotonic() - start, 3),
            "usage": result.get("usage"), "model": result.get("model")}





