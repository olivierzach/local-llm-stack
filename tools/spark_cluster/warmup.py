"""Exercise a bounded startup decode before admitting client traffic.

DSpark draft kernels are compiled lazily. A sufficiently long decode plus draft
counter progress verifies the speculative path for this request's shapes only;
it cannot rule out later compilation or prefill stalls for other shapes.
"""
from __future__ import annotations

import json
import time
import urllib.request

from .watchdog import metric_totals


_DRAFT_TOKENS = "vllm:spec_decode_num_draft_tokens_total"


def health_root(base_url):
    """Return the non ``/v1`` origin a vLLM health/metrics endpoint lives at."""
    url = base_url.rstrip("/")
    if url.endswith("/v1"):
        url = url[: -len("/v1")]
    return url


def warmup_speculative_payload(alias, speculative, max_output_cap=64):
    """Require a bounded decode using vLLM's minimum-token and EOS controls."""
    num_spec = 2
    if isinstance(speculative, dict):
        num_spec = speculative.get("num_speculative_tokens", 2)
    max_tokens = min(max(int(num_spec) * 8, 16), max_output_cap)
    if max_tokens < 1:
        raise ValueError("warmup output cap must be positive")
    return {
        "model": alias,
        "messages": [{"role": "user", "content": "Continue counting integers starting at one."}],
        "max_tokens": max_tokens,
        "min_tokens": max_tokens,
        "ignore_eos": True,
        "temperature": 0,
    }


def run_warmup(base_url, payload, timeout=600, require_speculation=False):
    """Verify one completed decode, optionally requiring draft-counter progress.

    All HTTP calls and publication polling share one monotonic deadline. The
    inference request is never retried. Success covers only the exercised shapes.
    """
    start = time.monotonic()
    deadline = start + timeout

    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("warmup operation deadline expired")
        return seconds

    required = payload.get("min_tokens", payload.get("max_tokens"))
    if type(required) is not int or required < 1:
        raise ValueError("warmup requires a positive token minimum")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def draft_tokens():
        request = urllib.request.Request(health_root(base_url) + "/metrics")
        with opener.open(request, timeout=remaining()) as response:
            text = response.read().decode("utf-8")
        remaining()
        return metric_totals(text, [_DRAFT_TOKENS])[_DRAFT_TOKENS]

    baseline = None
    if require_speculation:
        baseline = draft_tokens()
        if baseline is None:
            raise RuntimeError("warmup draft-token baseline is unavailable")

    request = urllib.request.Request(
        base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with opener.open(request, timeout=remaining()) as response:
        result = json.load(response)
    remaining()
    if not isinstance(result, dict) or result.get("error"):
        raise RuntimeError("warmup returned an invalid completion")
    choices = result.get("choices")
    choice = choices[0] if isinstance(choices, list) and len(choices) == 1 else None
    if not isinstance(choice, dict) or choice.get("finish_reason") not in ("stop", "length"):
        raise RuntimeError("warmup did not return a completed decode")
    message = choice.get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("warmup returned no text")
    usage = result.get("usage")
    generated = usage.get("completion_tokens") if isinstance(usage, dict) else None
    if type(generated) is not int or generated < required:
        raise RuntimeError("warmup completion did not reach the required decode length")

    if require_speculation:
        while True:
            current = draft_tokens()
            if current is not None:
                if current < baseline:
                    raise RuntimeError("warmup draft-token counter decreased")
                if current > baseline:
                    break
            time.sleep(min(0.25, remaining()))
    remaining()
    return {"ready": True, "elapsed_s": round(time.monotonic() - start, 3),
            "usage": usage, "model": result.get("model")}
