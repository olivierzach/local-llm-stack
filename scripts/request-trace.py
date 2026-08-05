#!/usr/bin/env python3
"""Compare one OpenAI-compatible chat request through vLLM and LiteLLM."""

from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import os
from pathlib import Path
import sys
import time
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECT_BASE_URLS = {
    "local-fast": "http://localhost:8001/v1",
    "local-balanced": "http://localhost:8002/v1",
    "local-large": "http://localhost:8003/v1",
    "local-qwen30-a3b": "http://localhost:8004/v1",
    "local-deepseek-r1-qwen32b": "http://localhost:8005/v1",
    "local-mistral-small": "http://localhost:8006/v1",
    "local-balanced-smoke-lora": "http://localhost:8007/v1",
    "local-vision": "http://localhost:8008/v1",
    "local-gpt-oss-120b": "http://localhost:8009/v1",
    "local-laguna-s-2.1": "http://localhost:8010/v1",
}


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Send the same prompt directly to vLLM and through LiteLLM."
    )
    p.add_argument("--model", default="local-fast", help="OpenAI model alias to request.")
    p.add_argument("--prompt", required=True, help="User prompt to send.")
    p.add_argument("--system", default="You are a concise local assistant.")
    p.add_argument("--image-path", help="Optional local image to send as a data URL for vision models.")
    p.add_argument("--image-url", help="Optional remote image URL for vision models.")
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--litellm-base-url", default=os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1"))
    p.add_argument("--vllm-base-url", default=None, help="Direct vLLM /v1 URL. Defaults from --model when known.")
    p.add_argument("--litellm-api-key", default=os.getenv("LITELLM_MASTER_KEY", "sk-change-me-local-router"))
    p.add_argument("--vllm-api-key", default=os.getenv("VLLM_API_KEY", "none"))
    p.add_argument("--timeout", type=float, default=180.0)
    p.add_argument("--skip-direct", action="store_true", help="Only call LiteLLM.")
    p.add_argument("--skip-litellm", action="store_true", help="Only call direct vLLM.")
    return p


def resolve_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return REPO_ROOT / candidate


def image_data_url(path: str) -> str:
    image_path = resolve_path(path)
    mime_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def user_content(prompt: str, image_path: str | None, image_url: str | None) -> str | list[dict[str, Any]]:
    if image_path and image_url:
        raise ValueError("Use only one of --image-path or --image-url.")
    if not image_path and not image_url:
        return prompt
    url = image_url or image_data_url(image_path or "")
    return [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": url}},
    ]


def add_token_rates(result: dict[str, Any]) -> None:
    usage = result.get("usage") or {}
    latency = float(result.get("latency_s") or 0)
    completion_tokens = usage.get("completion_tokens") or 0
    total_tokens = usage.get("total_tokens") or 0
    if latency > 0:
        if completion_tokens:
            result["output_tokens_per_second"] = round(completion_tokens / latency, 4)
        if total_tokens:
            result["total_tokens_per_second"] = round(total_tokens / latency, 4)


def post_chat(
    *,
    label: str,
    base_url: str,
    api_key: str,
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    started = time.perf_counter()
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        latency = time.perf_counter() - started
    except requests.RequestException as exc:
        return {
            "path": label,
            "url": url,
            "ok": False,
            "error": str(exc),
            "latency_s": round(time.perf_counter() - started, 4),
        }

    result: dict[str, Any] = {
        "path": label,
        "url": url,
        "ok": 200 <= response.status_code < 300,
        "status_code": response.status_code,
        "latency_s": round(latency, 4),
    }
    try:
        body = response.json()
    except ValueError:
        result["body"] = response.text
        return result

    result["finish_reason"] = (
        body.get("choices", [{}])[0].get("finish_reason")
        if isinstance(body.get("choices"), list)
        else None
    )
    result["response_text"] = extract_text(body)
    result["usage"] = body.get("usage")
    add_token_rates(result)
    if not result["ok"]:
        result["error_body"] = body
    return result


def extract_text(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not choices:
        return ""
    message = choices[0].get("message", {})
    content = message.get("content")
    if isinstance(content, str):
        return content
    return ""


def main() -> int:
    args = parser().parse_args()
    direct_base_url = args.vllm_base_url or DEFAULT_DIRECT_BASE_URLS.get(args.model)

    if args.skip_direct and args.skip_litellm:
        print("At least one path must be enabled.", file=sys.stderr)
        return 2
    if not args.skip_direct and not direct_base_url:
        print(
            f"No direct vLLM URL is known for {args.model!r}; pass --vllm-base-url or use --skip-direct.",
            file=sys.stderr,
        )
        return 2

    try:
        content = user_content(args.prompt, args.image_path, args.image_url)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    payload = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": args.system},
            {"role": "user", "content": content},
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
    }

    results: list[dict[str, Any]] = []
    if not args.skip_direct:
        results.append(
            post_chat(
                label="vllm-direct",
                base_url=direct_base_url,
                api_key=args.vllm_api_key,
                payload=payload,
                timeout=args.timeout,
            )
        )
    if not args.skip_litellm:
        results.append(
            post_chat(
                label="litellm-routed",
                base_url=args.litellm_base_url,
                api_key=args.litellm_api_key,
                payload=payload,
                timeout=args.timeout,
            )
        )

    print(json.dumps({"request": payload, "results": results}, indent=2))
    return 0 if all(result.get("ok") for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
