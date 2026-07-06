#!/usr/bin/env python3
"""Run a concurrent load test against an OpenAI-compatible chat API."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import json
import mimetypes
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Send concurrent chat requests and summarize latency and throughput.")
    p.add_argument("--model", default="local-fast")
    p.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL", "http://localhost:4000/v1"))
    p.add_argument("--api-key", default=os.getenv("LITELLM_MASTER_KEY", "sk-change-me-local-router"))
    p.add_argument("--prompt", default="Reply with exactly: load test ok")
    p.add_argument("--prompt-file", help="Read the prompt text from a file.")
    p.add_argument("--image-path", help="Optional local image to send with every request.")
    p.add_argument("--requests", type=int, default=20, help="Total requests to send.")
    p.add_argument("--concurrency", type=int, default=4, help="Maximum in-flight requests.")
    p.add_argument("--max-tokens", type=int, default=64)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--timeout", type=float, default=180.0)
    p.add_argument("--stream", action="store_true", help="Stream responses and record time to first token.")
    p.add_argument("--json", action="store_true", help="Print the summary as JSON.")
    p.add_argument("--jsonl", type=Path, help="Optional path for per-request JSONL results.")
    return p


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100) * (len(ordered) - 1)))))
    return round(ordered[index], 4)


def describe(values: list[float]) -> dict[str, float | None]:
    return {
        "min": round(min(values), 4) if values else None,
        "mean": round(statistics.mean(values), 4) if values else None,
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "max": round(max(values), 4) if values else None,
    }


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


def user_content(prompt: str, image_path: str | None) -> str | list[dict[str, Any]]:
    if not image_path:
        return prompt
    return [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": image_data_url(image_path)}},
    ]


def add_token_rates(record: dict[str, Any]) -> None:
    usage = record.get("usage") or {}
    latency = float(record.get("latency_s") or 0)
    completion_tokens = usage.get("completion_tokens") or 0
    total_tokens = usage.get("total_tokens") or 0
    if latency > 0:
        if completion_tokens:
            record["output_tokens_per_second"] = round(completion_tokens / latency, 4)
        if total_tokens:
            record["total_tokens_per_second"] = round(total_tokens / latency, 4)
    ttft = record.get("ttft_s")
    if ttft is not None and completion_tokens and latency > float(ttft):
        record["decode_tokens_per_second"] = round(completion_tokens / (latency - float(ttft)), 4)


def chat_once(
    *,
    request_id: int,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    if payload.get("stream"):
        return chat_stream_once(request_id=request_id, url=url, headers=headers, payload=payload, timeout=timeout)

    started = time.perf_counter()
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        latency = time.perf_counter() - started
    except requests.RequestException as exc:
        return {
            "request_id": request_id,
            "ok": False,
            "error": str(exc),
            "latency_s": round(time.perf_counter() - started, 4),
        }

    record: dict[str, Any] = {
        "request_id": request_id,
        "ok": 200 <= response.status_code < 300,
        "status_code": response.status_code,
        "latency_s": round(latency, 4),
    }
    try:
        body = response.json()
    except ValueError:
        record["response_text"] = response.text[:500]
        return record

    choices = body.get("choices") or [{}]
    message = choices[0].get("message", {}) if isinstance(choices, list) else {}
    record["finish_reason"] = choices[0].get("finish_reason") if isinstance(choices, list) else None
    record["response_text"] = message.get("content", "")
    record["usage"] = body.get("usage")
    add_token_rates(record)
    if not record["ok"]:
        record["error_body"] = body
    return record


def chat_stream_once(
    *,
    request_id: int,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    parts: list[str] = []
    finish_reason = None
    usage = None
    ttft = None
    status_code = None
    try:
        with requests.post(url, headers=headers, json=payload, timeout=timeout, stream=True) as response:
            status_code = response.status_code
            if not 200 <= response.status_code < 300:
                return {
                    "request_id": request_id,
                    "ok": False,
                    "status_code": response.status_code,
                    "latency_s": round(time.perf_counter() - started, 4),
                    "response_text": response.text[:500],
                }
            for raw_line in response.iter_lines(decode_unicode=True):
                if not raw_line or not raw_line.startswith("data: "):
                    continue
                data = raw_line.removeprefix("data: ").strip()
                if data == "[DONE]":
                    break
                chunk = json.loads(data)
                if chunk.get("usage"):
                    usage = chunk["usage"]
                choices = chunk.get("choices") or []
                if not choices:
                    continue
                finish_reason = choices[0].get("finish_reason") or finish_reason
                delta = choices[0].get("delta") or {}
                content = delta.get("content")
                if content:
                    if ttft is None:
                        ttft = time.perf_counter() - started
                    parts.append(content)
    except (requests.RequestException, json.JSONDecodeError) as exc:
        return {
            "request_id": request_id,
            "ok": False,
            "error": str(exc),
            "latency_s": round(time.perf_counter() - started, 4),
        }

    latency = time.perf_counter() - started
    record: dict[str, Any] = {
        "request_id": request_id,
        "ok": True,
        "status_code": status_code,
        "latency_s": round(latency, 4),
        "ttft_s": round(ttft, 4) if ttft is not None else None,
        "finish_reason": finish_reason,
        "response_text": "".join(parts),
        "usage": usage,
    }
    add_token_rates(record)
    return record


def summarize(records: list[dict[str, Any]], elapsed_s: float, args: argparse.Namespace) -> dict[str, Any]:
    successful = [r for r in records if r.get("ok")]
    failures = [r for r in records if not r.get("ok")]
    latencies = [float(r["latency_s"]) for r in successful]
    ttfts = [float(r["ttft_s"]) for r in successful if r.get("ttft_s") is not None]
    prompt_tokens = sum((r.get("usage") or {}).get("prompt_tokens") or 0 for r in successful)
    completion_tokens = sum((r.get("usage") or {}).get("completion_tokens") or 0 for r in successful)
    total_tokens = sum((r.get("usage") or {}).get("total_tokens") or 0 for r in successful)
    return {
        "model": args.model,
        "base_url": args.base_url,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "stream": args.stream,
        "ok": len(successful),
        "failed": len(failures),
        "elapsed_s": round(elapsed_s, 4),
        "requests_per_second": round(len(records) / elapsed_s, 4) if elapsed_s > 0 else None,
        "tokens": {
            "prompt": prompt_tokens,
            "completion": completion_tokens,
            "total": total_tokens,
        },
        "throughput": {
            "output_tokens_per_second": round(completion_tokens / elapsed_s, 4) if elapsed_s > 0 and completion_tokens else None,
            "total_tokens_per_second": round(total_tokens / elapsed_s, 4) if elapsed_s > 0 and total_tokens else None,
        },
        "latency_s": describe(latencies),
        "ttft_s": describe(ttfts),
    }


def main() -> int:
    args = parser().parse_args()
    if args.requests < 1 or args.concurrency < 1:
        print("--requests and --concurrency must be positive integers.", file=sys.stderr)
        return 2

    prompt = Path(args.prompt_file).read_text(encoding="utf-8") if args.prompt_file else args.prompt
    url = args.base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {args.api_key}",
        "Content-Type": "application/json",
    }
    payload: dict[str, Any] = {
        "model": args.model,
        "messages": [{"role": "user", "content": user_content(prompt, args.image_path)}],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
    }
    if args.stream:
        payload["stream"] = True
        payload["stream_options"] = {"include_usage": True}

    started = time.perf_counter()
    records: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(
                chat_once,
                request_id=i,
                url=url,
                headers=headers,
                payload=payload,
                timeout=args.timeout,
            )
            for i in range(1, args.requests + 1)
        ]
        for future in as_completed(futures):
            records.append(future.result())
    elapsed = time.perf_counter() - started

    records.sort(key=lambda item: item["request_id"])
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")

    summary = summarize(records, elapsed, args)
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(
            f"{summary['ok']}/{summary['requests']} ok, {summary['failed']} failed, "
            f"{summary['requests_per_second']} req/s"
        )
        print(f"tokens: {summary['tokens']}")
        print(f"throughput: {summary['throughput']}")
        print(f"latency seconds: {summary['latency_s']}")
        if args.stream:
            print(f"ttft seconds: {summary['ttft_s']}")
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
