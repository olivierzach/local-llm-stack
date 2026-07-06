#!/usr/bin/env python3
"""Run JSONL prompt sets against one or more OpenAI-compatible model aliases."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import mimetypes
import os
from pathlib import Path
import sys
import time
from typing import Any

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run fixed prompt files and save per-response JSONL.")
    p.add_argument("--models", nargs="+", required=True, help="Model aliases to evaluate.")
    p.add_argument("--prompt-file", type=Path, required=True, help="JSONL prompt set.")
    p.add_argument("--base-url", default=os.getenv("OPENAI_BASE_URL", "http://localhost:4000/v1"))
    p.add_argument("--api-key", default=os.getenv("LITELLM_MASTER_KEY", "sk-change-me-local-router"))
    p.add_argument("--output-dir", type=Path, default=Path("evals/runs"))
    p.add_argument("--max-tokens", type=int, default=128)
    p.add_argument("--temperature", type=float, default=0.0)
    p.add_argument("--timeout", type=float, default=180.0)
    return p


def load_prompts(path: Path) -> list[dict[str, Any]]:
    prompts: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
            if "id" not in item:
                raise ValueError(f"{path}:{line_no}: missing id")
            if "messages" not in item and "prompt" not in item:
                raise ValueError(f"{path}:{line_no}: expected messages or prompt")
            prompts.append(item)
    return prompts


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


def messages_for(item: dict[str, Any]) -> list[dict[str, Any]]:
    if "messages" in item:
        return item["messages"]
    if "image_path" in item:
        return [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": item["prompt"]},
                    {"type": "image_url", "image_url": {"url": image_data_url(item["image_path"])}},
                ],
            }
        ]
    return [{"role": "user", "content": item["prompt"]}]


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


def call_chat(
    *,
    model: str,
    prompt: dict[str, Any],
    url: str,
    headers: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "messages": messages_for(prompt),
        "temperature": prompt.get("temperature", args.temperature),
        "max_tokens": prompt.get("max_tokens", args.max_tokens),
    }
    started = time.perf_counter()
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=args.timeout)
        latency = time.perf_counter() - started
    except requests.RequestException as exc:
        return base_record(model, prompt, latency=time.perf_counter() - started) | {
            "ok": False,
            "error": str(exc),
            "validators": validate("", prompt),
        }

    record = base_record(model, prompt, latency=latency) | {
        "ok": 200 <= response.status_code < 300,
        "status_code": response.status_code,
    }
    try:
        body = response.json()
    except ValueError:
        text = response.text
        record["response_text"] = text
        record["validators"] = validate(text, prompt)
        return record

    choices = body.get("choices") or [{}]
    message = choices[0].get("message", {}) if isinstance(choices, list) else {}
    text = message.get("content", "")
    record.update(
        {
            "finish_reason": choices[0].get("finish_reason") if isinstance(choices, list) else None,
            "response_text": text,
            "usage": body.get("usage"),
            "validators": validate(text, prompt),
        }
    )
    add_token_rates(record)
    if not record["ok"]:
        record["error_body"] = body
    return record


def base_record(model: str, prompt: dict[str, Any], *, latency: float) -> dict[str, Any]:
    return {
        "model": model,
        "prompt_id": prompt["id"],
        "latency_s": round(latency, 4),
    }


def validate(text: str, prompt: dict[str, Any]) -> dict[str, bool]:
    validators: dict[str, bool] = {}
    expected = prompt.get("expect_exact")
    if expected is not None:
        validators["exact"] = text.strip().lower() == str(expected).strip().lower()
    if prompt.get("expect_json"):
        try:
            json.loads(text)
            validators["json"] = True
        except ValueError:
            validators["json"] = False
    return validators


def safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in value).strip("-")


def write_records(output_dir: Path, prompt_file: Path, records: list[dict[str, Any]]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = output_dir / f"{stamp}-{safe_name(prompt_file.stem)}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return path


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for record in records:
        model = record["model"]
        model_summary = summary.setdefault(
            model,
            {
                "ok": 0,
                "failed": 0,
                "tokens": {"prompt": 0, "completion": 0, "total": 0},
                "throughput": {"mean_output_tokens_per_second": None},
                "validators": {},
            },
        )
        if record.get("ok"):
            model_summary["ok"] += 1
        else:
            model_summary["failed"] += 1
        usage = record.get("usage") or {}
        model_summary["tokens"]["prompt"] += usage.get("prompt_tokens") or 0
        model_summary["tokens"]["completion"] += usage.get("completion_tokens") or 0
        model_summary["tokens"]["total"] += usage.get("total_tokens") or 0
        for name, passed in record.get("validators", {}).items():
            validator = model_summary["validators"].setdefault(name, {"passed": 0, "failed": 0})
            validator["passed" if passed else "failed"] += 1

    for model, model_summary in summary.items():
        rates = [
            record["output_tokens_per_second"]
            for record in records
            if record.get("model") == model and record.get("output_tokens_per_second") is not None
        ]
        model_summary["throughput"]["mean_output_tokens_per_second"] = (
            round(sum(rates) / len(rates), 4) if rates else None
        )
    return summary


def main() -> int:
    args = parser().parse_args()
    try:
        prompts = load_prompts(args.prompt_file)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 2

    url = args.base_url.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {args.api_key}",
        "Content-Type": "application/json",
    }

    records = [
        call_chat(model=model, prompt=prompt, url=url, headers=headers, args=args)
        for model in args.models
        for prompt in prompts
    ]
    output_path = write_records(args.output_dir, args.prompt_file, records)
    summary = summarize(records)
    print(json.dumps({"output": str(output_path), "summary": summary}, indent=2))

    any_failed_request = any(not record.get("ok") for record in records)
    any_failed_validator = any(
        passed is False
        for record in records
        for passed in record.get("validators", {}).values()
    )
    return 1 if any_failed_request or any_failed_validator else 0


if __name__ == "__main__":
    raise SystemExit(main())
