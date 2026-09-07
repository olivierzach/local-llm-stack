#!/usr/bin/env python3
"""Reproducible Qwen verification. Fixtures are synthetic; credentials are never saved."""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import time

import requests

ROOT = Path(__file__).resolve().parents[1]
MODEL = "local-qwen38-flash-next"


def load_env():
    for line in (ROOT / ".env").read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def memory():
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        values[key] = int(value.split()[0])
    return {
        "available_gib": round(values["MemAvailable"] / 1048576, 3),
        "free_gib": round(values["MemFree"] / 1048576, 3),
        "swap_used_gib": round((values["SwapTotal"] - values["SwapFree"]) / 1048576, 3),
    }


def chat(base, payload, key):
    start = time.monotonic()
    ttft = None
    content = ""
    calls = {}
    finish = None
    usage = {}
    done = False
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    with requests.post(base + "/chat/completions", json=payload, headers=headers,
                       stream=payload.get("stream", False), timeout=(10, 600)) as response:
        response.raise_for_status()
        guard = {k.lower(): v for k, v in response.headers.items() if k.lower().startswith("x-context-")}
        if payload.get("stream"):
            for line in response.iter_lines(chunk_size=1):
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                if data == b"[DONE]":
                    done = True
                    break
                event = json.loads(data)
                if event.get("error"):
                    raise RuntimeError("stream returned an error")
                usage = event.get("usage") or usage
                for choice in event.get("choices", []):
                    delta = choice.get("delta") or {}
                    text = delta.get("content") or ""
                    if ttft is None and (text or delta.get("tool_calls") or delta.get("reasoning_content") or delta.get("reasoning")):
                        ttft = time.monotonic() - start
                    content += text
                    finish = choice.get("finish_reason") or finish
                    for fragment in delta.get("tool_calls", []):
                        call = calls.setdefault(fragment["index"], {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                        call["id"] += fragment.get("id") or ""
                        for key_name in ("name", "arguments"):
                            call["function"][key_name] += (fragment.get("function") or {}).get(key_name) or ""
            if not done or not finish:
                raise RuntimeError("incomplete stream: no completed tool call may be executed")
        else:
            body = response.json()
            choice = body["choices"][0]
            content = choice["message"].get("content") or ""
            finish = choice["finish_reason"]
            calls = dict(enumerate(choice["message"].get("tool_calls") or []))
            usage = body.get("usage") or {}
    for call in calls.values():
        assert call["id"] and call["function"]["name"]
        json.loads(call["function"]["arguments"])
    elapsed = time.monotonic() - start
    record = {"elapsed_s": round(elapsed, 3), "ttft_s": round(ttft, 3) if ttft else None,
              "finish_reason": finish, "usage": usage, "guard": guard, "memory": memory()}
    if ttft and elapsed - ttft >= 1 and usage.get("completion_tokens", 0) >= 32:
        record["decode_estimate_tok_s"] = round((usage["completion_tokens"] - 1) / (elapsed - ttft), 2)
    return record, {"role": "assistant", "content": content, **({"tool_calls": list(calls.values())} if calls else {})}


def payload(messages=None, **kwargs):
    result = {"model": MODEL, "messages": messages or [{"role": "user", "content": "Reply with exactly: ready"}],
              "max_tokens": 64, "temperature": 0, "chat_template_kwargs": {"enable_thinking": False}}
    result.update(kwargs)
    if result.get("stream"):
        result["stream_options"] = {"include_usage": True}
    return result


def main():
    load_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["probe", "smoke", "acceptance", "benchmark", "soak"])
    parser.add_argument("--base-url", default="http://localhost:4010/v1")
    parser.add_argument("--minutes", type=float, default=45)
    args = parser.parse_args()
    host = os.getenv("QWEN38_BIND_HOST") or subprocess.check_output(
        ["docker", "network", "inspect", "bridge", "--format", "{{(index .IPAM.Config 0).Gateway}}"], text=True).strip()
    direct = f"http://{host}:{os.getenv('QWEN38_PORT', '8012')}/v1"
    key = os.environ.get("LITELLM_MASTER_KEY", "")
    expected_limit = int(os.getenv("QWEN38_MAX_MODEL_LEN", "262144"))
    result_dir = ROOT / "evals/runs/qwen38"
    result_dir.mkdir(parents=True, exist_ok=True)
    result_path = result_dir / f"{time.strftime('%Y%m%dT%H%M%S')}-{args.suite}.jsonl"

    def save(name, record):
        record = {"test": name, "timestamp": time.time(), **record}
        with result_path.open("a") as output:
            output.write(json.dumps(record) + "\n")
        print(json.dumps(record), flush=True)

    def run(name, request, base=None):
        base = base or args.base_url
        record, message = chat(base, request, "" if base == direct else key)
        save(name, record)
        return record, message

    def count(request):
        body = {k: v for k, v in request.items() if k not in {"max_tokens", "max_completion_tokens", "temperature", "stream", "stream_options"}}
        response = requests.post(direct.removesuffix("/v1") + "/tokenize", json=body, timeout=60)
        response.raise_for_status()
        return response.json()["count"]

    def long_request(target, stream=True):
        unit = "record = dict(alpha=17, beta=29, status='complete')\n"
        tokens_per_unit = max(1, count(payload([{"role": "user", "content": unit * 100}])) / 100)
        text = unit * int(target / tokens_per_unit)
        return payload([{"role": "user", "content": text + "\nReply with exactly: ready"}], stream=stream)

    models = requests.get(direct + "/models", timeout=10)
    models.raise_for_status()
    item = next(m for m in models.json()["data"] if m["id"] == MODEL)
    assert item.get("max_model_len") == expected_limit, item
    save("live-context", {"limit": item["max_model_len"], "memory": memory()})
    record, message = run("direct-probe", payload(), direct)
    assert "ready" in message["content"].lower(), message
    if args.suite == "probe":
        return
    for name, base in [("litellm", "http://localhost:4000/v1"), ("guard", args.base_url)]:
        record, message = run(name + "-small", payload(stream=True), base)
        assert "ready" in message["content"].lower(), message
        if name == "guard":
            assert "x-context-guard" not in record["guard"], record
            assert int(record["guard"]["x-context-input-tokens"]) == count(payload()), record
            assert int(record["guard"]["x-context-limit"]) == expected_limit
    if args.suite == "smoke":
        return
    if args.suite == "acceptance":
        tools = [{"type": "function", "function": {"name": "get_test_value", "description": "Return the test value for a named key.",
                  "parameters": {"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]}}}]
        request = payload([{"role": "user", "content": "Call get_test_value with key alpha."}], tools=tools, stream=True, max_tokens=256)
        assert count(request) > count(payload(request["messages"])), "tool schemas not counted"
        record, assistant = run("streaming-tool-call", request)
        assert record["finish_reason"] == "tool_calls" and assistant.get("tool_calls"), assistant
        call = assistant["tool_calls"][0]
        assert call["function"]["name"] == "get_test_value"
        request["messages"] += [assistant, {"role": "tool", "tool_call_id": call["id"], "content": "42"}]
        request["tools"] = tools
        record, message = run("tool-result-continuation", request)
        assert "42" in message["content"], message
        image_path = ROOT / "evals/assets/red-square.png"
        image = base64.b64encode(image_path.read_bytes()).decode()
        request = payload([{"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": "data:image/png;base64," + image}},
            {"type": "text", "text": "What color is the square? One word."}]}])
        _, message = run("vision", request)
        assert "red" in message["content"].lower(), message
        for streaming in (False, True):
            request = long_request(expected_limit + 20000, streaming)
            assert count(request) > expected_limit
            record, message = run(f"proactive-overflow-stream-{streaming}", request)
            assert record["guard"].get("x-context-guard") == "compacted", record
            assert message["content"]
        request = payload(max_completion_tokens=expected_limit * 2)
        request.pop("max_tokens")
        record, _ = run("clamp-completion-budget", request)
        assert int(record["guard"]["x-context-output-reserve"]) < expected_limit
        assert "x-context-guard" not in record["guard"], record
        assert int(record["guard"]["x-context-input-tokens"]) == count(request), record
    if args.suite == "benchmark":
        for target in (32768, 131072, expected_limit - 8192):
            request = long_request(target)
            request["messages"][0]["content"] = f"Benchmark {target} nonce {time.time_ns()}.\n" + request["messages"][0]["content"]
            save("prompt-size", {"target": target, "actual": count(request)})
            _, message = run(f"long-{target}", request)
            assert "ready" in message["content"].lower(), "long response failed its content check"
        for concurrency in (1, 2, 4):
            request = payload([{"role": "user", "content": "Write a detailed paragraph about database indexes."}],
                              max_tokens=256, stream=True)
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                results = list(pool.map(lambda _: chat(args.base_url, request, key), range(concurrency)))
            for record, _ in results:
                save(f"concurrency-{concurrency}", record)
    if args.suite == "soak":
        deadline = time.monotonic() + args.minutes * 60
        iteration = 0
        while time.monotonic() < deadline:
            request = long_request(min(expected_limit - 8192, 90000))
            request["messages"][0]["content"] = f"Test iteration {iteration}.\n" + request["messages"][0]["content"]
            _, message = run(f"soak-{iteration}", request)
            assert "ready" in message["content"].lower(), "soak response failed its content check"
            assert memory()["available_gib"] >= 10, "available memory below 10 GiB"
            iteration += 1
            time.sleep(5)
    print(f"Results: {result_path}", flush=True)


if __name__ == "__main__":
    main()
