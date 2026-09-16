#!/usr/bin/env python3
"""Bounded real gateway acceptance: text, SSE, tools and a synthetic tool result."""
import argparse
import json
from pathlib import Path
import time

import requests


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-url", required=True)
    p.add_argument("--key-file", type=Path, required=True)
    p.add_argument("--model", required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expected-deployment", help="Require every response to identify this deployment")
    args = p.parse_args()
    session = requests.Session()
    session.trust_env = False
    session.headers["Authorization"] = "Bearer " + args.key_file.read_text().strip()
    r = session.get(args.base_url + "/models", timeout=15)
    r.raise_for_status()
    model = next((m for m in r.json()["data"] if m["id"] == args.model), None)
    if model is None: raise RuntimeError("selected model is not advertised as available")
    result = {"model": model, "checks": []}
    deployments=set()

    def post(payload):
        r = session.post(args.base_url + "/chat/completions", json={"model": args.model, "temperature": 0,
            "max_tokens": 256, **payload}, timeout=120)
        r.raise_for_status()
        if args.expected_deployment and r.headers.get('X-Spark-Deployment') != args.expected_deployment:
            raise RuntimeError('response came from another deployment')
        if r.headers.get('X-Spark-Deployment'): deployments.add(r.headers['X-Spark-Deployment'])
        return r.json()

    def stream(payload):
        started = time.monotonic()
        first = None
        chunks = []
        done = False
        with session.post(args.base_url + "/chat/completions", json={"model": args.model, "temperature": 0,
                "max_tokens": 256, **payload, "stream": True}, stream=True, timeout=120) as response:
            response.raise_for_status()
            if args.expected_deployment and response.headers.get('X-Spark-Deployment') != args.expected_deployment:
                raise RuntimeError('stream came from another deployment')
            if response.headers.get('X-Spark-Deployment'): deployments.add(response.headers['X-Spark-Deployment'])
            for line in response.iter_lines(chunk_size=1, decode_unicode=True):
                if not line.startswith("data:"): continue
                data = line[5:].strip()
                if data == "[DONE]":
                    done = True
                    break
                chunk = json.loads(data)
                if first is None and any(c.get("delta", {}).get("content") or c.get("delta", {}).get("tool_calls")
                                         for c in chunk.get("choices", [])):
                    first = time.monotonic() - started
                chunks.append(chunk)
        if not done or first is None: raise RuntimeError("incomplete streaming response")
        return chunks, {"first_delta_s": round(first, 4), "total_s": round(time.monotonic()-started, 4)}

    reply = post({"messages": [{"role": "user", "content": "Reply with exactly: ready"}], "max_tokens": 16})
    if not reply["choices"][0]["message"].get("content"): raise RuntimeError("empty completion")
    result["checks"].append({"check": "text", "passed": True, "usage": reply.get("usage")})
    chunks, timing = stream({"messages": [{"role": "user", "content": "Count from one to five."}], "max_tokens": 32})
    if not any(c.get("delta", {}).get("content") for chunk in chunks for c in chunk.get("choices", [])):
        raise RuntimeError("stream had no text")
    result["checks"].append({"check": "stream-text", "passed": True, **timing})
    if model.get("capabilities", {}).get("tools"):
        payload = {"messages": [{"role": "user", "content": "Use get_temperature to get the temperature in Paris. Do not guess."}],
            "tools": [{"type": "function", "function": {"name": "get_temperature", "description": "Get a city's temperature.",
                "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"],
                               "additionalProperties": False}}}], "tool_choice": "auto"}
        reply = post(payload)
        message = reply["choices"][0]["message"]
        call = message["tool_calls"][0]
        if call["function"]["name"] != "get_temperature" or json.loads(call["function"]["arguments"])["city"].lower() != "paris":
            raise RuntimeError("wrong function call or malformed arguments")
        result["checks"].append({"check": "automatic-tool-call", "passed": True})
        followup = post({**payload, "messages": [*payload["messages"], message,
            {"role": "tool", "tool_call_id": call["id"], "content": '{"celsius":17}'}]})
        if "17" not in (followup["choices"][0]["message"].get("content") or ""):
            raise RuntimeError("tool-result continuation did not use the synthetic result")
        result["checks"].append({"check": "tool-result-continuation", "passed": True})
        chunks, timing = stream(payload)
        functions = {}
        for chunk in chunks:
            for choice in chunk.get("choices", []):
                for call in choice.get("delta", {}).get("tool_calls", []):
                    f = functions.setdefault(call["index"], {"name": "", "arguments": ""})
                    for key in f: f[key] += call.get("function", {}).get(key) or ""
        if len(functions) != 1: raise RuntimeError("stream did not produce exactly one tool call")
        f = next(iter(functions.values()))
        if f["name"] != "get_temperature" or json.loads(f["arguments"])["city"].lower() != "paris":
            raise RuntimeError("streaming tool argument assembly failed")
        result["checks"].append({"check": "stream-tool-call", "passed": True, **timing})
    result['deployment_digests']=sorted(deployments)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__": main()
