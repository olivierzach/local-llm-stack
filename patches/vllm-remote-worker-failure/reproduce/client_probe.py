"""External HTTP probe: observations, not a substitute for worker logs."""

import argparse
import concurrent.futures
import json
import time
import uuid

import httpx

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=["normal", "cancel", "failure", "lora", "report", "speculative"])
parser.add_argument("--url", required=True)
parser.add_argument("--model", required=True)
parser.add_argument("--timeout", type=float, default=60)
parser.add_argument("--failure-mode", choices=["exception", "exit"], default="exception")
args = parser.parse_args()


def post(path, body):
    start = time.monotonic()
    try:
        with httpx.Client(timeout=args.timeout, trust_env=False) as client:
            response = client.post(args.url + path, json=body)
        try:
            content = response.json()
        except ValueError:
            content = response.text
        return {
            "status": response.status_code,
            "seconds": time.monotonic() - start,
            "body": content,
        }
    except httpx.HTTPError as error:
        return {
            "transport_error": type(error).__name__,
            "detail": str(error),
            "seconds": time.monotonic() - start,
        }


def payload(prompt, max_tokens=32, **extra):
    return {
        "model": args.model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "seed": 42,
        "max_tokens": max_tokens,
        "chat_template_kwargs": {"enable_thinking": False},
        **extra,
    }


def generate(prompt, max_tokens=32, **extra):
    return post("/v1/chat/completions", payload(prompt, max_tokens, **extra))
def speculative_metrics():
    names = (
        "vllm:spec_decode_num_drafts_total",
        "vllm:spec_decode_num_draft_tokens_total",
        "vllm:spec_decode_num_accepted_tokens_total",
    )
    with httpx.Client(timeout=args.timeout, trust_env=False) as client:
        response = client.get(args.url + "/metrics")
        response.raise_for_status()
    samples = [
        line for line in response.text.splitlines()
        if any(line.startswith(name + "{") for name in names)
    ]
    values = {
        name: sum(float(line.rsplit(" ", 1)[1]) for line in samples if line.startswith(name + "{"))
        for name in names
    }
    assert all(any(line.startswith(name + "{") for line in samples) for name in names)
    return {"values": values, "samples": samples}




record = {"mode": args.mode, "unix_time": time.time()}
if args.mode == "report":
    record["workers"] = post("/collective_rpc", {"method": "e2e_report", "timeout": 15})
elif args.mode == "speculative":
    record["before"] = speculative_metrics()
    record["generation"] = generate(
        "Explain how a database B-tree index supports efficient range queries.", 128,
        ignore_eos=True,
    )
    deadline = time.monotonic() + 15
    while True:
        record["after"] = speculative_metrics()
        if record["after"]["values"]["vllm:spec_decode_num_drafts_total"] > record["before"]["values"]["vllm:spec_decode_num_drafts_total"]:
            break
        if time.monotonic() >= deadline:
            break
        time.sleep(1)
elif args.mode == "normal":
    record["single"] = generate("What is 6 * 7? Reply with only the number.")
    prompts = [
        "What is 7 + 5? Reply with only the number.",
        "What is 9 * 9? Reply with only the number.",
        "What is 20 - 3? Reply with only the number.",
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        record["concurrent"] = list(pool.map(generate, prompts))
elif args.mode == "cancel":
    fragments = []
    try:
        with httpx.Client(timeout=args.timeout, trust_env=False) as client:
            with client.stream(
                "POST",
                args.url + "/v1/chat/completions",
                json=payload("Count from 1 to 500, separated by commas.", 1024, stream=True),
            ) as response:
                record["stream_status"] = response.status_code
                for line in response.iter_lines():
                    if not line.startswith("data: ") or line == "data: [DONE]":
                        continue
                    event = json.loads(line[6:])
                    record["last_stream_event"] = event
                    choices = event.get("choices", [])
                    text = choices[0].get("delta", {}).get("content") if choices else None
                    if text:
                        fragments.append(text)
                    if len(fragments) == 3:
                        break
    except httpx.HTTPError as error:
        record["stream_error"] = type(error).__name__
    record["cancelled_after_fragments"] = fragments
    record["subsequent_request"] = generate("Calculate 23 + 19. Reply only with the number.")
elif args.mode == "failure":
    run_id = uuid.uuid4().hex
    record["run_id"] = run_id
    record["failure_mode"] = args.failure_mode
    record["arm"] = post(
        "/collective_rpc",
        {"method": "e2e_arm_failure", "kwargs": {"target_rank": 1, "run_id": run_id, "failure_mode": args.failure_mode}, "timeout": 15},
    )
    print(json.dumps({"event": "armed", **record}), flush=True)
    if record["arm"].get("status") == 200:
        record["request"] = generate(
            f"Describe a binary search tree in detail. Experiment {run_id}.",
            128,
            ignore_eos=True,
        )
elif args.mode == "lora":
    prompt = "Explain why database indexes speed up queries."
    record["before"] = [generate(prompt, 16, ignore_eos=True) for _ in range(4)]
    record["invalid_adapter"] = post(
        "/v1/load_lora_adapter",
        {"lora_name": "e2e_missing_adapter", "lora_path": "/work/nonexistent-adapter"},
    )
    time.sleep(2)
    record["after"] = [generate(prompt, 16, ignore_eos=True) for _ in range(4)]
print(json.dumps(record), flush=True)
