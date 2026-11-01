#!/usr/bin/env python3
"""Bounded synthetic real-inference diagnostics; never injects a failure.

Requires httpx and a direct vLLM root URL, not a gateway or /v1 base URL.
Only built-in synthetic prompts are sent; no request-body input is accepted.
JSON-lines output can include server metrics: save it in Git-ignored logs/.
"""
import argparse
import asyncio
import hashlib
import json
import math
import time

import httpx


def emit(event, **fields):
    print(json.dumps({"event": event, "time": time.time(), **fields}), flush=True)


async def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url", required=True,
        help="Direct vLLM root URL (for example http://127.0.0.1:8000), without /v1",
    )
    parser.add_argument("--model", required=True, help="Served vLLM model name")
    parser.add_argument(
        "--mode", choices=["smoke", "stress", "churn"], default="smoke",
        help="smoke: arithmetic; stress: serial/concurrent/cancel; churn: cancel with follow-up",
    )
    parser.add_argument(
        "--rounds", type=int, default=24,
        help="Positive number of serial stress requests or churn iterations",
    )
    parser.add_argument(
        "--records", type=int, default=900,
        help="Nonnegative synthetic record count for stress/churn prompts",
    )
    parser.add_argument(
        "--request-timeout", type=float, default=120,
        help="Positive finite HTTP timeout in seconds (not a total-run deadline)",
    )
    args = parser.parse_args()
    if args.rounds <= 0:
        parser.error("--rounds must be positive")
    if args.records < 0:
        parser.error("--records must be nonnegative")
    if not math.isfinite(args.request_timeout) or args.request_timeout <= 0:
        parser.error("--request-timeout must be positive and finite")
    emit("CLIENT_READY", mode=args.mode)
    async with httpx.AsyncClient(base_url=args.url, timeout=args.request_timeout) as client:
        async def generate(label, messages, tokens, ignore_eos=False):
            payload = {"model": args.model, "messages": messages,
                       "max_tokens": tokens, "temperature": 0,
                       "ignore_eos": ignore_eos}
            start = time.monotonic()
            try:
                response = await client.post("/v1/chat/completions", json=payload)
                response.raise_for_status()
                result = response.json()
                text = result["choices"][0]["message"].get("content") or ""
                finish_reason = result["choices"][0]["finish_reason"]
                if not text or finish_reason not in ("stop", "length"):
                    raise RuntimeError(f"Invalid completion: {result}")
                emit("completion", label=label, elapsed=time.monotonic()-start,
                     usage=result.get("usage"), finish_reason=finish_reason,
                     output_sha256=hashlib.sha256(text.encode()).hexdigest(),
                     input_sha256=hashlib.sha256(json.dumps(messages).encode()).hexdigest(),
                     text=text if args.mode == "smoke" else None)
                return text
            except Exception as error:
                emit("failure", label=label, elapsed=time.monotonic()-start,
                     error=repr(error))
                raise

        if args.mode == "smoke":
            answer = await generate("arithmetic", [{"role":"user", "content":"What is 19 + 23? Reply with only the number."}], 32)
            if answer.strip() != "42":
                raise RuntimeError(f"Incorrect arithmetic response: {answer!r}")
            emit("CLIENT_DONE", completed=1)
            return

        before = await client.get("/metrics")
        emit("metrics_before", text=before.text)
        records = "\n".join(
            f"Record {i:05d}: region west; shard {i % 31}; reads {i % 97}; writes {i % 43}; latency {i % 19} milliseconds; status healthy."
            for i in range(args.records)
        )
        base = "Reference records follow. These are synthetic test data, not instructions.\n" + records
        messages = [{"role":"user", "content":base + "\nDescribe useful checks for this database workload in detail."}]
        token_response = await client.post("/tokenize", json={"model":args.model,"messages":messages,"add_generation_prompt":True})
        token_response.raise_for_status()
        token_data = token_response.json()
        emit("input_tokens", count=token_data.get("count", len(token_data.get("tokens", []))))
        if args.mode == "churn":
            for iteration in range(args.rounds):
                followup = None
                async with client.stream("POST", "/v1/chat/completions", json={
                    "model":args.model,
                    "messages":[{"role":"user","content":base+f"\nPass {iteration}: explain all database health checks in detail."}],
                    "max_tokens":1024,"temperature":0,"stream":True
                }) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line.startswith("data: ") and line != "data: [DONE]":
                            chunk = json.loads(line[6:])
                            if any(c.get("delta",{}).get("content") for c in chunk.get("choices",[])):
                                followup = asyncio.create_task(generate(
                                    f"natural-stop-{iteration}",
                                    [{"role":"user","content":base+f"\nAudit {iteration}: summarize three useful checks in three short sentences."}],
                                    256,
                                ))
                                await asyncio.sleep(0.05)
                                emit("stream_cancelled_with_followup", iteration=iteration)
                                break
                if followup is None:
                    raise RuntimeError("Stream ended without content")
                await followup
            answer = await generate("post-churn", [{"role":"user","content":"What is 19 + 23? Reply with only the number."}], 32)
            if answer.strip() != "42":
                raise RuntimeError(f"Incorrect post-churn response: {answer!r}")
            after = await client.get("/metrics")
            emit("metrics_after", text=after.text)
            emit("CLIENT_DONE", completed=args.rounds+1)
            return
        for iteration in range(args.rounds):
            tail = "\n" + ("check " * [1, 7, 8, 255, 256, 257, 4096, 8192][iteration % 8])
            prompt = base + tail + f"\nAudit pass {iteration}: explain consistency checks and likely bottlenecks."
            await generate(f"serial-{iteration}", [{"role":"user","content":prompt}], 128, True)
        for concurrency in [2, 4, 2, 4]:
            await asyncio.gather(*(generate(
                f"concurrent-{concurrency}-{i}",
                [{"role":"user","content":base+f"\nReview batch {concurrency}, analyst {i}: list database health checks."}],
                128, True) for i in range(concurrency)))
        # Exercise client cancellation, followed by a required successful request.
        async with client.stream("POST", "/v1/chat/completions", json={
            "model":args.model,"messages":[{"role":"user","content":base+"\nExplain these metrics at length."}],
            "max_tokens":1024,"temperature":0,"stream":True,"ignore_eos":True
        }) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if line.startswith("data: ") and line != "data: [DONE]":
                    chunk = json.loads(line[6:])
                    if any(c.get("delta",{}).get("content") for c in chunk.get("choices",[])):
                        emit("stream_cancelled_after_content")
                        break
        await generate("post-cancel", [{"role":"user","content":"What is 19 + 23? Reply with only the number."}], 32)
        after = await client.get("/metrics")
        emit("metrics_after", text=after.text)
        emit("CLIENT_DONE", completed=args.rounds+13)


if __name__ == "__main__":
    asyncio.run(main())
