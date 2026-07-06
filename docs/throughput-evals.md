# Throughput Evals Lab

This lab measures request success, latency, time to first token, and token throughput.

## Run A Throughput Sweep

Use the routed API first:

```bash
make throughput-eval
```

That target loads `.env`, sends streamed requests through LiteLLM, and writes per-request JSONL under `evals/runs/`.

For direct experiments:

```bash
python scripts/load-test.py --model local-fast --concurrency 4 --requests 20 --max-tokens 128 --stream --json
```

Use `--prompt-file` to hold prefill constant and vary only concurrency or `max_tokens`.

## Metrics

`requests_per_second` is whole-request throughput. It is useful for API capacity but hides output length.

`output_tokens_per_second` is generated-token throughput from API usage accounting. This is the primary number for decode performance.

`total_tokens_per_second` includes prompt tokens and completion tokens. It is useful when comparing long-prefill workloads.

`ttft_s` is recorded when `--stream` is enabled. It measures time from request start until the first streamed content delta, so it captures queueing plus prefill.

## Real Test Matrix

Run each row at least twice, once warm and once after changing a server knob:

```text
short prompt, short max_tokens       decode overhead and routing cost
short prompt, long max_tokens        sustained decode throughput
long prompt, short max_tokens        prefill and KV cache pressure
vision prompt, short max_tokens      image preprocessing plus multimodal prefill
```

Save JSONL outputs and compare medians and p95s, not only means.
