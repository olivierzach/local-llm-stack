# vLLM Concurrency Lab

This lab connects load-test symptoms to vLLM internals.

## Run Load

Start with a small routed test:

```bash
python scripts/load-test.py --model local-fast --concurrency 2 --requests 4 --max-tokens 64
```

Then watch vLLM while increasing pressure:

```bash
scripts/watch-vllm.sh
python scripts/load-test.py --model local-fast --concurrency 8 --requests 40 --max-tokens 128 --json
```

Use `--jsonl evals/runs/load-local-fast.jsonl` when you want per-request
records for later inspection.

## Prefill And Decode

Inference has two different phases:

```text
prefill -> read the prompt and build KV cache
decode  -> generate one new token at a time
```

Long prompts make prefill expensive. Large `max_tokens` values keep decode work
alive longer. A load test that uses short prompts but high `max_tokens` stresses
decode scheduling differently than a load test with long prompts and short
answers.

## KV Cache

The KV cache stores attention state so the model does not reprocess the whole
prompt for every generated token. Higher context length, more concurrent
requests, and longer generations all reserve more cache. If vLLM fails during
startup or rejects work under pressure, lower `*_MAX_MODEL_LEN`, lower
`*_GPU_MEMORY_UTILIZATION`, or reduce concurrency.

## Batching Controls

`max_num_seqs` limits active sequences. It is a direct concurrency guard.

`max_num_batched_tokens` limits how many prompt and decode tokens can be
scheduled together. It protects memory and can trade peak throughput against
latency.

This repo exposes those controls for the fast service:

```text
FAST_MAX_NUM_SEQS=4
FAST_MAX_NUM_BATCHED_TOKENS=8192
```

Add equivalent environment variables to other services only after measuring a
specific bottleneck.

## Reading Metrics

Load-test latency rises for several reasons: queueing before the request is
scheduled, slow prefill from long prompts, slow decode from large outputs, or
GPU memory pressure reducing batch efficiency. Compare request count,
concurrency, prompt length, and `max_tokens` before changing server flags.

The useful learning loop is:

```text
change one load parameter -> watch vLLM logs/metrics -> save the run -> compare latency
```

## Token Throughput

Use `scripts/load-test.py --stream` or `make throughput-eval` when you need real output token/sec and time-to-first-token numbers. The load-test summary reports request/sec, output tokens/sec, total tokens/sec, latency percentiles, and streamed TTFT percentiles.
