# Qwen two-Spark TP + native speculation acceptance

Active objective requested September 10, 2026: make the full BF16
Qwen3-Next-80B-A3B-Instruct model usable across both Sparks with tensor parallelism
and native MTP speculative decoding, and leave the accepted deployment serving.
DeepSeek may stay down during this work. Preserve unrelated workloads, existing
model aliases and reversible single-node startup paths.

## Acceptance criteria

- Pinned model revision and runtime; matching worker configuration on both nodes.
- Actual tensor-parallel workers communicate through the direct RoCE cable.
- Native speculation accepts proposed tokens, with measured sustained decode speed.
- Repeated sequential requests complete without worker stalls, truncated-success
  streams or restarts. Include the previously failing code prompt, varied prompts,
  repeated prefixes, multi-turn traffic, greedy and sampled requests.
- Near-native context retrieval and repeated-prefix cache reuse pass with MTP
  enabled. Context limits include generated output; record actual token counts.
- Context Guard serves the accepted Qwen alias with text/SSE and correct token
  policy. Do not silently map the existing DeepSeek alias to Qwen.
- Deterministic configuration and CLI lifecycle reproduce the accepted setup from
  either node. Record which coordinator placements have actual hardware evidence.
- Save diagnostics, failed attempts, tests and performance receipts. A single
  fast answer, healthy HTTP endpoint or automatic restart is not acceptance.

## Known evidence and current diagnosis

The plain eager/synchronous 256K candidate passed three 1024-token outputs at
27–29 decode tokens/s and correct retrieval from 260026 input tokens. Native
MTP=2 completed the same explanation prompt at 50.16 tokens/s (1.84×), then froze
on the code prompt after approximately 244 generated tokens. The worker call
`sample_tokens` timed out and the engine failed. Root cause is not yet established.

The saved engine uses vLLM 0.25.1, PyTorch 2.11.0+cu130, FlashInfer 0.6.13 and
NCCL 2.28.9. Greedy target/draft sampling bypasses top-k/top-p selection; a known
older FlashInfer top-k sampler race is not sufficient evidence for this failure.
GPU utilization and zero RDMA progress alone cannot distinguish collective
mismatch from an earlier CUDA kernel blocking subsequent work.

Reproduction `mtp-diagnosis-01` retains the exact failed recipe and collects
nonblocking per-worker Python stacks and NCCL RAS collective counts on a stall.
Artifacts live under controller `state/serving-20260910/mtp-diagnosis-01/` and
per-node `~/scratch/mtp-diagnosis-01/`. Any targeted workaround still requires the
acceptance sequence above; do not mark this goal complete on successful startup.

## Repeated-request check

With an already-running deployment and its saved plan:

```bash
.venv/bin/python scripts/soak-spark-serving.py \
  --saved-plan /path/to/plan.json --rounds 3 --max-tokens 1024 \
  --output /path/to/new-soak-results.json
```

The default runs 18 sequential requests: explanation, code and planning, each
followed by a continuation, repeated across three rounds (temperatures 0, 0.7,
0). It checks completed SSE, real token usage, normal finish reasons and native
accepted-token counters, preserving completed requests if a later one fails.
It does not launch, restart or stop workers. Long-context retrieval remains a
separate check using `probe-spark-long-context.py`.

The reproduction froze again on the code request. Before debugger attachment,
NCCL RAS reported frozen all-gather counts **2713/2710** and all-reduce counts
**56033/56028** across ranks 0/1. Rank 0's Python stack waited in `_to_list`;
rank 1 waited in the drafter's `fc` all-gather. The runtime routes these gathers
through PyTorch and reductions through PyNccl, using separate communicators.
This supports testing cross-communicator launch ordering, but unequal launched
counts alone are not proof of the root cause. CUDA debugger attachment from a
separate namespace could not resolve the target symbols/devices; no actual
stuck CUDA kernel was identified by that capture.

Candidate `large-tp2-mtp2-ordered-{66f1,e8f1}` adds the typed recipe option
`nccl_launch_order_implicit: true`, rendered identically as
`NCCL_LAUNCH_ORDER_IMPLICIT=1` on both workers. Existing recipes are unchanged.
[NCCL 2.28.9 documents](https://docs.nvidia.com/deeplearning/nccl/archives/nccl_2289/user-guide/docs/env.html#nccl-launch-order-implicit)
this opt-in ordering mechanism for separate communicators on one device.
Hardware acceptance of this candidate is pending.
