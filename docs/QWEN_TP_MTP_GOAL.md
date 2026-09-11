# Qwen two-Spark TP + native speculation acceptance

Active objective requested September 10, 2026: make the full BF16
Qwen3-Next-80B-A3B-Instruct model usable across both Sparks with tensor parallelism
and native MTP speculative decoding, and leave the accepted deployment serving.
DeepSeek may stay down during this work. Preserve unrelated workloads, existing
model aliases and reversible single-node startup paths.

The latest hardware results are in [QWEN_TP_MTP_ACCEPTANCE.md](QWEN_TP_MTP_ACCEPTANCE.md).
The NCCL 2.30.7 candidate passed full serving and both guards with e8f1 coordinating;
66f1 coordinator acceptance is now running.

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
This candidate failed extended acceptance. It completed all three 1024-token
profiles at 47.16–52.83 tokens/s, retrieved all codes from 260029 input tokens,
and reused 259280 prefix tokens (TTFT 132.07s unique, 1.27s repeated). Eight
subsequent requests completed, then sampled code generation stalled. Captures
show rank 0 in drafter-logits all-gather and rank 1 synchronizing rejection
sampler output; collective counts stopped at AllGather 24971/24973 and AllReduce
514586/514589. Thus implicit ordering alone is not an accepted fix.

The next `large-tp2-mtp2-native-sampler-{66f1,e8f1}` candidate retains ordering
and adds `flashinfer_sampler: false`, rendering the supported
`VLLM_USE_FLASHINFER_SAMPLER=0` on both workers. The stock runtime source uses
this to select its native bonus-token sampler; MTP remains enabled. A
[reported FlashInfer 0.6.13 sampling hang](https://github.com/vllm-project/vllm/issues/52247)
motivates this experiment, but the kernel causing our stall has not been
identified. Full hardware acceptance remains required.

The native-sampler candidate also failed: three profiles and near-context
retrieval passed, then the third soak request (greedy code) stalled. Debugging
inside the owned worker's mount/PID namespaces succeeded: **both GPUs were in
`ncclDevKernel_AllGather_RING_LL`**, with matching RAS launched counts (12788
all-gathers, 263578 all-reduces). The kernel name alone does not establish the
selected protocol; NCCL shares kernel names across protocol implementations.
Disabling FlashInfer sampling therefore does not fix this failure.

A small model-free reproducer alternates PyNccl reductions with PyTorch gathers.
The mixed path stalled around iteration 2300; using PyTorch for both operations
completed 6000 iterations. An extended run with periodic reduction checks passed
60000 iterations (2.16 million collective calls) in 85.1 seconds. The mixed path
with the same checks stalled again around iteration 6500.
`scripts/stress-spark-collectives.py` preserves that
experiment, including gather correctness and periodic reduction checks. Run it
on two reserved, otherwise idle GPU containers with the same pinned image and
fabric environment, rank 0 on the chosen master and rank 1 on its peer. Wrap
each rank in `timeout --kill-after=15 420`; use matching `--master`, `--port`,
`--mode mixed|torch|pynccl`, and `--steps` arguments. It does not load a model.

Candidate `large-tp2-mtp2-torch-nccl-{66f1,e8f1}` adds `disable_pynccl: true`,
rendering the runtime's supported `VLLM_DISABLE_PYNCCL=1`. Its existing
all-reduce fallback uses PyTorch's device process group, matching all-gather.
The native sampler and NCCL ordering settings remain fixed during this test.
Full-model acceptance failed: two 1024-token profiles completed at 44.11 and
49.13 tokens/s, then the planning request stalled. Both GPUs were in all-gather,
with one communicator reporting matching counts (5433 gathers, 112064 reductions).
Thus unifying communicators is insufficient despite the small test passing.

The next candidate `large-tp2-mtp2-nccl2307-{66f1,e8f1}` keeps the native sampler
and implicit ordering, restores the normal mixed communicator path, and replaces
both runtime NCCL consumers with **2.30.7**. The same mixed-path correctness test
passed **60000 iterations / 2.16 million calls in 79.9 seconds** on both ranks.
Stock 2.28.9 also stalled with forced Simple protocol and blocking-wait settings.
This comparison motivates full serving acceptance; it is not acceptance itself.

The ARM64 NVIDIA wheel and extracted library have independent SHA-256 pins in
`cluster/runtime-libraries/nccl-2.30.7-aarch64.json`. Install on each node:

```bash
python3 scripts/install-spark-nccl.py \
  --manifest cluster/runtime-libraries/nccl-2.30.7-aarch64.json \
  --cache "$HOME/projects/local-llm-stack/data/huggingface"
```

Use `--wheel /path/to/pinned.whl` to reuse a download copied over the direct cable.
The installer stages a regular read-only file under `data/runtime-libraries`;
it does not install a host package or change drivers. Preflight, reservation and
start verify its digest. Compose mounts it over the image's original library
path and sets `VLLM_NCCL_SO_PATH` to that same path. Merely using `LD_PRELOAD`
left two library versions mapped and failed the test. Verify actual runtime
NCCL logs and mapped library paths: `torch.cuda.nccl.version()` reports PyTorch's
build-time NCCL macros, even when a different compatible library is loaded.


## Reproduce the candidate from either controller

Both installed controllers have the same deployment manifests. The machine
issuing the command need not be the chosen coordinator. With both GPUs free,
use a fresh receipt directory and the pinned controller release:

```bash
cd ~/projects/local-llm-stack-cluster/current
qwen_run="$HOME/projects/local-llm-stack-cluster/state/qwen-mtp-$(date +%Y%m%d-%H%M%S)"
qwen_deployment=cluster/deployments/large-tp2-mtp2-nccl2307-e8f1.json
scripts/sparkctl preflight --deployment "$qwen_deployment" --output "$qwen_run"
scripts/sparkctl up --deployment "$qwen_deployment" --output "$qwen_run" --timeout 3600
.venv/bin/python scripts/profile-spark-decode.py \
  --saved-plan "$qwen_run/plan.json" --max-tokens 1024 --output "$qwen_run/decode.json"
.venv/bin/python scripts/probe-spark-long-context.py \
  --saved-plan "$qwen_run/plan.json" --input-tokens 260032 --output "$qwen_run/long-context.json"
.venv/bin/python scripts/soak-spark-serving.py \
  --saved-plan "$qwen_run/plan.json" --rounds 3 --max-tokens 1024 --output "$qwen_run/soak.json"
```

Run commands sequentially and stop on any failure. Startup's short probe does
not replace these acceptance checks. The same serving sequence, including three
4096-token output profiles after the soak, is available as one command:

```bash
.venv/bin/python scripts/accept-spark-serving.py \
  --saved-plan "$qwen_run/plan.json" --output "$qwen_run/acceptance"
```

It stops at the first failed check, preserves receipts and does not restart
workers or install routes. Its result is bounded reliability evidence, not an
uptime guarantee. To inspect or stop only this deployment:

```bash
scripts/sparkctl status --saved-plan "$qwen_run/plan.json"
scripts/sparkctl down --saved-plan "$qwen_run/plan.json"
```

The `...-66f1.json` manifest changes coordinator placement; its hardware
acceptance remains separate. Do not launch both placements simultaneously.
The controller refuses unrelated GPU owners rather than stopping their jobs.
The full Linux controller suite passed **335 tests**, zero failures or skips,
on e8f1 at release `767e81b`. That result does not certify GPU runtime behavior.

## Client route after acceptance

To add the accepted saved plan to an existing stack's Context Guard, preserving
all other route overrides and the backend's served-model name:

```bash
.venv/bin/python scripts/configure-context-routes.py \
  --root "$HOME/projects/local-llm-stack" --plan /path/to/accepted-plan.json \
  --merge --alias local-qwen3-next-80b
```

When the existing guard already mounts the registry directory and has
`CONTEXT_GUARD_ROUTE_REGISTRY` set, its next request reads the updated registry;
no restart is needed. Otherwise activate using the stack's documented Context
Guard restart procedure. Then run the existing stack's `scripts/probe-context-route.py
--model local-qwen3-next-80b --output /path/to/new-route-receipt.json` through
each node, using that stack's `.env` and registry.
The new alias is distinct from `local-large` and `local-deepseek-v4-flash`; existing
clients are not silently assigned a different model. This does not yet certify
Qwen tool calling: the current TP recipe advertises text and streaming only.
