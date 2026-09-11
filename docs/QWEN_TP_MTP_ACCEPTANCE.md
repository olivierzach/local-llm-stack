# Qwen BF16 tensor parallelism with native speculation

The NCCL 2.30.7 candidate passed the full bounded serving suite with **e8f1
coordinating**, plus both existing Context Guards, on September 11, 2026.
The same suite with **66f1 coordinating is in progress**. This document records
actual evidence; it does not certify unrestricted concurrency or long-term uptime.

## Pinned configuration

- Model: `Qwen/Qwen3-Next-80B-A3B-Instruct`, revision
  `9c7f2fbe84465e40164a94cc16cd30b6999b0cc7`, BF16, no weight quantization.
- Runtime: vLLM image
  `sha256:e4f88a835143cd22aee2397a26ec6bb80b3a4a6fe0c882bcbc63822904766089`.
- NCCL: NVIDIA ARM64 `nvidia-nccl-cu13==2.30.7`, library SHA-256
  `fc7ea66334edbc934aa25959b9907dbb2b91a1d2485beff18839afc45cbc08d0`.
  The read-only mount overrides the original container library for both PyTorch
  and PyNccl. Both live workers reported 2.30.7 and mapped one NCCL library.
  Host drivers and system packages were not replaced.
- TP=2, native MTP=2, eager execution, synchronous scheduling, native sampler,
  implicit NCCL launch ordering, custom all-reduce disabled.
- Native context limit 262144, output cap 16384, prefix caching, one scheduled
  sequence, 80% memory budget. Outputs were tested through 4096 tokens.
- Both RoCE rails use the direct inter-Spark cable. These runs use host-staged
  RDMA (`GDR 0`), not validated GPU Direct RDMA or a 400-Gbit/s aggregate link.

Recipe: `cluster/recipes/qwen3-next-80b-256k-mtp2-nccl2307.json`.
Placement manifests: `cluster/deployments/large-tp2-mtp2-nccl2307-{66f1,e8f1}.json`.
The issuing controller and chosen coordinator can differ. The coordinator is a
per-deployment role, not a permanent hardware master.

## e8f1 coordinator results

Controller runtime source: `767e81b535e42b4a9690ab9c70c08b308a400668`.
Deployment digest: `6a15e0bf413e3ff18412d522037f3d27553e479d3bdc0da6039c494bf71119c3`.
Receipts: `data/cluster/serving-20260910/mtp-nccl2307-01/`, including `peer/`
for 66f1 telemetry and route checks. All serving checks passed without a worker
restart. The placement was intentionally stopped afterward to test the other
coordinator; that transition is recorded separately from the acceptance result.

| Case | Output tokens | Decode tokens/s |
|---|---:|---:|
| Explanation | 1024 | 46.61 |
| Code | 1024 | 52.56 |
| Planning | 1024 | 46.06 |
| Long explanation | 4096 | 49.02 |
| Long code | 4096 | 54.71 |
| Long planning | 3926 (normal stop) | 47.90 |

The 18-request soak generated 18432 tokens across fresh prompts and continuations
at temperatures 0, 0.7 and 0. All completed normally; median decode speed was
49.39 tokens/s. MTP accepted **11101 / 14664 proposed tokens (75.7%)**. Combined
profiles, retrieval and soak generated 33704 output tokens, excluding warmups
and route probes. These are performance/protocol checks, not a code-quality exam.

Near-context retrieval used **260020 input tokens**, returned all three codes
correctly, and repeated successfully. First-token latency was **141.00s** for the
fresh prefix and **1.29s** on repeat, with **259280 prefix-cache hits**.

Matched telemetry recorded approximately **170.5–170.8 GB in each direction**
over the two RoCE rails per node. Peer transmit/receive byte deltas matched
exactly; receive errors and transmit discards stayed at zero. This is direct-path
evidence for the workload, not a peak-bandwidth benchmark. Sampled minimum host
available memory was 9.88 GiB (66f1) and 10.12 GiB (e8f1); peak GPU temperatures
were 76°C and 77°C respectively.

## Context Guard

Both existing port-4010 guards passed text, SSE, exact token counting, the
262144 context limit and invalid-key rejection for **`local-qwen3-next-80b`**.
Their `local-deepseek-v4-flash` registry entries remained unchanged. DeepSeek is
intentionally down and is therefore omitted from health-filtered model discovery;
configuration preservation is separate from backend availability. The initial
probe requiring that offline model to be discoverable failed as expected; the
Qwen-specific probe then passed on both nodes. No guard restart was needed.

This Qwen recipe advertises text and streaming. Tool calling, image input and a
separate thinking stream are not accepted capabilities of this recipe.

## Reproduction and limits

The installed controller `c2b4e46f72cb74e103e175562650f7059b2d02f7` adds a single
fail-fast acceptance command. It retains the same serving recipe and digest as
`767e81b`. Follow [QWEN_TP_MTP_GOAL.md](QWEN_TP_MTP_GOAL.md) for library staging,
startup, acceptance, route installation and exact-plan cleanup.

The full Linux regression suite passed 335 tests at `767e81b`; three additional
acceptance-orchestration failure tests passed at `c2b4e46`. Earlier stock-NCCL
candidates failed repeatedly. Keep their diagnostics: the working combination
is the pinned recipe above, not a claim that every NCCL upgrade or speculative
configuration is interchangeable. Multi-request full-context concurrency,
compiled execution, PP+MTP and arbitrary remote drafting remain separate work.
