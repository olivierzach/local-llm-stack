# Qwen BF16 tensor parallelism with native speculation

The pinned NCCL 2.30.7 recipe passed the full bounded serving suite with **either
Spark coordinating** on September 11, 2026. The 66f1 placement subsequently
passed a tool-enabled upgrade; [QWEN_TOOL_CALLING.md](QWEN_TOOL_CALLING.md) records
the current service. Native MTP remains enabled and DeepSeek intentionally down. Both
existing Context Guards and both independent client gateways passed real text
and streaming checks. This completes the focused Qwen TP+MTP serving objective;
it does not certify unrestricted concurrency, other parallelism recipes or
long-term uptime.

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

## 66f1 coordinator baseline results

Controller source used by the unattended acceptance command:
`c2b4e46f72cb74e103e175562650f7059b2d02f7`.
Deployment digest: `ca01d2875558ae6de36a94375ebb74ad4020cc27c8981ed8abc6cbfc573fe1a9`.
Receipts: `data/cluster/serving-20260910/mtp-nccl2307-66f1-01/`; the full sequence
is in `serving/acceptance.json`, with e8f1 telemetry and gateway checks in `peer/`.
All checks completed without a worker restart or stall.

| Case | Output tokens | Decode tokens/s |
|---|---:|---:|
| Explanation | 1024 | 50.69 |
| Code | 1024 | 51.52 |
| Planning | 1024 | 45.83 |
| Long explanation | 4096 | 48.92 |
| Long code | 4096 | 53.73 |
| Long planning | 4096 | 48.70 |

The 18-request soak generated 18432 tokens; median decode speed was **48.02
tokens/s**, with **10959 / 14942 proposed tokens accepted (73.3%)**. Near-context
retrieval used **260023 input tokens**, recovered all codes, and reused 259280
prefix tokens. First-token latency was **140.63s fresh / 1.25s repeated**.
Profiles, soak and retrieval generated 33878 tokens, excluding warmups and
client probes. The complete sequence took 851.35 seconds after startup.

Both nodes recorded **171.25–171.55 GB per direction** on the direct RoCE rails,
with exactly matching peer byte deltas and zero receive errors/transmit discards.
Sampled available host memory stayed above 8.52 GiB (66f1) and 11.75 GiB (e8f1);
peak sampled GPU temperatures were 77°C on both. Temporary diagnostic monitors
were stopped after acceptance; the model workers remained healthy and running.

Backend: `http://10.10.20.1:8121/v1`, served internally as `local-large`.
Stable client alias: **`local-qwen3-next-80b`**. The alias was first verified
against e8f1, then kept unchanged while its route moved to the accepted 66f1
coordinator. No permanent master node is required.

From either installed controller, inspect or stop the current deployment:

```bash
cd ~/projects/local-llm-stack-cluster/current
qwen_plan="$HOME/projects/local-llm-stack-cluster/state/serving-20260911/qwen-tools-02/plan.json"
scripts/sparkctl status --saved-plan "$qwen_plan"
# When intentionally releasing both GPUs:
scripts/sparkctl down --saved-plan "$qwen_plan"
```

## Context Guard

Both existing port-4010 guards passed with each coordinator: text, SSE, exact token counting, the
262144 context limit and invalid-key rejection for **`local-qwen3-next-80b`**.
Their `local-deepseek-v4-flash` registry entries remained unchanged. DeepSeek is
intentionally down and is therefore omitted from health-filtered model discovery;
configuration preservation is separate from backend availability. The initial
probe requiring that offline model to be discoverable failed as expected; the
Qwen-specific probe then passed on both nodes. No guard restart was needed.

Both independent port-4110 gateways also passed text and SSE against the final
66f1 deployment, with its exact deployment digest in response headers. Their
existing `local-coder` routes were preserved. `spark-gateway routes --merge
--alias local-qwen3-next-80b --plan PLAN` reproduces the additive update under a
registry lock. Mac controller attachments were refreshed, and generated OMP,
OpenClaw, AIChat and llm profiles include Qwen on either gateway. Profile
configuration plus gateway acceptance does not certify every client feature.

The existing Mac OMP provider discovers
`spark-context-guard/local-qwen3-next-80b` through its unchanged endpoint. Its
model override now sets `maxTokens: 16384`; the catalog confirms a 262144 context
window, 16384 output cap, text input and no separate reasoning stream. All other
OMP configuration was preserved, with a private backup. To reproduce that cap,
add under the existing provider's `modelOverrides`:

```yaml
local-qwen3-next-80b:
  maxTokens: 16384
```

Fam Chat's configured upstream remains Context Guard; its authenticated UI
session was not separately exercised in this run.

The baseline recipe advertised text and streaming. The current successor also
passed tool-calling acceptance; see [QWEN_TOOL_CALLING.md](QWEN_TOOL_CALLING.md).
Image input and a separate thinking stream remain unsupported by this checkpoint.

## Reproduction and limits

Controller `c2b4e46f72cb74e103e175562650f7059b2d02f7` adds a single
fail-fast acceptance command. `558bdf76d979eaad8e4f4c3b6fc8045543bdbf2f` adds
the tested additive gateway publication command; both controllers have it. It retains the same serving recipe and digest as
`767e81b`. Follow [QWEN_TP_MTP_GOAL.md](QWEN_TP_MTP_GOAL.md) for library staging,
startup, acceptance, route installation and exact-plan cleanup.

The full Linux regression suite passed 335 tests at `767e81b`; three additional
acceptance-orchestration failure tests passed at `c2b4e46`. Gateway merge and
regression checks passed 26 tests locally; both new merge tests also passed on
Linux at `558bdf7`. Earlier stock-NCCL
candidates failed repeatedly. Keep their diagnostics: the working combination
is the pinned recipe above, not a claim that every NCCL upgrade or speculative
configuration is interchangeable. Multi-request full-context concurrency,
compiled execution, PP+MTP and arbitrary remote drafting remain separate work.
