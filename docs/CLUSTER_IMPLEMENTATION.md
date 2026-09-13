# Spark cluster: current implementation and remaining work

Status reviewed September 12, 2026. This is the current checklist; earlier
chronological notes are preserved in [the historical log](CLUSTER_HISTORY_20260907_10.md).
Use [CLUSTER.md](CLUSTER.md) for commands and [the scaling/recovery plan](SPARK_SCALING_RECOVERY_PLAN.md)
for work that has not yet been implemented or qualified.

## Current serving state

DeepSeek V4 Flash runs across both GPUs using TP2, BF16 expert activations,
DSpark2 and CUDA graphs. e8f1 currently coordinates. Both coordinator placements
passed the same 1,048,576-token recipe; the original 64K and single-node recipes
remain available. Inter-worker communication and cross-node backend requests
use the direct ConnectX-7 fabric. There is no permanent compute master.

The public alias is still `local-deepseek-v4-flash`. The published output cap is
8,192 tokens and scheduling is one active request. The existing Mac OMP override
uses 4,096 output tokens; its explicit thinking high/off toggle and real read-tool
continuation passed. Thinking is an OMP profile setting, not a changed global
server default. Fam-Chat keeps its existing bounded history, reply size and timeout.

Both BASE port-4010 and managed port-4110 guards publish the accepted deployment.
A 1,040,337-token request passed through the existing 66f1 gateway to e8f1 without
compaction, with exact token accounting and streaming keepalives. Fresh prefill
took 1,226 seconds, cached first text 5.1 seconds, and near-limit decoding 35 tok/s.
These are bounded synthetic measurements, not general accuracy or availability claims.

See [DEEPSEEK_TP.md](DEEPSEEK_TP.md) and [DEEPSEEK_CONTEXT.md](DEEPSEEK_CONTEXT.md)
for recipe identities, measured memory margins, tests and rollback commands.
Qwen3-Next BF16 TP2 plus native MTP also passed both coordinator roles using its
qualified NCCL 2.30.7 recipe; it is currently stopped while DeepSeek owns both GPUs.
See [QWEN_TP_MTP_ACCEPTANCE.md](QWEN_TP_MTP_ACCEPTANCE.md). Earlier stalled Qwen
variants remain diagnostic history, not the current qualified recipe.

## Implementation versus acceptance

| Area | Implemented and verified | Remaining boundary |
| --- | --- | --- |
| Controller | Versioned inventory/recipes, immutable release installs, deterministic CLI, shared GPU admission, owned cleanup | Multi-host fault/fencing campaign; latest maintenance receipt records installed revisions |
| DeepSeek TP2 | Both coordinators; text, tools, thinking, long context, sequential soak, direct RoCE | Concurrent serving and broader model-quality evaluation are separate |
| Qwen TP2 + MTP | Qualified recipe with either coordinator and client gateways | Not currently running; other PP/compiled variants are not inferred accepted |
| Model artifacts | Single-node catalog copied and hashed; native engines prepared; TP checkpoints verified on both nodes | Files and identical Makefiles alone do not certify all-model feature parity |
| Single-node inference | Nine Compose model configurations passed text/SSE on e8f1; native DeepSeek and Qwen tested; selected models tested on 66f1 | Full 66f1 catalog and per-model tool/vision coverage |
| Host Python | 167-package lock, dependency checks on both; e8f1 CUDA/Adam smoke and venv activation | 66f1 host CUDA smoke needs an idle GPU window; OS packages are checked separately |
| Vector Bucket | CLAP track/clip and MERT track GPU jobs on e8f1; matching source/models staged on 66f1 | Real 66f1 GPU acceptance; broader audio preprocessing |
| Loop LLM | Immutable source placement, bounded supervised jobs and e8f1 optimizer smoke | Real 66f1 managed-job GPU acceptance |
| Clients | Existing Mac OMP tools/thinking, llm, Fam-Chat upstream provider; generated AIChat; OpenClaw catalog/config | Fam-Chat browser workflow and full million-token workflows in every client not claimed |
| Gateway routing | Stable aliases, placement independent of compute, real auth/text/tool checks, CPU stream-failure tests | Stable frontend across host loss, migrated virtual-key accounting, application-state availability |
| More nodes | Generic inventory and launcher render N ranks | Peer SSH setup, topology discovery, model-compatibility gates and publication acceptance still need generalization |
| Remote drafter | Local DSpark and speculation-off paths | Separate remote-drafter engine/recipe has not been implemented or qualified |

## September 12 maintenance

- Controller release `1111496`: synchronization and no-restart verification
  recorded under `data/cluster/maintenance-20260912/` and the corresponding Spark state directory.
- System package baseline: **awaiting the user's sudo install on each node**.
  Initial audit found seven missing packages on 66f1 (ninja-build, git-lfs,
  ripgrep, sox, iperf3, openmpi-bin, libopenmpi-dev) and five on e8f1
  (all of those except ninja-build and git-lfs). Re-run the baseline check after
  installation; do not interpret this initial list as proof of current absence.
- Personal API keys, existing client URLs and GPU workers are not part of a
  controller source sync. Package parity means the declared functional baseline,
  not downgrading drivers/kernels to make all dpkg versions identical.

## Remaining work, in order

1. Finish and verify the source/package maintenance above.
2. Schedule 66f1 single-node model, Vector and Loop acceptance after releasing TP2;
   do not interrupt the user's active OMP testing to run it.
3. Implement and exercise [the recovery and stable-endpoint plan](SPARK_SCALING_RECOVERY_PLAN.md).
4. **Deferred at the user's request:** implement automatic node enrollment and
   network topology generalization once, then add nodes by inventory and
   deployment data; the proposed enrollment workflow is in the plan above.
   DeepSeek TP3 is rejected by the pinned model's 64-head
   partition constraint; a third node can instead run an independent workload.
5. **Planned; no execution now:** qualify agent concurrency versus context using
   [the DeepSeek concurrency test plan](DEEPSEEK_AGENT_CONCURRENCY_PLAN.md).
   The user accepts a smaller context window for multiple simultaneous agents;
   compare sequence limits 2/4/8 and context ceilings before choosing a profile.
   Include OMP/gateway compaction latency, cold/cached prefill, retained facts,
   incremental summaries and interference with other agents. The current 1M
   no-compaction acceptance does not qualify compaction quality or efficiency.
   Larger output budgets and remote drafting remain separate optional work.

Kubernetes migration is deferred; the [scaling plan](SPARK_SCALING_RECOVERY_PLAN.md)
records the recommendation, integration costs and conditions for revisiting it.

The broad interchangeable-node goal remains incomplete until its hardware and
recovery gaps are closed. The accepted DeepSeek serving recipe is usable now.
