# Spark cluster: current implementation and remaining work

Status reviewed September 16, 2026. This is the current checklist; earlier
chronological notes are preserved in [the historical log](CLUSTER_HISTORY_20260907_10.md).
Use [CLUSTER.md](CLUSTER.md) for commands and [the scaling/recovery plan](SPARK_SCALING_RECOVERY_PLAN.md)
for work that has not yet been implemented or qualified.

## Current serving state

GLM-5.3-Flash runs across both GPUs using TP2 and DFlash2, with e8f1
coordinating. Both placements passed the selected 0.82 memory recipe. Its alias
is `local-glm53-flash`, with 262,144 context tokens and an 8,192-token output cap.
Both existing port-4010 and managed port-4110 guards publish that deployment.
See [GLM53_TP.md](GLM53_TP.md) and [its evidence](GLM53_TP_EVIDENCE.md) for
the saved plan and limits. Four scheduler slots do not qualify four concurrent
full-window requests. Inter-worker traffic uses the direct ConnectX-7 fabric;
there is no permanent compute master.

DeepSeek V4 Flash TP2 is currently paused. Its BF16 expert activation, DSpark2
and CUDA graph recipe passed both coordinator placements at 1,048,576 tokens;
the original 64K and single-node recipes remain available.

Its retained alias is `local-deepseek-v4-flash`. The published output cap is
8,192 tokens and scheduling is one active request. The existing Mac OMP override
uses 4,096 output tokens; its explicit thinking high/off toggle and real read-tool
continuation passed. Thinking is an OMP profile setting, not a changed global
server default. Fam-Chat keeps its existing bounded history, reply size and timeout.

During DeepSeek qualification, a 1,040,337-token request passed through the existing 66f1 gateway to e8f1 without
compaction, with exact token accounting and streaming keepalives. Fresh prefill
took 1,226 seconds, cached first text 5.1 seconds, and near-limit decoding 35 tok/s.
These are bounded synthetic measurements, not general accuracy or availability claims.

See [DEEPSEEK_TP.md](DEEPSEEK_TP.md) and [DEEPSEEK_CONTEXT.md](DEEPSEEK_CONTEXT.md)
for recipe identities, measured memory margins, tests and rollback commands.
Qwen3-Next BF16 TP2 plus native MTP also passed both coordinator roles using its
qualified NCCL 2.30.7 recipe; it is currently stopped while GLM owns both GPUs.
See [QWEN_TP_MTP_ACCEPTANCE.md](QWEN_TP_MTP_ACCEPTANCE.md). Earlier stalled Qwen
variants remain diagnostic history, not the current qualified recipe.

## Implementation versus acceptance

| Area | Implemented and verified | Remaining boundary |
| --- | --- | --- |
| Controller | Versioned inventory/recipes, immutable release installs, deterministic CLI, shared GPU admission, owned cleanup | Multi-host fault/fencing campaign; latest maintenance receipt records installed revisions |
| GLM TP2 + DFlash2 | Both coordinators, near-limit retrieval, tools/thinking/vision, sequential soak and 1/2/4-request screen; currently serving | Full-window concurrency, broader quality evaluation and new AIChat/OpenClaw checks remain separate |
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

## Source and package maintenance

The [September 16 audit](audits/2026-09-16/audit.md) found the same 41 copied
integration files in both baseline checkouts. All substantive source was already
in the feature history; both installed controllers were at `696af2a`.
See [source reconciliation](SOURCE_RECONCILIATION.md) for the cleanup record.

The following records describe the earlier September 12 maintenance campaign:

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

1. Complete source reconciliation as recorded above. Treat the declared system package baseline as optional maintenance for build, audio and diagnostic workflows; it is not a prerequisite for the currently running GLM service.
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
recovery gaps are closed. GLM is the current service; restore the accepted DeepSeek recipe after releasing GLM with its exact saved plan.
