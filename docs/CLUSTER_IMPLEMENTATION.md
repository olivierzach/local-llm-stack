# Spark cluster: current implementation and remaining work

Status reviewed September 16, 2026. This is the current checklist; earlier
chronological notes are preserved in [the historical log](CLUSTER_HISTORY_20260907_10.md).
Use [CLUSTER.md](CLUSTER.md) for current commands,
[the local-first operations plan](LOCAL_FIRST_OPERATIONS_PLAN.md) for the full
automatic fallback/failback contract, and
[the scaling/recovery roadmap](SPARK_SCALING_RECOVERY_PLAN.md) for topology and
enrollment work that has not yet been implemented or qualified.

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

## Recovery implementation and remaining acceptance

The [local-first operations plan](LOCAL_FIRST_OPERATIONS_PLAN.md) now covers the
full preferred-TP2 → qualified existing single-node fallback → automatic TP2
failback lifecycle, not only the first observer slice. The continuing
[implementation goal](SPARK_RECOVERY_GOAL.md) records implementation and local
verification separately from physical acceptance. Local development does not
authorize live generation, restarts, route changes or hardware faults; both GPUs
remain occupied by GLM until separately approved maintenance.

The user has selected **this Mac, kept awake**, for the recovery authority/controller
and stable gateway. Implemented packaging uses launchd-supervised foreground services,
independent of OMP, VS Code and terminal sessions—not independent of the Mac being
awake, powered, network-connected and logged into the service user. Linux/systemd is a portability option, not
a required third host or an unresolved host-selection gate. Installation,
always-awake/trust/network provisioning and activation still require separate
approval; no recovery service has been installed or enabled by local development.
Local CPU/HTTP integration and read-only live monitoring have been exercised;
automatic enablement remains unqualified and disabled.

The Mac is a single point of failure, not HA. Sleep, power loss or required network
loss makes the stable gateway unavailable and suspends recovery. Authority loss
ends route-lease renewal; expired routes fail closed without releasing GPU ownership.
An operator must restore wake/power/connectivity as needed, after which services
must reconcile durable state and actual ownership before resuming. Physical
acceptance must cover these faults and separately prove service survival after
closing OMP/VS Code/terminal sessions.

Implementation stays on branch `feat/spark-automatic-recovery` in
`/Users/statsparrot/projects/local-llm-stack-recovery`; the original checkout
`/Users/statsparrot/projects/local-llm-stack` and its existing edits remain untouched.
The durable goal document is not a native `/goal` activation: that tool is unavailable
in this session.

1. **Observer and combined monitoring — locally verified.** `sparkctl observe`
   provides bounded partial observations without mutation locks. `spark-monitor`
   collects host/GPU/unified-memory and physical NIC/RDMA counters; exact native
   engine metrics travel over independent management SSH with container/model
   identity verification. Actual Prometheus/Grafana queries and plotted host,
   native engine and physical cable data were exercised read-only. Unsupported
   values remain unavailable. Two logical interfaces represent one QSFP cable,
   not two independent cables; no saturation test was run.
2. **Authority, stable routes and safe recovery — locally verified.**
   `spark-recover` persists authority epochs, worker fences, transition intents,
   quarantine, retry/circuit state and exact plans. Real loopback HTTP integration
   exercised preferred → surviving single → automatic exact preferred failback,
   strict aliases, authenticated SSE and actual backend/limit reporting, with
   simulated GPU/SSH state only. Independent serving is separate from collectives;
   only explicit `local-auto` may substitute recipes. Physical Wi-Fi/fabric
   independence, node fencing and ingress isolation still require acceptance.
3. **Scheduled symmetric fallback/failback qualification.** Qualify the chosen
   existing single-node recipe on each survivor, then exercise either host
   failing, cable loss/return, both coordinator choices, controller restart,
   competing owners and dropped replies. Coder tools/SSE evidence with e8f1
   serving and 66f1 absent is not coder66 acceptance. After stable returned-node
   and fabric readiness, failback must automatically drain all managed ingress,
   release fallback resources and restore the exact TP2 plan, with bounded
   rollback/backoff if restoration fails. Record the service gap; do not claim
   warm overlap, stream/KV migration or generation/tool replay. Maintenance
   pause/disable is valid; routine failback is not manual-only.
4. **Service packaging, enrollment, setup/reset and recipe expansion — implemented.**
   `spark-services` renders, validates and transactionally installs pinned,
   disabled-default LaunchAgents. Real foreground gateway start/shutdown and
   crash-repair regressions passed; actual launchd session-loss survival remains
   untested because installation is not approved. `spark-node` admits/removes
   reviewed inventory and stages only selected pinned artifacts; N-peer bootstrap
   requires independently pinned host keys and preserves unrelated SSH state.
   New physical members and recipes still need hardware qualification.
   Coordinator switching means a qualified full-group restart, not live
   rank promotion. The [scaling roadmap](SPARK_SCALING_RECOVERY_PLAN.md) retains
   topology/enrollment details: DeepSeek TP3 remains rejected by its 64-head
   partition constraint; a third node may instead run an independent workload.
   Owned reset leaves recovery disabled and preserves caches, credentials, epochs and
   history. The runnable approval/command contracts are in section 7 of the
   [operations plan](LOCAL_FIRST_OPERATIONS_PLAN.md).

### Separate maintenance and deferred acceptance

- Source reconciliation is complete; see its receipt above. The declared
  system package baseline remains optional maintenance for build, audio and
  diagnostic workflows, not a prerequisite for the running GLM service.
- Full 66f1 catalog, Vector and Loop GPU acceptance still needs an idle-GPU
  window; do not interrupt active OMP testing. The recovery recipe's per-node
  qualification does not claim completion of these unrelated acceptance items.
- **Planned; no execution now:** qualify agent concurrency versus context using
  [the DeepSeek concurrency test plan](DEEPSEEK_AGENT_CONCURRENCY_PLAN.md).
  The user accepts a smaller context window for multiple simultaneous agents;
  compare sequence limits 2/4/8 and context ceilings before choosing a profile.
  Include OMP/gateway compaction latency, cold/cached prefill, retained facts,
  incremental summaries and interference with other agents. The current 1M
  no-compaction acceptance does not qualify compaction quality or efficiency.
  Larger output budgets and remote drafting remain separate optional work.

K3s is an optional later backend/pilot after the two-node recovery foundation,
not a required migration or a capacity multiplier. The
[scaling roadmap](SPARK_SCALING_RECOVERY_PLAN.md) records supported-platform
references, integration costs and conditions for revisiting it.

Standard supervised foreground services, a private atomic journal, immutable plans,
expiring route leases, rollback and observability address recovery's actual safety
boundaries. Orchestrator choice changes operations, not GPU capacity or the selected
Mac's awake/power/network dependency.

The broad interchangeable-node goal remains incomplete until its hardware and
recovery gaps are closed. GLM is the current preferred service; automatic
failback restores its saved TP2 plan, not a different historical model. A
separate approved switch back to accepted DeepSeek likewise requires its exact
saved plan after owned release of GLM.
