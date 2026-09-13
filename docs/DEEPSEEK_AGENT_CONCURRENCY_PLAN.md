# DeepSeek TP2: context and agent concurrency test plan

Recorded September 12, 2026. **Plan only; execution is deferred at the user's
request.** Recording and pushing this document does not authorize starting the
campaign, restarting workers, changing routes or running load against the live
service. Automatic node enrollment is also deferred; see the separate
[scaling and recovery plan](SPARK_SCALING_RECOVERY_PLAN.md).

## Objective and current baseline

Choose a practical profile that finishes a group of agents sooner while keeping
each agent responsive. The user is willing to sacrifice maximum context for
multiple concurrent requests. Optimize completed, correct agent tasks and group
completion time, not aggregate tokens/second alone.

The accepted deployment is DeepSeek TP2 with DSpark2, BF16 expert activations,
CUDA graphs, a 1,048,576-token context ceiling, an 8,192-token server output cap,
80% GPU memory fraction and `max_num_seqs=1`. See
[DEEPSEEK_CONTEXT.md](DEEPSEEK_CONTEXT.md) for measured capacity and
[DEEPSEEK_TP.md](DEEPSEEK_TP.md) for acceptance and rollback.

Keep these quantities distinct:

- Context ceiling: maximum input plus generated output for one request.
- Engine sequence limit: sequences processed together in an engine iteration.
- Offered concurrency: simultaneous client requests, which may exceed the
  engine limit and wait in a queue.
- Agent count: agents can be executing tools or waiting for children without
  holding an active generation slot.

Two TP ranks form one engine, not two independent replicas. Sending requests
through different gateways does not create additional compute capacity. The
current gateway counts active requests for replica selection but has no
per-replica admission cap; an application queue policy must not be assumed.

## Adaptive screening matrix

First measure the unchanged 1M/sequence-1 baseline at offered concurrency 1, 2
and 4. Then screen the following candidates, keeping weights, runtime, precision,
DSpark2, memory fraction, output budgets and initial prefill budget unchanged.

| Context ceiling | Engine sequence limits | Question |
| --- | --- | --- |
| 1M (1,048,576) | 2, then 4 if viable | Is reducing context actually necessary? |
| 256K (262,144) | 2, 4 | Can a substantial context support an everyday agent group? |
| 128K (131,072) | 4, then 8 if promising | Does a smaller ceiling improve useful concurrency? |
| 64K (65,536) | 4, 8 | Is further context sacrifice worth the additional benefit? |

These are candidates, not supported capacity claims. Do not run a full Cartesian
sweep. Start with short requests, reject unsafe or consistently dominated
candidates, and expand only promising configurations. For each candidate compare
offered concurrency 1, 2 and 4, adding 8 for viable high-concurrency candidates.
Include a bounded overload test above the engine limit for finalists.

The starting hypothesis is **128K–256K with four active sequences**. It is not a
recommendation to deploy those settings before qualification. Measure identical
short workloads across context ceilings to isolate allocation effects. Separately
test the cost of each profile's actual long-context capability.

The recorded 1M allocator estimate of roughly 2.82 full-length requests is not
concurrency acceptance. This model's hybrid cache changes allocation ratios with
the configured ceiling. Do not extrapolate capacity linearly, add both ranks'
cache capacities, or assume lowering the ceiling frees proportional memory.

## Workloads and measurements

Use fixed, versioned synthetic fixtures and exact tokenizer accounting, without
private chats or repositories. Start with 4K, 16K and 64K input classes; include
128K and 256K classes only where input plus output and gateway margins fit.
Use matched 1K and 4K output budgets and record actual lengths, including
reasoning tokens where reported. The published 8K cap gets a separate boundary
check. Thinking high/off are separate measured cases, not pooled samples.

| Workload | What it reveals |
| --- | --- |
| Independent requests with unique prefixes | Cold prefill, queueing and sustained decode without deliberate prefix reuse |
| Shared-prefix agents and repeated continuations | Real cache benefit, branching cost and retention under pressure |
| One parent, four children, final synthesis | End-to-end group completion time including dependency barriers |
| Tool-heavy multi-turn loops with fixed synthetic tool delays | Interleaving generation with tool work; tool-call/result correctness |
| Long reasoning alongside short interactive requests | Streaming pauses, starvation and fairness |
| A large fresh prompt arriving during ongoing decoding | Prefill interference and responsiveness under mixed load |
| Concurrent near-ceiling requests | Advertised capacity, cache exhaustion, preemption and memory pressure |
| Cancellation and bounded overload | Queue cleanup, visible errors and service recovery without replaying tool effects |

For every request record submission, first stream event, first reasoning/content,
first useful answer or tool call, completion, output usage, finish reason and
error. Keep keepalive events out of first-token measurements. Record queue time
separately where the pinned runtime exposes it; otherwise label the measurement
as combined queue plus prefill latency. Report client-observed chunk gaps without
calling them exact token gaps when a chunk contains several tokens.

Report group completion time, completed correct tasks/minute, per-agent decode
speed, total throughput and latency distributions. Save per-request results,
including timeouts, cancellation and failed warmups. Track speculative acceptance,
KV occupancy, prefix hits, preemption/recomputation, both nodes' available memory,
paging/pressure, temperatures, worker health and direct-fabric/RDMA counters.

Warm each shape outside the timed phase. Repeat screening rounds at least three
times with matched fixtures, varying candidate order to reduce cache/thermal
bias. Finalists need at least 100 completed request observations per primary
workload class across repeated rounds before describing p95 as anything beyond
exploratory; retain sample counts and variability. A large sample does not itself
establish a production SLA.

## Selection, safety gates and optional tuning

First reject candidates with hangs, worker restarts, OOM, correctness/tool
regressions, unexplained stream truncation, missing cleanup or failed existing
memory-pressure gates. Stop an unsafe trial promptly, preserve evidence and
restore the exact accepted baseline. Do not reduce correctness requirements to
win throughput. Existing synthetic checks remain regression tests rather than
a broad model-quality guarantee.

For the survivors present a tradeoff table: context, engine/offered concurrency,
group completion time, median/p95 first useful response, worst streaming pauses,
per-agent/aggregate rate, failures and memory headroom. Prefer the smallest
sequence limit that achieves most of the agent-group benefit without unacceptable
response stalls. Mark acceptable latency bounds before finalist testing, using
the baseline and the user's interactive needs; do not invent a universal optimum.

Only after isolating sequence/context effects consider a second, narrow prefill
budget sweep on the best candidates. The initial recipe uses 2,048 batched
tokens. vLLM documents that smaller prefill batches can favor inter-token latency,
while larger batches can favor prompt processing; verify chunked-prefill behavior
and allowed settings in the pinned runtime before applying that guidance.
[vLLM tuning documentation](https://docs.vllm.ai/en/v0.22.1/configuration/optimization/)
is general background, not qualification of this custom DeepSeek engine.

Preserve DSpark2 during the primary comparison. If batching exposes speculative
kernel/graph incompatibilities or poor acceptance, treat speculation-off or a
different execution mode as separately identified diagnostic candidates. Do not
silently change the recipe or claim those candidates preserve accepted speedups.

## Implementation and release steps, when testing is resumed

1. Record the current exact plan, images, routes, worker IDs and rollback recipe.
   Arrange an exclusive test window: both GPUs are needed, and a different port
   does not allow a second full model to coexist safely.
2. Add parameterized candidate generation and a resumable experiment manifest.
   Use deterministic CLI orchestration, explicit bounds and saved results so
   reruns require no agent. Refuse conflicting GPU owners and never stop unrelated
   workloads. Validate DSpark batch/graph shapes before each candidate launch.
3. Reuse `profile-spark-serving.py` for bounded unique-prefix raw-backend screening.
   Its existing 262K input ceiling, 1,800-second request timeout and short synthetic
   workload do not cover this whole plan. Extend or add a harness for shared
   prefixes, multi-turn agents, reasoning, mixed arrivals, gateway traffic and
   partial-result retention; do not claim the current profiler implements them.
4. Run adaptive screening and qualify the best two candidates with a mixed soak
   of at least one hour, repeated tool/thinking checks and both coordinator roles.
   Confirm real OMP behavior through Context Guard. Exercise simultaneous traffic
   through both gateways against the same backend; preserve Fam-Chat settings.
5. Publish only a fully qualified profile. Update advertised limits and existing
   OMP overrides together, with a private configuration backup; test oversized
   input handling/compaction and unchanged model identity. Do not silently clip
   old long conversations into a smaller window.
6. Commit recipes, deterministic commands, tests and measured recommendations;
   keep raw receipts in ignored `data/cluster/` and mirror evidence across the
   approved wired path. Restore the baseline if no candidate passes.

The intended result is a measured **everyday agent profile**, with the accepted
**1M long-context profile preserved**. They are alternate launches of the same
two-GPU engine, not simultaneous services. Changing engine profiles requires a
controlled restart and matching gateway/client limits. Larger output caps,
remote drafting, node enrollment and recovery fault campaigns remain separately
scoped work.
