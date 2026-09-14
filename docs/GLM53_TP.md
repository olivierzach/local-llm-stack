# GLM-5.3-Flash across the two Sparks

**Preparation/qualification in progress. No live serving acceptance is claimed yet.**
The [focused goal and source review](GLM53_TP_GOAL.md) record the selected artifacts
and remaining gates. This adds a separate `local-glm53-flash` alias; existing
DeepSeek, Qwen and single-node recipes remain available.

## Candidate

| Setting | Starting point |
| --- | --- |
| Target | Pinned `canada-quant/glm-5.3-w4a16-mtp`, 191 GB snapshot |
| Draft | Pinned `incoai/GLM-5.3-Flash-DFlash2`, 2.34 GB snapshot |
| Precision | W4A16 experts, FP8 KV cache |
| Runtime | Pinned ARM64 vLLM image; reviewed hash-checked runtime patches |
| Parallelism | Native multiprocessing, TP2, one GPU on each Spark |
| Speculation | DFlash2, seven speculative tokens |
| Execution | Eager, async scheduling, Marlin MoE, prefix caching |
| Total context / output cap | 262,144 / 8,192 tokens |
| Scheduler / memory fraction | Four active requests / 0.82 |
| API / rendezvous port | 8125 / 29545 |
| Coordinator | Either `66f1` or `e8f1`; a placement choice, no permanent main |

The context limit is per request, with a shared cache pool. Four scheduler slots
do not promise four simultaneous full-window requests. Qualification records
single-request near-limit retrieval and a separate 1/2/4-request throughput
screen. Large concurrent prompts need their own measured capacity test.

The first 0.85 trial passed the primary placement but left only 4.06 GiB available
at its lowest point. The reversed placement started with less host headroom.
The current candidate uses 0.82 to reserve approximately another 3.65 GiB per
node; it retains the same request limits and must be requalified in both roles.

The full model snapshot exists on each machine for repeatable loading and rank
reversal. Each GPU loads its tensor-parallel shard. Speculative decoding adds a
small draft model; accepted draft tokens come from the target's verification.

## Reproduce from either node

Use the installed controller; these paths are the same on both Sparks:

```bash
cd ~/projects/local-llm-stack-cluster/current
make glm53-tp-prepare PEER=e8f1 OUTPUT=data/cluster/glm53-preparation
```

Use `PEER=66f1` when starting preparation on e8f1. Downloads are resumable, all
snapshot files are SHA-256 checked, and the second copy uses a verified direct
route and source-bound SSH connection. Runtime assets are built from the exact
image source with base/output hash checks. Preparation never starts a GPU worker.
It also checks the actual runtime's dense/shared BF16 and routed INT4 selection
against the pinned model configuration in a CPU-only, network-disabled container.

After checking inference is idle, explicitly stop the deployment occupying the
two GPUs using its saved plan. `glm53-tp-up` refuses a busy or reserved GPU.
It does not stop another service for you.

```bash
make glm53-tp-plan COORDINATOR=e8f1 OUTPUT=data/cluster/glm53-e8f1-01
python3 scripts/sparkctl preflight --saved-plan data/cluster/glm53-e8f1-01/plan.json
make glm53-tp-up COORDINATOR=e8f1 OUTPUT=data/cluster/glm53-e8f1-01
make glm53-tp-status PLAN=data/cluster/glm53-e8f1-01/plan.json
```

The startup receipt establishes text/SSE readiness, not full qualification.
Inspect owned container logs if startup fails; exact-plan cleanup is available
even when launch never reaches readiness.

## Qualification and publication

```bash
make glm53-tp-accept PLAN=data/cluster/glm53-e8f1-01/plan.json \
  OUTPUT=data/cluster/glm53-e8f1-01/qualification
```

This records both hosts' available memory, paging/pressure, runtime logs,
metrics and cable counters around synthetic text, reasoning on/off, image
color-order fixtures, tool call/result checks, 1K/4K output profiles, near-limit
varied input retrieval, prefix reuse, an 18-request soak and concurrency 1/2/4.
Tool probes execute synthetic functions only. Image fixtures establish basic
image processing, not OCR/document quality. Timing prose is not scored for accuracy.

Stop the exact plan and launch the identical recipe with the other coordinator.
Run `glm53-tp-accept ... ROLE_CHECK=1` there: text, reasoning, images, tools,
near-limit retrieval/prefix reuse/continuation, an 18-request soak and concurrency
checks are repeated. Both roles must pass the memory limits. The separate 1K/4K
prose/code/planning profile is required for at least one placement. Either
qualified role can remain live and be published; the full performance profile is
a reference measurement, not a requirement for a permanent coordinator.

Run `make glm53-tp-publish` on each Spark with `PLAN`, `ACCEPTANCE`,
`ALTERNATE_PLAN`, `ALTERNATE_ACCEPTANCE`, and a fresh `OUTPUT`. The two acceptance
arguments point to each campaign's `qualification/serving` directory. The helper
requires matching recipe/digests and both coordinator roles, upserts only the GLM
alias, and tests each node's existing and managed Context Guards. Other model
routes and client defaults are preserved. Follow with an actual OMP read-tool
probe and gateway image probe before declaring downstream integration complete.

For the existing Mac OMP provider, copy the saved plan and completed feature
receipt to the Mac, then add only this model's override:

```bash
.venv/bin/python scripts/configure-omp-glm53.py --saved-plan PATH/plan.json \
  --features PATH/qualification/serving/features.json \
  --output data/cluster/glm53-omp-install --apply
omp models refresh spark-context-guard
omp --model spark-context-guard/local-glm53-flash --thinking high
```

Use `--thinking off` for direct answers. The helper preserves provider endpoints,
credentials, other model overrides and the default model selection. The generated
`spark-client` profiles also expose the GLM alias's text/image/tool capabilities
and the same OMP thinking controls through either node's managed gateway. For
example, from either Spark (or the Mac):

```bash
.venv/bin/python scripts/spark-client run --node e8f1 --client omp \
  --output data/cluster/glm53-client-session -- \
  --model spark-e8f1/local-glm53-flash --thinking high
```

Use `--node 66f1` and the `spark-66f1/` provider prefix to enter through the other
gateway; this does not require moving the backend or creating another model alias.

The backend binds to the coordinator's `10.10.20.x` fabric IP. NCCL is restricted
to IB/RoCE and the two inventoried interfaces; Gloo/bootstrap uses the first
cable subnet. Confirm `NET/IB` in logs and increasing RDMA counters during tests.
These are two logical interfaces on one physical 200-GbE cable; do not add their
link-speed labels together. Wi-Fi is not a fallback for copies or GPU collectives.

## Stop and restore DeepSeek

```bash
make glm53-tp-down PLAN=data/cluster/glm53-e8f1-01/plan.json
make deepseek-tp-up COORDINATOR=e8f1 DEEPSEEK_TP_CONTEXT=1m \
  DEEPSEEK_TP_SPEC=dspark2 DEEPSEEK_TP_EXECUTION=graphs \
  DEEPSEEK_TP_EXPERTS=bf16 OUTPUT=data/cluster/deepseek-restored-after-glm
```

Existing DeepSeek routes remain pointed at e8f1 port 8123 when GLM alone is
published. They become available again once that accepted DeepSeek recipe is
restored. The two TP2 models do not run concurrently on these two GPUs. To restore
a different placement/recipe, update its route and client limits using the
existing DeepSeek publication workflow. GPU reservations belong to exact saved
plans, so always stop the currently owned deployment before switching.
