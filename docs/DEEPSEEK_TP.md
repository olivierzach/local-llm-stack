# Higher-precision DeepSeek V4 across two Sparks

This is a separate two-node deployment; the existing single-node `deepseekv4-*`
recipes and weights remain available on either Spark.

The original FP8-activation expert path failed a repeated-answer regression.
Layer traces isolated the first visible divergence to layer 0's routed experts;
embeddings, attention, router scores, and the shared expert were unchanged.
With `VLLM_B12X_MOE_FP4_FORCE_A16=1`, all 20 diagnostic requests returned the
correct answer and all recorded layer outputs matched across eight repeated
requests on both ranks. This changes expert activations to BF16; the checkpoint
remains the original mixed FP4/FP8 weights. No FP8 weight conversion is needed.

**DeepSeek TP2 with BF16 expert activations, DSpark2, CUDA graphs and a
1,048,576-token total context is published through both Sparks' Context Guards.**
Both coordinator roles passed full acceptance on September 12, 2026. The live
coordinator is e8f1; 66f1 passed the same recipe. There is no permanent master.
Qwen is stopped while DeepSeek uses both GPUs. The single-node recipes and the
original 64K TP2 variants remain available.

The alias remains `local-deepseek-v4-flash`. Both gateways on both nodes passed
text and tool checks requiring the exact live deployment digest. The
[first-principles context analysis and measured sweep](DEEPSEEK_CONTEXT.md)
explain the memory budget, long-prefill cost, client deadlines and reproducible
qualification commands. The larger limit covers input plus output; it does not
qualify concurrent full-length requests or improve answer quality by itself.

| 1M acceptance | 66f1 coordinator | e8f1 coordinator |
| --- | --- | --- |
| Repeated-answer regression, before + after load | 200/200 correct | 200/200 correct |
| Tool call/result protocol checks | 36 passed | 36 passed |
| Thinking on/off, streaming/non-streaming | 4 passed | 4 passed |
| Three 1,024-token answers | 49.02–55.38 tokens/sec | 50.75–55.92 tokens/sec |
| Answers with a 4,096-token limit | 51.89–57.58 tokens/sec | 51.10–58.07 tokens/sec |
| Actual lengths of those three answers | 4,096 / 4,096 / 4,096 | 4,061 / 4,096 / 4,096 |
| Sequential soak | 18 requests, 18,432 output tokens | 18 requests, 18,432 output tokens |
| Median soak decode speed | 52.52 tokens/sec | 52.61 tokens/sec |
| Draft tokens accepted during soak | 78.99% | 79.10% |
| Long-input retrieval and token accounting | 1,046,414 tokens, passed | 1,046,422 tokens, passed |
| First text, fresh / reused long prefix | 1,240.23 / 3.16 seconds | 1,241.62 / 3.17 seconds |
| 1,024-token decode near the context limit | 35.07 tokens/sec | 35.79 tokens/sec |
| Minimum available host memory, 66f1 / e8f1 | 8.36 / 12.14 GiB | 11.82 / 12.31 GiB |
| Direct fabric | Both RoCE interfaces active; zero measured error/drop deltas | Same |

These are bounded synthetic checks at concurrency one, not a general accuracy
benchmark or an uptime guarantee. Full input retrieval, token accounting,
stream completion and host-memory acceptance are separate checks; the generated
analysis text is used for timing, not as a scored reasoning benchmark.

The final public-path check sent 1,040,337 input tokens through 66f1's existing
port-4010 Context Guard to the e8f1 coordinator over the direct fabric. Retrieval,
exact tokenizer accounting, unchanged input, deployment identity and stream
completion passed. Fresh first text took 1,226.35 seconds, the cached repeat
5.12 seconds, and a 1,024-token continuation decoded at 35.01 tokens/sec.
The client allowed only 180 seconds of socket silence, so this also exercised
the gateway's 15-second SSE keepalives across the full prefill.

Final integration checks passed for OMP read-tool execution through each node's
managed gateway, the existing Mac OMP provider's read/edit/bash fixture, FamChat's
configured provider, the existing Mac `llm` alias and the generated AIChat profile.
Existing Mac OMP and OpenClaw model catalogs both report 1,048,576 tokens after
their configuration updates; provider URLs, credentials and unrelated model
limits were preserved. OpenClaw was configuration/catalog validated; FamChat was
tested from its container, not its browser UI. Those short client checks do not
claim every application's full million-token workflow was exercised.

Keep using `spark-context-guard/local-deepseek-v4-flash` in OMP. Start a new session
or refresh/reselect the model if an already-open session still shows the old
context. The supported route-derived AIChat command and the legacy static
configuration's smaller limit are described in [the context runbook](DEEPSEEK_CONTEXT.md).

Receipts are in `data/cluster/deepseek-context-20260912/` on the Mac and in
`~/projects/local-llm-stack-cluster/state/deepseek-context-20260912/` on both Sparks.
The runs are `1m-66f1-02` and `1m-e8f1-01`; the latter's acceptance is under
`qualification/acceptance`.

The accepted 1M plan digests are:

- 66f1: `0973eb73655b4a72e227435328732ca2c92baa489d73cae7282fbefc15f41c17`
- e8f1: `57e2f8f870062a204f757f1f6373f20d185a10fe39f8c8bd219e53d82af0fe18`

Inspect the current deployment from either managed controller:

```bash
make deepseek-tp-status PLAN=data/cluster/deepseek-context-20260912/1m-e8f1-01/plan.json
```

## Initial 64K qualification

The original 65,536-token recipe also passed both coordinator roles. Its receipts
remain in `data/cluster/deepseek-readiness-20260912/`, runs
`a16-dspark2-graphs-{66f1,e8f1}-01`. These results establish the smaller rollback
recipe, not the currently published context limit.

| Acceptance | 66f1 coordinator | e8f1 coordinator |
| --- | --- | --- |
| Repeated-answer regression, before + after load | 200/200 correct | 200/200 correct |
| Tool call/result protocol checks | 36 passed | 36 passed |
| Thinking on/off, streaming/non-streaming | 4 passed | 4 passed |
| Three 1,024-token answers | 50.79–56.69 tokens/sec | 50.18–57.13 tokens/sec |
| Answers with a 4,096-token limit | 48.88–57.25 tokens/sec | 49.92–57.97 tokens/sec |
| Actual lengths of those three answers | 3,370 / 4,096 / 4,096 | 3,520 / 4,096 / 4,096 |
| Sequential soak | 18 requests, 18,432 output tokens | 18 requests, 18,432 output tokens |
| Median soak decode speed | 51.59 tokens/sec | 51.79 tokens/sec |
| Draft tokens accepted during soak | 78.16% | 79.23% |
| Long-input retrieval and token accounting | 63,418 tokens, passed | 63,421 tokens, passed |
| First text, fresh / reused long prefix | 48.60 / 0.54 seconds | 48.07 / 0.53 seconds |
| Direct fabric | Both RoCE interfaces active; zero measured error/drop deltas | Same |

These are bounded synthetic checks at concurrency one, not a general accuracy
benchmark or an uptime guarantee. The explanation responses ended naturally
before 4,096 tokens. Runtime, model, and recipe changes require new qualification.
Historical FP8 expert results below are not qualified serving configurations.

The accepted plan digests are:

- 66f1: `8630942aa671253ba645f4ca53be7f8c02dd87cb8bdf88532f96c0108928404e`
- e8f1: `673eeab14262e267148d39ccde867c1d5a21f5a14427e89e9665269cd300344e`

## Pinned artifacts and serving limits

| Setting | Value |
| --- | --- |
| Checkpoint | `deepseek-ai/DeepSeek-V4-Flash-0731` |
| Revision | `7872f01b1d1fe23eabc4c98b48bffcef5a386062` |
| Download | 166,898,661,074 bytes, 74 files including 48 weight shards |
| Precision | Official mixed FP4 expert / FP8 weights; BF16 expert activations |
| Runtime | `eugr/spark-vllm-b12x@sha256:693a1d778e998ccf9d9268d70f5af0f1f397e4a8c0d2ce6e54bb75e22bd1b36b` |
| ARM64 image ID | `sha256:6d01fec064f7443a0d82360f918f6774212d4701f6bd08a2d338e10dafb696a2` |
| Packages | vLLM `0.1.dev20610+g4b276a363.d20260910`, Torch `2.13.0+cu130`, FlashInfer `0.7.0` |
| NCCL | Container-only `2.30.7` library pin, shared with the accepted Qwen setup |
| Parallelism | TP2, one GPU per node, native multiprocessing |
| Published context / output | 1,048,576 / 8,192 tokens; original 65,536-token variant retained |
| Concurrency / GPU memory fraction | 1 / 0.8 |
| KV cache | FP8, block size 256 |
| Scheduling | Synchronous; explicit eager baseline or CUDA graph variant |
| Public model alias | `local-deepseek-v4-flash` |
| Direct backend | Coordinator fabric IP, port 8123; rendezvous 29543 (64K variant: 8122 / 29542) |

Both plain decoding and optional DSpark2 have manifests for either coordinator:
`cluster/deployments/deepseek-tp2-a16-{66f1,e8f1}.json` and
`cluster/deployments/deepseek-tp2-a16-dspark2-{66f1,e8f1}.json`.
Use `DEEPSEEK_TP_EXPERTS=bf16` (default). `fp8` selects the historical recipes
for diagnosis; that activation path failed repeatability and is not qualified.
DSpark uses the draft module in the pinned checkpoint. It does not reuse the
single-node GGUF drafter, and does not add a remote-drafter service.

Execution can also be selected explicitly with `DEEPSEEK_TP_EXECUTION=eager|graphs`
on the plan/up targets. The default remains eager. The separate `-graphs-`
deployment manifests use the upstream `FULL_AND_PIECEWISE` mode with all custom
ops, bounded to capture size 8 for the initial single-request configuration.
Acceptance is specific to the exact recipe and coordinator. Do not infer
acceptance of every variant from a passing check on one variant.
Existing model plans are unchanged.

Select the untraced BF16 expert recipe explicitly (the default execution
mode remains eager, with speculation off):

```bash
cd ~/projects/local-llm-stack-cluster/current
make deepseek-tp-up COORDINATOR=e8f1 DEEPSEEK_TP_CONTEXT=1m DEEPSEEK_TP_SPEC=dspark2 DEEPSEEK_TP_EXECUTION=graphs OUTPUT=data/cluster/deepseek-new-run
```

Run only after releasing the current deployment's GPUs. Either node can run the
command; set `COORDINATOR=66f1` to place the API and rank zero there instead.

The initial default is non-thinking so protocol and retrieval checks can finish
within bounded output limits. Requests can opt into thinking with
`chat_template_kwargs: {"thinking": true, "reasoning_effort": "high"}`.
Think Max is not qualified by this recipe. Vision is not advertised.

The upstream [vLLM recipe](https://recipes.vllm.ai/deepseek-ai/DeepSeek-V4-Flash)
requires a Spark-specific build for GB10. The
[Spark runtime recipe](https://github.com/eugr/spark-vllm-docker/blob/main/recipes/deepseek-v4-flash-0731.yaml)
provides the B12X kernel switches and DeepSeek parsers. The accepted variant uses the native 1M context, concurrency one and CUDA graphs;
upstream performance numbers do not establish acceptance of this local configuration.

## Prepare without interrupting inference

Use the managed controller checkout on either Spark. Do not run preparation
twice concurrently against the same model cache.

```bash
cd ~/projects/local-llm-stack-cluster/current
make deepseek-tp-prepare PEER=e8f1 OUTPUT="$HOME/projects/local-llm-stack-cluster/state/deepseek-preparation"
```

When run on e8f1, select `PEER=66f1`. Preparation downloads the pinned model once
with a CPU-only container limited to 2 CPUs and 2 GiB RAM, verifies all upstream
SHA-256 values, and copies the exact snapshot over the direct cable. Transfers
verify interface, source address and peer hostname, disable jump hosts and SSH
multiplexing, and verify destination checksums. Other models and credentials
are not copied or deleted. Cache provisioning changes ownership only for newly
created model-specific directories.

The runtime layers also cross the cable. Docker's classic image store loses
registry digest associations during `save/load`, so the helper subsequently
resolves the pinned registry manifest and verifies the resulting image ID.
Preparation neither reserves GPUs nor changes live routes. The final
`preparation.json` says `staged-awaiting-gpu-testing`; it is not serving acceptance.

NCCL uses the same checksum-verified, read-only `2.30.7` library already staged
for Qwen. It overrides the image's library only inside the new DeepSeek
containers, without `LD_PRELOAD` or any host library modification. The preparation
command verifies it on both nodes and installs the pinned package if absent.
This carries forward the known-good communication-library version; the new
DeepSeek/runtime combination still requires its own live collective checks.
An isolated process confirmed `ncclGetVersion() == 23007` with CUDA uninitialized.
The image's PyTorch/package metadata reports `2.29.7`; this differs from the
runtime library and must not be used as proof that the override was ignored.

## Render and start an explicit test

These commands run from either node. Coordinator selection applies only to this
deployment; it does not create a permanent master node.

```bash
make deepseek-tp-plan COORDINATOR=66f1 OUTPUT=data/cluster/deepseek-plan
```

Only after choosing to interrupt current inference, stop its exact saved plan.
The accepted Qwen recovery handle is:

```bash
scripts/sparkctl down --saved-plan "$HOME/projects/local-llm-stack-cluster/state/serving-20260911/qwen-tools-02/plan.json"
make deepseek-tp-up COORDINATOR=66f1 OUTPUT=data/cluster/deepseek-plain-test
```

`up` refuses a busy/reserved GPU and never silently stops another workload.
`OUTPUT` should be a fresh test directory. Use `COORDINATOR=e8f1` to reverse ranks.
Native NCCL/Gloo communication is pinned to the inventoried direct fabric and
RoCE devices. Confirm `NET/IB` initialization and fabric traffic in worker logs
during the live test; configuration alone is not a traffic measurement.

## Acceptance before publishing

Run the repeatable acceptance target from the managed controller checkout:

```bash
make deepseek-tp-accept PLAN=data/cluster/deepseek-plain-test/plan.json OUTPUT=data/cluster/deepseek-plain-test/acceptance
```

This checks repeated first-token answers, tool calls/results and explicit thinking on/off with and without streaming, 1K/4K decode,
near-limit input retrieval/token accounting (about 63K for the original recipe), repeated-prefix reuse and
sequential-request stability. Also test a real OMP
read/edit/bash workflow. The pinned runtime's finish-reason behavior must be
validated; never weaken tool-name, argument or returned-result checks to pass it.

The qualified serving recipe is the explicit BF16 + DSpark2 + graphs combination.
Plain decoding is available as a diagnostic control; it is not necessary to
restart through every variant before using an already qualified recipe.
Acceptance checks native accepted-speculative-token counters. Increase context
or concurrency only after separate memory and stability measurements.

For extended contexts, use `make deepseek-tp-context-accept PLAN=... OUTPUT=...`
to collect the host-memory samples required for publication as well as the full
serving suite. See [the context runbook](DEEPSEEK_CONTEXT.md).

Publish with `make deepseek-tp-publish` as shown below. It preserves unrelated
routes and validates both gateways
and the Mac OMP provider. No client needs a different endpoint name for the same
DeepSeek alias. Route publication is deliberately separate from preparation/up.

## Return to Qwen or single-node DeepSeek

Stop only the two-node deployment identified by its saved plan:

```bash
make deepseek-tp-down PLAN=data/cluster/deepseek-plain-test/plan.json
```

Restore Qwen with its existing manifest and a fresh receipt directory:

```bash
scripts/sparkctl up --deployment cluster/deployments/large-tp2-mtp2-tools-nccl2307-66f1.json --output data/cluster/qwen-restored --timeout 3600
```

Or use the existing `make deepseekv4-up` in the selected node's BASE stack after
releasing both distributed reservations. If the public DeepSeek alias was moved
to the two-node backend during testing, restore its single-node route as part of
the switch. Keeping the old recipe files does not by itself move a published route.
Refresh client context metadata for the newly selected plan too; the OMP and
OpenClaw update scripts in the context runbook also accept a lower context limit.

## Historical FP8 expert results (not qualified)

Native GPU acceptance is recorded in `state/deepseek-testing-20260912/` on the
controllers and `data/cluster/deepseek-testing-20260912/` on the Mac. The full
suite used coordinator 66f1, plan digest
`501a7ded09994cb731d3a7ec7f9b35acbdad0e0cc8386d98363c5a43edece252`.

| Test | Observed result |
| --- | --- |
| Eager, speculation off; three 256-token samples | 4.59–4.60 tokens/sec |
| Eager DSpark2; same prompts/limits | 9.48–11.34 tokens/sec |
| CUDA graphs + DSpark2; same prompts/limits | 45.09–51.43 tokens/sec |
| Three 1,024-token answers with graphs + DSpark2 | 49.93–55.25 tokens/sec |
| Three complete 4,096-token answers | 50.20–55.05 tokens/sec |
| Sequential completion test | 18 requests, 18,432 output tokens, median 49.37 tokens/sec |
| Speculation during stability | 79.15% of proposed draft tokens accepted |
| Tools | 36 automatic/named/required call/result checks, streaming and non-streaming |
| Long input | 63,417 input tokens; exact retrieval and tokenizer accounting passed |
| Prefix reuse | First text 45.94 seconds on new prefix, 0.54 seconds on repeat |
| Thinking toggle | Four on/off and streaming/non-streaming checks passed |
| Network | NCCL 2.30.7 `NET/IB`, both direct RoCE interfaces active; receive-error deltas zero |

These are synthetic single-request measurements, not a model-quality evaluation
or a concurrent-load guarantee. The first thinking fixture returned correct
arithmetic as an equation and failed its bare-integer format requirement; an
explicit system-format instruction passed the unchanged exact-answer criteria.
Both the failed `thinking.json` and passed `thinking-v2.json` are retained.
The runtime warned about unsupported `torch.compile`, but target and drafter CUDA
graph capture completed successfully; graph memory was approximately 0.78 GiB.

Coordinator reversal launched successfully on e8f1 and passed all 36 tool checks.
The thinking fixture then answered `289` for `17 * 19`, which should be `323`.
Ten identical one-token requests at temperature zero and seed zero produced two
wrong answers and large changes in reported token scores. Removing speculation
and graphs produced three wrong answers in ten trials; prefix-cache hit rate was
zero. This is a serving-correctness concern, not proof of a particular kernel bug.
The receipts are `reverse-e8f1-01/` and `control-plain-e8f1-01/`.
The committed 20-request regression reproduced five wrong answers in the eager,
non-speculative control. The `serial-experts` control, using
`deepseek_v4.disable_shared_experts_stream=true`, also returned five wrong
answers out of twenty. Disabling shared-expert overlap did not resolve it.
Neither a successful health check nor the earlier long-context/performance suite
is sufficient to approve this runtime.

An explicit diagnostic recipe, `deepseek-v4-0731-flashinfer-control`, selects
`FLASHINFER_MLA_SPARSE_DSV4` attention while preserving B12X linear/MoE kernels,
the checkpoint, eager execution, and NCCL pin. It has manifests for either
coordinator and is not a published serving configuration. Mixed-backend DSpark
is rejected until separately supported and tested. The original recipes and
their saved-plan digests remain unchanged.
This control failed startup in `deep_gemm_fp8_o_proj` with a DeepGEMM
`t.dim() == N` assertion; it never reached the repeated-answer test.
`control-flashinfer-e8f1-01/coordinator-failure.log` preserves the traceback.

Subsequent eager controls with Torch collectives and CUDA launch blocking also
failed the regression. Bounded layer traces then located the first observed
divergence in the routed expert output. Selecting BF16 expert activations removed
that divergence in the traced control. This identifies a working alternative
kernel path; it does not establish a specific hardware or NCCL defect. The
current BF16 qualification and publication status is recorded at the top of
this document. The initial artifact-staging record follows.

Local staging evidence lives in `data/cluster/deepseek-tp-20260911/`; remote
preparation evidence lives in the controller's `state/deepseek-tp-20260911/`.
Artifact staging completed on September 12, 2026: all 74 files (48 weight shards,
166,898,661,074 bytes) passed source and destination checksum verification.
`prepared/model-copy.json` records the direct transfer from `10.10.20.1` to
`10.10.20.2` through `enp1s0f0np0`. Its `rsync_s` includes checksum work and SSH
transfer and is not a raw cable-bandwidth benchmark. The original download and
prepared copy manifests are identical.

The staging-time final preflight passed model-cache, runtime-image, NCCL-library, fabric,
RDMA-device and service-port checks on both nodes. Its only failed checks are
reservation, GPU-idle and shared-memory because Qwen occupied both GPUs then.
`final-preflight/preflight.json`, `prepared/preparation.json` and
`staging-summary.json` distinguish this completed staging from GPU acceptance.
The four staging-time candidate plans are under `final-plans/`; older top-level
staging plans predate the final NCCL pin and should not be used to launch tests.

The accepted Qwen plan digest remains
`a83ebf27f1b5f0ecbc92288191c206660ac5b13b3815ed411dea90e1383e3d20`.
Artifact staging alone does not establish serving readiness; use the later GPU
and client acceptance receipts for that conclusion. The later 1M qualification
is recorded above; concurrent load and the plain graph variant remain separate
qualification tasks.

## Publish only an accepted BF16 recipe

`make deepseek-tp-accept PLAN=... OUTPUT=...` now requires 100 repeated first-token
answers, 36 tool protocol checks, all four thinking/streaming checks, near-limit input,
a sequential soak, long answers with a 4,096-token limit, and another 100 regression
requests after that workload. It stops on a failure and
retains its receipts. Run it against the exact untraced recipe with each
coordinator, keeping the receipts in separate directories.
For a context above 64K, run it through `make deepseek-tp-context-accept` so the
publication gate also has memory and paging samples covering the serving suite.

After both roles pass and the desired deployment is running, copy both saved
plans and acceptance directories to each gateway host. Run this from the managed
controller on **each Spark**, with paths to those receipts:

```bash
make deepseek-tp-publish \
  PLAN=/path/to/running/plan.json ACCEPTANCE=/path/to/running/acceptance \
  ALTERNATE_PLAN=/path/to/other-coordinator/plan.json \
  ALTERNATE_ACCEPTANCE=/path/to/other-coordinator/acceptance \
  OUTPUT=/path/to/new/publication-receipt
```

The command verifies the same recipe passed in both coordinator roles and that
the exact selected deployment is healthy. It upserts only the existing
`local-deepseek-v4-flash` route in that host's BASE and managed Context Guards,
checks tool calls through both gateways, and restores the previous registries
if those checks fail. Clients keep their existing alias and Context Guard URL.
No permanent master node is created.

Verify the existing Mac OMP provider after publication:

When the context limit changes, first apply the existing-client metadata updates
from [the context runbook](DEEPSEEK_CONTEXT.md), then refresh and test the catalog.

```bash
omp models refresh spark-context-guard
.venv/bin/python scripts/probe-spark-omp.py --provider spark-context-guard --model local-deepseek-v4-flash --output data/cluster/client-check/omp-read.json
python3 scripts/probe-omp-coding.py --output data/cluster/client-check/omp-coding.json
```

The coding probe allows only OMP's read/edit/bash tools for a disposable local
fixture, verifies the resulting program and final tool result, and preserves
the event log. It is a noninteractive integration check, not a coding benchmark.
An ordinary interactive session keeps the same model selection:
`omp --model spark-context-guard/local-deepseek-v4-flash`.

From each Spark, `scripts/probe-spark-omp.py --node 66f1|e8f1 --model
local-deepseek-v4-flash --output ...` checks its managed gateway using a temporary
client profile. On the FamChat host, `scripts/probe-famchat-provider.py --output
...` tests the provider URL and credential configured in the existing Open WebUI
container; it does not create a stored conversation or test the browser UI.

Use `make deepseek-tp-status PLAN=/path/to/saved/plan.json` to inspect both workers.
TP2 needs both nodes: this does not provide automatic failover after a worker or
host failure. Workers deliberately have Docker restart disabled so a lone rank
cannot reclaim a GPU independently of the cluster reservation. Recover by stopping
the exact saved plan and starting the qualified recipe into a fresh receipt
directory, then publish the healthy deployment. Never delete reservations to
bypass another workload.

For a read-only preview, run `scripts/publish-deepseek-tp.py` with the same path
arguments and omit `--apply`. A traced diagnostic cannot be published by this
command. The trace overlay can be reproduced for diagnosis using
`scripts/prepare-deepseek-overlay.py --manifest runtime/deepseek-overlays/trace-layers.json`;
its base source and result are checksum-verified before any GPU launch.
