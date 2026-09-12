# Higher-precision DeepSeek V4 across two Sparks

This is a separate, staged two-node candidate. It has not passed GPU inference
acceptance. Qwen remains the active deployment until an explicit test switch.
The existing `deepseekv4-*` Make targets, native DS4 engine, GGUF weights, DSpark
drafter, and single-node service definitions are unchanged on both machines.

## Pinned artifacts and initial limits

| Setting | Value |
| --- | --- |
| Checkpoint | `deepseek-ai/DeepSeek-V4-Flash-0731` |
| Revision | `7872f01b1d1fe23eabc4c98b48bffcef5a386062` |
| Download | 166,898,661,074 bytes, 74 files including 48 weight shards |
| Precision | Official mixed FP4 expert / FP8 weights, versus the existing low-bit GGUF |
| Runtime | `eugr/spark-vllm-b12x@sha256:693a1d778e998ccf9d9268d70f5af0f1f397e4a8c0d2ce6e54bb75e22bd1b36b` |
| ARM64 image ID | `sha256:6d01fec064f7443a0d82360f918f6774212d4701f6bd08a2d338e10dafb696a2` |
| Packages | vLLM `0.1.dev20610+g4b276a363.d20260910`, Torch `2.13.0+cu130`, FlashInfer `0.7.0` |
| NCCL | Container-only `2.30.7` library pin, shared with the accepted Qwen setup |
| Parallelism | TP2, one GPU per node, native multiprocessing |
| Initial context / output | 65,536 / 8,192 tokens |
| Concurrency / GPU memory fraction | 1 / 0.8 |
| KV cache | FP8, block size 256 |
| Scheduling | Eager, synchronous; no initial graph-performance claim |
| Public model alias | `local-deepseek-v4-flash` |
| Direct backend | Coordinator fabric IP, port 8122; rendezvous 29542 |

Both plain decoding and optional DSpark2 have manifests for either coordinator:
`cluster/deployments/deepseek-tp2-{66f1,e8f1}.json` and
`cluster/deployments/deepseek-tp2-dspark2-{66f1,e8f1}.json`.
DSpark uses the draft module in the pinned checkpoint. It does not reuse the
single-node GGUF drafter, and does not add a remote-drafter service.

The initial default is non-thinking so protocol and retrieval checks can finish
within bounded output limits. Requests can opt into thinking with
`chat_template_kwargs: {"thinking": true, "reasoning_effort": "high"}`.
Think Max is excluded at this context size. Vision is not advertised.

The upstream [vLLM recipe](https://recipes.vllm.ai/deepseek-ai/DeepSeek-V4-Flash)
requires a Spark-specific build for GB10. The
[Spark runtime recipe](https://github.com/eugr/spark-vllm-docker/blob/main/recipes/deepseek-v4-flash-0731.yaml)
provides the B12X kernel switches and DeepSeek parsers. Our pinned variant starts
with smaller context/concurrency and eager execution; upstream performance
numbers do not establish acceptance of this local configuration.

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
for Qwen. It replaces the new image's bundled `2.29.7` only inside the new DeepSeek
containers, without `LD_PRELOAD` or any host library modification. The preparation
command verifies it on both nodes and installs the pinned package if absent.
This carries forward the known-good communication-library version; the new
DeepSeek/runtime combination still requires its own live collective checks.

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

Run from the controller's Python environment:

```bash
.venv/bin/python scripts/probe-spark-tool-calling.py --saved-plan data/cluster/deepseek-plain-test/plan.json --output data/cluster/deepseek-plain-test/tools.json
.venv/bin/python scripts/accept-spark-serving.py --profile deepseek-64k --saved-plan data/cluster/deepseek-plain-test/plan.json --output data/cluster/deepseek-plain-test/serving
```

This checks tool calls/results with and without streaming, 1K/4K decode,
approximately 63K-input retrieval/token accounting, repeated-prefix reuse and
sequential-request stability. Also test explicit thinking and a real OMP
read/edit/bash workflow. The pinned runtime's finish-reason behavior must be
validated; never weaken tool-name, argument or returned-result checks to pass it.

After plain decoding passes, stop its saved plan and launch the separate
`DEEPSEEK_TP_SPEC=dspark2` variant into a fresh output directory. Repeat acceptance,
check accepted speculative-token metrics, and compare decode latency. Increase
context/concurrency only after measuring available shared memory and stability.

Publish only the accepted deployment through `configure-context-routes.py
--plan ... --merge --alias local-deepseek-v4-flash` on each BASE stack root and
`spark-gateway routes --node ... --plan ... --merge --alias local-deepseek-v4-flash`
on each managed gateway. Preserve unrelated routes and validate both gateways
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

## Evidence and remaining work

Local staging evidence lives in `data/cluster/deepseek-tp-20260911/`; remote
preparation evidence lives in the controller's `state/deepseek-tp-20260911/`.
The accepted Qwen plan digest remains
`a83ebf27f1b5f0ecbc92288191c206660ac5b13b3815ed411dea90e1383e3d20`.
GPU startup, distributed kernel compatibility, tool/OMP acceptance, speculative
speedup, measured cable traffic, and reverse-coordinator inference remain to be
tested. No DeepSeek TP endpoint should be described as ready until those relevant
checks pass.
