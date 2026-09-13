# Preparing a model that needs both Sparks

This document preserves the September 10 capacity experiments. The subsequent
[Qwen TP/MTP acceptance](QWEN_TP_MTP_ACCEPTANCE.md) passed the qualified recipe
with either coordinator. Qwen is currently stopped while the accepted
[DeepSeek TP2 deployment](DEEPSEEK_TP.md) serves. Earlier stalled variants and
restoration notes below are historical; use [the current checklist](CLUSTER_IMPLEMENTATION.md)
for live state and remaining work.

The candidate `Qwen/Qwen3-Next-80B-A3B-Instruct` BF16 model has 162,659,161,528
bytes of weight shards (151.49 GiB), exceeding either Spark's physical memory.
On September 10, 2026, the pinned runtime passed actual two-node TP=2 loading,
generation and streaming, including a temporary Context Guard, with 66f1 as
coordinator. The 16K eager recipe is the first measured baseline. The later
single-request 256K eager/synchronous recipe also passed long-answer generation
and near-full-context retrieval with e8f1 coordinating. Compiled and 80B
pipeline-parallel variants still require acceptance; recipes add no gateway route
automatically.

The 128K eager capacity run subsequently completed single-request inputs near
127K at about 26.6 decode tokens/s. Its two-request near-127K warmup hit a worker
RPC timeout and EngineDeadError; owned workers were cleaned up and DeepSeek was
restored. This is a failed concurrency case, not validated 128K multi-user
capacity. The planned compiled sweep did not start after that failure.

The initial compiled 256K candidate loaded and replied `ready`, then stopped
producing tokens during the 256-token benchmark warmup. That is not a successful
long-answer or full-context acceptance. Its saved plan and logs remain under
`data/cluster/serving-20260910/decode-256k/` at release `293ba32`.

The revised candidates prioritize single-request long-answer speed:
`large-tp2-256k-plain-{66f1,e8f1}` and
`large-tp2-256k-mtp2-{66f1,e8f1}`. Both use the native 262144-token context,
one scheduled sequence, BF16 weights and the same pinned engine, with eager
execution and `async_scheduling: false`. These conservative settings remove
compilation and scheduling overlap as variables; they are not a proven diagnosis
of the preceding stall. The plain variant passed the bounded checks below;
MTP failed the multi-prompt acceptance run described below and remains experimental.
Neither replaces an existing route.
The MTP variant adds `speculative_config: {"method":"mtp","num_speculative_tokens":2}`;
omitting that field disables speculation. It changes deployment ownership and
requires a controlled restart. Existing recipes and endpoint aliases are unchanged.
The cached checkpoint contains 1553 `mtp.*` tensors in shard 41, and the pinned
vLLM build includes `Qwen3NextMTP`; no separate draft-model download is needed.
MTP plus pipeline parallelism is rejected until independently supported and tested.
This option does not claim support for an arbitrary remote drafter endpoint.

For a controlled off/on comparison after starting either candidate, run:

```bash
.venv/bin/python scripts/profile-spark-decode.py \
  --saved-plan /path/to/plan.json --max-tokens 1024 \
  --output /path/to/new-decode-results.json
.venv/bin/python scripts/probe-spark-long-context.py \
  --saved-plan /path/to/plan.json --input-tokens 260032 \
  --output /path/to/new-long-context-results.json
```

The decode profile compares three fixed long-answer tasks at concurrency one,
records actual output lengths and speculative acceptance counters, and rejects
streaming errors. The retrieval probe checks three synthetic codes spread through
a near-full input, then repeats it to verify prefix-cache reuse. Neither is a
comprehensive quality benchmark. The current Instruct model has no separate
thinking stream; these measurements describe answer generation, not a validated
thinking-model recipe. Upstream reference:
[vLLM Qwen3-Next MTP recipe](https://github.com/vllm-project/recipes/blob/main/Qwen/Qwen3-Next.md).

### Single-request baseline measured on September 10

Release `7245abc`, e8f1 coordinator, TP=2, eager execution, synchronous scheduling,
80% memory budget, 262144 context limit and one scheduled sequence:

| Task | Output tokens | Decode tok/s | Total seconds |
|---|---:|---:|---:|
| Explanation | 1024 | 27.22 | 37.83 |
| Python code | 1024 | 28.58 | 36.02 |
| Deployment plan | 1024 | 27.01 | 38.08 |

All completed normally at the requested output cap. This is a three-case
performance sample, not a code-correctness or reasoning-quality benchmark.
The separate retrieval probe processed **260026 input tokens** and recovered
all three synthetic codes. First-token latency was **126.43 seconds** on the
unique prompt and **1.26 seconds** when repeated; the repeat recorded **259488
prefix-cache hits**. Both answers were correct and ended normally. The 44-token
retrieval answers are too short to substitute for the sustained-answer benchmark.
Receipts: `data/cluster/serving-20260910/decode-256k-eager-sync/plain/`.
This does not validate multiple simultaneous full-context requests or long-term
serving reliability.
The matched per-node telemetry spans about 116.29 GB transmitted and received
per node over the two direct RoCE rails, with matching peer byte counts and
zero receive errors/transmit discards. This interval covers all measured plain
requests, including long-context prefill; it is path evidence, not peak bandwidth.

### Native MTP comparison: promising speed, failed acceptance

The matched MTP=2 recipe completed the explanation task with 1024 output tokens
at **50.16 decode tokens/s**, versus **27.22** without speculation (1.84×).
Total time fell from 37.83s to 20.65s. Counters recorded 595 accepted draft tokens
out of 860 proposed across 430 drafting steps (69.2% draft acceptance).

The following code-generation request stopped making progress and failed with
`RPC call to sample_tokens timed out`, then `EngineDeadError`. Both workers
reported high GPU utilization while sampled RDMA byte counters stopped changing;
no RDMA receive errors or transmit discards were recorded in those samples.
This does not establish the underlying cause. A successful health endpoint did
not imply that generation was progressing. The three-task comparison and MTP
near-full-context retrieval acceptance therefore did not complete. Do not present
this single completed answer as a reliable MTP serving configuration or claim
that the plain recipe's 260K retrieval result validates MTP at that length.

The eager/synchronous plain variant is the best-supported configuration from
this bounded experiment. Native speculation remains a reproducible, opt-in
candidate needing a runtime fix and successful repeated-request acceptance.
Receipts: `data/cluster/serving-20260910/decode-256k-eager-sync/mtp2/`.
The controller removed both test workers, released their reservations, and
restored native DeepSeek without cleanup errors. Both existing Context Guards
then passed real text, streaming, synthetic tool protocol and authentication
checks. The experimental Qwen endpoint is no longer running.

The [upstream model](https://huggingface.co/Qwen/Qwen3-Next-80B-A3B-Instruct/blob/9c7f2fbe84465e40164a94cc16cd30b6999b0cc7/README.md)
is public, Apache-2.0 licensed, and uses a non-thinking Instruct template.
The repository's download manifest pins revision
`9c7f2fbe84465e40164a94cc16cd30b6999b0cc7`, all 51 file sizes and SHA-256s,
the runtime image digest and `huggingface_hub` version. Total download size is
162,682,272,937 bytes. Allow that space plus at least 10 GiB on each node;
existing models and runtime caches consume additional space.

## Download once on a Spark

Use an installed, immutable controller release. On e8f1, run the following once.
This CPU-only Docker job survives SSH disconnection, uses two download workers,
has a six-hour watchdog and preserves partially downloaded cache files. It uses
no Hugging Face credential. Docker must already contain the pinned runtime image
or be able to pull it; the explicit download-byte budget covers model files only.

```bash
cd "$(readlink -f ~/projects/local-llm-stack-cluster/current)"
model_source="$PWD"
model_cache="$HOME/projects/local-llm-stack/data/huggingface"
model_manifest="$model_source/cluster/model-downloads/qwen3-next-80b-instruct.json"
model_image=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["image"])' "$model_manifest")
mkdir -p "$model_cache"
docker run -d --name spark-model-fetch-9c7f2fbe8446 \
  --runtime runc --restart no --read-only \
  --user "$(id -u):$(id -g)" --cpus 4 --memory 8g --memory-swap 8g \
  --pids-limit 256 --tmpfs /tmp:rw,size=1g,mode=1777 --workdir /tmp \
  --label io.spark.model-download=9c7f2fbe84465e40164a94cc16cd30b6999b0cc7 \
  --mount "type=bind,src=$model_cache,dst=/cache" \
  --mount "type=bind,src=$model_source/scripts/fetch-pinned-spark-model.py,dst=/fetch.py,readonly" \
  --mount "type=bind,src=$model_manifest,dst=/manifest.json,readonly" \
  -e HF_HOME=/cache -e HF_HUB_DISABLE_IMPLICIT_TOKEN=1 \
  -e HF_HUB_DISABLE_PROGRESS_BARS=1 -e HF_XET_NUM_CONCURRENT_RANGE_GETS=4 \
  --entrypoint timeout "$model_image" --kill-after=30s 6h \
  python3 -u /fetch.py fetch --manifest /manifest.json --cache /cache \
  --lock-output /cache/locks/qwen3-next-80b-9c7f2fbe8446.json \
  --max-download-bytes 162682272937 --workers 2
```

Keep that release while the job exists: Docker mounts its exact script and
manifest. Record the returned container ID. A second launch with the same name
fails instead of creating a second writer. Inspect that job directly:

```bash
docker inspect --format '{{.Id}} {{.State.Status}} {{.State.ExitCode}}' spark-model-fetch-9c7f2fbe8446
docker logs --tail 10 spark-model-fetch-9c7f2fbe8446
```

Exit code 0 plus the final `verified: true` log receipt proves every downloaded
file matched its upstream size and SHA-256. A running container's displayed exit
code is not a success receipt. The copy lock is written only after verification.
Code 124 means the watchdog expired. Other failures retain logs and cached files.
After inspecting a **terminal failed** job and correcting its cause, resume its
same command/mounts with `docker start spark-model-fetch-9c7f2fbe8446`. Never
restart solely because an observation command timed out. Do not change mounted
source files during a download. To change the manifest or script, use a separate
reviewed job after the previous writer has stopped.

## Copy the verified cache over the fabric

The destination must permit this user to create the selected model's files.
The copy preflight checks that access before reading all source weights. It
preserves ownership, permissions and timestamps of shared cache parents rather
than copying those attributes from the source node. Model identity is determined
by file SHA-256s and snapshot links, not Unix metadata.

On 66f1 the legacy `data/huggingface/hub` directory is root-owned. An administrator
can provision just the new model directory once, without changing existing model
files or shared-directory permissions. Run on 66f1 as `statsparrot`:

```bash
sudo install -d -o statsparrot -g statsparrot \
  ~/projects/local-llm-stack/data/huggingface/hub/models--Qwen--Qwen3-Next-80B-A3B-Instruct
```

Run on the download source, after successful verification:

```bash
cd "$(readlink -f ~/projects/local-llm-stack-cluster/current)"
python3 scripts/sync-spark-models.py sync \
  --cache ~/projects/local-llm-stack/data/huggingface \
  --lock ~/projects/local-llm-stack/data/huggingface/locks/qwen3-next-80b-9c7f2fbe8446.json \
  --peer spark-66f1-wired \
  --peer-cache /home/statsparrot/projects/local-llm-stack/data/huggingface
```

The source and destination hashes are checked, snapshot symlinks are preserved,
and unrelated cache entries remain intact. Phase events go to stderr; the final
JSON receipt remains on stdout. It reports source verification, rsync, destination
verification and total durations, alongside the original bytes and elapsed
copy-plus-destination-verification fields. The rsync phase includes its own file
checksum preparation before network traffic starts. This includes SSH encryption and disk
I/O, so it is not a measurement of RDMA or NCCL bandwidth. e8f1 was selected as
download source because its measured RDMA sender direction is faster; actual
SSH model-copy throughput must still be measured independently.

The sync command is a foreground operation. For an unattended copy after source
download verification, use this bounded user service on e8f1. It pins the current
release's absolute path and holds a local lock for the entire verification/copy:

```bash
copy_release="$(readlink -f ~/projects/local-llm-stack-cluster/current)"
copy_state="$HOME/projects/local-llm-stack-cluster/state"
copy_cache="$HOME/projects/local-llm-stack/data/huggingface"
copy_unit="spark-large-model-copy-$(date +%Y%m%d%H%M%S)"
systemd-run --user --unit="$copy_unit" \
  --property=RuntimeMaxSec=4h --property=MemoryMax=2G \
  --property=CPUQuota=400% --property=Nice=10 \
  --property=KillMode=control-group --property=TimeoutStopSec=30s \
  /usr/bin/flock --nonblock "$copy_state/large-model-copy.lock" \
  /usr/bin/python3 -u "$copy_release/scripts/sync-spark-models.py" sync \
  --cache "$copy_cache" \
  --lock "$copy_cache/locks/qwen3-next-80b-9c7f2fbe8446.json" \
  --peer spark-66f1-wired \
  --peer-cache /home/statsparrot/projects/local-llm-stack/data/huggingface
systemctl --user show "$copy_unit" -p LoadState -p ActiveState -p Result -p ExecMainStatus
journalctl --user -u "$copy_unit" --no-pager -o cat
```

An active service is not a success receipt: wait for the final verified JSON and
a successful terminal exit. Successful transient units may be garbage-collected
and report `LoadState=not-found`; their default property values are not retained
exit-status evidence. Keep the final verified JSON and journal from the exact
unit/invocation, and inspect any failure messages before retrying.
The memory limit includes reclaimable file cache;
hashing a model larger than that limit does not load its weights onto a GPU.
The four-hour limit and process-group cleanup apply to this source service; the
peer commands run through SSH and do not inherit its CPU/memory limits. The
copy can compete for destination disk/CPU resources with another workload.
Record these limits when comparing timings. This service does not load a model
or pause a research job.

Inspect the exact service and journal before retrying. The lock excludes another
copy launched through this wrapper on the same source; it does not coordinate
copies launched elsewhere or directly without the lock. Do not run simultaneous
copies into the same snapshot. A failed copy can be rerun: destination hashes
must pass before success is reported.

A user service started before Docker group membership changed may report socket
permission denied even when Docker works over SSH. Launch the Docker-dependent
command through `sg docker -c 'COMMAND'` in that service to use the authorized
group membership. Confirm membership with `id` first. Avoid restarting the whole
user manager while other supervisors are running.

For a copy scheduled before the download finishes, gate the copy on
`docker wait FULL_DOWNLOAD_CONTAINER_ID` returning the text `0`. Use the full ID
recorded at launch, an eight-hour user-service `RuntimeMaxSec`, and the exact
release's sync script. A timeout or failed download must leave the copy unstarted.
The copy lock alone is insufficient evidence that the current download succeeded.

## Select the coordinator and parallelism

### What tensor parallelism splits

Both nodes retain a complete checkpoint on their local SSD. The checkpoint's
41 safetensors shards are storage files, not assignments to individual GPUs.
At startup, vLLM assigns each worker a tensor-parallel rank and loads the
appropriate portions of the tensors into that worker's GPU memory. Full local
copies also let either node become coordinator without moving the checkpoint.
Other runtimes can use shared storage or pre-sharded checkpoints; neither is
required by this recipe.

The pinned vLLM implementation uses parallel layers, including
`QKVParallelLinear` and `RowParallelLinear` in `Qwen3NextForCausalLM`. Its weight
loaders select tensor slices using the rank and shard size (`Tensor.narrow`),
rather than masking a complete GPU-resident weight tensor. Column-partitioned
layers compute output slices; row-partitioned layers compute partial sums that
are combined by an all-reduce. Some tensors are replicated, and attention,
linear-attention and expert layers have architecture-specific partitioning.
Do not expect exactly half of total process memory on each node.

Both GPUs cooperate on the same requests and tokens. TP=2 does not mean running
two independent replicas, nor putting the first half of the layers on one node
and the second half on the other (pipeline parallelism). The coordinator hosts
the API for this deployment; it is not a permanent master node.

Bootstrap sockets and Gloo bind to the first direct fabric interface. NCCL is
restricted to the two configured RoCE devices. Verify actual startup logs for
`NET/IB` send/receive channels and the expected TP ranks, rather than assuming
environment variables prove transport selection. RDMA port counters supplement
the logs; ordinary `ip -s link` counters may omit hardware-offloaded RDMA
traffic. These two logical interfaces share the physical cable's capacity.

| Deployment | Coordinator | Tensor parallel | Pipeline parallel |
| --- | --- | ---: | ---: |
| `large-tp2-66f1.json` | 66f1 | 2 | 1 |
| `large-tp2-e8f1.json` | e8f1 | 2 | 1 |
| `large-pp2-66f1.json` | 66f1 | 1 | 2 |
| `large-pp2-e8f1.json` | e8f1 | 1 | 2 |

All four use the same model contract: `local-large`, 16,384 context tokens,
4,096 maximum output tokens and text/streaming capabilities. Tools and vision
are not advertised. The hybrid model uses prefix caching with the pinned
runtime's `mamba_cache_mode: align`; compiled execution remains disabled for
initial acceptance. TP/PP consume both GPUs, and only one such deployment can
reserve the nodes at a time.

```bash
scripts/sparkctl validate --deployment cluster/deployments/large-tp2-66f1.json
scripts/sparkctl render --deployment cluster/deployments/large-tp2-66f1.json
# Only after both cache checks pass and both GPUs are available:
scripts/sparkctl up --deployment cluster/deployments/large-tp2-66f1.json --timeout 3600
scripts/sparkctl status --deployment cluster/deployments/large-tp2-66f1.json
scripts/sparkctl down --deployment cluster/deployments/large-tp2-66f1.json
```

Admission rejects incomplete numbered shards and index references before taking
a GPU lease. It also refuses busy GPUs or insufficient available shared memory.
This structural cache check is not a substitute for the upstream SHA-256 check.
Successful rendering does not prove hardware support; `up` must complete real
inference before readiness is published. Retain its saved plan for recovery.
After direct generation and streaming acceptance, explicitly add the deployment
to the chosen gateways using the [normal gateway workflow](CLUSTER.md). Clients
then use the same gateway contract regardless of coordinator placement.

Initial offline acceptance loaded `Qwen3NextConfig` and the tokenizer inside the
pinned image with networking and GPU access absent. Attention heads 16/2 and
linear heads 16/32 divide by two; 48 layers divide into two 24-layer stages.
The non-thinking chat template rendered correctly. These metadata checks do
not establish distributed kernel correctness or sufficient runtime memory.

The full pinned cache is now present on both nodes. On September 8, the peer copy
verified all 51 files and 51 snapshot links (162,682,272,937 bytes) at 66f1 after
re-verifying its e8f1 source. Both caches pass the controller's structural check
with 41 weight shards. The complete operation took 1,001.102 seconds; a separate
30-second interface-counter sample during active SSH transfer measured 2.91 Gb/s.
This is file-transfer evidence, not RDMA throughput or 80B inference acceptance.
The research job on 66f1 was preserved during the copy. Distributed GPU loading
subsequently passed in the September 10 idle window. See [the implementation record](CLUSTER_IMPLEMENTATION.md)
for phase timings and the retained acceptance receipt.

## First TP=2 hardware acceptance

Evidence is retained under `data/cluster/serving-20260910/tp2-attempt-03/`.
The saved plan digest is
`a7d041507b6f97ead99161a3bb6c6c58e758b0ed2382c7db3fdfeed455791a99`.
Workers reported TP ranks 0 and 1 and NCCL `NET/IB` over both configured RoCE
devices, with bootstrap addresses `10.10.20.1` and `10.10.20.2`. The full
checkpoint took about eleven minutes to load; use the explicit startup timeout
above rather than the CLI's ten-minute default.
Recorded RDMA counter deltas were about 3.79 GB transmitted and received per
node, split across the two logical rails, with matching peer counts and no
recorded receive errors or transmit discards. This interval includes startup and
acceptance, so it is traffic-path evidence rather than a link bandwidth result.

The coordinator reported 74.3 GiB for model loading. Cache profiling reported
25.24/21.99 GiB available across the two ranks and a resulting 1,563,564-token
cache capacity. This is a runtime estimate, not validated serving concurrency:
the recipe admits at most four active sequences and 16,384 tokens per request.
The first real completion returned `ready`; kernel compilation made that cold
request take 35.7 seconds. Subsequent text and SSE requests through an isolated
Context Guard passed, and an invalid gateway key returned 401. Existing client
routes were preserved. Owned workers were removed and native DeepSeek restored
successfully before the separately authorized profiling run.

The initial warmed synthetic sample used roughly 500 input tokens and 128 output
tokens, with two requests per concurrency level:

| Concurrent requests | Median first token | Median per-request decode | Aggregate output |
| ---: | ---: | ---: | ---: |
| 1 | 0.392 s | 30.01 tokens/s | 27.60 tokens/s |
| 2 | 0.525 s | 26.57 tokens/s | 47.92 tokens/s |

These small samples are acceptance evidence, not a throughput SLA or quality
benchmark. Use the dedicated profiling command below for matched context and
concurrency comparisons.

## Reproduce a context and concurrency profile

The additive `large-tp2-128k-{eager,compiled}-{66f1,e8f1}.json` manifests keep the
same model, weights and 80% GPU memory budget while raising context to 131,072
tokens and the scheduler limit to 16 active sequences. They are profiling
candidates until the corresponding hardware receipts pass. The compiled variant
removes `--enforce-eager`; the pinned engine selects its compilation and CUDA
graph defaults. Context length includes both input and generated tokens.

Start one deployment with `sparkctl up --timeout 3600`, retain its exact saved
plan, and run without competing inference requests:

```bash
.venv/bin/python scripts/profile-spark-serving.py \
  --saved-plan data/cluster/OWNER/plan.json \
  --prompt-tokens 1024 8192 \
  --concurrency 1 2 4 --requests 4 --max-tokens 128 \
  --output data/cluster/OWNER/profile-short.json
```

The command sizes unique prompts using the running model's chat tokenizer,
warms each prompt length and concurrency before timing it, and verifies complete
text streams with actual usage matching tokenization. It records time to first
token, decode rate, aggregate output, cold-shape warmup cost, timestamps and the
saved deployment contract. It refuses to overwrite evidence and marks partial
or failed runs incomplete. It never starts, stops or changes a model. GPU
admission and lifecycle remain the controller's responsibility.

On a 128K deployment, test longer inputs with `--prompt-tokens 32768 65536 126976`
and a smaller `--concurrency 1 2` first. Client concurrency can exceed
`max_num_seqs` to measure queueing; it does not change the engine's active-request
limit. Keep output length, input lengths and concurrency matched when comparing
eager and compiled execution. Synthetic repeated text tests capacity and latency,
not whether answers use information reliably from a long document.

For hardware telemetry, run this read-only sampler locally on each participating
Spark using its exact worker ID from `sparkctl status`:

```bash
python3 scripts/sample-spark-runtime.py \
  --container FULL_WORKER_CONTAINER_ID --interval 5 --duration 7200 \
  --output data/cluster/OWNER/runtime-NODE.jsonl
```

It stops when that container stops or the duration expires. Samples include
GPU utilization, GPU-reported watts and temperature, available shared memory,
swap headroom, RDMA byte/error counters and ordinary network counters. Match
sample timestamps to profile levels before computing averages and deltas;
exclude weight loading and warmup when describing warmed serving. GPU-reported
power is not whole-system wall power. Counter totals span all traffic on those
interfaces, so competing workloads invalidate attribution to this model alone.
