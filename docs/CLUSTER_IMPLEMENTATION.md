# Interchangeable Spark implementation record

This is the implementation checklist, not a claim that every item already works.
Existing Compose services, aliases, `.env`, and Make targets remain the baseline.

## Design

Versioned inventory describes nodes, SSH transports, model caches and fabric
interfaces. Immutable recipes describe models or batch workloads. Deployment
files choose nodes, coordinator and ports. A deterministic Python/SSH controller
renders isolated Compose projects and manages ownership on each node. An agent
is not part of the operational dependency chain.

Compute nodes have no permanent main. A distributed deployment has a selected
coordinator, and moving that role requires restarting that deployment. Gateways
and Context Guard are placed independently. Stable client aliases must retain
the same model capabilities and token budgets when placements change.

GPU admission is exclusive by default. Existing GPU processes and containers
block admission; no existing inference or research job is stopped implicitly.
Reservations survive controller failure and require owned cleanup/recovery.
Other tools do not automatically participate in this reservation protocol.

## Acceptance checklist

- [x] Strict manifests, deterministic rendering, backwards compatibility tests
- [x] Node inspection and owned lifecycle, partial-failure cleanup and recovery unit tests
- [x] Pinned standalone model on either node; completion on both and streaming on e8f1
- [x] Gateway/guard placement and atomic route plus policy changes; HTTP tests and deployed authentication
- [ ] OpenClaw, OMP, AIChat, llm and OpenRouter configuration adapters
- [ ] Vector Bucket placement and actual embedding smoke on either node
- [ ] Looped LLM immutable-source placement and managed job smoke on either node
- [ ] Both fabric rails, repeatable model-copy checksums and throughput
- [x] GPU NCCL correctness/bandwidth; runtime/driver compatibility evidence
- [ ] True TP=2 completion on both GPUs; each coordinator placement
- [ ] Explicit supported PP/replica modes, capacity/performance comparisons
- [ ] Failure/recovery tests, operator runbook, clean reviewable changes

## Initial constraints (2026-09-07)

The original Spark (`66f1`) is running a looped-LLM research job. Its local source
tree contains uncommitted work. Preserve that job and source. The new Spark
(`e8f1`) is idle and has checksum-verified Qwen 4B, 14B and VL-4B caches.
The two logical fabric interfaces share one physical 200 Gb/s cable. Earlier
host-memory RDMA tests measured approximately 27 Gb/s combined, not 200 Gb/s;
GPU collective bandwidth and the cause of that gap remain to be established.

Distributed runtime reference: [NVIDIA two-node vLLM playbook](https://build.nvidia.com/spark/vllm/multi-node).
GPU collective reference: [NVIDIA NCCL playbook](https://build.nvidia.com/spark/nccl/stacked-sparks).
Any adapted runtime must be pinned, tested with installed drivers, and run under
durable container supervision rather than an SSH-terminal lifetime.

## Verified progress, 2026-09-07

The original research job finished naturally before GPU acceptance began. No
existing inference service or research process was stopped. Both gateways are
separate CPU containers on loopback port 4110; original ports 4000/4010 remain
unchanged. Test GPU workers were stopped after profiling, pending fabric tuning.

- OMP 18.1.11 installed and version-verified on both Sparks, matching the Mac.
- `nvtop` exists on both. All source user executables and all 10 locked image IDs
  are present on e8f1. Tailscale remains a source-only tool. System package
  differences are recorded rather than resolved through OS/driver downgrades.
- Standalone Qwen3-4B-Instruct-2507 served on either node using identical model
  revision and vLLM image. Both could serve independent deployments concurrently.
- Native vLLM multiprocess TP=2 completed real inference with 66f1 as coordinator.
  NCCL logs identified both RDMA rails and one GPU rank per Spark.
- The two-rank collective sweep passed 20 correctness checks. Large CUDA-buffer
  all-reduce/all-gather measured approximately 24–25 Gb/s with MTU 1500.
  This is measured throughput, not validation of the advertised 200 Gb/s.
- DGX Spark does not support GPUDirect RDMA; the measured NCCL path stages through
  host memory. See [NVIDIA's Spark CUDA porting guide](https://docs.nvidia.com/dgx/dgx-spark-porting-guide/porting/cuda.html).
- Both gateways passed real text, streaming, automatic tool-call, synthetic
  tool-result continuation and streamed-argument assembly checks with the
  `local-coder` recipe on e8f1. The first streamed text arrived in about 85–89 ms.
- OMP generated text on the Mac and both Sparks; llm and the AIChat container
  also generated text. OpenClaw's live provider inference probe passed using an
  isolated Node 22.23.2 runtime. Real agent tool execution/coding quality was not
  evaluated; OMP client probes disabled its tools.
- Direct controller SSH works both ways over the fabric. Keys are generated on
  each node; private keys are never copied. Gateway forwarding is limited to
  remote loopback port 4110.
- Identical looped-LLM source snapshots were staged and hash-verified on both:
  `5c522575af631c2ccf34218e81042477d1e9c4b1cf5a8a06492d36df79ab8b48`,
  133 files, 1,448,940 bytes. Neither research checkout was modified.
- Loop LLM preflight refusal with linger disabled was exercised on e8f1. It
  launched no job, and project cleanup released the shared GPU reservation.
- The controller ran on e8f1 and successfully inspected 66f1 through direct SSH.
- After updating recipe metadata, saved-plan cleanup stopped the owned coder
  test model and released its reservation. Both gateways then advertised no models.
- Full Linux validation suite: **109 passed** after adding shared workload lease
  and tool-parser contract tests. Peer-SSH provisioning was also verified with
  actual bidirectional SSH.

### Vector Bucket acceptance

The source tree and host Python environments remain unchanged. The new adapter
stages selected audio and Python source as an immutable bundle, then runs a pinned
offline container under the shared GPU lease. It supports track and clip artifacts,
explicit placement, bounded runtime, checksum-verified collection and owned cleanup.

- CLAP and MERT model transfer: 13 regular files and 13 snapshot symlinks,
  1,879,738,276 bytes, verified with SHA-256 at source and destination. Transfer
  plus destination verification took 5.468 seconds over the peer SSH path; this
  is not a pure network bandwidth measurement.
- e8f1 real CUDA embeddings: CLAP track 2×512 in 7.173 seconds, CLAP clip 4×512
  in 7.293 seconds, and MERT track 2×1024 in 11.077 seconds. All vectors were finite
  and normalized. These small synthetic inputs validate functionality, not
  production indexing throughput or embedding quality.
- Both nodes received the identical finalized 51-file, 4,428,548-byte source/input
  bundle `35edf316b12393b1020b9f18da4b6160587e85a8a235b8c444443e46a0ef3b10`.
- 66f1 has a newer running research job. An actual start attempt refused admission
  with its GPU reservation unchanged; embedding acceptance there remains pending.
- An initial container failed because Torch could not resolve its numeric user.
  Explicit user/cache environment settings fixed that failure. Cleanup retained
  logs and released only the owned failed job; no host packages were changed.
- Successful test containers were removed and e8f1's GPU lease released. Artifacts
  and plans remain under `data/cluster/vector/`. Repeat CLAP track runs produced
  the same NPZ SHA-256 in this runtime.
- Existing Vector Bucket artifact merging accepted all three output formats.
  A one-second runtime test exited with code 124, retained its lease until explicit
  cleanup, then left no owned container or reservation. No agent was required for
  supervision or recovery.
- Added regression coverage for archive traversal, injected files/symlinks,
  changed caches and references, local collection, busy-GPU admission and immutable
  container recovery. The complete Linux regression suite passes 127 tests;
  baseline Compose validation and shell syntax checks also pass.

Raw runtime evidence is intentionally ignored under `data/cluster/`; acceptance
receipts include image/model identities, owner IDs, token usage and elapsed time.
Original copy checksums and transfer measurements remain under the controller's
`~/spark-mirror-inventory/` directory. See [the operator runbook](CLUSTER.md).

## Remaining acceptance work

- Apply the staged system package baseline and MTU test on both nodes; enable
  user-service linger on e8f1 before durable Loop LLM jobs. These require sudo.
- Reprofile both rails at MTU 9000 and investigate throughput before claiming
  efficient use of the physical link.
- Test reverse TP coordinator placement and pipeline parallelism; compare
  single-node, TP and PP latency/throughput with an optimized model recipe.
- Validate real agent tool execution, vision and larger-model recipes. The
  baseline 4B recipe advertises no tools; the separate coder recipe has passed
  protocol-level tool calling and streaming, not coding-quality evaluation.
- Run an actual Loop LLM GPU job on e8f1 through shared ownership. Validate the
  staged Vector Bucket worker on 66f1 after its research job finishes; extend
  audio preprocessing only with the required pinned dependencies and acceptance.
- Complete explicit OpenRouter upstream credential provisioning, multi-host
  failure/recovery acceptance, optimized recipe/performance comparisons, and
  final review of the reproducible operator workflow.
