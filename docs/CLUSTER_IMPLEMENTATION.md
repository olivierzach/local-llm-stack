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

- Apply the staged system package baseline and MTU test on both nodes. These
  remain pending; user-service linger on e8f1 is now enabled.
- Reprofile both rails at MTU 9000 and investigate throughput before claiming
  efficient use of the physical link.
- Test reverse TP coordinator placement and pipeline parallelism; compare
  single-node, TP and PP latency/throughput with an optimized model recipe.
- Validate vision and larger combined-node recipes. The
  baseline 4B recipe advertises no tools; the separate coder recipe has passed
  protocol-level tool calling and streaming, not coding-quality evaluation.
- Validate the staged Loop LLM and Vector Bucket workers on 66f1 after its research
  job finishes; extend
  audio preprocessing only with the required pinned dependencies and acceptance.
- Complete explicit OpenRouter upstream credential provisioning, multi-host
  failure/recovery acceptance, optimized recipe/performance comparisons, and
  final review of the reproducible operator workflow.

## Loop LLM GPU acceptance

The normal `loginctl --no-ask-password enable-linger statsparrot` operation
succeeded on e8f1. Its existing project supervisor then ran
`capacity-foundation-002`, using immutable image ID
`sha256:2d08870c99da6e5e1f174a9aa53ba779fe30a8299675928931280a57dc60e4e8`.
It completed 20 real forward/backward/Adam steps on CUDA with finite losses.
The measured rate was 10,755 synthetic tokens/second, median step 95.09 ms, and
peak CUDA allocation 2,657,227,776 bytes. The report explicitly excludes corpus
quality, input-pipeline throughput and convergence claims.

The accepted source snapshot is
`89be22128c3ac1f554f01c69d9ef7bb1caa71885deabad6e341db4f38d832300`
(133 files, 1,449,489 bytes). It is present on both nodes. The newer snapshot
captures current user source without changing either research checkout.
Staging was also repeated from each Spark to itself and its peer, proving that controller
placement does not depend on a Mac key or self-SSH alias.

The supervisor persisted success, removed its exact owned container and released
its project window. The cluster CLI then fetched the checksummed probe report
and released its shared GPU reservation. The report SHA-256 is
`f36bf62e54e70045fe1f271db702f90ff0c2788a6110b7caf12549e43587b3cf`.
Plans, logs, status, artifact checksums and the original report are retained under
`data/cluster/loop/e8f1/capacity-foundation-002/`. The 66f1 research container was
still running during this acceptance and was preserved.

## Larger single-node acceptance and batching

Qwen3-14B revision `40c069824f4251a91eefaf281ebe4c544efd3e18` passed direct
completion on e8f1 under owner `balanced-e8f1-74d3d0e09112`. Both independently
placed gateways passed real text and SSE requests as `local-balanced`, with a
16,384-token context and 4,096-token output cap. The pinned recipe uses BF16,
eager execution, prefix caching, up to eight sequences, 45% GPU memory utilization
and non-thinking template defaults. The normal cache/image checks run before GPU
admission; this acceptance used the already-mirrored weights.

The bounded gateway benchmark used 128 output tokens, 64 repetitions of its
synthetic context sentence, unique request prefixes, warm weights/kernels and
actual SSE usage records. Four requests were measured at levels 1/2/4; eight
requests were measured at level 8 after a separate warmup. Small sample sizes
and synthetic prompts limit the conclusions to this workload.

| Configured concurrency | Aggregate output tokens/s | Median first-token seconds | Median per-request decode tokens/s |
| --- | ---: | ---: | ---: |
| 1 | 8.12 | 0.394 | 8.26 |
| 2 | 15.81 | 0.635 | 8.20 |
| 4 | 30.17 | 1.105 | 8.07 |
| 8 | 54.44 | 1.800 | 7.58 |

This demonstrates useful batching and its latency tradeoff. It does not compare
compiled execution, quantization, TP/PP, maximum context or production prompts.
Raw per-request timings, exact plans and gateway probes are retained under
`data/cluster/balanced-e8f1-74d3d0e09112/`. The prior gateway registries were
restored, the owned test container removed and its GPU reservation released.
The current larger-model placement on 66f1 is validated structurally, with GPU
acceptance pending its research job finishing.

The expanded Linux suite passes **142 tests**, including Loop finalization and
artifact isolation, model template defaults and streaming benchmark accounting.

## Replica routing and real OMP tools

The gateway now supports explicit replica groups made from identical pinned
recipes. Scheduling uses each gateway's count of in-flight requests, rotating
ties. One member handles the complete request, including tokenization, compaction
and streaming. Generation errors are not replayed on a different member.
New requests can select a remaining healthy member. This implements independent
request-level data parallelism, while TP/PP remain separate deployment modes.

Generated routes also verify the advertised model alias, snapshot root and
context length. A healthy server hosting a different model on a reused port no
longer makes an old generated route appear ready. Existing manual registry
schemas remain supported; the stronger identity checks apply when `model_root`
is present. Changing a single placement to replicas produces identical OMP,
OpenClaw, AIChat and llm client profiles.

Both gateways were upgraded to the same runtime
`spark-gateway-5b8c99428fc5`, retaining their individual keys and loopback ports.
Both passed real text, SSE, automatic tools, tool-result continuation and streamed
arguments with the e8f1 coding deployment. Each response selected the configured
e8f1 plan digest `c16571152b27e6eee0a0bf774b90fd21f39c866e8364f6abb95c1293cd0a65b2`;
the unavailable 66f1 member was excluded. This is hardware evidence for degraded
group operation, not simultaneous two-GPU scheduling or member-loss acceptance.

OMP 18.1.11 executed a real `read` tool on the Mac, 66f1 and e8f1. Every probe
created a fresh local verification file, withheld its value from the prompt,
enabled only `read`, and verified the matching tool-call/result plus final
assistant answer. The 66f1 case performed local tool execution while inference
ran on e8f1 through 66f1's gateway. The Mac's temporary SSH tunnel was closed by
its owning process. These tests establish agent/tool transport, not coding quality.

Evidence is under `data/cluster/coder-e8f1-c16571152b27/`. The test GPU worker was
removed and its reservation released. Both gateways retain the current coding
replica configuration and advertise no model while both members are unavailable.
The 66f1 research container remained running throughout. Simultaneous replica
throughput and live member-loss acceptance still await that job finishing.

The complete Linux suite passes **156 tests**, including concurrent routing,
stream leases, model mismatch, no generation replay, registry replacement,
gateway startup readiness and strict tool-result verification. NetworkManager
reports `auth` for profile/network changes from these SSH sessions; fabric tuning
still needs the previously staged privileged setup.
