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
- [x] OpenClaw, OMP, AIChat, llm and OpenRouter configuration adapters (provider fixture acceptance; no real cloud account test)
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
- Investigate the measured sender-direction asymmetry and repeat the host-memory
  profile with 66f1 idle. Compare MTU 9000 only as a measured follow-up; the fast
  direction already reaches about 185 Gb/s summed across rails at MTU 1500.
- Test reverse TP coordinator placement and pipeline parallelism; compare
  single-node, TP and PP latency/throughput with an optimized model recipe.
- Validate larger combined-node recipes and the vision GPU placement on 66f1.
  The baseline 4B recipe advertises no tools; the separate coder recipe has passed
  protocol-level tool calling and streaming, not coding-quality evaluation.
- Validate the staged Loop LLM and Vector Bucket workers on 66f1 after its research
  job finishes; extend
  audio preprocessing only with the required pinned dependencies and acceptance.
- Complete multi-host failure/recovery acceptance, optimized recipe/performance
  comparisons, and final review of the reproducible operator workflow.
- A real OpenRouter account/model call remains untested; provider transport and
  credential lifecycle have passed local fixture acceptance on both gateways.

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

## Reproducible controller installation and attachment

Both Sparks now have the same detached controller release
`f7d3bfa95a52856f5ef0fcbc33bbf36d4e3c6e44` under
`~/projects/local-llm-stack-cluster/releases/`. Stable commands live in that
prefix's `bin/`; `current` selects the release. The original production and
research checkouts remain separate. This installation requires no agent at run
time and does not depend on the temporary validation directory.

The installer takes a local trusted Git bundle and explicit full commit ID.
Six runtime dependencies are pinned with PyPI wheel hashes; both hosts installed
those exact versions under Python 3.12.3. The requirements lock SHA-256 is
`5af29d29fe1fe1da12047129ffe9622b0bd85779c0f76e2e8ed6454f8da92277`.
It validates dependency consistency and all five command entry points before
activation. Repeated installation reuses a checked release. An unrecognized
existing destination is refused; failed new releases cannot remove the prior
active release or shared recovery state. This is reproducible dependency/source
selection, not an assertion that all platform binaries are identical.

`state/` is shared by releases, including saved plans, profiles and gateway
attachments. On e8f1, a real rollback to
`5bc0c543845e422ceae22f7e3d7667f956354ef5` and restoration of the current release
preserved all 18 state files byte-for-byte. The previous release remains
available on both hosts. No active service was restarted during either upgrade
or rollback.

Both controllers successfully inspected both nodes over the existing SSH paths.
Each then attached to both owned gateways, using the explicit `spark-gateway
attach` command. It retrieves only that gateway's registry and key, stores the
key with mode `0600`, excludes its value from CLI output, and preserves any
conflicting controller credential. All four authenticated model-list probes
passed. Empty model lists are expected while no managed inference worker runs.
OMP, OpenClaw, AIChat and llm profiles were rendered for each gateway from each
controller. Gateway container IDs and start times were unchanged. The protected
66f1 research job remained running with container process 4030689.

Acceptance receipts and two-node doctor output are retained under
`data/cluster/controller-install/` on the Mac. The full installed-release Linux
suite passes **162 tests**, including real Git-bundle upgrade/rollback fixtures,
failed-release preservation and gateway attachment ownership/credential checks.
Baseline Compose configuration validation also passed. Fabric tuning, remaining
cross-node GPU acceptance and explicit OpenRouter credential provisioning remain
open as listed above.


## Provider credentials and optional OpenRouter routing

Both stable controllers now select release
`8983febee5cfbf1ca9c1abd36e937a4ee66b4c21`. Both optional gateways were upgraded
to `spark-gateway-2c33ee379a9b` after authenticated inspection showed no available
inference model. Their existing keys, loopback ports and complete coding-replica
registries were preserved. The protected 66f1 research container remained running
with process 4030689; baseline Compose services were not changed.

The gateway CLI supports explicit `credential-set`, `credential-status` and
`credential-remove` operations. Input is a private regular key file owned by the
controller user; credentials travel through SSH input, not command arguments.
The node stores them with mode `0600` in the gateway's private config directory.
Provisioning requires a registry reference and pins the exact upstream base URL.
Rotation is atomic and applies to new requests; in-flight requests retain their
selected key. Removal records a tombstone, including disabling legacy environment
fallback for that name. It does not revoke the credential at the cloud provider.
Unconfigured or destination-mismatched provider aliases are excluded from model
listings and fail before sending an inference request. Local routes remain usable.

The deterministic `probe-spark-provider.py` acceptance ran on both Sparks. Each
run created only a local HTTP provider, temporarily added one explicit alias,
and verified missing-key rejection, provisioning, model translation, authenticated
text, rotation, authenticated SSE, removal and subsequent rejection. Both runs
passed exactly two authorized provider requests; their fixture routes were removed
and test credentials disabled. Gateway container IDs/start times remained unchanged
during provisioning/rotation/removal. No cloud API key was read, copied or used,
and no real OpenRouter request was sent. This establishes integration mechanics,
not account authorization or cloud model quality.

Evidence is retained under `data/cluster/provider-provisioning/` on the Mac and
`~/projects/local-llm-stack-cluster/state/provider-probe.json` on each Spark. The
full installed-release Linux suite passes **167 tests**, including origin binding,
private-file validation, corruption handling and in-flight credential retention.
Compose configuration validation passed. The operator runbook contains explicit
OpenRouter route and per-gateway provisioning instructions; local models never
fall back to that cloud route implicitly.


## Bounded vision model and image-client acceptance

The pinned Qwen3-VL-4B-Instruct revision
`ebb281ec70b05090aa6165b016eac8ec08e71b17` passed on e8f1 under owned deployment
`vision-e8f1-8e5a780b29a7`. The recipe uses BF16, eager execution, an 8,192-token
context, a 2,048-token output cap, four concurrent sequences and 30% GPU memory.
Its explicit processor policy permits two images, disables video, and bounds
processed image area to 65,536–1,048,576 pixels. The pinned runtime's processor
implementation was inspected to verify support for these arguments.

The direct model and both independently placed gateways correctly recognized
two synthetic images in order, reversed the answer when image order reversed,
and streamed complete responses. The probe uses actual PNG data and never
places the expected colors in its prompt. Direct tokenizer counts matched model
usage, and gateway input-token headers matched both. Three-image requests were
rejected. A 1536×1536 source image was processed within the same token budget as
a 1024×1024 source, verifying the configured downscaling policy.

| Acceptance input | Original/input tokens | Tokens used after processing/compaction |
| --- | ---: | ---: |
| Text prompt without images | 34 | 34 |
| Same prompt plus two 384×384 images | 326 | 326 |
| Single 1024×1024 or 1536×1536 image | 1060 for either | 1060 |
| Long conversation ending in two images | 24832 | 6083 |

Both gateways preserved the images and answered correctly after compacting the
long conversation. The final gateway probes reported about 0.113–0.114 seconds
to the first streamed delta and about 3.60 seconds for compaction plus completion.
These are small synthetic acceptance samples with warm caches, not a throughput
benchmark. The first direct image request took 7.45 seconds, illustrating cold
image-path overhead. General photo/OCR quality, video, tools and optimized vision
performance are not established by this test.

Actual attachments also passed in OMP on both Sparks, AIChat's container on e8f1,
and llm on the Mac through an owned temporary SSH tunnel. Fixture filenames were
opaque and client sessions/profiles temporary. OpenClaw's generated profile
advertises image input correctly; an OpenClaw image-agent session remains untested.
Container AIChat now supports an explicit read-only attachment directory without
mounting the user's home or changing existing text-only behavior.

Evidence is retained under `data/cluster/vision-e8f1-8e5a780b29a7/` on the Mac.
The test model was stopped using its exact saved plan and its GPU reservation
released. Both original coding-replica registries were restored. Stable
controllers now select `228500e8cadae46812b68941b3a121218c2bf7c8`, which includes the
validated recipe and probes. Both optional gateway runtimes were reconciled to
`spark-gateway-2789753ab64e`, preserving registries and credentials. The 66f1
research container remained running with process 4030689. Full installed-release
Linux validation passes **170 tests**; baseline Compose configuration also passes.

## Persistent runtime caches and compiled 14B acceptance

The optional compiled Qwen3-14B recipe retains the baseline model revision,
BF16 precision, 16,384-token context, 4,096-token output cap and eight-sequence
limit. It enables vLLM's compiled execution defaults and a labeled Docker volume
for runtime artifacts. The existing eager recipe and legacy Compose remain
unchanged. Cache volumes are local to each node, keyed by pinned computation
inputs, and retained after the owned model container is removed. Explicit cache
removal checks ownership and refuses in-use volumes without stopping workers.

Both stable controllers now select `4ca4ab5b21e50720bbb50c3be95769e793625eaa`.
Installation restarted no services. The isolated Linux validation checkout
passed **175 tests**, including actual socket admission, cache lifecycle and
matched benchmark validation. Startup admission now permits TCP TIME_WAIT left
by a stopped server while still rejecting an active listener.

On e8f1, cold compiled owner `balanced-compiled-e8f1-c460d10ea7fb` passed real
completion. Its cache contained about 255 MB in 683 files. Docker refused an
attempt to clear that cache while its worker was running. After stopping the
owned worker, `balanced-compiled-e8f1-c0b13ca5b0f6` reused the same volume with
prefetched weight loading. Logs explicitly confirmed compiled-graph and AOT
cache hits: graph loading took 0.646 seconds and reported total compilation fell
from 13.91 to 2.10 seconds. The restart passed a real completion and both gateways
listed `local-balanced`; its full controller startup took 217.58 seconds.

Weight loading still dominated: 182.50 seconds in the initial run versus 180.94
seconds with prefetch. Page-cache prefetch itself took 2.99 seconds. These were
sequential runs with potentially warm filesystem caches, so this does not
establish a prefetch speedup. Persisting compiled artifacts reduced repeated
compilation work; throughput is evaluated separately. The 66f1 compiled GPU
placement remains untested while its research job owns that GPU.

The refreshed read-only parity audit in `data/cluster/parity-current/` confirms
OMP 18.1.11 and nvtop on both Sparks, no missing source user executables and no
missing source Docker images on e8f1. Tailscale remains the only source tool
absent there. Desktop and old kernel/driver package differences are not blindly
copied; the staged system package baseline remains pending.

The matched throughput runs used the same e8f1 gateway, unique approximately
1K-token prompts, 128 output tokens, eight requests at every concurrency level
and one excluded warmup. Both variants returned 1,024 output tokens per level.
The baseline owner was `balanced-e8f1-4f0ebc014dc8`; the compiled owner was
`balanced-compiled-e8f1-c460d10ea7fb` (before changing only its weight loader).

| Concurrency | Eager output tokens/s | Compiled output tokens/s | Eager median TTFT, s | Compiled median TTFT, s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8.02 | 8.18 | 0.393 | 0.414 |
| 2 | 15.35 | 15.66 | 0.688 | 0.728 |
| 4 | 29.30 | 29.89 | 1.138 | 1.190 |
| 8 | 52.85 | 53.64 | 1.787 | 1.913 |

Compiled throughput was about 1.5–2.0% higher in this matched sample, with median
first-token latency about 4.5–7.0% higher. These single sequential trials do not
establish a statistically significant speedup, a production latency advantage,
or a power-efficiency improvement. The original eager recipe remains the
baseline; compiled execution is opt-in. Weight-loader changes affect startup,
not the warm-throughput workload measured here.

Exact saved plans, full benchmark records and startup logs are retained under
the three owner directories in `data/cluster/`. The validated comparison is
`data/cluster/compiled-14b-comparison.json`. All owned test models were stopped,
both original coding-replica registries restored, and e8f1's GPU lease released.
The node-local compiled cache remains available for reuse. The 66f1 research
container and its process 4030689 remained running throughout.

## Bidirectional host-memory fabric profiling

A deterministic `profile-spark-fabric.py` runner now measures host-memory RDMA
writes in both directions, on each logical rail and both concurrently. It uses
explicit inventory addresses and matching RoCE v2 GIDs, records hardware/runtime
metadata and counter deltas, and binds short-lived test servers only to the
fabric IPs. It does not reconfigure networking or use a GPU. Independent GNU
watchdogs bound each test even if its controller connection disappears.

Three six-case sweeps completed at Ethernet MTU 1500. The verbs active MTU was
1024 bytes, with 4096 supported. Perftest version 6.20 and NIC firmware
28.45.4028 matched. Both NIC paths reported PCIe width 4 at 32 GT/s. CPU policies
were `performance` on both nodes. 66f1 ran kernel `6.17.0-1021-nvidia` and GPU
driver `580.159.03`; e8f1 ran `6.17.0-1032-nvidia` and `580.173.02`.

| Workload | Sender | Rail 0, Gb/s | Rail 1, Gb/s | Concurrent rail-average sum, Gb/s |
| --- | --- | ---: | ---: | ---: |
| 64 KiB, 1 QP, default CPU placement | 66f1 | 9.86 | 9.16 | 18.01 |
| 64 KiB, 1 QP, default CPU placement | e8f1 | 109.04 | 109.04 | 185.14 |
| 64 KiB, 1 QP, CPUs 5/15 | 66f1 | 9.27 | 9.17 | 17.78 |
| 64 KiB, 1 QP, CPUs 5/15 | e8f1 | 109.05 | 109.03 | 185.14 |
| 8 MiB, 4 QPs, CPUs 5/15 | 66f1 | 8.77 | 8.77 | 18.17 |
| 8 MiB, 4 QPs, CPUs 5/15 | e8f1 | 111.94 | 111.92 | 185.22 |

The concurrent column sums averages from overlapping perftest runs; it is not a
separately synchronized measurement window. Each case used a five-second test,
CQ moderation 1 and no post-list batching. These are bounded diagnostic samples,
not maximum-performance certification. They do establish a substantial
asymmetry and show that the physical path can carry far more host-memory traffic
than the earlier 24–25 Gb/s GPU-collective measurement. The latter remains the
measured NCCL result; these RDMA figures must not replace it in inference claims.

66f1's research process was active throughout; e8f1's GPU was idle. Pinning only
the test processes to two observed high-frequency CPUs did not remove the
asymmetry. No collected error, discard, drop or retransmission counter increased
in these sweeps. Different kernel/driver versions and concurrent research work
remain confounding factors. Repeat the same profile after 66f1 becomes idle
before attributing the slow direction to workload contention, software or MTU.
The 185 Gb/s direction already operated at Ethernet MTU 1500, so a jumbo-frame
change is not established as the fix for the slow direction.

Disconnect acceptance started one bounded server on e8f1, recorded watchdog PID
214535, then closed only its controller SSH connection. Subsequent inspection
confirmed that exact watchdog was absent, port 28560 had no listener and no
`ib_write_bw` process remained. The protected research container stayed running
with process 4030689. No agent performed remote cleanup.

Evidence lives in `data/cluster/fabric-host-mtu1500-q1-v2/`,
`data/cluster/fabric-host-mtu1500-cpu5-15/`,
`data/cluster/fabric-host-mtu1500-bulk/` and
`data/cluster/fabric-watchdog-acceptance.json`. Implementation commit `cd337ef`
passed the complete **182-test Linux suite**. Focused coverage includes bounds,
units, process ownership, counter resets, CPU-affinity scope and perftest's
nonstandard successful version-query exit code. Package installation and
multi-node GPU acceptance still require the previously recorded prerequisites.

## Controlled GPU-load comparison

To test whether GPU activity by itself reproduced the slow sender behavior, e8f1
ran the existing Loop LLM recurrent-80M capacity probe under the shared GPU lease.
The new optional `loop-fabric-load.json` recipe sets a 180-second probe budget and
a 300-second durable supervisor limit. Job `fabric-load-001` reused the verified
133-file snapshot `89be22128c3ac1f554f01c69d9ef7bb1caa71885deabad6e341db4f38d832300`
and the previously pinned Torch image. Neither source checkout was changed.

The owned CUDA process (PID 220400) was present before and after all six loaded
RDMA cases. Each e8f1 GPU-utilization sample reported 92%; instantaneous reported
power ranged approximately 44.4–46.6 W during those cases. These device telemetry
samples are not average power or energy measurements. The same 64 KiB, one-QP,
five-second RDMA settings and CPUs 5/15 were used in all three phases.

| Sender | Rail(s) | Before, Gb/s | During training, Gb/s | After, Gb/s |
| --- | --- | ---: | ---: | ---: |
| e8f1 | 0 | 109.05 | 106.25 | 109.03 |
| e8f1 | 1 | 109.03 | 106.28 | 109.04 |
| e8f1 | concurrent 0+1 sum | 185.14 | 183.07 | 185.14 |
| 66f1 | 0 | 9.27 | 9.17 | 9.14 |
| 66f1 | 1 | 9.17 | 10.30 | 9.00 |
| 66f1 | concurrent 0+1 sum | 17.78 | 18.30 | 18.81 |

The large directional gap persisted. This particular GPU workload reduced the
fast direction modestly and did not reproduce the slow 66f1 sender. Generic GPU
activity is therefore insufficient to explain the observed gap. The two nodes'
workloads, kernel and GPU-driver versions differ; this experiment does not
identify which difference causes it or exclude workload-specific interference.
A matched idle-node/software comparison remains necessary before prescribing
host changes. The concurrent figures retain the profiler's overlapping-window
qualification and are not NCCL or model-copy throughput claims.

The probe finished naturally with 1,842 measured forward/backward/Adam steps,
finite losses, BF16 autocast, approximately 10,539 synthetic tokens/s and a
96.61 ms median step. The supervisor removed its owned container and restored its
window; the cluster controller then collected the checksummed artifact and
released the GPU lease. e8f1 had no CUDA process before or after any idle case.
66f1's protected research process remained running throughout.

The 109,041-byte report `probes/fabric-load.json` has SHA-256
`fa3cde79243c3be17acc88330ce921e74c8fe5cd745af7205a5963df17d9fd61`.
Artifacts and recovery records are under `data/cluster/loop/e8f1/fabric-load-001/`.
Loaded and post-load profiles are under `data/cluster/fabric-host-e8f1-training-load/`
and `data/cluster/fabric-host-e8f1-post-training/`; the matched phase/PID checks
and comparison are retained in `data/cluster/fabric-controlled-load-comparison.json`.

## Combined-memory model preparation

The candidate Qwen3-Next-80B-A3B-Instruct BF16 revision
`9c7f2fbe84465e40164a94cc16cd30b6999b0cc7` has 41 weight shards totaling
162,659,161,528 bytes, exceeding one Spark's RAM. A committed manifest pins all
51 files (162,682,272,937 bytes), upstream SHA-256s, runtime image and download
library. The CPU-only e8f1 download is bounded by Docker resource limits and a
six-hour watchdog, uses no credential, and emits a peer-copy lock only after
every file passes verification. Download and peer-copy completion remain pending.

The exact pinned runtime loaded the model config and tokenizer offline:
`Qwen3NextConfig`, 48 layers, attention heads 16/2 and linear heads 16/32. Those
dimensions divide for TP=2 and PP=2, and the Instruct chat template rendered an
11-token probe correctly. No weights or GPU kernels were loaded by that check.
Its receipt is `data/cluster/large-model-preparation/offline-runtime-metadata.json`.

Four deployments select TP=2 or PP=2 with either coordinator and the same
`local-large` contract. Their recipe is explicitly a candidate: full model memory
fit, hybrid kernels, generation, streaming and performance remain unvalidated.
No gateway routes were changed. Admission now refuses partial numbered shard
sets and missing index references before reserving GPU resources. The download,
resume, copy and acceptance steps are documented in [SPARK_LARGE_MODEL.md](SPARK_LARGE_MODEL.md).

Release `7e868e0fe78d8698b238a0ed7c5e2b4af5a873d4` was installed on both nodes
without restarting services. Its complete Linux suite passed **188 tests**;
baseline Compose and shell syntax checks passed. The actual in-progress cache
with 21 of 41 shards was rejected by admission without a GPU reservation.

An eight-hour user service on e8f1 now waits on the exact download container's
successful exit before running the immutable release's peer-copy command. The
first service failed before copying because its lingering user manager lacked
the Docker group present in SSH sessions. The corrected `-v2` service uses
`sg docker` and was confirmed active, waiting on that same download. This did
not restart the user manager or the download. Neither download completion nor
peer-copy verification is claimed yet. Current receipt and exact job handles
are in `data/cluster/large-model-preparation/preparation-acceptance.json`.

66f1's previously observed research container has been replaced by another
running research job (`ca8939aab641`, CUDA PID 339241). Its research window is
entered, so combined-node GPU acceptance remains deferred. No research job was
stopped. e8f1 has no CUDA process or GPU reservation during this preparation.

## OpenClaw read-tool acceptance through either gateway

OpenClaw 2026.9.1 (`ad6fe23`) on the Mac, with its isolated Node 22.23.2 runtime,
completed an actual read-only tool turn through each physical Context Guard
gateway. The same managed Qwen3-4B coder deployment on e8f1 served both contexts.
Each run used an independent generated nonce fixture whose value was absent from
the prompt, and completed one `read` call plus a final answer in two model turns.
SQLite transcript inspection verified the selected provider/model, fixture path,
call ID, successful tool result and exact final value. No plugin, messaging tool,
shell execution, write tool or existing client session was used.

The repeatable `probe-spark-openclaw.py` script derives its provider config from
the normal client adapter, applies probe-only tool/workspace restrictions, and
retains message/result evidence before removing its temporary state. Nine
regression cases reject missing calls, mismatched files/IDs/providers, tool
errors, prompt leakage and contradictory final responses or tool summaries.
Actual reports are `data/cluster/openclaw/e8f1-read.json` and
`data/cluster/openclaw/66f1-read.json`, with exported message traces alongside.
This establishes client tool transport through either gateway with independent
model placement. It does not claim coding quality, OpenClaw installation on the
Sparks, or image-agent acceptance. 66f1's research job was left running.
