# Operating interchangeable Spark nodes

This optional controller extends the existing stack. The original Compose file,
Make targets, `.env`, service aliases and ports continue to work. The current
implementation and hardware acceptance status are tracked in
[CLUSTER_IMPLEMENTATION.md](CLUSTER_IMPLEMENTATION.md).

There is no permanent main compute node. Each distributed deployment selects
one coordinator for its API and worker rendezvous; changing that coordinator
requires restarting that deployment. A Context Guard gateway can live on either
node independently of model placement. Existing UI/database services stay where
they are until explicitly migrated.

## Foundation and parity

On the controller, use Python 3.10+ for `sparkctl`. Gateway/client profile commands
also need `requests`, provided by the repository environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -r tools/requirements.txt
.venv/bin/python scripts/audit-spark-parity.py
scripts/sparkctl doctor
```

The parity audit records installed packages, known tool paths/versions, user
executables, Python environment inventories and immutable image IDs. It never
copies credential stores. Functional parity permits newer compatible OS packages
and different driver patch versions. Review `data/cluster/parity/comparison.json`
for remaining gaps; do not install old kernel packages to make a name list match.

On each Spark, from this checkout:

```bash
bash scripts/bootstrap-spark-packages.sh --check
sudo bash scripts/bootstrap-spark-packages.sh --install
bash scripts/install-omp-spark.sh
```

OMP is pinned to 18.1.11 with the official ARM64 release checksum. Cloud logins
are independent on each machine. Node is optional for OMP, which is standalone.
For a compatible isolated OpenClaw runtime:

```bash
bash scripts/install-spark-client-node.sh
```

This installs Node 22.23.2 under `~/.local/opt/spark-client-node/`; it does not
change system/Homebrew executables. `spark-client` uses it only for OpenClaw.

### Versioned controller installation

Keep the production checkout separate from the optional controller. From a
trusted committed checkout, create a bundle and record its full revision:

```bash
git rev-parse HEAD
git bundle create /tmp/spark-controller.bundle HEAD
scp /tmp/spark-controller.bundle scripts/install-spark-controller.py spark-e8f1-wired:~/scratch/
```

On that Spark, substitute the full commit printed above:

```bash
python3 ~/scratch/install-spark-controller.py \
  --bundle ~/scratch/spark-controller.bundle --revision FULL_COMMIT_ID
~/projects/local-llm-stack-cluster/bin/sparkctl doctor
```

Repeat for `spark-66f1-wired`. The installer uses an isolated environment with
exact runtime package versions and PyPI wheel hashes. The initial installation
needs access to PyPI; it never installs system packages or starts/stops services.
Use Python 3.10+ with `venv` support, Git and OpenSSH. The release is accepted only
after dependency and command checks pass. The supplied bundle and installer must
come from your trusted checkout; commit matching provides reproducibility, not
an independent publisher signature.

The default prefix is `~/projects/local-llm-stack-cluster`:

- `bin/` contains stable commands for all five controller entry points.
- `releases/FULL_COMMIT_ID/` contains the exact detached checkout and its environment.
- `current` points to the selected release; `previous` records the prior selection.
- `state/` contains controller plans, receipts and profiles shared by every release.

Upgrade by running the same installer with a new bundle and full revision.
Rollback by running it with the previous revision and a bundle containing that
commit. Existing releases are checked for tracked source changes before reuse.
Neither operation restarts a model or gateway. Run `down --saved-plan PATH` with
the original plan when recovering a deployment; changing releases does not
rewrite that plan. Node-side GPU reservations and service state remain in their
existing per-user state directories. Keep old releases until no operation needs
them. This installer does not copy credentials or import a development checkout's
ignored artifacts. Existing recovery records remain usable by explicit path.

To use gateways already installed on the nodes, attach from each controller:

```bash
~/projects/local-llm-stack-cluster/bin/spark-gateway attach --node 66f1
~/projects/local-llm-stack-cluster/bin/spark-gateway attach --node e8f1
```

This explicitly retrieves only the selected stack gateway's registry and API
key over the existing SSH connection, verifies container ownership, and stores
the key with mode `0600`. It does not restart or reconfigure the gateway. A
conflicting controller key is preserved and reported as an error. Gateway keys
are distinct from cloud provider credentials; cloud logins are never imported.
Attach also supplies the registry/key needed for clients targeting a peer gateway.

### Controller SSH from either Spark

Initially bootstrap from a machine whose inventory SSH targets already work:

```bash
.venv/bin/python scripts/configure-spark-peer-ssh.py
```

The two-node bootstrap generates node-local Ed25519 controller keys, installs
public keys restricted to the fabric source addresses, verifies peer host keys
through the already trusted SSH sessions, and adds managed peer aliases. It
allows forwarding only to the peer's loopback port 4110 and disables agent
forwarding. Private keys remain on their originating machine.

The same inventory can then be used from either Spark. Calls targeting the local
hostname execute locally; peer calls use direct fabric SSH. A model coordinator
is a deployment setting, not the controller machine.

## Independent single-node inference

Recipes pin container digests, model commits, context/output limits, capabilities
and supported parallelism. Deployment files select nodes and ports.

```bash
scripts/sparkctl validate --deployment cluster/deployments/fast-e8f1.json
scripts/sparkctl render --deployment cluster/deployments/fast-e8f1.json
scripts/sparkctl up --deployment cluster/deployments/fast-e8f1.json
scripts/sparkctl status --deployment cluster/deployments/fast-e8f1.json
scripts/sparkctl down --deployment cluster/deployments/fast-e8f1.json
```

Use `fast-66f1.json` for the same model contract on 66f1. Two independent single
deployments can run simultaneously. Each keeps a full model copy and its own GPU
reservation. An explicit gateway replica group can distribute requests between
them without changing client profiles.

`balanced-e8f1.json` and `balanced-66f1.json` select the cached Qwen3-14B revision
as `local-balanced`, with a 16,384-token context and a 4,096-token output cap.
The recipe defaults to non-thinking responses so ordinary clients receive final
text without extra request settings. This uses vLLM's
[server-level template defaults](https://docs.vllm.ai/en/latest/features/reasoning_outputs/#server-level-default-chat-template-kwargs),
confirmed in the pinned runtime. The 14B model passed direct completion on e8f1
and text/SSE through both gateways. Its 66f1 placement awaits GPU acceptance.
This recipe advertises neither tools nor vision.

### Runtime caches and compiled execution

The original eager recipes remain available. The optional
`balanced-compiled-e8f1.json` and `balanced-compiled-66f1.json` select the same
14B model contract, enable the pinned runtime's compilation/CUDA-graph defaults,
and use persistent runtime caches plus prefetched weight loading. Check the
recipe's validation note and the implementation record for measured behavior;
compiled execution is not assumed to improve throughput.

```bash
scripts/sparkctl up --deployment cluster/deployments/balanced-compiled-e8f1.json
scripts/sparkctl cache-status --deployment cluster/deployments/balanced-compiled-e8f1.json
```

`runtime_cache: true` stores vLLM, TorchInductor, Triton, FlashInfer and CUDA
artifacts in a labeled, node-local Docker volume. The model snapshot mount stays
read-only. Cache identity includes the pinned image/model, compute settings,
architecture and distributed rank. Alias, port, validation prose and weight-load
strategy do not change compiled computation and therefore do not change the
volume name. The pinned runtime independently validates its internal cache keys.
No cache is copied between nodes automatically.

Stopping a deployment retains this cache. To discard it explicitly after its
containers are removed:

```bash
scripts/sparkctl down --deployment cluster/deployments/balanced-compiled-e8f1.json
scripts/sparkctl cache-clear --deployment cluster/deployments/balanced-compiled-e8f1.json
```

Both cache commands also accept `--saved-plan PATH`. Cache ownership labels and
the Unix user must match. Docker refuses removal while any container references
the volume; the command does not stop containers or run a general prune.
The optional `load_strategy` field accepts `lazy`, `eager` or `prefetch` and maps
to the pinned loader flag. The 14B candidate uses `prefetch`; other models need
their own memory/startup acceptance. See
[vLLM cache and startup tuning](https://docs.vllm.ai/en/stable/configuration/optimization/).

Save benchmark JSON beside each exact deployment plan. For a matched comparison:

```bash
.venv/bin/python scripts/compare-spark-benchmarks.py \
  --baseline /path/to/eager/benchmark.json --baseline-plan /path/to/eager/plan.json \
  --candidate /path/to/compiled/benchmark.json --candidate-plan /path/to/compiled/plan.json \
  --output data/cluster/runtime-comparison.json
```

The comparison requires matching model revisions, precision, context/output
contracts, workload settings and request counts. It rejects incomplete or
mismatched runs and reports throughput, first-token latency and throughput per
allocated GPU. It does not estimate model quality or wall-power efficiency.

### Image models

`vision-e8f1.json` and `vision-66f1.json` select the same pinned
Qwen3-VL-4B-Instruct snapshot as `local-vision`. The bounded baseline supports
text and up to two images, an 8,192-token context and a 2,048-token output cap.
It uses BF16, eager execution, four concurrent sequences and 30% GPU memory.
Video and tools are disabled. Image preprocessing uses a 65,536-pixel minimum
and 1,048,576-pixel maximum, so larger source images are downscaled. This limits
fine-detail/OCR resolution; choose and validate another recipe when higher
resolution is needed. The
[vLLM multimodal controls](https://docs.vllm.ai/en/latest/configuration/conserving_memory/#multi-modal-processor-arguments)
and the pinned image processor were checked before acceptance.

```bash
scripts/sparkctl up --deployment cluster/deployments/vision-e8f1.json
```

Add the saved plan's route to the complete gateway registry, preserving other
routes. Use `spark-gateway render --plan PATH` to generate the vision route, then
merge it into the registry used by `spark-gateway routes`. Model placement and
gateway placement are independent. The recipe passed on e8f1 through either
gateway; running its GPU worker on 66f1 still awaits the protected research job.

The repeatable acceptance probe includes actual image recognition, image-token
accounting, two-image rejection boundaries, oversized-image downscaling and
image preservation through context compaction. Run on either gateway's Spark
while e8f1 hosts the model:

```bash
.venv/bin/python scripts/probe-spark-vision.py \
  --base-url http://127.0.0.1:4110/v1 --tokenizer-url http://10.10.20.2:8101 \
  --key-file ~/.local/state/local-llm-cluster/gateway/api-key --guard \
  --output data/cluster/vision-acceptance.json
```

`probe-spark-vision-client.py --node e8f1 --client omp --output PATH` tests real
image attachments using generated profiles and opaque fixture filenames. It also
supports `--client llm` and `--client aichat`, with the usual explicit port,
registry and key-file options for a Mac tunnel. OMP passed on both Sparks, AIChat
passed on e8f1 and llm passed on the Mac. OpenClaw receives the image capability
in its generated profile; an OpenClaw image-agent session was not tested.

Container AIChat needs an explicitly selected attachment folder. For example:

```bash
scripts/spark-client run --node e8f1 --client aichat \
  --attachment-dir /path/to/images -- \
  --model spark:local-vision --file /path/to/images/example.png 'Describe this image.'
```

Only that directory is mounted, read-only at the same absolute path. Native
AIChat already reads local files directly. This option does not mount the
controller's home directory or change existing text-only client behavior.

To measure a live text model from its gateway's Spark:

```bash
.venv/bin/python scripts/benchmark-spark-inference.py \
  --base-url http://127.0.0.1:4110/v1 \
  --key-file "$HOME/.local/state/local-llm-cluster/gateway/api-key" \
  --model local-balanced --concurrency 1 2 4 --requests 4 \
  --max-tokens 128 --prompt-repeats 64 --prefix-mode unique \
  --output data/cluster/balanced-benchmark.json
```

The benchmark warms weights/kernels, then measures client-observed first-token
latency and server-reported completion counts. Each unique prompt starts with a
new identifier to avoid intentional prefix-cache reuse. `--prefix-mode shared`
measures repeated-prefix behavior separately. It rejects incomplete streams,
missing token usage and reasoning output rather than reporting misleading text
decode rates. Compare identical recipes/prompts before changing runtime settings;
these synthetic requests do not measure answer quality.

### Independent replicas and request-level data parallelism

Start matching standalone deployments with the normal `sparkctl up` commands
after each node is free. The `coder-66f1.json` / `coder-e8f1.json` pair uses the
same tool-enabled recipe; the `fast-66f1.json` / `fast-e8f1.json` pair uses the same
text-only recipe. Use their printed owner directories when installing a group:

```bash
.venv/bin/python scripts/spark-gateway up --node e8f1 --replicas \
  --plan data/cluster/CODER_66F1_OWNER/plan.json \
  --plan data/cluster/CODER_E8F1_OWNER/plan.json
```

For an existing gateway with this runtime, use `routes` instead of `up`. Repeat
for the other gateway if desired. Duplicate aliases still fail unless `--replicas`
is explicit, and members must use identical pinned recipes. This is request-level
data parallelism: model weights remain complete on each replica. TP/PP deployments
instead split one model's execution across GPUs.

Each gateway sends a new request to the healthy member with the fewest in-flight
requests, rotating ties. The selected member retains the full request, including
tokenization, compaction, context retries and the entire stream. Backend failure
does not replay generation on another member; subsequent requests may select a
healthy peer. The scheduling counts are local to each gateway, and there is no
session affinity or automatic cloud/model fallback.

Generated routes verify the advertised alias, pinned model snapshot and context
length in addition to `/health`, so a different model taking the same port does
not satisfy an old route. `/v1/models` advertises a group only when a member
passes those checks. Responses carry the selected saved-plan digest in
`X-Spark-Deployment`. Client-facing aliases, token budgets and capabilities stay
the same whether one or several members are available.

HTTP concurrency, stream isolation, unavailable/wrong-model exclusion and
non-replay tests pass. Both physical gateways have passed real tool and streaming
requests with e8f1 serving while the 66f1 replica is unavailable. Concurrent
two-GPU replica requests also passed through both gateways using `local-fast`:
each gateway's four-request batch selected two requests per node. After owned
removal of the 66f1 model, both unchanged registries served fresh text and SSE
requests entirely through e8f1. This tests new-request routing after member
removal; it does not claim recovery of an interrupted generation or physical
network-partition acceptance.

`benchmark-spark-inference.py` records each response's `deployment_digest` from
the gateway's selection header (null for a direct server). It identifies the
selected saved route, not cryptographic attestation of a running container.

The controller refuses admission when another GPU process, GPU container,
reservation or unresolved Loop LLM window exists. It does not stop other jobs.
Reservations are per Unix user and persist across lost SSH/controller sessions.
Unmanaged tools do not participate in the reservation protocol; use the managed
commands for cooperating jobs and avoid starting legacy GPU workloads concurrently.

`up` publishes readiness only after health checks and a real model completion.
The saved `data/cluster/<owner>/plan.json` is the recovery handle:

```bash
scripts/sparkctl status --saved-plan data/cluster/OWNER/plan.json
scripts/sparkctl down --saved-plan data/cluster/OWNER/plan.json
```

Recovery checks persisted ownership, immutable container IDs and labels. It uses
the saved render, so later renderer updates do not require recreating an old
configuration. Cleanup retains reservations if container ownership is ambiguous.
`down` stops owned requests/workers immediately; graceful request draining is not
implemented. Stop routing new requests before planned maintenance.

## Distributed inference and fabric profiling

For the candidate BF16 80B model that exceeds one node's memory, see
[the pinned large-model preparation workflow](SPARK_LARGE_MODEL.md). It includes
bounded downloads, checksum-verified peer copies and TP/PP placements with either
coordinator. Its full two-node runtime acceptance remains pending.

```bash
scripts/sparkctl up --deployment cluster/deployments/fast-tp2.json
scripts/sparkctl collectives --deployment cluster/deployments/fast-tp2.json
scripts/sparkctl down --deployment cluster/deployments/fast-tp2.json
```

The pinned vLLM runtime uses native multiprocess multi-node launch, explicit node
ranks, an explicit coordinator address and Docker supervision. TP=2 and PP=2
have passed real completion with either node coordinating, plus text/SSE through
both independent gateways for reverse TP and both PP placements. Choose
`fast-tp2.json` or `fast-tp2-e8f1.json` for TP; choose `fast-pp2.json` (e8f1
coordinator) or `fast-pp2-66f1.json` for PP. The small-model measurements and their
limits are recorded in [the implementation record](CLUSTER_IMPLEMENTATION.md).
Unsupported recipe modes fail
validation. TP × PP must equal the allocated GPU count.

One QSFP cable exposes two logical fabric rails. Seeing 200000 Mb/s on both
interfaces does not mean 400 Gb/s aggregate. Current measured bulk GPU collective
throughput is about 24–25 Gb/s at MTU 1500. Spark uses host-staged RDMA; do not
enable unsupported GPUDirect settings based on guides for other hardware.

For a repeatable host-memory diagnostic, run from either installed controller
(or this checkout on the Mac):

```bash
.venv/bin/python scripts/profile-spark-fabric.py \
  --qps 1 --sizes 65536 --seconds 5 \
  --output data/cluster/fabric-host-baseline
```

The runner uses the inventory's explicit link addresses, verifies each IPv4
RoCE v2 GID, and measures both directions on each rail and on both concurrently.
It records perftest versions, PCIe links, active verbs MTU, CPU policies/load,
background GPU processes, raw logs and counter changes. Each output directory
must be new; incomplete sweeps retain completed cases with `complete: false`.
The installed perftest defaults to host memory; this runner never enables CUDA
or modifies MTU, routes, power settings or another workload's affinity.

`--cpus CPU0 CPU1` optionally pins just the test processes, one CPU per rail,
after checking that both CPU IDs are available on both nodes. Select IDs from
the recorded topology/policies rather than assuming IDs identify the same core
type on all hardware. `--qps` accepts 1/2/4/8 and `--sizes` accepts 64 KiB, 1 MiB
and 8 MiB. The default sweep uses QP counts 1/4 and sizes 64 KiB/8 MiB. Duration
is bounded to 5–30 seconds per case. Tests use CQ moderation 1 and no post-list
batching; these settings are explicit diagnostic workloads, not an automatic
search for maximum bandwidth.

A per-node lock prevents overlapping runs of this profiler. Servers bind only
the selected fabric address and port (default 28550/28551). Readiness requires a
listener in the launched process group. Cleanup targets only those children,
and an independent GNU timeout bounds them even after controller/SSH loss.
On interruption, allow the node watchdog to expire before retrying; a surviving
profile lock refuses overlapping work. The timeout is the configured duration
plus 25 seconds, followed by a three-second kill deadline.

The reported concurrent result sums the individual averages from overlapping
runs; it is not a separately synchronized aggregate measurement. Raw host-memory
RDMA speed is distinct from model-copy throughput and host-staged GPU collective
speed. Background research work can affect either direction. See the measured
results and their limitations in [the implementation record](CLUSTER_IMPLEMENTATION.md).

For a controlled GPU-load comparison on an idle node, the optional
`cluster/workloads/loop-fabric-load.json` runs the existing 80M recurrent Loop
LLM capacity probe for up to 180 seconds, under a 300-second job-supervisor
limit. Use the normal Loop staging/start workflow below with this job config and
a fresh job ID. Confirm the owned CUDA process is active before running the
fabric profile. Then wait for the same job to finish, collect
`probes/fabric-load.json`, release its lease and repeat the identical profile.
The profiler records instantaneous GPU utilization and power samples before and
after each case. Verify the expected job PID is present in every loaded case and
absent in each idle case; a resident process alone is not evidence of GPU work.

This workload reuses synthetic inputs and records finite forward/backward/Adam
steps. It does not emulate every research workload, measure training quality,
or measure wall-power efficiency. GPU work on the other node is preserved and
remains a confounding factor. The recorded before/load/after experiment is in
[the implementation record](CLUSTER_IMPLEMENTATION.md).

To run the staged jumbo-frame comparison, stop managed GPU jobs first, then on
**both** nodes:

```bash
bash scripts/configure-spark-fabric-mtu.sh --check
sudo bash scripts/configure-spark-fabric-mtu.sh --apply
```

The script changes only the two fabric profiles and their current link MTU. It
records previous values under `/var/lib/local-llm-stack/` for `--rollback`.
Re-run real inference and collective correctness after both sides are changed;
retain a setting only with measured improvement and no link errors.

## Independent gateways and client contexts

Render an inference deployment first. Use its saved plan to populate either
gateway; it does not have to execute on the gateway's node:

```bash
.venv/bin/python scripts/spark-gateway up --node e8f1 --plan data/cluster/OWNER/plan.json
.venv/bin/python scripts/spark-gateway probe --node e8f1
.venv/bin/python scripts/spark-gateway routes --node e8f1 --plan data/cluster/OTHER_OWNER/plan.json
```

Repeat `--plan` for distinct aliases, or use the explicit `--replicas` mode above.
Route,
tokenizer and context policy are replaced atomically; each request keeps one
consistent route. Backend failure returns an explicit error, without silently
switching model or sending data to a cloud provider. `/v1/models` only advertises
live backends. The public API currently supports chat completions and model
discovery, not the Responses API.

Gateway keys are generated per node and stored with owner-only permissions.
Gateways bind to `127.0.0.1:4110`; access them through SSH. On the Mac, keep these
in separate terminals while using the clients:

```bash
.venv/bin/python scripts/spark-client tunnel --node 66f1 --port 4111
.venv/bin/python scripts/spark-client tunnel --node e8f1 --port 4112
```

Select the gateway through a generated isolated client context:

```bash
.venv/bin/python scripts/spark-client run --node e8f1 --port 4112 --client omp -- --model spark-e8f1/local-fast
.venv/bin/python scripts/spark-client run --node e8f1 --port 4112 --client llm -- -m spark-e8f1/local-fast 'Hello'
.venv/bin/python scripts/spark-client run --node e8f1 --port 4112 --client openclaw -- config validate
.venv/bin/python scripts/spark-client run --node e8f1 --port 4112 --client aichat -- 'Hello'
```

On the gateway's own Spark, omit `--port` to use local port 4110. The runner reads
that node's local gateway registry and key. From another controller, use a
locally provisioned gateway key (`--key-file`) and registry (`--registry`) if
they are not already in that checkout's controller state. Missing keys fail
explicitly. No existing default client configuration is overwritten.

Profiles are generated under `data/cluster/clients/<node>` and API keys are passed
through the launched process environment, never written into profile files.
`OMP_PROFILE` is cleared for this isolated run. The pinned 4B acceptance recipe
has **tools=false**; loading a coding client is not yet evidence of functioning
coding tools. Enable a validated tool parser/recipe before agent workloads.

OMP 18.1.11, llm 0.28, AIChat and OpenClaw's live provider probe have passed with
`coder-e8f1.json` (`local-coder`, 32768 context tokens, Hermes tool parser). Both
gateways passed automatic tool calling, tool-result continuation and streaming
argument assembly. These are transport/protocol checks, not a coding-quality
evaluation. Use the coder deployment and model alias for tool-enabled profiles.
OMP has also passed actual read-tool execution on the Mac and both Sparks using
a generated verification file. The value is absent from its prompt and must be
read through the tool and returned in the final assistant response. To repeat on
a Spark whose local gateway serves `local-coder`:

```bash
.venv/bin/python scripts/probe-spark-omp.py --node e8f1 --output data/cluster/omp-read.json
```

The probe enables only `read`, disables extensions/LSP, uses an ephemeral client
profile and enforces a time limit. On the Mac, add `--port 4112` while its gateway
tunnel is running. This verifies agent/tool transport, not general coding quality.
On Linux, the client runner uses the locked AIChat image if a native executable
is absent; it mounts only that isolated AIChat configuration directory and passes
the gateway key through the environment. It never pulls a mutable image tag.

OpenClaw 2026.9.1 has also passed a real read-tool turn from the Mac through
either gateway while e8f1 hosted `local-coder`. Repeat with OpenClaw installed
on the client machine and its gateway tunnel active:

```bash
.venv/bin/python scripts/probe-spark-openclaw.py --node e8f1 --port 4112 --output data/cluster/openclaw-e8f1-read.json
.venv/bin/python scripts/probe-spark-openclaw.py --node 66f1 --port 4111 --output data/cluster/openclaw-66f1-read.json
```

Each run uses the generated provider contract in a temporary config and state
directory, disables plugins and hosted catalog refresh, skips workspace bootstrap,
and allows only `read` within its generated workspace. It preserves ordinary
OpenClaw settings. A 90-second client deadline has a 150-second process-group
backstop. Acceptance requires a matching call, successful tool result and final
answer from the selected provider/model in the retained transcript; a text-only
guess cannot pass. Output paths must be new. The probe exports only its messages
and result diagnostics, then removes the temporary state database. Its transcript
reader is tested against this OpenClaw version; schema changes fail explicitly.
This is protocol/tool acceptance, not general coding-quality evaluation or an
OpenClaw image-agent test. OpenClaw's CLI was exercised on the Mac, not installed
or tested on both Sparks by this probe.

To repeat the bounded protocol acceptance against a live deployment:

```bash
.venv/bin/python scripts/probe-spark-gateway.py --base-url http://127.0.0.1:4112/v1 --key-file data/cluster/gateways/e8f1/api-key --model local-coder --output data/cluster/gateway-probe.json
```

### Optional OpenRouter upstream

OpenRouter can serve an explicitly selected cloud alias through either Context
Guard. Its [chat-completions API](https://openrouter.ai/docs/quickstart) uses
`https://openrouter.ai/api/v1` and a provider Bearer key. Clients continue using
the gateway's URL and gateway key. OpenRouter does not connect to the private
Spark, and local-model failures do not select a cloud model automatically.

Add a route to your registry alongside existing routes. Choose a real provider
model ID and verify its supported context/output limits and capabilities; these
illustrative limits and placeholder model are not an accepted model recipe:

```json
"cloud-openrouter": {
  "base_url": "https://openrouter.ai/api/v1",
  "upstream_model": "YOUR_PROVIDER_MODEL_ID",
  "upstream_key_env": "OPENROUTER_API_KEY",
  "context_tokens": 8192,
  "max_output_tokens": 1024,
  "capabilities": {"text": true, "vision": false, "tools": false, "streaming": true}
}
```

Apply the complete registry, then provision a key from a private regular file
owned by you (mode `0600`). The file must contain only the provider key. No key
value is placed in command arguments or saved to a client profile:

```bash
scripts/spark-gateway routes --node e8f1 --registry /path/to/complete-registry.json
scripts/spark-gateway credential-set --node e8f1 \
  --name OPENROUTER_API_KEY --key-file /path/to/private/openrouter-key
scripts/spark-gateway credential-status --node e8f1
```

Use the installed `~/projects/local-llm-stack-cluster/bin/` commands on a Spark,
or this checkout's environment on the Mac. Repeat with `--node 66f1` if that
gateway should also use the cloud provider. Provisioning is explicit for each
node; gateway attachment does not copy provider credentials.

Keys are stored in the gateway's private `config/credentials.json`, mounted
read-only into its container. Each key is bound to the exact upstream base URL
referenced by the registry at provisioning time. Changing the destination leaves
that route unconfigured until explicitly provisioned again. A credential name
must refer to exactly one base URL; multiple models at that URL can share it.
No shell environment is modified. Legacy environment-based credentials still
work for manually launched gateways when no managed entry exists.

Run `credential-set` again to rotate the key without a restart. Each request
retains its chosen key through context handling and streaming. To disable it:

```bash
scripts/spark-gateway credential-remove --node e8f1 --name OPENROUTER_API_KEY
```

Removal writes a tombstone that also disables legacy environment fallback for
that name; it does not revoke the key at the provider. Missing, removed or
mismatched credentials hide the cloud alias from `/v1/models` and reject new
requests. Local routes remain available. Reattach/render client profiles after
changing the registry, then select `spark-e8f1/cloud-openrouter` in OMP or llm,
or the corresponding generated OpenClaw/AIChat model. Cloud inference may incur
provider charges; this configuration never sends a test request automatically.

For a repeatable acceptance check using only a temporary local HTTP provider,
run on the gateway Spark:

```bash
.venv/bin/python scripts/probe-spark-provider.py --node e8f1 \
  --output data/cluster/provider-probe.json
```

It exercises credential provisioning, rotation, removal, model translation and
text/SSE responses. It removes its temporary route and disables its test key
while preserving other routes. This fixture verifies integration mechanics;
it does not establish real OpenRouter account access or model quality.

## Looped LLM project placement

The adapter uses the project's existing allowlisted source snapshot publisher
and durable systemd job/window manager. It does not modify a research checkout.
Stage the same source on either node using a fresh job ID:

```bash
.venv/bin/python scripts/spark-loop stage --node e8f1 --job capacity-001 --source ../looped-llm-lab
.venv/bin/python scripts/spark-loop inspect --node e8f1 --job capacity-001
```

Before running durable user jobs, enable user-service persistence once on the
new Spark:

```bash
loginctl --no-ask-password enable-linger statsparrot
loginctl show-user statsparrot -p Linger
```

Then, after other GPU workloads are stopped:

```bash
.venv/bin/python scripts/spark-loop start --node e8f1 --job capacity-001
.venv/bin/python scripts/spark-loop status --node e8f1 --job capacity-001
.venv/bin/python scripts/spark-loop logs --node e8f1 --job capacity-001
.venv/bin/python scripts/spark-loop wait --node e8f1 --job capacity-001 --timeout 600
.venv/bin/python scripts/spark-loop fetch --node e8f1 --job capacity-001 --artifact probes/recurrent-80m.json
.venv/bin/python scripts/spark-loop release --node e8f1 --job capacity-001
```

`release` requires the job to be finished. `stop` explicitly stops the owned job,
restores its window and releases the shared reservation. A failed or ambiguous
launch retains its reservation until this recovery path confirms the GPU is
idle. Other legacy services are never paused: the adapter requires `idle_only`.

`--job-config` selects another explicit supervised container job, while the
adapter replaces host/root/window/job identities with the chosen placement.
Models/datasets are separate artifacts; source staging does not clone research
data or credentials. The default smoke recipe performs a bounded recurrent-model
capacity probe. Linger is now enabled on both nodes; enabling it for the current
user succeeded without sudo on e8f1. If a different installation refuses that
normal authorization path, its administrator must enable it once.

New jobs write into `looped-llm-lab/runs/cluster/JOB/`, preserving earlier results.
`fetch` collects only explicitly named regular files from a terminal job's own
run directory and verifies their hashes. `wait` never relaunches a job on a
timeout or transport failure. The existing supervisor enforces the separate
`max_runtime_seconds` limit and persists its completion/recovery record.

Staging works on the Mac or either Spark, locally or over configured peer SSH.
`--ssh-key` remains an optional override; a Mac-specific private key is not
required on the worker. Both nodes have the identical accepted source snapshot.
The e8f1 CUDA probe passed 20 measured optimizer steps at about 10,755 synthetic
tokens/second, with a 95 ms median step and 2.66 GB peak CUDA allocation. This is
a training-shape smoke test, not corpus throughput or training-quality evidence.
The 66f1 acceptance run awaits its existing research job finishing.

## Vector Bucket placement

`spark-vector` stages a checksummed snapshot of the selected audio, job manifests,
Vector Bucket Python source and worker recipe. It runs a detached, bounded Docker
job with the same exclusive GPU reservation used by inference and Loop LLM.
The container uses the locked vLLM image's CUDA/audio libraries; it does not
install packages into either project's host environment. Networking is disabled,
source and model mounts are read-only, and Hugging Face credential files are not
mounted. Track and clip NPZ artifacts keep Vector Bucket's existing format.

First provision the exact CLAP and MERT snapshots. Run the following from a
checkout on the source Spark, where those revisions are already cached:

```bash
python3 scripts/sync-spark-models.py sync \
  --cache "$HOME/.cache/huggingface" \
  --lock cluster/vector-models.lock.json \
  --peer spark-e8f1-wired \
  --peer-cache /home/statsparrot/projects/local-llm-stack/data/vector-huggingface
```

The copy verifies SHA-256 at both ends, preserves snapshot symlinks, and refuses
a conflicting `refs/main`. It never deletes unrelated models. To select different
cached revisions, create a separate lock with the `lock` action and repeated
`--model organization/repository@COMMIT` arguments; use a separate managed cache
when references differ. The current acceptance recipes are validated only with
the committed CLAP/MERT lock.

From the controller, make a deterministic two-track fixture and run it:

```bash
.venv/bin/python scripts/make-vector-smoke.py --output data/cluster/vector-fixture
.venv/bin/python scripts/spark-vector stage --node e8f1 --id clap-001 \
  --source ../vector-bucket \
  --job data/cluster/vector-fixture/job.json \
  --data-dir data/cluster/vector-fixture/data
.venv/bin/python scripts/spark-vector start --node e8f1 --id clap-001
.venv/bin/python scripts/spark-vector wait --node e8f1 --id clap-001 --timeout 180
.venv/bin/python scripts/spark-vector fetch --node e8f1 --id clap-001
.venv/bin/python scripts/spark-vector stop --node e8f1 --id clap-001
```

Use a fresh job ID for new inputs. Select `--spec cluster/workloads/vector-mert.json`
for MERT or `vector-clap-clips.json` for clip output. Use your existing Vector
Bucket job and data directory for real audio. This runtime currently supports
`first` and `spread` windows; adaptive/riff-solo preprocessing is not validated.
For 66f1's existing cache, add `--model-cache /home/statsparrot/.cache/huggingface`
at staging; only its `hub/` directory is mounted. Both nodes use the same source
and model identities. A running research job blocks admission without pausing it.

`wait` is an observation deadline: losing the controller or reaching that deadline
does not restart or stop a job. The recipe's separate `max_runtime_seconds` bounds
the container command. Success or failure retains its GPU reservation until
explicit `stop`; cleanup checks immutable container IDs and ownership labels.
On failure, use `status` to obtain the container ID and inspect its Docker logs
on the worker before `stop`. Cleanup also saves the last 500 log lines under
`~/.local/state/local-llm-cluster/OWNER/logs/`. Artifacts survive cleanup.

Fetched results are under `data/cluster/vector/NODE/JOB/artifacts/`. The acceptance
receipt records vector dimensions/normalization, artifact hash, source bundle,
model revisions, runtime versions and elapsed time. CLAP track (2×512), CLAP clip
(4×512) and MERT track (2×1024) GPU jobs passed on e8f1. The identical worker is
staged on 66f1; its active research workload currently prevents GPU acceptance.
