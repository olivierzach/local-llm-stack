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
requests with e8f1 serving while the 66f1 replica is unavailable. Simultaneous
two-GPU replica throughput and live member-loss acceptance remain pending the
66f1 research job finishing.

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

```bash
scripts/sparkctl up --deployment cluster/deployments/fast-tp2.json
scripts/sparkctl collectives --deployment cluster/deployments/fast-tp2.json
scripts/sparkctl down --deployment cluster/deployments/fast-tp2.json
```

The pinned vLLM runtime uses native multiprocess multi-node launch, explicit node
ranks, an explicit coordinator address and Docker supervision. TP=2 with 66f1 as
coordinator has passed real inference. `fast-pp2.json` selects e8f1 as coordinator
and PP=2; its hardware acceptance is pending. Unsupported recipe modes fail
validation. TP × PP must equal the allocated GPU count.

One QSFP cable exposes two logical fabric rails. Seeing 200000 Mb/s on both
interfaces does not mean 400 Gb/s aggregate. Current measured bulk GPU collective
throughput is about 24–25 Gb/s at MTU 1500. Spark uses host-staged RDMA; do not
enable unsupported GPUDirect settings based on guides for other hardware.

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

To repeat the bounded protocol acceptance against a live deployment:

```bash
.venv/bin/python scripts/probe-spark-gateway.py --base-url http://127.0.0.1:4112/v1 --key-file data/cluster/gateways/e8f1/api-key --model local-coder --output data/cluster/gateway-probe.json
```

OpenRouter is an optional cloud upstream, not a way for OpenRouter's cloud to
reach a private Spark. Registry routes support explicit upstream model/key-env
translation, but remote gateway credential provisioning for that provider is
not yet implemented. Cloud fallback remains disabled.

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
