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
deployments can run simultaneously. This provides replicas/independent jobs;
automatic load balancing and failover are not implemented.

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

Repeat `--plan` for distinct aliases. Duplicate aliases are rejected. Route,
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
sudo loginctl enable-linger statsparrot
```

Then, after other GPU workloads are stopped:

```bash
.venv/bin/python scripts/spark-loop start --node e8f1 --job capacity-001
.venv/bin/python scripts/spark-loop status --node e8f1 --job capacity-001
.venv/bin/python scripts/spark-loop logs --node e8f1 --job capacity-001
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
capacity probe. Staging is verified on both nodes; actual GPU job acceptance is
pending linger setup on e8f1.

Vector Bucket's environment and audio/GPU checks pass on e8f1, but a real model
embedding and a shared-reservation placement adapter remain unfinished. Continue
using its existing isolated worker scripts until that acceptance is complete.
