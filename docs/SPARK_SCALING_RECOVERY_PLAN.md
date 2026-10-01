# Adding Sparks and recovering from failures

Design/qualification roadmap, updated September 16, 2026. This document does not
enable failover, change network settings or qualify TP3. The authoritative
[local-first operations plan](LOCAL_FIRST_OPERATIONS_PLAN.md) defines automatic
preferred-TP2 → qualified single-node fallback → automatic TP2 failback and its
safety/acceptance contract. This roadmap retains topology, enrollment and model
compatibility details; it does not define a second recovery state machine.
Current serving evidence is in [GLM53_TP.md](GLM53_TP.md), historical DeepSeek
qualification in [DEEPSEEK_TP.md](DEEPSEEK_TP.md), and implementation status in
[CLUSTER_IMPLEMENTATION.md](CLUSTER_IMPLEMENTATION.md).

## What adding a node should mean

Enroll the machine once, select an existing compatible workload recipe, render
an explicit placement, run preflight and acceptance, and publish the stable
model alias. No new Python branch or copied per-host startup script should be
needed for each additional machine. Hardware enrollment and model qualification
are still necessary; they cannot be replaced by discovering an IP address.

There are three independent choices:

- **Compute placement:** which GPUs run a standalone model, replicas, a TP/PP
  group, Vector Bucket or Loop LLM.
- **Per-deployment coordinator:** the API/rendezvous rank for one distributed
  workload. Any qualified member can take this role on a fresh launch. It is
  fixed for that running group; adding a node does not resize a live TP process.
- **Client entrypoint:** the stable URL and model alias. Gateway availability and
  application state must be handled independently of model placement.

A three-node cluster could run DeepSeek TP2 on A+B and a single-node model or
batch job on C. Four nodes could run two independent TP2 groups, multiple
standalone deployments, or a separately qualified TP4 model. A replica increases
independent request capacity; it does not split one request's weights.

## Is DeepSeek TP=3 supported?

**No, not by this pinned checkpoint/runtime.** The qualified
`DeepSeek-V4-Flash-0731` checkpoint at revision
`7872f01b1d1fe23eabc4c98b48bffcef5a386062` has 64 attention heads, hidden size
4096, 8 output groups and MoE intermediate size 2048. Its loaded
`vllm/models/deepseek_v4/attention.py` explicitly asserts
`self.n_heads % tp_size == 0`. Since 64 is not divisible by 3, a third GPU cannot
be added to this recipe by changing `tensor_parallel` to 3. This is a concrete
runtime constraint, not an inference from combined memory size.

The generic renderer was exercised with a temporary, synthetic third inventory
node. It generated three workers with ranks 0/1/2 and TP=3; **that was a CPU-only
configuration check, not an inference test**. It exposed a missing early gate:
manifest validation currently checks TP×PP equals the allocated GPU count but
does not inspect checkpoint partition constraints. The DeepSeek publication
validator independently requires TP2, so this candidate is not publishable.

TP4 passes the 64-head arithmetic check, but that alone does not establish
compatibility of the remaining tensors, quantized kernels, DSpark, memory or
network. It needs four GPUs and new acceptance. PP3 is also not a supported
DeepSeek recipe here; do not assume pipeline support from generic controller
support. Other models can have different valid TP sizes. No padding or model
surgery is proposed for the current deployment.

## The cable is a network, too

RoCE means RDMA over Ethernet. The direct QSFP cable carries an IP network as
well as the high-volume RDMA transfers. Wi-Fi, ordinary Ethernet and Tailscale
are different interfaces/transports; SSH does not imply Wi-Fi.

| Traffic | Current path | Policy for expansion |
| --- | --- | --- |
| Mac maintenance SSH | `spark-66f1-wired` via 10.10.10.2; e8f1 via that host and 10.10.20.2 | Keep explicit management aliases; add an independently reachable management path for fault recovery |
| Peer controller SSH | Managed aliases use the first direct-fabric IP | Make management transport explicit; an authorized Ethernet/Wi-Fi/Tailscale path can carry lifecycle commands, independently of tensor traffic |
| Artifact copies | `spark_transfer.transport` binds interface/source IP and rejects a gateway, jump host or multiplex fallback | Explicit bulk-transfer policy; remain fabric-only by default and verify every source/destination route |
| Model API from peer gateway | 10.10.20.x:8123 for the DeepSeek placement | Explicit backend address and reachability; HTTP traffic is not a tensor collective |
| Distributed rendezvous/Gloo | Coordinator fabric IP:29543; first fabric interface | Require a reachable control address for every rank; do not infer this from SSH success |
| NCCL tensor communication | Qualified `NET/IB` RoCE across both inventoried logical interfaces | Validate required RDMA transport and counters across every participating rank; no silent Wi-Fi fallback |
| Existing Mac OMP/Fam-Chat ingress | Existing `spark-66f1` URLs; Mac resolves this name to its Tailscale address | Preserve client identity; make gateway/application host availability explicit |

The two current logical interfaces (`enp1s0f0np0`, `enP2p1s0f0np0`) are two PCIe
paths to **one physical QSFP link**, not two 200-Gb/s cables. NVIDIA documents the
port/interface mapping and Ethernet-only QSFP operation in its
[ConnectX-7 guide](https://docs.nvidia.com/dgx/dgx-spark/spark-clustering.html).

Current manifests set `NCCL_SOCKET_IFNAME` and `GLOO_SOCKET_IFNAME` to a fabric
interface and select the inventoried RDMA HCAs. NCCL socket bootstrap is therefore
also wired. `NCCL_IB_DISABLE=0` permits RDMA; it is not alone proof that NCCL
selected it. The existing receipts verify `NET/IB` and fabric activity. A future
strict RDMA preflight should explicitly reject a TCP fallback even on the wired
interface when the recipe requires RDMA.

An optional management-over-Wi-Fi mode can supervise processes while tensor
traffic stays on the fabric. It must be explicitly provisioned with trusted SSH
identities and a reachable route. It must not turn a cable failure into an
attempt to continue the TP job over Wi-Fi. The present peer-key authorization
restricts source addresses to the fabric; an alternate path is not configured
merely by changing a DNS name.

Management reachability alone does not enable cable-free single-node serving.
Current reservation/preflight checks require all inventoried fabric interfaces
even for single-node mode, the renderer binds the API to the first fabric IP, and
gateway routes use the saved endpoint. The planned cutover must separate
management, serving and collective addresses in rendering, admission, health
checks and publication. Old saved plans stay immutable; newly accepted fallback
plans must explicitly use the independently reachable serving path. Neither SSH
retry nor editing an old plan is a substitute for this capability.

## Physical topology for three or more nodes

NVIDIA's current Cluster Assistant documents three-node direct cabling as a
triangle: each node connects to the other two, using three cables total.
Two-to-four nodes can instead use a switch; four requires a switch in that
supported workflow. See [NVIDIA's topology guide](https://docs.nvidia.com/sync/0.97.6/cluster-assistant.html).
This is the tool's documented support boundary, not proof that all larger
custom clusters are impossible.

For incremental growth, a suitable switched RoCE fabric is the simpler target:
new members join the same fabric networks instead of changing every pair's
point-to-point addressing. A three-node triangle remains a possible topology,
but must model the destination-specific port/path. Do not reuse the present
10.10.20.0/30 and 10.10.21.0/30 subnets for a third host: each has only two usable
host addresses. Cabling, addressing and route changes require a separate
maintenance window; none are applied by this plan.

## Node enrollment — implemented, physical acceptance separate

`scripts/spark-node` provides bounded discovery, preparation, reviewed inventory
admission and removal. No `sparkctl node enroll` command exists. Run from the
recovery worktree or its reviewed pinned controller release:

```bash
scripts/spark-node discover --inventory "$CANDIDATE"
scripts/spark-node check --inventory "$CANDIDATE" --deployment "$DEPLOYMENT"
# Only after approval; selected image/cache writes, no GPU workload:
scripts/spark-node prepare --inventory "$CANDIDATE" --deployment "$DEPLOYMENT" \
  --pull-image --apply
# Repeat --approve-stable-address for every approved fabric/serving address:
scripts/spark-node apply --inventory "$CANDIDATE" --base-inventory "$BASE" \
  --output "$NEW_INVENTORY" --approve-inventory-sha256 "$CANDIDATE_INVENTORY_SHA256" \
  --approve-stable-address "$APPROVED_ADDRESS" --apply
```
Use the `inventory_sha256` reported by discovery or dry admission for
`CANDIDATE_INVENTORY_SHA256`: it hashes canonical sorted compact inventory JSON,
**not the raw file bytes**. Removal uses the same canonical digest of the old inventory.


Outputs are new exclusive files; admission never rewrites the base inventory or
saved plans. Optional `--deployment` plus `--plan-output` renders a new placement.
Model staging is explicitly selected by `--model-manifest`, bounded with
`--max-download-bytes` and an explicit staging `--timeout`; cached content and
free-space checks precede approved writes. Repeating preparation reuses verified
artifacts. Host package/driver installation remains separately administered:
missing tools are reported, not fixed with implicit sudo.

Discovery can suggest a machine; it must not automatically trust a discovered
host or start a GPU workload. Observations distinguish detected and prepared state
from per-deployment GPU qualification. Current version-1 inventory accepts optional
management/serving policy and validated switched/direct/triangle topology. A plan
rendered from changed inventory is a new plan; the old exact recovery plan remains
immutable. Stable independent serving addresses require explicit approval; neither
`.local` discovery nor a currently observed DHCP address proves that stability.

`configure-spark-peer-ssh.py --inventory "$CANDIDATE" --trust "$TRUST"` checks
without applying. The trust document is `{version: 1, nodes: {NODE: {host_key: KEY}}}`
with independently acquired Ed25519 host keys for **every** participating node.
Optional node fields are `management: [{alias, address}]` and `source_addresses`.
With approved `--apply`, bootstrap authenticates each connection using those pins,
generates private keys only on their origin node, merges N-node managed public-key/
alias blocks and records rollback state. It disables ambient SSH key-update/DNS/
global-host-trust shortcuts, forwarding agents and connection multiplex reuse.
Management source restrictions are explicit; NCCL remains on the declared fabric.

Inventory removal uses `spark-node remove --inventory "$OLD" --node "$NODE"
--output "$NEW" --approve-inventory-sha256 "$OLD_INVENTORY_SHA256" --apply`; active or
unknown ownership is refused. Peer trust revocation is a separate approved
`configure-spark-peer-ssh.py --inventory "$OLD" --trust "$TRUST" --remove "$NODE"
--apply`, performed while the old inventory and trusted peer paths remain available.
Do not delete the old inventory/trust first.

Local regressions cover N-peer merge/remove preservation, pinned handshakes,
rollback and lost replies, candidate admission, missing tools, offline peers and
selected cache preparation. They do not qualify a physical third Spark, its
collectives or any enlarged model group. Admission never automatically grows a
live TP deployment or publishes an unqualified model alias.

## One-time controller generalization

| Work package | Planning limitation | Implementation and remaining acceptance |
| --- | --- | --- |
| Version-1 inventory extensions | One `ssh` alias and an undifferentiated fabric list | Optional management/serving policy and explicit topology validation are implemented without a v2 cutover; approved physical paths and each rendered plan still need acceptance |
| Node enrollment | Earlier bootstrap supported only two nodes | `spark-node` and pinned N-peer bootstrap implement reviewed admission/removal and preservation; local tests do not replace real added-node qualification |
| Topology preflight | Copy helper pairs interfaces by rail index; profile tool assumes two nodes/two rails | Reachability matrix across all participating nodes, physical-to-logical interface map, switch/direct topology checks, GID validation, measured link performance |
| Model compatibility | Generic GPU-count check permits mathematically invalid TP3 | Check pinned checkpoint/runtime tensor constraints before GPU reservation; recipes declare qualified TP/PP/speculation combinations; unknown combinations are candidates, not live aliases |
| Placement generation | Core renderer accepts N nodes, but convenience manifests/Make selectors enumerate the pair | Deterministic deployment generation from recipe + node list + coordinator; no host-specific code for new inventory members |
| Publication evidence | DeepSeek gate requires exactly TP2 and both coordinator roles | Keep existing TP2 gate; add a separate N-node acceptance contract requiring every allowed coordinator role, all-rank health and exact recipe/runtime/topology identity |
| Operational state | Node-local reservations prevent many conflicts; no qualified multi-host fencing under partitions | Durable single recovery authority, route generations and surviving-node ownership gates; quarantine unreachable owners without reusing their GPUs |
| User entrypoint | Existing URL can depend on one Spark | Independently hosted stable frontend with synchronized route policy and credentials; strict aliases unchanged, model substitution only through an explicit opt-in policy alias |

Generic orchestration should be changed once for these capabilities. Adding a
normal node afterward changes inventory/enrollment data; selecting a model group
changes deployment data. New model architectures, engines or unsupported network
topologies still need engineering and qualification. This is not a proposal to
adopt Kubernetes or a new scheduler just to run three Sparks.

## Kubernetes/K3s decision — optional later backend

Recommendation for this setup: retain Compose workers and the selected Mac's
launchd-supervised recovery/gateway services for the bounded recovery policy.
Kubernetes or K3s is a possible later backend/pilot, not a prerequisite for recovery,
repeatable enrollment or stable client aliases. No migration is authorized or
performed by this plan, and this is not a reason to build a general-purpose
scheduler into the current controller.

Kubernetes provides standard deployment reconciliation and resource scheduling;
GPU scheduling requires vendor device integration. Those are useful capabilities,
but do not validate DeepSeek tensor shapes, resize a running TP group, preserve
an in-flight generation, improve prefill speed or create a second model replica
when both GPUs already belong to one TP2 engine.
See [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
and [GPU scheduling](https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/).

NVIDIA's [GPU Operator 26.3 platform support matrix](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/26.3/platform-support.html)
lists Spark/K3s support. That is a supported-platform starting point, not proof
that this exact runtime, RDMA path, cache layout or recovery contract is qualified
under K3s. A pilot must pin compatible versions, verify GPU and RDMA access,
storage placement, secrets and upgrades, and avoid automatic driver replacement.
The [LeaderWorkerSet project](https://github.com/kubernetes-sigs/lws) provides
coordinated leader/worker groups to evaluate for TP lifecycle management; it
does not itself certify model constraints, inference readiness or safe fencing.
NVIDIA documents RDMA device/network integration separately in its
[Network Operator deployment guide](https://docs.nvidia.com/networking/display/kubernetes2612/deployment-guide-kubernetes.html).

Distinguish compute coordinator choice from control-plane ownership. A single
K3s server is a management failure boundary, not HA.
[K3s embedded-etcd HA](https://docs.k3s.io/datastore/ha-embedded) requires at least
three server nodes; two Sparks alone do not satisfy that design. Existing workers
may keep running during a control-plane outage while scheduling/recovery is
impaired. The selected **awake Mac** is the recovery-authority/gateway host.
It is independent of either Spark's lifetime, not independent of this Mac,
its logged-in service user, power or network. It is a single point of failure.

Revisit Kubernetes when there are multiple independently schedulable GPU groups,
frequent placement changes, several users needing quotas/queues, or enough service
churn that maintaining reconciliation and admission ourselves becomes substantial.
This is a workload/operations threshold, not a fixed node-count rule. Do not grow
the custom controller into a general-purpose scheduler merely to avoid Kubernetes.

Keep inventory, model recipes, stable gateway contracts and acceptance evidence
independent of the launcher. A future backend can translate a recipe/placement
into Kubernetes resources while preserving the same tests and aliases. Prove it
on a spare capacity/test deployment before migrating the serving pair. Stateless
gateways are a simpler first experiment than the TP engine; OpenWebUI/database
availability still needs an explicit data and storage plan under either system.

## Recovery and availability plan

Local implementation exists; physical acceptance and enablement remain separate
gates. Follow [LOCAL_FIRST_OPERATIONS_PLAN.md](LOCAL_FIRST_OPERATIONS_PLAN.md)
for runnable command contracts, lifecycle, deadlines and policy schema, and
[SPARK_RECOVERY_GOAL.md](SPARK_RECOVERY_GOAL.md) for verification evidence and
remaining acceptance. Local development does not install controllers, enable
live routes or authorize scheduled hardware faults.

**Phase 1 — observer, recovery evidence and combined monitoring.**
Implemented bounded, strictly read-only partial-result observation from the Mac
without mutation locks. It preserves exact saved-plan digests, pinned artifacts, owners,
worker IDs, installed releases, route generations and private backup locations.
The Prometheus/Grafana dashboard correlates request latency,
TTFT/tokens, queue/active/KV, supported GPU/unified-memory, and physical fabric
views. Show unavailable metrics as unavailable. Map both logical interfaces to
the one physical cable; it distinguishes per-direction measured rates, negotiated
speed, RDMA hardware counters, transport/errors and recovery events. No saturation
benchmark or profiler was run on the live service. The dashboard and native engine
collection were exercised read-only in disposable local services; persistent
installation and live ingress/recovery feeds remain uninstalled.

**Phase 2 — authority, stable ingress and safe automatic recovery.**
The implemented authority and guarded stable ingress target the selected awake
Mac. That Mac is an uptime dependency. Strict named aliases remain strict;
the implemented opt-in `local-auto` policy is **not enabled**. Once qualified it
may substitute an accepted existing single-node recipe and must expose the
real backend, capabilities and smaller limits. Different recipes are not a
replica group. Use the existing tool-enabled `coder` placements as agent fallback
candidates after per-node acceptance; use `fast` only for explicitly text-only
policy. Historical coder tools/SSE evidence is e8f1 serving through both gateways
with 66f1 absent, not coder66 acceptance. Fast text/SSE exercised both nodes;
balanced 14B acceptance on e8f1 does not qualify its pending 66f1 placement or
add tool support.

Withdraw the failed TP2 route generation before fallback. Prove durable authority,
all managed ingress fencing and the survivor's ownership/epoch gate, then stop
only its owned TP worker, verify GPU idle and admit its accepted single-node plan.
Keep an unreachable node quarantined with a reservation tombstone; do not wait
for its acknowledgement merely to use a proven-safe survivor, and do not free or
reschedule its GPU. If these fences cannot be proved, fail closed within the
configured bound and alert. Independent trusted management and serving paths
are required for cable-loss recovery; never move NCCL onto Wi-Fi.

Automatic failback is required after the returned node **and fabric** remain
ready for the configured stability interval. Reconcile stale owned work on the
returning node before readmission. Stage disk/network/artifact checks while
fallback serves, then apply global admission/drain across all managed ingress,
with an explicit deadline policy. Gateway-local in-flight counters alone are
not a global drain. Stop fallback to free the GPUs, restore the exact pinned
preferred TP2 saved plan, verify operational inference readiness, and publish a
new route generation. Both GPUs are needed by TP2, so there is a service gap,
not warm overlap or zero downtime. Failed restoration rolls back to the saved
accepted fallback with bounded backoff/circuit breaking. Operator maintenance
pause/disable remains available; returning a node does not require a routine
manual failback command.

Gateway availability does not make every application redundant. Fam-Chat
currently calls OpenWebUI on 66f1; that hop and its database remain a dependency
even if another Context Guard is healthy. Plan application-state availability
separately while retaining existing family-chat history/reply settings.
An interrupted stream fails explicitly and is never silently replayed; neither
generation/tool requests nor KV state migrate to the replacement engine.

**Phase 3 — symmetric hardware qualification (scheduled serving interruption).**
Use synthetic requests and an exact owned test deployment only in an approved
maintenance window with independent management access and bounded rollback.
Never disable the operator's only recovery path. Cover either failed host,
either coordinator placement, cable loss and return; a one-sided receipt is not
symmetric acceptance.

| Fault | Required observed behavior | Recovery proof |
| --- | --- | --- |
| Authority/controller exits or competing controller starts | Durable ownership and epochs prevent duplicate starts; ingress fails closed if authority cannot be established | Resume idempotently from durable state; test restart and dropped replies without repeating destructive actions |
| Gateway process stops | Managed ingress uses only the active verified generation; no replay of interrupted requests | Strict aliases/credentials preserved; policy alias exposes actual fallback identity and limits |
| One TP rank exits | Withdraw the failed TP group; explicit client errors, no false `[DONE]` | Fence/stop survivor's owned rank, admit its qualified fallback, then automatically restore exact TP2 after stable readiness |
| Fabric path/cable is lost | No Wi-Fi NCCL fallback; opt-in single-node policy may serve over the independent serving path | Prove cable-free single admission and reachability, restored direct RDMA stability and automatic failback |
| Either host disappears | No unfenced second owner; unreachable GPU remains quarantined | Survivor fallback without dead-host acknowledgement only with proven route/ownership fences; reconcile returned host before automatic failback |
| Host reboots or returns intermittently | Stale lease is not free capacity; readiness hysteresis prevents oscillation | Clean/reconcile stale owned work, test flapping, stable-return failback and maintenance pause |
| Different route/config versions | Mismatched generation or deployment is refused | All managed ingress enforces the authority generation before publication/reuse |
| TP2 restoration fails after fallback drain | No empty-success response or speculative second owner | Bounded exact fallback rollback, backoff/circuit-breaker event and visible operator status |

Measure detection, client errors, drain/cleanup/start gaps, rollback, and first
successful response after fallback and failback. Record observed bounds rather
than promise them in advance. Exercise text/SSE and each advertised capability;
do not require tools/reasoning from a text-only fallback or silently remove
requested capabilities. Verify no orphan owned workers, duplicate owners,
leaked reservations or unrelated process termination.

**Phase 4 — operating release, enrollment and recipe expansion.**
Enable automation only after the main plan's acceptance gates and scheduled
symmetric hardware receipts pass. Document setup/reset, pause and coordinator
switching with owned scope and saved-state preservation. Coordinator switching
requires a new qualified placement/full-group restart, not live rank promotion.
Then qualify additional nodes and recipes through the enrollment workflow above.
One TP2 group is one replica; GLM's four scheduler slots are four total, not eight
across two ranks. Extra replicas require real free GPUs, and context, compute and
fabric still constrain capacity. Do not widen the DeepSeek publication gate or
infer TP3/TP4 qualification from generic inventory support.
