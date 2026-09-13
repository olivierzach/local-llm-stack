# Adding Sparks and recovering from failures

Design/qualification plan, September 12, 2026. This document does not enable
failover, change network settings or qualify TP3. Current serving evidence is in
[DEEPSEEK_TP.md](DEEPSEEK_TP.md); the current implementation checklist is
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

**No, not by this pinned checkpoint/runtime.** The running
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
| Model API from peer gateway | 10.10.20.x:8123 for current DeepSeek | Explicit backend address and reachability; HTTP traffic is not a tensor collective |
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

## Automatic node enrollment — deferred

Requested September 12: document this direction, but hold implementation. The
target is one idempotent enrollment command, not a custom code update for each
Spark. The following is a proposed interface, **not an available command**:

```text
sparkctl node enroll --ssh spark-new --topology TOPOLOGY --check
sparkctl node enroll --ssh spark-new --topology TOPOLOGY --apply
```

Discovery can suggest a machine; it must not automatically trust a discovered
host or start a GPU workload. Enrollment should:

1. Verify the SSH host identity and collect architecture, GPU/driver, storage,
   NIC/RDMA and network information without reading private credentials.
2. Produce a reviewable inventory/bootstrap diff, including explicit management
   and fabric paths; require necessary administrative access for OS changes.
3. Install the pinned controller and functional tool baseline, merge managed
   peer public keys idempotently, and validate connectivity to existing members.
4. Stage selected model/runtime artifacts over the approved fabric with capacity
   checks, resumable copies and content verification. Do not copy every model by default.
5. Mark capabilities as detected, prepared or qualified. Run GPU acceptance only
   when admission allows it; enrollment must preserve active deployments.
6. Make the new member selectable by existing placement commands. Publish a model
   alias only after that exact deployment passes acceptance. Never enlarge an
   existing live TP group automatically.

Re-running enrollment must converge without replacing unrelated SSH/config
entries. A failed run needs resumable steps and an enrollment-owned rollback;
removal must refuse an active member. CPU fixture tests should cover a third node,
mixed software versions, missing sudo, unreachable peers and partial staging.
Hardware acceptance then proves the same path on a real added Spark. This is the
one-time investment that makes later nodes a data/configuration change.

## One-time controller generalization

| Work package | Present limitation | Deliverable and acceptance |
| --- | --- | --- |
| Inventory v2 with v1 compatibility | One `ssh` alias and an undifferentiated fabric list | Separate management, serving and bulk/collective policies; network/subnet identifiers, link peers and per-peer interface paths; v1 plans remain unchanged |
| Node enrollment | `configure-spark-peer-ssh.py` explicitly requires two nodes; `peer_ssh.py` writes one peer block | Idempotent N-node enrollment, merge all managed peer blocks, trusted public keys and source restrictions, no private-key copying; tests for adding/removing a peer without clobbering existing entries |
| Topology preflight | Copy helper pairs interfaces by rail index; profile tool assumes two nodes/two rails | Reachability matrix across all participating nodes, physical-to-logical interface map, switch/direct topology checks, GID validation, measured link performance |
| Model compatibility | Generic GPU-count check permits mathematically invalid TP3 | Check pinned checkpoint/runtime tensor constraints before GPU reservation; recipes declare qualified TP/PP/speculation combinations; unknown combinations are candidates, not live aliases |
| Placement generation | Core renderer accepts N nodes, but convenience manifests/Make selectors enumerate the pair | Deterministic deployment generation from recipe + node list + coordinator; no host-specific code for new inventory members |
| Publication evidence | DeepSeek gate requires exactly TP2 and both coordinator roles | Keep existing TP2 gate; add a separate N-node acceptance contract requiring every allowed coordinator role, all-rank health and exact recipe/runtime/topology identity |
| Operational state | Node-local reservations prevent many conflicts; no qualified multi-host fencing under partitions | Durable deployment generation/owner identity, conservative recovery and explicit fencing before rescheduling an unreachable owner |
| User entrypoint | Existing URL can depend on one Spark | Stable independently hosted frontend or explicit client-side gateway selection, synchronized route policy and compatible credentials; same model aliases |

Generic orchestration should be changed once for these capabilities. Adding a
normal node afterward changes inventory/enrollment data; selecting a model group
changes deployment data. New model architectures, engines or unsupported network
topologies still need engineering and qualification. This is not a proposal to
adopt Kubernetes or a new scheduler just to run three Sparks.

## Recovery and availability plan (item 4)

**Phase 1 — define failure semantics and preserve recovery state (no GPU disruption).**
Record the live plan digest, worker IDs, controller revision, gateway registries,
private credential-backup locations and baseline health. Store equivalent
recovery material on the Mac and both nodes. Add a recovery command that reports
whether each rank is alive, stopped or unreachable and refuses to release a
reservation on an ambiguous owner. State transitions must be logged and bounded.

**Phase 2 — stable gateway entrypoint (CPU implementation and synthetic tests).**
Choose an entrypoint independent of the compute coordinator, such as a small
frontend on the Mac Mini. Keep client model aliases and route budgets stable.
Health checks must verify the requested deployment/model contract, not just a
listening port. New requests can use another healthy gateway; an in-flight
stream fails explicitly and is never silently replayed. Test authentication,
virtual-key policy/accounting, mismatched registry versions, dead backends,
timeouts and recovery without real family messages.

Gateway redundancy does not make every application redundant. Fam-Chat currently
calls OpenWebUI on 66f1; that hop and its database remain a dependency even if a
second Context Guard exists. Plan its availability separately (or later use a
stable guarded API directly) while retaining the user's requested family-chat
history/reply settings. No app migration is authorized or performed by this plan.

**Phase 3 — owned hardware fault campaign (scheduled serving interruption).**
Use synthetic requests and an exact owned test deployment. Arrange independent
management access and a bounded/automatic rollback before any link manipulation.
Never disable the only interface through which the operator can recover a node.

| Fault | Required observed behavior | Recovery proof |
| --- | --- | --- |
| Controller process exits | Workers/reservations retain identity; no duplicate deployment starts | Reattach from either controller using the saved plan |
| Gateway process stops | Other gateway remains healthy; stable frontend routes only new requests | Same alias/credentials/limits and successful tool continuation on a fresh request |
| One TP rank exits | Group becomes unavailable; clients receive a visible failure, no false `[DONE]` success | Stop/fence remaining owned rank, relaunch exact recipe, republish only after healthy |
| Fabric path is lost | No fallback to Wi-Fi or another model; failure is bounded and visible | Restore link, inspect all ranks, perform owned cleanup/restart and verify direct RDMA again |
| Coordinator host disappears | No unfenced second owner; an orphan rank cannot independently reclaim GPUs | Confirm host/rank stopped or fenced before a fresh deployment; verify both role choices |
| Worker host reboots | Stale lease is not treated as a free GPU automatically | Reconcile owner/generation, restore service deliberately, verify old process absent |
| Different route/config versions | Mismatched deployment is refused, not silently substituted | Atomically publish the verified registry generation on both gateways |

Measure detection time, client error time, cleanup/restart time and the first
successful response after recovery. Establish observed bounds; do not promise
instant failover or resumption of a partially generated answer. KV state and a
partially emitted/tool-using conversation cannot simply move to another TP group.

**Phase 4 — acceptance and operating release.**
Run real text, SSE, tools and on/off reasoning after every recovery scenario;
repeat both coordinator roles. Verify no orphan containers, duplicate owners,
leaked leases or unexpected traffic. Update the current checklist with receipts
and a tested recovery command, then qualify an additional node using the same
workflow. Do not widen the live DeepSeek publication gate ahead of that evidence.
