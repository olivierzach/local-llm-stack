# Multi-Spark serving and recovery runbook

This is an operator procedure, not permission to change a host or a passing
qualification receipt. Begin with [connections and host prerequisites](SPARK_CONNECTIONS_RUNBOOK.md).
Use [single-node operation](SPARK_SINGLE_NODE_RUNBOOK.md) for each independent
fallback. Keep [emergency/offline restoration](SPARK_RECOVERY_RUNBOOK.md) available
on a machine that does not depend on the model being stopped.

**Recorded rollout boundary:** revision
`65b05d5503e6ffefd2f07d07ebe86333917d5086` is published and installed on the Mac.
All four new Mac launchd roles ran independently with PPID 1; the authenticated
gateway reported `accepting: false`, `active_requests: 0`, and recovery remained
disabled at epoch 0. All four roles are now confirmed stopped, with their plists,
config and state retained. Inspect status again before a future operation.
No Spark worker was changed and no inference/fault campaign was performed in this
rollout. Automatic recovery is not live-qualified or enabled. All GPU changes and
faults remain paused. Historical GLM serving qualification is not qualification of
the new authority, ingress or cable-loss policy.

## 1. Safety, bindings and identities

Command classifications in this document:

- **Inspection:** metadata, bounded network diagnostics or local rendering. An
  output file is still a local write. A filesystem/cache check is not inference.
- **Staging:** approved image/model/controller/SSH writes; no GPU launch, but
  substantial disk/network use and sometimes CPU-only containers.
- **Network/admin disruptive:** cables, addresses, routes, MTU, ACLs and service
  hosting. Requires an operator, independent console/management and rollback.
- **GPU-disruptive / inference:** admission, owned stop/start, qualification and
  collectives. Requires a separately approved idle/maintenance window.

Do not execute a later class because an earlier class succeeded. Record the
operator, interruption, abort deadline and restoration allowance. Run stop/start
under an independent supervisor, not an agent whose only model is the one stopped.
Never clear reservations, remove fences, prune Docker, kill guessed PIDs or reset
an existing checkout to make admission succeed.

Bind paths explicitly in each shell. The following names are a **portable local
layout**, not a claim these files exist; populate them through the foundation and
recovery runbooks from trusted source and private retained artifacts. `SOURCE`
must be a fresh trusted checkout, never the running stack checkout. Read/review
private environment files before sourcing them.

```bash
SOURCE="$HOME/src/local-llm-stack-trusted"
REVISION=65b05d5503e6ffefd2f07d07ebe86333917d5086
PREFIX="$HOME/projects/local-llm-stack-cluster"
RELEASE="$PREFIX/releases/$REVISION"
PY="$RELEASE/.venv/bin/python"
INVENTORY="$PREFIX/state/site/inventory.json"
TRUST="$PREFIX/state/site/peer-trust.json"
PLAN="$PREFIX/state/preserved/glm53-e8f1/plan.json"
PLAN_SHA256=69aa5dc93ea3b1fafe73cf2dfaaed43973dabcca9d01f3c10ff19d5eb4a3cf4e
umask 077
EVIDENCE=$(mktemp -d "$PREFIX/state/multi-spark.XXXXXX")
```

The example `PLAN_SHA256` is valid **only for the original preserved GLM plan**.
A new plan must use its own canonical full-plan hash from `sparkctl render` or
`config.plan_sha256`, independently recorded at admission. Do not substitute
`shasum plan.json`: raw JSON file bytes and canonical full-plan hashes differ.
Raw SHA-256 is appropriate for receipt references, not plan references.

| Original GLM restore identity | Recorded value |
| --- | --- |
| Plan on each Spark | `~/projects/local-llm-stack-cluster/state/glm53-20260913/e8f1-04/plan.json` |
| Owner | `glm53-tp2-256k-dflash2-e8f1-b1cf7901f81c` |
| Deployment digest | `b1cf7901f81c861758f5f7c14ba7cea5605519713c4fc4816f1e06af41eb3bb2` |
| Canonical full-plan SHA-256 | `69aa5dc93ea3b1fafe73cf2dfaaed43973dabcca9d01f3c10ff19d5eb4a3cf4e` |
| Placement / endpoint | e8f1 coordinator, 66f1 worker; `http://10.10.20.2:8125/v1` |
| Strict alias / original limits | `local-glm53-flash`; 262,144 context, 8,192 output, one image |

Copy and preserve the real saved plan and its artifacts; a table cannot recreate
it. The current recipe permits four images. Re-rendering
`glm53-tp2-256k-dflash2-e8f1.json` does **not** restore the old one-image plan.
Do not edit its addresses, Compose, ranks or coordinator. Keep the original
checkouts, other models, gateway secrets, databases, research jobs and receipts.

## 2. Choose the compute shape before wiring

**Independent replicas:** each node loads a complete model and serves independent
requests. Matching `coder-66f1.json` / `coder-e8f1.json`, or matching
`fast-66f1.json` / `fast-e8f1.json`, can form a gateway `--replicas` group after
per-node acceptance. This increases request capacity, not the memory available to
one request. Different recipes, contexts or capabilities are not interchangeable
replicas. See [CLUSTER replica operation](CLUSTER.md#independent-replicas-and-request-level-data-parallelism).

**Sharded TP2:** one logical engine occupies one GPU on each Spark. Each node
retains a full pinned model snapshot for loading/reproducibility, but GPUs execute
shards. Either missing rank or required collective link makes the group unavailable.
The coordinator hosts API/rendezvous for that deployment, not a permanent cluster
master. TP × PP must equal allocated GPUs; that arithmetic alone does not establish
checkpoint/runtime compatibility. Two occupied TP2 GPUs cannot also host warm
single-node fallbacks.

## 3. Wire and identify three distinct traffic paths

Complete the foundation's power, Internet/Wi-Fi, Mac Ethernet and host-key steps
first. Label physical ends with node, chassis port and cable ID. Compare serials,
port labels/MACs, `ethtool -i`, `rdma link show` and link state; do not identify a
socket solely from a Linux interface's spelling.

Recorded two-node map (not defaults for another installation):

| Purpose | Mac / 66f1 | e8f1 | Physical dependency |
| --- | --- | --- | --- |
| Management and independent serving | Mac `en0` `10.10.10.1/24`; 66f1 `enP7s7` `10.10.10.2/24` | `enP7s7` `10.10.10.3/24` | Ordinary Ethernet path reaching each Spark independently, normally a shared management switch; do not chain e8f1 through 66f1 |
| Collective logical rail 0 | 66f1 `enp1s0f0np0`, `10.10.20.1`, HCA `rocep1s0f0` | `enp1s0f0np0`, `10.10.20.2`, HCA `rocep1s0f0` | One directly connected QSFP cable |
| Collective logical rail 1 | 66f1 `enP2p1s0f0np0`, `10.10.21.1`, HCA `roceP2p1s0f0` | `enP2p1s0f0np0`, `10.10.21.2`, HCA `roceP2p1s0f0` | The same physical QSFP cable, not a second 200-Gb/s link |
| Internet / optional alternate management | `wlP9s9`, DHCP, default via `192.168.1.1` | Same policy | Wi-Fi/AP and upstream router; last observed `.31` / `.18` are not stable addresses |

For the pair, the repository MTU tool expects profiles `spark-link-20` and
`spark-link-21` with `/30` addresses on both ends. Fresh retained observation
confirms `.1/30` on both 66f1 rails and `.2/30` on both e8f1 rails.
`cluster/inventory.json` records IPs but **no masks**. Inspect actual address/profile
masks before any change; a mismatch is a maintenance planning gate, not an
invitation to alter a live collective link. `/30` has exactly two usable hosts.

[NVIDIA's ConnectX-7/Spark cabling guide](https://docs.nvidia.com/dgx/dgx-spark/spark-clustering.html)
explains physical/logical mapping. Verify the actual machine's port map before
connecting. Ethernet-only QSFP operation does not turn each PCIe/logical rail
into another physical cable or double negotiated capacity.

### Inspect both ends without starting a stress workload

Run these **inspection** commands locally on each Spark over its already trusted
management session. Save output privately with node/time labels. No commands here
alter routes, interfaces or GPU state.

```bash
hostname
ip -br address
ip -d link show enP7s7
ip -d link show enp1s0f0np0
ip -d link show enP2p1s0f0np0
nmcli -f NAME,UUID,TYPE,DEVICE connection show
nmcli connection show spark-link-20
nmcli connection show spark-link-21
ip -4 route show
ip -6 route show
ip route get 1.1.1.1
ethtool enp1s0f0np0
ethtool enP2p1s0f0np0
ethtool -i enp1s0f0np0
ethtool -i enP2p1s0f0np0
rdma link show
ibv_devinfo
```

The Internet route must use the approved ordinary Internet uplink, not a fabric
default. Confirm DNS and Internet separately as in the foundation. Pulling images,
locked Python wheels or uncached models requires Internet; cached inference does
not become Internet-dependent merely because preparation needed downloads.

On 66f1, bounded **diagnostic traffic**, not saturation testing:

```bash
ip route get 10.10.20.2 from 10.10.20.1
ip route get 10.10.21.2 from 10.10.21.1
ping -c 3 -W 2 -I enp1s0f0np0 10.10.20.2
ping -c 3 -W 2 -I enP2p1s0f0np0 10.10.21.2
```

On e8f1 use the reverse destinations and source addresses. Each route must be
on its corresponding interface with the intended source, without gateway, jump
host or Wi-Fi fallback. Check IPv4 RoCE-v2 GIDs for those addresses/HCA ports.
Ping establishes IP reachability only: not RDMA correctness, NCCL selection,
model readiness or measured bandwidth. `NET/IB` logs and all-rank send/receive
counters during a separately approved workload establish more than
`NCCL_IB_DISABLE=0`, which merely permits RDMA.

### Stable addresses and safe network rollback

Use an approved administrator and an independent console/management path to
provision persistent NetworkManager profiles. Before any change retain the exact
profile UUIDs/settings, active addresses/routes/MTUs and a private backup of the
relevant connection files. Do not capture or publish Wi-Fi secrets. Record exact
reversal commands for the specific changed profiles; never restore every profile
or flush every route. Retain a reachable console until both nodes pass post-change
checks and the operator accepts persistence.

For a **new, otherwise unconfigured** pair only, after confirming these names and
interfaces are not already managed, the intended profile construction on 66f1 is:

```bash
# NETWORK/ADMIN DISRUPTIVE: only on a new idle 66f1, after approval.
sudo nmcli connection add type ethernet con-name spark-link-20 \
  ifname enp1s0f0np0 ipv4.method manual ipv4.addresses 10.10.20.1/30 \
  ipv4.never-default yes ipv6.method disabled connection.autoconnect yes
sudo nmcli connection add type ethernet con-name spark-link-21 \
  ifname enP2p1s0f0np0 ipv4.method manual ipv4.addresses 10.10.21.1/30 \
  ipv4.never-default yes ipv6.method disabled connection.autoconnect yes
sudo nmcli connection up spark-link-20
sudo nmcli connection up spark-link-21
```

For a new e8f1, use the same interfaces/profile names and `.2/30` on each subnet.
No gateway/DNS/default route belongs on these profiles. On existing hosts, reuse
reviewed profiles instead of creating duplicates; preserve IPv6 policy unless the
approved new-link design explicitly disables it. If initial provisioning fails,
deactivate/remove **only the newly created profiles with their recorded UUIDs**,
and restore any affected previous connection through the console. For edits to an
existing profile, restore its recorded settings/active state instead; deletion is
not rollback. Recheck both source-specific peer routes and the Internet default.
Validate reconnect/reboot persistence only in an approved outage window.

There is no unattended sudo on the Mac or either Spark. Missing administrative
provisioning is a real blocker. Docker root access is not a workaround for host
network/firewall/SSH administration.

### MTU convention and bounded diagnostics

On each Spark, bind `RELEASE` to that host's installed pinned release as in section
1, then inspect:

```bash
bash "$RELEASE/scripts/configure-spark-fabric-mtu.sh" --check
```

The tool deliberately rejects unexpected hostname/profile/interface/address
mapping. It supports only this pair, not generic N-node provisioning. `--apply`
sets Ethernet MTU **9000**, both configured and active; `--rollback` restores
values captured in `/var/lib/local-llm-stack/fabric-mtu-backup.tsv`. Old performance
notes describe MTU 1500 and do not prove today's setting. Both ends must agree;
active verbs MTU is a separate quantity to inspect.

```bash
# NETWORK/ADMIN DISRUPTIVE: both nodes idle, approved window only.
sudo bash "$RELEASE/scripts/configure-spark-fabric-mtu.sh" --apply
# Reversal on each changed host, only with its inspected retained backup:
sudo bash "$RELEASE/scripts/configure-spark-fabric-mtu.sh" --rollback
```

The script refuses active compute processes. Do not stop them just to run a check.
Do not run apply followed immediately by rollback as a routine diagnostic. Retain
only a setting that passes separately approved correctness/error checks. A small
DF ping can check packet size after approved MTU change; a successful ping is not
performance acceptance. `profile-spark-fabric.py` sends host-memory RDMA traffic,
including concurrent rails; it is **not** a passive link check. `sparkctl collectives`
and `stress-spark-collectives.py` use GPU resources. Neither belongs in this paused
rollout or on an occupied inference GPU without a separate approved experiment.

## 4. Inventory, independent SSH and serving reachability

Use a private version-1 inventory. Keep immutable old plans separate from new
inventory. `management` accepts `{"ssh_targets": ["spark-66f1-wired"]}` on 66f1
and the corresponding e8f1 alias on e8f1. Independent fallback `serving` fields are
`{"address":"10.10.10.2","interface":"enP7s7"}` and
`{"address":"10.10.10.3","interface":"enP7s7"}`. Fabric remains the table above.
These fields describe the inventory schema, not permission to change an existing
recovery plan's node identity. For fallbacks paired with the original GLM plan,
preserve every saved node field except the permitted added `serving` binding;
put independent SSH selection in policy `management` overrides. Adding a new
`management` field to those fallback nodes while the preferred nodes lack it
fails the exact-identity comparison. A preferred plan that already specifies
`serving` requires the fallback to retain that exact binding.
These are reviewed configuration inputs, not commands that create addresses.

`spark-66f1-wired` must independently reach `10.10.10.2` and
`spark-e8f1-wired` must independently reach `10.10.10.3` as `statsparrot`. Reject an
old e8f1 `ProxyJump` through 66f1. `.local` names are separate LAN/Wi-Fi alternatives
only after verified resolution, route and pinned identity. Authorize alternate
source addresses explicitly; an observed DHCP address or DNS name is not durable
trust. The foundation owns discovery, public-key installation and Mac SSH aliases.

For controller operation from either Spark, independently verify each Ed25519
public host key through console/out-of-band records and place it in `TRUST`:
version 1, `nodes` keyed by **every** participating node, each with `host_key`.
Optional node `management` is a list of `{alias,address}` and `source_addresses`
is an explicitly approved list. Do not turn `ssh-keyscan` output from an untrusted
connection into verified trust.

```bash
# INSPECTION: authenticated checks, no peer SSH writes.
"$PY" "$RELEASE/scripts/configure-spark-peer-ssh.py" \
  --inventory "$INVENTORY" --trust "$TRUST"
# STAGING/SSH ADMIN: only after independent key/source review and approval.
"$PY" "$RELEASE/scripts/configure-spark-peer-ssh.py" \
  --inventory "$INVENTORY" --trust "$TRUST" --apply
```

Private controller keys stay on their origin machines. The tool pins handshakes,
merges managed aliases/public keys, restricts forwarding to peer loopback 4110,
disables agent forwarding/multiplex reuse and retains interrupted-write rollback
evidence. An ambiguous write requires reconciliation, not deleting the SSH journal.
Source restrictions must cover approved independent management paths; working
fabric SSH alone does not qualify cable-loss recovery. Model copies use the
separate `spark_transfer` source-bound direct fabric policy and must not silently
fall back through a gateway, jump host or Wi-Fi.

Verify three paths independently:

1. Mac → **each** Spark SSH; Spark → peer SSH if peer control/copy is required.
2. Every intended gateway → exact saved API and tokenizer endpoint, authenticated
   gateway → client, and all relevant IPv4/IPv6/forwarded bypasses.
3. Every distributed rank → coordinator rendezvous and every required fabric rail.
   GLM uses API 8125 and rendezvous 29545; read another plan's actual ports rather
   than inheriting these. NCCL/Gloo/bootstrap stay on the declared fabric.

The Mac currently routes legacy `10.10.20.2:8125` through its ordinary default;
this is **not a proven serving path**. Do not render a new GLM plan to conceal that
problem. The foundation/recovery runbooks describe the separately approved,
reversible candidate host route via `10.10.10.3`. Confirm actual endpoint reachability,
identity, ACL exposure and persistence afterward. A modern single fallback binds
to independent `10.10.10.x`; the old preferred plan remains fabric-bound. A route
success is not a passing isolation or readiness receipt.

## 5. Stage the pinned runtime and complete snapshots on both nodes

Install the pinned controller on each Spark using the foundation's trusted bundle
procedure. Record full revision, immutable release, Python/dependency lock and
installer receipt. Administrative package/driver/Docker/NVIDIA runtime/RDMA tool
provisioning is separate; `spark-node` reports missing prerequisites, it does not
install host packages. Reconcile versions rather than blindly upgrading drivers.
Verify free storage for complete target/draft snapshots, runtime images/overlays,
loading headroom and retained caches on **each** node. Keep reservations untouched.

**Inspection from the controller**, for a new candidate (not an old-plan restore):

```bash
DEPLOYMENT="$RELEASE/cluster/deployments/glm53-tp2-256k-dflash2-e8f1.json"
"$PY" "$RELEASE/scripts/spark-node" discover --inventory "$INVENTORY"
"$PY" "$RELEASE/scripts/spark-node" check --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT"
```

The existing GLM preparation tool stages both nodes using real manifests
`cluster/model-downloads/glm53-w4a16.json`, `glm53-dflash2.json`, image lock
`cluster/glm53-images.lock.json` and `cluster/runtime-overlays/glm53/patches.json`.
Run on **66f1**, with its local bindings; use `--peer 66f1` when running on e8f1:

```bash
# INSPECTION: prints manifests and total download size, no apply.
"$PY" "$RELEASE/scripts/prepare-glm53-tp.py" \
  --peer e8f1 --output "$EVIDENCE/glm-preparation"
# STAGING: explicit approval for downloads, cache writes and bulk fabric copy.
"$PY" "$RELEASE/scripts/prepare-glm53-tp.py" \
  --peer e8f1 --output "$EVIDENCE/glm-preparation" --apply
```

Important constraints: this tool uses its release's checked-in pair inventory,
not `--inventory`; it invokes the peer runtime helper through the peer's `current`
link. Before approved use, verify both nodes' installed selection resolves to the
reviewed pinned release and its inventory matches the intended pair. Do not use
it for arbitrary nodes/topology. It pulls immutable images, checks pinned ARM64
image IDs, hashes model files, copies over the verified fabric and builds/checks
runtime patches in CPU-only containers. Its bounded download budget is derived
from the manifests. It does not start an inference worker. Review its
`preparation.json`, per-model locks/copy receipts and runtime outputs; failure or
`complete: false` is not preparation success. Match every artifact to the **saved
plan**, not just to today's recipe; otherwise use the original pins/offline restore
procedure instead of silently replacing them.

For retained GLM caches, full file verification without a download can run on
each node (heavy disk reads, no inference). Bind `CACHE` to that node's inventoried
path; retain distinct output locks:

```bash
CACHE=/home/statsparrot/projects/local-llm-stack/data/huggingface
"$PY" "$RELEASE/scripts/fetch-pinned-spark-model.py" verify \
  --manifest "$RELEASE/cluster/model-downloads/glm53-w4a16.json" \
  --cache "$CACHE" --lock-output "$EVIDENCE/glm-target-verified.json"
"$PY" "$RELEASE/scripts/fetch-pinned-spark-model.py" verify \
  --manifest "$RELEASE/cluster/model-downloads/glm53-dflash2.json" \
  --cache "$CACHE" --lock-output "$EVIDENCE/glm-draft-verified.json"
```

Compare these manifest pins against the saved plan first. A `cache-status`,
preflight, existence/size check or `prepared: true` alone does **not** attest every
checkpoint byte. Keep full SHA-256 verification evidence and overlay/library pins.
For other models use their real recipe/download manifest; the single-node runbook
covers coder fallback staging and its exact per-node plans.

Approved aggregate preparation evidence can be produced after actual staging:

```bash
# STAGING-EVIDENCE WRITE: approved --apply; no pull/download flags here.
"$PY" "$RELEASE/scripts/spark-node" prepare --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT" --timeout 240 \
  --evidence-output "$EVIDENCE/prepared.json" --apply
```

This renders from the selected deployment and is **not** a saved-plan attestation
for a different historical render. For the original GLM plan retain the original
artifact producers and explicit verification against its pins. An aggregate
receipt is useful only for the nodes/artifacts it actually checked. Require a
successful command and `prepared: true`; an evidence-write failure may carry
`requires_reconciliation: true`, not permission to repeat blindly.

## 6. Save and start an exact distributed plan manually

For a **new** two-node candidate, validate/render without GPU mutation:

```bash
DEPLOYMENT="$RELEASE/cluster/deployments/fast-tp2-e8f1.json"
"$PY" "$RELEASE/scripts/sparkctl" validate --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT"
"$PY" "$RELEASE/scripts/sparkctl" render --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT" --output "$EVIDENCE/candidate-render"
```

`fast-tp2.json` selects 66f1 coordinator; `fast-tp2-e8f1.json` selects e8f1.
`fast-pp2.json` and `fast-pp2-66f1.json` are distinct PP placements, not live rank
changes. Stage the selected candidate's own artifacts first. Record the emitted
`plan_sha256`, inspect all per-node Compose and endpoint fields, and bind `PLAN`
to the resulting immutable `plan.json` and `PLAN_SHA256` to **that** hash before
activation. Do not reuse the GLM hash above. For restoration skip rendering entirely
and use the preserved original bindings.

Before maintenance, record bounded partial observation rather than treating
`status`/`preflight` as mutation-free observers:

```bash
# INSPECTION: exclusive output FILE, partial failures retained.
"$PY" "$RELEASE/scripts/sparkctl" observe --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --timeout 15 --output "$EVIDENCE/before.json"
```

Proceed only when all required hosts, SSH identities, pins, serving/fabric paths
and GPU ownership are reconciled, the relevant clients are drained/isolated, and
there is approved restoration time. `preflight` does not itself stop other jobs
and is not a full checkpoint hash check. An active recovery fence requires the
[recovery workflow](SPARK_RECOVERY_RUNBOOK.md), not manual force, a new authority,
or fence deletion. Do not race the recovery daemon with manual commands.

```bash
# MAINTENANCE CHECK: node helpers/locks and evidence writes, no generation.
"$PY" "$RELEASE/scripts/sparkctl" preflight --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --output "$EVIDENCE/preflight"
# GPU-DISRUPTIVE + INFERENCE: exact restore/start, explicitly approved only.
"$PY" "$RELEASE/scripts/sparkctl" up --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --output "$EVIDENCE/activation" --timeout 7200
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256"
```

Fresh activation evidence must establish **both ranks**, not merely HTTP 200:
reservation owner and deployment digest match, exact immutable container IDs equal
reservation IDs, expected image digest/ID, running+healthy state, recorded start
and restart identities. `up` waits for those checks, sends a real coordinator
completion probe, and only then writes `acceptance.json` and ready `endpoint.json`.
A successful start is not full tool/vision/context/fault qualification. Partial
startup can leave retained ownership when cleanup is ambiguous; preserve receipts
and reconcile, never prune to make retry pass. The 7200-second command bound is
not a promised startup time.

Publish only the accepted strict model route. For existing GLM ingress preserve
and restore the recorded registries through the recovery runbook, rather than
replacing complete registries. For a separately approved new gateway use the
existing `spark-gateway` operation with repeated plans for every intended alias;
omitting routes when replacing a registry removes them. `attach` retrieves the
selected gateway's key/registry without reconfiguring it. Gateway keys remain
private. [CLUSTER gateway procedures](CLUSTER.md#independent-gateways-and-client-contexts)
cover peer gateways, tunnels and authenticated clients. Full new GLM publication
uses [its two-role qualification procedure](GLM53_TP.md#qualification-and-publication),
not a raw HTTP probe.

Planned manual shutdown first stops new ingress and drains through the approved
client/gateway workflow. `sparkctl down --saved-plan "$PLAN"` immediately stops
owned workers/requests; it is not graceful drain. If a recovery authority owns
fences, use its controls instead. No manual coordinator rank swap is supported:
switching coordinator requires a separately rendered, qualified exact placement
and stop/start, or the controller's gated `switch-coordinator` workflow.

## 7. Host the gateway and recovery authority independently on the Mac

The selected **awake Mac** hosts one authority and the dedicated authenticated
policy gateway; Spark-side gateways are not automatically part of its global drain.
The Mac, its logged-in service user, power and network are a **single point of
failure**, not HA. LaunchAgents outlive OMP/VS Code/terminals, not logout/sleep or
power failure. Optional `caffeinate` is explicit; services cannot physically power
on a Spark, reconnect a cable, repair Wi-Fi or wake an unpowered Mac.

Bind the actual approved private files before using service commands:

```bash
SERVICE_DIR="$PREFIX/state/mac-recovery"
SERVICE_CONFIG="$SERVICE_DIR/services.json"
POLICY="$SERVICE_DIR/policy.json"
STATE="$SERVICE_DIR/authority"
```

These are layout examples; use the actual reviewed config, never create a second
empty authority over a retained journal. Service config version 1 requires absolute
`release` (the full revision directory, not `current`), `revision`, release-local
`python`, `policy`, `service_dir`, child `state_dir`, and `inventory`. Optional
`plans`, gateway bind/port, monitor bind/port/interval/timeout and boolean `awake`
are explicit. Key/registry/route files must be distinct children of
`service_dir/ingress`; config/policy are 0600, directories 0700. Keep state outside
the immutable release and separate from ingress/logs. The recorded candidate uses
loopback gateway 19842 and monitor 9842; inspect ownership before binding either.

```bash
# LOCAL INSPECTION/RENDER: does not authorize installation.
"$PY" "$RELEASE/scripts/spark-services" validate --config "$SERVICE_CONFIG"
"$PY" "$RELEASE/scripts/spark-services" render --config "$SERVICE_CONFIG"
"$PY" "$RELEASE/scripts/spark-services" status --config "$SERVICE_CONFIG"
# MAC ADMIN/HOSTING: separate approval required; creates/starts disabled authority.
"$PY" "$RELEASE/scripts/spark-services" install --config "$SERVICE_CONFIG" --apply
"$PY" "$RELEASE/scripts/spark-services" start --config "$SERVICE_CONFIG" --apply
```

Do not restart the currently paused rollout merely because these commands are
printed. Stop/uninstall preserves GPU ownership, credentials and history; it is
not model cleanup. Monitor data collected over management SSH can work even when
the Mac cannot reach the saved model endpoint. Monitoring success therefore does
not qualify gateway serving.

## 8. Isolation first, then actual qualification, then disabled adoption

The implemented policy schema is version 1 with `id`, alias exactly `local-auto`,
`preferred: {path,sha256,qualification}`, ordered
`fallbacks: [{node,path,sha256,qualification}]`, node-keyed `management` transport
overrides, `gateway: {url,key_file,registry,route_state,isolation}`, and `timings`.
Use the exact original preferred reference and separately rendered independent
coder plans. Missing qualification/isolation may be `null` only during preparation.
The gateway URL is the origin, not `/v1`. See the
[implemented policy contract](LOCAL_FIRST_OPERATIONS_PLAN.md#policy-preparation-and-controls),
not that document's older proposed-field table.

1. **Approve and prove network/ingress isolation before sending qualification.**
   Start only the approved dedicated gateway and disabled authority. Verify its
   authenticated `/_spark/recovery` endpoint and closed admission. Enumerate both
   Sparks' legacy 4010/4110, direct 8125/8101, client tunnels/SSH forwards, IPv6 and
   every other entrypoint. Isolate all bypasses using an approved host-admin plan;
   retain exact registry, ACL, binding and route reversals. Do not flush firewalls
   or guess container stops. Verify independent management and each fallback's
   independent serving path. An unexcluded bypass blocks progress.
2. Retain real private JSON evidence and an independently approved isolation
   receipt with exactly: `version`, `policy`, `gateway_url`, `registry`,
   `route_state`, `plan_sha256`, `exclusive_ingress`, `legacy_endpoints_blocked`,
   `independent_management`, `independent_serving`, `approved_by`, `evidence`.
   The version is 1; paths are resolved absolute policy paths; `plan_sha256` is
   the list of full canonical admitted-plan hashes; `evidence` is a nonempty list
   of `{path,sha256}` references to actual JSON evidence, using raw-file hashes.
   Set booleans true only after proving them. Never copy a CPU fixture, invent
   evidence, or supply an operator name as a substitute for verification.
3. Update **only** `gateway.isolation` to its `{path,sha256}` raw-file reference
   and adopt while disabled/reconciled:

   ```bash
   "$PY" "$RELEASE/scripts/spark-recover" validate --policy "$POLICY"
   "$PY" "$RELEASE/scripts/spark-recover" init --policy "$POLICY" \
     --state-dir "$STATE" --adopt-receipts --apply
   ```

4. For **each exact plan separately** (preferred, coder on 66f1, coder on e8f1),
   retain real artifact producer and per-node verification receipts. Build a
   private `STAGING_REFERENCES` JSON object keyed by **exactly that plan's node
   IDs**; each value is `{path: ABSOLUTE_RECEIPT_PATH, sha256: RAW_FILE_HASH}`.
   It is a references map, not the aggregate `prepared.json` itself. A receipt
   may be referenced for multiple nodes only if it genuinely verified those
   nodes. Keep full weight-hash evidence separately where structural preparation
   does not provide it.
5. Obtain explicit GPU-maintenance/inference approval. Start the exact plan if
   absent, using the prior section's ownership/fence gates. `qualify` does **not**
   stage or start workers. TP2 and the single candidates cannot occupy the same
   GPUs simultaneously. Rebind `PLAN` and its independently admitted canonical
   `PLAN_SHA256` for each candidate; bind its real staging-reference map and a
   **new** output filename before the command:

   ```bash
   STAGING_REFERENCES="$EVIDENCE/staging-references.json"
   NEW_QUALIFICATION_RECEIPT="$EVIDENCE/qualification.json"
   # INFERENCE: disabled authority, approved isolation, exact workers running.
   "$PY" "$RELEASE/scripts/spark-recover" qualify --policy "$POLICY" \
     --state-dir "$STATE" --plan "$PLAN" --plan-sha256 "$PLAN_SHA256" \
     --staging "$STAGING_REFERENCES" --output "$NEW_QUALIFICATION_RECEIPT" \
     --rounds 1 --apply --approve-inference
   ```

   Use a fresh evidence directory/map/output for the next plan; do not overwrite
   the prior receipt. The command excludes concurrent gateway consumers and ends
   with admission closed. It requires real text, tool arguments/continuation,
   complete SSE and endpoint identity. Independent serving is required for single
   fallback; legacy fabric-bound distributed preferred records
   `not-applicable-distributed`, not a fake independent-serving success.
6. Only after successful actual qualification, update that matching policy
   `qualification` reference with its raw-file hash. Repeat disabled
   `init --adopt-receipts --apply`. This admits receipt-only enrichment; it does
   not authorize changes to plans/network/timings/ingress. Failed/stale/mismatched
   receipts remain ineligible. Retain all failures.

Historical coder tools/SSE from e8f1 through both gateways does not qualify coder
on 66f1. The prepared fallback hashes recorded in the rollout plan are candidate
identities, not acceptance. Strict `local-coder` is tools/text/SSE, no vision;
`local-fast` is text-only and is not an agent fallback just because weights match.

## 9. Explicit enablement, failure/return semantics and fault limits

After all gates, review disabled status and one bounded reconciliation. Do not run
a second reconciler while the supervised daemon holds its singleton lock:

```bash
"$PY" "$RELEASE/scripts/spark-recover" status --policy "$POLICY" --state-dir "$STATE"
# Only if no other reconciler owns the authority:
"$PY" "$RELEASE/scripts/spark-recover" run --policy "$POLICY" --state-dir "$STATE" --once
# Separately approved canary/enable only, not part of documentation preparation:
"$PY" "$RELEASE/scripts/spark-recover" enable --policy "$POLICY" --state-dir "$STATE" \
  --apply --approve automatic-recovery
"$PY" "$RELEASE/scripts/spark-recover" status --policy "$POLICY" --state-dir "$STATE"
```

An enqueued command is not an applied command: require
`commands[ID].state == "applied"`, no unresolved intent, and actual preferred
serving through the stable authenticated ingress before a separately approved fault.

The intended qualified policy is:

1. Preferred exact GLM TP2 serves. Either rank/node/required cable failure causes
   bounded detection and withdrawal of the old route generation.
2. Durable authority, all-ingress fencing and survivor ownership/epoch must be
   proved. Stop only the survivor's owned TP worker; prove GPU idle; start that
   survivor's qualified saved coder plan. No failed-node acknowledgement is needed
   to use a proven-safe survivor, but the absent node remains quarantined with its
   reservation tombstone. Unknown/foreign ownership or an unprovable fence fails
   closed, not into guessed cleanup.
3. `local-auto` explicitly substitutes the real backend with its smaller limits.
   Strict `local-glm53-flash` stays GLM or unavailable, never secretly Qwen. Do not
   silently migrate strict clients. Tools, vision and context/output limits are
   checked against the active backend; incompatible requests fail explicitly.
4. When both nodes, artifacts, independent paths and required fabric return and
   remain stable for configured gates, the controller **automatically** drains
   global ingress, stops the owned fallback and restores the **same exact** TP2.
   No GPU qualification workload runs on the occupied fallback during return
   preparation. A manual maintenance pause can prevent transitions.
5. Failed preferred startup is fenced and cleaned up only by proven ownership;
   restore the last qualified single and persist backoff/circuit state. If rollback
   also fails, report unavailable/BLOCKED, never a passing route.

No stream/KV migration, replay of generations/tools, zero-downtime promise or
automatic physical power-on exists. Mac loss suspends recovery and makes the
stable gateway unavailable; expired leases fail closed wherever ingress still
runs, but do not free GPUs. The operator must restore power/wake/connectivity.

### Controller-only simulated node loss is not a physical fault receipt

A possible separately approved future canary can hide a node **only from the
controller's view** while preserving independent operator restoration access. That
requires a reviewed, bounded fault mechanism and exact reversal, not an arbitrary
SSH alias edit, firewall flush, or killing a worker. There is no documented
`spark-recover simulate-node-loss` switch. This runbook does not implement, execute
or qualify such a harness. Until one is reviewed, use only the existing isolated
CPU test campaign or an explicitly approved real fault procedure.

Such a simulation cannot prove power loss, stale processes on an unreachable host,
physical cable removal, independent management survival, asymmetric partitions,
or real switch/link return. Stopping one worker while both hosts remain reachable
is a worker-failure test, not node-loss acceptance. Physical qualification must
cover each node lost in turn and automatic return, cable loss with independent
management, preferred-start failure/rollback, and separately approved Mac failure.
Keep exact plans/worker IDs/routes, an independent restore supervisor and physical
access before any fault. No fault is authorized merely by enablement.

For an emergency or end-of-window disable/reset/restore, follow
[the recovery runbook](SPARK_RECOVERY_RUNBOOK.md). Disable closes admission and
retains workers; it is not a hold-open serving mode. Reset stops exact-owned work
but retains fencing highwaters. Never use manual `up` to bypass those fences or
uninstall services as a substitute for reconciled GPU cleanup.

## 10. Bounded extension beyond two Sparks

Adding nodes changes inventory, topology, trust and acceptance; it does not resize
a live process or automatically enlarge policy membership.

- Independent singles/replicas can use added capacity after each node's exact
  model/runtime/serving contract is qualified. Three nodes may run TP2 on two and
  a single on the third. Four may run two independent TP2 groups. No recipe is
  assumed to support arbitrary TP/PP=N merely because the renderer accepts ranks.
- The pinned DeepSeek-V4-Flash-0731 runtime requires attention-head divisibility:
  64 heads do not permit TP3. TP4 arithmetic alone does not qualify tensors,
  quantized kernels, memory, DSpark or network. GLM's qualified TP2 recipe does not
  become TP3/TP4 by editing a number.
- NVIDIA's [topology guide](https://docs.nvidia.com/sync/0.97.6/cluster-assistant.html)
  documents a three-node direct triangle and a switched two-to-four-node path;
  four requires a switch in that workflow. Select qualified Ethernet/RoCE hardware
  and destination-specific port/subnet mappings. The pair's two `/30` subnets
  cannot contain a third host. Do not extend them by adding an address casually.
- Current inventory has top-level `version` and `nodes`; there is **no** top-level
  `topology` object accepted by `config.validate_inventory`. A rail may add `peer`
  and `physical_link`. Direct declarations must be reciprocal and physical link
  identities agree. `spark-node check` checks declared graph connectivity; absent
  peer declarations remain unknown and require qualification. It does not prove
  switched RoCE, all-pairs routing/GIDs or collective performance. Maintain the
  switch/port/cable map as operator evidence; do not invent unsupported schema.
- Admit a reviewed candidate through `spark-node discover/check/prepare/apply`,
  using canonical `inventory_sha256`, explicit `--approve-stable-address` values
  and exclusive output paths as described in
  [node enrollment](SPARK_SCALING_RECOVERY_PLAN.md#node-enrollment--implemented-physical-acceptance-separate).
  Existing node edits require a separately reviewed new inventory; enrollment
  does not overwrite them. Old plans stay immutable.
- Peer SSH bootstrap supports pinned N-peer trust, but the fabric MTU and GLM
  preparation helpers above remain pair-specific. Audit bulk-copy rail mapping,
  every rank/coordinator path, HCA/GID identity and runtime support before using
  new topology. No broad script substitution makes them N-node tools.
- Qualify every admitted placement/coordinator and gateway contract with exact
  artifacts. Recovery is bounded explicit policy, not discover-and-launch
  scheduling. Model coordinator transfer is a drain/restart operation using
  a separately qualified plan; authority transfer additionally requires fencing
  the old authority and preserving its journal/highwaters through the recovery
  procedure. Neither is a live manual rank swap.

Removal requires reviewed idle/ownership checks and separate peer trust revocation
while old trusted paths still exist. Preserve removal IDs, admission guards and
partial receipts. Do not delete a node from an inventory to free its GPU or bypass
an unresolved revocation. More nodes do not remove the selected Mac's single point
of failure or provide automatic power/cabling administration.

## Source and preservation checklist

Parser/source grounding for these procedures:

- `tools/spark_cluster/config.py`: strict inventory/recipe fields, serving address,
  canonical full-plan hash and rank/Compose rendering.
- `tools/spark_cluster/enrollment.py`: topology constraints, immutable admission,
  explicit prepare receipts and CLI flags.
- `scripts/configure-spark-fabric-mtu.sh`: pair profile `/30` preconditions,
  MTU 9000, active-compute refusal and exact backup/rollback.
- `scripts/configure-spark-peer-ssh.py`: pinned trust/bootstrap/remove interface.
- `scripts/prepare-glm53-tp.py`, `prepare-glm53-runtime.py`,
  `fetch-pinned-spark-model.py`, `spark_transfer.py`: actual artifact preparation,
  full-file verification, pair-helper limitations and direct-copy policy.
- `tools/spark_cluster/cli.py`: observe, saved-plan hash gate, owned activation,
  readiness evidence and immediate owned stop.
- `tools/spark_cluster/recovery.py`, `recovery_routes.py`, `services.py`: actual
  policy/receipt schema, approval strings, disabled adoption and Mac hosting.
- [Local-first operations contract](LOCAL_FIRST_OPERATIONS_PLAN.md),
  [GLM qualification](GLM53_TP.md), [scaling constraints](SPARK_SCALING_RECOVERY_PLAN.md).

Preserve private exact plans/Compose, source/installer pins, verified artifact
locks, worker/reservation identities, inventories/trust public pins, authority
journal/highwaters, policy and qualification/isolation receipts, gateway registry
and credential **locations**, service config, admin network before/after snapshots
and exact reversals. Protect actual secrets in their approved backup, never in
this document or Git. Keep the foundation and emergency runbooks locally/offline;
no working model or Internet connection should be required to find the next safe
restoration step.
