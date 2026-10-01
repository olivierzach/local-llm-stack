# Local-first operations and automatic Spark recovery

Status: implementation and operating contract, September 16, 2026; not a physical acceptance receipt.
The [durable implementation goal](SPARK_RECOVERY_GOAL.md) tracks completion separately.
Implementation belongs in `/Users/statsparrot/projects/local-llm-stack-recovery`,
branch `feat/spark-automatic-recovery`; keep the original checkout at
`/Users/statsparrot/projects/local-llm-stack` and its existing edits unchanged.
Local implementation is authorized; installation, hardware tests and recovery
enablement need separate approval. Local implementation and its evidence are recorded
below and in the durable goal; automatic recovery remains disabled and not live-qualified.

## 1. Outcome and architecture decision

Run OMP, VS Code and the operator CLI on the Mac. Keep model execution, model caches
and node-owned runtime state on the Sparks; store service credentials/state on their
owning hosts, never in a checkout mirror.
Deliver **preferred exact TP2 plan → qualified existing single-node recipe on the
surviving Spark → automatic return to exact TP2 after both nodes and fabric stay ready**.
Either Spark may fail. A failed node's acknowledgement must not be required to
recover a safely fenced survivor. Automatic failback is required when its gates
pass; an explicit maintenance pause may prevent transitions.

Extend the existing Compose node services and `sparkctl`, not a second
general-purpose scheduler. **The user selected this Mac, kept awake, as the recovery
authority/controller and stable authenticated gateway host.** Plan launchd-supervised
foreground services on the Mac, with management/serving paths independent of either
Spark and its collective cable. Their lifetime must not depend on OMP, VS Code,
an interactive terminal or a remote agent session. Linux/systemd remains a portability
option, not required hardware or an outstanding third-host selection.

This Mac is a declared single point of failure, **not HA**. Recovery and the stable
gateway require it to stay awake, powered and network-connected. Sleep, power loss
or loss of its required connectivity makes the gateway unavailable and suspends
recovery; authority loss ends lease renewal, and expired routes fail closed wherever
ingress is still running. Leases do not release GPU ownership. An operator must
wake/power the Mac and restore connectivity as needed; services then reconcile
durable state and actual ownership before resuming. Host selection is settled;
service installation, always-awake configuration, trust/network provisioning and
activation still require their separate approvals and acceptance.
Before a production stop, restart, model switch, network change or disruptive test,
describe the expected interruption and restoration procedure and obtain explicit
approval. Run an approved stop/start transaction independently of the model being
stopped: an agent must not cut off its own inference backend between separate tools.
LaunchAgents additionally require the selected user to remain logged in; logout is
an availability failure even if the Mac remains powered.


Non-goals: uninterrupted streams, KV/session migration, generation/tool-request replay,
warm TP2/single overlap on occupied GPUs, new fallback models, automatic cloud fallback,
Wi-Fi NCCL migration, or claiming performance without measurements. Preserve unrelated
DeepSeek concurrency/compaction, Vector/Loop qualification, remote-drafter work and
historical source reconciliation as separate workstreams.

## 2. Dated baseline and existing versus proposed

The source-alignment record reported `main` at `c7adb11297f72824d60491580a41eecbc08b1101`
and installed controllers at `696af2a`; this is historical evidence, not a new remote check.
Last recorded service: `local-glm53-flash`, TP2/DFlash2, e8f1 coordinator, 66f1 worker;
owner `glm53-tp2-256k-dflash2-e8f1-b1cf7901f81c`, plan on both nodes at
`~/projects/local-llm-stack-cluster/state/glm53-20260913/e8f1-04/plan.json`.
Model API: 8125; guards: 4010; managed gateways: 4110. Both GPUs were occupied.
Caches/overlays: `~/projects/local-llm-stack/data/`; gateway state/credentials:
`~/.local/state/local-llm-cluster/`. Reconcile exact identities before any activation.

| Existing at planning baseline | Required change; current implementation is described in section 7 |
| --- | --- |
| `sparkctl` validate/render/doctor/preflight/up/status/down/probe/collectives/cache | Add bounded observe, exact restore and explicit recovery policy/service controls. |
| `up` includes generation probes; no `up --saved-plan` | Separate immutable-plan activation from rendering; inference only under approved activation. |
| `validate_saved_plan` checks identity, membership and ownership | Retain it; validate trusted saved Compose/artifact identity before executing it. |
| `status` takes `node.locked()` and aggregate inspection aborts on failed node | Mutation-free, deadline-bounded partial observation of every node. |
| Single-node reserve/preflight requires all inventoried fabric | Mode-aware admission; single mode must not require an unrelated dead cable. |
| Renderer binds API to first fabric IP; generated routes use saved endpoint | Separate management, serving and collective addresses before cable-free fallback. |
| Matching pinned single recipes form explicit `--replicas` groups | Policy substitution is not a replica group; keep replica validation strict. |
| Gateway request counters are local to each gateway | Authority-wide admission/drain and expiring generation fencing across all ingress. |
| Persistent per-user reservations and owned stop paths | Durable epoch/operation ledger, return quarantine and crash reconciliation. |
| Compose Prometheus/Grafana; scrape config has fixed vLLM targets | Correlated dynamic deployment, hardware, fabric and recovery dashboards. |

Saved plans are recovery handles, not signatures or proof of loaded code. Directory
names, friendly aliases, `current` links and historical status files cannot identify
a live worker. Compare owner, full plan digest, exact container IDs/labels, image and
reservation identities. Never mutate an old saved plan to change an address or role.

### Fallback selection and qualification

Use the existing deployments `cluster/deployments/coder-{66f1,e8f1}.json` for
agent-capable fallback, after saving and qualifying an exact plan for each node.
`cluster/recipes/qwen3-4b-tools.json` pins `Qwen/Qwen3-4B-Instruct-2507` revision
`cdbee75f17c01a7cc42f958dc650907174af0554`, image
`vllm/vllm-openai@sha256:e4f88a835143cd22aee2397a26ec6bb80b3a4a6fe0c882bcbc63822904766089`.
It advertises `local-coder`: text/tools/SSE, no vision, Hermes tool parser,
32,768 context, 4,096 output, BF16, 0.30 GPU-memory utilization, minimum 24,576 MiB
available, `max_num_seqs=4`, eager execution and prefix caching, single mode only.

`fast-{66f1,e8f1}.json` / `qwen3-4b.json` pin the same model/image, but advertise
`local-fast`: text/SSE only, no tools/vision, 8,192 context, 2,048 output,
`max_num_seqs=8`, eager execution without that recipe's prefix-cache flag.
Use fast only for an explicitly text-only policy, never silently for agent tools.
Both recipes' limits are contracts, not throughput or quality measurements.

`docs/CLUSTER.md` records coder tool/SSE through both gateways with **e8f1 serving
and 66f1 absent**; that does not qualify coder on 66f1. Fast text/SSE replica routing
was exercised on both nodes, but not physical cable failure or automatic recovery.
Balanced Qwen3-14B (16,384 context/4,096 output; no tools/vision) passed on e8f1,
with 66f1 GPU acceptance pending; it is not the default agent fallback.
Admission receipts must name node, exact saved-plan/recipe/image/model/tokenizer
identity, capability probes, controller release and address configuration.
Missing/stale acceptance means ineligible, not permission to improvise a recipe.

## 3. Proposed policy and durable state contract

All new command names and data fields below are **PROPOSED interfaces**. Freeze their
versioned schema before implementation; unknown fields/versions fail validation.
Store private policy/state durably on the selected Mac, outside the checkout and
OMP/VS Code sessions, with an approved backup/restore path.

| Policy field | Meaning / initial proposed value |
| --- | --- |
| `version`, `policy_id`, `enabled`, `maintenance_pause` | Version 1; stable ID; disabled initially; explicit pause. |
| `authority_id`, `authority_state_path`, `ingress_ids` | One pinned authority and exhaustive managed ingress membership. |
| `preferred_plan` | Immutable file/reference plus full digest, owner and artifact-manifest digest. |
| `fallback_plans` | Node → exact admitted coder plan/qualification receipt; optional separate text policy. |
| `policy_alias`, `required_capabilities` | Opt-in `local-auto`; text/tools/SSE for agent policy, no promise of vision fallback. |
| `management_routes`, `serving_endpoints` | Pinned host identities and approved independent addresses, outside immutable old plans. |
| `health_interval_s`, `health_timeout_s`, `failure_threshold` | 5, 2, 3 consecutive observations; independent signals recorded. |
| `route_lease_s`, `renew_interval_s` | 15, 5; expired/unknown lease rejects new requests. |
| `return_stable_s`, `minimum_fallback_dwell_s` | 120, 300; require continuous node, artifact and fabric readiness. |
| `drain_timeout_s`, `drain_timeout_action` | 120, `abort-transition`; explicit optional approved `terminate-owned` policy. |
| `activation_timeout_s`, `stop_timeout_s` | 900, 60; tune from approved startup/stop measurements, not guarantees. |
| `retry_backoff_s`, `retry_max_s`, `circuit_failures` | 30, 600, 3 failed activation cycles; persist backoff across restart. |
| `queue_limit`, `transition_new_requests` | 0, `reject-503`; no opaque long queue or automatic request replay. |
| `qualification_max_age_s`, `policy_revision` | Explicit acceptance freshness and optimistic-concurrency revision. |

Validate timer relationships, positive bounded values, unique nodes/ingress and a
fallback's single-node membership/capabilities. No policy may automatically exceed
a qualified plan's context, memory or parallelism. Changes require an auditable
policy revision; unrelated deployments never become recovery candidates by discovery.

Persist `state_version`, `authority_id`, monotonic `epoch`, `route_generation`,
`policy_revision`, desired/observed lifecycle state, preferred/fallback identities,
node quarantine/tombstones, operation UUID and step receipts, ingress acknowledgements,
retry count/deadline and last error. Use a private atomic fsync+rename journal with
durable ordering; test torn writes, disk-full and corruption. Corrupt/unavailable
durable state is fail-closed, not a fresh empty cluster.
A launchd-supervised foreground loop on the selected Mac holds an exclusive
authority process lock; restart reconciles before mutation. Linux/systemd packaging
may preserve the same contract. Periodic bounded metadata polling is permitted
for this approved service, unlike the one-shot observation milestone below.

## 4. Observable lifecycle and safety invariants

| State | Entry / permitted next step |
| --- | --- |
| `DISABLED` / `PAUSED` | No new recovery mutation; display current routes/workers. Resume reconciles first. |
| `RECONCILING` | Recover durable intent and actual IDs/epochs; never infer success from a lost reply. |
| `TP2_SERVING` | Exact preferred group ready and published; watch both ranks, gateway and fabric. |
| `SUSPECT` | Threshold/debounce evidence; keep healthy routes only while their leases/readiness hold. |
| `FENCING` | Persist next epoch; withdraw failed generation across ingress; prove expiry/acknowledgement. |
| `SINGLE_STARTING` | Own/stop survivor's old rank, verify GPU idle, activate eligible single saved plan. |
| `SINGLE_SERVING` | Publish ready fallback, monitor return and continue serving within smaller contract. |
| `RETURN_STABILIZING` | Reconcile returned node and continuously time node/artifact/fabric health. |
| `DRAINING` | Close global admission; await every ingress's zero outstanding work or deadline policy. |
| `TP2_STARTING` | Stop owned fallback; restore both exact TP2 ranks; readiness then publish generation. |
| `ROLLING_BACK` | Fence failed TP2 attempt, stop own partial workers, restore saved eligible fallback. |
| `BACKOFF` / `BLOCKED` | Persist reason, next retry or operator-required gate; no speculative launches. |

### Authority, fencing and ownership

There is one durable writer, not election between the two Sparks. Administrative
role transfer first pauses/fences the old authority and proves it cannot issue
accepted actions; otherwise transfer is blocked. Losing contact is not proof of death.
Every mutation carries authority ID, epoch, operation UUID, expected owner/digest
and expected prior state. Node helpers serialize mutations and durably reject stale
epochs; repeated UUIDs return the recorded result after inspecting actual state.
Recheck ownership/epoch at each external side-effect boundary. A lost SSH reply must
not produce duplicate launches or allow an old queued stop to kill a newer worker.

Initial implementation uses one policy-aware ingress on the selected Mac, independent
of the Spark hosts, so its locked request counter is global for `local-auto`.
Existing gateways do not gain global
drain by implication. Adding another policy ingress requires the same coordinated
fencing/drain contract; activation refuses unproved bypass isolation.

Every managed ingress accepts only the active generation under a bounded lease from
the pinned authority. Lease freshness uses monotonic local time; gateway restart
invalidates its lease until refreshed. Persist the highest accepted generation so
stale snapshots cannot revive it. Fence by explicit acknowledgement or waiting the
maximum outstanding lease plus declared clock/transport allowance; a disconnected
ingress must reject after expiry. Admit a request only against its checked generation.
All client paths, including legacy 4010/4110 and direct backend access, must be placed
behind this fence or isolated by approved ACL/bind rules before enabling recovery.
If any unfenced serving path remains, safe automatic recovery is not qualified.

The survivor must acknowledge its current epoch, stop only the owned TP rank and
prove its reservation/container/GPU state idle before single admission. The failed
node need not ACK: retain its reservation tombstone and quarantine; never reschedule
its GPU or declare its old process dead. Durable authority + expired old ingress
generation + survivor epoch gate permit useful fallback without dead-node cooperation.
An unprovable fence, wrong owner or unrelated GPU process yields bounded BLOCKED
and an alert. Never delete unrelated processes, reservations, caches or containers.
Returned nodes must reconcile/stop stale **owned** workers before readmission.

### Crash reconciliation and exact-plan restore

Persist intent before acting and the observed receipt afterward. On controller or
host restart, inspect nodes/routes in parallel bounded calls, compare operation
receipts and exact container IDs, then complete or roll back the recorded transition.
Ambiguity retains quarantine and fencing. Do not start by rendering today's recipe,
clearing reservations, or making an unknown worker satisfy a desired state.

`sparkctl up --saved-plan PATH --plan-sha256 SHA256` is the implemented primitive
shared by manual and automatic activation. It validates the saved plan and trusted receipt, required files,
image/model/drafter/tokenizer/overlay/library pins, node identities and recorded
Compose content; `validate_saved_plan` alone is not full Compose-content attestation.
Do not re-render, rewrite endpoint, silently pull a new tag, or change coordinator.
Use pre-staged pins and owned admission. Record exact IDs per rank and readiness
receipt; partial failure leaves a reversible owned operation, not a published route.
An incompatible older address plan is blocked or replaced through a separately
approved new immutable plan, never edited in place. Separate management transport
selection may reach a host without changing the plan's serving/collective identity.

## 5. Automatic failover, failback and request semantics

1. Detect loss of either rank/node or required collective fabric; distinguish a
   gateway-only failure from a worker failure. Do not restart healthy workers to
   repair a gateway. Thresholds reduce noise but never advertise known-wrong identity.
2. Fence the old TP2 generation. Select the admitted fallback on the reachable
   survivor, checking pinned artifacts and address reachability. If none qualifies,
   retain a clear unavailable route/503 and alert; no new model or unqualified node.
3. Stop the survivor's owned rank, verify GPU idle, then activate its saved coder
   plan. Bound startup, perform approved inference readiness, and publish only after
   endpoint identity/capabilities match. Record new generation, real backend and IDs.
4. On node return, quarantine/reconcile its old rank first. Stage CPU/disk/network
   checks while fallback serves: pins/cache presence, controller parity, serving
   connectivity, fabric link/peer/RDMA readiness. No competing GPU collective or
   inference test on the occupied fallback GPU during this stage.
5. After continuous stable health and minimum dwell, **automatically** close global
   admission and drain all ingress. Renew fencing/control leases throughout drain.
   A recovered node alone is insufficient while fabric is degraded or artifacts miss.
6. At zero work, stop fallback, verify both owned GPU slots available and restore
   the exact preferred plan. Perform approved startup/collective/inference readiness
   during activation, publish the new generation, then reopen admission.
7. If preferred activation fails, fence it, stop only its owned partial workers,
   restore the last saved qualified single plan and republish after readiness.
   Persist exponential backoff; after three failed cycles keep healthy fallback
   serving with automatic promotion blocked pending acknowledgement of the circuit.
   If fallback rollback also fails, expose BLOCKED/503 and both causes; never green.

Drain is global, not a sum of casually sampled local counters: close admission at
all ingress under the same generation, then collect outstanding request/stream
receipts. Unknown/disconnected ingress waits for lease expiry and its documented
request-abort deadline. Default drain timeout aborts voluntary failback and resumes
the still-healthy fallback; an explicitly approved forced deadline terminates owned
requests before stop. Catastrophic rank loss may already break in-flight requests.
Never replay generation or tools; clients receive a bounded explicit failure.
No warm overlap, stream migration or zero-downtime claim is possible on these GPUs.

Strict aliases (`local-glm53-flash`, `local-coder`, `local-fast`, others) retain exact
model/capability meaning: GLM returns unavailable during single fallback, not Qwen
under its name. Only clients explicitly configured for **`local-auto`** consent to
policy substitution. Do not model dissimilar recipes as `--replicas`.
Expose active backend alias/model/revision, saved-plan digest (`X-Spark-Deployment`),
policy generation and active context/output/capability limits through authenticated
model/status discovery and response metadata. Retain client authentication and
stable base URL across transitions; do not leak internal credentials/addresses.

Validate each request against the selected backend **before dispatch**: tools,
vision, format options, model identity and backend-pinned tokenizer/chat-template
accounting; include system/tool schema/history/image tokens and requested output.
Enforce input + output <= active context and output <= recipe cap. Unsupported
vision on coder, excessive GLM-sized history and incompatible tool requests produce
structured 4xx errors, not truncation, tool removal or lying about capabilities.
Pin a request to its admitted generation through the stream; reject a stale admission
if transition wins before dispatch. Different tokenizer counts cannot be reused blindly.

## 6. Three networks and combined live observability

Inventory evolution separates `management` (SSH/control), `serving` (API/gateway)
and `fabric` (collectives/RDMA) roles, with strict versioned validation and migration.
Keep existing immutable plans valid; render new fallback plans with independent
serving addresses. Update admission, API binds, health checks and gateway publication
together. Fixing only SSH retry cannot make today's fabric-bound single mode work
without its cable. TP2 must continue requiring its qualified collective fabric.

Provide independent Ethernet or Wi-Fi management access to **each** Spark, with
pinned SSH host keys, approved public-key trust, bounded reconnect and no agent/private
key copying. Current e8f1 wired SSH from the Mac jumps through 66f1: remove that
single management dependency in approved setup. Serving over management Wi-Fi must
be explicitly bound/firewalled and measured; reachability is not bandwidth equivalence.
When cable disappears, control and qualified single serving remain possible; TP2
collectives do not hot-migrate to Wi-Fi. Cable return must stabilize before failback.
Physical/logical topology details remain in [the scaling plan](SPARK_SCALING_RECOVERY_PLAN.md).

Extend existing `docker-compose.yml` Prometheus/Grafana and `config/prometheus.yml`;
reuse authenticated existing metrics where supported, not a second monitoring stack.
Replace fixed-only vLLM targeting with admitted deployment identity/service discovery.
Bound scrape overhead/cardinality; never label metrics with prompts, token text or secrets.

| Dashboard / alerts | Required signals and interpretation |
| --- | --- |
| Service and users | End-to-end TTFT, output tokens/s, request duration/errors, queue/admission, active streams; label backend and generation. |
| Engine | Per-engine running/waiting sequences, supported KV-cache utilization, prefill/decode counters and configured context/output/max_num_seqs. |
| Nodes | Supported GPU busy/power/temperature, GPU/unified-memory pressure, CPU/RAM, disk/cache availability and controller reachability. |
| Physical fabric | One QSFP cable mapped to its two logical interfaces; negotiated speed and separately measured TX/RX bit rates per direction. |
| RDMA | Hardware port bytes/packets, errors/retries/discards/congestion/PFC where supported, link state and selected transport; ordinary netdev bytes alone are insufficient. |
| Recovery | State/epoch/generation, fencing/lease age, quarantine, drain, failover/failback duration, startup/rollback failures and circuit/backoff. |

Convert counter units explicitly (including hardware word counters); handle resets,
missing exporters and stale samples. Unsupported metrics are `unavailable`, never zero.
Show negotiated capacity beside observed rate; do not add both ends of the same
cable or opposite directions into a fictitious one-way link speed. Correlate one
request timeline with engine, node, fabric and recovery events using synchronized
wall time plus monotonic durations. Idle links cannot prove attainable throughput.
No saturation benchmarks, profilers or synthetic generation on the live service.

A TP2 group is **one replica**: GLM `max_num_seqs=4` is four engine sequence slots,
not four per GPU/eight total or four full-context guarantees. Coder single has four,
fast single eight. Additional full replicas consume real independent GPU/memory
capacity; replicas may increase aggregate throughput but not one request's context.
Compute, KV memory, prompt length, prefill and fabric can bottleneck first. Publish
measured operating envelopes per admitted recipe, never infer linear node scaling.

## 7. Reproducible operations and future admission

The implemented entrypoints are `scripts/spark-recover`, `scripts/spark-services`,
`scripts/spark-monitor`, `scripts/spark-node` and `scripts/sparkctl`. Installed
controller releases expose the same names without `scripts/`. There is no
`sparkctl recovery` or `sparkctl node enroll` subcommand.

### Policy preparation and controls

Prepare a private JSON policy (0600, private parent directory). Required version-1
fields are:

| Field | Contract |
| --- | --- |
| `version`, `id`, `alias` | `1`, a stable policy identifier, and exactly `local-auto`. |
| `preferred` | `{path, sha256, qualification}` for the exact saved distributed plan. |
| `fallbacks` | Ordered `{node, path, sha256, qualification}` references, one independent-serving coder plan per node. |
| `management` | Node-keyed transport overrides accepted by `cli.transport_node`, e.g. `{ssh: "approved-independent-alias"}`. |
| `gateway` | `{url, key_file, registry, route_state, isolation}`; URL is the dedicated gateway origin, not `/v1`. |
| `timings` | `{}` accepts validated defaults; unknown keys and nonfinite/bool values are rejected. |

Plan `sha256` is **canonical full-plan SHA-256**, emitted by `sparkctl render` or
`config.plan_sha256`, not the deployment digest or raw file hash. A non-null
`qualification` or `isolation` is `{path, sha256}` using the artifact's raw-file
SHA-256. Relative references resolve against the policy file. Missing qualification
and isolation receipts may be `null` during preparation but cannot authorize enablement.
The original preferred plan need not acquire new serving fields: a separately
rendered fallback may specify its approved independent serving bind while preserving
the preferred plan's immutable node identity/topology/cache fields.
If the preferred plan already declares `serving`, fallback must preserve that exact
binding. Only a legacy preferred plan without `serving` may add an independent
fallback binding; contradictory explicit bindings are rejected before activation.

```bash
scripts/spark-recover validate --policy "$POLICY"
scripts/spark-recover init --policy "$POLICY" --state-dir "$STATE" --apply
scripts/spark-recover run --policy "$POLICY" --state-dir "$STATE" --once
scripts/spark-recover status --policy "$POLICY" --state-dir "$STATE"
```

`init` creates a disabled authority; `run` never implicitly enables it. Without
`--once`, `run` is the supervised foreground reconciler. Status reports queued
commands and their applied/refused outcomes; queueing is not proof of application.
Commands work while the daemon owns its singleton lock.

After separately approved physical preparation, these controls require `--apply`
and the indicated `--approve` value:

| Command | Approval | Effect |
| --- | --- | --- |
| `enable`, `resume` | `automatic-recovery` | Require current exact qualifications and trusted ingress-isolation evidence before enabling transitions. |
| `pause`, `disable` | `close-ingress` | Close/drain policy ingress; retain workers, ownership, journal and fences. |
| `reset` | `stop-exact-owned-workers` | Drain and stop only proven owned workers; leave disabled; retain caches, secrets, history and highwaters. |
| `clear-circuit` | `retry-owned-transitions` | Clear persisted retry/circuit controls, not ownership or qualification requirements. |
| `switch-coordinator` | `saved-coordinator-drain-rollback` | Check a separately qualified target and current artifact preparation before drain; use exact rollback on startup failure. |

For example, **only after approval**:

```bash
scripts/spark-recover enable --policy "$POLICY" --state-dir "$STATE" \
  --apply --approve automatic-recovery
```

Qualification does not stage or start workers. During approved maintenance, start
the exact admitted plan, then exercise its real text, tool arguments/continuation,
streaming completion and authenticated identity through the dedicated gateway:

```bash
scripts/spark-recover qualify --policy "$POLICY" --state-dir "$STATE" \
  --plan "$PLAN" --plan-sha256 "$PLAN_SHA256" --staging "$STAGING_REFERENCES" \
  --output "$NEW_QUALIFICATION_RECEIPT" --rounds 1 --apply --approve-inference
```

`STAGING_REFERENCES` maps each plan node to an independently trusted `{path, sha256}`
artifact. Qualification requires a disabled authority, excludes concurrent gateway
consumers, and finishes with ingress closed. A failed receipt is retained but is
ineligible. A legacy fabric-bound distributed preferred plan records independent
serving as not applicable; single-node fallback must actually pass that gate.
After updating only qualification/isolation references in the same policy, explicit
`init --adopt-receipts --apply` enriches the disabled journal without resetting its
authority or highwaters. It refuses deployment, network, timing or ingress changes.

### Mac service hosting

Install a new, reviewed, pinned controller release with
`scripts/install-spark-controller.py`; do not patch an existing installed release.
Its receipt hashes the complete runtime payload and dependency lock. Service config
version 1 requires absolute `release`, full Git `revision`, release-local `python`,
`policy`, `service_dir`, child `state_dir`, and `inventory`. Optional fields are
`plans`, `gateway` (`bind`, `port`), `monitor` (`bind`, `port`, `interval`, `timeout`)
and explicit boolean `awake`. Default listeners are loopback; monitor port is 9842.
Policy key/registry/route files must be separate children of `service_dir/ingress`.
Use mode 0600 for config/policy and 0700 for their directories; state must be outside
the immutable release and separate from ingress/logs. `release` must name the
revision directory, not a mutable `current` symlink.

```bash
scripts/spark-services validate --config "$SERVICE_CONFIG"
scripts/spark-services render --config "$SERVICE_CONFIG"
# The commands below change this Mac and require separate installation approval:
scripts/spark-services install --config "$SERVICE_CONFIG" --apply
scripts/spark-services start --config "$SERVICE_CONFIG" --apply
scripts/spark-services status --config "$SERVICE_CONFIG"
scripts/spark-services stop --config "$SERVICE_CONFIG" --apply
scripts/spark-services uninstall --config "$SERVICE_CONFIG" --apply
```

Install creates private dedicated ingress and a disabled journal, not enabled
recovery. LaunchAgents execute `spark-services run-role --config ABS --role ROLE`
for gateway, recovery and monitor; optional awake uses `caffeinate` only with opt-in.
They do not depend on OMP/VS Code/terminal lifetime, but actual installed
session-loss survival still requires physical acceptance. Stop/uninstall never
free GPUs or delete credentials/history. Interrupted plist transactions retain
ownership evidence and are repaired only by explicit mutation; unknown user edits
are preserved rather than adopted or overwritten.

### Monitoring and enrollment

`spark-monitor --inventory PATH --plan SAVED_PLAN` accepts repeated plans, optional
`--recovery-state "$STATE/status.json"` and authenticated `--gateway-url ORIGIN`
with `--gateway-key-file PATH`. `--once` is a bounded read-only snapshot. Native
engine metrics are collected over the inventory's management SSH transport from
the saved coordinator, verifying container name/labels/image/command, model
root/context, and unchanged container identity around the scrape. The Mac need
not directly route onto the collective network for monitoring. Serving through
the Mac gateway still requires a separately provisioned path to each saved endpoint.

The existing Compose Prometheus/Grafana configuration provisions the Spark dashboard
and alerts. Docker Desktop reaches the host exporter at `host.docker.internal:9842`;
that path was exercised with the loopback exporter. CPU counters use one
`spark_host_cpu_seconds_total{node,mode}` family. Inactive saved engines remain
visible but do not raise selected-engine outage alerts. Missing authority/ingress
feeds remain unavailable, not inferred healthy from running GPU workers.

`spark-node discover/check/prepare --inventory PATH` separates observed, prepared
and qualified state. `prepare` additionally requires `--deployment PATH`; selected
image/model staging needs `--apply`, `--pull-image` and/or `--model-manifest`,
with an explicit `--max-download-bytes` bound for downloads. It does not install
root packages, run GPU inference, enroll a recovery policy or publish live aliases.
New inventory admission and removal, independently pinned peer SSH trust and
topology limits are detailed in [SPARK_SCALING_RECOVERY_PLAN.md](SPARK_SCALING_RECOVERY_PLAN.md).

Setup: inventory/trust → pinned controller/runtime prerequisites → independent
management/serving checks → pre-staged image/model/overlays → immutable plans →
per-node qualification receipts → stable gateway/auth → disabled policy → dry-run
reconciliation → approved canary/enable. Prepare pinned launchd service packaging
on the selected Mac, with durable private state, restricted credentials, logs and
restart backoff; installation is not yet approved or performed. No service lifetime
may depend on OMP/VS Code or a terminal session. Awake/power/network availability
is still required. Approve and provision the always-awake configuration, required
privileges and host-key trust separately; Linux/systemd is an optional port.
Do not blindly upgrade drivers, copy private keys, purge caches or adopt third-party
launch scripts.

Reset is an explicit stop-and-disable operation, not a metadata wipe or implicit
restart. Later approved enablement reconciles ownership and qualifications before
selecting service and eventually returning to the preferred plan. A destructive
host reprovision is a distinct operation. Never delete journal/fence files to force
adoption of a new authority.

Switching the **model coordinator** is not moving the recovery authority. Save and
qualify a separate immutable TP2 plan, then invoke `switch-coordinator` on an enabled,
unpaused reconciled authority with `--plan`, `--plan-sha256`, `--qualification`,
`--qualification-sha256`, `--apply` and `--approve saved-coordinator-drain-rollback`.
The controller performs preparation, global drain, exact startup and bounded
rollback; it is not live rank promotion. Reusing an old digest while swapping ranks
is prohibited. Switching the
**authority host** additionally requires fencing the old authority, transferring
private journal/trust through an approved backup/restore path, and reconciling epochs.

Extra nodes: enroll identity/architecture/GPU count/address roles/trust, verify pinned
runtime compatibility/cache capacity and management independence, stage artifacts,
qualify single and required distributed roles, then add explicit policy membership.
New recipes: pin all artifacts/tokenizer/template, declare capabilities and context,
measure admission/memory/concurrency, qualify requested node placements and gateway
contract, then save immutable plans/receipts. No discover-and-launch auto-enrollment.
Scaling TP beyond two requires model/runtime/topology qualification; a third node
can host a real independent replica only if its GPU and model fit. Preserve ownership
and selection determinism; this remains bounded policy placement, not a new scheduler.

### Why not require K3s now?

Keep Compose for Spark workers and launchd for the selected Mac's authority/gateway:
standard supervised foreground services, a private atomic journal, immutable plans,
expiring route leases, rollback and observability address the actual ownership and
routing risks without another scheduler. Linux/systemd is a portability option.
K3s is a conditional future backend/pilot, not a prerequisite. Orchestrator choice
does not create GPU capacity or remove the selected Mac's availability dependency.
[NVIDIA GPU Operator 26.3](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/26.3/platform-support.html)
lists Spark/K3s support; verify exact ARM64/driver/runtime matrix before any pilot.
[LWS](https://github.com/kubernetes-sigs/lws) manages coordinated worker groups, but
still needs pinned artifacts, readiness, ingress/model identity and recovery policy.
[HA embedded-etcd K3s](https://docs.k3s.io/datastore/ha-embedded) needs at least three
server nodes; two Sparks do not provide quorum HA. Revisit when node count, shared
scheduling/multi-tenancy or operational staffing justifies migration. A disposable
pilot must prove GPU/RDMA integration, group recovery and rollback without changing
this service's contracts; do not migrate a working TP2 merely to obtain restarts.

## 8. Deliverables, dependency order and owners

Each row has one integration owner; named parallel units may proceed only after
shared schemas are fixed. Proposed new files are identified explicitly; existing
entry points/tests are reused rather than creating parallel orchestration stacks.

| Phase / owner | Deliverable, source/test mapping and exit gate |
| --- | --- |
| A — observation owner | `cli.py`, `node.py`, `tests/test_cluster.py`: side-effect-free partial observer and private evidence; no GPU/state mutation. |
| B — contract/integration owner | `config.py`, proposed recovery policy/journal module under `tools/spark_cluster/`, `tests/test_cluster.py`: strict schemas, exact restore/receipt validation, persistent epochs/idempotency and CPU crash fixtures. |
| C1 — node/address owner | `config.py`, `node.py`, `tests/test_cluster_preflight.py`: single-mode address/admission correction, independent management, owned epoch gate and return quarantine. Serialize shared config edits with B. |
| C2 — ingress owner | `gateway.py`, `gateway_node.py`, `scripts/context-guard-proxy.py`, `tests/test_cluster_gateway.py`: all-ingress leases/admission/drain, strict aliases, explicit policy alias and capability/token limits. Parallel with C1 after B contracts. |
| C3 — monitoring owner | Existing Compose/Prometheus config and dashboard assets: identity-aware metrics and documented unsupported signals; no live benchmark. Parallel with C1/C2 using B identities. |
| D — recovery integration owner | `cli.py`, proposed recovery loop module and Mac launchd services: failover, stable automatic failback, rollback/circuit and reconcile; consumes B/C1/C2. Linux/systemd is optional portability packaging. |
| E — operations owner | Existing installer/bootstrap surfaces and CLI: pinned reproducible setup on the selected Mac, approved always-awake/power/network provisions, reset, coordinator switch, enrollment and disabled-default service packaging; consumes D. Host selection is complete; install/activation approval and proof are not. |
| F — acceptance/operator owner | CPU suite via existing `tests/test.sh`, then separately approved physical matrix below; evidence and release/policy receipts required before enablement. |

The integration owner runs formatting/tests once after concurrent edits settle;
workers do not run competing project-wide validation. Keep source contracts and
runbooks aligned at each integration gate. Pending Mac service/trust/network
provisioning or GPU maintenance approval blocks physical deployment/acceptance,
not reachable local implementation; no additional authority host must be selected.

## 9. Evidence, phased acceptance and enablement

### Local readiness gate

From the recovery worktree with its existing test environment:

```bash
./tests/test.sh -rs
docker compose --profile '*' config --quiet
# Fast, CPU-only regression subset for the recovery/hosting boundary:
.venv/bin/python -m pytest tests/test_cluster_recovery.py \
  tests/test_cluster_recovery_cli.py tests/test_cluster_services.py \
  tests/test_cluster_enrollment.py
```

These tests use isolated state, fake GPU/SSH boundaries and real loopback HTTP;
they do not install LaunchAgents or contact production engines. Use the pinned
release wrappers after installation; use `.venv/bin/python scripts/COMMAND` when
running source entrypoints without an activated virtual environment.

A testing release must include the committed recovery changes, not merely the old
base revision. Build a Git bundle of the recovery branch and install its **full
commit ID** into an approved dedicated prefix using the installer in section 7.
Keep service installation, ingress cutover and GPU qualification as separate
approvals. Do not point tests at production journal, key, registry or route files.
The existing live plan must be copied and independently pinned; a new render of
the current four-image GLM recipe is not the historical one-image saved deployment.

The local regressions additionally cover target-specific startup evidence during
rollback/reselection, repeated coordinator switches through the real CLI, ambiguous
serving bindings, invalid monitor service timing, and preservation of unrelated
SSH edits after an interrupted trust transaction. Before-only legacy SSH journals
cannot prove ownership of later edits and require manual reconciliation; do not
delete their journal merely to force bootstrap to proceed.

### Physical acceptance gates

**A: non-disruptive observation first.** Implemented `sparkctl observe --saved-plan
LOCAL_PLAN --output NEW_PRIVATE_JSON --timeout 15` uses bounded metadata SSH/GET,
partial per-node results and no mutation lock/state creation. `--output` is an
exclusive file, not a directory. `status`, `preflight` and generation-sending `probe`
are not substitutes.
Report each node reachable/timed-out/unknown with exact owners, IDs, pins, reservations,
routes and management path; compare critical identities before/after. Missing/stale
or inconsistent evidence is not green. Preserve partial output and nonzero status.

Create exclusive ignored `data/cluster/operations/<run-id>/` (0700, files 0600).
Keep exact plan copies, source/controller pins, allowlisted receipts/fingerprints,
secret locations/access dependencies, not secret values. Retain sanitized historical
`/tmp` receipts if present; missing is missing, never reconstructed success. Opt-in
logs are owned-ID/time/size bounded and redacted; no prompts, environments or cache
scans. No inference, package/pull/download, network changes, restart or reservation
write in this phase. The later approved service is intentionally allowed bounded
polling and owned transitions; these observer restrictions do not prohibit it.

**B–E: CPU proof before hardware.** Exercise actual recovery primitives using isolated
fake transport/clock/engine/ingress boundaries: either node loss, failed-owner silence,
lease expiry/stale generation, concurrent controllers, restart at each persisted step,
dropped replies, delayed stale stop, malformed/corrupt journal, disk-full, flapping,
missing artifacts/ineligible fallback, drain timeout, endpoint/auth/capability mismatch,
fallback startup failure, TP2 startup failure with rollback, rollback failure, policy
pause/disable and exact-plan mismatch. Tests assert observable no-double-start,
no-stale-route, no-unowned-stop and deterministic convergence, not command strings.

**F: scheduled physical acceptance**, approved only after recording exact live plan,
worker IDs, reservations, gateway snapshots and deterministic stop/restore procedure:

- Qualify coder separately on e8f1 and 66f1, including tools, continuation and SSE;
  record exact model/image/tokenizer/plan IDs and independent serving address.
- Lose 66f1, recover coder on e8f1; restore 66f1/fabric and observe **automatic TP2**.
  Repeat with e8f1 lost and 66f1 surviving; coordinator-role placement stays exact.
- Remove the physical cable while independent Wi-Fi/Ethernet management stays up;
  qualified single remains reachable; cable/node stable return automatically restores TP2.
- Fail preferred startup after drain; observe owned cleanup and saved-single rollback,
  preserved stable URL/auth, explicit backend limits and persistent retry/backoff.
- Exercise flapping, authority process crash/reboot, stale gateway generation and
  asymmetric network partition; verify safe unavailable behavior where fences cannot prove.
- Under separate fault-test approval, exercise Mac sleep/power/network loss: record
  stable-gateway unavailability, suspended recovery and routing-lease expiry without
  GPU ownership release. Restore wake/power/connectivity manually where needed and
  verify reconciliation before routes or mutations resume. Separately prove that
  closing OMP/VS Code/terminal sessions does not stop the supervised services.
- Exercise absent pins/no eligible fallback without download/substitution; wrong endpoint,
  stale model advertisement, unauthorized request, tool/vision/context mismatch fail correctly.
- Record all generations, owners, full digests, container IDs, timings, errors, actual
  transport/cable rates and request outcomes. No inferred acceptance from CPU mocks.

Canary with recovery disabled/shadow decisions first, then explicitly enable one
policy and a controlled opt-in client cohort in a maintenance window. Observe the
complete symmetric failure/return matrix; publish measured recovery intervals, not
an invented RTO. Do not switch existing strict-model clients to `local-auto` silently.
Disable prevents new automatic transitions but preserves the verified current worker;
complete/reconcile any in-progress operation safely. Rollback restores the recorded
controller release, compatible journal/schema backup, routes and exact saved deployment
under fencing/drain; never downgrade a journal blindly or delete recovery tombstones.

Permanent owner: name an operator for the Mac's awake/power/network availability,
authority backups, trust/secrets, qualification expiry, alert response and policy
reviews; a release maintainer owns schema upgrades and restoration drills. Document
manual Mac wake/power/connectivity restoration and control-plane reconciliation, and
periodically rehearse approved hardware recovery. Completion requires both software
proof and physical receipts; until then label automatic enablement **not qualified**.
