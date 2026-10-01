# Durable goal: automatic Spark recovery and local-first operations

Status: local software implemented and verified; readiness review updated September 30, 2026. Physical
deployment/acceptance and automatic recovery enablement require separate approval;
recovery is not installed, enabled or live-qualified. This document is the durable
goal record: a native harness `/goal` tool is unavailable, so no native tracker was
activated. Checked software items below are local evidence, **not** completion of
the full physical recovery goal.

## Exact objective

Implement the full [local-first operations plan](LOCAL_FIRST_OPERATIONS_PLAN.md):
preferred exact GLM TP2 deployment → existing, qualified single-node recipe on the
surviving Spark → **automatic failback to the exact preferred TP2 plan once the
returned node and collective fabric remain healthy and all safety gates pass**.
Either Spark may fail; a dead node's acknowledgement must not be required to recover
a safely fenced survivor. **The user selected this Mac, kept awake, to host the
authority/controller and stable gateway.** Implemented launchd packaging targets
independence from OMP, VS Code, terminal and remote-agent sessions, but depends on
this Mac remaining awake, powered, connected and logged into the service user. An explicit maintenance pause
remains available; manual-only return to TP2 is not completion.

The Mac is a single point of failure, not HA. Sleep, power loss or lost required
connectivity makes the stable gateway unavailable and suspends recovery; authority
loss stops route-lease renewal, and expired routes fail closed without releasing
GPU ownership. Manual wake/power/connectivity restoration may be required before
services reconcile durable state and actual ownership and resume. This host choice
does not approve service installation, always-awake/network/trust provisioning,
hardware faults or activation; those gates remain open.

Use the existing tool-capable coder recipe for the agent fallback, subject to
per-node qualification. Existing coder acceptance with e8f1 serving through both
gateways does not qualify coder66. Text-only fast is a separate explicit policy,
not a substitute for tools. Preserve strict model aliases and client defaults;
model substitution requires opt-in `local-auto` with real backend identity and limits.

Deliver simple, repeatable setup/reset/coordinator switching, independent Ethernet
or Wi-Fi management/serving fallback, more-node/new-recipe admission, and correlated
service/GPU/memory/fabric/recovery monitoring. Cable restoration is a readiness gate,
not permission to run NCCL on Wi-Fi. Keep Compose on the Sparks, launchd on the
selected Mac and sparkctl as the default; Linux/systemd is an optional portability
target, not required third-host hardware. K3s is not required and does not create
GPU capacity or two-node quorum HA.

## Work location and preservation boundary

- Original checkout: `/Users/statsparrot/projects/local-llm-stack`.
- Implementation worktree: `/Users/statsparrot/projects/local-llm-stack-recovery`.
- Implementation branch: `feat/spark-automatic-recovery`.
- Preserve the original checkout, its untracked main plan and pre-existing modified
  documents. Do not copy implementation edits back, reset, clean or commit there.
- Local source changes, CPU simulation and documentation can proceed in the isolated
  worktree. Do not install packages, synchronize remote releases, change networks,
  reserve/stop/start GPUs, generate model requests or activate recovery without the
  corresponding explicit approval. Preparing implementation does not authorize rollout.
- Before any production stop, restart, model switch, network change or potentially
  disruptive fault test, describe the expected interruption and recovery path and
  obtain explicit approval. Resuming implementation is not maintenance approval.
  Run an approved stop/start transaction under an independent supervisor; an agent
  using GLM must not stop its own inference backend and depend on another model
  response to issue the startup command.
- Both Spark GPUs were occupied in the recorded baseline. Historical observations
  and receipts are dated evidence, not a fresh check of current service.

## Completion checklist

### Local implementation and reproducibility

- [x] Mutation-free bounded observer reports both nodes, preserves partial failures,
  validates exact live identities and writes private/redacted evidence.
- [x] Strict versioned policy, immutable plan/qualification references, persistent
  authority journal, epoch/operation idempotency and crash reconciliation exist.
- [x] Exact saved-plan restore never re-renders or rewrites the saved plan; checks
  pins/ownership/Compose trust and reports actual worker/container identities.
- [x] Single-node admission, serving bind, endpoint publication and health checks
  are independent of missing collective fabric; old immutable plans remain intact.
- [x] All managed ingress and survivor ownership are generation-fenced; unknown
  failed-node ownership stays quarantined/tombstoned without requiring its ACK.
- [x] Automatic failover, stable-health automatic failback, global bounded drain,
  startup rollback, persistent backoff/circuit and pause/disable are implemented.
- [x] Strict aliases remain exact; opt-in policy requests validate actual backend
  capabilities, pinned tokenizer context/output limits, identity and authentication.
- [x] No replay of generation/tool requests, no stream/KV migration, no overlapping
  GPU claims, and no unrelated process/container/cache removal.
- [x] Existing monitoring stack correlates end-to-end requests, supported GPU/unified
  memory and engine metrics, per-direction physical-link speed/rate, hardware RDMA
  counters and recovery events; unsupported measurements are visibly unavailable.
- [x] Pinned service packaging/setup/reset and both coordinator roles are reproducible;
  authority transfer, enrollment and new-recipe admission have explicit safety gates.
- [ ] Durable launchd services on the selected Mac are independent of OMP/VS Code
  and terminal sessions; awake/power/network dependencies, authority loss/restart
  behavior and manual host-restoration conditions are implemented and documented.
  Foreground gateway lifecycle and service ownership/crash-repair are locally
  verified; installed launchd/session-loss proof remains blocked on installation approval.
- [x] CPU acceptance covers symmetric loss/return, lease expiry/stale routes,
  concurrent controllers, every crash boundary, dropped replies, partition/flapping,
  drain/startup/rollback failures, missing artifacts and endpoint/auth/capability errors.
- [x] Changed source, CLI help, tests and runbooks agree; evidence records exactly
  what ran rather than implying physical acceptance from mocks or documentation.

### Approved physical qualification and operational ownership

Host selection is complete: this Mac, kept awake. The unchecked items below cover
provisioning, qualification and enablement, not another authority-host selection.

- [ ] Selected Mac launchd installation, always-awake/power/network configuration,
  SSH trust and independent management/serving paths to each Spark are separately
  approved and provisioned; legacy/direct unfenced paths are excluded.
- [ ] Closing OMP/VS Code/terminal sessions leaves supervised services running.
  Approved Mac sleep/power/network faults demonstrate gateway unavailability,
  suspended recovery and lease expiry without GPU ownership release; manual
  wake/power/connectivity restoration is followed by safe reconciliation.
- [ ] Coder e8f1 and coder66 each have valid exact-plan text/tools/continuation/SSE
  receipts through the actual stable gateway and independent serving path.
- [ ] Loss of 66f1 produces qualified e8f1 single service; stable node/fabric return
  automatically restores exact TP2, with recorded generations and container IDs.
- [ ] Loss of e8f1 produces qualified 66f1 single service; stable node/fabric return
  automatically restores exact TP2, including the correct saved coordinator identity.
- [ ] Cable-only loss with independent Wi-Fi/Ethernet management reaches safe single
  service; cable stability automatically restores TP2 without Wi-Fi collectives.
- [ ] Failed preferred startup rolls back to the saved single deployment; flapping,
  controller restart, stale routes and asymmetric partitions preserve ownership/fencing.
- [ ] Missing pins/ineligible fallback remain explicitly unavailable; stable endpoint,
  authentication, real model identity and reduced capability/context behavior pass.
- [ ] Canaries, measured recovery intervals, disable/rollback and exact restoration
  receipts are recorded; enablement is approved rather than inferred from tests.
- [ ] Named operator owns authority backups/trust/secrets, alerts, qualification
  freshness and restoration drills; release owner owns schema compatibility.

## Commit readiness review — September 30, 2026

Final integrated gate: `./tests/test.sh -rs` — **669 passed, 4 skipped** in
150.37 seconds. The four skips require Linux `/proc` or Linux rsync; they are not
claimed as verified on this Mac. All imported Spark runtime modules were confirmed
to come from this recovery worktree. All-profile Compose validation passed, all
12 shell scripts passed syntax checks, and all four new CLI entrypoints loaded.
A fresh actual `validate → init → run --once → status` smoke with isolated CPU-only
inputs remained disabled, epoch zero, with no selected deployment.

The recovery branch incorporates committed `main` at
`47c5f1b07c8a2bee228e7f839da15df4c3cc5823`, including the four-image GLM recipe.
This does not rewrite historical saved plans or change a running deployment.
The original checkout's uncommitted documentation stays outside this worktree.

Review found and repaired five software defects before committing:

- Startup artifacts are separated by exact plan within a transition, allowing real
  `cli.up` to restore a different fallback after preferred failure or survivor reselection.
- Explicit preferred/fallback serving bindings must match; only a legacy preferred
  plan without that field permits adding independent fallback serving.
- CLI coordinator changes validate against the durable current preference, so
  switching to the alternate coordinator and back is possible.
- Interrupted SSH transactions verify before/after snapshots before rollback.
  Unknown later operator edits and ambiguous old journals are preserved and refused.
- Service monitor timing validation agrees with the actual collector's limits.

The targeted recovery, enrollment and service regressions passed **76 tests**.
The first three defects were reproduced with failing regressions before repair;
monitor validation and SSH edit loss were reproduced separately in temporary local
state. The enrollment runbook now uses the emitted canonical `inventory_sha256`,
not a raw file checksum. Section 9 of the operations plan gives repeatable local
test commands and clearly separates physical maintenance approvals.

No production SSH, model requests, service installation, routing changes or worker
restarts were performed for this readiness review. The September 16 observations
below remain historical evidence, not a fresh claim about today's running service.

## Verified local evidence — September 16, 2026

- Final worktree gate: `.venv/bin/python -m pytest` — **656 passed, 4 skipped**
  in 151.94 seconds. CPU tests simulate node/GPU failures; they are not physical
  qualification. The preserved coordinator-preparation regression failed before
  the fix and passed afterward: an unprepared switch target leaves the old route,
  preferred plan and workers untouched. Startup failure still exercises exact rollback.
- A joined real loopback HTTP scenario exercised controller → authenticated
  gateway/SSE → monitor across preferred, surviving single and automatic preferred
  restoration. GPU/SSH state and backend generation were simulated; generation
  headers advanced and strict preferred aliases rejected fallback substitution.
- Actual `spark-recover validate`, `init --apply`, `run --once` and `status` ran
  against private CPU-only inputs with unreachable fixture management targets.
  The resulting authority remained disabled, with no selected plan or GPU action.
  `spark-services` foreground gateway start, authenticated closed admission,
  SIGTERM shutdown and port release were exercised without installing LaunchAgents.
- Disposable local Prometheus/Grafana services scraped the read-only Spark
  exporter. **All 54 dashboard PromQL queries succeeded**, with 38 returning data.
  Browser screenshots show actual native engine, unified memory, CPU/GPU,
  negotiated physical speed and per-direction cable traffic. Unsupported GPU
  memory readings remain unavailable. `promtool test rules` passed the selected-
  engine alert scenario: inactive saved plans do not page, selected failure does,
  and recovery clears the alert. No GPU inference or saturation benchmark was
  generated by this monitoring verification.
- Final bounded read-only observation found both GLM workers healthy, restart
  counts zero, and the same IDs as the separately approved incident restoration:
  `66f1:14be9ccac15e…`, `e8f1:9eb350f1815c…`. Exact full-plan SHA-256 remains
  `69aa5dc93ea3b1fafe73cf2dfaaed43973dabcca9d01f3c10ff19d5eb4a3cf4e`;
  deployment digest remains
  `b1cf7901f81c861758f5f7c14ba7cea5605519713c4fc4816f1e06af41eb3bb2`.
  The original checkout's uncommitted `max_images: 4` edit remains intact; it was
  not applied to the exact restored one-image deployment or copied into this worktree.
- Private ignored evidence lives at `data/cluster/recovery-dev-a80329ec/`:
  `recovery-cli-smoke.json`, `dashboard-query-verification.json`,
  `inactive-engine-alert-result.json`, `final-readonly-observation.json`,
  `monitor-hosts.webp`, `monitor-fabric.webp`, and the dated incident receipts.
  Disposable monitoring processes/containers/network and the restoration helper
  were removed after verification. The production gateway is unchanged; it does
  not expose the new recovery/metrics feeds. Missing feeds are not green.

The next gate is separately approved installation and stable independent
management/serving/trust provisioning, followed by scheduled per-node coder,
ingress-isolation and physical failover/failback qualification. The existing Mac
forwarder is not an independently supervised recovery gateway. Monitoring over SSH
does not prove that the Mac can serve every saved endpoint. No cable/host/power
fault was injected, no new recovery services were installed, and no policy was enabled.

## Continuing work without another architecture round

Treat [LOCAL_FIRST_OPERATIONS_PLAN.md](LOCAL_FIRST_OPERATIONS_PLAN.md) as authoritative
for policy/state machine, fencing, addressing, request semantics and phase dependencies.
Use [SPARK_SCALING_RECOVERY_PLAN.md](SPARK_SCALING_RECOVERY_PLAN.md) for physical topology
and enrollment detail; [CLUSTER_IMPLEMENTATION.md](CLUSTER_IMPLEMENTATION.md) records
the broader implementation checklist. Existing GLM acceptance remains model evidence,
not evidence that this recovery service has been installed or physically qualified.

Continue reachable local implementation until all software criteria have actual
proof. Keep one owner for shared schema/journal integration, and parallelize node,
ingress and monitoring work only after the contracts are fixed. Inspect the current
worktree before changing files; preserve other contributors' changes. Record concrete
completed capabilities and their evidence here; do not check boxes on intent alone.

If a physical gate cannot proceed, finish the local software and state the exact
missing prerequisite (selected Mac service/trust/network provisioning, maintenance
approval, per-node GPU or fabric qualification). Do not mark the full goal complete,
fabricate live results,
weaken automatic failback to manual intervention, or stop at a plausible scaffold.
An operator approval boundary is not authorization to perform live fault injection.

## Rationale

The failure is a coupled model group, so restarting one rank or adding a replica
label cannot solve it. Safe useful fallback requires independent control/serving
paths, durable ownership and fenced ingress before reusing the survivor GPU.
Automatic return requires stable collective fabric, known pins, bounded drain and
rollback because TP2 and fallback cannot coexist on the same occupied resources.
Standard supervised foreground services—Compose on Sparks and launchd on this
Mac—plus a private atomic journal, immutable plans, expiring route leases, rollback
and observability address those risks with a smaller operational change than a
Kubernetes migration. Linux/systemd remains a portability option. Any later
orchestrator must preserve these contracts; its choice neither creates capacity
nor removes the Mac's awake/power/network dependency. Real per-node/cable and
control-host receipts, not scheduler branding or synthetic capacity arithmetic,
determine whether the service can be enabled.
