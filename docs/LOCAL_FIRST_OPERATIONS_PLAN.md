# Local-first operations: non-disruptive GLM recovery readiness

Status: proposed implementation plan, September 16, 2026. Writing this plan does
not authorize or perform a rollout, model request, restart, or recovery operation.

## Goal and first deliverable

Operate from the Mac checkout of `local-llm-stack`, with OMP and VS Code local.
Neither Spark needs a VS Code window or remote coding agent open. The Sparks
remain execution hosts with deployed code, models, credentials and persistent
state; the local checkout is not a live filesystem mirror.

The next deliverable is **one bounded, read-only observation command run from
the Mac**, plus a private evidence bundle and a human-readable recovery-readiness
report. It must describe both nodes even when one cannot be reached. It must not
try to fix anything. Successful delivery establishes observation and recovery
preparedness, **not tested failover or automatic recovery**.

This is the non-disruptive first slice of the existing
[recovery plan](SPARK_SCALING_RECOVERY_PLAN.md#recovery-and-availability-plan-item-4),
not a second controller or a replacement for the deployment model.

## Starting point: verify rather than assume

The source-alignment check found all three baseline checkouts clean on `main` at
`c7adb11297f72824d60491580a41eecbc08b1101`, with all 374 tracked files identical.
Both installed controllers remained pinned at `696af2a`. These are dated
observations, not requirements that prohibit later reviewed releases.

The last verified serving deployment was:

- Alias `local-glm53-flash`, TP2/DFlash2, coordinator `e8f1`, worker `66f1`.
- Owner `glm53-tp2-256k-dflash2-e8f1-b1cf7901f81c`.
- Saved plan on both nodes:
  `~/projects/local-llm-stack-cluster/state/glm53-20260913/e8f1-04/plan.json`.
- Model API port 8125; existing guards on 4010 and managed gateways on 4110.
- Model caches and runtime overlays still under `~/projects/local-llm-stack/data/`.
- Separate gateway snapshots/credentials under `~/.local/state/local-llm-cluster/`.

Resolve the live plan against actual reservations and container identities before
using it. Do not use a directory named `current`, a friendly alias, a historical
status file, or a freshly rendered desired plan as proof of the live deployment.

## Serving-protection boundary

Allowed: local development and CPU-only fixture tests; bounded SSH metadata
reads; filtered Docker inspection; small configuration/receipt reads; narrowly
scoped health/model-discovery GETs if their implementation is confirmed not to
trigger inference; private writes to the Mac's ignored evidence directory.

Forbidden in this milestone:

- Generation, tokenization, warmup, benchmarks, qualification, soak or GPU tests.
- Model/container/service start, stop, restart, recreation or restart-policy changes.
- GPU reservation changes, lock creation, cache clearing, route publication,
  controller installation, package installation or remote source synchronization.
- Changes to SSH configuration, interfaces, fabric, credentials or client URLs.
- Model downloads, image pulls, full cache rehashing, recursive cache/database scans.
- Fault injection, failover activation, cleanup of old releases or runtime files.

Read-only is not zero overhead. Use small bounded requests, at most one active
inspection per host, one baseline and one completion observation, and no polling
loop. Default to metadata only; log collection is opt-in, restricted to explicit
owned container IDs, bounded by both time and size, and redacted before saving.
Do not save conversations, prompts, full Docker environments, or credential values.

If identity or protected state changes, stop the collection and mark the report
inconclusive. If a host is unreachable, preserve a partial report; do not assume
its GPU is free or its process dead. Never release a reservation or initiate repair.
There can be no absolute latency guarantee from metadata alone; do not claim one.

## Reuse existing interfaces, account for their limits

| Existing surface | Role in this work |
| --- | --- |
| `scripts/sparkctl`, `tools/spark_cluster/cli.py` | Keep the same local CLI and SSH transport; no new resident daemon or SSH library. |
| `tools/spark_cluster/config.py::validate_saved_plan` | Validate the exact captured recovery identity locally without re-rendering it. |
| `tools/spark_cluster/node.py::verify_host` and `status` | Reuse identity/metadata logic where safe; make the observation path independent of mutation locks. |
| `cluster/inventory.json` and saved-plan node records | Existing transport and node identities; report which SSH route was used. |
| `scripts/install-spark-controller.py` | Remains the separate release installer; never invoked by observation. |
| `docs/GLM53_TP.md` and `docs/SOURCE_RECONCILIATION.md` | Serving identity, source/state separation and deployment safeguards. |

Current `sparkctl status` enters `node.py::locked()`, which can create the state
directory/mutex and block behind a mutation. Its aggregate inspection also aborts
on a failed node call. It is not yet the strict read-only, partial-result interface
required here. Current `sparkctl probe` sends a chat-completion request.
`preflight` is launch-oriented and may bind candidate ports; it is not a substitute
for observing an occupied deployment. Do not run these as an unexamined health suite.

The inventory uses `spark-66f1-wired` and `spark-e8f1-wired`; the latter currently
jumps through 66f1 from the Mac. OMP's saved SSH names are separate configuration.
Record that management dependency. Do not silently rewrite the saved plan or
switch transports to manufacture a healthy result. Independent management access
belongs to the later recovery design.

## Implementation sequence

### A. Add the smallest read-only observation path

Proposed interface, **not implemented yet**:

```text
python3 scripts/sparkctl observe --saved-plan LOCAL_PLAN --output LOCAL_REPORT_DIRECTORY
```

Extend the existing CLI and host helper; do not add a parallel deployment system.
The new action must validate host identity and a supplied saved plan, then read
only. Do not call the mutation dispatcher, take its lock, or import code that
creates remote state. Use existing SSH authentication and no interactive prompts.
Add explicit bounded subprocess deadlines to the observation path; the current
240-second generic transport timeout is not its acceptance target.

Report per node: reachable/unreachable/timed-out, observed hostname, source and
controller revisions, plan owner/digest, reservation phase and container IDs,
actual container owner labels/digest/IDs, state, start time, health and restart
policy, image identity, relevant mounts, and gateway route identity. Check expected
node membership and exact ownership rather than accepting any healthy container.

Report `healthy-observed`, `degraded`, or `unknown`; missing, stale, inconsistent
or unreachable evidence must never be green. A single-host snapshot is not an
atomic cluster snapshot. Compare critical identity before/after observation and
report concurrent change instead of presenting an inconsistent result as verified.

Return a nonzero exit for degraded/unknown observations while still writing the
partial report. Keep existing command contracts unchanged. Use a versioned,
allowlisted report schema rather than storing raw Docker or environment dumps.

### B. Gather a private recovery-readiness bundle from the Mac

Use a new, exclusive directory under ignored `data/cluster/operations/<run-id>/`,
mode 0700, with evidence files mode 0600. Never overwrite earlier evidence.

1. Retrieve the small exact saved plan from each reachable node, validate it
   locally and compare its identity with observed owners, reservations and ranks.
2. Record source revisions, image pins, relevant overlay identities, saved-plan
   locations, route registry fingerprints and the selected controller release.
3. Record locations and access requirements for secrets, not their contents.
   This is not a credentials/database backup; document those separate dependencies.
4. Retain sanitized copies of the final reconciliation test/validation receipts
   currently held in `/tmp`, if still present. Missing historical receipts must be
   reported, not reconstructed or represented as fresh passing tests.
5. Produce a concise local report: what is running, how each component is reached,
   which source/state it depends on, what can be observed, and what blocks recovery.

Do not commit live plans or private reports. Only sanitized, reviewed summary
results belong in versioned documentation. No model weights or application data
are transferred. The Mac remains sufficient for operating the report.

### C. Write the recovery decision table, without executing it

Use the existing recovery roadmap and exact-plan safeguards to distinguish:

| Observation | Required decision in this milestone |
| --- | --- |
| Both ranks and gateways match the expected deployment | Leave serving unchanged. |
| Gateway unavailable, workers healthy | Report the gateway boundary; no restart or route switch. |
| One rank exited | Report the TP group degraded; no independent rank restart. |
| Host or fabric unreachable | Ownership unknown; no lease release, failover or cleanup. |
| Plan, route or container identity mismatch | Stop and require reconciliation, never substitute another model. |
| Controller process exits | Record how to reconnect to existing ownership; do not launch duplicates. |
| Host reboot/restart needed | State the coordinated-recovery gap and required maintenance approval. |

GLM currently uses Docker restart policy `no`. Do not turn it into `always` as a
shortcut. The existing CLI does not offer exact-saved-plan `up`; document that
capability gap rather than inventing a recovery command or launching a re-rendered
plan. Actual recovery execution/fencing and a stable gateway frontend remain later
work requiring separate implementation and maintenance-window acceptance.

### D. Prove the new observer safely

First use local CPU fixtures: healthy ownership, missing rank, unreachable host,
SSH timeout, stale reservation, wrong digest, mismatched routes, concurrent state
change and malformed saved plan. Tests should defend classification and preservation
contracts, including rejection of remote mutations—not command-string snapshots.

Then run one bounded observation from the Mac against the current deployment.
Do not rerun model acceptance or send a synthetic prompt. Compare before/after:
worker IDs/start times, restart counters, reservation identities/phases, route
fingerprints, pinned `current` targets, and protected configuration fingerprints.
Only compare stable identity/configuration fields; natural request counters, logs
and caches can change while real users continue inference.

## Acceptance and stopping conditions

- All work is initiated from the local checkout; no remote VS Code/OMP session is needed.
- One report covers both nodes, or clearly records partial/unreachable evidence.
- Every critical health assertion is tied to the exact live deployment identity.
- No remote persistent write, generation request, restart or serving-policy change.
- The live observation has a 60-second overall deadline, with per-operation
  deadlines and no automatic retry loop; preserve partial evidence on timeout.
- Worker identities/start times and protected configuration remain unchanged.
- Local reports are private, ignored and redacted; saved-state locations are documented.
- The user receives the report, a recovery decision table, and explicit unresolved gaps.
- Stop at delivery of evidence. Do not proceed automatically to a maintenance window.

The operational claim is deliberately limited: **non-disruptive observation was
exercised and no deployment/state changes were observed**, not that failover,
reboot recovery, loaded-code identity or inference latency has been fully qualified.

## Deferred work and approval boundary

Do not fold these into this task: 66f1 GPU/model/Vector/Loop acceptance, service
migration to different source paths, independent management-network setup, gateway
failover activation, package parity, automatic enrollment, DeepSeek concurrency or
compaction, remote drafting, Kubernetes, or cleanup of old state/releases.

After reviewing the observation report, select a separate scheduled recovery or
66f1 acceptance window with an explicit stop/restore procedure. Until then, leave
GLM serving on both GPUs.
