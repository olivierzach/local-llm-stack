# Non-serving Qwen3-4B TP2 → Mac mini experiment

Release: **`live-tp2-mini-v1`**. Recipe: [`cluster/workloads/qwen3-4b-tp2-mini.json`](../cluster/workloads/qwen3-4b-tp2-mini.json).
Admission helper: [`scripts/spark-mini-tp2`](../scripts/spark-mini-tp2).
Dedicated producer and full launch/cleanup instructions live in the sibling
[disagg-lab TP2 reproduction runbook](https://github.com/olivierzach/disagg-lab/blob/experiment/spark-prefill-mini-decode/experiments/mini/tp2-reproduction.md).
Pin an immutable lab commit from that branch before staging either host.

This is an operator recipe, **not** a `sparkctl` vLLM deployment/model registration.
Do not feed it to `sparkctl up` or add it to a production route registry.
The two GB10s run real BF16 tensor-parallel Qwen prefill only: rank0 on
`spark-66f1`, rank1 on `spark-e8f1`. Rank0 sends the existing wire-compatible
split to `live_mini.py` for CodeNeedle decode on the mini. Neither Spark decodes.
The original TP1 producer remains unchanged. No measured TP2 correctness,
performance, quality or successful hardware run is claimed by this recipe.

## CPU-only validation and planning

From this repository, without SSH, CUDA, Metal, or service changes:

```bash
python3 scripts/spark-mini-tp2 validate
python3 scripts/spark-mini-tp2 --help
```

The first command validates the non-serving recipe and inventoried rank identities;
it does not prove admission, installed dependencies, fabric connectivity, or producer
correctness. Use the lab runbook's CPU validation for the producer/wire contract too.

Prepare a **fresh private** run directory (the helper creates it):

```bash
RUN="data/cluster/mini-tp2/$(date -u +%Y%m%dT%H%M%SZ)"
LAB_REVISION=$(git -C ../disagg-lab rev-parse HEAD)
python3 scripts/spark-mini-tp2 plan --job "tp2-$(date -u +%Y%m%d%H%M%S)" \
  --lab-revision "$LAB_REVISION" --output "$RUN"
python3 scripts/spark-mini-tp2 validate --saved-plan "$RUN/plan.json"
```

Check that this is the intended committed lab revision, not merely a checkout with
uncommitted code. The plan pins the recipe, inventory, lab revision, unique job,
owner and digest. `$RUN/rank0.json` and `$RUN/rank1.json` are controller request
identities, **not proof of a lease**. Preserve all three files through recovery.
Do not publish runtime plans/receipts, host state, or private logs with the recipe.

## Exclusive window: both Sparks AND mini

**This collides with production DeepSeek TP2 on both Sparks and with production
Qwen on the mini. It is not a side-by-side deployment.** Before any admission:

1. Obtain an explicit bounded maintenance window for all three GPUs. Record a UTC
   deadline, current workload owners, exact production saved plans, source revisions,
   deployment/recipe inputs, service states, and restore commands privately.
2. Drain clients/new work without editing aliases, model settings, routing registries,
   OMP roles, or the production default. Expect a temporary unavailable route rather
   than substituting the experimental producer. Arrange an operator to restore even
   if the controller terminal/SSH disappears. A deadline is not automatic restoration.
3. Save the current **actual** DeepSeek plan and exact source/config inputs needed to
   recreate it. Do not substitute a historical/default profile. Use
   `scripts/sparkctl status --saved-plan "$PRODUCTION_PLAN"` and, only in the approved
   window, `scripts/sparkctl down --saved-plan "$PRODUCTION_PLAN"`. Stop other GPU
   workloads only through their owners (including speech if active); never kill
   unidentified processes or delete reservation files.
4. On the mini, drain and stop the existing `local.llm-stack.qwen-mini` worker through
   its recorded launchd management procedure; retain its plist and prior state for
   restart. Confirm its process actually exited and no other Metal job is running.
   The Spark controller cannot reserve the mini: this is a separately coordinated
   exclusive mini window, not a fabricated third GPU lease. Leave its tunnel and
   all production route/configuration files unchanged.
5. Run `python3 scripts/sparkctl doctor` to inspect both Sparks. Resolve existing
   owners/research windows through their owning projects. Never weaken admission
   or claim that an idle-looking GPU overrides another owner's lease.

## Real paired admission, then launch

Run as the same inventoried Unix account used by the rank processes. Reservations
are per Unix user; unrelated accounts/unmanaged tools do not participate.

```bash
python3 scripts/spark-mini-tp2 reserve --saved-plan "$RUN/plan.json"
python3 scripts/spark-mini-tp2 status --saved-plan "$RUN/plan.json"
```

Both commands must succeed before either rank starts. `reserve` calls the existing
`spark_cluster.cli.remote` → `node.py reserve-workload` controller on **each** host,
with the saved owner/digest and memory admission floor. The controller checks GPU
processes, GPU containers, other reservations, unresolved research windows and
available shared memory. The leases protect the real producer workload; no dummy
process/gate is used. `status` reads the real host reports and fails unless both
reservations match owner/digest and `phase=workload`.

Paired acquisition is sequential, not atomic. If rank1 refuses or a reply is lost,
**do not launch either rank**. The helper deliberately retains possible rank0/rank1
leases rather than guessing whether a process launched. Inspect with the saved plan,
resolve the cause, then retry the same admission or follow explicit cleanup below.
Never create a new owner to bypass an ambiguous admission.

Stage the same committed dedicated lab release/dependencies and pinned BF16 snapshot
on each host as specified by the lab runbook. Copy `$RUN/rank0.json` to rank0's
`<DEDICATED_RELEASE>/admission-request.json`, and rank1's corresponding file to rank1.
Give the mini both original files. The producer must verify the **real local**
`~/.local/state/local-llm-cluster/gpu.json` against its request before CUDA work;
passing a request file alone is not admission.

Launch through direct SSH/systemd on each host, not a two-process local launch:

Use `/home/statsparrot/disagg-lab-releases/live-tp2-mini-v1` as
`<DEDICATED_RELEASE>` on each Spark; its pinned fixtures are
`fixture-manifest.json` and `cases/`.

| Environment | rank0 / spark-66f1 | rank1 / spark-e8f1 |
| --- | --- | --- |
| `WORLD_SIZE` | `2` | `2` |
| `RANK` | `0` | `1` |
| `LOCAL_RANK` | `0` | `0` |
| `MASTER_ADDR` | `10.10.20.1` | `10.10.20.1` |
| `MASTER_PORT` | same operator-selected free private port | same port |
| `NCCL_SOCKET_IFNAME` | `enp1s0f0np0` | `enp1s0f0np0` |
| `GLOO_SOCKET_IFNAME` | `enp1s0f0np0` | `enp1s0f0np0` |

Verify these fabric addresses/interfaces against the saved inventory and live host
before launching. Rank1's first-rail address is `10.10.20.2`. Do not use public
listeners or expose rendezvous to the LAN. The workload admission helper does not
check rendezvous port availability. No inference about RDMA performance follows
from setting these interfaces.

Follow the linked lab runbook for the exact dedicated producer entry point, systemd
units, prepared manifest/cases, endpoint/transport, and mini invocation. In order:

1. Start `live_mini.py` in the approved mini window with `--lease-request "$RUN/rank0.json"`
   and `--tp-peer-lease "$RUN/rank1.json"`. Use `tp_pair.py run` as its producer command.
   The mini builds native references and publishes the **local** ready marker before either Spark starts.
2. Confirm both live leases, mini exclusivity, the actual remaining deadline, private fabric
   firewall policy and staged fixture identity. Use `tp_pair.py gate` with both saved requests;
   it verifies remote ownership and publishes a fresh gate, not a lease.
3. The mini then spawns `tp_pair.py`, which starts one bounded SSH/systemd worker on
   each Spark, pins the fabric and relays only rank0's protocol. **Do not** start separate
   rank processes or pass the TP1 worker. The mini checks both rank identities on readiness
   and checks both ranks' per-request contribution before decoding. Retain private logs.
4. Collect correctness/qualification evidence before any performance claim. On
   error/deadline, stop the experiment; do not fall back to TP1, Spark decode,
   production routing, or an unleased retry.

## Cleanup and exact production restoration

1. Stop the mini experimental consumer and **both** identified rank units/processes
   using the lab runbook. Wait for process exit on each host; check for surviving
   child CUDA processes. A closed SSH terminal or one dead rank is not cleanup.
2. Release both leases using the same saved plan, even after partial admission:

   ```bash
   python3 scripts/spark-mini-tp2 release --saved-plan "$RUN/plan.json" --ranks-stopped
   python3 scripts/sparkctl doctor
   ```

   `--ranks-stopped` records operator intent, not fabricated verification: each real
   `release-workload` still checks ownership and refuses while GPU processes or GPU
   containers remain. Both hosts are attempted even if one release fails. On failure
   preserve the plan/remaining lease, stop only this job's processes, then retry.
   An absent reservation is idempotent; a different owner is never released. Check
   both doctor reports for absence of this experiment's lease and GPU processes.
   Do not restore old `gpu.json`, unlink leases manually, or treat an unreachable
   peer as released. `spark-mini-tp2 status` intentionally fails after release.
3. Restore DeepSeek from the exact production inputs/source saved before teardown.
   Render to a fresh directory using `scripts/sparkctl render --deployment
   "$PRODUCTION_DEPLOYMENT" --output "$RESTORE_RENDER"` (include the original
   `--recipe-file` override if used). Compare rendered plan owner/digest and full
   inputs to `$PRODUCTION_PLAN`; any mismatch blocks startup. Run `preflight
   --saved-plan "$RESTORE_RENDER/plan.json"`, then `up --deployment
   "$PRODUCTION_DEPLOYMENT" --output "$RESTORE_RUN"` with the same explicit recipe
   override and appropriate timeout. **`up --saved-plan` is not supported.** See
   [the cluster recovery contract](CLUSTER.md) and the station's private approved
   exact-plan recovery record; do not choose a default model/recipe to get green.
4. Check `scripts/sparkctl status --saved-plan "$RESTORE_RUN/plan.json"` and
   `scripts/sparkctl probe --saved-plan "$RESTORE_RUN/plan.json"`. Require the saved
   production owner/digest, healthy workers on both hosts, and a real completion.
   Restore any speech jobs stopped for this window through their own memory guards.
5. Restart the mini production worker only if it was running before the experiment
   (existing installed agent: `launchctl kickstart gui/$(id -u)/local.llm-stack.qwen-mini`).
   Check its real `/health` and `/status` at `http://127.0.0.1:8765`, then exercise
   the unchanged guarded mini model and DeepSeek routes with explicit model selection.
   Confirm aliases/settings/registries and prior service states are unchanged before
   reopening clients and closing the window. Failed restoration is an incident, not
   a successful experiment; preserve evidence and keep the operator engaged.
