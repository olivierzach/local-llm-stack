# Emergency recovery and offline setup handoff

Use this when the model, controller, Internet connection or normal SSH path is
unavailable. It must be usable without an LLM. Print/save this page and the
[connection playbook](SPARK_CONNECTIONS_RUNBOOK.md) on an independent device before
maintenance. A bookmark to a service on the failed machine is not an offline copy.

Start with [connections](SPARK_CONNECTIONS_RUNBOOK.md), then choose the
[single-Spark](SPARK_SINGLE_NODE_RUNBOOK.md) or
[multi-Spark](SPARK_MULTI_NODE_RUNBOOK.md) setup procedure. This page covers
preservation, emergency decisions, exact restoration and the handoff between
recovery-owned and manually operated workers.

## 1. Stop conditions and scope

- A broken model does not mean the operating system is down. Check an independent
  SSH path before rebooting, reinstalling, deleting containers or changing cables.
- If SSH is unavailable, use the Spark's local console/keyboard/display and the
  connection playbook. Neither the controller nor these commands power on a Spark,
  plug in a cable, supply an unknown Wi-Fi password or authorize administrator work.
- Do not change addresses, hostnames, usernames, cache locations or SSH host keys
  merely to make an old plan validate. Reprovisioning a host is a reviewed identity
  migration, not ordinary model restoration.
- Do not run `docker system prune`, broad Compose teardown, cache deletion, Git
  clean/reset, or removal of GPU reservations/recovery fences as troubleshooting.
  Another workload may own those resources. Preserve ambiguous state for reconciliation.
- A saved plan, Git bundle and model cache are different artifacts. Git does not
  contain private credentials, ignored runtime state, checkpoint weights or Docker
  images. An offline rebuild needs those artifacts or a restored Internet path.
- Mark each command's host. The examples below run on the **Mac/controller** unless
  explicitly labeled **Spark**. Administrator prompts are expected where needed;
  do not use privileged Docker containers to bypass missing sudo authorization.

## 2. Prepare a recovery packet before disruption

Keep a private packet on the controller and an independently accessible encrypted
copy. Keep a short, non-secret printed ledger with host names, physical port labels,
management addresses, public host-key fingerprints, artifact locations and the
operator's contact information. Do not put credentials or private keys in Git.

### What must be retained

| Layer | Retain | Restore constraint |
| --- | --- | --- |
| Physical/network | Cable/port map; device names and MAC identification; management, serving and collective address/prefix roles; Internet/default gateway and DNS; network profile UUIDs and intended persistent settings | Confirm the actual device/profile before changing it; no default gateway on the dedicated collective links. |
| SSH | Independently verified public host keys, reviewed aliases, usernames and public-key authorization restrictions | Keep each machine's private keys local. If a key is lost, generate a new local key and explicitly re-enroll its public key from a trusted console; never disable host-key checking. |
| Source/runtime | Full source revision, verified Git bundle, trusted installer, locked dependency files, platform-specific wheelhouse, image digests and optional offline image archives | A wheelhouse is specific to OS/architecture/Python compatibility. An uncommitted bind-mounted runtime edit is not contained in a Git bundle. |
| Model | Repository and exact commit, upstream verification/download manifest, peer-copy lock, tokenizer/config files, every checkpoint shard and symlink target, source overlays/draft model/NCCL artifacts required by the recipe | Structural cache checks alone are not checkpoint SHA-256 verification. Retain the verified cache or plan for a pinned re-download. |
| Deployment | Exact saved `plan.json`, canonical full-plan SHA-256, per-node Compose files, endpoint and qualification receipts, current owner/digest/container identities | Restore from the saved plan, not from a recipe that may have changed since the original launch. |
| Authority/ingress | Private service config, policy and referenced plans/receipts, complete journal/history/commands, route state and route highwater, gateway registry/key, service ownership/transaction records | Quiesce the writer before backing up. A backup is not permission to roll back epochs/generations or activate a second authority. |
| Existing applications | Approved backups of the actual bind-mounted configuration/source, `.env`, UI/router persistent data and independently recorded gateway/firewall changes | Treat these as secret-bearing. Preserve unrelated user changes and applications; this controller does not reconstruct them from Git. |

Record dynamic worker/container IDs immediately before a test. They legitimately
change after recreation; the saved owner, deployment digest and complete runtime
contract are the restoration targets. Record which route/firewall rules and services
existed before the window so rollback removes only the window's owned additions.

### Collect non-secret connection facts

**Spark, inspection only:**

```bash
hostnamectl --static
ip -br link
ip -br address
ip route show
ip -6 route show
ip route get 1.1.1.1
resolvectl status
nmcli -f NAME,UUID,TYPE,DEVICE connection show
ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

Use `nmcli` only if NetworkManager is the existing configuration owner; otherwise
record the existing renderer/configuration identified by the connection playbook.
Do not request `--show-secrets`. Root-owned Wi-Fi profile backups belong in the
operator's approved encrypted storage, not this repository or a terminal transcript.

**Mac, inspection only:**

```bash
networksetup -listallhardwareports
networksetup -listnetworkserviceorder
scutil --nwi
scutil --dns
route -n get 10.10.10.2
route -n get 10.10.20.2
ssh -G spark-66f1-wired
ssh -G spark-e8f1-wired
```

`ssh -G` shows the effective configuration, not a successful connection. Record the
specific service's additional routes with `networksetup -getadditionalroutes` using
the service name found above; do not guess that a device named `en0` is always the
same physical adapter. Keep any detailed host inventories private.

### Create a new private packet and verify the exact plan

**Controller, local files only.** Supply paths from the startup ledger, not a newly
rendered recipe. The concrete current installation is listed in section 8.

```bash
export REVISION=65b05d5503e6ffefd2f07d07ebe86333917d5086
export PREFIX="$HOME/projects/local-llm-stack-cluster"
export RELEASE="$PREFIX/releases/$REVISION"
export PY="$RELEASE/.venv/bin/python"
umask 077
mkdir -p "$HOME/spark-recovery-kits"
export KIT="$(mktemp -d "$HOME/spark-recovery-kits/packet-XXXXXXXX")"
read -r -p 'Absolute saved-plan path from the startup ledger: ' PLAN
read -r -p 'Trusted canonical full-plan SHA-256 from that ledger: ' PLAN_SHA256
read -r -p 'Absolute private inventory path: ' INVENTORY
export PLAN PLAN_SHA256 INVENTORY
PYTHONPATH="$RELEASE/tools" "$PY" - <<'PY'
import json, os
from pathlib import Path
from spark_cluster import config
plan = Path(os.environ['PLAN'])
inventory = Path(os.environ['INVENTORY'])
if not plan.is_absolute() or not inventory.is_absolute():
    raise SystemExit('Absolute input paths required')
p = config.read(plan)
config.validate_saved_plan(p, os.environ['PLAN_SHA256'])
config.validate_inventory(config.read(inventory))
kit = Path(os.environ['KIT'])
for name, source in [('plan.json', plan), ('inventory.json', inventory)]:
    with (kit / name).open('xb') as target:
        target.write(source.read_bytes())
with (kit / 'plan-admission.json').open('x') as target:
    json.dump({'revision': os.environ['REVISION'], 'plan_sha256': config.plan_sha256(p),
               'deployment_digest': p['digest'], 'owner': p['owner']}, target, indent=2)
    target.write('\n')
print('Validated recovery inputs copied; no remote operation performed')
PY
"$PY" "$RELEASE/scripts/sparkctl" observe --inventory "$INVENTORY" \
  --timeout 20 --output "$KIT/before-observation.json"
```

Read the observation's `complete`, errors, reservations, GPU processes/containers
and recovery fences. A partial observation is evidence of uncertainty, not an idle
machine. Keep private packet files mode 0600 and directories 0700.

If automatic recovery is already operating, its consistent backup is a separate
maintenance operation: close/drain admission, stop the supervised writer, verify
all roles stopped, then retain the **whole** private service directory and all
referenced artifacts. Include the ingress key and highwater files in encrypted
storage. Do not copy a live journal and call it a consistent recovery checkpoint.
Preserve node-side reservation/fence files for evidence; never replay old container
IDs or overwrite newer node fences from a backup.

### Source, dependencies and genuinely offline recovery

From the trusted immutable release, make a new local bundle and copy its installer:

```bash
git -C "$RELEASE" bundle create "$KIT/controller.bundle" HEAD
git -C "$RELEASE" bundle verify "$KIT/controller.bundle"
cp "$RELEASE/scripts/install-spark-controller.py" "$KIT/install-spark-controller.py"
"$PY" -m pip download --require-hashes --only-binary=:all: \
  -r "$RELEASE/tools/controller-requirements.lock" --dest "$KIT/wheels"
```

The download requires Internet and disk space during preparation; do it before a
fault. Produce a separate compatible wheelhouse for each target platform/Python.
Retain hashes of the bundle, installer and archives in the encrypted packet, with
the trusted revision/plan hashes independently recorded in the operator ledger.

**Cold-install limitation:** this pinned installer deliberately invokes pip with
`--isolated` and an explicit PyPI index. `PIP_NO_INDEX`/`PIP_FIND_LINKS` environment
variables do **not** turn a new installation into an offline one. A wheelhouse is
useful retained evidence, but this installer has no wheelhouse input flag. Restore
Internet before a cold installation; do not silently bypass the hash-locked installer.

For offline reuse/restoration, retain the **complete already-installed prefix**,
including its immutable release, `.git`, `.venv`, release receipt, wrappers, shared
state and symlinks, plus the separately located private service directory and all
referenced artifacts. Restore it only to the same approved absolute paths on a
compatible OS/architecture with the required system Python still installed.
Do not overwrite an unknown existing prefix or assume a venv is portable to another
machine/Python layout. Once that verified installed release is present:

```bash
if test -f "$RELEASE/.controller-release.json" && test -x "$PY"; then
  python3 "$KIT/install-spark-controller.py" \
    --bundle "$KIT/controller.bundle" --revision "$REVISION" --prefix "$PREFIX"
else
  printf '%s\n' 'STOP: no complete installed release; restore the approved backup or Internet first.'
fi
```

The existing-release path checks source/dependency receipts and local command health;
it does not reinstall dependencies or start services. Prove this from a disposable
same-path backup restore with network denied before relying on it. OS prerequisites,
model weights, connectivity, source authenticity and compatible private state remain
separate requirements. Preserve an independently accessible trusted installer and
its hash; fetching the current default branch during an outage is not equivalent.

For offline images, **on the Spark**, save each recipe's exact image digest to a
new private output directory with `docker image save --output ... IMAGE@sha256:...`.
The literal image value comes from the verified plan, not a tag or this example.
Check free disk space first. On restoration, `docker image load --input ARCHIVE`
must be followed by `docker image inspect` and comparison of the exact digest and
architecture; never substitute a different tag. Docker save/load can lose the
repository-digest association: if `docker image inspect IMAGE@sha256:...` fails after
load, that archive alone is insufficient for this pinned recipe. Restore Internet
and pull the exact digest, or use a separately verified digest-preserving registry
backup; a new tag is not a fix. Model-cache archives must include symlink targets
as well as snapshot directories. Retain hashes and verify a restored copy. Large
image/model copies need an explicit disk/I/O budget; they are not a status command.

Use the operator's approved encrypted backup tool for secret-bearing configuration
and keep its recovery credential independently available. Verify decryption to a
**new scratch directory** and compare hashes before relying on that copy. Do not
put unencrypted packets in Git, upload them as PR artifacts, or make an outage the
first time someone tries the backup password.

## 3. Decide what actually failed

| Observation | First action | Do not do |
| --- | --- | --- |
| Model fails but independent SSH works | Inspect exact deployment status, container health, journal and gateway admission; preserve logs. | Reboot both Sparks or delete reservations. |
| One SSH alias fails | Try another independently verified management path; inspect addresses/default route locally if needed. | Treat the collective cable as the only management path or accept a changed host key blindly. |
| No Internet, but local SSH works | Check Wi-Fi/carrier, default route, DNS and clock separately. Cached inference may still be viable. | Put an Internet gateway on a collective rail or upgrade drivers to fix DNS. |
| Mac/gateway unavailable | Restore Mac wake, user login and connectivity. Expiring admission leases must not free GPU ownership. | Start another authority from a stale copied journal. |
| Reservation/fence/worker identity is unknown | Stop mutations; regain observations and reconcile the exact owner. | Interpret a timeout as successful cleanup. |
| Saved source/plan/model artifacts missing | Retrieve the independent verified packet or restore Internet for the pinned preparation workflow. | Re-render a newer recipe and label it the original deployment. |

## 4. Restore ordinary manual single-node or distributed serving

This section applies only when **no active recovery fence owns the nodes**. Both
manual single-node and manual multi-node deployment use the same saved-plan path.
If recovery owns them, complete section 5 first. If the saved deployment is already
healthy and matches the recorded contract, leave it running.

**Controller, inspection:**

```bash
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256"
```

Inspect the JSON `healthy` value and worker identities; exit status alone is not
an inference acceptance result. If cleanup is necessary, it is **GPU-disruptive**
and needs the recorded owner, working management paths and maintenance approval:

```bash
"$PY" "$RELEASE/scripts/sparkctl" down --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256"
```

`down` removes exact owned containers and discovery endpoint files, not checkpoint
or compiled caches. Use a working copy of the plan, not an immutable backup packet
with endpoint artifacts beside it. Any ownership/fence error blocks the next step.
Observe again and require the intended nodes free of reservations, GPU containers
and GPU processes before a new start. Do not tear down unrelated serving/UI services.

For restoration, copy the admitted saved plan into a fresh working directory and
revalidate its canonical hash. Choose another fresh directory for startup evidence:

```bash
mkdir -p "$HOME/spark-operations"
export WORK="$(mktemp -d "$HOME/spark-operations/restore-XXXXXXXX")"
cp "$KIT/plan.json" "$WORK/admitted-plan.json"
export PLAN="$WORK/admitted-plan.json"
export RESTORE_OUTPUT="$WORK/start"
```

Start under an independent supervisor, not a chat tool whose process may disappear.
On the **logged-in Mac**, this one-shot launchd job uses exact argv and private logs:

```bash
export RESTORE_LABEL="org.spark-recovery.restore.$(date -u +%Y%m%dT%H%M%SZ)"
export RESTORE_JOB="$WORK/restore.plist"
"$PY" - <<'PY'
import os, plistlib
from pathlib import Path
keys = ['PY', 'RELEASE', 'PLAN', 'RESTORE_OUTPUT', 'WORK', 'RESTORE_JOB']
if any(not Path(os.environ[k]).is_absolute() for k in keys):
    raise SystemExit('Absolute paths required')
job = {'Label': os.environ['RESTORE_LABEL'], 'RunAtLoad': True, 'KeepAlive': False,
       'ProgramArguments': [os.environ['PY'], '-u',
           str(Path(os.environ['RELEASE']) / 'scripts/sparkctl'), 'up',
           '--saved-plan', os.environ['PLAN'], '--plan-sha256', os.environ['PLAN_SHA256'],
           '--output', os.environ['RESTORE_OUTPUT'], '--timeout', '7200'],
       'WorkingDirectory': os.environ['WORK'],
       'StandardOutPath': str(Path(os.environ['WORK']) / 'restore.stdout.log'),
       'StandardErrorPath': str(Path(os.environ['WORK']) / 'restore.stderr.log')}
with Path(os.environ['RESTORE_JOB']).open('xb') as handle:
    os.chmod(os.environ['RESTORE_JOB'], 0o600)
    plistlib.dump(job, handle)
PY
# GPU-disruptive startup; only after all admission/restore gates above:
launchctl bootstrap "gui/$(id -u)" "$RESTORE_JOB"
launchctl print "gui/$(id -u)/$RESTORE_LABEL"
```

Submission is not success. Inspect the logs and fresh startup evidence; require
successful process exit, matching ownership/health and the bounded authenticated
acceptance checks in the serving playbook. A terminal closing does not terminate
this launchd job, but Mac logout/power loss remains a limitation. After the job has
finished and evidence is retained, remove **only this exact one-shot job**:

```bash
launchctl bootout "gui/$(id -u)/$RESTORE_LABEL"
```

If startup failed, retain its plan and error/cleanup receipts. Reconcile any partial
owned deployment before using a new attempt directory. Do not overwrite a prior
attempt, loop retries around ambiguous mutations, or report readiness from an old
`endpoint.json`. Restore only the previously recorded client/gateway routes after
actual model readiness; a wrong model behind a healthy port is not restoration.

## 5. Hand recovery-owned workers back to manual operation

Use the retained private service config/policy paths from the ledger. **This closes
admission and stops owned GPU workers.** It is not a metadata-only reset.

```bash
read -r -p 'Absolute service config path from the ledger: ' SERVICE_CONFIG
read -r -p 'Absolute recovery policy path from the ledger: ' POLICY
read -r -p 'Absolute authority state directory from the ledger: ' STATE
export SERVICE_CONFIG POLICY STATE
"$PY" "$RELEASE/scripts/spark-recover" disable --policy "$POLICY" --state-dir "$STATE" \
  --apply --approve close-ingress
"$PY" "$RELEASE/scripts/spark-recover" reset --policy "$POLICY" --state-dir "$STATE" \
  --apply --approve stop-exact-owned-workers
"$PY" "$RELEASE/scripts/spark-recover" status --policy "$POLICY" --state-dir "$STATE"
```

The authority and gateway must be running to consume/drain these commands. Require
both returned command IDs to have `state: applied`, a disabled reconciled journal,
no pending transition and both nodes reachable/idle. A queued or refused command
is not cleanup. If the authority is unavailable, restore its exact config/source
and reconcile before requesting cleanup; do not downgrade or delete its journal.

Then stop/uninstall only its owned jobs; this preserves configs, credentials,
journal and history and prevents the test jobs reloading on the next login:

```bash
"$PY" "$RELEASE/scripts/spark-services" stop --config "$SERVICE_CONFIG" --apply
"$PY" "$RELEASE/scripts/spark-services" uninstall --config "$SERVICE_CONFIG" --apply
"$PY" "$RELEASE/scripts/spark-services" status --config "$SERVICE_CONFIG"
```

Reset deliberately retains node fencing highwaters. Before manual `sparkctl up`,
release still-active fences using the pinned primitive below. It defaults to
**inspection only**; do not change the approval value until its proposed identities
have been reviewed. It refuses a live authority, queued command, non-idle node,
foreign authority or unknown digest. A stopped gateway alone is insufficient.

```bash
export APPLY_IDLE_FENCE_RELEASE=no
PYTHONPATH="$RELEASE/tools" "$PY" - <<'PY'
import json, os, uuid
from pathlib import Path
from spark_cluster import cli, recovery, services
require = recovery.require
cfg = services.load_config(Path(os.environ['SERVICE_CONFIG']))
policy = recovery.load_policy(Path(os.environ['POLICY']))
directory = Path(os.environ['STATE'])
require(cfg['state_dir'] == directory.resolve() and cfg['policy_data'] == policy,
        'Service config, policy and authority directory must match')
status = services.manage(cfg, 'status')
require(status.get('installed') is False and not status.get('transaction_pending') and
        not any(r.get('loaded') for r in status.get('roles', {}).values()),
        'Stop and uninstall the owned authority jobs first')
require((directory / 'journal.json').is_file(), 'Existing authority journal required')
with recovery.lock(directory / 'authority.lock'), recovery.lock(directory / 'commands.lock'):
    journal = recovery.Journal(directory, policy)
    state = journal.value
    queue = directory / 'commands.json'
    require(not queue.exists() or recovery.recovery_routes.private_json(queue) == [],
            'Pending control commands block handoff')
    require(state['enabled'] is False and state['intent'] is None and state['current'] is None,
            'Applied reset and disabled reconciled authority required')
    require(state['route'] is None or state['route']['accepting'] is False,
            'Ingress must be closed')
    refs = [state['preferred'], policy['preferred'], *policy['fallbacks']]
    plans = [recovery.load_plan(ref) for ref in refs]
    primary = recovery.load_plan(policy['preferred'])
    observed = cli.observe(primary['nodes'], timeout=20, transports=policy['management'])
    require(observed['complete'], 'Both nodes must be reachable')
    actions = []
    for node, expected in primary['nodes'].items():
        value = observed['nodes'][node]
        require(recovery.reached(value, expected) and not value.get('errors') and
                value['reservation'] is None and not value['gpu_containers'] and
                not value['gpu_processes'], 'Every node must be fully observed and idle: ' + node)
        gate = value['recovery_fence']
        if gate is None:
            continue
        require(gate['policy'] == policy['id'] and gate['authority'] == state['authority'] and
                0 < gate['generation'] <= state['epoch'], 'Unknown/newer authority fence: ' + node)
        require(set(gate['allowed_digests']) <= {p['digest'] for p in plans if node in p['nodes']},
                'Fence admits an unreviewed deployment: ' + node)
        if not gate['active']:
            continue
        plan = next((p for p in plans if node in p['nodes'] and p['digest'] in gate['allowed_digests']), None)
        require(plan is not None, 'No trusted plan admitted by fence: ' + node)
        context = {k: gate[k] for k in ('policy', 'authority', 'generation')}
        context['operation_id'] = str(uuid.uuid4())
        actions.append((node, plan, context))
    print(json.dumps({'planned': [{'node': n, 'digest': p['digest'], 'context': c}
                                  for n, p, c in actions]}, indent=2), flush=True)
    if os.environ.get('APPLY_IDLE_FENCE_RELEASE') == 'release-reviewed-idle-fences':
        for node, plan, context in actions:
            result = cli.call(plan, node, 'release-fence', recovery=context,
                              transport=policy['management'][node])
            require(result.get('active') is False, 'Fence release was not acknowledged')
            print(json.dumps({'node': node, 'released': result}), flush=True)
    else:
        print('Inspection only; no fence mutation performed')
PY
```

For the separately approved application, change the environment value to
`release-reviewed-idle-fences` and rerun the same block. Retain its stdout privately.
The node atomically rechecks the exact fence and empty GPU/reservation state. If a
reply is lost or only one node completes, stop and observe again: an inactive matching
fence is a completed release; an unknown or still-active fence is not permission to
start a new authority. Never erase tombstones. Reobserve both nodes after release,
then use section 4 with the original admitted plan and fresh startup evidence.

This is a handoff to manual operation, not the normal automatic failback path. During
normal recovery, the controller retains authority and starts the preferred saved
plan itself after the stable-return/dwell, drain and ownership gates.

## 6. Restore settings, not just a listening port

Before declaring success, compare the recorded image/model/recipe contract, worker
command and configured environment, mounts, network mode, owner and full deployment
digest. Confirm both ranks for a distributed plan. Run the serving playbook's bounded
text/tool/stream acceptance through the intended authenticated client path. Keep
strict model aliases strict; fallback must not masquerade as the requested full model.

Restore gateway/ACL/network changes only from the approved before-snapshots and only
if their current identity still matches this window's owned change. Never restore an
entire old firewall table over unrelated concurrent edits. A route that predated the
window must not be deleted as cleanup. Credentials and user application settings
that were never changed should remain untouched.

A controller release rollback does not make an older controller compatible with a
newer journal. Preserve highwaters and use a fenced, reviewed authority handoff for
journal/schema recovery. Network and identity restoration precede service activation.

## 7. Safer simulated failures

For the first live campaign, keep both operating systems and normal SSH sessions up.
Inject a node-unreachable result only into the test controller's transport boundary;
block **all** of that controller's mutation paths to the simulated-lost node, not just
one observation. Keep normal operator SSH independent, one authority lock, bounded
fault duration, fresh observations on return and an independently supervised restore.

The real model can still be interrupted: surviving rank cleanup, qualified single-node
startup, fallback drain and full distributed restart are real GPU operations. Isolation
and per-plan qualification remain mandatory. A simulated observation is not evidence
that a physical network/power failure was tested. The fault-injection harness must be
reviewed and exercised before this live campaign; no existing generic CLI flag is
claimed to implement it. The repository's deterministic CPU regressions are useful
proof of logic, not substitutes for real model lifecycle acceptance.

## 8. Recorded installation and current hold point

This is a **site-specific record**, not portable configuration for a new machine:

- Mac release: `~/projects/local-llm-stack-cluster/releases/65b05d5503e6ffefd2f07d07ebe86333917d5086`.
- Private operation root: `~/projects/local-llm-stack-recovery/data/cluster/operations/pr2-20261002T055115Z/`.
- Service config/policy/inventory: `service/services.json`, `service/policy.json`,
  `service/inventory.json` below that root; authority state is `service/authority/`.
- **Historical, now stale** admitted plan: `service/plans/preferred.json`; full-plan SHA-256
  `69aa5dc93ea3b1fafe73cf2dfaaed43973dabcca9d01f3c10ff19d5eb4a3cf4e`.
- Original GLM deployment digest:
  `b1cf7901f81c861758f5f7c14ba7cea5605519713c4fc4816f1e06af41eb3bb2`.
  Preserve its one-image settings as history; do not regenerate it from the newer
  four-image recipe or assume it is still the live configuration.
- Rollout evidence: `rollout-20261003T044051Z/` below the private operation root
  (UTC timestamp). Actual worker settings were saved privately and the image, command,
  configured environment and network mode matched the original saved plan at that
  initial observation—not the subsequently replaced deployment.
- Actual Mac install/start/stop was exercised: four launchd roles had parent PID 1;
  authenticated gateway admission was closed with zero active requests. Recovery
  stayed disabled at epoch 0. All four roles were subsequently confirmed stopped
  and their test plists uninstalled, preventing login-time loading during this hold.
  The installed release, private configuration and authority state remain preserved.
- A separate one-shot launchd observer completed with exit 0 and was removed.
  It observed healthy replacement workers started around 04:54 UTC, under owner
  `glm53-tp2-256k-dflash2-e8f1-cuda-log-c8964024b73e`, with deployment digest
  `c8964024b73e24f031d08ee4e17d0992e3c94a089606b97cca0836345c9295ba`.
  **The older prepared policy/plan is not the current restoration baseline.**
  Reconcile the new owner's exact saved plan and settings with its operator before
  any disruptive action; do not stop or overwrite it using the older packet.
  The source of the replacement was not established. Both observations and an
  explicit `BLOCKED_STALE_BASELINE.json` record are retained privately.
  Complete replacement-worker Docker settings were also saved separately; those
  inspections do not substitute for the new owner's canonical saved plan.
- This rollout issued no GPU start/stop commands, ran no GPU inference qualification
  and injected no fault. Healthy workers do not prove their settings stayed unchanged.
- Unattended administrator access was unavailable on all three machines. Independent
  routing, full ingress isolation/reversal and a proven supervised restoration path
  remain prerequisites before any GPU-disruptive test. Operator admin provisioning
  is a one-time prerequisite; ordinary automatic model recovery should not require
  repeated manual SSH commands.

The private operation directory is **not in Git**. Copy/verify the recovery packet
and make this handoff accessible before relying on it. A clone of these playbooks
alone does not restore the site's secrets, private authority state or model caches.
