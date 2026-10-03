# Standalone Spark serving runbook

This is a **future operator procedure, not permission to execute now**. The live
rollout is paused before GPU mutation or inference. No receipt in this document
claims that a new placement has passed. Start with
[connections, cabling, Internet, SSH and persistence](SPARK_CONNECTIONS_RUNBOOK.md).
For a distributed deployment use [the multi-node runbook](SPARK_MULTI_NODE_RUNBOOK.md);
for interruption, lost ownership, fences or restoration use
[the recovery runbook](SPARK_RECOVERY_RUNBOOK.md).

## 1. Scope and admission gates

The concrete service here is `coder-e8f1.json`: one complete
`Qwen/Qwen3-4B-Instruct-2507` model on **one** Spark, exposed as `local-coder`.
The recipe `qwen3-4b-tools.json` pins:

- Model commit `cdbee75f17c01a7cc42f958dc650907174af0554`.
- Runtime `vllm/vllm-openai@sha256:e4f88a835143cd22aee2397a26ec6bb80b3a4a6fe0c882bcbc63822904766089`.
- BF16, eager execution, Hermes tools, 32,768 context tokens, 4,096 maximum
  output tokens, four sequences and 30% GPU memory utilization.
- Single-node mode only, TP=1 and PP=1; at least 24,576 MiB available shared memory.
- Text, tools and SSE, **not vision**. Historic e8f1 protocol acceptance does not
  qualify a fresh host, changed serving path or this newly rendered plan.

A single member of a TP2 deployment is not a standalone server or a replica.
This procedure does not fit or restore the current full GLM model on one Spark.
Never substitute this recipe under an existing distributed alias as a silent
fallback. It is a separately identified, newly approved service.

| Phase | Impact and required approval | Stop condition |
| --- | --- | --- |
| Foundation/inspection | Inspect identities, routes, caches and ownership; no inference | Untrusted host, incorrect address/interface, inaccessible owner records |
| OS prerequisites | Administrator provisioning; potentially network/service disruptive | No approved sudo session, incompatible driver/runtime, package failure |
| Source/controller/artifacts | Local files, Internet downloads, CPU/disk work, Docker image/cache writes | Wrong pins, missing disk, checksum failure, ambiguous transfer |
| Plan admission | Local immutable evidence, explicit review of full plan and GPU window | Foreign GPU work, active recovery fence, unapproved plan hash |
| `up` | GPU-disruptive launch **and a real bounded text request** | Any admission/startup/ownership/readiness failure |
| Gateway/client | CPU container and route/credential writes; routing may affect clients | Existing conflicting gateway/key/route or wrong backend identity |
| Protocol acceptance | Five bounded real text/tools/SSE requests | Missing check, mismatched deployment, malformed/incomplete stream |
| Stop/restart | Owned worker interruption; restart repeats inference | Other owner/fence, failed cleanup or ambiguous mutation |

Do not interpret a successful `doctor`, image pull, model download or
`prepared: true` as GPU qualification or automatic-recovery enrollment.
Do not run broad `make up`, `make down`, Compose switching targets, `docker prune`,
`docker system prune`, volume removal, reservation-file deletion or force-removal
of arbitrary containers. Legacy targets may stop another engine; the controller
protects owned workers only when its own ownership-aware commands are used.

### Host contexts and prerequisites

Unless a section says **Mac**, execute it in the non-root Linux account on the
**selected Spark**. This keeps cold-cache work and all local paths on that Spark.
The controller detects its inventory hostname and executes locally; another
controller uses the inventoried SSH alias. The example is the recorded e8f1
identity/user, not a discovery mechanism or a universal factory configuration.

Require the foundation's verified host identity/key, administrator-approved
stable `10.10.10.3/24` serving address on `enP7s7`, and working Internet through
Wi-Fi/LAN, not a default route through the compute fabric. The Mac must reach
management `10.10.10.3` independently of the two fabric rails. Do not assume a
`.local` hostname or last DHCP address is stable. No second Spark or connected
collective cable is required for this recipe with explicit `serving` configured.
The inventory schema still requires fabric descriptors; they describe the
recorded hardware but single-mode admission does not require their carrier.
The recorded fabric addresses use `/30` masks; the JSON carries addresses only,
not network configuration. Inspect the actual masks in the foundation before any
network change; this runbook does not reconfigure either fabric interface.

Require Python 3.10+ with venv, Git/OpenSSH, Docker Engine/Compose and an NVIDIA
container runtime compatible with the Spark's driver. Provision these through
the approved vendor/admin process in the foundation. The package bootstrap is
not a Docker/driver installer. Docker access is a privileged capability, not a
substitute for permission to perform administrative host changes. There is no
unattended sudo on the recorded hosts.

## 2. Fresh pinned source and versioned controller

**Spark — local file/dependency mutation, after approval.** Never reset or
checkout a different revision in a live production tree. The SHA below is the
published controller code pin used for the recorded rollout, not a claim that
this service is already running. Retain the bundle and its installer offline.
If using a different approved full SHA, review its parsers and recipes before
following these commands.

```bash
set -euo pipefail
umask 077
REVISION=65b05d5503e6ffefd2f07d07ebe86333917d5086
PREFIX="$HOME/projects/local-llm-stack-cluster"
mkdir -p "$HOME/projects" "$HOME/spark-evidence"
SOURCE=$(mktemp -d "$HOME/projects/spark-source.XXXXXX")
EVIDENCE=$(mktemp -d "$HOME/spark-evidence/single-coder.XXXXXX")
git clone --no-checkout https://github.com/olivierzach/local-llm-stack.git "$SOURCE"
git -C "$SOURCE" checkout --detach "$REVISION"
test "$(git -C "$SOURCE" rev-parse HEAD)" = "$REVISION"
git -C "$SOURCE" bundle create "$EVIDENCE/controller.bundle" HEAD
cp "$SOURCE/scripts/install-spark-controller.py" "$EVIDENCE/install-spark-controller.py"
python3 "$EVIDENCE/install-spark-controller.py" \
  --bundle "$EVIDENCE/controller.bundle" --revision "$REVISION" --prefix "$PREFIX"
RELEASE="$PREFIX/releases/$REVISION"
PY="$RELEASE/.venv/bin/python"
printf 'SOURCE=%s\nEVIDENCE=%s\nRELEASE=%s\n' "$SOURCE" "$EVIDENCE" "$RELEASE"
```

The installer uses hashed dependency wheels, a detached versioned release and
shared `state/`; it neither starts a model nor copies credentials. It needs PyPI
on a first install. Existing controller activation changes stable `bin/` wrappers
but does not restart services; schedule that change if another operator uses
those wrappers. Commands below explicitly address `RELEASE` rather than `current`.
A full SHA and bundle checksum establish reproducibility, not publisher trust;
verify the source origin independently. Offline artifact retention and cold-install
limits are covered by the recovery runbook; the Git bundle contains neither model
weights nor a portable, already-installed environment.

**Spark — inspection, then separately approved admin installation if necessary:**

```bash
bash "$RELEASE/scripts/bootstrap-spark-packages.sh" --check
```

A missing baseline is an expected stop on a fresh host. Review
`$RELEASE/cluster/system-packages.txt`, obtain an interactive administrator
session, and only then run:

```bash
sudo bash "$RELEASE/scripts/bootstrap-spark-packages.sh" --install
bash "$RELEASE/scripts/bootstrap-spark-packages.sh" --check
docker version
docker compose version
nvidia-smi
```

Success is a complete baseline plus functioning user Docker/Compose and driver
inspection. A package install is not proof that GPU containers work; that is
established during the approved launch. Do not downgrade kernels/drivers to
match another machine's package list. Optional OMP on this Spark is installed
with `bash "$RELEASE/scripts/install-omp-spark.sh"` after separate client-install
approval; the standalone server itself does not need a coding agent or Node.js.

## 3. Explicit one-node inventory and initial inspection

**Spark — local configuration mutation.** Confirm this example exactly matches
the admitted host before writing it. For 66f1 or another Spark, create a reviewed
one-node inventory with its real hostname, user paths, SSH alias and stable
serving address, and choose a deployment whose `nodes` and `coordinator` match.
Do not put both nodes in this inventory merely because two machines exist.

```bash
INVENTORY="$EVIDENCE/inventory.json"
DEPLOYMENT="$RELEASE/cluster/deployments/coder-e8f1.json"
CACHE="$HOME/.cache/local-llm-stack/huggingface"
test "$(hostname)" = spark-e8f1
test "$HOME" = /home/statsparrot
mkdir -p "$CACHE"
cat > "$INVENTORY" <<'JSON'
{
  "version": 1,
  "nodes": {
    "e8f1": {
      "hostname": "spark-e8f1",
      "ssh": "spark-e8f1-wired",
      "management": {"ssh_targets": ["spark-e8f1-wired"]},
      "serving": {"address": "10.10.10.3", "interface": "enP7s7"},
      "architecture": "aarch64",
      "gpus": 1,
      "cache": "/home/statsparrot/.cache/local-llm-stack/huggingface",
      "projects": "/home/statsparrot/projects",
      "fabric": [
        {"interface": "enp1s0f0np0", "ip": "10.10.20.2", "rdma": "rocep1s0f0"},
        {"interface": "enP2p1s0f0np0", "ip": "10.10.21.2", "rdma": "roceP2p1s0f0"}
      ]
    }
  }
}
JSON
cp "$DEPLOYMENT" "$EVIDENCE/deployment.json"
cp "$RELEASE/cluster/recipes/qwen3-4b-tools.json" "$EVIDENCE/recipe.json"
"$PY" "$RELEASE/scripts/sparkctl" validate --inventory "$INVENTORY" --deployment "$DEPLOYMENT"
"$PY" "$RELEASE/scripts/sparkctl" doctor --inventory "$INVENTORY" --node e8f1
"$PY" "$RELEASE/scripts/sparkctl" observe --inventory "$INVENTORY" \
  --output "$EVIDENCE/initial-observation.json"
if ! "$PY" "$RELEASE/scripts/spark-node" check --inventory "$INVENTORY" --deployment "$DEPLOYMENT"; then
  printf '%s\n' 'Not prepared: review every reported failure before proceeding to artifact staging.'
fi
```

The new cache directory is intentional; alternatively select an already trusted
cache in the inventory, without moving/deleting its data. A cold-cache `check`
will fail for absent images/weights. Inspect all other failures now. Doctor
prints facts and may exit successfully despite missing optional tools; review
its reported hostname, GPU processes/containers, memory, reservation and research
window. Do not stop research or another owner to obtain an idle node.
`spark-node discover/check` inspect candidates; `prepare` stages selected artifacts;
`apply` publishes an explicitly approved inventory snapshot. None enrolls recovery
or qualifies inference. For this standalone procedure the private one-node
inventory is the explicitly reviewed input; no existing shared inventory is edited.

## 4. Pinned image and true cold-cache preparation

**Spark — approved CPU/network/disk mutation, no GPU/inference.** Schedule I/O
around other users. Inspect free disk with `df -h "$CACHE"`; model download below
has a 16 GiB declared payload budget and a 10 GiB free-space reserve, in addition
to Docker image storage. This budget is a ceiling, not a measured model size.

```bash
if ! "$PY" "$RELEASE/scripts/spark-node" prepare --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT" --pull-image --apply --timeout 2400 \
  --evidence-output "$EVIDENCE/image-preparation.json"; then
  printf '%s\n' 'Preparation incomplete: inspect the receipt; only absent model artifacts are expected here.'
fi
```

On an empty cache this may return nonzero even after the image pull succeeds:
read the retained image staging receipt and missing-model checks. Do not blindly
repeat a mutation because its overall preparation status is false. Continue to
the model phase only if the exact image exists with the expected architecture:

```bash
IMAGE=$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["image"])' "$EVIDENCE/recipe.json")
docker image inspect "$IMAGE" --format '{{.Id}} {{.Architecture}} {{json .RepoDigests}}'
```

### Empty-cache Internet path (no second Spark required)

There is **no checked-in `fetch-pinned-spark-model` checksum manifest for this
4B recipe**. Do not use the unrelated 80B, GLM or DeepSeek manifests. The following
CPU-only pinned-image operation obtains metadata for the exact public commit,
checks its declared total against the byte budget, and constructs a real manifest
accepted by the existing SHA-verifying fetcher. Large LFS files use their upstream
SHA-256; small Git files are fetched and checked against their Git blob identity
before a SHA-256 is recorded. No weight is downloaded during this metadata step.
This is trust in the selected Hugging Face repository/HTTPS metadata, not an
independent publisher signature. Archive the generated manifest before fetching.

```bash
MANIFEST="$EVIDENCE/model-download.json"
MAX_DOWNLOAD_BYTES=17179869184
# Read-only inspection of the pin above is required before this cache-write step.
docker run --rm -i --pull never --user "$(id -u):$(id -g)" \
  --security-opt no-new-privileges --cap-drop ALL \
  --mount "type=bind,src=$CACHE,dst=/cache" \
  --mount "type=bind,src=$EVIDENCE,dst=/evidence" \
  --env HF_HOME=/cache --env HF_HUB_DISABLE_IMPLICIT_TOKEN=1 \
  --env NVIDIA_VISIBLE_DEVICES=void \
  --entrypoint python3 "$IMAGE" - "$MAX_DOWNLOAD_BYTES" <<'PYTHON'
import hashlib, json, pathlib, shutil, sys
import huggingface_hub
from huggingface_hub import HfApi, hf_hub_download
recipe = json.loads(pathlib.Path('/evidence/recipe.json').read_text())
repo, revision = recipe['model'], recipe['revision']
info = HfApi().model_info(repo, revision=revision, files_metadata=True, token=False)
if info.sha != revision:
    raise RuntimeError('repository returned a different commit')
if not info.siblings or any(type(f.size) is not int or f.size < 0 for f in info.siblings):
    raise RuntimeError('incomplete file-size metadata')
total = sum(f.size for f in info.siblings)
if total > int(sys.argv[1]) or shutil.disk_usage('/cache').free < total + 10 * 1024**3:
    raise RuntimeError('download budget or disk reserve exceeded')
files = {}
for entry in info.siblings:
    name = pathlib.PurePosixPath(entry.rfilename)
    if name.is_absolute() or '..' in name.parts:
        raise RuntimeError('unsafe repository filename')
    if entry.lfs:
        expected = entry.lfs.sha256
    else:
        if entry.size > 8 * 1024**2:
            raise RuntimeError('large non-LFS file requires separate review')
        path = pathlib.Path(hf_hub_download(repo, entry.rfilename, revision=revision,
                                           cache_dir='/cache/hub', token=False))
        data = path.read_bytes()
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        if len(data) != entry.size or actual != entry.blob_id:
            raise RuntimeError('Git blob verification failed: ' + entry.rfilename)
        expected = hashlib.sha256(data).hexdigest()
    files[entry.rfilename] = {'size': entry.size, 'sha256': expected}
manifest = {'version': 1, 'repo': repo, 'revision': revision,
            'image': recipe['image'], 'hub_version': huggingface_hub.__version__, 'files': files}
with open('/evidence/model-download.json', 'x') as stream:
    json.dump(manifest, stream, indent=2, sort_keys=True)
print(json.dumps({'repo': repo, 'revision': revision, 'declared_bytes': total,
                  'manifest': '/evidence/model-download.json'}))
PYTHON
"$PY" "$RELEASE/scripts/spark-node" prepare --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT" --model-manifest "$MANIFEST" \
  --max-download-bytes "$MAX_DOWNLOAD_BYTES" --timeout 10800 --apply \
  --evidence-output "$EVIDENCE/model-preparation.json"
"$PY" "$RELEASE/scripts/spark-node" check --inventory "$INVENTORY" --deployment "$DEPLOYMENT"
```

Success requires `prepared: true` and a staging receipt with the selected model,
revision, manifest hash and per-file lock. `fetch` checks every downloaded file's
size and SHA-256, the exact snapshot file inventory and safe cache symlinks.
Archive the receipt rather than merely recording that the directory exists.
A timeout or lost receipt is ambiguous: inspect CPU download containers/cache and
reconcile via the recovery procedure before retrying; never prune to make a retry
look clean. A checksum mismatch is a stop, not permission to trust the current
bytes or regenerate an expected checksum from them.

This path deliberately uses `token=False`; it neither reads nor forwards cached
HF credentials. A 401/403 or gated model requires account access/license approval,
not a guessed token, copied credential store or a bypass. This public 4B recipe
normally needs no login. A different gated model requires a separately reviewed
recipe and authenticated acquisition workflow, with a private mounted credential
file and approved egress; the existing `fetch-pinned-spark-model.py` does **not**
support authenticated downloads. Do not put tokens in arguments, inventory,
manifest, shell history or evidence. Never forward a token into the serving worker.

### Existing cache/offline alternative

If a trusted source already has the exact snapshot, use
`scripts/sync-spark-models.py lock --cache SOURCE_CACHE --model REPO@REVISION --lock FILE`
on that source, then its `sync --cache SOURCE_CACHE --lock FILE --peer SSH_ALIAS
--peer-cache DESTINATION_CACHE`. Bind those arguments to the approved absolute
paths and the same concrete model/commit above. The script hashes every regular
file at both ends, preserves snapshot symlinks and leaves unrelated cached models
and credentials alone; use the foundation's verified transport. A capture made
from unknown bytes proves later equality only, not trusted model provenance.
For a completely offline machine, restore the exact digest-addressable runtime
image and the verified cache from the recovery runbook before `check`; never pull
a mutable tag as a substitute. Docker image save/load can lose RepoDigest
association, so an image ID alone may not satisfy this recipe's exact digest lookup.

`doctor`, `check`, and `preflight` perform structural snapshot checks, **not full
weight hashing**. Retain the SHA-verifying staging/copy receipt separately.

## 5. Capture and approve the exact plan; then launch

**Spark — local plan/evidence writes and inspection only:**

```bash
"$PY" "$RELEASE/scripts/sparkctl" render --inventory "$INVENTORY" \
  --deployment "$DEPLOYMENT" --output "$EVIDENCE/rendered"
PLAN="$EVIDENCE/rendered/plan.json"
PLAN_SHA256=$(PYTHONPATH="$RELEASE/tools" "$PY" -c \
  'import sys; from spark_cluster.config import read,plan_sha256; print(plan_sha256(read(sys.argv[1])))' "$PLAN")
printf '%s\n' "$PLAN_SHA256" > "$EVIDENCE/plan.sha256"
"$PY" "$RELEASE/scripts/sparkctl" preflight --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --output "$EVIDENCE/preflight"
```

`PLAN_SHA256` is the canonical hash of **all** saved-plan fields, including Compose
and endpoint. It is not `sha256sum plan.json`, a recipe hash or the deployment
`digest`. Record the hash in the operator's independent approval record; computing
and immediately passing it is not, by itself, approval. Review the complete saved
plan: one node, expected cache/image/model, serving `10.10.10.3:8101`, read-only
model mount, correct limits, owner and digest. Plans contain configuration, not
private keys. The raw backend is not an authenticated gateway: keep its management
network private and do not expose port 8101 to untrusted clients or the Internet.

Require `launchable: true`, no foreign lease, no research window, no GPU processes
or GPU containers, enough shared memory, correct serving carrier/address and a
free backend port. Preflight briefly binds and closes ports; it does not reserve
a GPU. It can race with other work; `up` rechecks admission. Also inspect observation
for a recovery fence; preflight is not permission to bypass one. If the node is
part of an existing model, obtain that model owner's stop/recovery procedure first.

**Explicit GPU and inference approval:** record the full plan hash, selected host,
operator, maintenance window and startup timeout. Only then:

```bash
"$PY" "$RELEASE/scripts/sparkctl" up --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --timeout 600 --output "$EVIDENCE/start-01"
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256"
```

`up` reserves the GPU, starts the owned worker, waits for healthy Docker state,
checks the model alias and sends a 16-output-token text request. It is not a
readiness-only command. Success requires `healthy: true`, matching reservation,
container IDs/owner/digest/image, `start-01/endpoint.json` with `ready: true`, and
`start-01/acceptance.json` with real nonempty text. The endpoint should be
`http://10.10.10.3:8101/v1`, not the legacy fabric endpoint reached over a Mac
default route. A launch failure retains diagnostics and may attempt cleanup of
only its own newly acquired work. If cleanup is incomplete or its receipt is lost,
stop here and use the recovery runbook; do not issue an unfenced force/delete.
Do not route general traffic yet: direct text acceptance is not tool qualification.

## 6. Gateway, credentials and one bounded protocol qualification

**Spark — approved CPU image/container and route mutation.** Gateway placement is
independent of model placement; this example keeps it on the same standalone host.
The gateway uses an exact image ID, not the model image. For a truly fresh machine:

```bash
GATEWAY_IMAGE=ghcr.io/berriai/litellm@sha256:80ea654c506da9083503d00c1b323b2fecddf17af6add9bf01b2603938e17bd2
GATEWAY_ID=sha256:a7ec324e5bb322cfa5e120e3c80a1f425a3aee9295916c260a643580b2024137
docker pull "$GATEWAY_IMAGE"
test "$(docker image inspect "$GATEWAY_IMAGE" --format '{{.Id}}')" = "$GATEWAY_ID"
"$PY" "$RELEASE/scripts/spark-gateway" status --inventory "$INVENTORY" --node e8f1
```

Require the expected ARM64 image identity from `cluster/images.lock.json`. If
there is **no existing gateway**, approve port 4110 and install it:

```bash
"$PY" "$RELEASE/scripts/spark-gateway" up --inventory "$INVENTORY" \
  --node e8f1 --port 4110 --plan "$PLAN"
```

If an owned gateway already exists, **do not run that `up` block** to replace its
registry. Inspect its owner, routes, credentials and recovery policy first. After
approval to add/replace only the `local-coder` alias, preserve the other routes:

```bash
"$PY" "$RELEASE/scripts/spark-gateway" attach --inventory "$INVENTORY" --node e8f1
"$PY" "$RELEASE/scripts/spark-gateway" routes --inventory "$INVENTORY" \
  --node e8f1 --plan "$PLAN" --merge
```

`--merge` upserts the named alias; it still replaces an existing `local-coder`
route, so that alias needs owner approval. An active recovery-owned route or
runtime/key mismatch is a recovery gate, not permission to restart the gateway.
`attach` retrieves only this gateway's registry/key and verifies ownership; it
preserves/report conflicts rather than overwriting a different controller key.

```bash
"$PY" "$RELEASE/scripts/spark-gateway" probe --inventory "$INVENTORY" --node e8f1
KEY_FILE="$HOME/.local/state/local-llm-cluster/gateway/api-key"
REGISTRY="$HOME/.local/state/local-llm-cluster/gateway/config/registry.json"
DEPLOYMENT_DIGEST=$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["digest"])' "$PLAN")
```

Success requires authenticated model discovery including `local-coder`. Discovery
alone is not inference. Keys are private files, not printed/exported into command
arguments; keep directory mode 0700/file mode 0600 and avoid shell tracing. Client
profiles contain environment references, not key values. Provider/cloud credentials
are unrelated, are not needed here and must not be imported with gateway attachment.

**Spark — separate inference approval:** execute this **once per qualification
attempt**, with a new output name. It is a five-request protocol sequence, not a
load benchmark or an agent allowed to modify files. GNU `timeout` supplies a
15-minute total bound in addition to the probe's individual request deadlines.

```bash
test ! -e "$EVIDENCE/gateway-acceptance-01.json"
timeout --signal=TERM --kill-after=10s 900s \
  "$PY" "$RELEASE/scripts/probe-spark-gateway.py" \
  --base-url http://127.0.0.1:4110/v1 --key-file "$KEY_FILE" \
  --model local-coder --expected-deployment "$DEPLOYMENT_DIGEST" \
  --output "$EVIDENCE/gateway-acceptance-01.json"
```

Accept only all five passing checks: `text`, `stream-text`, `automatic-tool-call`,
`tool-result-continuation`, `stream-tool-call`, and exactly the expected deployment
digest. The SSE probes require `[DONE]` and real text/tool deltas. The tool is a
synthetic temperature function: no real outside tool is executed. Verify the
advertised model has `tools: true`; otherwise the script skips tool checks and the
receipt is **not** this runbook's acceptance. Missing output, nonzero exit, timeout,
empty text or a mismatched digest is a failure. Preserve failures and investigate;
do not widen context, concurrency or memory utilization to turn this check green.
This qualifies this exact service's protocol path, not coding quality, vision,
long-context performance, load capacity or recovery eligibility.

## 7. Mac connection and isolated clients

**Mac — local controller/credential/profile writes, no inference until client use.**
Install the same pinned controller using section 2 on the Mac in a fresh source
directory and new private evidence directory. Skip Linux package/bootstrap,
Docker/GPU/model/serving sections there. The same `PREFIX`, `REVISION`, `RELEASE`
and `PY` bindings then refer to **Mac paths**. Keep the Spark evidence path
recorded separately; it is not automatically present on the Mac.

Copy the approved inventory explicitly from the Spark (enter its recorded absolute
path; do not guess another run's directory):

```bash
printf 'Absolute Spark path to the approved inventory JSON: '
read -r SPARK_INVENTORY
case "$SPARK_INVENTORY" in /*) ;; *) exit 1 ;; esac
INVENTORY="$EVIDENCE/inventory.json"
scp "spark-e8f1-wired:$SPARK_INVENTORY" "$INVENTORY"
"$PY" "$RELEASE/scripts/spark-gateway" attach --inventory "$INVENTORY" --node e8f1
KEY_FILE="$PREFIX/state/gateways/e8f1/api-key"
REGISTRY="$PREFIX/state/gateways/e8f1/registry.json"
"$PY" "$RELEASE/scripts/spark-client" render --inventory "$INVENTORY" \
  --node e8f1 --port 4112 --registry "$REGISTRY" --output "$EVIDENCE/client-profiles"
```

Compare the inventory and registry with the approved plan/receipt, and verify the
foundation's host-key-pinned `spark-e8f1-wired` alias. In a dedicated Mac terminal,
rebind `REVISION`, `PREFIX`, `RELEASE`, `PY` and `INVENTORY` to the exact Mac values
above, then keep this foreground tunnel open:

```bash
"$PY" "$RELEASE/scripts/spark-client" tunnel --inventory "$INVENTORY" \
  --node e8f1 --port 4112 --remote-port 4110
```

This expands to SSH `-N -T`, `BatchMode=yes`, `ExitOnForwardFailure=yes`,
`ServerAliveInterval=15`, `ServerAliveCountMax=3` and
`-L 127.0.0.1:4112:127.0.0.1:4110`. It binds only Mac loopback, not all interfaces.
Host-key checking comes from the already verified SSH configuration; never disable
it. The client tunnel uses `node.ssh`, not automatic fallback among management
aliases. A port collision or dead tunnel must be resolved, not bypassed by exposing
the backend. The address for OpenAI-compatible clients is
`http://127.0.0.1:4112/v1`, with the gateway key read from its private local file.
The supported surface is chat completions/model discovery, not the Responses API.

With an approved client already installed, **after inference/client-tool approval**:

```bash
"$PY" "$RELEASE/scripts/spark-client" run --inventory "$INVENTORY" \
  --node e8f1 --port 4112 --registry "$REGISTRY" --key-file "$KEY_FILE" \
  --output "$EVIDENCE/client-profiles" --client omp -- --model spark-e8f1/local-coder
```

The runner uses isolated profiles, clears inherited `OMP_PROFILE`, and supplies
the key through the child environment. It does not install the chosen client or
prove agent quality. OMP tools can access the directory in which the client is
launched; select an approved workspace deliberately, not the live stack checkout.
For a plain API application select model `local-coder`; the `spark-e8f1/` prefix
belongs to generated client profiles, not the HTTP model ID. On the gateway Spark
use loopback port 4110 directly rather than creating a tunnel to itself.
Keep ordinary client/provider settings and credential stores untouched.

## 8. Normal stop, exact-plan restart and restore inputs

**Spark — planned GPU disruption only with owner approval.** Stop new client
requests and wait for outstanding work before stopping. Do not tear down a shared
gateway just to stop this backend; unavailable routes are not advertised as live.
If automation manages this plan, quiesce it and use the recovery runbook's fence
workflow rather than racing its reconciler. An active recovery fence blocks manual
operations and must not be deleted or bypassed.

For a later shell, bind the original paths before any command:

```bash
set -euo pipefail
umask 077
REVISION=65b05d5503e6ffefd2f07d07ebe86333917d5086
PREFIX="$HOME/projects/local-llm-stack-cluster"
RELEASE="$PREFIX/releases/$REVISION"
PY="$RELEASE/.venv/bin/python"
printf 'Absolute original Spark evidence directory: '
read -r EVIDENCE
case "$EVIDENCE" in /*) ;; *) exit 1 ;; esac
INVENTORY="$EVIDENCE/inventory.json"
PLAN="$EVIDENCE/rendered/plan.json"
PLAN_SHA256=$(cat "$EVIDENCE/plan.sha256")
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" --plan-sha256 "$PLAN_SHA256"
"$PY" "$RELEASE/scripts/sparkctl" down --saved-plan "$PLAN" --plan-sha256 "$PLAN_SHA256"
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" --plan-sha256 "$PLAN_SHA256"
```

Require only this plan's containers/reservation to be removed; inspect returned
ownership state, not merely a zero exit. `down` retains model caches, compiled
volumes, controller releases, gateway keys and other applications' data. Keep the
original acceptance evidence even though `down` removes stale endpoint-ready files.
A failed stop or lost receipt enters the recovery workflow; never broaden cleanup.

After a new approved GPU/inference window, confirm the original hash against its
independent approval record. Reuse the saved plan, not a fresh rendering of a recipe
that may have changed. Allocate new evidence paths for every attempt:

```bash
RESTART=$(mktemp -d "$EVIDENCE/restart.XXXXXX")
"$PY" "$RELEASE/scripts/sparkctl" preflight --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --output "$RESTART/preflight"
"$PY" "$RELEASE/scripts/sparkctl" up --saved-plan "$PLAN" \
  --plan-sha256 "$PLAN_SHA256" --timeout 600 --output "$RESTART/start"
"$PY" "$RELEASE/scripts/sparkctl" status --saved-plan "$PLAN" --plan-sha256 "$PLAN_SHA256"
```

Repeat readiness and the approved bounded gateway acceptance with a new output;
old passing receipts do not certify the restarted process. Docker restart policy
helps retained owned containers survive daemon/reboot events, but is not a complete
restore mechanism. If host reboot, driver loss, failed readiness or a fence requires
intervention, follow [recovery](SPARK_RECOVERY_RUNBOOK.md), not ad hoc restarts.

Before calling the service operational, preserve in a private independent backup:

- Full revision, trusted source bundle, matching installer, controller release and
  dependency restoration inputs; these are not the model weights.
- Inventory, stable-address/host-key approval, deployment/recipe, complete saved
  plan, canonical plan hash and independently recorded operator approval.
- Both pinned runtime and gateway image identities plus offline image artifacts;
  the exact selected model snapshot/blobs/symlinks and SHA-verifying manifest/lock.
- Launch/status/protocol evidence tied to that plan; do not invent a successful
  receipt when a run failed or never happened.
- Controller `state/`, node `~/.local/state/local-llm-cluster/`, gateway registry,
  private API key and provider credentials if independently provisioned. Preserve
  permissions and handle credentials separately from shareable diagnostics.
- Existing Docker volumes, databases, unrelated caches and client credentials.
  The eager coder recipe has no compiled runtime-cache requirement; other recipes
  may have labeled persistent volumes that `down` intentionally retains.

Backups of ownership/fence state are evidence, not authorization to copy stale
locks onto a live host. Exact restoration, credential protection and reconciliation
are specified in [the recovery runbook](SPARK_RECOVERY_RUNBOOK.md). No cache-clear,
prune or volume deletion is part of normal standalone stop/restart.

## Command and contract sources

These repository sources define the procedure; reading them does not establish
that any live host is qualified:

- `cluster/deployments/coder-e8f1.json`, `cluster/recipes/qwen3-4b-tools.json`,
  `cluster/images.lock.json`, `cluster/single-node-models.lock.json`.
- `scripts/install-spark-controller.py`, `scripts/bootstrap-spark-packages.sh`,
  `cluster/system-packages.txt`, `scripts/install-omp-spark.sh`.
- `tools/spark_cluster/config.py` (inventory/serving/plan hash), `cli.py`
  (saved-plan lifecycle), `node.py` (admission, structural cache checks, probes).
- `tools/spark_cluster/enrollment.py`, `scripts/fetch-pinned-spark-model.py`,
  `scripts/sync-spark-models.py` (selected staging and content verification).
- `scripts/spark-gateway`, `tools/spark_cluster/gateway_node.py`,
  `scripts/spark-client`, `scripts/probe-spark-gateway.py` (routing, keys, tunnel,
  bounded protocol checks).

The cold-cache metadata snippet uses the pinned runtime's public
`huggingface_hub` API. It is an operator acquisition recipe, not a checked-in
model checksum manifest or a historical passing receipt. Metadata/schema failure
is an explicit stop requiring inspection; do not bypass its budget or hash checks.
