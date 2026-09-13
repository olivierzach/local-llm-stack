"""Host-side stdlib helper transported over SSH, not a resident agent/service.

Reservations are persistent, scoped to the Unix user, and never expire silently.
Every mutation serializes through flock. Cleanup requires matching container IDs
and ownership labels. No sudo, host configuration changes or unrelated stops.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

STATE = Path.home() / ".local/state/local-llm-cluster"


def run(args, timeout=30):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if p.returncode:
        # Do not disclose Docker environment/configuration or command argv.
        raise RuntimeError(f"{args[0]} {args[1] if len(args)>1 else ''} failed ({p.returncode}): {p.stderr[-1500:]}")
    return p.stdout.strip()


def atomic(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, sort_keys=True, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextlib.contextmanager
def locked():
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (STATE / "mutex").open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def reservation():
    p = STATE / "gpu.json"
    return json.loads(p.read_text()) if p.exists() else None


def containers():
    ids = run(["docker", "ps", "-aq"]).split()
    return json.loads(run(["docker", "inspect"] + ids)) if ids else []


def gpu_containers(items):
    return [c for c in items if c["State"]["Status"] in ("running", "restarting", "created", "paused")
            and (c["HostConfig"].get("DeviceRequests") or
                 any("nvidia" in d.get("PathOnHost", "") for d in c["HostConfig"].get("Devices", []) or []))]


def memory():
    data = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return {key: int(data[key].split()[0]) // 1024 for key in ("MemAvailable", "MemTotal", "SwapFree", "SwapTotal")}


def research_window():
    pointer = Path.home() / ".local/state/looped-llm-lab/window-owner.json"
    if not pointer.exists():
        return None
    record = Path(json.loads(pointer.read_text())["record"])
    if not record.exists():
        return "unresolved"
    return json.loads(record.read_text()).get("status", "unresolved")


def doctor(node):
    items = containers()
    names = "nvtop nvidia-smi docker git gh codex python3 ffmpeg cmake ninja tmux htop jq rg rsync iperf3 ethtool rdma ibv_devinfo ib_write_bw mpirun numactl".split()
    search = os.environ.get("PATH", "") + os.pathsep + str(Path.home()/".local/bin") + ":/usr/local/cuda/bin"
    result = {"hostname": platform.node(), "architecture": platform.machine(),
              "memory_mib": memory(), "disk_free_bytes": shutil.disk_usage(node["cache"]).free,
              "gpu_processes": run(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"]).splitlines(),
              "gpu_containers": [{"id": c["Id"], "name": c["Name"], "state": c["State"]["Status"]}
                                  for c in gpu_containers(items)],
              "tools": {n: shutil.which(n, path=search) for n in names + ["omp", "nvcc"]},
              "gpu": run(["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"]),
              "reservation": reservation(), "research_window": research_window(), "fabric": []}
    for rail in node["fabric"]:
        path = Path("/sys/class/net") / rail["interface"]
        result["fabric"].append({**rail, "carrier": (path/"carrier").read_text().strip(),
                                  "speed_mbps": (path/"speed").read_text().strip(),
                                  "mtu": (path/"mtu").read_text().strip(),
                                  "addresses": json.loads(run(["ip", "-j", "address", "show", "dev", rail["interface"]]))})
    return result


def verify_host(node):
    if platform.node() != node["hostname"] or platform.machine() != node["architecture"]:
        raise RuntimeError("SSH reached a host/architecture different from inventory")


def owned(request):
    saved = reservation()
    if not saved or saved["owner"] != request["owner"] or saved["digest"] != request["digest"]:
        raise RuntimeError("reservation ownership mismatch; no changes made")
    return saved


def check_port(address, port):
    # Match server restart semantics: TIME_WAIT is reusable, a listener is not.
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((address, port))
        s.listen(1)



def validate_cached_snapshot(snapshot):
    if snapshot.is_symlink():
        raise RuntimeError("model snapshot directory must not be a symlink")
    if not (snapshot / "config.json").is_file() or not any(snapshot.glob("*.safetensors")):
        raise RuntimeError("pinned model snapshot is not cached; run model sync first")
    for entry in snapshot.rglob("*"):
        if entry.is_symlink() and (not entry.exists() or not entry.resolve().is_relative_to(snapshot.parent.parent.resolve())):
            raise RuntimeError("model snapshot has broken or escaping symlinks")
    index = snapshot / "model.safetensors.index.json"
    if index.exists():
        mapping = json.loads(index.read_text()).get("weight_map")
        if not isinstance(mapping, dict) or not mapping or not all(isinstance(path, str) for path in mapping.values()):
            raise RuntimeError("invalid model weight index")
        for filename in set(mapping.values()):
            relative = Path(filename)
            if relative.is_absolute() or ".." in relative.parts or not filename.endswith(".safetensors"):
                raise RuntimeError("unsafe model shard path")
            path = snapshot / relative
            if not path.is_file() or path.stat().st_size == 0:
                raise RuntimeError("pinned model snapshot is incomplete; missing shard: " + filename)
        # The index is authoritative. GPT-OSS uses zero-based shard names whose
        # suffix denotes the last index, unlike conventional one-based totals.
        return
    numbered = {}
    for path in snapshot.glob("*.safetensors"):
        match = re.fullmatch(r"(.+)-(\d+)-of-(\d+)\.safetensors", path.name)
        if match:
            part, count = int(match[2]), int(match[3])
            if not 1 <= part <= count <= 4096:
                raise RuntimeError("invalid numbered model shard without a weight index")
            numbered.setdefault((match[1], count), set()).add(part)
    if any(parts != set(range(1, count+1)) for (_, count), parts in numbered.items()):
        raise RuntimeError("pinned model snapshot is incomplete; numbered shards missing")


def verify_source_overlays(request):
    verified = []
    for overlay in request['recipe'].get('deepseek_v4', {}).get('source_overlays', []):
        path = Path(request['node']['cache']).parent / 'runtime-overlays' / overlay['sha256'] / overlay['path']
        if not path.is_file() or path.is_symlink():
            raise RuntimeError('pinned source overlay missing or not a regular file')
        if hashlib.sha256(path.read_bytes()).hexdigest() != overlay['sha256']:
            raise RuntimeError('pinned source overlay SHA-256 mismatch')
        verified.append(overlay)
    return verified


def verify_nccl_library(request):
    library = request['recipe'].get('nccl_library')
    if not library:
        return {'override': False}
    path = Path(request['node']['cache']).parent / 'runtime-libraries/nccl' / library['sha256'] / 'libnccl.so.2'
    if not path.is_file() or path.is_symlink():
        raise RuntimeError('pinned NCCL library missing or not a regular file')
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != library['sha256']:
        raise RuntimeError('pinned NCCL library SHA-256 mismatch')
    return {'override': True, 'path': str(path), **library}


def preflight(request):
    """Point-in-time launch diagnostics; never takes a lease or starts a worker."""
    node, recipe = request['node'], request['recipe']
    checks = []

    def observe(label, action):
        try:
            details = action()
            checks.append({'check': label, 'passed': True, 'details': details})
            return details
        except Exception as exc:
            checks.append({'check': label, 'passed': False, 'error': str(exc)})

    def condition(label, passed, details):
        checks.append({'check': label, 'passed': bool(passed), 'details': details})

    report = observe('host-inspection', lambda: doctor(node))
    if report is not None:
        condition('reservation', report['reservation'] is None, report['reservation'])
        condition('research-window', report['research_window'] in (None, 'released', 'restored'), report['research_window'])
        condition('gpu-idle', not report['gpu_processes'] and not report['gpu_containers'],
                  {'processes': report['gpu_processes'], 'containers': report['gpu_containers']})
        condition('shared-memory', report['memory_mib']['MemAvailable'] >= recipe['min_available_mib'],
                  {'available_mib': report['memory_mib']['MemAvailable'], 'required_mib': recipe['min_available_mib']})
        for rail in report['fabric']:
            actual = {a['local'] for item in rail['addresses'] for a in item['addr_info'] if a['family'] == 'inet'}
            condition('fabric-' + rail['interface'], rail['carrier'] == '1' and rail['ip'] in actual,
                      {'carrier': rail['carrier'], 'expected_ip': rail['ip'], 'actual_ips': sorted(actual), 'mtu': rail['mtu']})

    def image_check():
        image = json.loads(run(['docker', 'image', 'inspect', recipe['image']]))[0]
        expected = {'aarch64': 'arm64', 'x86_64': 'amd64'}[node['architecture']]
        if image['Architecture'] != expected:
            raise RuntimeError('container architecture mismatch')
        return {'image': recipe['image'], 'architecture': image['Architecture']}

    def cache_check():
        snapshot = Path(node['cache']) / 'hub' / ('models--' + recipe['model'].replace('/', '--')) / 'snapshots' / recipe['revision']
        validate_cached_snapshot(snapshot)
        return {'snapshot': str(snapshot), 'weight_shards': len(list(snapshot.glob('*.safetensors'))),
                'qualification': 'Structural check only; use the model-copy receipt for SHA-256 verification.'}

    def port_check(port):
        check_port(node['fabric'][0]['ip'], port)
        return {'address': node['fabric'][0]['ip'], 'port': port}

    observe('runtime-image', image_check)
    if recipe.get('deepseek_v4', {}).get('source_overlays'):
        observe('source-overlays', lambda: verify_source_overlays(request))
    if recipe.get('nccl_library'):
        observe('nccl-library', lambda: verify_nccl_library(request))
    observe('model-cache', cache_check)
    observe('api-port', lambda: port_check(request['deployment']['port']))
    if request['deployment']['mode'] != 'single':
        condition('rdma-devices', Path('/dev/infiniband').is_dir(), {'path': '/dev/infiniband'})
        observe('master-port', lambda: port_check(request['deployment']['master_port']))
    return {'launchable': all(item['passed'] for item in checks), 'checks': checks,
            'qualification': 'Point-in-time diagnostics. Ports are briefly bound and closed. No GPU reservation or worker is created; up rechecks admission.'}


def reserve(request):
    old = reservation()
    if old:
        owned(request)
        return {"reserved": True, "existing": True}
    node, recipe = request["node"], request["recipe"]
    verify_nccl_library(request)
    verify_source_overlays(request)
    report = doctor(node)
    if report.get("research_window") not in (None, "released", "restored"):
        raise RuntimeError("looped-LLM has an unresolved GPU window; recover it with its owning project")
    if report["gpu_processes"] or report["gpu_containers"]:
        raise RuntimeError("GPU is busy or a GPU container is pending; no workloads stopped")
    if report["memory_mib"]["MemAvailable"] < recipe["min_available_mib"]:
        raise RuntimeError("insufficient available shared memory")
    for rail in report["fabric"]:
        actual = {a["local"] for i in rail["addresses"] for a in i["addr_info"] if a["family"] == "inet"}
        if rail["carrier"] != "1" or rail["ip"] not in actual:
            raise RuntimeError("fabric carrier/address does not match inventory")
    image = json.loads(run(["docker", "image", "inspect", recipe["image"]]))[0]
    if image["Architecture"] != {"aarch64": "arm64", "x86_64": "amd64"}[node["architecture"]]:
        raise RuntimeError("container architecture mismatch")
    snapshot = Path(node["cache"]) / "hub" / ("models--" + recipe["model"].replace("/", "--")) / "snapshots" / recipe["revision"]
    validate_cached_snapshot(snapshot)
    check_port(node["fabric"][0]["ip"], request["deployment"]["port"])
    if request["deployment"]["mode"] != "single":
        if not Path("/dev/infiniband").is_dir():
            raise RuntimeError("RDMA devices unavailable")
        check_port(node["fabric"][0]["ip"], request["deployment"]["master_port"])
    atomic(STATE / "gpu.json", {"owner": request["owner"], "digest": request["digest"],
                                "created_at": time.time(), "container_ids": [], "phase": "reserved"})
    return {"reserved": True, "existing": False}


def status(request):
    saved = reservation()
    items = [c for c in containers() if (c["Config"].get("Labels") or {}).get("io.spark.owner") == request["owner"]]
    return {"reservation": saved, "containers": [{"id": c["Id"], "state": c["State"]["Status"],
             "exit_code": c["State"]["ExitCode"], "health": c["State"].get("Health", {}).get("Status")}
             for c in items]}


def runtime_cache(request, create=False, clear=False):
    definition = request["compose"].get("volumes", {}).get("runtime-cache")
    if not request["recipe"].get("runtime_cache"):
        if definition: raise RuntimeError("unexpected runtime cache volume")
        return {"enabled": False}
    if not isinstance(definition, dict) or set(definition) != {"name", "external"} or definition["external"] is not True:
        raise RuntimeError("invalid runtime cache definition")
    name = definition["name"]
    if not re.fullmatch(r"spark-runtime-[a-f0-9]{64}", name):
        raise RuntimeError("invalid runtime cache name")
    labels = {"io.spark.runtime-cache": name.removeprefix("spark-runtime-"),
              "io.spark.cache-format": "1", "io.spark.cache-user": str(os.getuid())}
    names = run(["docker", "volume", "ls", "--format", "{{.Name}}"]).splitlines()
    if name not in names and create:
        args = ["docker", "volume", "create"]
        for key, value in labels.items(): args += ["--label", key+"="+value]
        run(args+[name])
        names.append(name)
    if name not in names:
        return {"enabled": True, "name": name, "present": False}
    info = json.loads(run(["docker", "volume", "inspect", name]))[0]
    if info.get("Name") != name or any((info.get("Labels") or {}).get(k) != v for k, v in labels.items()):
        raise RuntimeError("runtime cache ownership mismatch; no cache changes made")
    if clear:
        # Docker refuses removal while any container, including a stopped one,
        # references the volume. Never prune or stop those containers here.
        run(["docker", "volume", "rm", name])
    return {"enabled": True, "name": name, "present": not clear}


def start(request):
    saved = owned(request)
    verify_nccl_library(request)
    verify_source_overlays(request)
    folder = STATE / request["owner"]
    folder.mkdir(exist_ok=True, mode=0o700)
    compose = folder / "compose.json"
    if compose.exists() and json.loads(compose.read_text()) != request["compose"]:
        raise RuntimeError("saved Compose definition changed")
    runtime_cache(request, create=True)
    atomic(compose, request["compose"])
    base = ["docker", "compose", "-f", str(compose)]
    run(base + ["config", "--quiet"])
    if saved["phase"] == "reserved":
        # Journal intent before create. Recover checks deterministic name+labels
        # if the controller connection dies between create and ID persistence.
        saved["phase"] = "creating"
        atomic(STATE / "gpu.json", saved)
    if saved["phase"] == "creating":
        run(base + ["create", "--pull", "never"], timeout=120)
        items = [c for c in containers() if (c["Config"].get("Labels") or {}).get("io.spark.owner") == request["owner"]]
        if len(items) != len(request["compose"]["services"]):
            raise RuntimeError("created container count mismatch")
        if any(c["Config"]["Labels"].get("io.spark.digest") != request["digest"] for c in items):
            raise RuntimeError("container digest label mismatch")
        saved["container_ids"] = [c["Id"] for c in items]
        saved["phase"] = "created"
        atomic(STATE / "gpu.json", saved)
    if saved["phase"] == "created":
        run(["docker", "start"] + saved["container_ids"], timeout=120)
        saved["phase"] = "started"
        atomic(STATE / "gpu.json", saved)
    return status(request)


def stop(request):
    saved = reservation()
    if not saved:
        # No reservation is not permission to remove anything with a similar name.
        return {"released": True, "already_absent": True}
    owned(request)
    if saved["phase"] == "workload":
        raise RuntimeError("batch workload reservation requires its project-specific cleanup")
    expected = set(saved["container_ids"])
    all_items = containers()
    items = [c for c in all_items if (c["Config"].get("Labels") or {}).get("io.spark.owner") == request["owner"] or c["Id"] in expected]
    for c in items:
        labels = c["Config"].get("Labels") or {}
        if labels.get("io.spark.owner") != request["owner"] or labels.get("io.spark.digest") != request["digest"]:
            raise RuntimeError("cleanup refused: container digest mismatch")
        if expected and c["Id"] not in expected:
            raise RuntimeError("cleanup refused: container was replaced")
        if not expected and saved["phase"] != "creating":
            raise RuntimeError("cleanup refused: unexpected unjournaled container")
    for c in items:
        log_dir = STATE / request["owner"] / "logs"
        log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        log = subprocess.run(["docker", "logs", "--tail", "500", c["Id"]],
                             capture_output=True, text=True, timeout=30)
        log_path = log_dir / (c["Id"] + ".log")
        with log_path.open("w") as f:
            os.chmod(log_path, 0o600)
            f.write(log.stdout + log.stderr)
        run(["docker", "rm", "-f", c["Id"]], timeout=120)
    if any((c["Config"].get("Labels") or {}).get("io.spark.owner") == request["owner"] for c in containers()):
        raise RuntimeError("cleanup incomplete; reservation retained")
    (STATE / "gpu.json").unlink()
    return {"released": True}


def workload_reservation(request, release=False, batch=False):
    saved = reservation()
    if saved:
        owned(request)
        if saved["phase"] != "workload":
            raise RuntimeError("reservation belongs to an inference deployment")
        if not release:
            return {"reserved": True, "existing": True}
    elif release:
        return {"released": True, "already_absent": True}
    report = doctor(request["node"])
    if report["gpu_processes"] or report["gpu_containers"]:
        raise RuntimeError("GPU workload is active or pending; reservation unchanged")
    if report.get("research_window") not in (None, "released", "restored"):
        raise RuntimeError("research window is unresolved; restore it with its owning project")
    if release:
        (STATE / "gpu.json").unlink()
        return {"released": True}
    minimum = request.get("min_available_mib", 16384)
    if type(minimum) is not int or minimum < 1024 or report["memory_mib"]["MemAvailable"] < minimum:
        raise RuntimeError("insufficient available shared memory for workload")
    atomic(STATE / "gpu.json", {"owner": request["owner"], "digest": request["digest"],
        "phase": "reserved" if batch else "workload", "container_ids": [], "created_at": time.time(),
        **({"kind": "batch"} if batch else {})})
    return {"reserved": True, "existing": False}


def reserve_batch(request):
    saved = reservation()
    if saved:
        owned(request)
        if saved.get("kind") != "batch": raise RuntimeError("reservation belongs to another workload kind")
        return {"reserved": True, "existing": True}
    image = request["compose"]["services"]["worker"]["image"]
    if not re.fullmatch(r"[A-Za-z0-9./_-]+@sha256:[a-f0-9]{64}", image):
        raise RuntimeError("batch image must use an immutable registry digest")
    metadata = json.loads(run(["docker", "image", "inspect", image]))[0]
    if metadata["Architecture"] != {"aarch64": "arm64", "x86_64": "amd64"}[request["node"]["architecture"]]:
        raise RuntimeError("batch image architecture mismatch")
    return workload_reservation(request, batch=True)


def probe(request):
    url = request["endpoint"]["base_url"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url + "/models", timeout=5) as f:
        models = json.load(f)
    if request["recipe"]["alias"] not in [m["id"] for m in models["data"]]:
        raise RuntimeError("endpoint serves a different model alias")
    payload = {"model": request["recipe"]["alias"], "messages": [{"role": "user", "content": "Reply with exactly: ready"}],
               "max_tokens": 16, "temperature": 0}
    start_time = time.monotonic()
    req = urllib.request.Request(url + "/chat/completions", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with opener.open(req, timeout=90) as f:
        result = json.load(f)
    content = result["choices"][0]["message"].get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("model returned no text")
    return {"ready": True, "elapsed_s": round(time.monotonic()-start_time, 3), "usage": result.get("usage"),
            "reply": content, "model": result["model"]}


def main(request):
    verify_host(request["node"])
    action = request["action"]
    if action == "doctor":
        return doctor(request["node"])
    if not re.fullmatch(r"[a-z0-9-]{1,70}", request["owner"]) or not re.fullmatch(r"[a-f0-9]{64}", request["digest"]):
        raise RuntimeError("invalid ownership identity")
    if action == "probe":
        return probe(request)
    if action == "preflight":
        return preflight(request)
    with locked():
        if action == "reserve-batch": return reserve_batch(request)
        if action == "reserve-workload": return workload_reservation(request)
        if action == "release-workload": return workload_reservation(request, release=True)
        if action == "cache-status": return runtime_cache(request)
        if action == "cache-clear": return runtime_cache(request, clear=True)
        if action == "reserve": return reserve(request)
        if action == "start": return start(request)
        if action == "stop": return stop(request)
        if action == "status": return status(request)
        raise RuntimeError("unknown node operation")


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "result": main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
