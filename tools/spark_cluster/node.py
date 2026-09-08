"""Host-side stdlib helper transported over SSH, not a resident agent/service.

Reservations are persistent, scoped to the Unix user, and never expire silently.
Every mutation serializes through flock. Cleanup requires matching container IDs
and ownership labels. No sudo, host configuration changes or unrelated stops.
"""
from __future__ import annotations

import contextlib
import fcntl
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


def reserve(request):
    old = reservation()
    if old:
        owned(request)
        return {"reserved": True, "existing": True}
    node, recipe = request["node"], request["recipe"]
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
    if not (snapshot / "config.json").is_file() or not any(snapshot.glob("*.safetensors")):
        raise RuntimeError("pinned model snapshot is not cached; run model sync first")
    for entry in snapshot.iterdir():
        if entry.is_symlink() and not entry.exists():
            raise RuntimeError("model snapshot has broken symlinks")
    with socket.socket() as s:
        s.bind((node["fabric"][0]["ip"], request["deployment"]["port"]))
    if request["deployment"]["mode"] != "single":
        if not Path("/dev/infiniband").is_dir():
            raise RuntimeError("RDMA devices unavailable")
        with socket.socket() as s:
            s.bind((node["fabric"][0]["ip"], request["deployment"]["master_port"]))
    atomic(STATE / "gpu.json", {"owner": request["owner"], "digest": request["digest"],
                                "created_at": time.time(), "container_ids": [], "phase": "reserved"})
    return {"reserved": True, "existing": False}


def status(request):
    saved = reservation()
    items = [c for c in containers() if (c["Config"].get("Labels") or {}).get("io.spark.owner") == request["owner"]]
    return {"reservation": saved, "containers": [{"id": c["Id"], "state": c["State"]["Status"],
             "exit_code": c["State"]["ExitCode"], "health": c["State"].get("Health", {}).get("Status")}
             for c in items]}


def start(request):
    saved = owned(request)
    folder = STATE / request["owner"]
    folder.mkdir(exist_ok=True, mode=0o700)
    compose = folder / "compose.json"
    if compose.exists() and json.loads(compose.read_text()) != request["compose"]:
        raise RuntimeError("saved Compose definition changed")
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
    with locked():
        if action == "reserve-batch": return reserve_batch(request)
        if action == "reserve-workload": return workload_reservation(request)
        if action == "release-workload": return workload_reservation(request, release=True)
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
