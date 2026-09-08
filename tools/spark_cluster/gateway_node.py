"""Stdlib-only host operations for the optional CPU gateway; no GPU reservation."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path.home() / ".local/state/local-llm-cluster/gateway"


def run(argv):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"gateway {argv[0]} operation failed: {result.stderr[-1000:]}")
    return result.stdout.strip()


def write(path, text, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            os.fchmod(f.fileno(), mode)
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def lookup(name):
    ids = run(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"])
    return json.loads(run(["docker", "inspect", ids]))[0] if ids else None


def checked(saved):
    c = lookup(saved["name"])
    if c and ((c["Config"].get("Labels") or {}).get("io.spark.gateway") != saved["digest"] or
              (saved.get("id") and c["Id"] != saved["id"])):
        raise RuntimeError("gateway container ownership mismatch")
    return c


def await_ready(saved, timeout=20):
    deadline=time.monotonic()+timeout
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while True:
        container=checked(saved)
        if not container or not container['State']['Running']:
            raise RuntimeError('gateway process stopped; saved state retained for inspection/recovery')
        try:
            with opener.open(f"http://127.0.0.1:{saved['port']}/health",timeout=2) as response:
                if json.load(response).get('status')=='ok': return
        except (OSError,ValueError): pass
        if time.monotonic()>=deadline:
            raise RuntimeError('gateway health deadline reached; running container and saved state retained')
        time.sleep(.25)


def main(req):
    if platform.node() != req["node"]["hostname"] or platform.machine() != req["node"]["architecture"]:
        raise RuntimeError("gateway host mismatch")
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (ROOT / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = ROOT / "state.json"
        saved = json.loads(state.read_text()) if state.exists() else None
        if req["action"] == "status":
            c = checked(saved) if saved else None
            return {"installed": bool(saved), "running": bool(c and c["State"]["Running"]),
                    "port": saved["port"] if saved else None}
        if req["action"] == "attach":
            if not saved or not checked(saved):
                raise RuntimeError("gateway not installed")
            # Returned only to the SSH controller; its CLI stores this key in a
            # private file and never includes it in user-visible output.
            return {"api_key": (ROOT / "api-key").read_text().strip(),
                    "registry": json.loads((ROOT / "config/registry.json").read_text()),
                    "port": saved["port"]}
        if req["action"] == "down":
            if saved:
                c = checked(saved)
                if c: run(["docker", "rm", "-f", c["Id"]])
                state.unlink()
            return {"stopped": True}
        if req["action"] == "routes":
            if not saved or not checked(saved): raise RuntimeError("gateway not installed")
            write(ROOT / "config/registry.json", json.dumps(req["registry"], indent=2))
            return {"routes_replaced": True, "aliases": sorted(req["registry"]["routes"])}
        if req["action"] == "probe":
            if not saved or not checked(saved): raise RuntimeError("gateway not installed")
            key = (ROOT / "api-key").read_text().strip()
            r = urllib.request.Request(f"http://127.0.0.1:{saved['port']}/v1/models",
                                       headers={"Authorization": "Bearer " + key})
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(r, timeout=15) as response: models = json.load(response)
            return {"authenticated": True, "models": [m["id"] for m in models["data"]]}
        if req["action"] != "up": raise RuntimeError("unknown gateway operation")
        definition = {k: req[k] for k in ("files", "port", "image")}
        digest = hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        name = "spark-gateway-" + digest[:12]
        if saved and saved["digest"] != digest:
            raise RuntimeError("gateway runtime changed; explicitly stop the old gateway before replacing it")
        if type(req["port"]) is not int or not 1024 <= req["port"] <= 65535:
            raise RuntimeError("invalid gateway port")
        if len(req["api_key"]) < 24 or not all(c.isalnum() or c in "-_" for c in req["api_key"]):
            raise RuntimeError("invalid gateway key")
        if (ROOT / "api-key").exists() and (ROOT / "api-key").read_text().strip() != req["api_key"]:
            raise RuntimeError("controller key differs from installed gateway; use the original credential file")
        run(["docker", "image", "inspect", req["image"], "--format", "{{.Id}}"])
        allowed = {"scripts/context-guard-proxy.py", "tools/spark_cluster/gateway.py", "tools/spark_cluster/config.py", "tools/spark_cluster/__init__.py"}
        if set(req["files"]) != allowed: raise RuntimeError("unexpected gateway source files")
        source = ROOT / digest
        for path, content in req["files"].items(): write(source / path, content, 0o644)
        write(ROOT / "api-key", req["api_key"])
        write(ROOT / "gateway.env", "SPARK_GATEWAY_KEY=" + req["api_key"] + "\n")
        write(ROOT / "config/registry.json", json.dumps(req["registry"], indent=2))
        if not saved:
            saved = {"name": name, "digest": digest, "port": req["port"], "id": None}
            write(state, json.dumps(saved))
        c = checked(saved)
        if not c:
            identity = run(["docker", "create", "--name", name, "--label", "io.spark.gateway=" + digest,
                "--network", "host", "--init", "--restart", "unless-stopped",
                "--log-opt", "max-size=10m", "--log-opt", "max-file=3",
                "--env-file", str(ROOT / "gateway.env"), "-e", "PYTHONPATH=/workspace/tools",
                "--mount", f"type=bind,src={source},dst=/workspace,readonly",
                "--mount", f"type=bind,src={ROOT / 'config'},dst=/registry,readonly",
                "--entrypoint", "python", req["image"], "-m", "spark_cluster.gateway",
                "--registry", "/registry/registry.json", "--port", str(req["port"])])
            saved["id"] = identity
            write(state, json.dumps(saved))
        run(["docker", "start", saved["name"]])
        await_ready(saved)
        return {"installed": True, "ready": True, "container": name, "port": req["port"], "binding": "127.0.0.1"}


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "result": main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
