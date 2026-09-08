"""Execute only explicit operations in a verified, staged looped-LLM project."""
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import sys


def main(req):
    if platform.node() != req["node"]["hostname"]:
        raise RuntimeError("loop workload host mismatch")
    root = Path(req["snapshot"]["path"])
    if not root.is_relative_to(Path(req["node"]["projects"]) / "looped-llm-lab/snapshots"):
        raise RuntimeError("snapshot path is outside the project snapshot directory")
    manifest = json.loads((root / "snapshot.json").read_text())
    identity = manifest.pop("identity")
    calculated = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if calculated != identity or identity != req["snapshot"]["identity"]:
        raise RuntimeError("snapshot identity mismatch")
    for entry in manifest["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("unsafe snapshot path")
        path = root / relative
        if path.is_symlink() or path.stat().st_size != entry["size"] or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise RuntimeError("snapshot contents changed")
    spec = importlib.util.spec_from_file_location("loop_operations", root / "src/looped_llm_lab/operations.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = req["config"]
    action = req["action"]
    def restore():
        state = root / config["state_dir"]
        if (state / "window.json").exists():
            return module.window_restore(config)
        if (state / "jobs" / config["job_id"] / "record.json").exists():
            raise RuntimeError("job exists but its window record is missing; preserve the GPU lease")
        return {"status": "not-entered"}
    if action == "inspect": return module.inspect_host(config)
    if action == "start":
        for key in ("data_dir", "runs_dir"): Path(config["container"][key]).mkdir(parents=True, exist_ok=True)
        window = module.window_enter(config)
        return {"window": window, "job": module.run_job({**config, "action": "start"})}
    if action in ("status", "logs"):
        return module.run_job({**config, "action": action})
    if action == "stop":
        # A failed start may leave a window but no registered job.
        record = root / config["state_dir"] / "jobs" / config["job_id"] / "record.json"
        result = module.run_job({**config, "action": "stop"}) if record.exists() else {"job": "not-created"}
        return {"job": result, "window": restore()}
    if action == "release": return {"window": restore()}
    raise RuntimeError("unsupported loop workload action")


if __name__ == "__main__":
    try: print(json.dumps({"ok": True, "result": main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
