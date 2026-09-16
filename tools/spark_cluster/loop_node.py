"""Execute only explicit operations in a verified, staged looped-LLM project."""
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import sys


def artifacts(req, status):
    if status['status'] not in {'succeeded','failed','stopped','interrupted','launch_failed'}:
        raise RuntimeError('job is not terminal; artifacts may still be changing')
    config = req['config']
    directory = Path(config['container']['runs_dir'])
    expected = Path(req['node']['projects']) / 'looped-llm-lab/runs/cluster' / config['job_id']
    if directory != expected or directory.is_symlink():
        raise RuntimeError('artifact collection requires this job\'s isolated run directory')
    files = {}
    for relative in req['artifacts']:
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts or not path.parts:
            raise RuntimeError('unsafe artifact path')
        source = directory / path
        if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(directory.resolve()):
            raise RuntimeError('artifact is missing or escapes its job directory')
        digest = hashlib.sha256()
        with source.open('rb') as f:
            for chunk in iter(lambda:f.read(4*1024*1024),b''): digest.update(chunk)
        files[str(path)] = {'path':str(source),'size':source.stat().st_size,'sha256':digest.hexdigest()}
    return {'job_id':config['job_id'],'status':status['status'],'snapshot':req['snapshot']['identity'],'files':files}


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
    if action == 'artifacts': return artifacts(req,module.run_job({**config,'action':'status'}))
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
