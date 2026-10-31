"""Noninteractive CLI. All deployment inputs are explicit versioned files."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time

from .config import ConfigError, load, plan, read, validate_inventory, validate_saved_plan

ROOT = Path(__file__).resolve().parents[2]
NODE_SOURCE = Path(__file__).with_name("node.py")


def remote(node, request, source_path=NODE_SOURCE):
    source = source_path.read_text()
    command = ["python3", "-c", source]
    if socket.gethostname() != node["hostname"]:
        command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                   "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
                   node["ssh"], shlex.join(command)]
    result = subprocess.run(command, input=json.dumps(request), text=True,
                            capture_output=True, timeout=240)
    try:
        response = json.loads(result.stdout)
    except ValueError:
        raise RuntimeError(f"{node['hostname']}: transport failed ({result.returncode}): {result.stderr[-500:]}") from None
    if result.returncode or not response.get("ok"):
        raise RuntimeError(f"{node['hostname']}: {response.get('error', 'node operation failed')}")
    return response["result"]


def request(p, node_id, action):
    return {"action": action, "node": p["nodes"][node_id], "owner": p["owner"], "digest": p["digest"],
            "recipe": p["recipe"], "deployment": p["deployment"],
            "compose": p["compose"][node_id], "endpoint": p["endpoint"]}


def call(p, node_id, action):
    return remote(p["nodes"][node_id], request(p, node_id, action))


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    try:
        with temporary.open("x") as f:
            json.dump(value, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def emit(value):
    print(json.dumps(value, sort_keys=True), flush=True)


def inspect(p):
    results = {node: call(p, node, "status") for node in p["nodes"]}
    healthy = all(s["containers"] and all(c["state"] == "running" and c["health"] == "healthy"
                                        for c in s["containers"]) for s in results.values())
    return {"owner": p["owner"], "healthy": healthy, "nodes": results}


def warmup(p, timeout=600):
    """Exercise decode and verify draft activity for DSpark before readiness."""
    from .warmup import run_warmup, warmup_speculative_payload

    speculative = p["recipe"].get("speculative_config") or {}
    payload = warmup_speculative_payload(p["recipe"]["alias"], speculative)
    return run_warmup(p["endpoint"]["base_url"], payload, timeout=timeout,
                      require_speculation=speculative.get("method") == "dspark")


def up(p, timeout, output, warmup_timeout=600):
    # Save exact inputs before any side effects. This artifact is the recovery
    # handle even if inventory or recipes subsequently change.
    save_json(output / "plan.json", p)
    for node, compose in p["compose"].items():
        save_json(output / f"compose-{node}.json", compose)
    cleanup = []
    try:
        for node in sorted(p["nodes"]):
            existing = call(p, node, "status")["reservation"]
            if existing is None:
                # Include the attempted node before sending: reply loss may still
                # mean the reservation succeeded on the other end.
                cleanup.append(node)
            result = call(p, node, "reserve")
            emit({"node": node, **result})
        for node in p["deployment"]["nodes"]:
            emit({"node": node, "start": call(p, node, "start")})
        deadline = time.monotonic() + timeout
        while True:
            report = inspect(p)
            if report["healthy"]:
                # A health response is not proof that speculative decode works.
                # Warmup failure follows the same owned cleanup path as probe failure.
                if (p["recipe"].get("speculative_config") or {}).get("method") == "dspark":
                    emit({"warmup": warmup(p, warmup_timeout)})
                evidence = call(p, p["deployment"]["coordinator"], "probe")
                save_json(output / "acceptance.json", evidence)
                # Desired endpoint is never published as ready before inference.
                endpoint = {**p["endpoint"], "ready": True, "owner": p["owner"], "digest": p["digest"]}
                save_json(output / "endpoint.json", endpoint)
                emit({"endpoint": endpoint, "acceptance": evidence})
                return
            if any(c["state"] in ("exited", "dead") for s in report["nodes"].values() for c in s["containers"]):
                raise RuntimeError("worker exited during startup; inspect owned container logs")
            if time.monotonic() >= deadline:
                raise RuntimeError("startup deadline exceeded")
            emit({"owner": p["owner"], "phase": "waiting-for-health"})
            time.sleep(min(10, max(0.1, deadline-time.monotonic())))
    except BaseException:
        errors = []
        for node in reversed(cleanup):
            try:
                call(p, node, "stop")
            except Exception as exc:
                errors.append(str(exc))
        # Preserve exact recovery inputs, remove stale readiness advertisement.
        (output / "endpoint.json").unlink(missing_ok=True)
        if errors:
            emit({"cleanup_incomplete": errors, "recover_with": f"sparkctl down --saved-plan {output / 'plan.json'}"})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "render", "doctor", "preflight", "up", "status", "down", "probe", "collectives", "cache-status", "cache-clear", "warmup", "watchdog"))
    parser.add_argument("--inventory", type=Path, default=ROOT / "cluster/inventory.json")
    parser.add_argument("--deployment", type=Path)
    parser.add_argument("--saved-plan", type=Path, help="exact rendered inputs for status/down/recovery")
    parser.add_argument("--node", help="limit doctor to one inventory node")
    parser.add_argument("--output", type=Path, help="local rendered plan and evidence directory")
    parser.add_argument("--timeout", type=int, default=600, help="startup/watchdog recovery health deadline in seconds")
    parser.add_argument("--warmup-timeout", type=int, default=600, help="total warmup and draft-metric verification deadline in seconds")
    parser.add_argument("--stall-seconds", type=int, default=180, help="watchdog: generation-freeze before restart")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="watchdog: probe interval in seconds")
    parser.add_argument("--max-restarts", type=int, default=3, help="watchdog: consecutive restarts before giving up")
    parser.add_argument("--watchdog-once", action="store_true", help="watchdog: exit after the first supervised interval")
    args = parser.parse_args(argv)
    if args.action in ("up", "watchdog") and not 10 <= args.timeout <= 7200:
        raise ConfigError("startup/recovery timeout must be 10..7200 seconds")
    if args.action in ("up", "warmup") and not 1 <= args.warmup_timeout <= 7200:
        raise ConfigError("warmup timeout must be 1..7200 seconds")
    if args.action == "doctor":
        inv = read(args.inventory)
        validate_inventory(inv)
        nodes = inv["nodes"]
        if args.node:
            if args.node not in nodes: raise ConfigError("unknown doctor node")
            nodes = {args.node: nodes[args.node]}
        failures = 0
        for node_id, node in nodes.items():
            try:
                emit({"node": node_id, "doctor": remote(node, {"action": "doctor", "node": node})})
            except Exception as exc:
                emit({"node": node_id, "error": str(exc)})
                failures += 1
        return int(bool(failures))
    if args.saved_plan:
        if args.deployment or args.action not in ("preflight", "status", "down", "probe", "cache-status", "cache-clear", "warmup", "watchdog"):
            raise ConfigError("saved plans are for preflight/status/down/probe/cache operations, without --deployment")
        p = read(args.saved_plan)
        validate_saved_plan(p)
    else:
        if not args.deployment: raise ConfigError("--deployment is required")
        p = plan(*load(ROOT, args.inventory, args.deployment))
    output = args.output or ROOT / "data/cluster" / p["owner"]
    if args.action == "validate":
        emit({"valid": True, "owner": p["owner"], "validation": p["recipe"]["validation"]})
    elif args.action == "render":
        save_json(output / "plan.json", p)
        for node, compose in p["compose"].items(): save_json(output / f"compose-{node}.json", compose)
        emit({"owner": p["owner"], "output": str(output), "endpoint": p["endpoint"]})
    elif args.action == "preflight":
        reports = {}
        for node_id in p['nodes']:
            try:
                reports[node_id] = call(p, node_id, 'preflight')
            except Exception as exc:
                reports[node_id] = {'launchable': False, 'error': str(exc)}
        result = {'owner': p['owner'], 'digest': p['digest'], 'nodes': reports,
                  'launchable': all(report['launchable'] for report in reports.values()),
                  'recipe_validation': p['recipe']['validation']}
        save_json(output / 'preflight.json', result)
        emit(result)
        return int(not result['launchable'])
    elif args.action == "up":
        up(p, args.timeout, output, warmup_timeout=args.warmup_timeout)
    elif args.action == "warmup":
        emit({"warmup": warmup(p, args.warmup_timeout)})
    elif args.action == "watchdog":
        from .watchdog import HttpProbe, StallDetector, run_watchdog

        probe = HttpProbe(p["endpoint"]["base_url"], timeout=5)
        detector = StallDetector(stall_seconds=args.stall_seconds,
                                 health_grace=max(10, args.poll_interval * 2))
        def restart():
            for node in p["nodes"]:
                emit({"node": node, "restart": call(p, node, "restart")})

        restarts = run_watchdog(
            detector, probe, restart,
            poll_interval=args.poll_interval, max_restarts=args.max_restarts,
            recovery_timeout=args.timeout,
            stop_after=(args.poll_interval if args.watchdog_once else None))
        emit({"owner": p["owner"], "watchdog_exit": "ok", "restarts": restarts})
    elif args.action == "status":
        emit(inspect(p))
    elif args.action == "probe":
        emit(call(p, p["deployment"]["coordinator"], "probe"))
    elif args.action in ("cache-status", "cache-clear"):
        for node in p["deployment"]["nodes"]:
            emit({"node": node, "cache": call(p, node, args.action)})
    elif args.action == "collectives":
        from .profile import collectives
        emit(collectives(p, output, call))
    elif args.action == "down":
        errors = []
        for node in reversed(p["deployment"]["nodes"]):
            try: emit({"node": node, **call(p, node, "stop")})
            except Exception as exc: errors.append(str(exc))
        (output / "endpoint.json").unlink(missing_ok=True)
        if args.saved_plan: (args.saved_plan.parent / "endpoint.json").unlink(missing_ok=True)
        if errors: raise RuntimeError("; ".join(errors))
    return 0


def entrypoint():
    try:
        return main()
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print(f"sparkctl: {exc}", file=sys.stderr)
        return 1
