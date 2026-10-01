"""Noninteractive CLI. All deployment inputs are explicit versioned files."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, wait
import json
import math
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time

from .config import (ConfigError, load, plan, read, validate_inventory, validate_saved_plan,
                     plan_sha256, ssh_targets)

ROOT = Path(__file__).resolve().parents[2]
NODE_SOURCE = Path(__file__).with_name("node.py")
READ_ONLY = frozenset({"identity", "observe", "monitor", "doctor", "status", "preflight", "cache-status"})


class AmbiguousMutationError(RuntimeError):
    """A dispatched mutation may have completed; reconcile before any new action."""


def remote(node, request, source_path=NODE_SOURCE, *, timeout=240):
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ConfigError("remote timeout must be positive and finite")
    targets = ssh_targets(node)
    deadline = time.monotonic() + timeout
    # Verify even custom helpers and local transports before any helper code runs.
    source = ("import platform\n"
              f"if (platform.node(), platform.machine()) != {(node['hostname'], node['architecture'])!r}:\n"
              " raise RuntimeError('reached host identity mismatch')\n"
              "exec(" + repr(source_path.read_text()) + ")")
    local = socket.gethostname() == node["hostname"]
    if local:
        targets = [None]

    def execute(target, payload, budget):
        command = ["python3", "-c", source]
        if target is not None:
            command = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                       "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5",
                       "-o", "ServerAliveCountMax=1", target, shlex.join(command)]
        result = subprocess.run(command, input=json.dumps(payload), text=True,
                                capture_output=True, timeout=budget)
        try:
            response = json.loads(result.stdout)
        except ValueError:
            raise ConnectionError("transport returned no valid operation receipt") from None
        if not isinstance(response, dict) or "ok" not in response:
            raise ConnectionError("invalid operation receipt")
        if not response["ok"]:
            raise RuntimeError(f"{node['hostname']}: {response.get('error', 'node operation failed')}")
        if result.returncode or "result" not in response:
            raise ConnectionError("transport ended without a successful receipt")
        return response["result"]

    errors = []
    readonly = request.get("action") in READ_ONLY
    for index, target in enumerate(targets):
        budget = (deadline - time.monotonic()) / (len(targets) - index)
        if budget <= 0:
            break
        if not readonly and len(targets) > 1:
            # Selection is read-only; the mutation itself is sent exactly once.
            try:
                remote({**node, "management": {"ssh_targets": [target]}},
                       {"action": "identity", "node": request["node"]}, timeout=budget)
            except (RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
                errors.append(type(exc).__name__)
                continue
            budget = deadline - time.monotonic()
            if budget <= 0:
                break
        try:
            payload = dict(request)
            if payload.get("action") == "observe":
                payload["timeout"] = min(240, budget)
            return execute(target, payload, budget)
        except (ConnectionError, OSError, subprocess.TimeoutExpired) as exc:
            if not readonly:
                raise AmbiguousMutationError(f"{node['hostname']}: mutation receipt lost; reconcile via observe before retry") from exc
            errors.append(type(exc).__name__)
        except RuntimeError:
            # An application rejection is authoritative, not a transport failure.
            raise
    raise RuntimeError(f"{node['hostname']}: management unavailable ({', '.join(errors) or 'deadline exceeded'})")


def request(p, node_id, action, *, recovery=None):
    result = {"action": action, "node": p["nodes"][node_id], "owner": p["owner"], "digest": p["digest"],
              "recipe": p["recipe"], "deployment": p["deployment"],
              "compose": p["compose"][node_id], "endpoint": p["endpoint"]}
    if recovery is not None:
        result["recovery"] = recovery
    return result


def transport_node(node, transport):
    if transport is None:
        return node
    if not isinstance(transport, dict) or set(transport) - {"hostname", "architecture", "ssh", "management"}:
        raise ConfigError("transport override may only change trusted SSH targets")
    for key in ("hostname", "architecture"):
        if key in transport and transport[key] != node[key]:
            raise ConfigError("transport override changed host identity")
    result = {**node, **transport}
    if "ssh" in transport and "management" not in transport:
        result.pop("management", None)
    ssh_targets(result)
    return result


def call(p, node_id, action, *, recovery=None, timeout=240, transport=None):
    return remote(transport_node(p["nodes"][node_id], transport),
                  request(p, node_id, action, recovery=recovery), timeout=timeout)


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    created = False
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
        with os.fdopen(fd, "w") as f:
            json.dump(value, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if created:
            temporary.unlink(missing_ok=True)


def save_exclusive(path, value):
    """Observation evidence cannot overwrite a plan, symlink or previous report."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def emit(value):
    print(json.dumps(value, sort_keys=True), flush=True)


def observe(nodes, *, p=None, timeout=15, transports=None):
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 < timeout <= 240:
        raise ConfigError("observation timeout must be greater than zero and at most 240 seconds")
    deadline = time.monotonic() + timeout
    pool = ThreadPoolExecutor(max_workers=min(len(nodes), 32))

    def collect(node_id, node):
        payload = request(p, node_id, "observe") if p else {"action": "observe", "node": node}
        return remote(transport_node(node, (transports or {}).get(node_id)), payload,
                      timeout=max(0.001, deadline - time.monotonic()))

    futures = {pool.submit(collect, node_id, node): node_id for node_id, node in nodes.items()}
    done, pending = wait(futures, timeout=max(0, deadline - time.monotonic()))
    results = {}
    for future in done:
        node_id = futures[future]
        try:
            results[node_id] = future.result()
        except Exception as exc:
            results[node_id] = {"complete": False, "error": type(exc).__name__}
    for future in pending:
        results[futures[future]] = {"complete": False, "error": "overall deadline exceeded"}
        future.cancel()
    pool.shutdown(wait=False, cancel_futures=True)
    return {"observed_at": time.time(), "complete": all(r.get("complete") is True for r in results.values()),
            "nodes": results}


def inspect(p, *, recovery=None, transports=None, timeout=None):
    options = {"recovery": recovery} if recovery is not None else {}
    deadline = time.monotonic() + timeout if timeout is not None else None
    results = {}
    for node in p["nodes"]:
        per_node = dict(options)
        if transports and node in transports:
            per_node["transport"] = transports[node]
        if deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("status deadline exceeded")
            per_node["timeout"] = left
        results[node] = call(p, node, "status", **per_node)
    healthy = all(
        isinstance(s.get("reservation"), dict) and
        s["reservation"].get("owner") == p["owner"] and s["reservation"].get("digest") == p["digest"] and
        len(s.get("containers", [])) == len(p["compose"][node]["services"]) and
        {c["id"] for c in s["containers"]} == set(s["reservation"].get("container_ids", [])) and
        all(c.get("state") == "running" and c.get("health") == "healthy" and
            c.get("owner") == p["owner"] and c.get("digest") == p["digest"] and
            c.get("image") == p["recipe"]["image"] and c.get("image_id") and
            c.get("started_at") and type(c.get("restart_count")) is int
            for c in s["containers"])
        for node, s in results.items())
    return {"owner": p["owner"], "healthy": healthy, "nodes": results}


def up(p, timeout, output, *, recovery=None, transports=None, expected_sha256=None, saved_plan=False):
    validate_saved_plan(p, expected_sha256)
    if (saved_plan or recovery is not None) and expected_sha256 is None:
        raise ConfigError("exact recovery activation requires a trusted full plan SHA-256")
    output = Path(output)
    plan_path = output / "plan.json"
    if plan_path.exists():
        if plan_sha256(read(plan_path)) != plan_sha256(p):
            raise ConfigError("refusing to overwrite a different saved plan")
    else:
        save_json(plan_path, p)
    for node, compose in p["compose"].items():
        target = output / f"compose-{node}.json"
        if target.exists():
            if read(target) != compose:
                raise ConfigError("saved Compose artifact differs")
        else:
            save_json(target, compose)
    cleanup = []
    deadline = time.monotonic() + timeout

    def invoke(node, action, *, cleanup_call=False):
        options = {}
        if recovery is not None:
            options["recovery"] = recovery
        if transports and node in transports:
            options["transport"] = transports[node]
        # Legacy call wrappers remain valid when no recovery options are supplied.
        if recovery is not None or transports:
            left = 30 if cleanup_call else deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("startup deadline exceeded")
            options["timeout"] = min(240, left)
        return call(p, node, action, **options)

    try:
        for node in sorted(p["nodes"]):
            existing = invoke(node, "status")["reservation"]
            if existing is None:
                cleanup.append(node)
            elif existing.get("owner") != p["owner"] or existing.get("digest") != p["digest"]:
                raise RuntimeError("foreign reservation prevents activation")
            elif recovery is not None and existing.get("phase") != "started":
                cleanup.append(node)
            emit({"node": node, **invoke(node, "reserve")})
        for node in p["deployment"]["nodes"]:
            emit({"node": node, "start": invoke(node, "start")})
        while True:
            report = inspect(p, recovery=recovery, transports=transports,
                             timeout=max(0.001, deadline - time.monotonic())) if recovery is not None or transports else inspect(p)
            if report["healthy"]:
                evidence = invoke(p["deployment"]["coordinator"], "probe")
                save_json(output / "acceptance.json", evidence)
                endpoint = {**p["endpoint"], "ready": True, "owner": p["owner"], "digest": p["digest"]}
                save_json(output / "endpoint.json", endpoint)
                emit({"endpoint": endpoint, "acceptance": evidence})
                return {"endpoint": endpoint, "acceptance": evidence}
            if any(c["state"] in ("exited", "dead") for s in report["nodes"].values() for c in s["containers"]):
                raise RuntimeError("worker exited during startup; inspect owned container logs")
            if time.monotonic() >= deadline:
                raise RuntimeError("startup deadline exceeded")
            emit({"owner": p["owner"], "phase": "waiting-for-health"})
            time.sleep(min(10, max(0.1, deadline-time.monotonic())))
    except BaseException as failure:
        errors = []
        for node in reversed(cleanup):
            try:
                # A lost mutation response requires a fresh ownership observation.
                if isinstance(failure, AmbiguousMutationError) or recovery is not None:
                    current = invoke(node, "status", cleanup_call=True)["reservation"]
                    if current is None:
                        continue
                    if current.get("owner") != p["owner"] or current.get("digest") != p["digest"]:
                        raise RuntimeError("cleanup ownership reconciliation failed")
                invoke(node, "stop", cleanup_call=True)
            except Exception as exc:
                errors.append(str(exc))
        (output / "endpoint.json").unlink(missing_ok=True)
        if errors:
            emit({"cleanup_incomplete": errors, "recover_with": f"sparkctl down --saved-plan {plan_path}"})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("validate", "render", "doctor", "observe", "preflight", "up", "status", "down", "probe", "collectives", "cache-status", "cache-clear"))
    parser.add_argument("--inventory", type=Path, default=ROOT / "cluster/inventory.json")
    parser.add_argument("--deployment", type=Path)
    parser.add_argument("--saved-plan", type=Path, help="exact immutable rendered inputs; never re-rendered")
    parser.add_argument("--plan-sha256", help="trusted canonical full saved-plan SHA-256; required for saved-plan up")
    parser.add_argument("--node", help="limit doctor/observe to one inventory node")
    parser.add_argument("--output", type=Path, help="evidence directory; observe uses an exclusive private JSON file")
    parser.add_argument("--timeout", type=float, help="overall observe deadline (default 15) or startup deadline (default 600)")
    args = parser.parse_args(argv)
    if args.action == "observe":
        if args.deployment:
            raise ConfigError("observe accepts inventory or saved plan, not a rendered deployment")
        p = read(args.saved_plan) if args.saved_plan else None
        if p:
            validate_saved_plan(p, args.plan_sha256)
            nodes = p["nodes"]
        else:
            inv = read(args.inventory)
            validate_inventory(inv)
            nodes = inv["nodes"]
        if args.node:
            if args.node not in nodes: raise ConfigError("unknown observation node")
            nodes = {args.node: nodes[args.node]}
        result = observe(nodes, p=p, timeout=args.timeout if args.timeout is not None else 15)
        if args.output:
            save_exclusive(args.output, result)
        emit(result)
        return int(not result["complete"])
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
        if args.deployment or args.action not in ("up", "preflight", "status", "down", "probe", "cache-status", "cache-clear"):
            raise ConfigError("saved plans are for activation/inspection/cleanup, without --deployment")
        p = read(args.saved_plan)
        validate_saved_plan(p, args.plan_sha256)
        if args.action == "up" and not args.plan_sha256:
            raise ConfigError("saved-plan activation requires --plan-sha256 from trusted admission")
    else:
        if args.plan_sha256: raise ConfigError("--plan-sha256 requires --saved-plan")
        if not args.deployment: raise ConfigError("--deployment is required")
        p = plan(*load(ROOT, args.inventory, args.deployment))
    output = args.output or (args.saved_plan.parent if args.saved_plan and args.action == "up" else ROOT / "data/cluster" / p["owner"])
    if args.action == "validate":
        emit({"valid": True, "owner": p["owner"], "validation": p["recipe"]["validation"]})
    elif args.action == "render":
        save_json(output / "plan.json", p)
        for node, compose in p["compose"].items(): save_json(output / f"compose-{node}.json", compose)
        emit({"owner": p["owner"], "output": str(output), "endpoint": p["endpoint"], "plan_sha256": plan_sha256(p)})
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
        timeout = args.timeout if args.timeout is not None else 600
        if not 10 <= timeout <= 7200: raise ConfigError("startup timeout must be 10..7200 seconds")
        up(p, timeout, output, expected_sha256=args.plan_sha256, saved_plan=bool(args.saved_plan))
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
