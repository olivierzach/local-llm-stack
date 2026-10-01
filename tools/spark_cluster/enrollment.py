"""Explicit node/recipe preparation; discovery and cached artifacts never qualify GPUs.

This file is also the stdlib-only SSH helper. Configuration/controller imports are
local to controller functions so the transported helper needs no installed package.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2] if "__file__" in globals() else None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def topology(inventory, deployment):
    """Reject impossible declared links; unknown topology is explicitly unqualified."""
    from .config import require
    members = set(deployment["nodes"])
    if deployment["mode"] == "single":
        return {"supported": True, "collective_required": False, "qualification_required": False}
    graph = {key: set() for key in members}
    unknown = False
    for key in members:
        for rail in inventory["nodes"][key]["fabric"]:
            peer = rail.get("peer")
            if peer is None:
                unknown = True
                continue
            require(peer in inventory["nodes"], "fabric peer is not inventoried")
            if peer in members:
                graph[key].add(peer)
                reverse = [r for r in inventory["nodes"][peer]["fabric"] if r.get("peer") == key]
                require(reverse, "declared direct fabric link is not reciprocal")
                if rail.get("physical_link"):
                    require(any(r.get("physical_link") == rail["physical_link"] for r in reverse),
                            "physical fabric link identity differs at endpoints")
    reached, pending = set(), [deployment["coordinator"]]
    while pending:
        node = pending.pop()
        if node not in reached:
            reached.add(node)
            pending.extend(graph[node] - reached)
    require(unknown or reached == members, "declared fabric topology disconnects deployment members")
    return {"supported": True, "collective_required": True, "declared_connectivity": not unknown,
            "qualification_required": True,
            "remaining": ["GPU collective qualification", "runtime/checkpoint TP and PP support"]}


def tensor_shape(config, tensor_parallel):
    """Check known checkpoint invariants without assuming arbitrary TP counts work."""
    if tensor_parallel == 1:
        return {"tensor_parallel": 1, "static_constraints_checked": True}
    inner = config.get("text_config", config)
    heads = inner.get("num_attention_heads")
    if type(heads) is not int or heads <= 0:
        raise ValueError("checkpoint attention-head metadata unknown; TP requires qualification")
    if heads % tensor_parallel:
        raise ValueError("tensor parallelism does not divide checkpoint attention heads")
    kv = inner.get("num_key_value_heads", heads)
    if type(kv) is not int or kv <= 0 or (kv % tensor_parallel and tensor_parallel % kv):
        raise ValueError("tensor parallelism cannot partition or replicate checkpoint KV heads")
    return {"tensor_parallel": tensor_parallel, "attention_heads": heads, "kv_heads": kv,
            "static_constraints_checked": True, "gpu_qualification_required": True}


def discovery(inventory, *, observer=None, resolver=None, timeout=15):
    from .cli import observe
    observer = observer or observe
    resolver = resolver or socket.getaddrinfo
    found = {}
    for key, node in inventory["nodes"].items():
        names = list(dict.fromkeys([node["hostname"] + ".local", *node.get("management", {}).get("ssh_targets", [node["ssh"]])]))
        records = []
        for name in names:
            # SSH aliases need not be DNS names. Failed lookup is not failed SSH.
            try:
                addresses = sorted({item[4][0] for item in resolver(name.split("@")[-1], None, socket.AF_INET)})
                records.append({"name": name, "observed_addresses": addresses,
                                "address_status": "needs-explicit-approval-and-stability"})
            except OSError:
                records.append({"name": name, "observed_addresses": [], "address_status": "unresolved-or-ssh-alias"})
        found[key] = records
    observed = observer(inventory["nodes"], timeout=timeout)
    return {"version": 1, "inventory_sha256": digest(inventory), "candidate_inventory": inventory,
            "discovery": found, "observation": observed, "qualified": False, "eligible_automation": False,
            "requires": ["independent host-key trust", "stable management and serving address approval",
                         "selected artifacts", "exact-plan GPU qualification", "explicit recovery policy admission"]}


def package_check(packages):
    results = []
    for package in packages:
        if not re.fullmatch(r"[a-z0-9][a-z0-9.+-]*", package):
            raise ValueError("invalid system package name")
        try:
            result = subprocess.run(["dpkg-query", "-W", "-f=${db:Status-Status}", package],
                                    capture_output=True, text=True, timeout=10)
            installed = result.returncode == 0 and result.stdout.strip() == "installed"
        except FileNotFoundError:
            installed = False
        results.append({"package": package, "installed": installed})
    return {"complete": all(r["installed"] for r in results), "packages": results,
            "installation_performed": False}


def artifact_checks(request, node_module):
    recipe, node = request["recipe"], request["node"]
    checks = []
    def check(label, operation):
        try:
            checks.append({"check": label, "passed": True, "details": operation()})
        except Exception as exc:
            checks.append({"check": label, "passed": False, "error": str(exc)})
    def image():
        value = json.loads(node_module["run"](["docker", "image", "inspect", recipe["image"]]))[0]
        if value["Architecture"] != {"aarch64": "arm64", "x86_64": "amd64"}[node["architecture"]]:
            raise ValueError("runtime image architecture differs")
        return {"image": recipe["image"], "image_id": value["Id"]}
    snapshot = Path(node["cache"]) / "hub" / ("models--" + recipe["model"].replace("/", "--")) / "snapshots" / recipe["revision"]
    def cached():
        node_module["validate_cached_snapshot"](snapshot)
        return {"snapshot": str(snapshot), "attestation": "structural-only; staging receipt retains SHA provenance"}
    check("runtime-image", image)
    check("model-cache", cached)
    check("tensor-shape", lambda: tensor_shape(json.loads((snapshot / "config.json").read_text()), request["deployment"]["tensor_parallel"]))
    if recipe.get("deepseek_v4", {}).get("source_overlays") or recipe.get("glm53", {}).get("source_overlays"):
        check("source-overlays", lambda: node_module["verify_source_overlays"](request))
    if recipe.get("nccl_library"):
        check("nccl-library", lambda: node_module["verify_nccl_library"](request))
    if recipe.get("speculative_config", {}).get("method") == "dflash":
        check("draft-cache", lambda: node_module["verify_draft_cache"](request))
    return checks


def fetch_manifest(source, manifest, recipe, max_bytes):
    module = {"__name__": "spark_pinned_fetch"}
    exec(compile(source, "fetch-pinned-spark-model.py", "exec"), module)
    total = module["validate"](manifest)
    if (manifest["repo"], manifest["revision"], manifest["image"]) != (recipe["model"], recipe["revision"], recipe["image"]):
        raise ValueError("selected model manifest differs from recipe pins")
    if type(max_bytes) is not int or max_bytes < total or max_bytes < 1:
        raise ValueError("selected manifest exceeds explicit download byte budget")
    return module


def stage(request):
    """Only an explicit selected digest and selected model manifest can mutate cache."""
    if request.get("apply") is not True:
        raise ValueError("artifact staging requires explicit apply")
    recipe = request["recipe"]
    if not re.fullmatch(r"[a-zA-Z0-9./_-]+@sha256:[0-9a-f]{64}", recipe["image"]):
        raise ValueError("staging image must be pinned")
    manifest = request.get("model_manifest")
    if manifest is not None:
        fetch_manifest(request["fetch_source"], manifest, recipe, request["max_download_bytes"])
    receipts = {}
    if request.get("pull_image"):
        subprocess.run(["docker", "pull", recipe["image"]], check=True, capture_output=True, text=True, timeout=1800)
        receipts["image"] = recipe["image"]
    if manifest is not None:
        cache = Path(request["node"]["cache"])
        if not cache.is_absolute() or ":" in str(cache) or cache.is_symlink():
            raise ValueError("unsafe selected model cache")
        cache.mkdir(parents=True, exist_ok=True)
        # Reuse the existing SHA-verifying fetcher in its exact runtime image, with
        # no GPU/device/host-network access and no package installation on the host.
        program = ("import json,sys\nr=json.load(sys.stdin)\n"
                   "m={'__name__':'spark_fetch'}\nexec(compile(r['source'],'fetch-pinned-spark-model.py','exec'),m)\n"
                   "lock=m['fetch'](m['Path'](r['cache']),r['manifest'],r['budget'],2)\n"
                   "print(json.dumps({'staging_lock':lock}))\n")
        result = subprocess.run(["docker", "run", "--rm", "--user", str(os.getuid()) + ":" + str(os.getgid()),
                                 "--security-opt", "no-new-privileges", "--cap-drop", "ALL", "--pull", "never",
                                 "-v", str(cache) + ":" + str(cache), "-i", "--entrypoint", "python3", recipe["image"], "-c", program],
                                input=json.dumps({"source": request["fetch_source"], "cache": str(cache), "manifest": manifest,
                                                  "budget": request["max_download_bytes"]}),
                                text=True, capture_output=True, check=True, timeout=7200)
        lock = json.loads(result.stdout.splitlines()[-1])["staging_lock"]
        receipts["model"] = {"manifest_sha256": digest(manifest), "lock_sha256": digest(lock),
                             "repo": manifest["repo"], "revision": manifest["revision"], "lock": lock}
    return receipts


def host(request):
    # The transport verifies hostname/architecture before executing this module.
    module = {"__name__": "spark_enrollment_node"}
    exec(compile(request["node_source"], "node.py", "exec"), module)
    module["verify_host"](request["node"])
    receipts = stage(request) if request["action"] == "prepare" else {}
    observation = module["observe"]({"node": request["node"], "timeout": request.get("timeout", 15)})
    packages = package_check(request["packages"])
    checks = artifact_checks(request, module) if "recipe" in request else []
    return {"observation": observation, "packages": packages, "checks": checks, "staging_receipts": receipts,
            "artifacts_prepared": bool(checks) and all(c["passed"] for c in checks),
            "prepared": bool(checks) and packages["complete"] and all(c["passed"] for c in checks),
            "qualified": False, "eligible_automation": False}


def inspect_candidates(inventory, plan=None, *, selected=None, timeout=30, remote_fn=None,
                       apply=False, pull_image=False, model_manifest=None, max_download_bytes=None):
    from . import cli
    remote_fn = remote_fn or cli.remote
    nodes = selected or list(plan["nodes"] if plan else inventory["nodes"])
    packages = [line.strip() for line in (ROOT / "cluster/system-packages.txt").read_text().splitlines()
                if line.strip() and not line.lstrip().startswith("#")]
    node_source = cli.NODE_SOURCE.read_text()
    fetch_source = None
    if model_manifest is not None:
        if plan is None:
            raise ValueError("selected model staging requires a deployment")
        fetch_source = (ROOT / "scripts/fetch-pinned-spark-model.py").read_text()
        fetch_manifest(fetch_source, model_manifest, plan["recipe"], max_download_bytes)
    requests = []
    for key in nodes:
        req = cli.request(plan, key, "prepare" if apply else "observe") if plan else {"node": inventory["nodes"][key], "action": "observe"}
        req.update({"node_source": node_source, "packages": packages, "timeout": min(timeout, 240)})
        if apply:
            req.update({"apply": True, "pull_image": pull_image, "model_manifest": model_manifest,
                        "max_download_bytes": max_download_bytes})
            if fetch_source is not None:
                req["fetch_source"] = fetch_source
        requests.append((key, req))
    results = {}
    for key, req in requests:
        try:
            results[key] = remote_fn(inventory["nodes"][key], req, Path(__file__), timeout=timeout)
        except Exception as exc:
            # Never replay an ambiguous mutation. All prior receipts remain visible.
            results[key] = {"prepared": False, "qualified": False, "error": type(exc).__name__,
                            "requires_reconciliation": apply}
            if apply:
                break
    return {"version": 1, "inventory_sha256": digest(inventory),
            "plan_sha256": cli.plan_sha256(plan) if plan else None, "nodes": results,
            "prepared": len(results) == len(nodes) and all(r.get("prepared") for r in results.values()),
            "qualified": False, "eligible_automation": False,
            "remaining": ["exact-plan text/tools/continuation/SSE GPU qualification", "explicit recovery policy admission"]}


def merge_candidate(base, candidate):
    from .config import require, validate_inventory
    validate_inventory(base)
    validate_inventory(candidate)
    for key in base["nodes"].keys() & candidate["nodes"].keys():
        require(base["nodes"][key] == candidate["nodes"][key],
                "enrollment cannot edit an existing node; create an explicitly reviewed new inventory separately")
    result = {"version": 1, "nodes": {**base["nodes"], **candidate["nodes"]}}
    validate_inventory(result)
    return result


def publish_snapshot(path, value):
    """Retry identical admission without overwriting an existing immutable object."""
    from .cli import save_exclusive
    from .config import read, require
    require(not path.is_symlink(), "snapshot must not be a symlink")
    if path.exists():
        require(read(path) == value, "existing immutable snapshot differs")
        return
    try:
        save_exclusive(path, value)
    except FileExistsError:
        require(not path.is_symlink() and read(path) == value, "concurrent immutable snapshot differs")


def approve(inventory, expected, addresses):
    from .config import require
    require(expected == digest(inventory), "explicit inventory SHA-256 approval is required")
    stable = {str(ipaddress.ip_address(value)) for value in addresses}
    required = {rail["ip"] for node in inventory["nodes"].values() for rail in node["fabric"]}
    required.update(node["serving"]["address"] for node in inventory["nodes"].values() if "serving" in node)
    require(required <= stable, "all candidate fabric/serving addresses require explicit stability approval")


def remove_candidate(inventory, node_id, *, observer=None, timeout=15):
    from .cli import observe
    from .config import require, validate_inventory
    from .peer_ssh import removable
    require(node_id in inventory["nodes"], "unknown removal node")
    report = (observer or observe)({node_id: inventory["nodes"][node_id]}, timeout=timeout)["nodes"][node_id]
    removable(report, inventory["nodes"][node_id])
    result = {"version": 1, "nodes": {k: v for k, v in inventory["nodes"].items() if k != node_id}}
    validate_inventory(result)
    return result, report


def main(argv=None):
    from .cli import emit, save_exclusive
    from .config import load, plan as render, read, require, validate_inventory
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("discover", "check", "prepare", "apply", "remove"))
    parser.add_argument("--inventory", type=Path, required=True, help="explicit candidate/current version1 inventory")
    parser.add_argument("--node", action="append", help="selected candidate node ID; repeatable")
    parser.add_argument("--root", type=Path, default=ROOT, help="recipe repository root")
    parser.add_argument("--deployment", type=Path, help="new deployment manifest; never rewrites a saved plan")
    parser.add_argument("--base-inventory", type=Path, help="existing inventory to preserve during admission")
    parser.add_argument("--output", type=Path, help="exclusive NEW inventory output for apply/remove")
    parser.add_argument("--plan-output", type=Path, help="exclusive NEW saved plan during apply")
    parser.add_argument("--evidence-output", type=Path, help="exclusive evidence receipt; requires --apply")
    parser.add_argument("--approve-inventory-sha256")
    parser.add_argument("--approve-stable-address", action="append", default=[])
    parser.add_argument("--pull-image", action="store_true", help="prepare only the selected immutable runtime image")
    parser.add_argument("--model-manifest", type=Path, help="selected fetch-pinned-spark-model manifest")
    parser.add_argument("--max-download-bytes", type=int)
    parser.add_argument("--timeout", type=int, default=30, help="per-node deadline; explicitly increase for staging")
    parser.add_argument("--apply", action="store_true", help="authorize selected cache writes or new inventory publication")
    args = parser.parse_args(argv)
    require(1 <= args.timeout <= 10800, "timeout must be 1..10800 seconds")
    require(not args.apply or args.action in ("prepare", "apply", "remove"), "discover/check are always read-only")
    require(not args.evidence_output or args.apply, "evidence file writing requires --apply")
    require(not args.plan_output or (args.action == "apply" and args.deployment), "plan output requires apply and deployment")
    require(not (args.pull_image or args.model_manifest) or args.action == "prepare", "staging options require prepare")
    inventory = read(args.inventory)
    validate_inventory(inventory)
    selected = args.node
    require(not selected or (len(set(selected)) == len(selected) and set(selected) <= inventory["nodes"].keys()), "invalid selected nodes")
    p, topo = None, None
    if args.deployment:
        inv, recipe, deployment = load(args.root, args.inventory, args.deployment)
        topo = topology(inv, deployment)
        p = render(inv, recipe, deployment)
        require(not selected or set(selected) <= p["nodes"].keys(), "selected node not in deployment")
    if args.action == "discover":
        target = {"version": 1, "nodes": {k: v for k, v in inventory["nodes"].items() if not selected or k in selected}}
        result = discovery(target, timeout=min(args.timeout, 240))
    elif args.action in ("check", "prepare"):
        require(args.action != "prepare" or p is not None, "prepare requires a selected deployment")
        require(not args.apply or args.pull_image or args.model_manifest, "prepare --apply requires selected artifact operations")
        result = inspect_candidates(inventory, p, selected=selected, timeout=args.timeout,
                                    apply=args.apply, pull_image=args.pull_image,
                                    model_manifest=read(args.model_manifest) if args.model_manifest else None,
                                    max_download_bytes=args.max_download_bytes)
        result["topology"] = topo
        result["apply"] = args.apply
        if args.action == "prepare" and not args.apply:
            result["planned_staging"] = {"image": p["recipe"]["image"] if args.pull_image else None,
                                         "model_manifest": str(args.model_manifest) if args.model_manifest else None}
    elif args.action == "apply":
        require(args.base_inventory is not None, "admission requires --base-inventory")
        result_inventory = merge_candidate(read(args.base_inventory), inventory)
        result = {"version": 1, "apply": args.apply, "candidate_inventory": result_inventory,
                  "inventory_sha256": digest(inventory), "qualified": False, "eligible_automation": False,
                  "admission": "inventory-only; this is not a recovery qualification receipt",
                  "recovery_enrolled": False, "topology": topo}
        if args.apply:
            require(args.output is not None, "apply requires a new --output inventory path")
            approve(inventory, args.approve_inventory_sha256, args.approve_stable_address)
            # Check all destinations before either exclusive publication.
            for path, value in [(args.output, result_inventory), (args.plan_output, p)]:
                require(path is None or (not path.is_symlink() and
                        (not path.exists() or read(path) == value)), "existing immutable snapshot differs")
            require(args.evidence_output is None or (not args.evidence_output.exists() and
                    not args.evidence_output.is_symlink()), "evidence output already exists")
            require(len({str(path.absolute()) for path in [args.output, args.plan_output, args.evidence_output] if path}) ==
                    len([path for path in [args.output, args.plan_output, args.evidence_output] if path]), "output paths must differ")
            if args.plan_output:
                publish_snapshot(args.plan_output, p)
                result["plan_sha256"] = digest(p)
            publish_snapshot(args.output, result_inventory)
    else:
        require(selected is not None and len(selected) == 1, "remove requires exactly one --node")
        result_inventory, observation = remove_candidate(inventory, selected[0], timeout=min(args.timeout, 240))
        result = {"version": 1, "apply": args.apply, "candidate_inventory": result_inventory,
                  "inventory_sha256": digest(inventory),
                  "observation": observation, "removed": selected[0], "recovery_enrolled": False,
                  "peer_trust_revocation": "use configure-spark-peer-ssh.py --remove NODE --trust TRUST --apply before discarding old inventory"}
        if args.apply:
            require(args.approve_inventory_sha256 == digest(inventory), "removal requires current inventory SHA-256 approval")
            require(args.output is not None, "remove requires new --output inventory")
            publish_snapshot(args.output, result_inventory)
    result["observed_at"] = time.time()
    if args.evidence_output:
        save_exclusive(args.evidence_output, result)
    emit(result)
    return 1 if args.action in ("check", "prepare") and not result.get("prepared") else 0


def entrypoint():
    try:
        return main()
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "result": host(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
