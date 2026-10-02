#!/usr/bin/env python3
"""Check or explicitly apply incremental, independently pinned peer SSH trust."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import platform
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster.cli import AmbiguousMutationError, NODE_SOURCE, remote as ambient_remote
from spark_cluster.config import fields, read, ssh_targets, validate_inventory, require
from spark_cluster.peer_ssh import address, public, removable, token


def pinned_remote(node, request, source_path, host_key, *, timeout=240):
    """Authenticate the SSH handshake against the independent pin, not its reply."""
    require(type(timeout) in (int, float) and math.isfinite(timeout) and timeout > 0,
            "pinned transport deadline must be positive and finite")
    pin = public(host_key)
    targets = ssh_targets(node)
    deadline = time.monotonic() + timeout
    local = socket.gethostname() == node["hostname"]
    if local:
        require(platform.node() == node["hostname"] and platform.machine() == node["architecture"],
                "local host identity differs from inventory")
        require(public(Path("/etc/ssh/ssh_host_ed25519_key.pub").read_text()) == pin,
                "local host key differs from independent trust")
        targets = [None]
    source = ("import platform\n"
              f"if (platform.node(), platform.machine()) != {(node['hostname'], node['architecture'])!r}:\n"
              " raise RuntimeError('reached host identity mismatch')\n"
              "exec(" + repr(source_path.read_text()) + ")")
    identity_source = source if source_path.name == "peer_ssh.py" else (
        "import platform\n"
        f"if (platform.node(), platform.machine()) != {(node['hostname'], node['architecture'])!r}:\n"
        " raise RuntimeError('reached host identity mismatch')\n"
        "exec(" + repr((ROOT / "tools/spark_cluster/peer_ssh.py").read_text()) + ")")
    host_alias = "spark-enrollment-" + token(node["hostname"])
    readonly = request.get("action") in ("identity", "observe")
    with tempfile.TemporaryDirectory(prefix="spark-peer-trust-") as directory:
        known_hosts = Path(directory) / "known_hosts"
        fd = os.open(known_hosts, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(host_alias + " " + pin + "\n")
        options = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                   "-o", "HostKeyAlias=" + host_alias, "-o", "UserKnownHostsFile=" + str(known_hosts),
                   "-o", "GlobalKnownHostsFile=none", "-o", "KnownHostsCommand=none",
                   "-o", "HostKeyAlgorithms=ssh-ed25519", "-o", "VerifyHostKeyDNS=no",
                   "-o", "UpdateHostKeys=no", "-o", "CheckHostIP=no",
                   "-o", "ControlMaster=no", "-o", "ControlPath=none", "-o", "ControlPersist=no",
                   "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no",
                   "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=1"]

        def execute(target, payload, program, budget):
            command = ["python3", "-c", program]
            if target is not None:
                command = [*options, target, shlex.join(command)]
            result = subprocess.run(command, input=json.dumps(payload), capture_output=True, text=True, timeout=budget)
            # A forged JSON reply is not evidence that SSH authenticated the pin.
            if result.returncode == 255:
                raise ConnectionError("pinned SSH authentication or transport failed")
            try:
                response = json.loads(result.stdout)
            except ValueError:
                raise ConnectionError("pinned transport returned no operation receipt") from None
            if not isinstance(response, dict) or type(response.get("ok")) is not bool:
                raise ConnectionError("invalid pinned operation receipt")
            if response["ok"] is False:
                raise RuntimeError(node["hostname"] + ": " + str(response.get("error", "operation refused")))
            if result.returncode or "result" not in response:
                raise ConnectionError("pinned operation ended without a successful receipt")
            return response["result"]

        for index, target in enumerate(targets):
            budget = (deadline - time.monotonic()) / (len(targets) - index)
            if budget <= 0:
                break
            if not readonly:
                # Select a pin-authenticated transport read-only, then dispatch once.
                try:
                    execute(target, {"action": "identity", "node": node}, identity_source, budget)
                except (ConnectionError, OSError, subprocess.TimeoutExpired):
                    continue
                budget = deadline - time.monotonic()
                if budget <= 0:
                    break
            try:
                return execute(target, request, source, budget)
            except (ConnectionError, OSError, subprocess.TimeoutExpired) as exc:
                if not readonly:
                    raise AmbiguousMutationError(
                        node["hostname"] + ": pinned mutation receipt lost; reconcile before retry") from exc
        raise ConnectionError(node["hostname"] + ": no independently pinned transport available")


def trust_records(value, inventory):
    fields(value, ("version", "nodes"))
    require(value["version"] == 1, "unsupported peer trust version")
    require(isinstance(value["nodes"], dict) and set(value["nodes"]) == set(inventory["nodes"]),
            "trust must independently pin every inventory node")
    for key, record in value["nodes"].items():
        fields(record, ("host_key",), ("management", "source_addresses"))
        public(record["host_key"])
        require(isinstance(record.get("source_addresses", []), list), "source addresses must be a list")
        approved = {address(v) for v in record.get("source_addresses", [])}
        require(isinstance(record.get("management", []), list), "management entries must be a list")
        aliases = {inventory["nodes"][key]["ssh"]}
        for item in record.get("management", []):
            fields(item, ("alias", "address"))
            require(token(item["alias"]) not in aliases, "duplicate peer alias")
            aliases.add(item["alias"])
            require(address(item["address"]) in approved, "management address needs explicit source approval")
    return value["nodes"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "cluster/inventory.json")
    parser.add_argument("--trust", type=Path, help="independently approved version1 host keys and stable management addresses")
    parser.add_argument("--node", action="append", help="add/check only edges involving these node IDs; default all")
    parser.add_argument("--remove", metavar="NODE", help="revoke an idle node on every remaining peer")
    parser.add_argument("--removal-id", help="resume a retained removal lease using its receipt ID")
    parser.add_argument("--apply", action="store_true", help="allow key generation and SSH file changes")
    args = parser.parse_args(argv)
    inv = read(args.inventory)
    validate_inventory(inv)
    nodes = inv["nodes"]
    require(len(nodes) >= 2, "peer bootstrap requires at least two nodes")
    selected = set(args.node or nodes)
    require(selected <= nodes.keys(), "unknown selected node")
    require(not args.remove or args.remove in nodes, "unknown removal node")
    require(not (args.remove and args.node), "remove and node selection are mutually exclusive")
    require(not args.removal_id or (args.remove and args.apply), "removal-id requires remove and apply")
    require(args.removal_id is None or re.fullmatch(r"[a-f0-9]{32}", args.removal_id),
            "removal-id must be the 32-character receipt ID")
    require(not args.apply or args.trust is not None, "apply requires independently approved --trust; discovery is not trust")
    trusted = trust_records(read(args.trust), inv) if args.trust else None
    helper = ROOT / "tools/spark_cluster/peer_ssh.py"
    def call(key, request, source=helper, *, timeout=240):
        if trusted is not None:
            return pinned_remote(nodes[key], request, source, trusted[key]["host_key"], timeout=timeout)
        # Ambient trust is discovery-only and can never authorize a mutation.
        require(request["action"] in ("identity", "observe"), "mutation requires independent trust")
        return ambient_remote(nodes[key], request, source, timeout=timeout)

    # Check ALL independent host pins before generating even one controller key.
    prepared = {key: call(key, {"action": "identity", "node": node}) for key, node in nodes.items()}
    if trusted:
        for key in nodes:
            require(public(prepared[key]["host_key"]) == public(trusted[key]["host_key"]),
                    "observed host key differs from independent trust: " + key)
    removal = None
    if args.remove and not args.apply:
        removal = call(args.remove, {"action": "observe", "node": nodes[args.remove]}, NODE_SOURCE, timeout=15)
        removable(removal, nodes[args.remove])
    if not args.apply:
        print(json.dumps({"apply": False, "nodes": prepared, "trust_verified": trusted is not None,
                          "remove": args.remove, "requires_approval": trusted is None,
                          "private_key_copied": False}, sort_keys=True))
        return 0
    if not args.remove:
        for key, node in nodes.items():
            if prepared[key]["public_key"] is None or prepared[key].get("transaction_pending"):
                prepared[key] = call(key, {"action": "prepare", "node": node, "apply": True})
                require(public(prepared[key]["host_key"]) == public(trusted[key]["host_key"]), "host key changed")
    results = []
    guard = None
    guard_request = None
    removal_id = None
    key = peer_id = None
    try:
        if args.remove:
            require(prepared[args.remove]["public_key"] is not None,
                    "missing controller public key; nothing can be safely revoked")
            removal_id = args.removal_id or uuid.uuid4().hex
            # Bind an explicit retry to the same complete revocation scope and keys.
            scope = {"inventory": inv, "trust": trusted, "node": args.remove,
                     "public_key": public(prepared[args.remove]["public_key"])}
            guard = {"owner": "peer-removal-" + removal_id,
                     "digest": hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
            guard_request = {"node": nodes[args.remove], **guard}
            # Publish ownership before the first mutation, including a lost acquire reply.
            print(json.dumps({"remove": args.remove, "removal_id": removal_id,
                              "removal_guard": guard, "requires_reconciliation": True}, sort_keys=True),
                  file=sys.stderr, flush=True)
            call(args.remove, {**guard_request, "action": "reserve-workload"}, NODE_SOURCE)
        for key, node in nodes.items():
            for peer_id, peer in nodes.items():
                if key == peer_id:
                    continue
                if args.remove:
                    if peer_id != args.remove:
                        continue
                    # The durable workload lease excludes manual and recovery admission.
                    # Re-observation also refuses native/unmanaged activity and unknown state.
                    removal = call(peer_id, {"action": "observe", "node": peer}, NODE_SOURCE, timeout=15)
                    removable(removal, peer, guard)
                elif key not in selected and peer_id not in selected:
                    continue
                require(prepared[peer_id]["public_key"] is not None, "missing controller public key; nothing can be safely revoked")
                request = {"action": "remove" if args.remove else "configure", "apply": True,
                           "node": node, "peer": peer, "peer_public_key": prepared[peer_id]["public_key"],
                           "peer_host_key": prepared[peer_id]["host_key"],
                           "trusted_host_key": trusted[peer_id]["host_key"],
                           "management": trusted[peer_id].get("management", []),
                           "source_addresses": trusted[peer_id].get("source_addresses", []),
                           "removal": removal, "removal_guard": guard}
                results.append({"node": key, **call(key, request)})
        if guard is not None:
            call(args.remove, {**guard_request, "action": "release-workload"}, NODE_SOURCE)
    except Exception as exc:
        # Never release on failure: even a lost reply may have revoked trust.
        print(json.dumps({"ok": False, "apply": True, "results": results, "failed_node": key,
                          "failed_peer": peer_id, "error": type(exc).__name__,
                          "remove": args.remove, "removal_id": removal_id, "removal_guard": guard,
                          "guard_release_confirmed": False,
                          "requires_reconciliation": True}, sort_keys=True))
        return 1
    print(json.dumps({"apply": True, "results": results, "recovery_enrolled": False,
                      "remove": args.remove, "removal_id": removal_id, "removal_guard": guard,
                      "guard_release_confirmed": guard is not None}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        sys.exit(1)
