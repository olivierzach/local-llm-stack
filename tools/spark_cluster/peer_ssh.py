"""Host-side incremental SSH trust. Only public keys cross the transport."""
import base64
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile


FILES = ("config", "config.spark-cluster", "known_hosts_spark_controller", "authorized_keys")


def write(path, text):
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            os.fchmod(f.fileno(), 0o600)
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def public(value):
    parts = value.split()
    if len(parts) < 2 or parts[0] != "ssh-ed25519":
        raise RuntimeError("expected Ed25519 public key")
    data = base64.b64decode(parts[1], validate=True)
    # SSH wire encoding: string algorithm + string 32-byte public key.
    if len(data) != 51 or data[:19] != b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20":
        raise RuntimeError("invalid Ed25519 public key encoding")
    return " ".join(parts[:2])


def token(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,253}", value):
        raise RuntimeError("invalid SSH name")
    return value


def address(value):
    parsed = ipaddress.ip_address(value)
    if parsed.is_unspecified or parsed.is_multicast or parsed.is_loopback:
        raise RuntimeError("source/management address must identify a reachable host")
    return str(parsed)


def read_files(directory):
    result = {}
    for name in FILES:
        path = directory / name
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise RuntimeError("refusing nonregular SSH file: " + name)
        result[name] = path.read_text() if path.exists() else None
    return result


def snapshot_hashes(values):
    return {name: hashlib.sha256(value.encode()).hexdigest() if value is not None else None
            for name, value in values.items()}


def restore(directory, before, current):
    for name in FILES:
        if before[name] == current[name]:
            continue
        if before[name] is None:
            (directory / name).unlink(missing_ok=True)
        else:
            write(directory / name, before[name])
    sync_directory(directory)


def recover(directory):
    journal = directory / "spark-cluster-transaction.json"
    if journal.is_symlink() or (journal.exists() and not journal.is_file()):
        raise RuntimeError("unsafe SSH transaction journal")
    if journal.exists():
        transaction = json.loads(journal.read_text())
        if (not isinstance(transaction, dict) or set(transaction) != {
                "version", "before", "after", "before_sha256", "after_sha256"}
                or type(transaction["version"]) is not int or transaction["version"] != 1):
            raise RuntimeError("invalid SSH transaction journal; manual recovery required")
        for phase in ("before", "after"):
            values = transaction[phase]
            if (not isinstance(values, dict) or set(values) != set(FILES)
                    or not all(value is None or isinstance(value, str) for value in values.values())
                    or snapshot_hashes(values) != transaction[phase + "_sha256"]):
                raise RuntimeError("invalid SSH transaction snapshot; manual recovery required")
        current = read_files(directory)
        if any(current[name] not in (transaction["before"][name], transaction["after"][name]) for name in FILES):
            raise RuntimeError("SSH transaction conflicts with an unknown file; preserved")
        # Validate every file before restoring any. A partial rollback remains a
        # known before/after mixture, so another interruption is safely retryable.
        restore(directory, transaction["before"], current)
        journal.unlink()
        sync_directory(directory)


def commit(directory, before, after):
    changed = [name for name in FILES if before[name] != after[name]]
    if not changed:
        return
    journal = directory / "spark-cluster-transaction.json"
    if journal.exists() or journal.is_symlink():
        raise RuntimeError("pending SSH transaction must be recovered first")
    if read_files(directory) != before:
        raise RuntimeError("SSH files changed before commit; preserved")
    write(journal, json.dumps({"version": 1, "before": before, "after": after,
                              "before_sha256": snapshot_hashes(before), "after_sha256": snapshot_hashes(after)}))
    try:
        for name in changed:
            if after[name] is None:
                (directory / name).unlink(missing_ok=True)
                sync_directory(directory)
            else:
                write(directory / name, after[name])
    except BaseException:
        # Immediate rollback uses the same all-file conflict check as crash repair.
        # Unknown operator edits or another I/O failure retain the journal.
        recover(directory)
        raise
    journal.unlink()
    sync_directory(directory)


def peer_block(text, hostname):
    start, end = "# BEGIN spark-peer " + hostname, "# END spark-peer " + hostname
    lines = text.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == start]
    ends = [i for i, line in enumerate(lines) if line.rstrip("\r\n") == end]
    if len(starts) != len(ends) or len(starts) > 1 or (starts and ends[0] <= starts[0]):
        raise RuntimeError("malformed managed peer block")
    if not starts:
        return text, ""
    a, b = starts[0], ends[0] + 1
    return "".join(lines[:a] + lines[b:]), "".join(lines[a:b])


def removable(report, peer):
    if not isinstance(report, dict) or any(report.get(k) != peer[k] for k in ("hostname", "architecture")):
        raise RuntimeError("removal requires verified peer identity")
    required = ("reservation", "recovery_fence", "containers", "gpu_containers", "gpu_processes")
    if any(k not in report or k in report.get("errors", {}) for k in required):
        raise RuntimeError("unknown ownership/fence prevents removal")
    if report["reservation"] is not None or any(report[k] is None for k in required[2:]):
        raise RuntimeError("reserved or unknown peer prevents removal")
    gate = report["recovery_fence"]
    if gate is not None and (not isinstance(gate, dict) or gate.get("active") is not False):
        raise RuntimeError("active or unresolved recovery fence prevents removal")
    if report["gpu_containers"] or report["gpu_processes"] or any(
            c.get("state") not in ("exited", "dead") for c in report["containers"]):
        raise RuntimeError("active workloads prevent removal")


def changes(directory, req, before, resolve=None):
    """Validate every trust conflict before computing one transaction."""
    peer = req["peer"]
    hostname, alias = token(peer["hostname"]), token(peer["ssh"])
    host_alias = hostname + "-cluster"
    host_key, pub = public(req["peer_host_key"]), public(req["peer_public_key"])
    if public(req["trusted_host_key"]) != host_key:
        raise RuntimeError("peer host key differs from independently trusted key")
    management = req.get("management", [])
    sources = sorted({address(r["ip"]) for r in peer["fabric"]} |
                     {address(v) for v in req.get("source_addresses", [])})
    aliases = [(alias, address(peer["fabric"][0]["ip"]))]
    for entry in management:
        if set(entry) != {"alias", "address"}:
            raise RuntimeError("management entry requires alias and approved address")
        target = address(entry["address"])
        if target not in sources:
            raise RuntimeError("management address requires approved source restriction")
        aliases.append((token(entry["alias"]), target))
    if len({a for a, _ in aliases}) != len(aliases):
        raise RuntimeError("duplicate managed alias")
    original = before["config"] or ""
    managed = before["config.spark-cluster"] or ""
    remainder, old_block = peer_block(managed, hostname)
    # One-time exact migration of the former pair-only file, never an arbitrary Host block.
    legacy_header = "# Managed by local-llm-stack configure-spark-peer-ssh.py\n"
    if managed.startswith(legacy_header) and "# BEGIN spark-peer " not in managed:
        hosts = re.findall(r"(?m)^Host (\S+)$", managed)
        hostkeys = re.findall(r"(?m)^  HostKeyAlias (\S+)$", managed)
        if len(hosts) != 1 or len(hostkeys) != 1 or not hostkeys[0].endswith("-cluster"):
            raise RuntimeError("unrecognized legacy SSH file")
        old_host = token(hostkeys[0][:-8])
        managed = "# BEGIN spark-peer " + old_host + "\n" + managed + "# END spark-peer " + old_host + "\n"
        remainder, old_block = peer_block(managed, hostname)
    for name, _ in aliases:
        if any(name in line.split()[1:] for line in remainder.splitlines()
               if line.split() and line.split()[0].lower() == "host"):
            raise RuntimeError("alias belongs to another managed peer")
        owned = re.search(r"(?m)^Host " + re.escape(name) + r"$", old_block)
        if not owned and resolve is not None and resolve(name) != name:
            raise RuntimeError("peer alias already configured outside its managed block")
    known = before["known_hosts_spark_controller"] or ""
    record = host_alias + " " + host_key
    matches = [line for line in known.splitlines() if host_alias in line.split(" ", 1)[0].split(",")]
    if matches and matches != [record]:
        raise RuntimeError("peer host key differs from the trusted controller key")
    authorized = before["authorized_keys"] or ""
    options = 'from="' + ",".join(sources) + '",restrict,port-forwarding,permitopen="127.0.0.1:4110"'
    marker = "spark-cluster-" + hostname
    entry = options + " " + pub + " " + marker
    auth_matches = [line for line in authorized.splitlines() if pub in line or line.endswith(" " + marker)]
    if auth_matches and (len(auth_matches) != 1 or not auth_matches[0].endswith(" " + pub + " " + marker)):
        raise RuntimeError("controller public key already has different ownership")
    if req["action"] == "remove":
        removable(req.get("removal"), peer)
        block = ""
        if matches:
            known = "".join(line for line in known.splitlines(keepends=True) if line.rstrip("\r\n") != record)
        new_auth = ""
    else:
        lines = ["# BEGIN spark-peer " + hostname]
        for name, target in aliases:
            lines.extend(["Host " + name, "  HostName " + target, "  User " + token(Path.home().name),
                          "  IdentityFile " + str(directory / "id_ed25519_spark_controller"),
                          "  IdentitiesOnly yes", "  BatchMode yes", "  HostKeyAlias " + host_alias,
                          "  UserKnownHostsFile " + str(directory / "known_hosts_spark_controller"),
                          "  StrictHostKeyChecking yes", "  ForwardAgent no"])
        lines.append("# END spark-peer " + hostname)
        block = "\n".join(lines) + "\n"
        if not matches:
            known += ("\n" if known and not known.endswith("\n") else "") + record + "\n"
        new_auth = entry + "\n"
    if auth_matches:
        authorized = "".join(new_auth if line.rstrip("\r\n") == auth_matches[0] else line
                             for line in authorized.splitlines(keepends=True))
    elif new_auth:
        authorized += ("\n" if authorized and not authorized.endswith("\n") else "") + new_auth
    if old_block:
        managed = managed.replace(old_block, block, 1)
    else:
        managed = remainder + ("\n" if remainder and not remainder.endswith("\n") and block else "") + block
    include = "Include " + str(directory / "config.spark-cluster")
    if req["action"] != "remove" and include not in original.splitlines():
        original = include + "\n" + original
    after = {"config": original, "config.spark-cluster": managed,
             "known_hosts_spark_controller": known, "authorized_keys": authorized}
    return {name: None if before[name] is None and not text else text for name, text in after.items()}


def main(req):
    if platform.node() != req["node"]["hostname"]:
        raise RuntimeError("SSH configuration host mismatch")
    directory = Path.home() / ".ssh"
    if directory.is_symlink():
        raise RuntimeError("refusing symlinked SSH directory")
    identity = directory / "id_ed25519_spark_controller"
    if req["action"] == "identity":
        if identity.is_symlink():
            raise RuntimeError("unsafe controller identity")
        pub = subprocess.check_output(["ssh-keygen", "-y", "-f", str(identity)], text=True, timeout=10) if identity.exists() else None
        return {"public_key": public(pub) if pub else None,
                "host_key": public(Path("/etc/ssh/ssh_host_ed25519_key.pub").read_text()),
                "transaction_pending": (directory / "spark-cluster-transaction.json").exists()}
    if req.get("apply") is not True or req["action"] not in ("prepare", "configure", "remove"):
        raise RuntimeError("SSH mutation requires explicit apply")
    directory.mkdir(mode=0o700, exist_ok=True)
    lockpath = directory / "spark-cluster.lock"
    if lockpath.is_symlink():
        raise RuntimeError("unsafe SSH lock")
    with lockpath.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        recover(directory)
        if req["action"] == "prepare":
            if identity.is_symlink() or identity.with_suffix(".pub").is_symlink():
                raise RuntimeError("unsafe controller identity")
            if not identity.exists():
                subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(identity),
                                "-C", "spark-cluster-" + platform.node()], check=True, timeout=30)
            return main({**req, "action": "identity"})
        before = read_files(directory)
        def resolve(alias):
            resolved = subprocess.check_output(["ssh", "-G", alias], stderr=subprocess.DEVNULL, text=True, timeout=10)
            return next(line.split(maxsplit=1)[1] for line in resolved.splitlines() if line.startswith("hostname "))
        after = changes(directory, req, before, resolve)
        commit(directory, before, after)
        return {"peer": req["peer"]["hostname"], "alias": req["peer"]["ssh"],
                "removed": req["action"] == "remove", "gateway_forward_port": 4110, "private_key_copied": False}


if __name__ == "__main__":
    try:
        print(json.dumps({"ok": True, "result": main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
