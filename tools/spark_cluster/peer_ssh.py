"""Host-side peer controller SSH setup. Private keys never leave their node."""
import base64
import fcntl
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import tempfile


def write(path, text):
    fd, temp = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, "w") as f:
            os.fchmod(f.fileno(), 0o600)
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def public(value):
    parts = value.split()
    if len(parts) < 2 or parts[0] != "ssh-ed25519": raise RuntimeError("expected Ed25519 public key")
    base64.b64decode(parts[1], validate=True)
    return " ".join(parts[:2])


def main(req):
    if platform.node() != req["node"]["hostname"]: raise RuntimeError("SSH configuration host mismatch")
    directory = Path.home() / ".ssh"
    directory.mkdir(mode=0o700, exist_ok=True)
    with (directory / "spark-cluster.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        identity = directory / "id_ed25519_spark_controller"
        if req["action"] == "prepare":
            if not identity.exists():
                subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(identity),
                                "-C", "spark-cluster-" + platform.node()], check=True, timeout=30)
            pub = subprocess.check_output(["ssh-keygen", "-y", "-f", str(identity)], text=True, timeout=10)
            return {"public_key": public(pub), "host_key": public(Path("/etc/ssh/ssh_host_ed25519_key.pub").read_text())}
        if req["action"] != "configure": raise RuntimeError("unknown SSH operation")
        peer = req["peer"]
        alias = peer["ssh"]
        host_alias = peer["hostname"] + "-cluster"
        known_hosts = directory / "known_hosts_spark_controller"
        record = host_alias + " " + public(req["peer_host_key"])
        existing = known_hosts.read_text().splitlines() if known_hosts.exists() else []
        matching = [line for line in existing if line.startswith(host_alias + " ")]
        if matching and matching != [record]: raise RuntimeError("peer host key differs from the trusted controller key")
        if not matching: write(known_hosts, "\n".join(existing + [record]) + "\n")
        config = directory / "config"
        include = "Include " + str(directory / "config.spark-cluster")
        original = config.read_text() if config.exists() else ""
        # If the alias was already configured elsewhere, preserve it for review.
        resolved = subprocess.check_output(["ssh", "-G", alias], stderr=subprocess.DEVNULL, text=True)
        actual = next(line.split(maxsplit=1)[1] for line in resolved.splitlines() if line.startswith("hostname "))
        if include not in original.splitlines() and actual != alias:
            raise RuntimeError("peer alias already configured outside the managed file")
        block = "\n".join(["# Managed by local-llm-stack configure-spark-peer-ssh.py", "Host " + alias,
            "  HostName " + peer["fabric"][0]["ip"], "  User " + Path.home().name,
            "  IdentityFile " + str(identity), "  IdentitiesOnly yes", "  BatchMode yes",
            "  HostKeyAlias " + host_alias, "  UserKnownHostsFile " + str(known_hosts),
            "  StrictHostKeyChecking yes", "  ForwardAgent no", ""])
        write(directory / "config.spark-cluster", block)
        if include not in original.splitlines(): write(config, include + "\n" + original)
        authorized = directory / "authorized_keys"
        auth = authorized.read_text() if authorized.exists() else ""
        pub = public(req["peer_public_key"])
        options = 'from="' + ",".join(r["ip"] for r in peer["fabric"]) + '",restrict,port-forwarding,permitopen="127.0.0.1:4110"'
        entry = options + " " + pub + " spark-cluster-" + peer["hostname"]
        matching = [line for line in auth.splitlines() if pub in line]
        if matching and matching != [entry]: raise RuntimeError("controller public key already has different authorization options")
        if not matching: write(authorized, auth.rstrip("\n") + ("\n" if auth else "") + entry + "\n")
        return {"peer": peer["hostname"], "alias": alias, "gateway_forward_port": 4110,
                "private_key_copied": False}


if __name__ == "__main__":
    try: print(json.dumps({"ok": True, "result": main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
