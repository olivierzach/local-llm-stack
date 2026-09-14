#!/usr/bin/env python3
"""Run on the source Spark; stream pinned images directly to its SSH fabric peer.

Image identity and tag conflicts are checked before transfer. No containers,
volumes, credentials or model data are changed. SSH configuration is explicit;
the destination host key must already be trusted. No private keys are copied.
"""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import time


def checked(argv):
    return subprocess.check_output(argv, text=True).strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--lock", type=Path, required=True)
    p.add_argument("--peer", required=True, help="SSH alias or user@fabric-IP")
    p.add_argument("--identity", type=Path)
    p.add_argument("--known-hosts", type=Path)
    p.add_argument("--host-key-alias")
    p.add_argument("--fabric-inventory", type=Path, help="require an inventoried direct Spark cable route")
    p.add_argument("--restore-registry-digests", action="store_true",
                   help="after copying layers, resolve registry manifests and verify the pinned image IDs")
    p.add_argument("--apply", action="store_true", help="default is a read-only audit")
    args = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]*", args.peer): p.error("invalid SSH peer")
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15"]
    if args.identity: ssh += ["-i", str(args.identity.expanduser()), "-o", "IdentitiesOnly=yes"]
    if args.known_hosts: ssh += ["-o", "UserKnownHostsFile=" + str(args.known_hosts.expanduser())]
    if args.host_key_alias: ssh += ["-o", "HostKeyAlias=" + args.host_key_alias]
    if args.fabric_inventory:
        if args.identity or args.known_hosts or args.host_key_alias:
            p.error('fabric inventory uses the inventoried SSH alias; do not combine transport overrides')
        from spark_transfer import transport
        ssh, args.peer, fabric = transport(args.peer, json.loads(args.fabric_inventory.read_text()))
        print(json.dumps({'fabric': fabric}), flush=True)
    ssh += [args.peer]
    manifest = json.loads(args.lock.read_text())
    if manifest["version"] != 1: p.error("unknown lock version")
    # Digest-only pulls can be hidden by Docker's default listing. Include all
    # images so a successfully verified immutable pull is not called missing.
    local_ids = set(checked(["docker", "image", "ls", "-aq", "--no-trunc"]).splitlines())
    peer_ids = set(checked(ssh + ["docker image ls -aq --no-trunc"]).splitlines())
    missing = []
    # Preflight every image/tag before any writes; refuse replacing another tag.
    for image in manifest["images"]:
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", image["id"]): p.error("unlocked image")
        if args.restore_registry_digests and not re.fullmatch(r'[A-Za-z0-9./_-]+@sha256:[a-f0-9]{64}', image.get('registry') or ''):
            p.error('registry restoration requires immutable registry pins for every image')
        if image["id"] not in local_ids: raise RuntimeError(f"Source missing {image['tag']}")
        tag = image["tag"]
        if not re.fullmatch(r"[a-zA-Z0-9./_:-]+", tag): p.error("invalid tag")
        check = subprocess.run(ssh + [shlex.join(["docker", "image", "inspect", tag, "--format", "{{.Id}}"] )],
                               capture_output=True, text=True)
        if check.returncode == 0 and check.stdout.strip() != image["id"]:
            raise RuntimeError(f"Destination tag {tag} points to a different image; no tags changed")
        if check.returncode not in (0, 1): raise RuntimeError("destination image inspection failed")
        if image["id"] not in peer_ids: missing.append(image)
    print(json.dumps({"missing": [i["tag"] for i in missing], "apply": args.apply}), flush=True)
    if not args.apply: return
    for image in missing:
        started = time.monotonic()
        # Save by immutable ID, not a mutable tag. SSH ciphertext stays entirely
        # on the Spark-to-Spark link when --peer is its fabric address.
        producer = subprocess.Popen(["docker", "image", "save", image["id"]], stdout=subprocess.PIPE)
        try:
            consumer = subprocess.run(ssh + ["docker image load --quiet"], stdin=producer.stdout,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            producer.stdout.close()
            source_code = producer.wait()
            if source_code or consumer.returncode: raise RuntimeError("image stream failed")
        finally:
            if producer.poll() is None:
                producer.terminate()
                producer.wait()
        actual = checked(ssh + [shlex.join(["docker", "image", "inspect", image["id"], "--format", "{{.Id}}"] )])
        if actual != image["id"]: raise RuntimeError("destination image identity mismatch")
        print(json.dumps({"image": image["tag"], "verified_id": actual,
                          "elapsed_s": round(time.monotonic()-started, 3)}), flush=True)
    for image in manifest["images"]:
        current = subprocess.run(ssh + [shlex.join(["docker", "image", "inspect", image["tag"], "--format", "{{.Id}}"] )],
                                 capture_output=True, text=True)
        if current.returncode == 0:
            if current.stdout.strip() != image["id"]:
                raise RuntimeError("Destination tag changed during transfer; refusing to replace it")
            continue
        if current.returncode != 1: raise RuntimeError("destination image inspection failed")
        checked(ssh + [shlex.join(["docker", "image", "tag", image["id"], image["tag"]])])
    if args.restore_registry_digests:
        for image in manifest['images']:
            # docker-save/load on the classic image store drops RepoDigests.
            # All image layers have already crossed the direct cable above;
            # pulling the pin restores the registry manifest association.
            pull = subprocess.run(ssh + [shlex.join(['docker', 'pull', image['registry']])],
                                  text=True, capture_output=True, check=True)
            print(json.dumps({'registry_restore': image['registry'], 'output': pull.stdout}), flush=True)
            actual = checked(ssh + [shlex.join(['docker', 'image', 'inspect', image['registry'], '--format', '{{.Id}}'])])
            if actual != image['id']:
                raise RuntimeError('restored registry image differs from the copied immutable ID')
    print(json.dumps({"verified_images": len(manifest["images"])}))


if __name__ == "__main__":
    main()
