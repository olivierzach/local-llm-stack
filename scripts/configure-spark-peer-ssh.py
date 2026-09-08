#!/usr/bin/env python3
"""Provision direct controller SSH and restricted gateway forwarding on a node pair."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster.cli import remote
from spark_cluster.config import read, validate_inventory, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, default=ROOT / "cluster/inventory.json")
    args = parser.parse_args()
    inv = read(args.inventory)
    validate_inventory(inv)
    require(len(inv["nodes"]) == 2, "this bootstrap currently supports a pair of nodes")
    helper = ROOT / "tools/spark_cluster/peer_ssh.py"
    prepared = {key: remote(node, {"action": "prepare", "node": node}, helper) for key, node in inv["nodes"].items()}
    for key, node in inv["nodes"].items():
        peer_id = next(k for k in inv["nodes"] if k != key)
        peer = inv["nodes"][peer_id]
        print(json.dumps(remote(node, {"action": "configure", "node": node, "peer": peer,
            "peer_public_key": prepared[peer_id]["public_key"], "peer_host_key": prepared[peer_id]["host_key"]}, helper)))


if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"configure-spark-peer-ssh: {exc}", file=sys.stderr)
        sys.exit(1)
