#!/usr/bin/env python3
"""Refresh CPU gateway policy and its managed proxy dependency, preserving keys and routes.

Run from the managed controller on either Spark. A source hash precondition
protects changes in the user's base checkout; backups permit automatic rollback.
This does not start or stop model workers.
"""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway_node
from spark_cluster.config import read, validate_inventory


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path.home() / 'projects/local-llm-stack')
    p.add_argument('--expected-source-sha256', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    inventory = read(ROOT / 'cluster/inventory.json'); validate_inventory(inventory)
    node_id = socket.gethostname().removeprefix('spark-')
    node = inventory['nodes'][node_id]
    target = args.root.expanduser().resolve() / 'tools/spark_cluster/gateway.py'
    previous = target.read_text()
    if hashlib.sha256(previous.encode()).hexdigest() != args.expected_source_sha256:
        raise RuntimeError('base gateway source changed; inspect it before refreshing')
    saved = read(gateway_node.ROOT / 'state.json')
    managed = gateway_node.checked(saved)
    if not managed or not managed['State']['Running']: raise RuntimeError('managed gateway must be running')
    files = {name: (gateway_node.ROOT / saved['digest'] / name).read_text() for name in (
        'scripts/context-guard-proxy.py', 'tools/spark_cluster/gateway.py',
        'tools/spark_cluster/config.py', 'tools/spark_cluster/__init__.py')}
    registry = read(gateway_node.ROOT / 'config/registry.json')
    ids = gateway_node.run(['docker', 'ps', '-q', '--filter', 'label=com.docker.compose.service=context-guard']).split()
    candidates = [read_container for identity in ids
                  for read_container in json.loads(gateway_node.run(['docker', 'inspect', identity]))
                  if any(m.get('Source') == str(args.root.expanduser().resolve() / 'tools/spark_cluster')
                         for m in read_container['Mounts'])]
    if len(candidates) != 1: raise RuntimeError('could not identify the base checkout Context Guard container')
    base_id = candidates[0]['Id']
    replacement = (ROOT / 'tools/spark_cluster/gateway.py').read_text()
    # The managed gateway imports ProxyConfig and the relay implementation from
    # this file. Updating only gateway.py can leave a healthy /health endpoint
    # whose first real request fails against an older dataclass/API.
    managed_replacements = {name: (ROOT / name).read_text() for name in files}
    report = {'node': node_id, 'applied': False, 'base_container': base_id,
              'old_source_sha256': args.expected_source_sha256,
              'new_source_sha256': hashlib.sha256(replacement.encode()).hexdigest(),
              'managed_source_sha256': {name: hashlib.sha256(value.encode()).hexdigest()
                                        for name, value in managed_replacements.items()}}
    if not args.apply:
        print(json.dumps(report)); return
    args.output.mkdir(parents=True, exist_ok=False)
    gateway_node.write(args.output / 'gateway.py.before', previous)
    gateway_node.write(args.output / 'registry.before.json', json.dumps(registry, indent=2))
    key = (gateway_node.ROOT / 'api-key').read_text().strip()
    request = {'node': node, 'action': 'up', 'files': files, 'port': saved['port'],
               'image': managed['Image'], 'registry': registry, 'api_key': key}
    mode = target.stat().st_mode & 0o777
    try:
        gateway_node.write(target, replacement, mode)
        gateway_node.run(['docker', 'restart', base_id])
        gateway_node.main({'node': node, 'action': 'down'})
        gateway_node.main({**request, 'files': {**files, **managed_replacements}})
        gateway_node.main({'node': node, 'action': 'probe'})
        # Check the base process using its published health endpoint.
        subprocess.run(['curl', '--fail', '--silent', '--show-error', '--retry', '10',
                        '--retry-connrefused', '--retry-delay', '1',
                        'http://127.0.0.1:4010/health'], check=True, capture_output=True, timeout=30)
        if read(gateway_node.ROOT / 'config/registry.json') != registry:
            raise RuntimeError('registry changed during the refresh')
        report.update(applied=True, passed=True, routes_and_credentials_preserved=True)
    except BaseException as exc:
        gateway_node.write(target, previous, mode)
        gateway_node.run(['docker', 'restart', base_id])
        gateway_node.main({'node': node, 'action': 'down'})
        gateway_node.main(request)
        report.update(passed=False, rolled_back=True, error=repr(exc))
        raise
    finally:
        gateway_node.write(args.output / 'refresh.json', json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
