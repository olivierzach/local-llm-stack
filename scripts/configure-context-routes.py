#!/usr/bin/env python3
"""Install explicit route overrides without changing clients or starting models."""
import argparse
import fcntl
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway
from spark_cluster.gateway_node import write


def configure(root, registry=None, disable=False):
    if not disable:
        gateway.validate_registry(registry)
    env = root / '.env'
    if not env.is_file() or env.is_symlink():
        raise ValueError('initialize a regular .env first with make init')
    directory = root / 'data/context-guard-routes'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / 'lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        text = env.read_text()
        key = 'CONTEXT_GUARD_ROUTE_REGISTRY'
        lines = [line for line in text.splitlines()
                 if line.strip().split('=', 1)[0].removeprefix('export ').strip() != key]
        lines.append(key + '=' + ('' if disable else '/routes/registry.json'))
        if not disable:
            write(directory / 'registry.json', json.dumps(registry, indent=2) + '\n', 0o644)
        write(env, '\n'.join(lines) + '\n', env.stat().st_mode & 0o777)
    return {'enabled': not disable, 'routes': sorted(registry['routes']) if not disable else [],
            'models_started': False, 'services_restarted': False,
            'activation': 'docker compose up -d --no-deps context-guard'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument('--registry', type=Path, help='complete replacement registry; unspecified aliases keep the legacy path')
    choice.add_argument('--plan', type=Path, action='append', help='saved sparkctl plan; repeat for multiple models')
    choice.add_argument('--disable', action='store_true')
    parser.add_argument('--replicas', action='store_true')
    args = parser.parse_args()
    if args.replicas and not args.plan: parser.error('--replicas requires --plan')
    registry = None
    if args.registry: registry = gateway.read(args.registry)
    if args.plan: registry = gateway.from_plans([gateway.read(p) for p in args.plan], replicas=args.replicas)
    print(json.dumps(configure(args.root.expanduser().resolve(), registry, args.disable)))


if __name__ == '__main__':
    main()
