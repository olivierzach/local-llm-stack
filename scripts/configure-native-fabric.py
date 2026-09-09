#!/usr/bin/env python3
"""Configure native model listeners from this node's private fabric inventory."""
import argparse
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import node
from spark_cluster.gateway_node import write


def configure(root, local, addresses, apply=False):
    node.verify_host(local)
    rail = local['fabric'][0]
    address = str(ipaddress.IPv4Address(rail['ip']))
    if not ipaddress.ip_address(address).is_private:
        raise ValueError('native workers require a private fabric address')
    if not any(entry.get('ifname') == rail['interface'] and
               any(a.get('local') == address for a in entry.get('addr_info', []))
               for entry in addresses):
        raise ValueError('the inventoried fabric address is not assigned; no configuration changed')
    env = root / '.env'
    if not env.is_file() or env.is_symlink():
        raise ValueError('initialize a regular .env first with make init')
    settings = {'DEEPSEEKV4_BIND_HOST': address, 'QWEN38_BIND_HOST': address}
    # Share the .env writer lock used by configure-context-routes.py.
    directory = root / 'data/context-guard-routes'
    if apply:
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / 'lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            original = env.read_text()
            lines = [line for line in original.splitlines()
                     if line.strip().split('=', 1)[0].removeprefix('export ').strip() not in settings]
            lines += [key + '=' + value for key, value in settings.items()]
            updated = '\n'.join(lines) + '\n'
            if updated != original:
                # Preserve the first pre-change configuration, without exposing its secrets.
                backup = directory / 'before-native-fabric.env'
                if not backup.exists():
                    with open(backup, 'x', opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
                        stream.write(original)
                write(env, updated, env.stat().st_mode & 0o777)
    return {'hostname': local['hostname'], 'settings': settings, 'applied': apply,
            'services_restarted': False, 'models_started': False,
            'activation': 'next native model start; restart an existing model explicitly during an idle window'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    import platform
    nodes = json.loads((root / 'cluster/inventory.json').read_text())['nodes'].values()
    matches = [n for n in nodes if n['hostname'] == platform.node()]
    if len(matches) != 1:
        parser.error('host must match exactly one inventory node')
    addresses = json.loads(subprocess.check_output(['ip', '-j', 'address', 'show'], text=True))
    print(json.dumps(configure(root, matches[0], addresses, args.apply)))


if __name__ == '__main__':
    main()
