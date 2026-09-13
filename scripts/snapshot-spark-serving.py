#!/usr/bin/env python3
"""Save live deployment status, owned worker logs, metrics and direct-fabric counters."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import runpy
import shlex
import socket
import subprocess
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import inspect, save_json
from spark_cluster.config import read, validate_saved_plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--saved-plan', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    plan = read(args.saved_plan); validate_saved_plan(plan)
    state = inspect(plan)
    for status in state['nodes'].values():
        if not status.get('reservation') or status['reservation']['digest'] != plan['digest']:
            raise RuntimeError('the exact deployment must still own both nodes')
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    save_json(args.output / 'status.json', state)
    profile = runpy.run_path(str(ROOT / 'scripts/profile-spark-fabric.py'))
    with ThreadPoolExecutor(max_workers=len(plan['nodes'])) as pool:
        counters = dict(zip(plan['nodes'], pool.map(profile['inspect'], plan['nodes'].values())))
    save_json(args.output / 'fabric.json', {'time': time.time(), 'nodes': counters})
    for name, node in plan['nodes'].items():
        for index, identity in enumerate(state['nodes'][name]['reservation']['container_ids']):
            argv = ['docker', 'logs', '--timestamps', identity]
            if socket.gethostname() != node['hostname']:
                argv = ['ssh', '-o', 'BatchMode=yes', node['ssh'], shlex.join(argv)]
            with (args.output / f'{name}-{index}.log').open('w') as log:
                subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=90)
    with requests.Session() as session:
        session.trust_env = False
        r = session.get(plan['endpoint']['base_url'].removesuffix('/v1') + '/metrics', timeout=15)
        r.raise_for_status()
        (args.output / 'metrics.txt').write_text(r.text)
    print(json.dumps({'saved': str(args.output), 'deployment_digest': plan['digest']}))


if __name__ == '__main__': main()
