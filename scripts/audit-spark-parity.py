#!/usr/bin/env python3
"""Read-only functional package/tool/image parity audit; never copies credentials."""
import argparse
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_inventory

AUDIT = r'''
import json, os, pathlib, platform, shutil, subprocess
home = pathlib.Path.home()
os.environ['PATH'] = str(home / '.local/bin') + ':/usr/local/cuda/bin:' + os.environ.get('PATH', '')
def run(argv):
    try:
        p = subprocess.run(argv, text=True, capture_output=True, timeout=60)
        return p.returncode, p.stdout.strip()
    except (OSError, subprocess.TimeoutExpired): return 127, ''
code, rows = run(['dpkg-query', '-W', '-f=${binary:Package}\t${Version}\t${db:Status-Status}\n'])
packages = {row.split('\t')[0]: row.split('\t')[1] for row in rows.splitlines() if row.endswith('\tinstalled')}
tools = {}
for binary in ('nvtop','nvidia-smi','nvcc','docker','git','gh','codex','omp','uv','node','npm',
               'aichat','llm','openclaw','python3','gcc','g++','cmake','ninja','ffmpeg','sox',
               'rg','jq','rsync','tmux','htop','numactl','ethtool','iperf3','ib_write_bw','mpirun','tailscale'):
    path = shutil.which(binary)
    tools[binary] = {'path': path}
    if path and binary in ('omp','codex','gh','uv','node','npm','llm','aichat','python3'):
        code, version = run([path, '--version'])
        tools[binary]['version'] = version.splitlines()[0] if code == 0 and version else None
code, output = run(['docker','image','ls','--no-trunc','--format','{{json .}}'])
images = [json.loads(row) for row in output.splitlines()] if code == 0 else []
venvs = {}
for relative in ('projects/local-llm-stack/.venv', 'projects/vector-bucket/.venv', 'scratch/dgx-smoke/.venv'):
    python = home / relative / 'bin/python'
    if python.exists():
        code, output = run([str(python), '-m', 'pip', 'list', '--format=json', '--disable-pip-version-check'])
        venvs[relative] = json.loads(output) if code == 0 else {'error': 'pip inventory unavailable'}
local_bin = home / '.local/bin'
print(json.dumps({'hostname': platform.node(), 'architecture': platform.machine(), 'packages': packages,
  'tools': tools, 'images': images, 'venvs': venvs,
  'local_executables': sorted(p.name for p in local_bin.iterdir() if p.is_file() and os.access(p, os.X_OK)) if local_bin.exists() else []}))
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory', type=Path, default=ROOT / 'cluster/inventory.json')
    p.add_argument('--source', default='66f1')
    p.add_argument('--target', default='e8f1')
    p.add_argument('--output', type=Path, default=ROOT / 'data/cluster/parity')
    args = p.parse_args()
    inv = read(args.inventory)
    validate_inventory(inv)
    audits = {}
    for node in (args.source, args.target):
        config = inv['nodes'][node]
        command = ['python3', '-c', AUDIT]
        if socket.gethostname() != config['hostname']:
            command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', config['ssh'], shlex.join(command)]
        result = subprocess.run(command, text=True, capture_output=True, timeout=240, check=True)
        audits[node] = json.loads(result.stdout)
        if audits[node]['hostname'] != config['hostname']: raise ValueError('remote identity mismatch')
        save_json(args.output / (node + '.json'), audits[node])
    a, b = audits[args.source], audits[args.target]
    source_ids = {i['ID'] for i in a['images']}
    target_ids = {i['ID'] for i in b['images']}
    report = {'source': args.source, 'target': args.target,
        'missing_tools': [k for k, v in a['tools'].items() if v['path'] and not b['tools'][k]['path']],
        'missing_local_executables': sorted(set(a['local_executables']) - set(b['local_executables'])),
        'missing_image_ids': sorted(source_ids - target_ids),
        'missing_packages': sorted(set(a['packages']) - set(b['packages'])),
        'package_version_differences': {k: {'source': v, 'target': b['packages'][k]} for k, v in a['packages'].items()
            if k in b['packages'] and v != b['packages'][k]},
        'policy': 'Review functional gaps; do not downgrade OS, driver or kernel versions to match.'}
    save_json(args.output / 'comparison.json', report)
    print(json.dumps({k: v for k, v in report.items() if k != 'package_version_differences'}))


if __name__ == '__main__':
    try: main()
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        print(f'audit-spark-parity: {exc}', file=sys.stderr)
        sys.exit(1)
