#!/usr/bin/env python3
"""Stage pinned DeepSeek TP artifacts on both Sparks; never start or stop a GPU workload.

Run on either inventoried Spark. Downloads once, verifies upstream checksums,
then copies the runtime and model to the other Spark over its direct cable.
An interrupted preparation can be rerun with the same output directory.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.config import read, validate_inventory
from spark_cluster.cli import save_json
from spark_transfer import transport


def run(argv, **kwargs):
    return subprocess.run(list(map(str, argv)), check=True, **kwargs)


def provision_command(image, cache, directory, uid, gid):
    # Only newly created model-specific directories change ownership. Existing
    # caches, other models, host credentials and service files are untouched.
    code = '''import os,sys
from pathlib import Path
directory,uid,gid=sys.argv[1],int(sys.argv[2]),int(sys.argv[3])
assert directory.startswith('models--') and '/' not in directory and '..' not in directory
for relative in [directory,'.locks/'+directory]:
    path=Path('/cache/hub')/relative
    assert not path.is_symlink()
    if not path.exists():
        path.mkdir(parents=True)
        os.chown(path,uid,gid)
    elif path.stat().st_uid != uid:
        raise RuntimeError('existing model directory belongs to another user: '+str(path))
'''
    return ['docker', 'run', '--rm', '--network', 'none', '--entrypoint', 'python3',
            '-v', str(cache) + ':/cache', image, '-c', code, directory, str(uid), str(gid)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--peer', required=True, choices=('66f1', 'e8f1'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true', help='default is a local plan only')
    args = parser.parse_args()
    inventory = read(ROOT / 'cluster/inventory.json')
    validate_inventory(inventory)
    sources = [n for n in inventory['nodes'].values() if n['hostname'] == socket.gethostname()]
    if len(sources) != 1 or sources[0] == inventory['nodes'][args.peer]:
        parser.error('run on one Spark and select the other as --peer')
    source, peer = sources[0], inventory['nodes'][args.peer]
    manifest_path = ROOT / 'cluster/model-downloads/deepseek-v4-flash-0731.json'
    manifest = read(manifest_path)
    images_path = ROOT / 'cluster/deepseek-images.lock.json'
    images = read(images_path)['images']
    total = sum(f['size'] for f in manifest['files'].values())
    print(json.dumps({'apply': args.apply, 'source': source['hostname'], 'peer': peer['hostname'],
        'model': manifest['repo'], 'revision': manifest['revision'], 'download_bytes': total,
        'gpu_workloads_changed': False}), flush=True)
    if not args.apply:
        return
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    with (output / 'prepare.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def phase(name, **details):
            data = dict(phase=name, time=time.time(), **details)
            save_json(output / 'preparation.json', data)
            print(json.dumps(data), flush=True)
        try:
            phase('checking-direct-cable')
            ssh, alias, fabric = transport(args.peer, inventory)
            save_json(output / 'fabric.json', fabric)
            phase('runtime-image')
            for image in images:
                run(['docker', 'pull', image['registry']])
                actual = subprocess.check_output(['docker', 'image', 'inspect', image['registry'],
                                                  '--format', '{{.Id}}'], text=True).strip()
                if actual != image['id']:
                    raise RuntimeError('downloaded runtime differs from the pinned ARM64 image ID')
            run([sys.executable, ROOT / 'scripts/sync-spark-images.py', '--lock', images_path,
                 '--peer', args.peer, '--fabric-inventory', ROOT / 'cluster/inventory.json',
                 '--restore-registry-digests', '--apply'])
            phase('pinned-nccl-library')
            run([sys.executable, ROOT / 'scripts/install-spark-nccl.py', '--manifest',
                 ROOT / 'cluster/runtime-libraries/nccl-2.30.7-aarch64.json', '--cache', source['cache']])
            # Both nodes use the managed controller; the installer only verifies
            # an already-present pin, or stages it separately from host libraries.
            peer_root = Path(peer['projects']) / 'local-llm-stack-cluster/current'
            run(ssh + [alias, shlex.join(['python3', str(peer_root / 'scripts/install-spark-nccl.py'),
                '--manifest', str(peer_root / 'cluster/runtime-libraries/nccl-2.30.7-aarch64.json'),
                '--cache', peer['cache']])])
            phase('model-download-and-verification')
            run(['docker', 'pull', manifest['image']])
            directory = 'models--' + manifest['repo'].replace('/', '--')
            run(provision_command(manifest['image'], source['cache'], directory, os.getuid(), os.getgid()))
            run(['docker', 'run', '--rm', '--memory', '2g', '--cpus', '2',
                 '--user', f'{os.getuid()}:{os.getgid()}', '--network', 'host', '--entrypoint', 'python3',
                 '-e', 'HF_HOME=/cache', '-e', 'HF_HUB_DISABLE_IMPLICIT_TOKEN=1',
                 '-e', 'HF_XET_NUM_CONCURRENT_RANGE_GETS=4', '-e', 'HF_XET_CACHE=/tmp/xet',
                 '-v', source['cache'] + ':/cache', '-v', str(ROOT / 'scripts') + ':/scripts:ro',
                 '-v', str(manifest_path) + ':/manifest.json:ro', '-v', str(output) + ':/receipts',
                 manifest['image'], '/scripts/fetch-pinned-spark-model.py', 'fetch',
                 '--manifest', '/manifest.json', '--cache', '/cache', '--workers', '2',
                 '--lock-output', '/receipts/model-copy.lock.json', '--max-download-bytes', str(total)])
            phase('model-copy-over-direct-cable')
            ids = subprocess.check_output(ssh + [alias, 'id -u; id -g'], text=True).splitlines()
            if len(ids) != 2 or not all(i.isdigit() for i in ids):
                raise RuntimeError('cannot determine peer cache owner')
            run(ssh + [alias, shlex.join(provision_command(manifest['image'], peer['cache'],
                                                         directory, int(ids[0]), int(ids[1])))])
            with (output / 'model-copy.json').open('w') as receipt:
                run([sys.executable, ROOT / 'scripts/sync-spark-models.py', 'sync',
                     '--cache', source['cache'], '--lock', output / 'model-copy.lock.json',
                     '--peer', args.peer, '--peer-cache', peer['cache'],
                     '--fabric-inventory', ROOT / 'cluster/inventory.json'], stdout=receipt)
            phase('staged-awaiting-gpu-testing', complete=True, gpu_workloads_changed=False,
                  model=manifest['repo'], revision=manifest['revision'], bytes=total,
                  qualification='Artifacts verified on both nodes; inference and client acceptance are still required.')
        except BaseException as exc:
            phase('failed', complete=False, error=repr(exc), gpu_workloads_changed=False)
            raise


if __name__ == '__main__':
    main()
