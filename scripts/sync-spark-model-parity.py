#!/usr/bin/env python3
"""Copy an explicit single-node model catalog from this Spark to its peer.

Only selected model files are transferred. Login files, runtime state and
unrelated cache entries are excluded. Re-running verifies and repairs copies;
run under a bounded systemd user service for unattended operation.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
import subprocess
import time


def run(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, **kwargs)


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


GGUF_RECEIVER = '''
import hashlib,json,pathlib,sys
r=json.load(sys.stdin)
root=pathlib.Path(r['root'])
assert root.is_absolute()
for name,item in r['files'].items():
 p=pathlib.Path(name)
 assert not p.is_absolute() and '..' not in p.parts
 assert (p.parent==pathlib.Path('models/deepseek-v4') and p.suffix=='.gguf') or str(p)=='data/huggingface/tiktoken/o200k_base.tiktoken'
 path=root/p
 assert not path.is_symlink() and not any(q.is_symlink() for q in path.parents)
 if r['action']=='prepare':
  path.parent.mkdir(parents=True,exist_ok=True)
 else:
  assert path.is_file() and path.stat().st_size==item['size'], name
  h=hashlib.sha256()
  with path.open('rb') as f:
   for chunk in iter(lambda:f.read(8*1024*1024),b''): h.update(chunk)
  assert h.hexdigest()==item['sha256'], name
print(json.dumps({'action':r['action'],'files':len(r['files'])}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--peer', required=True)
    parser.add_argument('--peer-root', required=True)
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, default=Path(__file__).resolve().parents[1] / 'cluster/inventory.json')
    parser.add_argument('--fabric-rail', type=int, default=0)
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]*', args.peer):
        parser.error('invalid SSH peer')
    if not re.fullmatch(r'/[A-Za-z0-9_./-]+', args.peer_root):
        parser.error('peer root must be an absolute plain filesystem path')
    catalog = json.loads(args.catalog.read_text())
    if catalog['version'] != 1:
        parser.error('unsupported catalog')
    root = args.root.expanduser().resolve()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    helper = Path(__file__).with_name('sync-spark-models.py')
    spec = importlib.util.spec_from_file_location('model_sync', helper)
    sync = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sync)
    from spark_transfer import transport
    transport_argv, peer, fabric = transport(args.peer, json.loads(args.inventory.read_text()), args.fabric_rail)
    ssh = transport_argv + [peer]
    report = {'catalog': catalog, 'fabric': fabric, 'started_at': time.time(), 'completed': [], 'failures': []}

    def save():
        temporary = output / 'parity.json.tmp'
        temporary.write_text(json.dumps(report, indent=2) + '\n')
        temporary.replace(output / 'parity.json')

    save()
    # DeepSeek is first so its native runtime can be checked during later copies.
    try:
        files = {}
        for name in catalog['gguf'] + catalog.get('auxiliary', []):
            relative = Path(name)
            if (relative.is_absolute() or '..' in relative.parts or
                    not ((relative.parent == Path('models/deepseek-v4') and relative.suffix == '.gguf') or
                         str(relative) == 'data/huggingface/tiktoken/o200k_base.tiktoken')):
                raise ValueError('unsafe GGUF catalog path')
            path = root / relative
            if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
                raise ValueError('missing or unsafe GGUF: ' + name)
            print(json.dumps({'phase': 'hash-source-file', 'file': name}), flush=True)
            files[name] = {'size': path.stat().st_size, 'sha256': digest(path)}
        (output / 'deepseek-gguf.lock.json').write_text(json.dumps(files, indent=2) + '\n')
        def receiver(action):
            return run(ssh + [shlex.join(['python3', '-c', GGUF_RECEIVER])],
                       input=json.dumps({'root': args.peer_root, 'files': files, 'action': action}),
                       timeout=1800)
        receiver('prepare')
        for name in files:
            print(json.dumps({'phase': 'copy-gguf', 'file': name}), flush=True)
            run(['rsync', '-r', '--checksum', '--partial', '--partial-dir=.spark-parity-partial',
                 '--protect-args', '-e', shlex.join(transport_argv),
                 str(root / name), peer + ':' + args.peer_root + '/' + name], timeout=14400)
        receiver('verify')
        if any(digest(root / name) != item['sha256'] for name, item in files.items()):
            raise ValueError('source GGUF changed during copy')
        report['completed'].append({'kind': 'gguf', 'files': files, 'verified_at': time.time()})
    except Exception as error:
        report['failures'].append({'kind': 'gguf', 'error': str(error)})
    save()
    for model in catalog['models']:
        name = model['repo'] + '@' + model['revision']
        print(json.dumps({'phase': 'model', 'model': name}), flush=True)
        try:
            lock = sync.capture(root / 'data/huggingface', [name])
            lock_path = output / (model['repo'].replace('/', '--') + '.lock.json')
            lock_path.write_text(json.dumps(lock, indent=2) + '\n')
            result = run(['python3', str(helper), 'sync', '--cache', str(root / 'data/huggingface'),
                          '--lock', str(lock_path), '--peer', peer,
                          '--fabric-inventory', str(args.inventory.resolve()), '--fabric-rail', str(args.fabric_rail),
                          '--peer-cache', args.peer_root + '/data/huggingface'],
                         stdout=subprocess.PIPE, timeout=14400)
            receipt = json.loads(result.stdout)
            report['completed'].append({'kind': 'huggingface', **model, **receipt, 'verified_at': time.time()})
            print(json.dumps({'phase': 'verified', 'model': name, **receipt}), flush=True)
        except Exception as error:
            report['failures'].append({'kind': 'huggingface', **model, 'error': str(error)})
        save()
    report['finished_at'] = time.time()
    report['artifacts_verified'] = not report['failures']
    report['inference_verified'] = False
    save()
    print(json.dumps({'report': str(output / 'parity.json'),
                      'artifacts_verified': report['artifacts_verified']}), flush=True)
    return int(bool(report['failures']))


if __name__ == '__main__':
    raise SystemExit(main())
