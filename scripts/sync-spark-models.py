#!/usr/bin/env python3
"""Lock cached Hugging Face revisions and copy their exact files over peer SSH.

Run on the source Spark. Transfers preserve snapshot symlinks and verify every
regular file with SHA-256 at both ends. No model/token/login credentials are
copied; unrelated cached models and snapshots are never deleted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time

# Model identity is file content plus snapshot links, not Unix ownership/mtime.
# In particular, never apply source attributes to shared implied cache parents.
RSYNC_FLAGS = ['-rl', '--checksum', '--no-implied-dirs', '--protect-args', '--from0']


def phase(name, **details):
    print(json.dumps({'phase': name, **details}), file=sys.stderr, flush=True)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b''): h.update(chunk)
    return h.hexdigest()


def capture(cache, specifications):
    files, links, models = {}, {}, []
    seen = set()
    for specification in specifications:
        repo, revision = specification.rsplit('@', 1)
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo) or not re.fullmatch(r'[a-f0-9]{40}', revision):
            raise ValueError('model must be organization/name@40-character-commit')
        directory = 'models--' + repo.replace('/', '--')
        if repo in seen: raise ValueError('each managed cache lock must select one revision per model repository')
        seen.add(repo)
        base = cache / 'hub' / directory
        snapshot = base / 'snapshots' / revision
        if not snapshot.is_dir() or snapshot.is_symlink() or not (snapshot / 'config.json').is_file():
            raise ValueError('pinned snapshot/config is absent or unsafe: ' + specification)
        models.append({'repo': repo, 'revision': revision, 'directory': directory})
        for path in sorted(snapshot.rglob('*')):
            if path.is_dir():
                if path.is_symlink(): raise ValueError('snapshot directory symlinks are not supported')
                continue
            if not path.is_file(): raise ValueError('snapshot has a missing/nonregular file')
            if not path.resolve().is_relative_to(base.resolve()): raise ValueError('snapshot path escapes its model cache')
            if path.is_symlink():
                target = path.resolve(strict=True)
                if not target.is_relative_to(base.resolve()) or target.is_symlink():
                    raise ValueError('snapshot symlink escapes its model cache')
                links[str(path.relative_to(cache))] = os.readlink(path)
                path = target
            relative = str(path.relative_to(cache))
            if relative not in files: files[relative] = {'size': path.stat().st_size, 'sha256': sha(path)}
    return {'version': 1, 'models': models, 'files': files, 'symlinks': links}


RECEIVER = r'''
import hashlib,json,os,pathlib,sys,tempfile
request=json.load(sys.stdin)
root=pathlib.Path(request['cache'])
if not root.is_absolute(): raise ValueError('target cache must be absolute')
manifest=request['lock']
if manifest['version']!=1: raise ValueError('unknown cache lock version')
def safe(relative):
    p=pathlib.Path(relative)
    if p.is_absolute() or '..' in p.parts or not p.parts: raise ValueError('unsafe cache path')
    for parent in (root/p).parents:
        if parent.is_symlink(): raise ValueError('cache parent is a symlink')
    return root/p
for relative in manifest['files']:
    if safe(relative).is_symlink(): raise ValueError('cache file is a symlink')
for relative in manifest['symlinks']:
    safe(relative)
for model in manifest['models']:
    ref=safe('hub/'+model['directory']+'/refs/main')
    if ref.is_symlink(): raise ValueError('cache reference is a symlink')
    if ref.exists() and ref.read_text().strip()!=model['revision']:
        raise ValueError('target main reference differs; use a separate managed cache')
if request['action']=='prepare':
    # Check actual create/replace access before the source hashes a large model.
    # Existing read-only shared ancestors are valid when selected model dirs
    # beneath them already exist and are writable. Do not chmod/chown them.
    parents={safe(relative).parent for relative in [*manifest['files'],*manifest['symlinks']]}
    parents.update(safe('hub/'+model['directory']+'/refs/main').parent for model in manifest['models'])
    checked=set()
    for parent in sorted(parents):
        ancestor=parent
        while not ancestor.exists(): ancestor=ancestor.parent
        if not ancestor.is_dir(): raise ValueError('cache parent is not a directory: '+str(ancestor))
        if ancestor in checked: continue
        checked.add(ancestor)
        try:
            fd,probe=tempfile.mkstemp(prefix='.spark-copy-check-',dir=ancestor)
        except OSError as error:
            raise ValueError('destination cache parent is not writable: '+str(ancestor)+
                '; provision the selected model directory for this user before copying') from error
        os.close(fd)
        os.unlink(probe)
    root.mkdir(parents=True,exist_ok=True)
    print(json.dumps({'prepared':True}))
else:
    for relative,expected in manifest['files'].items():
        path=safe(relative)
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=expected['size']:
            raise ValueError('cache file missing/changed: '+relative)
        h=hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda:f.read(8*1024*1024),b''): h.update(chunk)
        if h.hexdigest()!=expected['sha256']: raise ValueError('cache checksum mismatch: '+relative)
    for relative,target in manifest['symlinks'].items():
        path=safe(relative)
        if not path.is_symlink() or os.readlink(path)!=target or not path.exists() or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('cache symlink mismatch: '+relative)
    for model in manifest['models']:
        ref=safe('hub/'+model['directory']+'/refs/main')
        ref.parent.mkdir(parents=True,exist_ok=True)
        if not ref.exists():
            with ref.open('x') as f: f.write(model['revision'])
    print(json.dumps({'verified_files':len(manifest['files']), 'verified_symlinks':len(manifest['symlinks'])}))
'''


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=('lock', 'sync'))
    p.add_argument('--cache', type=Path, required=True, help='source HF_HOME directory, containing hub/')
    p.add_argument('--model', action='append', default=[])
    p.add_argument('--lock', type=Path, required=True)
    p.add_argument('--peer')
    p.add_argument('--peer-cache')
    args = p.parse_args()
    cache = args.cache.expanduser().resolve()
    if args.action == 'lock':
        if not args.model: p.error('lock requires at least one --model')
        manifest = capture(cache, args.model)
        args.lock.parent.mkdir(parents=True, exist_ok=True)
        args.lock.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
        print(json.dumps({'lock': str(args.lock), 'files': len(manifest['files']),
            'bytes': sum(f['size'] for f in manifest['files'].values())}))
        return
    if not args.peer or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]*', args.peer): p.error('valid --peer required')
    if not args.peer_cache or not args.peer_cache.startswith('/') or any(c in args.peer_cache for c in '\n\r\x00'):
        p.error('absolute --peer-cache required')
    manifest = json.loads(args.lock.read_text())
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15', args.peer]
    def target(action):
        r = subprocess.run(ssh + [shlex.join(['python3', '-c', RECEIVER])],
            input=json.dumps({'action': action, 'cache': args.peer_cache, 'lock': manifest}),
            text=True, capture_output=True, timeout=1800)
        if r.returncode: raise RuntimeError(r.stderr[-1500:])
        return json.loads(r.stdout)
    total_started = time.monotonic()
    phase('destination-preflight')
    target('prepare')
    phase('source-verification', bytes=sum(f['size'] for f in manifest['files'].values()))
    verify_started = time.monotonic()
    if manifest != capture(cache, [m['repo'] + '@' + m['revision'] for m in manifest['models']]):
        raise ValueError('source cache differs from its lock')
    source_verify_s = time.monotonic()-verify_started
    phase('source-verified', elapsed_s=round(source_verify_s, 3))
    started = time.monotonic()
    with tempfile.NamedTemporaryFile() as file_list:
        for relative in sorted(set(manifest['files']) | set(manifest['symlinks'])):
            file_list.write(relative.encode() + b'\0')
        file_list.flush()
        phase('rsync', note='includes rsync checksum preparation and SSH transfer')
        subprocess.run(['rsync', *RSYNC_FLAGS, '--files-from=' + file_list.name,
            '-e', 'ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15',
            str(cache) + '/', args.peer + ':' + args.peer_cache.rstrip('/') + '/'], check=True)
    rsync_s = time.monotonic()-started
    phase('destination-verification', rsync_s=round(rsync_s, 3))
    verify_started = time.monotonic()
    result = target('verify')
    result.update(elapsed_s=round(time.monotonic()-started, 3), bytes=sum(f['size'] for f in manifest['files'].values()))
    result.update(source_verify_s=round(source_verify_s, 3), rsync_s=round(rsync_s, 3),
                  destination_verify_s=round(time.monotonic()-verify_started, 3),
                  total_elapsed_s=round(time.monotonic()-total_started, 3))
    print(json.dumps(result))


if __name__ == '__main__': main()
