"""Verify and prepare an immutable Vector Bucket job; never edits shared environments."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sys


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4*1024*1024), b''): h.update(chunk)
    return h.hexdigest()


def main(req):
    if platform.node() != req['node']['hostname']: raise RuntimeError('vector worker host mismatch')
    if req['action'] == 'identity': return {'uid':os.getuid(),'gid':os.getgid()}
    if not re.fullmatch(r'[a-z0-9-]{1,70}', req['owner']): raise RuntimeError('invalid job owner')
    state = Path.home() / '.local/state/local-llm-cluster'
    output = state / req['owner'] / 'artifacts'
    if req['action'] == 'result':
        report = json.loads((output / 'acceptance.json').read_text())
        if digest(output / 'vectors.npz') != report['artifact_sha256']:
            raise RuntimeError('embedding artifact checksum mismatch')
        return {'artifact_dir': str(output), 'acceptance': report}
    if req['action'] != 'prepare': raise RuntimeError('unknown vector operation')
    bundle = Path(req['bundle']['path'])
    if bundle != state / 'bundles' / req['bundle']['identity'] or bundle.is_symlink():
        raise RuntimeError('bundle path/identity mismatch')
    manifest = json.loads((bundle / '.spark-bundle.json').read_text())
    if hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()).hexdigest() != req['bundle']['identity']:
        raise RuntimeError('bundle manifest identity changed')
    actual = set()
    for path in bundle.rglob('*'):
        if path.is_symlink(): raise RuntimeError('bundle contains a symlink')
        if path.is_file(): actual.add(str(path.relative_to(bundle)))
    if actual != set(manifest['files']) | {'.spark-bundle.json'}:
        raise RuntimeError('bundle file set changed')
    for relative, expected in manifest['files'].items():
        relative = Path(relative)
        if relative.is_absolute() or '..' in relative.parts: raise RuntimeError('unsafe bundle path')
        path = bundle / relative
        if path.is_symlink() or not path.is_file() or path.stat().st_size != expected['size'] or digest(path) != expected['sha256']:
            raise RuntimeError('bundle content changed')
    cache = Path(req['model_cache'])
    if not cache.is_absolute(): raise RuntimeError('model cache must be absolute')
    lock = json.loads((bundle / 'model-lock.json').read_text())
    for model in lock['models']:
        if (cache / 'hub' / model['directory'] / 'refs/main').read_text().strip() != model['revision']:
            raise RuntimeError('model cache main reference differs from pinned revision')
    for relative, expected in lock['files'].items():
        path = cache / relative
        if not path.resolve().is_relative_to(cache.resolve()) or path.is_symlink() or not path.is_file() or digest(path) != expected['sha256']:
            raise RuntimeError('model cache differs from its checksum lock')
    for relative, target in lock['symlinks'].items():
        path = cache / relative
        if not path.is_symlink() or str(path.readlink()) != target or not path.resolve().is_relative_to(cache.resolve()):
            raise RuntimeError('model snapshot symlink changed')
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    return {'prepared': True, 'artifact_dir': str(output), 'model_revisions': {m['repo']:m['revision'] for m in lock['models']}}


if __name__ == '__main__':
    try: print(json.dumps({'ok': True, 'result': main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': str(exc)}))
        sys.exit(1)
