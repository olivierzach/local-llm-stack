#!/usr/bin/env python3
"""Stage a pinned container-only NCCL library; never modify host libraries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import urllib.request
import zipfile


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def install(manifest, cache, wheel=None):
    for key in ('sha256', 'wheel_sha256'):
        if not re.fullmatch('[a-f0-9]{64}', manifest[key]):
            raise ValueError('invalid digest')
    target = cache.parent / 'runtime-libraries/nccl' / manifest['sha256'] / 'libnccl.so.2'
    if target.exists() or target.is_symlink():
        if target.is_symlink() or not target.is_file() or digest(target) != manifest['sha256']:
            raise RuntimeError('existing library does not match; refusing to overwrite it')
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.install-', dir=target.parent) as temporary:
        work = Path(temporary)
        if wheel is None:
            if not manifest['url'].startswith('https://files.pythonhosted.org/'):
                raise ValueError('only HTTPS Python package downloads are supported')
            wheel = work / 'nccl.whl'
            with urllib.request.urlopen(manifest['url'], timeout=120) as response, wheel.open('wb') as output:
                shutil.copyfileobj(response, output)
        if digest(wheel) != manifest['wheel_sha256']:
            raise RuntimeError('wheel SHA-256 mismatch')
        staged = work / 'libnccl.so.2'
        with zipfile.ZipFile(wheel) as archive, archive.open(manifest['member']) as source, staged.open('wb') as output:
            shutil.copyfileobj(source, output)
        if digest(staged) != manifest['sha256']:
            raise RuntimeError('library SHA-256 mismatch')
        staged.chmod(0o444)
        # Exclusive link publishes a complete file without clobbering another installer.
        os.link(staged, target)
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--cache', type=Path, required=True, help='node Hugging Face cache from inventory')
    parser.add_argument('--wheel', type=Path, help='already downloaded pinned ARM64 wheel')
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    path = install(manifest, args.cache, args.wheel)
    print(json.dumps({'path': str(path), 'version': manifest['version'], 'sha256': digest(path)}))


if __name__ == '__main__':
    main()
