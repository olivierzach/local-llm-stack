#!/usr/bin/env python3
"""Install an exact trusted Git bundle revision without changing a production checkout."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

COMMANDS = ('sparkctl', 'spark-gateway', 'spark-client', 'spark-loop', 'spark-vector')
MARKER = {'format': 1, 'managed_by': 'install-spark-controller'}


def run(argv, **kwargs):
    return subprocess.run([str(arg) for arg in argv], check=True, **kwargs)


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def smoke(release):
    python = release / '.venv/bin/python'
    run([python, '-m', 'pip', 'check'])
    for command in COMMANDS:
        run([python, release / 'scripts' / command, '--help'], stdout=subprocess.DEVNULL)
    run([python, release / 'scripts/sparkctl', 'validate', '--deployment',
         release / 'cluster/deployments/fast-e8f1.json'], stdout=subprocess.DEVNULL)
    run([python, release / 'scripts/sparkctl', 'validate', '--deployment',
         release / 'cluster/deployments/fast-66f1.json'], stdout=subprocess.DEVNULL)


def verify_release(release, revision, state):
    receipt = json.loads((release / '.controller-release.json').read_text())
    if receipt.get('revision') != revision or receipt.get('format') != 1:
        raise RuntimeError('existing release receipt does not match the requested revision')
    head = subprocess.check_output(['git', '-C', str(release), 'rev-parse', 'HEAD'], text=True).strip()
    changes = subprocess.check_output(['git', '-C', str(release), 'status', '--porcelain', '--untracked-files=no'], text=True)
    if head != revision or changes:
        raise RuntimeError('existing release has changed; refusing to activate it')
    if (release / 'data/cluster').resolve() != state:
        raise RuntimeError('existing release does not use the shared controller state')
    lock_hash = hashlib.sha256((release / 'tools/controller-requirements.lock').read_bytes()).hexdigest()
    if receipt.get('requirements_sha256') != lock_hash:
        raise RuntimeError('existing release dependency receipt does not match')
    smoke(release)


def prepare_release(bundle, revision, release, state):
    if release.exists() or release.is_symlink():
        verify_release(release, revision, state)
        return
    # Create exclusively: cleanup below may remove only this invocation's directory.
    release.mkdir()
    try:
        run(['git', '-c', 'core.hooksPath=/dev/null', 'clone', '--no-checkout', bundle, release])
        run(['git', '-C', release, '-c', 'core.hooksPath=/dev/null', 'checkout', '--detach', revision])
        head = subprocess.check_output(['git', '-C', str(release), 'rev-parse', 'HEAD'], text=True).strip()
        if head != revision:
            raise RuntimeError('bundle checkout did not match the requested commit')
        (release / 'data').mkdir(exist_ok=True)
        (release / 'data/cluster').symlink_to(state, target_is_directory=True)
        with (release / '.git/info/exclude').open('a') as stream:
            stream.write('\n.controller-release.json\n')
        run([sys.executable, '-m', 'venv', release / '.venv'])
        python = release / '.venv/bin/python'
        lock = release / 'tools/controller-requirements.lock'
        run([python, '-m', 'pip', '--isolated', 'install', '--index-url', 'https://pypi.org/simple',
             '--only-binary=:all:', '--require-hashes', '-r', lock])
        smoke(release)
        atomic_json(release / '.controller-release.json', {
            'format': 1, 'revision': revision,
            'requirements_sha256': hashlib.sha256(lock.read_bytes()).hexdigest(),
            'python': subprocess.check_output([str(python), '--version'], text=True).strip(),
        })
    except BaseException:
        shutil.rmtree(release)
        raise


def activate(prefix, release):
    current = prefix / 'current'
    if current.exists() and not current.is_symlink():
        raise RuntimeError('current is not a managed symlink; refusing to replace it')
    previous = os.readlink(current) if current.is_symlink() else None
    if previous is not None and (prefix / previous).resolve() != release.resolve():
        prior = prefix / 'previous.tmp'
        prior.unlink(missing_ok=True)
        prior.symlink_to(previous, target_is_directory=True)
        prior.replace(prefix / 'previous')
    temporary = prefix / 'current.tmp'
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(release.relative_to(prefix), target_is_directory=True)
    temporary.replace(current)
    for command in COMMANDS:
        target = prefix / 'bin' / command
        content = '#!/bin/sh\nexec ' + shlex.quote(str(current / '.venv/bin/python')) + ' ' + shlex.quote(str(current / 'scripts' / command)) + ' "$@"\n'
        temporary = target.with_suffix('.tmp')
        temporary.write_text(content)
        temporary.chmod(0o755)
        temporary.replace(target)


def install(bundle, revision, prefix):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('--revision must be an explicit full Git commit ID')
    bundle = bundle.expanduser().resolve(strict=True)
    if not bundle.is_file():
        raise ValueError('--bundle must be a local Git bundle file')
    prefix = prefix.expanduser().absolute()
    if prefix.is_symlink():
        raise RuntimeError('installation prefix must not be a symlink')
    marker = prefix / '.spark-controller.json'
    if prefix.exists():
        if not marker.is_file() or json.loads(marker.read_text()) != MARKER:
            raise RuntimeError('destination is not a managed controller installation; choose an empty new path')
    else:
        prefix.mkdir(parents=True, mode=0o700)
        atomic_json(marker, MARKER)
    prefix = prefix.resolve()
    with (prefix / '.install.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for folder in ('releases', 'state', 'bin'):
            (prefix / folder).mkdir(mode=0o700, exist_ok=True)
        release = prefix / 'releases' / revision
        prepare_release(bundle, revision, release, prefix / 'state')
        activate(prefix, release)
    return {'revision': revision, 'release': str(release), 'commands': str(prefix / 'bin'),
            'state': str(prefix / 'state'), 'services_restarted': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--prefix', type=Path, default=Path.home() / 'projects/local-llm-stack-cluster')
    args = parser.parse_args()
    print(json.dumps(install(args.bundle, args.revision, args.prefix), sort_keys=True))


if __name__ == '__main__':
    main()
