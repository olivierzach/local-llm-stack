#!/usr/bin/env python3
"""Build or check the pinned Spark host environment without replacing a venv."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / 'cluster/python/spark-host-cp312-aarch64.lock'


def expected_packages(lock):
    result = {}
    for line in lock.read_text().splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        match = re.fullmatch(r'([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+) --hash=sha256:([0-9a-f]{64})', line)
        if not match:
            raise ValueError('expected a fully pinned, wheel-hashed package on each lock line')
        name = re.sub(r'[-_.]+', '-', match[1]).lower()
        if name in result:
            raise ValueError('duplicate package in lock: ' + name)
        result[name] = match[2]
    if not result:
        raise ValueError('empty package lock')
    return result


def check(venv, lock):
    expected = expected_packages(lock)
    python = venv / 'bin/python'
    probe = 'import json,platform,sys;print(json.dumps([sys.version_info[:2],platform.system(),platform.machine(),sys.prefix]))'
    version, system, machine, prefix = json.loads(subprocess.check_output([str(python), '-c', probe], text=True))
    if version != [3, 12] or system != 'Linux' or machine != 'aarch64' or Path(prefix).resolve() != venv.resolve():
        raise ValueError('environment must be a Linux aarch64 Python 3.12 venv')
    rows = json.loads(subprocess.check_output([str(python), '-m', 'pip', 'list', '--format=json', '--disable-pip-version-check'], text=True))
    actual = {re.sub(r'[-_.]+', '-', p['name']).lower(): p['version'] for p in rows}
    differences = {k: {'expected': v, 'actual': actual.get(k)} for k, v in expected.items() if actual.get(k) != v}
    if differences:
        raise ValueError('package differences: ' + json.dumps(differences, sort_keys=True))
    subprocess.run([str(python), '-m', 'pip', 'check'], check=True)
    return {'passed': True, 'venv': str(venv), 'locked_packages': len(expected),
            'extra_packages': sorted(set(actual) - set(expected)),
            'lock_sha256': hashlib.sha256(lock.read_bytes()).hexdigest(),
            'scope': 'Package versions and dependency consistency; no GPU workload executed.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['create', 'check'])
    parser.add_argument('--venv', type=Path, required=True)
    parser.add_argument('--lock', type=Path, default=LOCK)
    parser.add_argument('--wheelhouse', type=Path, help='Use only these local wheels; network disabled for pip')
    args = parser.parse_args()
    venv = args.venv.expanduser().absolute()
    lock = args.lock.expanduser().resolve()
    expected_packages(lock)
    if args.action == 'create':
        if platform.system() != 'Linux' or platform.machine() != 'aarch64' or sys.version_info[:2] != (3, 12):
            parser.error('create requires Linux aarch64 Python 3.12')
        if venv.exists() or venv.is_symlink():
            parser.error('destination already exists; select a fresh venv path')
        if args.wheelhouse and not args.wheelhouse.expanduser().is_dir():
            parser.error('wheelhouse does not exist')
        subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True)
        source = (['--no-index', '--find-links', str(args.wheelhouse.expanduser().resolve())]
                  if args.wheelhouse else ['--index-url', 'https://pypi.org/simple'])
        subprocess.run([str(venv / 'bin/python'), '-m', 'pip', '--isolated', 'install',
                        '--only-binary=:all:', '--require-hashes', '--no-deps', *source, '-r', str(lock)], check=True)
    result = check(venv, lock)
    if args.action == 'create':
        (venv / 'spark-environment.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print('bootstrap-spark-python: ' + str(error), file=sys.stderr)
        sys.exit(1)
