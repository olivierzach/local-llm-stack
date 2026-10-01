#!/usr/bin/env python3
"""Refresh CPU gateway sources without changing workers, routes or credentials.

Run on the managed gateway host. The gateway hash is mandatory; other changed
base modules must match the owned snapshot or --expected-module-sha256 (a JSON
object mapping repository-relative paths to reviewed SHA-256 hashes). Dry-run
reports source identities without writing files or restarting containers.
"""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway_node
from spark_cluster.config import read, validate_inventory

GATEWAY = 'tools/spark_cluster/gateway.py'
HELPER = 'tools/spark_cluster/recovery_routes.py'
FILES = ('scripts/context-guard-proxy.py', GATEWAY, HELPER,
         'tools/spark_cluster/config.py', 'tools/spark_cluster/__init__.py')
BASE_FILES = FILES[1:]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(path):
    path = Path(path).expanduser().absolute()
    if path.resolve() != path:
        raise RuntimeError(f'noncanonical or symlink path: {path}')
    return path


def capture(path, optional=False):
    path = safe_path(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if optional and path.parent.is_dir():
            return None
        raise
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError(f'expected unlinked regular file: {path}')
        return stream.read(), stat.S_IMODE(info.st_mode)


def put(path, value):
    """Atomic byte-preserving replacement; callers supply an existing safe parent."""
    path = safe_path(path)
    if value is None:
        path.unlink(missing_ok=True)
        return
    data, mode = value
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        safe_path(path)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def recovery_state(saved, managed):
    """Old state.json omits this setting; recover it from the owned container."""
    cmd = managed['Config'].get('Cmd') or []
    values = []
    for index, arg in enumerate(cmd):
        if arg == '--recovery-state':
            if index + 1 >= len(cmd):
                raise RuntimeError('missing managed recovery state argument')
            values.append(cmd[index + 1])
        elif arg.startswith('--recovery-state='):
            values.append(arg.split('=', 1)[1])
    mounts = [m for m in managed['Mounts'] if m['Destination'] == '/recovery']
    if not values:
        if mounts or saved.get('recovery_state') is not None:
            raise RuntimeError('inconsistent managed recovery metadata')
        return None
    if len(values) != 1 or len(mounts) != 1:
        raise RuntimeError('ambiguous managed recovery metadata')
    container_path = Path(values[0])
    if container_path.parent != Path('/recovery') or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', container_path.name):
        raise RuntimeError('unexpected managed recovery path')
    mount = mounts[0]
    path = safe_path(Path(mount['Source']) / container_path.name)
    info = path.parent.stat()
    host = managed['HostConfig']
    if (mount.get('Type') != 'bind' or not mount.get('RW') or
            info.st_uid != os.getuid() or info.st_mode & 0o077 or
            managed['Config'].get('User') != f'{os.getuid()}:{os.getgid()}' or
            host.get('NetworkMode') != 'host' or not host.get('Init') or
            host.get('RestartPolicy', {}).get('Name') != 'unless-stopped'):
        raise RuntimeError('unsafe managed recovery mount, UID or lifecycle')
    if saved.get('recovery_state', str(path)) != str(path):
        raise RuntimeError('managed recovery path differs from saved state')
    return str(path)


def uses_helper(source):
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and (node.module or '').split('.')[-1] == 'recovery_routes':
            return True
        if isinstance(node, (ast.Import, ast.ImportFrom)) and any(
                alias.name.split('.')[-1] == 'recovery_routes' for alias in node.names):
            return True
    return False


def refresh(args, node_id, node):
    base = safe_path(args.root)
    installed = safe_path(gateway_node.ROOT)
    output = safe_path(args.output)
    if output.exists():
        raise RuntimeError('refresh output directory already exists')
    state_before = capture(installed / 'state.json')
    saved = json.loads(state_before[0])
    if not re.fullmatch(r'[a-f0-9]{64}', saved['digest']):
        raise RuntimeError('invalid managed snapshot digest')
    managed = gateway_node.checked(saved)
    if not managed or not managed['State']['Running']:
        raise RuntimeError('managed gateway must be running')
    recovery = recovery_state(saved, managed)
    snapshot = installed / saved['digest']
    old = {}
    for name in FILES:
        value = capture(snapshot / name, optional=name == HELPER)
        if value is not None:
            old[name] = value[0].decode('utf-8')
    definition = {'files': old, 'port': saved['port'], 'image': managed['Config']['Image']}
    if recovery is not None:
        definition['recovery_state'] = recovery
    if sha(json.dumps(definition, sort_keys=True, separators=(',', ':')).encode()) != saved['digest']:
        raise RuntimeError('managed snapshot sources or runtime differ from their recorded digest')
    replacement = {name: capture(ROOT / name)[0].decode('utf-8') for name in FILES}
    rollback_files = dict(old)
    additions = {}
    if HELPER not in old:
        if uses_helper(old[GATEWAY]) or recovery is not None:
            raise RuntimeError('old snapshot requires its missing recovery helper; refusing invented rollback source')
        rollback_files[HELPER] = replacement[HELPER]
        additions[HELPER] = {'sha256': sha(replacement[HELPER].encode()),
                             'reason': 'Unused by old gateway; added only to satisfy current installer source-set validation.'}
    expected = getattr(args, 'expected_module_sha256', None) or {}
    if not isinstance(expected, dict) or any(name not in BASE_FILES or name == GATEWAY or
            not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{64}', value)
            for name, value in expected.items()):
        raise RuntimeError('expected module hashes must name only baseline dependency modules')
    before = {name: capture(base / name, optional=name == HELPER) for name in BASE_FILES}
    if before[GATEWAY] is None or sha(before[GATEWAY][0]) != args.expected_source_sha256:
        raise RuntimeError('base gateway source changed; inspect it before refreshing')
    changes = {}
    sources = {}
    for name, previous in before.items():
        new = replacement[name].encode('utf-8')
        old_hash = sha(previous[0]) if previous else None
        required = (args.expected_source_sha256 if name == GATEWAY else
                    expected.get(name, sha(old[name].encode()) if name in old else None))
        sources[name] = {'before_sha256': old_hash, 'after_sha256': sha(new),
                         'expected_sha256': required, 'before_mode': previous[1] if previous else None}
        if name in expected and old_hash != expected[name]:
            raise RuntimeError(f'base source changed: {name}; expected {expected[name]}, found {old_hash}')
        if previous is None or previous[0] != new:
            if previous is not None and old_hash != required:
                raise RuntimeError(f'base source changed: {name}; expected {required}, found {old_hash}; inspect and supply --expected-module-sha256')
            changes[name] = (new, previous[1] if previous else 0o644)
    ids = gateway_node.run(['docker', 'ps', '-q', '--filter', 'label=com.docker.compose.service=context-guard']).split()
    candidates = [c for identity in ids for c in json.loads(gateway_node.run(['docker', 'inspect', identity]))
                  if any(m.get('Type') == 'bind' and m.get('Source') == str(base / 'tools/spark_cluster')
                         for m in c['Mounts'])]
    if len(candidates) != 1:
        raise RuntimeError('could not identify the base checkout Context Guard container')
    base_id = candidates[0]['Id']
    controls = {name: capture(installed / name) for name in ('api-key', 'gateway.env', 'config/registry.json')}
    registry = json.loads(controls['config/registry.json'][0])
    key = controls['api-key'][0].decode().strip()
    request = {'node': node, 'action': 'up', 'files': rollback_files, 'port': saved['port'],
               'image': managed['Image'], 'registry': registry, 'api_key': key}
    if recovery is not None:
        request['recovery_state'] = recovery
    rollback_definition = {k: request[k] for k in ('files', 'port', 'image')}
    if recovery is not None:
        rollback_definition['recovery_state'] = recovery
    report = {'node': node_id, 'applied': False, 'base_container': base_id,
              'old_image_reference': managed['Config']['Image'], 'pinned_image': managed['Image'],
              'old_source_sha256': args.expected_source_sha256,
              'new_source_sha256': sha(replacement[GATEWAY].encode()),
              'base_sources': sources, 'old_snapshot_digest': saved['digest'],
              'old_snapshot_source_sha256': {k: sha(v.encode()) for k, v in old.items()},
              'rollback_snapshot_digest': sha(json.dumps(rollback_definition, sort_keys=True, separators=(',', ':')).encode()),
              'rollback_added_sources': additions, 'recovery_state': recovery,
              'managed_source_sha256': {k: sha(v.encode()) for k, v in replacement.items()}}
    if not args.apply:
        return report
    # Refuse changes made since planning, before writing either checkout or runtime.
    if capture(installed / 'state.json') != state_before or gateway_node.checked(saved) != managed:
        raise RuntimeError('managed gateway changed during refresh planning')
    for name, value in before.items():
        if capture(base / name, optional=name == HELPER) != value:
            raise RuntimeError(f'base source changed during refresh planning: {name}')
    for name, value in controls.items():
        if capture(installed / name) != value:
            raise RuntimeError(f'gateway configuration changed during refresh planning: {name}')
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    for name, value in before.items():
        if value is not None:
            backup = output / 'base-before' / name
            backup.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            put(backup, (value[0], 0o600))
    put(output / 'state.before.json', (state_before[0], 0o600))
    for name, value in controls.items():
        backup = output / 'managed-before' / name
        backup.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        put(backup, (value[0], 0o600))
    changed = []
    base_restarted = managed_changed = False
    try:
        # Every dependency is present before either process can restart.
        for name, value in changes.items():
            if capture(base / name, optional=name == HELPER) != before[name]:
                raise RuntimeError(f'base source changed before replacement: {name}')
            put(base / name, value)
            changed.append(name)
        base_restarted = True
        gateway_node.run(['docker', 'restart', base_id])
        managed_changed = True
        gateway_node.main({'node': node, 'action': 'down'})
        gateway_node.main({**request, 'files': replacement})
        gateway_node.main({'node': node, 'action': 'probe'})
        subprocess.run(['curl', '--fail', '--silent', '--show-error', '--retry', '10',
                        '--retry-connrefused', '--retry-delay', '1',
                        'http://127.0.0.1:4010/health'], check=True, capture_output=True, timeout=30)
        if read(installed / 'config/registry.json') != registry or capture(installed / 'api-key')[0].decode().strip() != key:
            raise RuntimeError('routes or credentials changed during refresh')
        report.update(applied=True, passed=True, routes_and_credentials_preserved=True)
    except BaseException as exc:
        errors = []
        for name in reversed(changed):
            try:
                if capture(base / name, optional=name == HELPER) != changes[name]:
                    raise RuntimeError(f'base source changed during refresh; refusing rollback overwrite: {name}')
                put(base / name, before[name])
            except BaseException as rollback_error:
                errors.append(repr(rollback_error))
        if base_restarted:
            try:
                gateway_node.run(['docker', 'restart', base_id])
            except BaseException as rollback_error:
                errors.append(repr(rollback_error))
        if managed_changed:
            try:
                gateway_node.main({'node': node, 'action': 'down'})
                gateway_node.main(request)
            except BaseException as rollback_error:
                errors.append(repr(rollback_error))
        for name, value in controls.items():
            try:
                put(installed / name, value)
            except BaseException as rollback_error:
                errors.append(repr(rollback_error))
        report.update(passed=False, rolled_back=not errors, error=repr(exc), rollback_errors=errors)
        raise
    finally:
        put(output / 'refresh.json', ((json.dumps(report, indent=2) + '\n').encode(), 0o600))
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path.home() / 'projects/local-llm-stack')
    p.add_argument('--expected-source-sha256', required=True)
    p.add_argument('--expected-module-sha256', type=json.loads, default={},
                   help='JSON mapping of reviewed baseline dependency paths to their existing SHA-256 hashes')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    inventory = read(ROOT / 'cluster/inventory.json')
    validate_inventory(inventory)
    node_id = socket.gethostname().removeprefix('spark-')
    print(json.dumps(refresh(args, node_id, inventory['nodes'][node_id])))


if __name__ == '__main__':
    main()
