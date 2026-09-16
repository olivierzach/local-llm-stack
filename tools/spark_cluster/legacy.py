"""Admission for baseline Make operations; normal services keep their own managers.

A durable transaction covers each launch/switch, including controller death.
Successful operations hand back to Compose/systemd; their resident GPU processes
then block other placements. Failed operations retain their recovery identities.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import time
import uuid

from . import node

TARGETS = {
    'up': 'vllm-fast', 'balanced-up': 'vllm-balanced', 'large-up': 'vllm-large',
    'qwen30-up': 'vllm-qwen30a3b', 'deepseek32-up': 'vllm-deepseek32b',
    'mistral24-up': 'vllm-mistral24b', 'gptoss120-up': 'vllm-gptoss120b',
    'lagunas21-up': 'vllm-lagunas21', 'vision-up': 'vllm-vision',
    'lora-serve': 'vllm-lora', 'training-up': 'training', 'lora-train': 'training',
    'qwen38-up': 'native-qwen38', 'deepseekv4-up': 'native-deepseek',
    'qwen38-down': 'native-qwen38', 'deepseekv4-down': 'native-deepseek', 'down': None,
}
SWITCHES = {'qwen38-up', 'deepseekv4-up'}
COMPOSE_SERVICES = {v for v in TARGETS.values() if v and not v.startswith('native-')}


def identity(root):
    digest = hashlib.sha256(str(root).encode()).hexdigest()
    return {'owner': 'legacy-' + digest[:24], 'digest': digest}


def public_setting(root, key, default):
    value = None
    path = root / '.env'
    if path.exists():
        for line in path.read_text().splitlines():
            name, sep, text = line.partition('=')
            if sep and name.strip().removeprefix('export ') == key:
                value = text.strip().strip('"').strip("'")
    value = os.environ.get(key, value) or default
    if '$' in value or '`' in value:
        raise RuntimeError('admission requires a literal ' + key)
    return value


def process_identity(pid):
    try:
        # comm may contain spaces/parentheses; starttime is field 22.
        return (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def process_alive(record):
    return bool(record and record.get('start') and process_identity(record['pid']) == record['start'])


def verify(root):
    with node.locked():
        node.owned(identity(root))
        saved = node.reservation()
        if saved.get('kind') != 'legacy-make' or not process_alive(saved.get('child')):
            raise RuntimeError('internal Make target requires its admitted parent operation')
        pid = os.getpid()
        for _ in range(128):
            if pid == saved['child']['pid']:
                return {'admitted': True}
            if pid <= 1:
                break
            pid = int((Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[1])
        raise RuntimeError('internal Make target is outside the admitted process tree')


def native_unit(root):
    unit = public_setting(root, 'DEEPSEEKV4_SYSTEMD_UNIT', 'local-deepseek-v4.service')
    if '/' in unit or not unit.endswith('.service'):
        raise RuntimeError('invalid native unit name')
    result = subprocess.run(['systemctl', '--user', 'show', unit, '-p', 'ActiveState',
                             '-p', 'MainPID', '-p', 'InvocationID', '-p', 'LoadState', '-p', 'Environment'], capture_output=True, text=True)
    values = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    if values.get('LoadState') == 'not-found':
        return None
    result.check_returncode()
    if values.get('ActiveState') in ('inactive', 'failed', None):
        return None
    pid = int(values.get('MainPID', '0'))
    engine = Path(public_setting(root, 'DEEPSEEKV4_ENGINE_DIR', str(root / 'data/deepseek-v4/ds4'))).expanduser()
    if pid <= 0 or (Path('/proc') / str(pid) / 'exe').resolve() != (engine / 'ds4-server').resolve():
        raise RuntimeError('native unit is pending or has an unexpected executable; retry after it settles')
    nonce = next((v.split('=', 1)[1] for v in shlex.split(values.get('Environment', ''))
                  if v.startswith('SPARK_LEGACY_TRANSACTION=')), '')
    return {'service': 'native-deepseek', 'unit': unit, 'pid': pid,
            'start': process_identity(pid), 'invocation': values['InvocationID'], 'transaction': nonce}


def inspect_stack(root):
    research = node.research_window()
    if research not in (None, 'released', 'restored'):
        raise RuntimeError('research window is active or unresolved; no stack changes made')
    workloads = []
    known_pids = set()
    for container in node.gpu_containers(node.containers()):
        labels = container['Config'].get('Labels') or {}
        service = labels.get('com.docker.compose.service')
        if (labels.get('com.docker.compose.project.working_dir') == str(root)
                and service in COMPOSE_SERVICES):
            pass
        elif (container['Name'] == '/local-qwen38-flash-next'
              and labels.get('io.spark.legacy-root-sha256') == identity(root)['digest']):
            service = 'native-qwen38'
        else:
            raise RuntimeError('a GPU container belongs to another workload; no stack changes made')
        workloads.append({'service': service, 'id': container['Id'],
                          'transaction': labels.get('io.spark.legacy-transaction', '')})
        if container['State']['Status'] not in ('created', 'exited', 'dead'):
            rows = node.run(['docker', 'top', container['Id'], '-eo', 'pid']).splitlines()[1:]
            known_pids.update(row.strip() for row in rows if row.strip().isdigit())
    native = native_unit(root)
    if native:
        workloads.append(native)
        known_pids.add(str(native['pid']))
    gpu = set(node.run(['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader']).split())
    if gpu - known_pids:
        raise RuntimeError('a GPU process belongs to another workload; no stack changes made')
    return workloads


def admit(root, target, workloads):
    if target not in TARGETS:
        raise ValueError('unknown Make operation')
    services = {w['service'] for w in workloads}
    if 'training' in services and target not in ('down', 'training-up'):
        raise RuntimeError('training is active; stop it explicitly before serving a model')
    if target in SWITCHES or target == 'down':
        return
    if services - {TARGETS[target]}:
        raise RuntimeError('another stack model is resident; stop it before this operation')
    if target == 'lora-train' and services:
        raise RuntimeError('training is already resident; a second training run is refused')


def transaction(root):
    saved = node.reservation()
    if saved:
        node.owned(identity(root))
        if saved.get('kind') != 'legacy-make':
            raise RuntimeError('reservation belongs to a different workload')
        raise RuntimeError('an earlier Make transaction remains; inspect it and run make gpu-recover')


def validate_settings(root, target):
    if target == 'up' and public_setting(root, 'COMPOSE_PROFILES', ''):
        raise RuntimeError('make up admits the default model only; unset COMPOSE_PROFILES and use a named model target')
    if target == 'deepseekv4-up':
        mode = os.environ.get('DRAFT_MODE') or public_setting(root, 'DEEPSEEKV4_DRAFT_MODE', 'auto')
        if mode not in ('auto', 'local', 'off'):
            raise RuntimeError('pinned DS4 supports DRAFT_MODE=auto|local|off; no services changed')
        if mode == 'auto' and public_setting(root, 'DEEPSEEKV4_DSPARK_ENABLED', 'true').lower() not in (
                '1', 'true', 'yes', 'on', '0', 'false', 'no', 'off'):
            raise RuntimeError('invalid DEEPSEEKV4_DSPARK_ENABLED; no services changed')


def check(root, target):
    with node.locked():
        transaction(root)
        workloads = inspect_stack(root)
        admit(root, target, workloads)
        validate_settings(root, target)
    return {'launchable': True, 'target': target, 'resident_services': sorted({w['service'] for w in workloads})}


def execute(root, target, make='make'):
    request = identity(root)
    with node.locked():
        transaction(root)
        before = inspect_stack(root)
        admit(root, target, before)
        validate_settings(root, target)
        saved = {**request, 'kind': 'legacy-make', 'phase': 'workload', 'root': str(root),
                 'target': target, 'container_ids': [], 'created_at': time.time(), 'before': before,
                 'parent': {'pid': os.getpid(), 'start': process_identity(os.getpid())}}
        # Reuse a resident model's label so an unchanged `make up` remains
        # idempotent instead of forcing a checkpoint reload just to relabel it.
        saved['transaction'] = next((w['transaction'] for w in before
                                     if w['service'] == TARGETS[target] and w.get('transaction')), uuid.uuid4().hex)
        node.atomic(node.STATE / 'gpu.json', saved)
        read_fd, write_fd = os.pipe()
        # Do not let Make mutate anything before its exact process identity is
        # durable. If the parent dies before opening the gate, EOF cancels Make.
        gate = "import os,sys;fd=int(sys.argv[1]);ok=os.read(fd,1)==b'1';os.close(fd);os.execvp(sys.argv[2],sys.argv[2:]) if ok else sys.exit(1)"
        try:
            child = subprocess.Popen([sys.executable, '-c', gate, str(read_fd), make,
                                      '--no-print-directory', '_spark-' + target], cwd=root, pass_fds=(read_fd,),
                                      env={**os.environ, 'SPARK_LEGACY_TRANSACTION': saved['transaction'],
                                           'SPARK_LEGACY_ROOT_SHA256': request['digest']})
            saved['child'] = {'pid': child.pid, 'start': process_identity(child.pid)}
            node.atomic(node.STATE / 'gpu.json', saved)
            os.write(write_fd, b'1')
        finally:
            os.close(read_fd)
            os.close(write_fd)
    code = child.wait()
    with node.locked():
        node.owned(request)
        after = inspect_stack(root)
        if code == 0 or after == before:
            (node.STATE / 'gpu.json').unlink()
        else:
            saved.update(failed=True, exit_code=code, after=after)
            node.atomic(node.STATE / 'gpu.json', saved)
    return code


def recover(root):
    with node.locked():
        saved = node.reservation()
        if not saved:
            return {'released': True, 'already_absent': True}
        node.owned(identity(root))
        if saved.get('kind') != 'legacy-make' or saved.get('root') != str(root):
            raise RuntimeError('reservation belongs to another workload')
        if process_alive(saved.get('parent')) or process_alive(saved.get('child')):
            raise RuntimeError('the Make operation is still running; recovery refused')
        current = inspect_stack(root)
        # New workers carry the operation label before becoming resident, so a
        # parent crash cannot leave an unidentifiable half-started worker.
        expected = saved.get('after', saved['before'])
        if any(w not in expected and ('after' in saved or w.get('transaction') != saved['transaction'])
               for w in current):
            raise RuntimeError('workload identities changed during the interrupted operation; inspect before cleanup')
        for workload in current:
            if 'id' in workload:
                node.run(['docker', 'stop', '--time', '30', workload['id']], timeout=60)
                node.run(['docker', 'rm', workload['id']], timeout=30)
            else:
                if native_unit(root) != workload:
                    raise RuntimeError('native unit identity changed; cleanup refused')
                node.run(['systemctl', '--user', 'stop', workload['unit']], timeout=60)
        if inspect_stack(root):
            raise RuntimeError('GPU cleanup incomplete; reservation retained')
        (node.STATE / 'gpu.json').unlink()
        return {'released': True, 'stopped': current}


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['run', 'check', 'recover', 'verify'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--target', choices=TARGETS)
    parser.add_argument('--make', default='make')
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    hosts = [n for n in json.loads((root / 'cluster/inventory.json').read_text())['nodes'].values()
             if n['hostname'] == platform.node()]
    if len(hosts) != 1:
        parser.error('current host must match exactly one inventory node')
    node.verify_host(hosts[0])
    if args.action == 'verify':
        verify(root)
        return 0
    if args.action == 'recover':
        print(json.dumps(recover(root)))
        return 0
    if not args.target:
        parser.error('--target is required')
    if args.action == 'check':
        print(json.dumps(check(root, args.target)))
        return 0
    return execute(root, args.target, args.make)
