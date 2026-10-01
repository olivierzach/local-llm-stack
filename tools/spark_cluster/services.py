"""Private, release-pinned launchd hosting for one explicitly selected awake Mac.

Config version 1 requires release (absolute immutable release directory), revision
(full Git SHA), python (that release's .venv/bin/python), policy, service_dir,
state_dir and inventory. Optional plans adds monitor saved plans; gateway accepts
bind/port, monitor accepts bind/port/interval/timeout, awake is an explicit bool.
Policy gateway files must be distinct children of service_dir/ingress. Install
creates that dedicated ingress, never adopts a legacy gateway. Install does not
start jobs or enable recovery. All host mutations require --apply.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import plistlib
import re
import secrets
import signal
import stat
import subprocess
import sys
import tempfile
import threading
from urllib.parse import urlsplit

ROLES = ('gateway', 'recovery', 'monitor', 'awake')
MANAGED = 'spark-services-v1'
WARNING = ('This Mac is the single authority and ingress: power loss, forced sleep, '
           'lid closure, logout, or network loss interrupts service. LaunchAgents require '
           'this user to remain logged in. Caffeinate is optional and is not HA; it '
           'does not override forced sleep or guarantee closed-lid operation. Stopping '
           'services expires routing leases; it never frees Spark GPUs or reservations.')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def absolute(value):
    require(isinstance(value, str) and value and not any(ord(c) < 32 for c in value),
            'paths must be nonempty absolute strings without control characters')
    path = Path(value)
    require(path.is_absolute() and '..' not in path.parts, 'absolute canonical paths are required')
    return path


def no_symlinks(path):
    for parent in (path, *path.parents):
        require(not parent.is_symlink(), 'symbolic links are not allowed for managed paths')


def private(path, directory=False):
    no_symlinks(path)
    info = path.stat()
    require(info.st_uid == os.getuid() and not info.st_mode & 0o077,
            'private paths must be owned by this user without group/other permissions')
    require(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode),
            'private path has the wrong file type')


def read_json(path, secret=False):
    if secret:
        private(path)
    return json.loads(path.read_text())


def atomic(path, payload):
    """Only write beneath an already private, symlink-free directory."""
    private(path.parent, directory=True)
    no_symlinks(path)
    if path.exists():
        private(path)
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()


def private_dir(path):
    no_symlinks(path)
    missing = []
    parent = path
    while not parent.exists():
        missing.append(parent)
        parent = parent.parent
    for parent in reversed(missing):
        parent.mkdir(mode=0o700, exist_ok=True)
        private(parent, directory=True)
    private(path, directory=True)


def release_check(root, revision, python):
    require(re.fullmatch(r'[0-9a-f]{40}', revision or '') is not None, 'revision must be a full Git SHA')
    no_symlinks(root)
    require(root.name == revision and root.is_dir(), 'release must name an immutable revision, not current')
    require(python == root / '.venv/bin/python' and python.is_file() and os.access(python, os.X_OK),
            'python must be the executable interpreter of the pinned release')
    receipt = read_json(root / '.controller-release.json')
    require(receipt.get('format') == 1 and receipt.get('revision') == revision,
            'release receipt does not match the configured revision')
    hashes = receipt.get('source_sha256')
    require(isinstance(hashes, dict) and hashes, 'release needs a source-hashed installer receipt; install a new revision')
    required = ['scripts/' + name for name in ('spark-services', 'spark-recover', 'spark-monitor', 'spark-node')]
    required += ['tools/spark_cluster/' + name + '.py' for name in
                 ('services', 'recovery', 'monitor', 'enrollment', 'gateway', 'config')]
    require(all(name in hashes for name in required), 'release lacks required runtime entrypoints/modules')
    for name, digest in hashes.items():
        relative = Path(name)
        require(not relative.is_absolute() and '..' not in relative.parts and
                relative.parts[0] in ('scripts', 'tools'), 'invalid release payload path')
        path = root / relative
        no_symlinks(path)
        require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == digest,
                'immutable release payload has changed')
    # Untracked modules can shadow the signed payload even when tracked files match.
    for path in (root / 'tools').rglob('*.py'):
        require(str(path.relative_to(root)) in hashes, 'untracked Python module in release')
    require(hashlib.sha256((root / 'tools/controller-requirements.lock').read_bytes()).hexdigest()
            == receipt.get('requirements_sha256'), 'release dependency receipt has changed')


def bind_options(value, defaults):
    require(isinstance(value, dict) and not set(value) - set(defaults), 'unknown listener settings')
    result = {**defaults, **value}
    require(isinstance(result['bind'], str) and
            isinstance(ipaddress.ip_address(result['bind']), ipaddress.IPv4Address),
            'listeners require an IPv4 bind address')
    require(type(result['port']) is int and 1 <= result['port'] <= 65535, 'invalid listener port')
    for key, maximum in (('interval', 3600), ('timeout', 120)):
        if key in result:
            number = result[key]
            require(type(number) in (int, float) and math.isfinite(number) and .05 <= number <= maximum,
                    'monitor timing must be finite and bounded')
    return result


def load_config(path):
    from . import recovery
    from .config import read, validate_inventory, validate_saved_plan
    path = absolute(str(path))
    private(path)
    private(path.parent, directory=True)
    raw = read_json(path)
    required = {'version', 'release', 'revision', 'python', 'policy', 'service_dir', 'state_dir', 'inventory'}
    require(isinstance(raw, dict) and required <= raw.keys() and
            not raw.keys() - required - {'plans', 'gateway', 'monitor', 'awake'}, 'invalid service config fields')
    require(type(raw['version']) is int and raw['version'] == 1, 'unsupported service config version')
    cfg = dict(raw)
    for name in ('release', 'python', 'policy', 'service_dir', 'state_dir', 'inventory'):
        cfg[name] = absolute(raw[name])
    cfg['config'] = path
    release_check(cfg['release'], raw['revision'], cfg['python'])
    require(type(raw.get('awake', False)) is bool, 'awake must be an explicit boolean')
    cfg['awake'] = raw.get('awake', False)
    for name in ('policy', 'service_dir', 'state_dir', 'inventory'):
        no_symlinks(cfg[name])
    private(cfg['policy'])
    cfg['policy_data'] = recovery.load_policy(cfg['policy'])
    policy = cfg['policy_data']
    require(re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', policy['id']) is not None,
            'service policy id must be a safe launchd identifier')
    require(cfg['state_dir'].is_relative_to(cfg['service_dir']) and cfg['state_dir'] != cfg['service_dir'],
            'state_dir must be inside the dedicated service_dir')
    require(not cfg['service_dir'].is_relative_to(cfg['release']), 'service state must be outside the immutable release')
    require(not cfg['release'].is_relative_to(cfg['service_dir']),
            'service_dir must not contain the immutable release')
    ingress = cfg['service_dir'] / 'ingress'
    paths = [absolute(str(policy['gateway'][name])) for name in ('key_file', 'registry', 'route_state')]
    require(len(set(paths)) == 3 and all(p.parent == ingress for p in paths),
            'gateway key, registry and route state must be distinct files in service_dir/ingress')
    require(cfg['state_dir'] != ingress and not cfg['state_dir'].is_relative_to(ingress),
            'controller journal must be separate from ingress')
    require(not cfg['state_dir'].is_relative_to(cfg['service_dir'] / 'logs'),
            'controller journal must be separate from service logs')
    for folder in (cfg['service_dir'], cfg['state_dir'], ingress):
        no_symlinks(folder)
        if folder.exists():
            private(folder, directory=True)
    for file in paths:
        no_symlinks(file)
        if file.exists():
            private(file)
    url = urlsplit(policy['gateway']['url'])
    require(url.scheme == 'http' and url.hostname and not url.username and not url.password,
            'local gateway service requires an HTTP origin without credentials')
    cfg['gateway'] = bind_options(raw.get('gateway', {}), {'bind': '127.0.0.1', 'port': url.port or 80})
    require(cfg['gateway']['port'] == (url.port or 80), 'gateway listener and policy ports differ')
    if ipaddress.ip_address(cfg['gateway']['bind']).is_loopback:
        require(url.hostname in ('localhost', '127.0.0.1', '::1'), 'loopback gateway requires a local policy URL')
    cfg['monitor'] = bind_options(raw.get('monitor', {}),
                                  {'bind': '127.0.0.1', 'port': 9842, 'interval': 10, 'timeout': 8})
    require(cfg['monitor']['port'] != cfg['gateway']['port'], 'listeners must use distinct ports')
    validate_inventory(read(cfg['inventory']))
    require(isinstance(raw.get('plans', []), list), 'plans must be a list')
    cfg['plans'] = [absolute(p) for p in raw.get('plans', [])]
    # Always monitor all policy engines, including TP ranks as one saved deployment.
    cfg['plans'] = list(dict.fromkeys(cfg['plans'] + [Path(ref['path']) for ref in
                             [policy['preferred'], *policy['fallbacks']]]))
    for plan in cfg['plans']:
        validate_saved_plan(read(plan))
    return cfg


def label(cfg, role):
    require(role in ROLES, 'unknown role')
    return 'org.spark-recovery.' + cfg['policy_data']['id'] + '.' + role


def roles(cfg):
    return ['gateway', 'monitor', 'recovery'] + (['awake'] if cfg['awake'] else [])


def render(cfg):
    result = {}
    for role in roles(cfg):
        name = label(cfg, role)
        result[role] = plistlib.dumps({
            'Label': name,
            'ProgramArguments': [str(cfg['python']), str(cfg['release'] / 'scripts/spark-services'),
                                 'run-role', '--config', str(cfg['config']), '--role', role],
            'WorkingDirectory': str(cfg['service_dir']),
            'EnvironmentVariables': {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'PYTHONNOUSERSITE': '1',
                                     'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUNBUFFERED': '1'},
            'RunAtLoad': True, 'KeepAlive': True, 'ThrottleInterval': 10, 'ExitTimeOut': 30,
            'Umask': 0o077, 'ProcessType': 'Background',
            'StandardOutPath': str(cfg['service_dir'] / 'logs' / (role + '.out.log')),
            'StandardErrorPath': str(cfg['service_dir'] / 'logs' / (role + '.err.log')),
            'SparkServicesOwner': MANAGED, 'SparkServicesConfig': str(cfg['config']),
        }, sort_keys=True)
    return result


def run_role(cfg, role):
    require(role in roles(cfg), 'role not enabled in this config; awake requires explicit opt-in')
    require(Path(__file__).resolve() == cfg['release'] / 'tools/spark_cluster/services.py',
            'foreground roles must execute the configured immutable release')
    private(cfg['service_dir'], directory=True)
    private(cfg['state_dir'], directory=True)
    ownership(cfg)
    os.umask(0o077)
    if role == 'awake':
        require(sys.platform == 'darwin', 'awake role requires macOS')
        os.execv('/usr/bin/caffeinate', ['/usr/bin/caffeinate', '-i', '-s'])
    if role == 'recovery':
        from . import recovery
        return recovery.main(['run', '--policy', str(cfg['policy']), '--state-dir', str(cfg['state_dir'])])
    gateway = cfg['policy_data']['gateway']
    if role == 'gateway':
        from .gateway import serve
        from .monitor import private_key
        private(Path(gateway['registry']))
        server = serve(Path(gateway['registry']), cfg['gateway']['bind'], cfg['gateway']['port'],
                       private_key(gateway['key_file']), recovery_state=Path(gateway['route_state']))
        stopping = threading.Event()
        def stop(signum, frame):
            if not stopping.is_set():
                stopping.set()
                # shutdown waits for serve_forever: never call it on this thread.
                threading.Thread(target=server.shutdown, daemon=True).start()
        previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            server.serve_forever(poll_interval=.2)
        finally:
            try:
                # Recovery GatewayServer drains its non-daemon handlers before
                # releasing the singleton ingress lock. Never close that lock early.
                server.server_close()
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
        return 0
    from . import monitor
    argv = ['--inventory', str(cfg['inventory']), '--recovery-state', str(cfg['state_dir'] / 'status.json'),
            '--gateway-url', gateway['url'], '--gateway-key-file', str(gateway['key_file'])]
    for plan in cfg['plans']:
        argv += ['--plan', str(plan)]
    for name, value in cfg['monitor'].items():
        argv += ['--' + name, str(value)]
    return monitor.main(argv)


def owner_value(cfg):
    return {'version': 1, 'managed_by': MANAGED, 'config': str(cfg['config']),
            'policy': cfg['policy_data']['id']}


def ownership(cfg):
    marker = cfg['service_dir'] / '.spark-services.json'
    value = read_json(marker, secret=True)
    require(value == owner_value(cfg), 'service directory belongs to a different config or policy')
    return value


class Launchctl:
    def __init__(self):
        require(sys.platform == 'darwin', 'launchd management requires macOS')
        self.domain = 'gui/' + str(os.getuid())

    def call(self, *args):
        result = subprocess.run(['/bin/launchctl', *map(str, args)], capture_output=True, text=True,
                                timeout=30, check=False)
        return result

    def inspect(self, name):
        result = self.call('print', self.domain + '/' + name)
        if result.returncode:
            # launchctl's absent-service status; other failures are not absence.
            if result.returncode == 113:
                return None
            raise RuntimeError('launchctl could not inspect the user service domain')
        match = re.search(r'^\s*path = (.+)$', result.stdout, re.MULTILINE)
        require(match is not None, 'cannot establish loaded launchd job ownership')
        pid = re.search(r'^\s*pid = (\d+)$', result.stdout, re.MULTILINE)
        return {'path': match.group(1).strip(), 'pid': int(pid.group(1)) if pid else None}

    def start(self, path):
        require(self.call('bootstrap', self.domain, path).returncode == 0, 'launchctl bootstrap failed')

    def stop(self, name):
        require(self.call('bootout', self.domain + '/' + name).returncode == 0, 'launchctl bootout failed')


def installed(cfg, launch_dir, controller):
    """Preflight every known role before any action, including previously enabled awake."""
    manifest = cfg['service_dir'] / 'installed.json'
    ownership(cfg)
    records = read_json(manifest, secret=True) if manifest.exists() else {}
    require(isinstance(records, dict) and not records.keys() - set(ROLES), 'invalid installed service manifest')
    result = {}
    for role in ROLES:
        path = launch_dir / (label(cfg, role) + '.plist')
        no_symlinks(path)
        loaded = controller.inspect(label(cfg, role))
        if path.exists():
            private(path)
            payload = path.read_bytes()
            require(records.get(role) == hashlib.sha256(payload).hexdigest(), 'refusing an unowned or modified launchd plist')
            plist = plistlib.loads(payload)
            require(plist.get('SparkServicesOwner') == MANAGED and
                    plist.get('SparkServicesConfig') == str(cfg['config']) and plist.get('Label') == label(cfg, role),
                    'launchd plist ownership mismatch')
        else:
            require(role not in records, 'owned launchd plist is missing; refusing ambiguous service changes')
        if loaded:
            require(path.exists() and loaded['path'] == str(path), 'refusing a foreign loaded launchd label')
        result[role] = {'path': path, 'loaded': loaded, 'exists': path.exists()}
    return result

def plist_snapshot(cfg, launch_dir):
    paths = {role: launch_dir / (label(cfg, role) + '.plist') for role in ROLES}
    paths['manifest'] = cfg['service_dir'] / 'installed.json'
    values = {}
    for name, path in paths.items():
        no_symlinks(path)
        if path.exists():
            private(path)
            require(path.stat().st_size <= 1024 * 1024, 'service file exceeds transaction bound')
            values[name] = path.read_text()
        else:
            values[name] = None
    return paths, values


def snapshot_hashes(values):
    return {name: hashlib.sha256(value.encode()).hexdigest() if value is not None else None
            for name, value in values.items()}


def replace_plist(path, value):
    """Atomic private plist replacement; LaunchAgents itself may be mode 0755."""
    no_symlinks(path)
    if path.exists():
        private(path)
    if value is None:
        path.unlink(missing_ok=True)
    else:
        fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def recover_plists(cfg, launch_dir, controller):
    journal = cfg['service_dir'] / '.services-transaction.json'
    no_symlinks(journal)
    if not journal.exists():
        return
    private(journal)
    require(journal.stat().st_size <= 1024 * 1024, 'service transaction exceeds size bound')
    tx = read_json(journal)
    require(isinstance(tx, dict) and set(tx) == {
        'version', 'owner', 'launch_dir', 'before', 'after', 'before_sha256', 'after_sha256'} and
        tx['version'] == 1 and tx['owner'] == owner_value(cfg) and tx['launch_dir'] == str(launch_dir),
        'service transaction ownership mismatch')
    names = set(ROLES) | {'manifest'}
    for phase in ('before', 'after'):
        values = tx[phase]
        require(isinstance(values, dict) and set(values) == names and
                all(value is None or isinstance(value, str) for value in values.values()) and
                snapshot_hashes(values) == tx[phase + '_sha256'], 'invalid service transaction snapshot')
        for role in ROLES:
            if values[role] is not None:
                plist = plistlib.loads(values[role].encode())
                require(plist.get('Label') == label(cfg, role) and
                        plist.get('SparkServicesOwner') == MANAGED and
                        plist.get('SparkServicesConfig') == str(cfg['config']),
                        'service transaction contains a foreign plist')
        records = json.loads(values['manifest']) if values['manifest'] is not None else {}
        require(records == {role: tx[phase + '_sha256'][role] for role in ROLES
                            if values[role] is not None}, 'transaction manifest disagrees with its plists')
    paths, current = plist_snapshot(cfg, launch_dir)
    hashes = snapshot_hashes(current)
    # Validate the entire boundary before restoring any file. Never overwrite a
    # user edit merely because a previous installer left a transaction behind.
    require(all(hashes[name] in (tx['before_sha256'][name], tx['after_sha256'][name])
                for name in names), 'service transaction conflicts with an unknown file; preserved')
    for role in ROLES:
        loaded = controller.inspect(label(cfg, role))
        if loaded:
            require(current[role] is not None and loaded['path'] == str(paths[role]) and
                    tx['before'][role] == tx['after'][role],
                    'cannot recover a changed or foreign loaded service')
    for name in (*ROLES, 'manifest'):
        if current[name] != tx['before'][name]:
            replace_plist(paths[name], tx['before'][name])
    # Directory fsync makes journal removal the commit point for the rollback.
    replace_plist(journal, None)


def commit_plists(cfg, launch_dir, rendered):
    paths, before = plist_snapshot(cfg, launch_dir)
    after = {role: rendered[role].decode() if role in rendered else None for role in ROLES}
    after['manifest'] = json_bytes({role: hashlib.sha256(payload).hexdigest()
                                    for role, payload in rendered.items()}).decode()
    if before == after:
        return
    journal = cfg['service_dir'] / '.services-transaction.json'
    require(not journal.exists(), 'pending service transaction must be recovered first')
    atomic(journal, json_bytes({'version': 1, 'owner': owner_value(cfg), 'launch_dir': str(launch_dir),
                              'before': before, 'after': after,
                              'before_sha256': snapshot_hashes(before), 'after_sha256': snapshot_hashes(after)}))
    # Leave the durable journal on ANY interruption, including a full disk during
    # rollback. The next explicit host mutation can safely restore the old set.
    for name in (*ROLES, 'manifest'):
        if before[name] != after[name]:
            replace_plist(paths[name], after[name])
    replace_plist(journal, None)


def initialize(cfg):
    from . import recovery
    from .gateway import from_plans
    from .monitor import private_key
    root = cfg['service_dir']
    private_dir(root)
    marker = root / '.spark-services.json'
    ingress = root / 'ingress'
    if marker.exists():
        ownership(cfg)
    else:
        require(not ingress.exists(), 'refusing to adopt an existing gateway directory')
        require(not cfg['state_dir'].exists(), 'refusing to adopt an existing controller journal')
        atomic(marker, json_bytes(owner_value(cfg)))
    private_dir(ingress)
    private_dir(cfg['state_dir'])
    if not (cfg['state_dir'] / 'journal.json').exists():
        recovery.initialize(cfg['policy_data'], cfg['state_dir'])
    private_dir(root / 'logs')
    gateway = cfg['policy_data']['gateway']
    key = Path(gateway['key_file'])
    if not key.exists():
        atomic(key, (secrets.token_urlsafe(48) + '\n').encode())
    private_key(key)
    registry = Path(gateway['registry'])
    if not registry.exists():
        atomic(registry, json_bytes(from_plans([recovery.load_plan(cfg['policy_data']['preferred'])])))
    else:
        from .config import read
        from .gateway import validate_registry
        validate_registry(read(registry))
    for role in ROLES:
        for suffix in ('out', 'err'):
            path = root / 'logs' / (role + '.' + suffix + '.log')
            if not path.exists():
                atomic(path, b'')
            else:
                private(path)


def manage(cfg, action, apply=False, *, controller=None, launch_dir=None):
    require(action in ('install', 'start', 'status', 'stop', 'uninstall'), 'invalid service action')
    require(action == 'status' or apply, 'host changes require --apply')
    controller = controller or Launchctl()
    launch_dir = launch_dir or Path.home() / 'Library/LaunchAgents'
    no_symlinks(launch_dir)
    if launch_dir.exists():
        info = launch_dir.stat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o022,
                'LaunchAgents directory must be owned and not writable by others')
    if action == 'install':
        # Check collisions before adopting any directory or generating credentials.
        for role in ROLES:
            path = launch_dir / (label(cfg, role) + '.plist')
            if not (cfg['service_dir'] / '.spark-services.json').exists():
                require(not path.exists() and not path.is_symlink() and not controller.inspect(label(cfg, role)),
                        'refusing an existing launchd label')
        private_dir(cfg['service_dir'])
    if action != 'install' and not (cfg['service_dir'] / '.spark-services.json').exists():
        require(action == 'status', 'services are not installed')
        require(not any(controller.inspect(label(cfg, role)) for role in ROLES),
                'unowned launchd label exists for this policy')
        return {'installed': False, 'warning': WARNING}
    if action == 'status':
        pending = cfg['service_dir'] / '.services-transaction.json'
        no_symlinks(pending)
        if pending.exists():
            private(pending)
            return {'transaction_pending': True, 'repair': 'rerun install, stop or uninstall with --apply',
                    'warning': WARNING}
        current = installed(cfg, launch_dir, controller)
        return {'installed': any(v['exists'] for v in current.values()), 'awake_opt_in': cfg['awake'],
                'roles': {r: {'installed': v['exists'], 'loaded': bool(v['loaded']),
                              'pid': v['loaded']['pid'] if v['loaded'] else None} for r, v in current.items()},
                'warning': WARNING}
    lock_path = cfg['service_dir'] / '.services.lock'
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+') as lock:
        private(lock_path)
        fcntl.flock(lock, fcntl.LOCK_EX)
        if action == 'install':
            initialize(cfg)
        ownership(cfg)
        recover_plists(cfg, launch_dir, controller)
        current = installed(cfg, launch_dir, controller)
        if action == 'install':
            launch_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            rendered = render(cfg)
            require(not any(v['loaded'] and (r not in rendered or
                        v['path'].read_bytes() != rendered[r]) for r, v in current.items()),
                    'stop owned services before changing their launch configuration')
            commit_plists(cfg, launch_dir, rendered)
        elif action == 'start':
            rendered = render(cfg)
            require({r for r, v in current.items() if v['exists']} == set(rendered),
                    'install the current role set before starting')
            private(cfg['service_dir'] / 'logs', directory=True)
            for role in roles(cfg):
                value = current[role]
                require(value['exists'] and value['path'].read_bytes() == rendered[role],
                        'install the current service configuration before starting')
                for suffix in ('out', 'err'):
                    private(cfg['service_dir'] / 'logs' / (role + '.' + suffix + '.log'))
            for role in roles(cfg):
                if not current[role]['loaded']:
                    controller.start(current[role]['path'])
        else:
            for role in ('recovery', 'gateway', 'monitor', 'awake'):
                if current[role]['loaded']:
                    controller.stop(label(cfg, role))
            if action == 'uninstall':
                commit_plists(cfg, launch_dir, {})
    return {'action': action, 'state_preserved': True, 'warning': WARNING}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=WARNING)
    parser.add_argument('action', choices=('render', 'validate', 'install', 'start', 'status', 'stop', 'uninstall', 'run-role'))
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--role', choices=ROLES)
    parser.add_argument('--apply', action='store_true', help='authorize launchd/filesystem host changes, never GPU cleanup')
    args = parser.parse_args(argv)
    if (args.action == 'run-role') != (args.role is not None):
        parser.error('--role is required only for run-role')
    if args.apply and args.action in ('run-role', 'render', 'validate', 'status'):
        parser.error('--apply is only for host-changing commands')
    if args.action in ('install', 'start', 'stop', 'uninstall') and not args.apply:
        parser.error('host-changing commands require --apply')
    cfg = load_config(args.config)
    if args.action == 'run-role':
        return run_role(cfg, args.role)
    if args.action == 'render':
        result = {'plists': {role: payload.decode() for role, payload in render(cfg).items()}, 'warning': WARNING}
    elif args.action == 'validate':
        result = {'valid': True, 'roles': roles(cfg), 'warning': WARNING}
    else:
        result = manage(cfg, args.action, args.apply)
    print(json.dumps(result, sort_keys=True))
    return 0
