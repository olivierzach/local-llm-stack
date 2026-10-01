"""Service ownership and foreground ingress tests; launchctl is the only fake host boundary."""
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, ProxyHandler

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config, services


class FakeLaunchctl:
    def __init__(self):
        self.jobs = {}
        self.starts = 0
        self.stops = []

    def inspect(self, label):
        return self.jobs.get(label)

    def start(self, path):
        label = plistlib.loads(path.read_bytes())['Label']
        assert label not in self.jobs
        self.jobs[label] = {'path': str(path), 'pid': 1234}
        self.starts += 1

    def stop(self, label):
        del self.jobs[label]
        self.stops.append(label)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(json.dumps(value))
    path.chmod(0o600)


@pytest.fixture
def service_setup(tmp_path):
    root = tmp_path.resolve()
    root.chmod(0o700)
    revision = 'a' * 40
    release = root / 'immutable releases' / revision
    (release / 'scripts').mkdir(parents=True)
    shutil.copytree(ROOT / 'tools/spark_cluster', release / 'tools/spark_cluster',
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    for name in ('spark-services', 'spark-recover', 'spark-monitor', 'spark-node', 'context-guard-proxy.py'):
        shutil.copyfile(ROOT / 'scripts' / name, release / 'scripts' / name)
    shutil.copyfile(ROOT / 'tools/controller-requirements.lock', release / 'tools/controller-requirements.lock')
    python = release / '.venv/bin/python'
    # Reuse installed test dependencies, not a bare interpreter masquerading as a venv.
    (release / '.venv').symlink_to(Path(sys.prefix), target_is_directory=True)
    hashes = {str(p.relative_to(release)): hashlib.sha256(p.read_bytes()).hexdigest()
              for folder in ('tools', 'scripts') for p in (release / folder).rglob('*') if p.is_file()}
    save(release / '.controller-release.json', {
        'format': 1, 'revision': revision, 'source_sha256': hashes,
        'requirements_sha256': hashes['tools/controller-requirements.lock'],
    })
    inventory = config.read(ROOT / 'cluster/inventory.json')
    for index, (node_id, node) in enumerate(inventory['nodes'].items(), 10):
        node['serving'] = {'address': '192.0.2.' + str(index), 'interface': 'management0'}
    inventory_path = root / 'inventory.json'
    save(inventory_path, inventory)
    refs = []
    for name in ('glm53-tp2-256k-dflash2-e8f1', 'coder-e8f1', 'coder-66f1'):
        _, recipe, deployment = config.load(ROOT, ROOT / 'cluster/inventory.json',
                                             ROOT / 'cluster/deployments' / (name + '.json'))
        plan = config.plan(inventory, recipe, deployment)
        path = root / (name + '.json')
        save(path, plan)
        refs.append({'path': str(path), 'sha256': config.plan_sha256(plan), 'qualification': None})
    refs[1]['node'] = 'e8f1'
    refs[2]['node'] = '66f1'
    service_dir = root / 'private services'
    ingress = service_dir / 'ingress'
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    policy = {
        'version': 1, 'id': 'cpu-service-test', 'alias': 'local-auto',
        'preferred': refs[0], 'fallbacks': refs[1:],
        'management': {node: {'ssh': 'independent-' + node} for node in inventory['nodes']},
        'gateway': {'url': 'http://127.0.0.1:' + str(port),
                    'key_file': str(ingress / 'key'), 'registry': str(ingress / 'registry.json'),
                    'route_state': str(ingress / 'route.json'), 'isolation': None},
        'timings': {},
    }
    policy_path = root / 'policy.json'
    save(policy_path, policy)
    raw = {'version': 1, 'release': str(release), 'revision': revision, 'python': str(python),
           'policy': str(policy_path), 'service_dir': str(service_dir),
           'state_dir': str(service_dir / 'recovery'), 'inventory': str(inventory_path)}
    path = root / 'services.json'
    save(path, raw)
    cfg = services.load_config(path)
    return cfg, FakeLaunchctl(), root / 'LaunchAgents'


def manage(setup, action, apply=True):
    cfg, controller, launch_dir = setup
    return services.manage(cfg, action, apply, controller=controller, launch_dir=launch_dir)


def test_rendered_foreground_gateway_authenticates_and_stays_closed(service_setup):
    cfg, controller, launch_dir = service_setup
    manage(service_setup, 'install')
    key = Path(cfg['policy_data']['gateway']['key_file']).read_text().strip()
    rendered = services.render(cfg)
    for role, payload in rendered.items():
        plist = plistlib.loads(payload)
        assert key.encode() not in payload
        assert plist['Umask'] == 0o077
        assert plist['Label'] == services.label(cfg, role)
    assert 'awake' not in rendered
    argv = plistlib.loads(rendered['gateway'])['ProgramArguments']
    env = {**os.environ, 'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONNOUSERSITE': '1'}
    process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    opener = build_opener(ProxyHandler({}))
    base = cfg['policy_data']['gateway']['url']
    try:
        deadline = time.monotonic() + 15
        while True:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                pytest.fail('foreground gateway exited: ' + stderr.decode())
            try:
                with opener.open(base + '/health', timeout=.3) as response:
                    assert response.status == 200
                break
            except (URLError, TimeoutError):
                if time.monotonic() >= deadline:
                    pytest.fail('foreground gateway did not bind its configured listener')
                time.sleep(.05)
        with pytest.raises(HTTPError) as denied:
            opener.open(base + '/_spark/recovery', timeout=2)
        assert denied.value.code == 401
        request = Request(base + '/_spark/recovery', headers={'Authorization': 'Bearer ' + key})
        with opener.open(request, timeout=2) as response:
            state = json.load(response)
        assert state['accepting'] is False
        assert state['fresh'] is False
        # No route lease exists: no backend or inference request may be dispatched.
        request = Request(base + '/v1/chat/completions', method='POST',
                          headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'},
                          data=json.dumps({'model': 'local-auto', 'messages': [{'role': 'user', 'content': 'x'}]}).encode())
        with pytest.raises(HTTPError) as unavailable:
            opener.open(request, timeout=2)
        assert unavailable.value.code == 503
        process.terminate()
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0
        assert key.encode() not in stdout + stderr
        # SIGTERM must release the actual listener, not leave an orphaned server.
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((cfg['gateway']['bind'], cfg['gateway']['port']))
    finally:
        process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
        assert key.encode() not in stdout + stderr


def test_install_start_stop_uninstall_preserve_secrets_and_highwaters(service_setup):
    cfg, controller, launch_dir = service_setup
    manage(service_setup, 'install')
    key_path = Path(cfg['policy_data']['gateway']['key_file'])
    key = key_path.read_bytes()
    journal = cfg['state_dir'] / 'journal.json'
    save(journal, {'epoch': 77, 'generation': 93, 'authority': 'preserve-on-uninstall'})
    manage(service_setup, 'install')
    assert not controller.jobs
    manage(service_setup, 'start')
    manage(service_setup, 'start')
    assert controller.starts == 3
    manage(service_setup, 'stop')
    manage(service_setup, 'stop')
    assert len(controller.stops) == 3
    assert controller.stops[0].endswith('.recovery')
    manage(service_setup, 'uninstall')
    manage(service_setup, 'uninstall')
    assert key_path.read_bytes() == key
    assert json.loads(journal.read_text())['epoch'] == 77
    assert not list(launch_dir.glob('*.plist'))
    manage(service_setup, 'install')
    assert key_path.read_bytes() == key
    assert json.loads(journal.read_text())['generation'] == 93
    assert key_path.stat().st_mode & 0o777 == 0o600


def test_foreign_labels_and_modified_plists_are_never_stopped_or_replaced(service_setup):
    cfg, controller, launch_dir = service_setup
    label = services.label(cfg, 'gateway')
    controller.jobs[label] = {'path': '/unrelated/gateway.plist', 'pid': 987}
    with pytest.raises(ValueError, match='existing launchd label'):
        manage(service_setup, 'install')
    assert not cfg['service_dir'].exists()
    controller.jobs.clear()
    manage(service_setup, 'install')
    manage(service_setup, 'start')
    path = launch_dir / (services.label(cfg, 'monitor') + '.plist')
    original = path.read_bytes()
    path.write_bytes(original + b'\n<!-- external modification -->\n')
    with pytest.raises(ValueError, match='unowned or modified'):
        manage(service_setup, 'uninstall')
    assert len(controller.jobs) == 3
    assert not controller.stops
    assert path.read_bytes() != original


def test_host_changes_require_apply_and_never_adopt_existing_ingress(service_setup):
    cfg, controller, _ = service_setup
    with pytest.raises(ValueError, match='--apply'):
        manage(service_setup, 'install', apply=False)
    assert not cfg['service_dir'].exists()
    ingress = cfg['service_dir'] / 'ingress'
    ingress.mkdir(parents=True, mode=0o700)
    cfg['service_dir'].chmod(0o700)
    existing = ingress / 'legacy-key'
    existing.write_text('never overwrite an existing gateway')
    with pytest.raises(ValueError, match='existing gateway'):
        manage(service_setup, 'install')
    assert existing.read_text() == 'never overwrite an existing gateway'
    assert not controller.jobs


def test_awake_requires_explicit_opt_in_and_cannot_remain_after_role_change(service_setup):
    cfg, controller, _ = service_setup
    with pytest.raises(ValueError, match='opt-in'):
        services.run_role(cfg, 'awake')
    cfg['awake'] = True
    manage(service_setup, 'install')
    assert plistlib.loads(services.render(cfg)['awake'])['KeepAlive'] is True
    manage(service_setup, 'start')
    cfg['awake'] = False
    with pytest.raises(ValueError, match='role set'):
        manage(service_setup, 'start')
    with pytest.raises(ValueError, match='stop owned services'):
        manage(service_setup, 'install')
    manage(service_setup, 'stop')
    manage(service_setup, 'install')
    assert not manage(service_setup, 'status')['roles']['awake']['installed']


@pytest.mark.parametrize('settings', [
    {'timeout': 150}, {'timeout': .01}, {'interval': .01}, {'interval': 3601},
])
def test_service_validation_rejects_timings_monitor_cannot_run(service_setup, settings):
    cfg, controller, _ = service_setup
    raw = json.loads(cfg['config'].read_text())
    raw['monitor'] = settings
    save(cfg['config'], raw)
    with pytest.raises(ValueError):
        services.load_config(cfg['config'])
    assert not cfg['service_dir'].exists()
    assert not controller.jobs


def test_service_validation_accepts_supported_long_sampling_interval(service_setup, capsys):
    cfg, _, _ = service_setup
    raw = json.loads(cfg['config'].read_text())
    raw['monitor'] = {'interval': 3600, 'timeout': 120}
    save(cfg['config'], raw)
    assert services.main(['validate', '--config', str(cfg['config'])]) == 0
    assert json.loads(capsys.readouterr().out)['valid'] is True
    assert not cfg['service_dir'].exists()


def test_release_tampering_and_unsafe_credentials_are_rejected(service_setup):
    cfg, _, _ = service_setup
    manage(service_setup, 'install')
    key = Path(cfg['policy_data']['gateway']['key_file'])
    key.chmod(0o644)
    with pytest.raises(ValueError, match='private paths'):
        services.load_config(cfg['config'])
    key.chmod(0o600)
    target = cfg['release'] / 'tools/spark_cluster/services.py'
    target.write_text(target.read_text() + '\n# changed after installation\n')
    with pytest.raises(ValueError, match='payload has changed'):
        services.load_config(cfg['config'])


def test_symlinked_gateway_credential_cannot_be_adopted(service_setup):
    cfg, _, _ = service_setup
    manage(service_setup, 'install')
    key = Path(cfg['policy_data']['gateway']['key_file'])
    unrelated = cfg['config'].parent / 'unrelated-secret'
    unrelated.write_bytes(key.read_bytes())
    unrelated.chmod(0o600)
    key.unlink()
    key.symlink_to(unrelated)
    with pytest.raises(ValueError):
        services.load_config(cfg['config'])
    assert unrelated.exists()


def test_public_logs_block_all_service_starts(service_setup):
    cfg, controller, _ = service_setup
    manage(service_setup, 'install')
    (cfg['service_dir'] / 'logs/monitor.err.log').chmod(0o644)
    with pytest.raises(ValueError, match='private paths'):
        manage(service_setup, 'start')
    assert not controller.jobs
    assert controller.starts == 0


@pytest.mark.parametrize('boundary', ['plist', 'manifest'])
@pytest.mark.parametrize('operation', ['install', 'uninstall'])
def test_interrupted_plist_transaction_restores_ownership(service_setup, monkeypatch, boundary, operation):
    cfg, _, launch_dir = service_setup
    manage(service_setup, 'install')
    key = Path(cfg['policy_data']['gateway']['key_file']).read_bytes()
    manifest = cfg['service_dir'] / 'installed.json'
    old_manifest = manifest.read_bytes()
    journal = cfg['state_dir'] / 'journal.json'
    old_journal = journal.read_bytes()
    cfg['awake'] = True
    original = services.replace_plist
    interrupted = False

    def fail_after_real_write(path, value):
        nonlocal interrupted
        original(path, value)
        trigger = path == manifest if boundary == 'manifest' else path.suffix == '.plist'
        if trigger and not interrupted:
            interrupted = True
            raise OSError('simulated disk failure after durable replacement')

    with monkeypatch.context() as patch:
        patch.setattr(services, 'replace_plist', fail_after_real_write)
        with pytest.raises(OSError):
            manage(service_setup, operation)
    assert interrupted
    transaction = cfg['service_dir'] / '.services-transaction.json'
    assert transaction.exists()
    assert manage(service_setup, 'status')['transaction_pending'] is True
    if boundary == 'plist':
        assert manifest.read_bytes() == old_manifest
    # Repair precedes ordinary ownership checks, even when the new manifest was
    # committed but journal removal was interrupted.
    manage(service_setup, operation)
    assert not transaction.exists()
    assert Path(cfg['policy_data']['gateway']['key_file']).read_bytes() == key
    assert journal.read_bytes() == old_journal
    status = manage(service_setup, 'status')
    assert status['installed'] is (operation == 'install')
    if operation == 'install':
        assert status['roles']['awake']['installed'] is True
        manage(service_setup, 'start')
        manage(service_setup, 'stop')
    else:
        assert not list(launch_dir.glob('*.plist'))


def test_pending_plist_repair_preserves_unknown_user_edits(service_setup, monkeypatch):
    cfg, _, launch_dir = service_setup
    original = services.replace_plist

    def fail_after_real_plist(path, value):
        original(path, value)
        if path.suffix == '.plist':
            raise OSError('interrupted after first plist')

    with monkeypatch.context() as patch:
        patch.setattr(services, 'replace_plist', fail_after_real_plist)
        with pytest.raises(OSError):
            manage(service_setup, 'install')
    path = launch_dir / (services.label(cfg, 'gateway') + '.plist')
    path.write_bytes(path.read_bytes() + b'\\n<!-- independent user change -->\\n')
    saved = path.read_bytes()
    transaction = cfg['service_dir'] / '.services-transaction.json'
    saved_transaction = transaction.read_bytes()
    with pytest.raises(ValueError, match='unknown file'):
        manage(service_setup, 'uninstall')
    assert path.read_bytes() == saved
    assert transaction.read_bytes() == saved_transaction
    assert not (cfg['service_dir'] / 'installed.json').exists()


@pytest.mark.parametrize('argv', [
    ['run-role'], ['run-role', '--role', 'gateway', '--apply'],
    ['status', '--role', 'gateway'], ['install'],
])
def test_cli_refuses_ambiguous_role_or_mutation_before_opening_config(argv, tmp_path):
    with pytest.raises(SystemExit) as rejected:
        services.main([*argv, '--config', str(tmp_path / 'not-created.json')])
    assert rejected.value.code == 2
    assert not (tmp_path / 'not-created.json').exists()
