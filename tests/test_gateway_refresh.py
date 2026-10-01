"""CPU-only refresh transactions using the real snapshot installer and a fake Docker boundary."""
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('gateway_refresh', ROOT / 'scripts/refresh-spark-gateway-policy.py')
refresh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh)
installer = refresh.gateway_node


def put(path, data, mode=0o644):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(mode)


def tree(path):
    return {str(p.relative_to(path)): (p.read_bytes(), stat.S_IMODE(p.stat().st_mode))
            for p in path.rglob('*') if p.is_file()}


@pytest.fixture
def setup_refresh(tmp_path, monkeypatch):
    def setup(*, policy=False, old_helper=False):
        base, release, installed = (tmp_path / name for name in ('base', 'release', 'installed'))
        old = {
            refresh.FILES[0]: 'PROXY = "old"\r\n',
            refresh.GATEWAY: 'VERSION = "old"\r\n',
            'tools/spark_cluster/config.py': 'VERSION = "old-config"\r\n',
            'tools/spark_cluster/__init__.py': '',
        }
        if old_helper:
            old[refresh.HELPER] = 'TOKEN = "old-helper"\r\n'
            old[refresh.GATEWAY] = 'from .recovery_routes import TOKEN\r\nVERSION = "old"\r\n'
        new = {**old, refresh.HELPER: 'TOKEN = "new-helper"\n',
               refresh.GATEWAY: 'from .recovery_routes import TOKEN\nfrom .config import VERSION\n',
               'tools/spark_cluster/config.py': 'VERSION = "new-config"\n',
               refresh.FILES[0]: 'PROXY = "new"\n'}
        for name, source in new.items():
            put(release / name, source.encode())
        for name in refresh.BASE_FILES:
            if name in old:
                put(base / name, old[name].encode(), 0o640)
        definition = {'files': old, 'port': 4110, 'image': 'python:test'}
        recovery = None
        if policy:
            recovery = tmp_path / 'private' / 'route.json'
            recovery.parent.mkdir(mode=0o700)
            put(recovery, b'{"policy":"preserved"}\n', 0o600)
            put(recovery.with_name('route.json.fence.json'), b'{"generation":7}\n', 0o600)
            definition['recovery_state'] = str(recovery)
        digest = refresh.sha(json.dumps(definition, sort_keys=True, separators=(',', ':')).encode())
        for name, source in old.items():
            put(installed / digest / name, source.encode())
        registry = {'version': 1, 'routes': {'local-fast': {'base_url': 'http://127.0.0.1:8000/v1'}}}
        put(installed / 'config/registry.json', (json.dumps(registry) + '\n').encode(), 0o640)
        put(installed / 'api-key', b'test-only-original-key-0000000000\n', 0o600)
        put(installed / 'gateway.env', b'SPARK_GATEWAY_KEY=test-only-original-key-0000000000\n', 0o600)
        put(installed / 'config/credentials.json', b'{"PROVIDER_KEY":"not-touched"}\n', 0o600)
        name = 'spark-gateway-' + digest[:12]
        saved = {'name': name, 'digest': digest, 'port': 4110, 'id': 'old-container'}
        put(installed / 'state.json', json.dumps(saved).encode(), 0o600)
        mounts = []
        cmd = ['-m', 'spark_cluster.gateway']
        user = ''
        if recovery:
            mounts = [{'Type': 'bind', 'Source': str(recovery.parent), 'Destination': '/recovery', 'RW': True}]
            cmd += ['--recovery-state', '/recovery/' + recovery.name]
            user = f'{os.getuid()}:{os.getgid()}'
        container = {'Id': saved['id'], 'Image': 'sha256:actual-image', 'State': {'Running': True},
                     'Config': {'Image': definition['image'], 'Labels': {'io.spark.gateway': digest},
                                'Cmd': cmd, 'User': user}, 'Mounts': mounts,
                     'HostConfig': {'NetworkMode': 'host', 'Init': True, 'RestartPolicy': {'Name': 'unless-stopped'}}}
        containers = {name: container}
        events = []
        runtime = SimpleNamespace(fail_ready=False, base_restarts=[])

        def docker(argv):
            events.append(argv)
            if argv[:3] == ['docker', 'ps', '-q']:
                return 'base-container'
            if argv == ['docker', 'inspect', 'base-container']:
                return json.dumps([{'Id': 'base-container', 'Mounts': [
                    {'Type': 'bind', 'Source': str(base / 'tools/spark_cluster'), 'Destination': '/app/spark_cluster'}]}])
            if argv == ['docker', 'restart', 'base-container']:
                runtime.base_restarts.append(tree(base))
                return 'base-container'
            if argv[:3] == ['docker', 'image', 'inspect']:
                return 'sha256:actual-image'
            if argv[:3] == ['docker', 'rm', '-f']:
                for key, value in list(containers.items()):
                    if value['Id'] == argv[3]:
                        del containers[key]
                return ''
            if argv[:2] == ['docker', 'create']:
                new_name = argv[argv.index('--name') + 1]
                new_digest = argv[argv.index('--label') + 1].split('=', 1)[1]
                image_index = argv.index('--entrypoint') + 2
                new_mounts = []
                for i, value in enumerate(argv):
                    if value == '--mount':
                        opts = dict(item.split('=', 1) for item in argv[i + 1].split(',') if '=' in item)
                        new_mounts.append({'Type': opts['type'], 'Source': opts['src'],
                                           'Destination': opts['dst'], 'RW': 'readonly' not in argv[i + 1]})
                containers[new_name] = {
                    'Id': 'created-' + new_digest, 'Image': 'sha256:actual-image', 'State': {'Running': False},
                    'Config': {'Image': argv[image_index], 'Labels': {'io.spark.gateway': new_digest},
                               'Cmd': argv[image_index + 1:], 'User': argv[argv.index('--user') + 1] if '--user' in argv else ''},
                    'Mounts': new_mounts,
                    'HostConfig': {'NetworkMode': argv[argv.index('--network') + 1], 'Init': '--init' in argv,
                                   'RestartPolicy': {'Name': argv[argv.index('--restart') + 1]}}}
                return containers[new_name]['Id']
            if argv[:2] == ['docker', 'start']:
                containers[argv[2]]['State']['Running'] = True
                return argv[2]
            pytest.fail(f'unexpected Docker operation: {argv}')

        def ready(saved, **kwargs):
            if runtime.fail_ready:
                runtime.fail_ready = False
                raise RuntimeError('injected mid-restart readiness failure')
            assert containers[saved['name']]['State']['Running']

        monkeypatch.setattr(refresh, 'ROOT', release)
        monkeypatch.setattr(installer, 'ROOT', installed)
        monkeypatch.setattr(installer, 'lookup', lambda name: containers.get(name))
        monkeypatch.setattr(installer, 'run', docker)
        monkeypatch.setattr(installer, 'await_ready', ready)
        monkeypatch.setattr(installer.urllib.request, 'build_opener', lambda *a: SimpleNamespace(
            open=lambda *a, **kw: io.BytesIO(b'{"data": []}')))
        monkeypatch.setattr(refresh.subprocess, 'run', lambda *a, **kw: SimpleNamespace(returncode=0))
        args = SimpleNamespace(root=base, output=tmp_path / 'receipt', apply=True,
                               expected_source_sha256=refresh.sha(old[refresh.GATEWAY].encode()), expected_module_sha256={})
        node = {'hostname': installer.platform.node(), 'architecture': installer.platform.machine()}
        return SimpleNamespace(base=base, release=release, installed=installed, old=old, new=new,
                               digest=digest, args=args, node=node, events=events, containers=containers,
                               runtime=runtime, recovery=recovery, registry=registry)
    return setup


def load_gateway(snapshot, monkeypatch):
    """Exercise relative imports from the on-disk bundle, not the controller modules."""
    package_path = snapshot / 'tools/spark_cluster'
    package = importlib.util.spec_from_file_location('refresh_test_package', package_path / '__init__.py',
                                                   submodule_search_locations=[str(package_path)])
    module = importlib.util.module_from_spec(package)
    monkeypatch.setitem(sys.modules, package.name, module)
    package.loader.exec_module(module)
    for name in ('config', 'recovery_routes', 'gateway'):
        spec = importlib.util.spec_from_file_location(package.name + '.' + name, package_path / (name + '.py'))
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
    return module


def test_refresh_stages_complete_sources_before_restart(setup_refresh, monkeypatch):
    case = setup_refresh()
    report = refresh.refresh(case.args, 'test', case.node)
    saved = json.loads((case.installed / 'state.json').read_bytes())
    snapshot = case.installed / saved['digest']
    for name, source in case.new.items():
        assert (snapshot / name).read_bytes() == source.encode()
    assert load_gateway(snapshot, monkeypatch).TOKEN == 'new-helper'
    assert load_gateway(case.base, monkeypatch).VERSION == 'new-config'
    assert case.runtime.base_restarts[0][refresh.HELPER][0] == case.new[refresh.HELPER].encode()
    assert stat.S_IMODE((case.base / refresh.GATEWAY).stat().st_mode) == 0o640
    assert not (case.installed / case.digest / refresh.HELPER).exists()
    assert refresh.HELPER not in report['old_snapshot_source_sha256']
    assert report['rollback_added_sources'][refresh.HELPER]['sha256'] == refresh.sha(case.new[refresh.HELPER].encode())
    assert json.loads((case.installed / 'config/registry.json').read_bytes()) == case.registry
    assert (case.installed / 'api-key').read_text().strip() == 'test-only-original-key-0000000000'


@pytest.mark.parametrize('policy', [False, True])
def test_mid_restart_failure_restores_sources_and_fencing(setup_refresh, monkeypatch, policy):
    case = setup_refresh(policy=policy, old_helper=policy)
    before_base = tree(case.base)
    before_snapshot = tree(case.installed / case.digest)
    before_controls = {name: refresh.capture(case.installed / name) for name in (
        'api-key', 'gateway.env', 'config/registry.json', 'config/credentials.json')}
    before_recovery = tree(case.recovery.parent) if policy else None
    case.runtime.fail_ready = True
    with pytest.raises(RuntimeError, match='injected mid-restart'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.base) == before_base
    assert tree(case.installed / case.digest) == before_snapshot
    assert {name: refresh.capture(case.installed / name) for name in before_controls} == before_controls
    saved = json.loads((case.installed / 'state.json').read_bytes())
    rollback = case.installed / saved['digest']
    assert load_gateway(rollback, monkeypatch).VERSION == 'old'
    assert {name: (rollback / name).read_bytes() for name in case.old} == {name: value.encode() for name, value in case.old.items()}
    assert (rollback / refresh.HELPER).is_file()
    receipt = json.loads((case.args.output / 'refresh.json').read_bytes())
    assert receipt['rolled_back'] is True
    assert receipt['rollback_snapshot_digest'] == saved['digest']
    assert receipt['old_snapshot_digest'] == case.digest
    assert case.containers[saved['name']]['Config']['Image'] == 'sha256:actual-image'
    assert case.runtime.base_restarts[-1] == before_base
    if policy:
        assert tree(case.recovery.parent) == before_recovery
        container = case.containers[saved['name']]
        assert refresh.recovery_state(saved, container) == str(case.recovery)
        assert receipt['rollback_added_sources'] == {}
    else:
        assert not (case.base / refresh.HELPER).exists()
        assert saved['digest'] != case.digest
        assert refresh.HELPER in receipt['rollback_added_sources']


def test_policy_refresh_keeps_private_mount_identity_and_lifecycle(setup_refresh):
    case = setup_refresh(policy=True, old_helper=True)
    before = tree(case.recovery.parent)
    refresh.refresh(case.args, 'test', case.node)
    saved = json.loads((case.installed / 'state.json').read_bytes())
    assert refresh.recovery_state(saved, case.containers[saved['name']]) == str(case.recovery)
    assert tree(case.recovery.parent) == before


@pytest.mark.parametrize('name', [refresh.GATEWAY, refresh.HELPER, 'tools/spark_cluster/config.py'])
def test_stale_base_source_is_not_overwritten(setup_refresh, name):
    case = setup_refresh()
    put(case.base / name, b'UNRELATED = "user work"\n', 0o600)
    before_base, before_installed = tree(case.base), tree(case.installed)
    with pytest.raises(RuntimeError, match='base.*source changed'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.base) == before_base
    assert tree(case.installed) == before_installed
    assert not case.args.output.exists()
    assert not case.runtime.base_restarts


def test_reviewed_dependency_hash_allows_explicit_change(setup_refresh):
    case = setup_refresh()
    put(case.base / refresh.HELPER, b'TOKEN = "reviewed-existing-helper"\n')
    case.args.expected_module_sha256 = {refresh.HELPER: refresh.sha((case.base / refresh.HELPER).read_bytes())}
    report = refresh.refresh(case.args, 'test', case.node)
    assert (case.base / refresh.HELPER).read_bytes() == case.new[refresh.HELPER].encode()
    assert report['base_sources'][refresh.HELPER]['before_sha256'] == case.args.expected_module_sha256[refresh.HELPER]


def test_dry_run_leaves_sources_and_snapshot_untouched(setup_refresh):
    case = setup_refresh(policy=True, old_helper=True)
    case.args.apply = False
    before_base, before_installed, before_recovery = tree(case.base), tree(case.installed), tree(case.recovery.parent)
    report = refresh.refresh(case.args, 'test', case.node)
    assert not report['applied']
    assert report['recovery_state'] == str(case.recovery)
    assert tree(case.base) == before_base
    assert tree(case.installed) == before_installed
    assert tree(case.recovery.parent) == before_recovery
    assert not case.args.output.exists()
    assert not case.runtime.base_restarts
    assert all(event[1] in ('ps', 'inspect') for event in case.events)


@pytest.mark.parametrize('kind', ['module', 'snapshot', 'output'])
def test_refresh_refuses_symlink_paths(setup_refresh, tmp_path, kind):
    case = setup_refresh()
    if kind == 'module':
        target = case.base / refresh.HELPER
        target.symlink_to(case.release / refresh.HELPER)
    elif kind == 'snapshot':
        target = case.installed / case.digest / refresh.GATEWAY
        target.unlink()
        target.symlink_to(case.base / refresh.GATEWAY)
    else:
        case.args.output.symlink_to(tmp_path / 'not-created')
    with pytest.raises(RuntimeError, match='symlink'):
        refresh.refresh(case.args, 'test', case.node)
    assert not case.runtime.base_restarts


def test_changed_snapshot_refused_before_mutation(setup_refresh):
    case = setup_refresh()
    put(case.installed / case.digest / 'tools/spark_cluster/config.py', b'VERSION = "tampered"\n')
    before = tree(case.installed)
    with pytest.raises(RuntimeError, match='recorded digest'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.installed) == before
    assert not case.args.output.exists()
