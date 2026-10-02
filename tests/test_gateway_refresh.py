"""CPU-only refresh transactions using the real snapshot installer and a fake Docker boundary."""
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import sys
from types import SimpleNamespace
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
import threading

import pytest
import requests

from test_cluster_gateway import Backend, KEY

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
    def setup(*, policy=False, old_helper=False, real_sources=False):
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
        base_old = {**old, refresh.LEGACY: '# pre-recovery legacy adapter\n'}
        base_new = {**new, refresh.LEGACY: '# matching legacy adapter\n'}
        if real_sources:
            historical = ROOT / 'tests/fixtures/pre_recovery_gateway'
            new = {name: (ROOT / name).read_text() for name in refresh.FILES}
            old = dict(new)
            base_old = {**old, refresh.LEGACY: (historical / 'legacy_gateway.py').read_text()}
            base_new = {**new, refresh.LEGACY: (ROOT / refresh.LEGACY).read_text()}
        for name, source in base_new.items():
            put(release / name, source.encode())
        for name in refresh.BASE_FILES:
            if name in base_old:
                put(base / name, base_old[name].encode(), 0o640)
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
        runtime = SimpleNamespace(fail_ready=False, base_restarts=[], on_base_restart=None)

        def docker(argv):
            events.append(argv)
            if argv[:3] == ['docker', 'ps', '-q']:
                return 'base-container'
            if argv == ['docker', 'inspect', 'base-container']:
                return json.dumps([{'Id': 'base-container', 'Mounts': [
                    {'Type': 'bind', 'Source': str(base / 'tools/spark_cluster'), 'Destination': '/app/spark_cluster'}]}])
            if argv == ['docker', 'restart', 'base-container']:
                runtime.base_restarts.append(tree(base))
                if runtime.on_base_restart:
                    runtime.on_base_restart()
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
                               expected_source_sha256=refresh.sha(old[refresh.GATEWAY].encode()),
                               expected_module_sha256={refresh.LEGACY: refresh.sha(base_old[refresh.LEGACY].encode())})
        node = {'hostname': installer.platform.node(), 'architecture': installer.platform.machine()}
        return SimpleNamespace(base=base, release=release, installed=installed, old=old, new=new,
                               digest=digest, args=args, node=node, events=events, containers=containers,
                               runtime=runtime, recovery=recovery, registry=registry)
    return setup


@contextmanager
def running_legacy(base, registry, monkeypatch):
    """Load the installed ingress payload afresh, as a container restart does."""
    with monkeypatch.context() as isolated:
        isolated.setattr(sys, 'dont_write_bytecode', True)
        isolated.setitem(sys.modules, 'spark_legacy_guard', sys.modules.get('spark_legacy_guard'))
        package_path = base / 'tools/spark_cluster'
        spec = importlib.util.spec_from_file_location('refresh_test_package', package_path / '__init__.py',
                                                     submodule_search_locations=[str(package_path)])
        package = importlib.util.module_from_spec(spec)
        isolated.setitem(sys.modules, spec.name, package)
        spec.loader.exec_module(package)
        for name in ('config', 'recovery_routes', 'gateway', 'legacy_gateway'):
            path = package_path / (name + '.py')
            if not path.exists():
                continue
            spec = importlib.util.spec_from_file_location(package.__name__ + '.' + name, path)
            module = importlib.util.module_from_spec(spec)
            isolated.setitem(sys.modules, spec.name, module)
            spec.loader.exec_module(module)
            setattr(package, name, module)
        guard = package.legacy_gateway.guard
        config = guard.build_config(guard.parser().parse_args([]))
        config.headroom_tokens = 128
        config.discover_model_context = False
        server = package.legacy_gateway.serve(registry, '127.0.0.1', 0, KEY, config)
        with running_http(server) as url:
            yield url


@contextmanager
def running_http(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture
def routed_backend(tmp_path):
    backend = ThreadingHTTPServer(('127.0.0.1', 0), Backend)
    backend.received = []
    backend.strict_tokenizer_model = 'physical-model'
    with running_http(backend) as url:
        registry = tmp_path / 'base-registry.json'
        put(registry, json.dumps({'version': 1, 'routes': {'local-fast': {
            'base_url': url + '/v1', 'upstream_model': 'physical-model',
            'health_url': url + '/health', 'tokenizer_base_url': url,
            'context_tokens': 4096, 'max_output_tokens': 256,
            'capabilities': {'text': True, 'vision': False, 'tools': False, 'streaming': True},
        }}}).encode())
        yield backend, registry


def assert_migrated_chat(base, registry, backend, monkeypatch, stream=False):
    before = len(backend.received)
    with running_legacy(base, registry, monkeypatch) as url:
        assert requests.get(url + '/health', timeout=5).status_code == 200
        response = requests.post(url + '/v1/chat/completions',
                                 headers={'Authorization': 'Bearer ' + KEY},
                                 json={'model': 'local-fast', 'messages': [{'role': 'user', 'content': 'hello'}],
                                       'max_tokens': 128, 'stream': stream}, timeout=5)
        assert response.status_code == 200, response.text
        assert response.headers['X-Context-Limit'] == '4096'
        assert response.headers['X-Context-Input-Tokens'] == '10'
        if stream:
            assert 'data: [DONE]' in response.text
        else:
            assert response.json()['choices'][0]['message']['content'] == 'ready'
    assert len(backend.received) == before + 1
    assert backend.received[-1]['payload']['model'] == 'physical-model'
    assert backend.received[-1]['authorization'] is None
    assert backend.tokenizer_model == 'physical-model'

def assert_mixed_ingress_failure(base, registry, backend, monkeypatch):
    """The historical adapter still serves health while routed chats disconnect."""
    before = list(backend.received)
    with running_legacy(base, registry, monkeypatch) as url:
        assert requests.get(url + '/health', timeout=5).status_code == 200
        with pytest.raises(requests.ConnectionError):
            requests.post(url + '/v1/chat/completions', headers={'Authorization': 'Bearer ' + KEY},
                          json={'model': 'local-fast', 'messages': [{'role': 'user', 'content': 'hello'}],
                                'max_tokens': 128}, timeout=5)
    assert backend.received == before



@pytest.mark.parametrize('stream', [False, True])
def test_refresh_migrates_pre_recovery_base_ingress(setup_refresh, routed_backend, monkeypatch, stream):
    case = setup_refresh(real_sources=True)
    backend, registry = routed_backend
    before = tree(case.base)
    assert_mixed_ingress_failure(case.base, registry, backend, monkeypatch)
    case.runtime.on_base_restart = lambda: assert_migrated_chat(case.base, registry, backend, monkeypatch, stream)
    report = refresh.refresh(case.args, 'test', case.node)
    assert report['passed']
    for name, (source, mode) in before.items():
        assert (case.args.output / 'base-before' / name).read_bytes() == source
        assert stat.S_IMODE((case.base / name).stat().st_mode) == mode
    assert refresh.LEGACY not in report['managed_source_sha256']
    assert refresh.sha((case.base / refresh.LEGACY).read_bytes()) == report['base_sources'][refresh.LEGACY]['after_sha256']


def test_failed_refresh_restores_pre_recovery_base_ingress(setup_refresh, routed_backend, monkeypatch):
    case = setup_refresh(real_sources=True)
    backend, registry = routed_backend
    before = tree(case.base)
    assert_mixed_ingress_failure(case.base, registry, backend, monkeypatch)

    def restarted():
        if len(case.runtime.base_restarts) == 1:
            assert_migrated_chat(case.base, registry, backend, monkeypatch)
        else:
            assert_mixed_ingress_failure(case.base, registry, backend, monkeypatch)

    case.runtime.on_base_restart = restarted
    case.runtime.fail_ready = True
    with pytest.raises(RuntimeError, match='injected mid-restart'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.base) == before
    assert json.loads((case.args.output / 'refresh.json').read_bytes())['rolled_back']
    assert_mixed_ingress_failure(case.base, registry, backend, monkeypatch)


def test_partial_base_write_failure_restores_all_dependencies(setup_refresh, monkeypatch):
    case = setup_refresh()
    before = tree(case.base)
    original_put = refresh.put

    def fail_legacy_write(path, value):
        if path == case.base / refresh.LEGACY:
            raise OSError('injected legacy source write failure')
        return original_put(path, value)

    monkeypatch.setattr(refresh, 'put', fail_legacy_write)
    with pytest.raises(OSError, match='injected legacy source write'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.base) == before
    assert not case.runtime.base_restarts
    assert json.loads((case.args.output / 'refresh.json').read_bytes())['rolled_back']


def test_unreviewed_legacy_adapter_is_not_overwritten(setup_refresh):
    case = setup_refresh()
    case.args.expected_module_sha256 = {}
    before = tree(case.base)
    with pytest.raises(RuntimeError, match='base source changed'):
        refresh.refresh(case.args, 'test', case.node)
    assert tree(case.base) == before
    assert not case.args.output.exists()
    assert not case.runtime.base_restarts


def test_refresh_stages_complete_sources_before_restart(setup_refresh, monkeypatch):
    case = setup_refresh()
    report = refresh.refresh(case.args, 'test', case.node)
    saved = json.loads((case.installed / 'state.json').read_bytes())
    snapshot = case.installed / saved['digest']
    for name, source in case.new.items():
        assert (snapshot / name).read_bytes() == source.encode()
    assert refresh.LEGACY not in report['managed_source_sha256']
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


@pytest.mark.parametrize('name', refresh.BASE_FILES)
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
    case.args.expected_module_sha256[refresh.HELPER] = refresh.sha((case.base / refresh.HELPER).read_bytes())
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
