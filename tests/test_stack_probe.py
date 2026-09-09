import importlib.util
import json
from pathlib import Path
import shutil

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('stack_probe', ROOT / 'scripts/probe-stack-models.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def request():
    return {'owner': 'accept-stack-fixture', 'digest': 'a'*64, 'node': {},
            'recipe': {'alias': 'local-fast', 'model': 'Qwen/Qwen3-test', 'image': 'sha256:'+'b'*64, 'revision': 'c'*40},
            'deployment': {'port': 8180}}


def test_failed_start_cleans_only_after_acquiring_lease(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(probe.node, 'check_port', lambda *a: None)
    def call(req):
        calls.append(req['action'])
        if req['action'] == 'start':
            raise RuntimeError('startup failed')
        return {'released': True}
    monkeypatch.setattr(probe.node, 'main', call)
    monkeypatch.setattr(probe.node, 'reservation', lambda: None)
    result = probe.exercise(request(), tmp_path / 'run', 10)
    assert calls == ['reserve', 'start', 'stop']
    assert not result['passed'] and result['cleanup_verified']
    assert json.loads((tmp_path / 'run/request.json').read_text()) == request()


def test_foreign_lease_refusal_never_calls_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(probe.node, 'check_port', lambda *a: None)
    calls = []
    def call(req):
        calls.append(req['action'])
        raise RuntimeError('foreign lease')
    monkeypatch.setattr(probe.node, 'main', call)
    result = probe.exercise(request(), tmp_path / 'run', 10)
    assert calls == ['reserve'] and not result['passed']


def test_interrupt_during_start_cleans_up_and_propagates(tmp_path, monkeypatch):
    monkeypatch.setattr(probe.node, 'check_port', lambda *a: None)
    monkeypatch.setattr(probe.node, 'reservation', lambda: None)
    calls = []
    def call(req):
        calls.append(req['action'])
        if req['action'] == 'start':
            raise SystemExit(143)
        return {'released': True}
    monkeypatch.setattr(probe.node, 'main', call)
    with pytest.raises(SystemExit):
        probe.exercise(request(), tmp_path / 'run', 10)
    assert calls == ['reserve', 'start', 'stop']


def test_probe_keeps_model_arguments_but_excludes_credentials(monkeypatch):
    service = {'command': ['python3', '-m', 'vllm.entrypoints.openai.api_server', '--model', 'test/model',
                           '--served-model-name', 'local-fast', '--host', '0.0.0.0', '--port', '8000',
                           '--max-model-len', '32768'], 'image': 'test/image', 'volumes': [],
               'environment': {'HF_TOKEN': 'secret', 'HF_HOME': '/cache', 'MAX_JOBS': '4', 'PRIVATE_KEY': 'secret'}}
    monkeypatch.setattr(probe.subprocess, 'check_output', lambda *a, **kw: json.dumps([{'Id': 'sha256:'+'b'*64}]))
    r = probe.build_request(Path('/stack'), {'services': {'vllm-fast': service}},
                            {'repo': 'test/model', 'revision': 'c'*40, 'alias': 'local-fast'}, {}, 8180, 'fixture')
    worker = r['compose']['services']['worker']
    assert 'secret' not in json.dumps(r)
    assert worker['environment']['HF_HUB_OFFLINE'] == '1'
    assert worker['command'][-2:] == ['--revision', 'c'*40]
    assert worker['command'][worker['command'].index('--max-model-len')+1] == '32768'
    assert worker['restart'] == 'no' and worker['labels']['io.spark.owner'] == r['owner']


@pytest.mark.skipif(not shutil.which('docker'), reason='requires Compose CLI, no Docker daemon or GPU')
def test_sweep_renders_optional_profiles_without_starting_containers(tmp_path):
    (tmp_path / 'compose.yaml').write_text('''name: profile-render-fixture
services:
  default-model:
    image: example.invalid/unused
  optional-model:
    image: example.invalid/unused
    profiles: [optional]
''')
    assert set(probe.load_definition(tmp_path)['services']) == {'default-model', 'optional-model'}
