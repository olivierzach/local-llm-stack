import json
import os
from pathlib import Path
import sys
import subprocess
import threading
from http.server import ThreadingHTTPServer

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway, legacy_gateway
from spark_cluster.cli import save_json
from test_cluster_gateway import Backend, KEY


@pytest.fixture
def running(tmp_path):
    servers, threads = [], []
    for _ in range(2):
        backend = ThreadingHTTPServer(('127.0.0.1', 0), Backend)
        backend.received = []
        servers.append(backend)
    legacy, moved = servers
    old_base, new_base = [f'http://127.0.0.1:{s.server_port}' for s in servers]
    registry = {'version': 1, 'routes': {'local-fast': {
        'base_url': new_base + '/v1', 'upstream_model': 'physical-model',
        'health_url': new_base + '/health', 'tokenizer_base_url': new_base,
        'context_tokens': 4096, 'max_output_tokens': 256,
        'capabilities': {'text': True, 'vision': False, 'tools': False, 'streaming': True}}}}
    path = tmp_path / 'registry.json'
    save_json(path, registry)
    config = gateway.guard.build_config(gateway.guard.parser().parse_args([]))
    config.upstream_base_url = old_base + '/v1'
    config.headroom_tokens = 128
    config.discover_model_context = False
    config.model_contexts = {'legacy-other': 8192}
    config.tokenizer_base_urls = {'legacy-other': old_base}
    server = legacy_gateway.serve(path, '127.0.0.1', 0, KEY, config)
    servers.append(server)
    for s in servers:
        t = threading.Thread(target=s.serve_forever, daemon=True)
        t.start()
        threads.append(t)
    yield f'http://127.0.0.1:{server.server_port}', legacy, moved, registry, path
    for s in servers:
        s.shutdown()
        s.server_close()
    for t in threads: t.join()


def chat(url, model, key=KEY, **kwargs):
    return requests.post(url + '/v1/chat/completions', headers={'Authorization': 'Bearer ' + key},
                         json={'model': model, 'messages': [{'role': 'user', 'content': 'hello'}],
                               'max_tokens': 128, **kwargs}, timeout=5)


def test_existing_alias_moves_without_forwarding_client_credential(running):
    url, legacy, moved, *_ = running
    response = chat(url, 'local-fast', stream=True)
    assert response.status_code == 200, response.text
    assert response.headers['X-Context-Limit'] == '4096'
    assert 'data: [DONE]' in response.text
    assert moved.received[-1]['payload']['model'] == 'physical-model'
    assert moved.received[-1]['authorization'] is None
    assert not legacy.received


def test_unregistered_alias_keeps_legacy_path_and_virtual_key(running):
    url, legacy, moved, *_ = running
    response = chat(url, 'legacy-other', key='existing-litellm-virtual-key')
    assert response.status_code == 200, response.text
    assert response.headers['X-Context-Limit'] == '8192'
    assert legacy.received[-1]['authorization'] == 'Bearer existing-litellm-virtual-key'
    assert legacy.received[-1]['payload']['model'] == 'legacy-other'
    assert not moved.received


def test_migrated_alias_never_bypasses_virtual_key_policy(running):
    url, legacy, moved, *_ = running
    assert chat(url, 'local-fast', key='existing-litellm-virtual-key').status_code == 401
    assert not legacy.received and not moved.received


def test_failed_override_does_not_run_old_model(running):
    url, legacy, moved, *_ = running
    moved.available = False
    assert chat(url, 'local-fast').status_code == 503
    assert not legacy.received and not moved.received
    listed = requests.get(url + '/v1/models', headers={'Authorization': 'Bearer ' + KEY}, timeout=5).json()
    assert 'local-fast' not in [m['id'] for m in listed['data']]


def test_registry_updates_limits_and_destination_alias_together(running):
    url, legacy, moved, registry, path = running
    assert chat(url, 'local-fast').headers['X-Context-Limit'] == '4096'
    registry['routes']['local-fast'].update(context_tokens=8192, upstream_model='replacement-model')
    save_json(path, registry)
    assert chat(url, 'local-fast').headers['X-Context-Limit'] == '8192'
    assert moved.received[-1]['payload']['model'] == 'replacement-model'
    path.write_text('{broken')
    assert chat(url, 'local-fast').status_code == 503
    assert not legacy.received


def test_disabled_mode_uses_original_entrypoint(monkeypatch):
    monkeypatch.delenv('CONTEXT_GUARD_ROUTE_REGISTRY', raising=False)
    monkeypatch.setattr(legacy_gateway.guard, 'main', lambda: 17)
    assert legacy_gateway.main() == 17


def test_native_api_and_tokenizer_follow_the_same_endpoint(monkeypatch):
    monkeypatch.setenv('DEEPSEEKV4_API_BASE', 'http://10.10.20.2:8011/v1')
    monkeypatch.delenv('CONTEXT_GUARD_TOKENIZER_BASE_URLS', raising=False)
    cfg = gateway.guard.build_config(gateway.guard.parser().parse_args([]))
    assert cfg.tokenizer_base_urls['local-deepseek-v4-flash'] == 'http://10.10.20.2:8011'


def test_compose_native_placement_keeps_router_and_tokenizer_consistent(monkeypatch):
    environment = {**os.environ, 'DEEPSEEKV4_BIND_HOST': '10.10.20.2', 'DEEPSEEKV4_API_BASE': ''}
    definition = json.loads(subprocess.check_output(
        ['docker', 'compose', 'config', '--format', 'json'], cwd=ROOT, env=environment, text=True))
    services = definition['services']
    for service in ('litellm', 'context-guard'):
        assert services[service]['environment']['DEEPSEEKV4_API_BASE'] == 'http://10.10.20.2:8011/v1'
    for key, value in services['context-guard']['environment'].items():
        if key.startswith('CONTEXT_GUARD_') or key == 'DEEPSEEKV4_API_BASE':
            monkeypatch.setenv(key, str(value))
    cfg = gateway.guard.build_config(gateway.guard.parser().parse_args([]))
    assert cfg.tokenizer_base_urls['local-deepseek-v4-flash'] == 'http://10.10.20.2:8011'
