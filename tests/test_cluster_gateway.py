import json
import io
from pathlib import Path
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import gateway, gateway_node, config
from spark_cluster.cli import save_json

KEY = "test-only-gateway-key-0000000000000000"


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def reply(self, body):
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        if not getattr(self.server,'available',True):
            self.send_error(503)
        elif self.path == '/v1/models':
            self.reply({'data':[{'id':getattr(self.server,'model_alias','local-fast'),
                'root':getattr(self.server,'model_root','/cache/test'), 'max_model_len':8192}]})
        else: self.reply({"status": "ok"})
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/tokenize":
            self.server.tokenizer_model = body.get('model')
            if getattr(self.server, 'strict_tokenizer_model', None) and body.get('model') != self.server.strict_tokenizer_model:
                self.send_error(400)
                return
            self.reply({"count": 10})
            return
        self.server.received.append({"payload": body, "authorization": self.headers.get("Authorization")})
        if getattr(self.server,'entered',None):
            self.server.entered.set()
            self.server.unblock.wait(timeout=5)
        if getattr(self.server,'fail_completion',False):
            self.send_error(503)
            return
        if body.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b'data: {"choices":[{"delta":{"content":"ready"}}]}\n\ndata: [DONE]\n\n')
        else:
            self.reply({"model": body["model"], "choices": [{"message": {"role": "assistant", "content": "ready"}}]})


@pytest.fixture
def running(tmp_path):
    backend = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    backend.received = []
    thread = threading.Thread(target=backend.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{backend.server_port}"
    registry = {"version": 1, "routes": {"local-fast": {
        "base_url": base + "/v1", "upstream_model": "local-fast", "health_url": base + "/health",
        "tokenizer_base_url": base, "context_tokens": 8192, "max_output_tokens": 256,
        "capabilities": {"text": True, "vision": False, "tools": False, "streaming": True}}}}
    path = tmp_path / "registry.json"
    save_json(path, registry)
    server = gateway.serve(path, "127.0.0.1", 0, KEY)
    backend.gateway = server
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    yield f"http://127.0.0.1:{server.server_port}", backend, registry, path
    server.shutdown()
    backend.shutdown()
    server.server_close()
    backend.server_close()
    worker.join()
    thread.join()


def chat(url, **extra):
    return requests.post(url + "/v1/chat/completions", headers={"Authorization": "Bearer " + KEY},
        json={"model": "local-fast", "messages": [{"role": "user", "content": "hello"}], **extra}, timeout=5)


def test_requires_real_authentication(running):
    url, backend, *_ = running
    r = requests.get(url + "/v1/models", headers={"Authorization": "Bearer wrong"}, timeout=5)
    assert r.status_code == 401
    assert not backend.received


def test_guarded_completion_uses_declared_limits(running):
    url, backend, *_ = running
    r = chat(url, max_tokens=4096)
    assert r.status_code == 200, r.text
    assert r.headers["X-Context-Limit"] == "8192"
    assert backend.received[-1]["payload"]["max_tokens"] <= 256
    assert backend.received[-1]["authorization"] is None  # Never leak the client gateway key.


def test_streaming_preserved(running):
    r = chat(running[0], stream=True)
    assert r.status_code == 200, r.text
    assert "text/event-stream" in r.headers["Content-Type"]
    assert '"content":"ready"' in r.text and "data: [DONE]" in r.text


def test_route_and_policy_change_together(running):
    url, backend, registry, path = running
    assert chat(url).headers["X-Context-Limit"] == "8192"
    registry["routes"]["local-fast"].update(context_tokens=4096, max_output_tokens=128, upstream_model="changed-model")
    save_json(path, registry)
    r = chat(url, max_tokens=1000)
    assert r.headers["X-Context-Limit"] == "4096"
    assert backend.received[-1]["payload"]["model"] == "changed-model"
    assert backend.received[-1]["payload"]["max_tokens"] <= 128


def test_long_context_timeouts_follow_route_and_do_not_mutate_defaults(running, monkeypatch):
    url, backend, registry, path = running
    route = registry['routes']['local-fast']
    route.update(upstream_model='different-model', request_timeout_s=3600, tokenizer_timeout_s=180)
    save_json(path, registry)
    original = requests.post
    observed = []
    def post(target, **kwargs):
        if target.startswith(route['tokenizer_base_url']):
            observed.append((target, kwargs.get('timeout')))
        return original(target, **kwargs)
    monkeypatch.setattr(requests, 'post', post)
    assert chat(url).status_code == 200
    assert any(target.endswith('/tokenize') and timeout == 180 for target, timeout in observed)
    assert any(target.endswith('/chat/completions') and timeout == 3600 for target, timeout in observed)
    assert 'different-model' not in backend.gateway.config.model_timeouts
    assert backend.gateway.config.tokenizer_timeout_s == 3
    del route['request_timeout_s'], route['tokenizer_timeout_s']
    save_json(path, registry)
    observed.clear()
    assert chat(url).status_code == 200
    assert any(target.endswith('/tokenize') and timeout == 3 for target, timeout in observed)
    assert any(target.endswith('/chat/completions') and timeout == backend.gateway.config.timeout_s
               for target, timeout in observed)


@pytest.mark.parametrize('field,value', [('request_timeout_s', 3601), ('request_timeout_s', 0),
    ('request_timeout_s', True), ('tokenizer_timeout_s', 181), ('tokenizer_timeout_s', '180')])
def test_invalid_route_timeout_fails_closed(running, field, value):
    url, backend, registry, path = running
    registry['routes']['local-fast'][field] = value
    save_json(path, registry)
    assert chat(url).status_code == 503
    assert not backend.received


def test_unhealthy_route_fails_without_fallback(running, monkeypatch):
    monkeypatch.setattr(gateway, "live", lambda _: False)
    r = chat(running[0])
    assert r.status_code == 503
    assert not running[1].received


def test_invalid_registry_fails_closed(running):
    running[3].write_text('{"version":')
    assert chat(running[0]).status_code == 503
    assert not running[1].received


@pytest.mark.parametrize("extra", [
    {"tools": [{"type": "function", "function": {"name": "test"}}]},
    {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,test"}}]}]},
])
def test_unsupported_capabilities_rejected(running, extra):
    r = chat(running[0], **extra)
    assert r.status_code == 400
    assert not running[1].received


def test_provider_secret_resolved_server_side(running, monkeypatch):
    url, backend, registry, path = running
    registry["routes"]["local-fast"]["upstream_key_env"] = "TEST_UPSTREAM_KEY"
    save_json(path, registry)
    monkeypatch.delenv("TEST_UPSTREAM_KEY", raising=False)
    assert chat(url).status_code == 503
    monkeypatch.setenv("TEST_UPSTREAM_KEY", "upstream-test-only")
    assert chat(url).status_code == 200
    assert backend.received[-1]["authorization"] == "Bearer upstream-test-only"


@pytest.fixture
def replicas(running):
    url,first,registry,path=running
    second=ThreadingHTTPServer(('127.0.0.1',0),Backend)
    second.received=[]
    thread=threading.Thread(target=second.serve_forever,daemon=True)
    thread.start()
    route=registry['routes']['local-fast']
    route.update(model_root='/cache/test',deployment_digest='a'*64)
    other=f'http://127.0.0.1:{second.server_port}'
    route['replicas']=[{k:route[k] for k in ('base_url','health_url','tokenizer_base_url','deployment_digest')},
        {'base_url':other+'/v1','health_url':other+'/health','tokenizer_base_url':other,'deployment_digest':'b'*64}]
    save_json(path,registry)
    yield url,first,second,registry,path
    if getattr(first,'unblock',None): first.unblock.set()
    second.shutdown()
    second.server_close()
    thread.join()


def test_replica_routing_rotates_and_preserves_client_contract(replicas):
    url,first,second,*_=replicas
    replies=[chat(url) for _ in range(4)]
    assert all(r.status_code==200 for r in replies)
    assert [r.headers['X-Spark-Deployment'] for r in replies]==['a'*64,'b'*64]*2
    assert len(first.received)==len(second.received)==2
    assert all(r.headers['X-Context-Limit']=='8192' for r in replies)
    assert first.gateway.replica_pool.active=={}


def test_busy_replica_keeps_lease_for_stream_and_other_requests_use_peer(replicas):
    url,first,second,*_=replicas
    first.entered=threading.Event()
    first.unblock=threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow=pool.submit(chat,url,stream=True)
        assert first.entered.wait(3)
        for _ in range(2):
            r=chat(url)
            assert r.status_code==200 and r.headers['X-Spark-Deployment']=='b'*64
        assert sum(first.gateway.replica_pool.active.values())==1
        first.unblock.set()
        assert '[DONE]' in slow.result(timeout=5).text
    assert first.gateway.replica_pool.active=={}


def test_unhealthy_or_wrong_model_replica_is_excluded(replicas):
    url,first,second,*_=replicas
    first.model_root='/cache/another-model'
    assert chat(url).headers['X-Spark-Deployment']=='b'*64
    second.available=False
    assert chat(url).status_code==503
    r=requests.get(url+'/v1/models',headers={'Authorization':'Bearer '+KEY},timeout=5)
    assert r.json()['data']==[]
    assert not first.received


def test_failed_generation_is_not_replayed_on_another_replica(replicas):
    url,first,second,*_=replicas
    first.fail_completion=True
    assert chat(url).status_code==503
    assert len(first.received)==1 and not second.received
    assert first.gateway.replica_pool.active=={}


def test_registry_change_does_not_move_an_inflight_request(replicas):
    url,first,second,registry,path=replicas
    first.entered=threading.Event()
    first.unblock=threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        old=pool.submit(chat,url)
        assert first.entered.wait(3)
        registry['routes']['local-fast']['context_tokens']=4096
        save_json(path,registry)
        first.unblock.set()
        assert old.result(timeout=5).headers['X-Context-Limit']=='8192'
    # Neither backend advertises the newly declared context; new traffic fails closed.
    assert chat(url).status_code==503


def test_replica_plan_grouping_is_explicit_and_checks_pinned_recipe():
    inv,recipe,deployment=config.load(ROOT,ROOT/'cluster/inventory.json',ROOT/'cluster/deployments/fast-e8f1.json')
    first=config.plan(inv,recipe,deployment)
    other={**deployment,'name':'fast-66f1','nodes':['66f1'],'coordinator':'66f1'}
    second=config.plan(inv,recipe,other)
    with pytest.raises(ValueError,match='duplicate alias'): gateway.from_plans([first,second])
    grouped=gateway.from_plans([first,second],replicas=True)
    assert len(grouped['routes']['local-fast']['replicas'])==2
    changed=config.plan(inv,{**recipe,'revision':'a'*40},other)
    with pytest.raises(ValueError,match='identical pinned recipe'): gateway.from_plans([first,changed],replicas=True)
    with pytest.raises(ValueError,match='duplicate replica'): gateway.from_plans([first,first],replicas=True)


def test_gateway_start_waits_for_the_same_process(monkeypatch):
    observations=[]
    monkeypatch.setattr(gateway_node,'checked',lambda saved:observations.append(saved) or {'State':{'Running':True}})
    monkeypatch.setattr(gateway_node,'run',lambda *a:pytest.fail('readiness must not relaunch anything'))
    class Opener:
        calls=0
        def open(self,*a,**kw):
            self.calls+=1
            if self.calls==1: raise OSError('starting')
            return io.BytesIO(b'{"status":"ok"}')
    opener=Opener()
    monkeypatch.setattr(gateway_node.urllib.request,'build_opener',lambda *a:opener)
    monkeypatch.setattr(gateway_node.time,'sleep',lambda n:None)
    saved={'port':4110,'id':'original'}
    gateway_node.await_ready(saved)
    assert observations==[saved,saved]


def test_gateway_start_refuses_terminal_process_without_restart(monkeypatch):
    monkeypatch.setattr(gateway_node,'checked',lambda saved:{'State':{'Running':False}})
    monkeypatch.setattr(gateway_node,'run',lambda *a:pytest.fail('terminal process must not be relaunched'))
    with pytest.raises(RuntimeError,match='retained'): gateway_node.await_ready({'port':4110})


def test_attach_requires_owned_gateway_and_does_not_change_service(tmp_path, monkeypatch):
    root = tmp_path / 'gateway'
    (root / 'config').mkdir(parents=True)
    saved = {'name': 'gateway-test', 'id': 'original', 'digest': 'abc', 'port': 4110}
    registry = {'version': 1, 'routes': {}}
    (root / 'state.json').write_text(json.dumps(saved))
    (root / 'api-key').write_text(KEY)
    (root / 'config/registry.json').write_text(json.dumps(registry))
    monkeypatch.setattr(gateway_node, 'ROOT', root)
    monkeypatch.setattr(gateway_node, 'run', lambda *a: pytest.fail('attach must not mutate Docker'))
    container = {'Id': 'original', 'Config': {'Labels': {'io.spark.gateway': 'abc'}}}
    monkeypatch.setattr(gateway_node, 'lookup', lambda _: container)
    request = {'action': 'attach', 'node': {'hostname': gateway_node.platform.node(),
                                          'architecture': gateway_node.platform.machine()}}
    result = gateway_node.main(request)
    assert result == {'api_key': KEY, 'registry': registry, 'port': 4110}
    assert json.loads((root / 'state.json').read_text()) == saved
    container['Id'] = 'somebody-elses-container'
    with pytest.raises(RuntimeError, match='ownership mismatch'):
        gateway_node.main(request)


def test_attach_stores_private_key_without_returning_it_and_preserves_conflicts(tmp_path):
    import runpy
    attach = runpy.run_path(str(ROOT / 'scripts/spark-gateway'))['store_attachment']
    result = {'api_key': KEY, 'registry': {'version': 1, 'routes': {}}, 'port': 4110}
    output = tmp_path / 'controller'
    report = attach(output, result)
    assert KEY not in json.dumps(report)
    assert (output / 'api-key').stat().st_mode & 0o777 == 0o600
    assert (output / 'api-key').read_text() == KEY
    before = (output / 'registry.json').read_bytes()
    with pytest.raises(RuntimeError, match='preserved'):
        attach(output, {**result, 'api_key': 'different-private-key-' * 3})
    assert (output / 'api-key').read_text() == KEY
    assert (output / 'registry.json').read_bytes() == before


def test_managed_provider_credentials_rotate_remove_and_bind_destination(running, monkeypatch):
    url, backend, registry, path = running
    route = registry['routes']['local-fast']
    route.update(upstream_key_env='TEST_CLOUD_KEY', upstream_model='provider/model')
    save_json(path, registry)
    credentials = path.with_name('credentials.json')
    def models():
        return requests.get(url+'/v1/models', headers={'Authorization': 'Bearer '+KEY}, timeout=5).json()['data']
    assert models() == []
    assert chat(url).status_code == 503 and not backend.received
    save_json(credentials, {'TEST_CLOUD_KEY': {'base_url': route['base_url'], 'key': 'first-provider-key'}})
    assert len(models()) == 1
    assert chat(url).status_code == 200
    assert backend.received[-1]['payload']['model'] == 'provider/model'
    assert backend.received[-1]['authorization'] == 'Bearer first-provider-key'
    save_json(credentials, {'TEST_CLOUD_KEY': {'base_url': route['base_url'], 'key': 'second-provider-key'}})
    assert chat(url, stream=True).status_code == 200
    assert backend.received[-1]['authorization'] == 'Bearer second-provider-key'
    save_json(credentials, {'TEST_CLOUD_KEY': {'base_url': 'https://another-provider.invalid/v1', 'key': 'second-provider-key'}})
    count = len(backend.received)
    assert models() == []
    assert chat(url).status_code == 503 and len(backend.received) == count
    monkeypatch.setenv('TEST_CLOUD_KEY', 'legacy-must-not-return')
    save_json(credentials, {'TEST_CLOUD_KEY': None})
    assert models() == []
    assert chat(url).status_code == 503 and len(backend.received) == count


def test_provider_rotation_does_not_change_an_active_request(running):
    url, backend, registry, path = running
    route = registry['routes']['local-fast']
    route['upstream_key_env'] = 'TEST_CLOUD_KEY'
    save_json(path, registry)
    credentials = path.with_name('credentials.json')
    save_json(credentials, {'TEST_CLOUD_KEY': {'base_url': route['base_url'], 'key': 'first-provider-key'}})
    backend.entered = threading.Event()
    backend.unblock = threading.Event()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(chat, url, stream=True)
            assert backend.entered.wait(3)
            save_json(credentials, {'TEST_CLOUD_KEY': None})
            assert chat(url).status_code == 503
            assert backend.received[0]['authorization'] == 'Bearer first-provider-key'
            backend.unblock.set()
            assert request.result(timeout=5).status_code == 200
    finally:
        backend.unblock.set()


def test_corrupt_credentials_fail_closed_for_provider_without_breaking_local(running):
    url, backend, registry, path = running
    path.with_name('credentials.json').write_text('{"credential":broken')
    assert chat(url).status_code == 200
    registry['routes']['local-fast']['upstream_key_env'] = 'TEST_CLOUD_KEY'
    save_json(path, registry)
    count = len(backend.received)
    assert chat(url).status_code == 503 and len(backend.received) == count


def test_provider_credential_operations_are_scoped_private_and_redacted(tmp_path, monkeypatch):
    root = tmp_path / 'gateway'
    (root / 'config').mkdir(parents=True)
    registry = {'routes': {'cloud': {'base_url': 'https://openrouter.ai/api/v1', 'upstream_key_env': 'OPENROUTER_API_KEY'}}}
    (root / 'config/registry.json').write_text(json.dumps(registry))
    monkeypatch.setattr(gateway_node, 'ROOT', root)
    result = gateway_node.credentials({'action': 'credential-set', 'name': 'OPENROUTER_API_KEY', 'key': 'private-provider-token'})
    assert result == {'name': 'OPENROUTER_API_KEY', 'configured': True}
    path = root / 'config/credentials.json'
    assert path.stat().st_mode & 0o777 == 0o600
    assert 'private-provider-token' not in json.dumps(gateway_node.credentials({'action': 'credential-status'}))
    before = path.read_bytes()
    for name in ('UNREFERENCED_KEY', 'SPARK_GATEWAY_KEY'):
        with pytest.raises(RuntimeError):
            gateway_node.credentials({'action': 'credential-set', 'name': name, 'key': 'not-installed'})
        assert path.read_bytes() == before
    result = gateway_node.credentials({'action': 'credential-remove', 'name': 'OPENROUTER_API_KEY'})
    assert result['configured'] is False
    assert json.loads(path.read_text()) == {'OPENROUTER_API_KEY': None}


def test_provider_key_input_requires_private_regular_file(tmp_path):
    import runpy
    read_key = runpy.run_path(str(ROOT / 'scripts/spark-gateway'))['read_provider_key']
    path = tmp_path / 'key'
    path.write_text('test-private-token\n')
    path.chmod(0o644)
    with pytest.raises(RuntimeError, match='group/other'):
        read_key(path)
    path.chmod(0o600)
    assert read_key(path) == 'test-private-token'
    symlink = tmp_path / 'link'
    symlink.symlink_to(path)
    with pytest.raises(OSError):
        read_key(symlink)
    path.write_text('token\nInjected: header')
    with pytest.raises(RuntimeError, match='printable'):
        read_key(path)
