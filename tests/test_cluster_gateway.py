import json
import io
from pathlib import Path
import sys
import threading
import time
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import gateway, gateway_node, config, recovery_routes
from spark_cluster.cli import save_json

KEY = "test-only-gateway-key-0000000000000000"


def test_gateway_rejects_stale_proxy_dependency_before_serving_health(tmp_path, monkeypatch):
    from dataclasses import make_dataclass
    old = make_dataclass('OldProxyConfig', [('tokenizer_base_urls', dict), ('model_timeouts', dict)])({}, {})
    monkeypatch.setattr(gateway.guard, 'build_config', lambda args: old)
    monkeypatch.setattr(gateway.guard, 'ContextGuardServer',
                        lambda *args: pytest.fail('incompatible gateway must not open a listener'))
    path = tmp_path / 'registry.json'
    save_json(path, {'version': 1, 'routes': {}})
    with pytest.raises(ValueError, match='matching context-guard-proxy'):
        gateway.serve(path, '127.0.0.1', 0, KEY)


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
            self.server.tokenizer_payload = body
            if getattr(self.server, "tokenizer_entered", None):
                self.server.tokenizer_entered.set()
                self.server.tokenizer_unblock.wait(timeout=5)
            if getattr(self.server, "tokenizer_failed", False):
                self.send_error(503)
                return
            if getattr(self.server, 'strict_tokenizer_model', None) and body.get('model') != self.server.strict_tokenizer_model:
                self.send_error(400)
                return
            count = getattr(self.server, "token_count", 10)
            self.reply({"count": count(body) if callable(count) else count})
            return
        self.server.received.append({"payload": body, "authorization": self.headers.get("Authorization")})
        if getattr(self.server, "context_error", False):
            self.send_error(400, "maximum context length exceeded")
            return
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
            self.wfile.write(getattr(self.server, "stream_content",
                b'data: {"choices":[{"delta":{"content":"ready"}}]}\n\n'))
            self.wfile.flush()
            if getattr(self.server, "stream_entered", None):
                self.server.stream_entered.set()
                self.server.stream_unblock.wait(timeout=5)
            if not getattr(self.server, "incomplete_stream", False):
                self.wfile.write(b'data: [DONE]\n\n')
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


@pytest.fixture
def policy(running, tmp_path):
    _, backend, registry, _ = running
    registry = deepcopy(registry)
    route = registry["routes"]["local-fast"]
    route.update(model_root="/cache/test", deployment_digest="a" * 64)
    # Deliberately include a static policy alias: it must never bypass fencing.
    registry["routes"]["local-auto"] = deepcopy(route)
    path = tmp_path / "policy-registry.json"
    save_json(path, registry)
    state_path = tmp_path / "recovery" / "route.json"
    server = gateway.serve(path, "127.0.0.1", 0, KEY, recovery_state=state_path)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    state = {"version": 1, "policy": "agent", "authority": "authority-one", "generation": 1,
             "alias": "local-auto", "accepting": True, "route": deepcopy(route),
             "mode": "fallback", "reason": "qualified single", "issued_at": time.time(),
             "expires_at": time.time() + 15}
    recovery_routes.write_state(state_path, state, gateway.validate_registry)
    result = {"url": f"http://127.0.0.1:{server.server_port}", "backend": backend,
              "state": state, "path": state_path, "server": server}
    yield result
    for name in ("stream_unblock", "tokenizer_unblock", "unblock"):
        if getattr(backend, name, None):
            getattr(backend, name).set()
    server.shutdown()
    server.server_close()
    worker.join()


def policy_publish(policy, **changes):
    state = deepcopy(policy["state"])
    state.update(changes)
    state["issued_at"] = time.time()
    state["expires_at"] = state["issued_at"] + 15
    recovery_routes.write_state(policy["path"], state, gateway.validate_registry)
    policy["state"] = state
    return state


def policy_status(policy):
    response = requests.get(policy["url"] + "/_spark/recovery",
        headers={"Authorization": "Bearer " + KEY}, timeout=5)
    assert response.status_code == 200
    return response.json()


def test_policy_exposes_real_backend_and_keeps_strict_alias(policy):
    response = chat(policy["url"], model="local-auto", max_tokens=17)
    assert response.status_code == 200, response.text
    assert response.json()["model"] == "local-fast"
    assert response.headers["X-Spark-Backend"] == "local-fast"
    assert response.headers["X-Spark-Deployment"] == "a" * 64
    assert response.headers["X-Spark-Generation"] == "1"
    assert response.headers["X-Context-Output-Reserve"] == "17"
    assert policy["backend"].tokenizer_model == "local-fast"
    models = requests.get(policy["url"] + "/v1/models",
        headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
    advertised = next(m for m in models if m["id"] == "local-auto")
    assert (advertised["backend_alias"], advertised["context_length"], advertised["max_output_tokens"]) == ("local-fast", 8192, 256)
    assert advertised["capabilities"]["tools"] is False
    strict = chat(policy["url"], max_tokens=4096)
    assert strict.status_code == 200 and strict.json()["model"] == "local-fast"
    assert policy["backend"].received[-1]["payload"]["max_tokens"] == 256
    policy_publish(policy, generation=2, accepting=False)
    rejected = chat(policy["url"], model="local-auto")
    assert rejected.status_code == 503 and rejected.headers["Retry-After"] == "5"
    strict = chat(policy["url"], max_tokens=4096)
    assert strict.status_code == 503 and strict.headers["Retry-After"] == "5"
    assert len(policy["backend"].received) == 2


@pytest.mark.parametrize("payload", [
    {"tools": [{"type": "function", "function": {"name": "work"}}]},
    {"messages": [{"role": "tool", "tool_call_id": "call", "content": "result"}]},
    {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "https://example.test/image"}}]}]},
    {"response_format": {"type": "json_object"}},
    {"max_tokens": 257}, {"max_completion_tokens": True},
    {"chat_template": "{{ messages }}"}, {"n": 2},
    {"messages": [{"role": "user", "content": [{"type": "input_audio", "input_audio": {}}]}]},
])
def test_policy_rejects_incompatible_requests_before_generation(policy, payload):
    response = chat(policy["url"], model="local-auto", **payload)
    assert response.status_code == 400, response.text
    assert not policy["backend"].received
    assert policy_status(policy)["active_requests"] == 0


def test_policy_context_never_compacts_or_replays(policy):
    backend = policy["backend"]
    history = [{"role": "system", "content": "keep every instruction"},
               {"role": "user", "content": "earlier " * 1000},
               {"role": "assistant", "content": "earlier answer"},
               {"role": "user", "content": "continue"}]
    backend.token_count = 8190
    response = chat(policy["url"], model="local-auto", messages=history, max_tokens=3)
    assert response.status_code == 400
    assert response.json()["error"]["type"] == "context_length_exceeded"
    assert not backend.received
    assert backend.tokenizer_payload["messages"] == history
    # The exact boundary is accepted, retaining full history and requested output.
    backend.token_count = 8189
    response = chat(policy["url"], model="local-auto", messages=history, max_tokens=3)
    assert response.status_code == 200
    assert len(backend.received) == 1
    assert backend.received[0]["payload"]["messages"] == history
    assert backend.received[0]["payload"]["max_tokens"] == 3


def test_policy_tokenizer_failure_does_not_use_estimate(policy):
    policy["backend"].tokenizer_failed = True
    response = chat(policy["url"], model="local-auto")
    assert response.status_code == 503
    assert response.json()["error"]["type"] == "tokenizer_unavailable"
    assert not policy["backend"].received


@pytest.mark.parametrize("corruption", ["missing", "malformed", "public", "symlink"])
def test_policy_file_failure_never_falls_through_static_alias(policy, corruption):
    path = policy["path"]
    if corruption == "missing":
        path.unlink()
    elif corruption == "malformed":
        path.write_text('{"version":')
    elif corruption == "public":
        path.chmod(0o644)
    else:
        actual = path.with_name("actual.json")
        path.rename(actual)
        path.symlink_to(actual)
    response = chat(policy["url"], model="local-auto")
    assert response.status_code == 503
    assert chat(policy["url"], model="local-fast").status_code == 503
    assert not policy["backend"].received
    assert not policy_status(policy)["fresh"]
    models = requests.get(policy["url"] + "/v1/models",
        headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
    assert not models


def test_policy_expiry_uses_wall_and_monotonic_time(policy, monkeypatch):
    from types import SimpleNamespace
    assert policy_status(policy)["fresh"]
    now = policy["state"]["expires_at"] + 1
    monkeypatch.setattr(recovery_routes, "time", SimpleNamespace(time=lambda: now, monotonic=time.monotonic))
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert chat(policy["url"], model="local-fast").status_code == 503
    models = requests.get(policy["url"] + "/v1/models",
        headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
    assert not models
    status = policy_status(policy)
    assert not status["fresh"] and not status["accepting"]
    assert status["generation"] == status["highest_generation"] == 1
    assert status["authority"] == "authority-one"
    assert not policy["backend"].received


def test_policy_generation_rollback_and_authority_change_are_fenced(policy):
    assert policy_status(policy)["fresh"]
    policy_publish(policy, generation=3, accepting=False)
    status = policy_status(policy)
    assert status["generation"] == 3 and status["fresh"] and not status["accepting"]
    policy_publish(policy, generation=2, accepting=True)
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert policy_status(policy)["highest_generation"] == 3
    policy_publish(policy, generation=4, accepting=True, authority="other-authority")
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert not policy["backend"].received
    policy_publish(policy, generation=4, authority="authority-one")
    assert chat(policy["url"], model="local-auto").status_code == 200


def test_policy_same_generation_contract_change_is_rejected(policy):
    assert policy_status(policy)["fresh"]
    policy_publish(policy, accepting=False)
    assert not policy_status(policy)["fresh"]
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert not policy["backend"].received


def test_policy_restart_requires_fresh_heartbeat_and_persists_highwater(policy):
    assert policy_status(policy)["fresh"]
    policy_publish(policy, generation=5)
    assert policy_status(policy)["generation"] == 5
    server = policy["server"]
    server.shutdown()
    server.server_close()
    replacement = gateway.serve(server.registry_path, "127.0.0.1", 0, KEY, recovery_state=policy["path"])
    worker = threading.Thread(target=replacement.serve_forever, daemon=True)
    worker.start()
    try:
        policy["url"] = f"http://127.0.0.1:{replacement.server_port}"
        assert chat(policy["url"], model="local-auto").status_code == 503
        assert not policy_status(policy)["fresh"]
        policy_publish(policy, generation=4)
        assert chat(policy["url"], model="local-auto").status_code == 503
        policy_publish(policy, generation=5)
        assert chat(policy["url"], model="local-auto").status_code == 200
    finally:
        replacement.shutdown()
        replacement.server_close()
        worker.join()


@pytest.mark.parametrize("baseline", ["persisted", "no-fence", "unaccepted-newer", "missing-route"])
def test_policy_restart_clock_catchup_requires_new_heartbeat(tmp_path, monkeypatch, baseline):
    from types import SimpleNamespace
    clock = SimpleNamespace(wall=999.0, monotonic=10.0)
    monkeypatch.setattr(recovery_routes, "time", SimpleNamespace(
        time=lambda: clock.wall, monotonic=lambda: clock.monotonic))
    path = tmp_path / "route.json"
    base = "http://127.0.0.1:8000"
    route = {
        "base_url": base + "/v1", "upstream_model": "local-fast", "health_url": base + "/health",
        "tokenizer_base_url": base, "context_tokens": 8192, "max_output_tokens": 256,
        "capabilities": {"text": True, "vision": False, "tools": False, "streaming": True},
        "model_root": "/cache/test", "deployment_digest": "a" * 64,
    }
    state = {"version": 1, "policy": "agent", "authority": "authority-one", "generation": 1,
             "alias": "local-auto", "accepting": True, "route": route,
             "mode": "fallback", "reason": "qualified single", "issued_at": 1000.0,
             "expires_at": 1030.0}
    if baseline == "no-fence":
        recovery_routes.write_state(path, state, gateway.validate_registry)
    else:
        original = recovery_routes.RecoveryRoutes(path, gateway.validate_registry)
        try:
            clock.wall = 1000.0
            recovery_routes.write_state(path, state, gateway.validate_registry)
            assert original.status()["fresh"]
        finally:
            original.close()
        if baseline == "unaccepted-newer":
            # The authority published again, but the prior gateway never observed it.
            state.update(issued_at=1005.0, expires_at=1035.0)
            recovery_routes.write_state(path, state, gateway.validate_registry)
        elif baseline == "missing-route":
            path.unlink()

    clock.wall = 900.0
    clock.monotonic = 20.0
    replacement = recovery_routes.RecoveryRoutes(path, gateway.validate_registry)
    try:
        if baseline == "missing-route":
            recovery_routes.write_state(path, state, gateway.validate_registry)
        assert not replacement.status()["fresh"]
        # Catching up to an unchanged, still wall-valid lease cannot revive its authority.
        clock.wall = state["issued_at"] + 0.1
        clock.monotonic = 125.1
        assert not replacement.status()["fresh"]
        with pytest.raises(recovery_routes.RouteUnavailable):
            replacement.acquire()

        # A real heartbeat may renew the same generation and route after restart.
        clock.wall = 1006.0
        clock.monotonic = 126.0
        state.update(issued_at=clock.wall, expires_at=clock.wall + 30)
        recovery_routes.write_state(path, state, gateway.validate_registry)
        assert replacement.status()["fresh"]
        admitted = replacement.acquire()
        try:
            assert admitted["generation"] == 1
            replacement.check(admitted)
        finally:
            replacement.release()

        # A closed generation still receives a fresh ACK without granting admission.
        clock.wall = 1007.0
        clock.monotonic = 127.0
        state.update(generation=2, accepting=False, issued_at=clock.wall, expires_at=clock.wall + 30)
        recovery_routes.write_state(path, state, gateway.validate_registry)
        status = replacement.status()
        assert status["fresh"] and not status["accepting"]
        assert status["generation"] == status["highest_generation"] == 2
        assert status["active_requests"] == 0
        with pytest.raises(recovery_routes.RouteUnavailable):
            replacement.acquire()
    finally:
        replacement.close()


def test_policy_single_ingress_lock_excludes_second_process(policy):
    with pytest.raises(BlockingIOError):
        gateway.serve(policy["server"].registry_path, "127.0.0.1", 0, KEY, recovery_state=policy["path"])
    assert chat(policy["url"], model="local-auto").status_code == 200


@pytest.mark.parametrize("alias", ["local-auto", "local-fast"])
def test_policy_drain_counts_stream_across_generations(policy, alias):
    backend = policy["backend"]
    backend.stream_entered = threading.Event()
    backend.stream_unblock = threading.Event()
    with ThreadPoolExecutor() as pool:
        future = pool.submit(chat, policy["url"], model=alias, stream=True)
        assert backend.stream_entered.wait(3)
        try:
            policy_publish(policy, generation=2, accepting=False)
            status = policy_status(policy)
            assert status["generation"] == 2 and status["active_requests"] == 1 and not status["accepting"]
            for name in ("local-auto", "local-fast"):
                rejected = chat(policy["url"], model=name)
                assert rejected.status_code == 503
            models = requests.get(policy["url"] + "/v1/models",
                headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
            assert not models
            assert len(backend.received) == 1
            policy_publish(policy, generation=3, accepting=True)
            assert policy_status(policy)["active_requests"] == 1
        finally:
            backend.stream_unblock.set()
        response = future.result(timeout=5)
    assert "data: [DONE]" in response.text
    assert response.headers["X-Spark-Generation"] == "1"
    assert policy_status(policy)["active_requests"] == 0


@pytest.mark.parametrize("alias", ["local-auto", "local-fast"])
@pytest.mark.parametrize("transition", ["generation", "backend-identity", "route"])
def test_policy_rechecks_admission_after_tokenizer_before_dispatch(policy, alias, transition):
    backend = policy["backend"]
    backend.tokenizer_entered = threading.Event()
    backend.tokenizer_unblock = threading.Event()
    with ThreadPoolExecutor() as pool:
        future = pool.submit(chat, policy["url"], model=alias)
        assert backend.tokenizer_entered.wait(3)
        try:
            if transition == "generation":
                policy_publish(policy, generation=2, accepting=False)
            elif transition == "route":
                route = deepcopy(policy["state"]["route"])
                route["deployment_digest"] = "b" * 64
                policy_publish(policy, generation=2, route=route)
            else:
                backend.model_root = "/cache/stale"
            assert policy_status(policy)["active_requests"] == 1
        finally:
            backend.tokenizer_unblock.set()
        response = future.result(timeout=5)
    assert response.status_code == 503
    assert not backend.received
    assert policy_status(policy)["active_requests"] == 0


def test_policy_stale_backend_identity_and_upstream_failure_are_not_replayed(policy):
    backend = policy["backend"]
    backend.model_root = "/cache/wrong"
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert not backend.received
    backend.model_root = "/cache/test"
    backend.fail_completion = True
    response = chat(policy["url"], model="local-auto")
    assert response.status_code == 503
    assert len(backend.received) == 1
    assert policy_status(policy)["active_requests"] == 0


def test_recovery_status_and_metrics_are_authenticated_and_payload_free(policy):
    for path in ("/_spark/recovery", "/metrics"):
        assert requests.get(policy["url"] + path, timeout=5).status_code == 401
    assert chat(policy["url"], model="local-auto", messages=[{"role": "user", "content": "SECRET-PROMPT"}]).status_code == 200
    headers = {"Authorization": "Bearer " + KEY}
    metrics = requests.get(policy["url"] + "/metrics", headers=headers, timeout=5).text
    labels = '{alias="local-auto",backend="local-fast",mode="fallback"}'
    assert "spark_gateway_requests_total" + labels + " 1" in metrics
    assert "spark_gateway_inflight" + labels + " 0" in metrics
    assert "spark_gateway_time_to_first_token_seconds_count" + labels not in metrics
    assert "SECRET-PROMPT" not in metrics and KEY not in metrics
    assert "base_url" not in json.dumps(policy_status(policy))
    assert chat(policy["url"], model="local-auto", stream=True).status_code == 200
    metrics = requests.get(policy["url"] + "/metrics", headers=headers, timeout=5).text
    assert "spark_gateway_time_to_first_token_seconds_count" + labels + " 1" in metrics


def test_stream_role_and_heartbeat_are_not_reported_as_generated_token_latency(policy):
    policy["backend"].stream_content = b': heartbeat\n\ndata: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
    response = chat(policy["url"], model="local-auto", stream=True)
    assert response.status_code == 200 and "data: [DONE]" in response.text
    metrics = requests.get(policy["url"] + "/metrics", headers={"Authorization": "Bearer " + KEY}, timeout=5).text
    assert 'spark_gateway_time_to_first_token_seconds_count{alias="local-auto"' not in metrics


def test_interrupted_stream_is_counted_as_error_without_replay(policy):
    policy["backend"].incomplete_stream = True
    response = chat(policy["url"], model="local-auto", stream=True)
    assert "upstream_stream_interrupted" in response.text and "data: [DONE]" not in response.text
    assert len(policy["backend"].received) == 1
    assert policy_status(policy)["active_requests"] == 0
    metrics = requests.get(policy["url"] + "/metrics", headers={"Authorization": "Bearer " + KEY}, timeout=5).text
    assert 'spark_gateway_errors_total{alias="local-auto",backend="local-fast",mode="fallback"} 1' in metrics


def test_policy_upstream_context_error_is_not_compacted_or_replayed(policy):
    policy["backend"].context_error = True
    response = chat(policy["url"], model="local-auto")
    assert response.status_code == 400
    assert len(policy["backend"].received) == 1
    assert policy_status(policy)["active_requests"] == 0


def test_policy_transition_selects_only_the_explicit_new_backend(policy):
    second = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    second.received = []
    second.model_alias = "local-coder"
    second.model_root = "/cache/coder"
    second.strict_tokenizer_model = "local-coder"
    worker = threading.Thread(target=second.serve_forever, daemon=True)
    worker.start()
    try:
        assert chat(policy["url"], model="local-auto").status_code == 200
        base = f"http://127.0.0.1:{second.server_port}"
        route = deepcopy(policy["state"]["route"])
        route.update(base_url=base + "/v1", tokenizer_base_url=base, health_url=base + "/health",
                     upstream_model="local-coder", model_root="/cache/coder", deployment_digest="b" * 64)
        route["capabilities"]["tools"] = True
        policy_publish(policy, generation=2, route=route)
        response = chat(policy["url"], model="local-auto",
                        tools=[{"type": "function", "function": {"name": "work", "parameters": {"type": "object"}}}])
        assert response.status_code == 200, response.text
        assert response.json()["model"] == "local-coder"
        assert response.headers["X-Spark-Deployment"] == "b" * 64
        assert len(policy["backend"].received) == len(second.received) == 1
        assert second.tokenizer_payload["tools"] == second.received[0]["payload"]["tools"]
        second.available = False
        assert chat(policy["url"], model="local-auto").status_code == 503
        assert len(policy["backend"].received) == len(second.received) == 1
    finally:
        second.shutdown()
        second.server_close()
        worker.join()


def test_policy_unchanged_lease_cannot_outlive_monotonic_deadline(policy, monkeypatch):
    from types import SimpleNamespace
    assert policy_status(policy)["fresh"]
    observed_wall = time.time()
    deadline = policy["server"].recovery.lease_deadline
    monkeypatch.setattr(recovery_routes, "time", SimpleNamespace(time=lambda: observed_wall, monotonic=lambda: deadline + 1))
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert not policy_status(policy)["fresh"]
    assert not policy["backend"].received


def test_policy_lease_extension_without_new_heartbeat_is_rejected(policy):
    assert policy_status(policy)["fresh"]
    state = deepcopy(policy["state"])
    state["expires_at"] += 10
    recovery_routes.write_state(policy["path"], state, gateway.validate_registry)
    assert chat(policy["url"], model="local-auto").status_code == 503
    assert not policy["backend"].received


@pytest.mark.parametrize("changes", [
    {"version": True}, {"unknown": 1}, {"generation": True},
    {"generation": 0}, {"accepting": "true"}, {"authority": "bad\\nauthority"},
    {"issued_at": float("nan")}, {"route": None},
])
def test_invalid_policy_state_is_rejected_without_static_alias_bypass(policy, changes):
    state = deepcopy(policy["state"])
    state.update(changes)
    policy["path"].write_text(json.dumps(state))
    response = chat(policy["url"], model="local-auto")
    assert response.status_code == 503
    assert not policy["backend"].received


def test_policy_route_publication_and_fence_are_private_and_durable(policy):
    assert policy_status(policy)["fresh"]
    fence = policy["path"].with_name("route.json.fence.json")
    assert policy["path"].stat().st_mode & 0o777 == 0o600
    assert fence.stat().st_mode & 0o777 == 0o600
    persisted = json.loads(fence.read_text())
    assert (persisted["authority"], persisted["generation"]) == ("authority-one", 1)
    assert persisted["issued_at"] == policy["state"]["issued_at"]
    fence.write_text('{"partial":')
    server = policy["server"]
    server.shutdown()
    server.server_close()
    with pytest.raises(ValueError):
        gateway.serve(server.registry_path, "127.0.0.1", 0, KEY, recovery_state=policy["path"])


@pytest.mark.parametrize("field,value", [
    ("deployment_digest", "b" * 64),
    ("context_tokens", 4096),
    ("max_output_tokens", 128),
    ("tokenizer_timeout_s", 30),
])
def test_policy_strict_alias_requires_entire_pinned_route(policy, field, value):
    path = policy["server"].registry_path
    registry = json.loads(path.read_text())
    registry["routes"]["local-fast"][field] = value
    save_json(path, registry)
    response = chat(policy["url"], model="local-fast")
    assert response.status_code == 503
    assert not policy["backend"].received
    assert policy_status(policy)["active_requests"] == 0
    models = requests.get(policy["url"] + "/v1/models",
        headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
    assert {model["id"] for model in models} == {"local-auto"}
    assert chat(policy["url"], model="local-auto").status_code == 200
    assert policy["backend"].received[-1]["payload"]["model"] == "local-fast"


def test_closed_policy_does_not_fence_provider_or_ordinary_gateway(policy, running, monkeypatch):
    path = policy["server"].registry_path
    registry = json.loads(path.read_text())
    provider = deepcopy(registry["routes"]["local-fast"])
    provider["upstream_key_env"] = "TEST_PROVIDER_KEY"
    registry["routes"]["provider-chat"] = provider
    save_json(path, registry)
    monkeypatch.setenv("TEST_PROVIDER_KEY", "provider-only-secret")
    policy_publish(policy, generation=2, accepting=False)
    assert chat(policy["url"], model="local-fast").status_code == 503
    response = chat(policy["url"], model="provider-chat", max_tokens=4096)
    assert response.status_code == 200 and response.json()["model"] == "local-fast"
    assert policy["backend"].received[-1]["authorization"] == "Bearer provider-only-secret"
    assert policy["backend"].received[-1]["payload"]["max_tokens"] == 256
    assert policy_status(policy)["active_requests"] == 0
    models = requests.get(policy["url"] + "/v1/models",
        headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
    assert {model["id"] for model in models} == {"provider-chat"}
    # A separately configured ordinary gateway retains its pre-policy contract.
    ordinary = chat(running[0], max_tokens=4096)
    assert ordinary.status_code == 200 and ordinary.json()["model"] == "local-fast"
    assert policy["backend"].received[-1]["authorization"] is None
    assert policy["backend"].received[-1]["payload"]["max_tokens"] == 256


def test_policy_global_drain_counts_strict_stream_and_auto_request(policy):
    backend = policy["backend"]
    backend.stream_entered = threading.Event()
    backend.stream_unblock = threading.Event()
    backend.unblock = threading.Event()
    with ThreadPoolExecutor() as pool:
        strict = pool.submit(chat, policy["url"], model="local-fast", stream=True)
        assert backend.stream_entered.wait(3)
        backend.entered = threading.Event()
        automatic = pool.submit(chat, policy["url"], model="local-auto")
        try:
            assert backend.entered.wait(3)
            policy_publish(policy, generation=2, accepting=False)
            status = policy_status(policy)
            assert status["generation"] == 2 and not status["accepting"]
            assert status["active_requests"] == 2
            for alias in ("local-fast", "local-auto"):
                assert chat(policy["url"], model=alias).status_code == 503
            backend.unblock.set()
            assert automatic.result(timeout=5).status_code == 200
            assert policy_status(policy)["active_requests"] == 1
        finally:
            backend.unblock.set()
            backend.stream_unblock.set()
        response = strict.result(timeout=5)
    assert response.status_code == 200 and "data: [DONE]" in response.text
    assert len(backend.received) == 2
    assert policy_status(policy)["active_requests"] == 0


@pytest.mark.parametrize("close_at", ["tokenizer", "summary", None])
def test_strict_compaction_obeys_policy_lease(policy, close_at):
    backend = policy["backend"]
    policy["server"].config.keep_last_messages = 1
    history = [{"role": "user", "content": "earlier"},
               {"role": "assistant", "content": "earlier answer"},
               {"role": "user", "content": "continue"}]
    backend.token_count = lambda body: 8000 if body["messages"] == history else 10
    backend.tokenizer_entered = threading.Event()
    backend.tokenizer_unblock = threading.Event()
    backend.entered = threading.Event()
    backend.unblock = threading.Event()
    with ThreadPoolExecutor() as pool:
        future = pool.submit(chat, policy["url"], model="local-fast", messages=history)
        try:
            assert backend.tokenizer_entered.wait(3)
            assert policy_status(policy)["active_requests"] == 1
            if close_at == "tokenizer":
                policy_publish(policy, generation=2, accepting=False)
            backend.tokenizer_unblock.set()
            if close_at != "tokenizer":
                assert backend.entered.wait(3)
                assert policy_status(policy)["active_requests"] == 1
                if close_at == "summary":
                    policy_publish(policy, generation=2, accepting=False)
            backend.unblock.set()
            response = future.result(timeout=5)
        finally:
            backend.tokenizer_unblock.set()
            backend.unblock.set()
    assert response.status_code == (200 if close_at is None else 503)
    assert len(backend.received) == {None: 2, "summary": 1, "tokenizer": 0}[close_at]
    if close_at is None:
        final = backend.received[-1]["payload"]
        assert final["model"] == "local-fast"
        assert final["messages"] != history
        assert final["messages"][-1] == history[-1]
    assert policy_status(policy)["active_requests"] == 0


def test_strict_alias_rebinding_selects_exact_fallback_placement(policy):
    second = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
    second.received = []
    worker = threading.Thread(target=second.serve_forever, daemon=True)
    worker.start()
    try:
        assert chat(policy["url"], model="local-fast").status_code == 200
        base = f"http://127.0.0.1:{second.server_port}"
        route = deepcopy(policy["state"]["route"])
        route.update(base_url=base + "/v1", tokenizer_base_url=base, health_url=base + "/health",
                     deployment_digest="b" * 64)
        policy_publish(policy, generation=2, accepting=False)
        path = policy["server"].registry_path
        registry = json.loads(path.read_text())
        registry["routes"]["local-fast"] = route
        recovery_routes.atomic_json(path, registry)
        assert chat(policy["url"], model="local-fast").status_code == 503
        policy_publish(policy, generation=3, route=route, accepting=True)
        strict = chat(policy["url"], model="local-fast")
        assert strict.status_code == 200 and strict.json()["model"] == "local-fast"
        assert strict.headers["X-Spark-Deployment"] == "b" * 64
        assert strict.headers["X-Spark-Generation"] == "3"
        assert len(policy["backend"].received) == len(second.received) == 1
        models = requests.get(policy["url"] + "/v1/models",
            headers={"Authorization": "Bearer " + KEY}, timeout=5).json()["data"]
        assert {model["id"] for model in models} == {"local-auto", "local-fast"}
        assert all(model["deployment_digest"] == "b" * 64 for model in models)
    finally:
        second.shutdown()
        second.server_close()
        worker.join()
