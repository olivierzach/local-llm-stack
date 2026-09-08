import json
from pathlib import Path
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import gateway
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
    def do_GET(self): self.reply({"status": "ok"})
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/tokenize":
            self.reply({"count": 10})
            return
        self.server.received.append({"payload": body, "authorization": self.headers.get("Authorization")})
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
