"""CPU-only socket failures through real standalone and existing-guard handlers."""
import json
from pathlib import Path
import socket
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway, legacy_gateway

KEY = 'synthetic-routing-key-0000000000000000'
DELTA = b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'
FINISH = b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n'
DONE = b'data: [DONE]\n\n'


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, body):
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if not self.server.available:
            self.send_error(503)
        elif self.path == '/v1/models':
            self.reply({'data': [{'id': 'local-fast', 'root': '/cache/test', 'max_model_len': 8192}]})
        else:
            self.reply({'status': 'ok'})

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/tokenize':
            self.reply({'count': 10})
            return
        self.server.received.append(payload)
        mode = self.server.mode
        if mode == 'before_headers':
            self.connection.shutdown(socket.SHUT_RDWR)
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.end_headers()
        try:
            self.wfile.write(DELTA)
            self.wfile.flush()
            if mode == 'partial_frame':
                self.wfile.write(b'data: {"choices":[{"delta":{"tool_calls":[')
            elif mode == 'malformed_event':
                self.wfile.write(b'data: {broken json}\n\n' + DONE)
            elif mode == 'error':
                self.wfile.write(b'data: {"error":{"type":"overloaded","message":"busy"}}\n\n' + DONE)
            elif mode == 'stall':
                self.server.unblock.wait(3)
            elif mode == 'gated':
                self.server.unblock.wait(3)
                self.wfile.write(FINISH + DONE)
            elif mode == 'cancel':
                # A later write detects the client's TCP reset and lets us
                # verify the gateway closes this upstream connection too.
                self.server.unblock.wait(3)
                for _ in range(100):
                    self.wfile.write(DELTA * 256)
                    self.wfile.flush()
                    if self.server.closed.wait(0.01):
                        break
            elif mode == 'normal':
                self.wfile.write(FINISH + DONE)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.server.closed.set()
        finally:
            self.close_connection = True


@pytest.fixture(params=['standalone', 'existing-guard'])
def stack(tmp_path, request):
    servers, threads = [], []
    for _ in range(3):
        backend = ThreadingHTTPServer(('127.0.0.1', 0), Backend)
        backend.mode = 'normal'
        backend.available = True
        backend.received = []
        backend.unblock = threading.Event()
        backend.closed = threading.Event()
        servers.append(backend)
    first, second, original = servers
    bases = [f'http://127.0.0.1:{s.server_port}' for s in servers]
    endpoints = [{'base_url': base + '/v1', 'health_url': base + '/health',
                  'tokenizer_base_url': base, 'deployment_digest': digest * 64}
                 for base, digest in zip(bases[:2], ['a', 'b'])]
    route = {**endpoints[0], 'replicas': endpoints, 'upstream_model': 'local-fast',
             'model_root': '/cache/test', 'context_tokens': 8192, 'max_output_tokens': 128,
             'capabilities': {'text': True, 'tools': True, 'vision': False, 'streaming': True}}
    path = tmp_path / 'registry.json'
    path.write_text(json.dumps({'version': 1, 'routes': {'local-fast': route}}))
    if request.param == 'standalone':
        server = gateway.serve(path, '127.0.0.1', 0, KEY)
    else:
        cfg = gateway.guard.build_config(gateway.guard.parser().parse_args([]))
        cfg.upstream_base_url = bases[2] + '/v1'
        server = legacy_gateway.serve(path, '127.0.0.1', 0, KEY, cfg)
    server.config.timeout_s = 1
    servers.append(server)
    for s in servers:
        thread = threading.Thread(target=s.serve_forever, daemon=True)
        thread.start()
        threads.append(thread)
    yield SimpleNamespace(url=f'http://127.0.0.1:{server.server_port}', first=first,
                          second=second, original=original, gateway=server)
    for s in servers[:3]:
        s.unblock.set()
        s.closed.set()
    for s in servers:
        s.shutdown()
        s.server_close()
    for thread in threads:
        thread.join(5)


def post(stack, streaming_client=False):
    return requests.post(stack.url + '/v1/chat/completions',
                         headers={'Authorization': 'Bearer ' + KEY},
                         json={'model': 'local-fast', 'messages': [{'role': 'user', 'content': 'synthetic'}],
                               'stream': True, 'max_tokens': 64}, stream=streaming_client, timeout=5)


def await_released(stack):
    deadline = time.monotonic() + 3
    while stack.gateway.replica_pool.active and time.monotonic() < deadline:
        threading.Event().wait(0.01)
    assert stack.gateway.replica_pool.active == {}


@pytest.mark.parametrize('mode', ['eof', 'partial_frame', 'malformed_event', 'stall'])
def test_interrupted_stream_is_explicit_and_not_replayed(stack, mode):
    stack.first.mode = mode
    response = post(stack)
    assert response.status_code == 200  # Headers were already sent.
    assert '"content":"partial"' in response.text
    assert 'upstream_stream_interrupted' in response.text
    assert '[DONE]' not in response.text and 'tool_calls' not in response.text
    assert '{broken json}' not in response.text
    assert len(stack.first.received) == 1
    assert not stack.second.received and not stack.original.received
    await_released(stack)
    # Only a NEW explicit request may select another healthy replica.
    stack.first.available = False
    retry = post(stack)
    assert retry.status_code == 200 and retry.headers['X-Spark-Deployment'] == 'b' * 64
    assert retry.text.endswith(DONE.decode())
    assert len(stack.second.received) == 1


def test_disconnect_before_headers_is_502_without_retry(stack):
    stack.first.mode = 'before_headers'
    response = post(stack)
    assert response.status_code == 502
    assert len(stack.first.received) == 1 and not stack.second.received and not stack.original.received
    await_released(stack)


def test_backend_error_is_preserved_without_success_marker(stack):
    stack.first.mode = 'error'
    response = post(stack)
    assert 'overloaded' in response.text and '[DONE]' not in response.text
    assert 'upstream_stream_interrupted' not in response.text
    await_released(stack)


def test_events_arrive_before_upstream_closes_and_keep_replica_busy(stack):
    stack.first.mode = 'gated'
    stack.gateway.config.timeout_s = 5
    with post(stack, streaming_client=True) as response:
        lines = response.iter_lines(chunk_size=1)
        assert next(lines) == DELTA.splitlines()[0]
        assert sum(stack.gateway.replica_pool.active.values()) == 1
        assert post(stack).headers['X-Spark-Deployment'] == 'b' * 64
        stack.first.unblock.set()
        remainder = b'\n'.join(lines)
        assert b'finish_reason' in remainder and b'[DONE]' in remainder
    await_released(stack)


def test_client_reset_closes_upstream_and_releases_replica(stack):
    stack.first.mode = 'cancel'
    address = ('127.0.0.1', stack.gateway.server_port)
    client = socket.create_connection(address, timeout=5)
    payload = json.dumps({'model': 'local-fast', 'messages': [{'role': 'user', 'content': 'synthetic'}], 'stream': True}).encode()
    client.sendall((f'POST /v1/chat/completions HTTP/1.0\r\nHost: localhost\r\nAuthorization: Bearer {KEY}\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n').encode() + payload)
    received = b''
    while DELTA not in received:
        data = client.recv(4096)
        assert data
        received += data
    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    client.close()
    stack.first.unblock.set()
    await_released(stack)
    assert stack.first.closed.wait(3)
    assert not stack.second.received and not stack.original.received


@pytest.mark.parametrize('separator', [b'\n', b'\r\n', b'\r'])
def test_sse_delimiters_and_utf8_survive_every_byte_boundary(separator):
    message = 'data: {"text":"hello 🌍"}'.encode() + separator * 2
    ending = b'data: [DONE]' + separator * 2
    frames = list(gateway.guard.sse_events(bytes([byte]) for byte in message + ending))
    assert frames == [message, ending]


def test_sse_buffer_is_bounded_and_partial_frame_is_not_emitted():
    with pytest.raises(gateway.guard.IncompleteEventStream):
        list(gateway.guard.sse_events([b'data: ' + b'x' * 100], max_event_bytes=32))
    frames = gateway.guard.sse_events([DELTA, b'data: {"unfinished":'])
    assert next(frames) == DELTA
    with pytest.raises(gateway.guard.IncompleteEventStream):
        next(frames)


def test_successful_stream_is_preserved_byte_for_byte(stack):
    response = post(stack)
    assert response.content == DELTA + FINISH + DONE
    assert response.headers['X-Context-Limit'] == '8192'
    await_released(stack)


def test_older_transport_fallback_preserves_stream():
    import io
    handler = object.__new__(gateway.guard.ContextGuardHandler)
    handler.wfile = io.BytesIO()
    def chunks(chunk_size):
        assert chunk_size == 1
        yield from (bytes([byte]) for byte in DELTA + FINISH + DONE)
    response = SimpleNamespace(raw=SimpleNamespace(), iter_content=chunks)
    handler.relay_chat_events(response)
    assert handler.wfile.getvalue() == DELTA + FINISH + DONE


def test_client_cancelling_before_headers_also_closes_upstream():
    handler = object.__new__(gateway.guard.ContextGuardHandler)
    def disconnected(_):
        raise BrokenPipeError()
    handler.send_response = disconnected
    closed = []
    response = SimpleNamespace(status_code=200, close=lambda: closed.append(True))
    handler.relay_response(response, stream=True)
    assert closed == [True] and handler.close_connection
