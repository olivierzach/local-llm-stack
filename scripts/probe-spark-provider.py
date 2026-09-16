#!/usr/bin/env python3
"""Exercise managed provider credentials on a local Spark gateway without cloud calls."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import threading
import uuid

import requests

ROOT = Path(__file__).resolve().parents[1]


class Provider(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        valid = self.path == '/v1/chat/completions' and self.headers.get('Authorization') == 'Bearer '+self.server.key
        valid = valid and payload.get('model') == 'fixture/provider-model'
        self.server.calls.append({'authorized': valid, 'stream': bool(payload.get('stream'))})
        if not valid:
            self.send_error(401)
            return
        if payload.get('stream'):
            body = b'data: {"choices":[{"delta":{"content":"provider-ready"}}]}\n\ndata: [DONE]\n\n'
            kind = 'text/event-stream'
        else:
            body = json.dumps({'model': 'fixture/provider-model', 'choices': [
                {'message': {'role': 'assistant', 'content': 'provider-ready'}, 'finish_reason': 'stop'}]}).encode()
            kind = 'application/json'
        self.send_response(200)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if socket.gethostname() != 'spark-'+args.node:
        parser.error('run this probe on the gateway Spark itself')
    gateway_root = Path.home()/'.local/state/local-llm-cluster/gateway'
    registry_path = gateway_root/'config/registry.json'
    state = json.loads((gateway_root/'state.json').read_text())
    identity = subprocess.check_output(['docker', 'inspect', '--format', '{{.Id}} {{.State.StartedAt}}', state['name']], text=True)
    original = json.loads(registry_path.read_text())
    alias = 'provider-probe-'+uuid.uuid4().hex[:8]
    name = 'PROVIDER_PROBE_'+uuid.uuid4().hex.upper()
    server = ThreadingHTTPServer(('127.0.0.1', 0), Provider)
    server.key = secrets.token_urlsafe(32)
    server.calls = []
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    route = {'base_url': f'http://127.0.0.1:{server.server_port}/v1', 'upstream_model': 'fixture/provider-model',
             'upstream_key_env': name, 'context_tokens': 8192, 'max_output_tokens': 256,
             'capabilities': {'text': True, 'vision': False, 'tools': False, 'streaming': True}}
    base = f'http://127.0.0.1:{state["port"]}'
    headers = {'Authorization': 'Bearer '+(gateway_root/'api-key').read_text().strip()}
    def command(*arguments):
        result = subprocess.run([sys.executable, str(ROOT/'scripts/spark-gateway'), *arguments, '--node', args.node],
                                text=True, capture_output=True, timeout=45, check=True)
        return json.loads(result.stdout)
    def completion(stream=False):
        return requests.post(base+'/v1/chat/completions', headers=headers, json={'model': alias,
            'messages': [{'role': 'user', 'content': 'Return the fixture response.'}], 'max_tokens': 32,
            'stream': stream}, timeout=20)
    def advertised():
        response = requests.get(base+'/v1/models', headers=headers, timeout=20)
        response.raise_for_status()
        return alias in {m['id'] for m in response.json()['data']}
    installed = False
    with tempfile.TemporaryDirectory(prefix='spark-provider-probe-') as folder:
        temporary = Path(folder)
        registry = temporary/'registry.json'
        key = temporary/'provider-key'
        key.touch(mode=0o600)
        try:
            registry.write_text(json.dumps({**original, 'routes': {**original['routes'], alias: route}}))
            installed = True
            command('routes', '--registry', str(registry))
            assert not advertised() and completion().status_code == 503
            key.write_text(server.key)
            command('credential-set', '--name', name, '--key-file', str(key))
            assert advertised()
            response = completion()
            response.raise_for_status()
            assert response.json()['choices'][0]['message']['content'] == 'provider-ready'
            server.key = secrets.token_urlsafe(32)
            key.write_text(server.key)
            command('credential-set', '--name', name, '--key-file', str(key))
            response = completion(stream=True)
            response.raise_for_status()
            assert 'provider-ready' in response.text and '[DONE]' in response.text
            command('credential-remove', '--name', name)
            assert not advertised() and completion().status_code == 503
            assert len(server.calls) == 2 and all(c['authorized'] for c in server.calls)
            store = gateway_root/'config/credentials.json'
            assert store.stat().st_mode & 0o777 == 0o600
            after = subprocess.check_output(['docker', 'inspect', '--format', '{{.Id}} {{.State.StartedAt}}', state['name']], text=True)
            assert identity == after
            report = {'passed': True, 'node': args.node, 'provider_requests': server.calls,
                      'missing_key_rejected': True, 'rotation_passed': True, 'removal_passed': True,
                      'gateway_unchanged': True, 'credential_mode': '0600',
                      'scope': 'Local provider fixture; no real OpenRouter key or cloud requests.'}
        finally:
            try:
                if installed:
                    try:
                        command('credential-remove', '--name', name)
                    finally:
                        current = json.loads(registry_path.read_text())
                        if current['routes'].get(alias) == route:
                            del current['routes'][alias]
                            registry.write_text(json.dumps(current))
                            command('routes', '--registry', str(registry))
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=5)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
