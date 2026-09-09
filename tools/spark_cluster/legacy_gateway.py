"""Opt-in registry overrides on the existing Context Guard address.

Unregistered aliases retain the legacy LiteLLM path and authentication. Migrated
aliases use the existing master key, an atomic route/limits/tokenizer snapshot,
and the same guarded deployment path as the standalone cluster gateway.
"""
import hmac
import json
import os
from pathlib import Path

import requests

from . import gateway

guard = gateway.guard


class LegacyGatewayHandler(gateway.GatewayHandler):
    def registry(self):
        if hasattr(self, '_registry_snapshot'):
            return self._registry_snapshot
        return super().registry()

    def incoming_headers(self):
        if getattr(self, '_legacy', False):
            return guard.ContextGuardHandler.incoming_headers(self)
        return super().incoming_headers()

    def do_POST(self):
        if self.parsed_path() != '/v1/chat/completions':
            self._legacy = True
            return guard.ContextGuardHandler.do_POST(self)
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 16 * 1024 * 1024:
                self.write_json(413, {'error': {'message': 'Request body must be 1 byte to 16 MiB'}})
                return
            payload = json.loads(self.read_body())
            if not isinstance(payload, dict): raise ValueError()
        except (ValueError, TypeError):
            self.write_json(400, {'error': {'message': 'Invalid request body'}})
            return
        try:
            self._registry_snapshot = self.registry()
        except (ValueError, OSError, TypeError, KeyError):
            self.write_json(503, {'error': {'message': 'Invalid route registry'}})
            return
        alias = guard.normalize_model_name(str(payload.get('model', '')))
        if alias not in self._registry_snapshot['routes']:
            self._legacy = True
            return guard.ContextGuardHandler.do_POST(self)
        return super().do_POST()

    def do_GET(self):
        # Preserve legacy endpoints and virtual-key behavior. Only the master
        # key may discover/use overrides; do not treat a model-list response as
        # authorization for a virtual key to bypass LiteLLM's policy/accounting.
        master = hmac.compare_digest(self.headers.get('Authorization', '').encode(),
                                     ('Bearer ' + self.server.api_key).encode())
        if self.parsed_path() != '/v1/models' or not master:
            self._legacy = True
            return guard.ContextGuardHandler.do_GET(self)
        try:
            registry = self.registry()
        except (ValueError, OSError, TypeError, KeyError):
            self.write_json(503, {'error': {'message': 'Invalid route registry'}})
            return
        legacy_models = []
        try:
            with requests.get(self.server.config.upstream_base_url.rstrip('/') + '/models',
                              headers={'Authorization': self.headers['Authorization']}, timeout=3) as response:
                response.raise_for_status()
                legacy_models = response.json()['data']
                if not isinstance(legacy_models, list): raise ValueError()
        except (requests.RequestException, ValueError, KeyError):
            pass
        data = [m for m in legacy_models if isinstance(m, dict) and m.get('id') not in registry['routes']]
        for alias, route in registry['routes'].items():
            if gateway.provider_ready(route, self.server.registry_path) and any(gateway.live(m) for m in gateway.members(route)):
                data.append({'id': alias, 'object': 'model', 'owned_by': 'spark-cluster',
                             'context_length': route['context_tokens'],
                             'max_output_tokens': route['max_output_tokens'], 'capabilities': route['capabilities']})
        self.write_json(200, {'object': 'list', 'data': data})

    def context_headers(self, payload, **kwargs):
        if getattr(self, '_legacy', False):
            return guard.ContextGuardHandler.context_headers(self, payload, **kwargs)
        return super().context_headers(payload, **kwargs)

    def post_upstream(self, headers, payload):
        if getattr(self, '_legacy', False):
            return guard.ContextGuardHandler.post_upstream(self, headers, payload)
        return super().post_upstream(headers, payload)

    def summarize_messages(self, messages, *, model, headers):
        if getattr(self, '_legacy', False):
            return guard.ContextGuardHandler.summarize_messages(self, messages, model=model, headers=headers)
        return super().summarize_messages(messages, model=model, headers=headers)


def serve(registry_path, host, port, key, config):
    if not isinstance(key, str) or len(key) < 24:
        raise ValueError('registry overrides require the existing LiteLLM master key (at least 24 characters)')
    gateway.validate_registry(gateway.read(registry_path))
    server = guard.ContextGuardServer((host, port), LegacyGatewayHandler, config)
    server.registry_path = Path(registry_path)
    server.api_key = key
    server.replica_pool = gateway.ReplicaPool()
    return server


def main():
    path = os.getenv('CONTEXT_GUARD_ROUTE_REGISTRY')
    if not path:
        return guard.main()
    guard.load_dotenv(guard.REPO_ROOT / '.env')
    args = guard.parser().parse_args()
    server = serve(path, args.host, args.port, os.getenv('LITELLM_MASTER_KEY', ''), guard.build_config(args))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
