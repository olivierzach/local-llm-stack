#!/usr/bin/env python3
"""Test an already-running model through its existing Context Guard alias."""
import argparse
import json
import os
from pathlib import Path
import runpy
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import gateway

# Reuse the stream/tool-fragment parser already exercised by native acceptance.
chat = runpy.run_path(str(ROOT / 'scripts/qwen38-verify.py'))['chat']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model', required=True)
    parser.add_argument('--base-url', default='http://127.0.0.1:4010/v1')
    parser.add_argument('--registry', type=Path, default=ROOT / 'data/context-guard-routes/registry.json')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tools', action='store_true', help='Synthetic tool call and result; executes no external tool')
    parser.add_argument('--expected-deployment', help='Require every response to identify this exact deployment digest')
    parser.add_argument('--thinking-disabled', action='store_true', help='DS4 thinking control for short acceptance replies')
    parser.add_argument('--preserve-alias', action='append', default=[])
    args = parser.parse_args()
    gateway.guard.load_dotenv(ROOT / '.env')
    key = os.environ.get('LITELLM_MASTER_KEY', '')
    if not key:
        parser.error('the local guard master key must be configured')
    registry = gateway.read(args.registry)
    gateway.validate_registry(registry)
    route = registry['routes'][args.model]
    if route.get('replicas') or route.get('upstream_key_env') or not route.get('tokenizer_base_url'):
        parser.error('this probe requires one local backend/coordinator with an exact tokenizer')
    output = args.output.expanduser().absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        parser.error('use a fresh output filename')
    report = {'passed': False, 'model': args.model, 'gateway': args.base_url,
              'backend': route['base_url'], 'checks': [],
              'scope': 'Synthetic text/stream/tool protocol and exact token-policy acceptance; not model quality or failover.'}
    headers = {'Authorization': 'Bearer ' + key}
    payload = {'model': args.model, 'messages': [{'role': 'user', 'content': 'Reply with exactly: FABRIC_OK'}],
               'temperature': 0, 'max_tokens': 128}
    if args.thinking_disabled:
        payload['thinking'] = {'type': 'disabled'}

    def exercise(label, body, finish='stop'):
        record, message = chat(args.base_url, body, key)
        record.pop('memory', None)  # The API client's host memory is not backend GPU memory.
        expected = finish if isinstance(finish, tuple) else (finish,)
        assert record['finish_reason'] in expected, record
        if args.expected_deployment:
            assert record['guard'].get('x-spark-deployment') == args.expected_deployment, 'response came from another deployment'
        assert int(record['guard']['x-context-limit']) == route['context_tokens'], record
        token_payload = {k: v for k, v in body.items() if k not in ('max_tokens', 'temperature', 'stream', 'stream_options')}
        token_payload['model'] = route['upstream_model']
        response = requests.post(route['tokenizer_base_url'].rstrip('/') + '/tokenize', json=token_payload, timeout=10)
        response.raise_for_status()
        assert int(record['guard']['x-context-input-tokens']) == response.json()['count'], record
        report['checks'].append({'test': label, **record})
        return message

    try:
        response = requests.get(args.base_url + '/models', headers=headers, timeout=10)
        response.raise_for_status()
        models = {m['id']: m for m in response.json()['data']}
        assert models[args.model]['context_length'] == route['context_tokens']
        assert all(alias in models for alias in args.preserve_alias)
        report['preserved_aliases'] = args.preserve_alias
        for stream in (False, True):
            message = exercise('stream' if stream else 'text', {**payload, 'stream': stream})
            assert message['content'].strip() == 'FABRIC_OK', message
        if args.tools:
            assert route['capabilities']['tools']
            tools = [{'type': 'function', 'function': {'name': 'get_test_value',
                'description': 'Return a test value for the requested key.',
                'parameters': {'type': 'object', 'properties': {'key': {'type': 'string'}}, 'required': ['key']}}}]
            body = {**payload, 'messages': [{'role': 'user', 'content': 'Call get_test_value with key alpha, then tell me its value.'}],
                    'tools': tools, 'tool_choice': {'type': 'function', 'function': {'name': 'get_test_value'}},
                    'max_tokens': 256, 'stream': True}
            # vLLM uses stop for named calls; other backends use tool_calls.
            # Both still must return the exact complete function call below.
            assistant = exercise('streamed-tool-call', body, ('stop', 'tool_calls'))
            calls = assistant.get('tool_calls', [])
            assert len(calls) == 1 and calls[0]['function']['name'] == 'get_test_value', assistant
            assert json.loads(calls[0]['function']['arguments']) == {'key': 'alpha'}
            body['messages'] += [assistant, {'role': 'tool', 'tool_call_id': calls[0]['id'], 'content': '42'}]
            body['tool_choice'] = 'none'
            message = exercise('tool-result-continuation', body)
            assert '42' in message['content'], message
        response = requests.post(args.base_url + '/chat/completions', json=payload,
                                 headers={'Authorization': 'Bearer invalid-acceptance-key'}, timeout=10)
        assert response.status_code == 401
        report.update(passed=True, rejected_invalid_key=True)
    except Exception as error:
        report['error'] = str(error)
    output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return int(not report['passed'])


if __name__ == '__main__':
    raise SystemExit(main())
