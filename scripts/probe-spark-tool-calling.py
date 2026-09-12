#!/usr/bin/env python3
"""Exercise tool parsing and multi-turn results on an existing deployment.

Runs synthetic functions only; executes no model-requested code. Keeps receipts
on failure and does not change workers or gateway routes.
"""
import argparse
import json
from pathlib import Path
import runpy
import sys
import time
import uuid

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan

chat = runpy.run_path(str(ROOT / 'scripts/qwen38-verify.py'))['chat']
counters = runpy.run_path(str(ROOT / 'scripts/profile-spark-decode.py'))['counters']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=3)
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not plan['recipe']['capabilities']['tools'] or not 1 <= args.rounds <= 10:
        parser.error('requires tool capability and 1..10 rounds')
    if args.output.exists():
        parser.error('use a fresh output filename')
    base = plan['endpoint']['base_url']
    report = dict(complete=False, deployment_digest=plan['digest'], checks=[],
                  started_at=time.time(), scope='Synthetic protocol acceptance, not tool-use quality.')
    session = requests.Session()
    session.trust_env = False

    def metrics():
        response = session.get(base.removesuffix('/v1') + '/metrics', timeout=15)
        response.raise_for_status()
        return counters(response.text)

    def exercise(label, body, expected_finish):
        record, message = chat(base, body, None)
        record.pop('memory', None)
        if record['finish_reason'] != expected_finish:
            raise RuntimeError(f'{label}: unexpected finish: {record}')
        report['checks'].append(dict(test=label, **record))
        save_json(args.output, report)
        print(json.dumps(dict(test=label, **record)), flush=True)
        return message

    tools = [{'type': 'function', 'function': {
        'name': 'lookup_value', 'description': 'Look up the secret verification value for a key.',
        'parameters': {'type': 'object', 'properties': {'key': {'type': 'string'}},
                       'required': ['key'], 'additionalProperties': False}}}]
    save_json(args.output, report)
    try:
        before = metrics()
        for round_idx in range(args.rounds):
            for mode in ('auto', 'named', 'required'):
                for stream in (False, True):
                    label = f'{round_idx}-{mode}-stream-{stream}'
                    key = 'key_' + uuid.uuid4().hex[:12]
                    choice = {'type': 'function', 'function': {'name': 'lookup_value'}} if mode == 'named' else mode
                    body = {'model': plan['recipe']['alias'], 'temperature': 0, 'max_tokens': 256,
                            'messages': [{'role': 'user', 'content': f'Call lookup_value with key {key}. Then reply only with its returned value.'}],
                            'tools': tools, 'tool_choice': choice, 'stream': stream}
                    if stream:
                        body['stream_options'] = {'include_usage': True}
                    # vLLM deliberately uses stop for named choices; automatic
                    # and required calls use tool_calls. Validate the structured
                    # call and arguments below regardless of the finish label.
                    assistant = exercise(label + '-call', body, 'stop' if mode == 'named' else 'tool_calls')
                    calls = assistant.get('tool_calls', [])
                    if len(calls) != 1 or calls[0]['function']['name'] != 'lookup_value' or json.loads(calls[0]['function']['arguments']) != {'key': key}:
                        raise RuntimeError('incorrect function or arguments: ' + repr(assistant))
                    value = 'verified_' + uuid.uuid4().hex
                    body['messages'] += [assistant, {'role': 'tool', 'tool_call_id': calls[0]['id'], 'content': value}]
                    body['tool_choice'] = 'none'
                    final = exercise(label + '-result', body, 'stop')
                    if final.get('tool_calls') or value not in final['content']:
                        raise RuntimeError('continuation did not return the actual tool result')
        after = metrics()
        report['speculative_counter_deltas'] = {k: after[k] - before.get(k, 0) for k in after}
        if plan['recipe'].get('speculative_config') and report['speculative_counter_deltas'].get('vllm:spec_decode_num_accepted_tokens_total', 0) <= 0:
            raise RuntimeError('no accepted speculative tokens observed during tool conversations')
        report['complete'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        report['ended_at'] = time.time()
        save_json(args.output, report)
        session.close()


if __name__ == '__main__':
    main()
