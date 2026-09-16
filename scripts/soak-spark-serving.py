#!/usr/bin/env python3
"""Bounded sequential serving acceptance; no model lifecycle or route changes."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan
spec = importlib.util.spec_from_file_location('decode_profile', ROOT / 'scripts/profile-spark-decode.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def conversation(case, previous=None):
    messages = [{'role': 'user', 'content': profile.CASES[case]}]
    if previous is not None:
        messages += [{'role': 'assistant', 'content': previous},
                     {'role': 'user', 'content': 'Continue with concrete edge cases and detailed examples. Do not repeat the introduction.'}]
    return messages


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=3, help='Each round has three fresh/repeated and three continuation requests')
    parser.add_argument('--max-tokens', type=int, default=1024)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not 1 <= args.rounds <= 10 or not 256 <= args.max_tokens <= min(4096, plan['recipe']['max_output_tokens']):
        parser.error('bounds: rounds 1..10, output 256..4096 within recipe limit')
    if args.output.exists(): parser.error('use a fresh output filename')
    result = {'complete': False, 'deployment_digest': plan['digest'], 'recipe': plan['recipe'],
              'started_at': time.time(), 'records': [], 'rounds': args.rounds,
              'qualification': 'Sequential synthetic protocol/reliability sample; not a quality evaluation or uptime guarantee.'}
    save_json(args.output, result)
    session = requests.Session()
    session.trust_env = False
    base = plan['endpoint']['base_url']
    def counters():
        response = session.get(base.removesuffix('/v1') + '/metrics', timeout=15)
        response.raise_for_status()
        return profile.counters(response.text)
    try:
        before = counters()
        for round_idx in range(args.rounds):
            temperature = 0 if round_idx % 2 == 0 else 0.7
            for case in profile.CASES:
                previous = None
                for turn in range(2):
                    record = profile.benchmark.measure(base, None, plan['recipe']['alias'], '',
                        args.max_tokens, 180, capture_text=True, temperature=temperature,
                        messages=conversation(case, previous))
                    record.update(round=round_idx, case=case, turn=turn, temperature=temperature)
                    result['records'].append(record)
                    save_json(args.output, result)
                    print(json.dumps({k: v for k, v in record.items() if k != 'text'}), flush=True)
                    if record['finish_reason'] not in ('stop', 'length') or record['completion_tokens'] < 128:
                        raise RuntimeError('abnormal or too-short continuation')
                    previous = record['text']
        after = counters()
        result['speculative_counter_deltas'] = {k: after[k] - before.get(k, 0) for k in after}
        if plan['recipe'].get('speculative_config') and result['speculative_counter_deltas'].get('vllm:spec_decode_num_accepted_tokens_total', 0) <= 0:
            raise RuntimeError('no native speculative acceptance observed')
        result['complete'] = True
    except BaseException as exc:
        result['error'] = repr(exc)
        raise
    finally:
        result['ended_at'] = time.time()
        save_json(args.output, result)
        session.close()


if __name__ == '__main__': main()
