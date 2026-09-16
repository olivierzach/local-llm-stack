#!/usr/bin/env python3
"""Bounded, single-request long-answer measurements on an existing deployment.

Run the same cases with speculation off/on, without other inference traffic.
Records real completion lengths, visible-text decode speed and native speculative
counters. This is a small performance sample, not a reasoning quality evaluation.
"""
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

spec = importlib.util.spec_from_file_location('decode_benchmark', ROOT / 'scripts/benchmark-spark-inference.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)

CASES = {
    'explanation': 'Write a detailed tutorial explaining tensor parallelism, pipeline parallelism, '
        'KV caching and speculative decoding to a Python programmer. Include worked examples, '
        'tradeoffs, and a troubleshooting guide. Aim for at least 1800 words.',
    'code': 'Implement a complete Python standard-library-only LRU cache with a configurable '
        'capacity, per-key expiry, a monotonic clock, thread safety, statistics, and unit tests. '
        'Provide the full implementation and tests, then explain the design in detail.',
    'planning': 'Design a reproducible deployment workflow for a two-machine inference service. '
        'Explain model version pinning, health checks, request routing, admission control, '
        'failure recovery and rollback. Include concrete configuration examples and test cases. '
        'Write a thorough design document of at least 1800 words.',
}


def counters(text):
    result = {}
    for line in text.splitlines():
        if not line.startswith('vllm:spec_decode_'): continue
        name = line.split('{', 1)[0].split()[0]
        if not name.endswith('_total'): continue
        result[name] = result.get(name, 0) + float(line.rsplit(None, 1)[1])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--max-tokens', type=int, default=1024)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not 256 <= args.max_tokens <= min(4096, plan['recipe']['max_output_tokens']):
        parser.error('output must be 256..4096 tokens within the recipe limit')
    if args.output.exists(): parser.error('output already exists')
    base, model = plan['endpoint']['base_url'], plan['recipe']['alias']
    result = {'deployment_digest': plan['digest'], 'recipe': plan['recipe'],
              'concurrency': 1, 'max_tokens': args.max_tokens,
              'qualification': 'Small warmed visible-text sample; temperature zero. Compare identical '
              'cases and actual lengths, not only headline medians. No reasoning-quality claim.',
              'records': [], 'complete': False, 'started_at': time.time()}
    save_json(args.output, result)
    session = requests.Session()
    session.trust_env = False

    def metrics():
        response = session.get(base.removesuffix('/v1') + '/metrics', timeout=15)
        response.raise_for_status()
        return counters(response.text)

    try:
        benchmark.measure(base, None, model, 'Explain how a computer generates text, with examples.', 256, 600)
        for label, prompt in CASES.items():
            before = metrics()
            record = benchmark.measure(base, None, model, prompt, args.max_tokens, 600, capture_text=True)
            after = metrics()
            record.update(case=label, speculative_counter_deltas={
                key: after[key] - before.get(key, 0) for key in after})
            result['records'].append(record)
            save_json(args.output, result)
            print(json.dumps({k: v for k, v in record.items() if k != 'text'}), flush=True)
            if record['finish_reason'] not in ('stop', 'length') or record['completion_tokens'] < 256:
                raise RuntimeError('abnormal completion or answer too short for long-answer measurement')
        if plan['recipe'].get('speculative_config'):
            accepted = sum(r['speculative_counter_deltas'].get('vllm:spec_decode_num_accepted_tokens_total', 0)
                           for r in result['records'])
            if accepted <= 0: raise RuntimeError('no accepted speculative tokens observed')
        result['complete'] = True
    except BaseException as exc:
        result['error'] = repr(exc)
        raise
    finally:
        result['ended_at'] = time.time()
        save_json(args.output, result)
        session.close()


if __name__ == '__main__':
    main()
