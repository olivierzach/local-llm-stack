#!/usr/bin/env python3
"""Profile an already running deployment with tokenizer-sized, unique prompts.

Does not start, stop, or reconfigure any model. Run without competing inference.
Context is input plus output; request concurrency may exceed the scheduler limit
to measure queueing. Results record the exact saved deployment and actual usage.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import sys
import time
import uuid

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan

spec = importlib.util.spec_from_file_location('serving_benchmark', ROOT / 'scripts/benchmark-spark-inference.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)

UNIT = 'A compute node can run an independent workload or join a distributed deployment.\n'
SUFFIX = '\nExplain the operational tradeoffs in detail, with several concrete examples.'


def sized_prompt(count, target, nonce):
    """Fit repeated complete sentences to within one sentence of the token budget."""
    prefix = nonce + '\n'
    fixed = count(prefix + SUFFIX)
    step = count(prefix + UNIT + SUFFIX) - fixed
    if step <= 0 or fixed >= target:
        raise RuntimeError('tokenizer cannot size the requested prompt')
    repeats = max(0, (target - fixed) // step)
    for _ in range(16):
        text = prefix + UNIT * repeats + SUFFIX
        actual = count(text)
        if 0 <= target - actual <= max(32, step):
            return text, actual
        change = max(1, abs(target - actual) // step)
        repeats = max(0, repeats + (change if actual < target else -change))
    raise RuntimeError('tokenizer-sized prompt did not converge')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--prompt-tokens', type=int, nargs='+', default=[1024, 8192])
    parser.add_argument('--concurrency', type=int, nargs='+', default=[1, 2, 4])
    parser.add_argument('--requests', type=int, default=4, help='minimum requests per level; at least concurrency')
    parser.add_argument('--max-tokens', type=int, default=128)
    parser.add_argument('--request-timeout', type=int, default=600)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not (1 <= args.requests <= 64 and 2 <= args.max_tokens <= plan['recipe']['max_output_tokens']
            and 10 <= args.request_timeout <= 1800
            and all(1 <= c <= 32 for c in args.concurrency)
            and all(256 <= n <= min(262144, plan['recipe']['context_tokens'] - args.max_tokens)
                    for n in args.prompt_tokens)):
        parser.error('profile bounds exceeded or prompt plus output exceeds deployment context')
    if args.output.exists():
        parser.error('output already exists; use a new evidence path')
    base = plan['endpoint']['base_url']
    model = plan['recipe']['alias']
    session = requests.Session()
    session.trust_env = False

    def count(text):
        r = session.post(base.removesuffix('/v1') + '/tokenize', json={
            'model': model, 'messages': [{'role': 'user', 'content': text}],
            'add_generation_prompt': True}, timeout=60)
        r.raise_for_status()
        value = r.json()['count']
        if type(value) is not int or value <= 0: raise RuntimeError('invalid token count')
        return value

    def measure(text):
        return benchmark.measure(base, None, model, text, args.max_tokens, args.request_timeout)

    result = {'deployment_digest': plan['digest'], 'recipe': plan['recipe'],
              'deployment': plan['deployment'], 'started_at': time.time(),
              'prefix_mode': 'unique', 'max_tokens': args.max_tokens,
              'qualification': 'Synthetic client-observed SSE timings with server-reported usage. '
              'Unique prompt prefixes avoid deliberate prefix reuse. Warmup and tokenization are '
              'outside measured levels. Each level warms its prompt length and concurrency separately. '
              'Run without competing requests; small samples are not an SLA.',
              'levels': [], 'complete': False}
    save_json(args.output, result)
    try:
        warmup, _ = sized_prompt(count, min(512, plan['recipe']['context_tokens'] - args.max_tokens),
                                'warmup-' + uuid.uuid4().hex)
        measure(warmup)
        for target in args.prompt_tokens:
            for concurrency in args.concurrency:
                warm_prompts = [sized_prompt(count, target, 'warmup-' + uuid.uuid4().hex)[0]
                                for _ in range(concurrency)]
                warm_started = time.monotonic()
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    warm_records = list(pool.map(measure, warm_prompts))
                warm_elapsed = time.monotonic() - warm_started
                prompts = [sized_prompt(count, target, uuid.uuid4().hex)
                           for _ in range(max(args.requests, concurrency))]
                began_at = time.time()
                started = time.monotonic()
                with ThreadPoolExecutor(max_workers=concurrency) as pool:
                    records = list(pool.map(measure, [text for text, _ in prompts]))
                elapsed = time.monotonic() - started
                for record, (_, expected) in zip(records, prompts):
                    if record['prompt_tokens'] != expected:
                        raise RuntimeError('generation usage differs from tokenizer-sized input')
                    if record.get('cached_prompt_tokens', 0):
                        raise RuntimeError('unexpected cached input in unique-prefix profile')
                    if record.get('finish_reason') not in ('stop', 'length'):
                        raise RuntimeError('generation did not finish normally')
                level = {'target_prompt_tokens': target, 'concurrency': concurrency,
                         'started_at': began_at, 'ended_at': time.time(),
                         'warmup': benchmark.summarize(warm_records, warm_elapsed),
                         **benchmark.summarize(records, elapsed), 'records': records}
                result['levels'].append(level)
                save_json(args.output, result)
                print(json.dumps({k: v for k, v in level.items() if k != 'records'}), flush=True)
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
