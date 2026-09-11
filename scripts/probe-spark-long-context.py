#!/usr/bin/env python3
"""Check long-input retrieval and identical-prefix reuse on an existing deployment.

Uses only synthetic text and three random codes placed throughout the input.
This narrow retrieval check is not a comprehensive long-context quality eval.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import sys
import time
import uuid

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan

spec = importlib.util.spec_from_file_location('long_context_benchmark', ROOT / 'scripts/benchmark-spark-inference.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)
UNIT = 'This archive entry contains ordinary background material about maintaining a computing system.\n'


def build(repeats, codes, nonce):
    chunks = [nonce + '\nRead this synthetic archive and remember its three tagged verification codes.\n']
    quarter, remainder = divmod(repeats, 4)
    for label, value in codes.items():
        chunks += [UNIT * quarter, f'\nVERIFICATION RECORD: {label} = {value}\n\n']
    chunks += [UNIT * (quarter + remainder),
               '\nReturn only a JSON object with keys alpha, beta, gamma and their exact verification codes.']
    return ''.join(chunks)


def fit(count, target, codes, nonce):
    fixed = count(build(0, codes, nonce))
    step = (count(build(4, codes, nonce)) - fixed) / 4
    if step <= 0 or fixed >= target: raise RuntimeError('tokenizer cannot size retrieval input')
    repeats = max(0, int((target - fixed) / step))
    for _ in range(16):
        text = build(repeats, codes, nonce)
        actual = count(text)
        if 0 <= target - actual <= max(32, step): return text, actual
        change = max(1, int(abs(target - actual) / step))
        repeats = max(0, repeats + (change if actual < target else -change))
    raise RuntimeError('retrieval input sizing did not converge')


def prefix_metrics(session, base):
    r = session.get(base.removesuffix('/v1') + '/metrics', timeout=15)
    r.raise_for_status()
    result = {}
    for line in r.text.splitlines():
        if line.startswith(('vllm:prefix_cache_queries_total', 'vllm:prefix_cache_hits_total')):
            name = line.split('{', 1)[0].split()[0]
            result[name] = result.get(name, 0) + float(line.rsplit(None, 1)[1])
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--saved-plan', type=Path, required=True)
    p.add_argument('--input-tokens', type=int, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not 512 <= args.input_tokens <= min(262144, plan['recipe']['context_tokens'] - 128):
        p.error('input plus output exceeds context or probe bounds')
    if args.output.exists(): p.error('output already exists')
    codes = {label: uuid.uuid4().hex[:8] for label in ('alpha', 'beta', 'gamma')}
    base, model = plan['endpoint']['base_url'], plan['recipe']['alias']
    session = requests.Session()
    session.trust_env = False
    result = {'deployment_digest': plan['digest'], 'input_token_target': args.input_tokens,
              'expected_codes': codes, 'runs': [], 'complete': False, 'started_at': time.time()}
    save_json(args.output, result)

    def count(text):
        r = session.post(base.removesuffix('/v1') + '/tokenize', json={
            'model': model, 'messages': [{'role': 'user', 'content': text}],
            'add_generation_prompt': True}, timeout=60)
        r.raise_for_status()
        return r.json()['count']

    try:
        prompt, actual = fit(count, args.input_tokens, codes, uuid.uuid4().hex)
        result['actual_input_tokens'] = actual
        for label in ('unique-prefix', 'repeated-prefix'):
            before = prefix_metrics(session, base)
            record = benchmark.measure(base, None, model, prompt, 128, 900, capture_text=True)
            after = prefix_metrics(session, base)
            body = record['text'].strip()
            if body.startswith('```'):
                body = re.sub(r'^```(?:json)?\s*|\s*```$', '', body).strip()
            try: correct = json.loads(body) == codes
            except ValueError: correct = False
            record.update(label=label, retrieval_passed=correct,
                          tokenizer_usage_match=record['prompt_tokens'] == actual,
                          prefix_metric_deltas={k: after[k] - before.get(k, 0) for k in after})
            result['runs'].append(record)
            save_json(args.output, result)
            print(json.dumps({k: v for k, v in record.items() if k != 'text'}), flush=True)
            if not correct or not record['tokenizer_usage_match'] or record['finish_reason'] != 'stop':
                raise RuntimeError('long-context retrieval, token accounting, or completion check failed')
        result['prefix_reuse_observed'] = result['runs'][1]['prefix_metric_deltas'].get('vllm:prefix_cache_hits_total', 0) > 0
        if not result['prefix_reuse_observed']: raise RuntimeError('repeated-prefix cache hit was not observed')
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
