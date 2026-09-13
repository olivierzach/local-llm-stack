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


def build(repeats, codes, nonce, corpus='repeated'):
    chunks = [nonce + '\nRead this synthetic archive and remember its three tagged verification codes.\n']
    quarter, remainder = divmod(repeats, 4)
    def filler(start, length):
        if corpus == 'repeated':
            return UNIT * length
        return ''.join(f'Archive row {i}: host node-{i % 97}, component {("cache", "router", "worker", "storage")[i % 4]}, '
                       f'build {(i * 7919) % 100003}, latency {(i * 31) % 997} ms, '
                       f'checksum {(i * 2654435761) % 4294967296:08x}. Routine observation; no verification code.\n'
                       for i in range(start, start + length))
    for section, (label, value) in enumerate(codes.items()):
        chunks += [filler(section * quarter, quarter), f'\nVERIFICATION RECORD: {label} = {value}\n\n']
    chunks += [filler(3 * quarter, quarter + remainder),
               '\nReturn only a JSON object with keys alpha, beta, gamma and their exact verification codes.']
    return ''.join(chunks)


def fit(count, target, codes, nonce, corpus='repeated'):
    fixed = count(build(0, codes, nonce, corpus))
    step = (count(build(4, codes, nonce, corpus)) - fixed) / 4
    if step <= 0 or fixed >= target: raise RuntimeError('tokenizer cannot size retrieval input')
    if corpus == 'varied':
        # Numeric identifiers change BPE length as the corpus grows. Bracket
        # the actual tokenizer count instead of assuming a constant row size.
        low, low_count = 0, fixed
        high = max(1, int((target - fixed) / step) + 1)
        high_count = count(build(high, codes, nonce, corpus))
        while high_count <= target:
            low, low_count = high, high_count
            high *= 2
            high_count = count(build(high, codes, nonce, corpus))
        for _ in range(32):
            if target - low_count <= 128:
                return build(low, codes, nonce, corpus), low_count
            if high - low <= 1:
                raise RuntimeError('one archive row exceeds the sizing tolerance')
            guess = low + int((target - low_count) * (high - low) / (high_count - low_count))
            guess = min(high - 1, max(low + 1, guess))
            actual = count(build(guess, codes, nonce, corpus))
            if actual <= target:
                low, low_count = guess, actual
            else:
                high, high_count = guess, actual
        raise RuntimeError('varied retrieval input sizing did not converge')
    repeats = max(0, int((target - fixed) / step))
    for _ in range(16):
        text = build(repeats, codes, nonce, corpus)
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
    p.add_argument('--corpus', choices=('repeated', 'varied'), default='repeated')
    p.add_argument('--measure-decode', action='store_true', help='Also measure a 1024-token answer at this context length')
    p.add_argument('--gateway-url', help='Send inference through this /v1 gateway; tokenizer and metrics still use the pinned backend')
    p.add_argument('--key-file', type=Path, help='Private gateway API key file; never sent to the raw backend')
    args = p.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    reserve = 2048 if args.measure_decode else 128
    if not 512 <= args.input_tokens <= min(1048576, plan['recipe']['context_tokens'] - reserve):
        p.error('input plus output exceeds context or probe bounds')
    if args.output.exists(): p.error('output already exists')
    if bool(args.gateway_url) != bool(args.key_file): p.error('--gateway-url and --key-file must be provided together')
    codes = {label: uuid.uuid4().hex[:8] for label in ('alpha', 'beta', 'gamma')}
    base, model = plan['endpoint']['base_url'], plan['recipe']['alias']
    inference_base = args.gateway_url.rstrip('/') if args.gateway_url else base
    # A long gateway prefill must survive an ordinary client read deadline via
    # SSE keepalives, not just because this probe tolerates an hour of silence.
    read_timeout = 180 if args.gateway_url else 3600
    key = args.key_file.read_text().strip() if args.key_file else None
    session = requests.Session()
    session.trust_env = False
    result = {'deployment_digest': plan['digest'], 'input_token_target': args.input_tokens,
              'expected_codes': codes, 'corpus': args.corpus, 'sse_read_timeout_s': read_timeout,
              'runs': [], 'complete': False, 'started_at': time.time()}
    if args.gateway_url: result['gateway_url'] = inference_base
    save_json(args.output, result)

    def count(text):
        r = session.post(base.removesuffix('/v1') + '/tokenize', json={
            'model': model, 'messages': [{'role': 'user', 'content': text}],
            'add_generation_prompt': True}, timeout=180)
        r.raise_for_status()
        return r.json()['count']

    try:
        prompt, actual = fit(count, args.input_tokens, codes, uuid.uuid4().hex, args.corpus)
        result['actual_input_tokens'] = actual
        for label in ('unique-prefix', 'repeated-prefix'):
            before = prefix_metrics(session, base)
            record = benchmark.measure(inference_base, key, model, prompt, 128, read_timeout, capture_text=True)
            if args.gateway_url:
                headers = {k.lower(): v for k, v in record['context_headers'].items()}
                if (record['deployment_digest'] != plan['digest'] or
                    headers.get('x-context-limit') != str(plan['recipe']['context_tokens']) or
                    headers.get('x-context-input-tokens') != str(actual) or
                    headers.get('x-context-guard') == 'compacted'):
                    raise RuntimeError('gateway changed deployment, context, input accounting, or compacted the probe')
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
        if args.measure_decode:
            instruction = 'Return only a JSON object with keys alpha, beta, gamma and their exact verification codes.'
            decode_prompt = prompt.removesuffix(instruction) + ('Write a detailed engineering analysis of this archive: '
                'explain its structure, the reliability checks a production system needs, and how to validate '
                'retrieval over long logs. Include concrete examples and edge cases. Aim for at least 1500 words.')
            expected_tokens = count(decode_prompt)
            record = benchmark.measure(inference_base, key, model, decode_prompt, 1024, read_timeout, capture_text=True)
            record['tokenizer_usage_match'] = record['prompt_tokens'] == expected_tokens
            result['decode_run'] = record
            save_json(args.output, result)
            print(json.dumps({'phase': 'long-context-decode', **{k:v for k,v in record.items() if k != 'text'}}), flush=True)
            if not record['tokenizer_usage_match'] or record['completion_tokens'] < 256 or record['finish_reason'] not in ('stop', 'length'):
                raise RuntimeError('long-context decode failed')
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
